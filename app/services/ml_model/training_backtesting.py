from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score

from app.perf import current_perf_trace
from app.services.forecasting.data import _build_forecast_rows as _build_scenario_forecast_rows
from app.services.model_quality import compute_classification_metrics, compute_count_metrics

from .constants import (
    CLASSIFICATION_THRESHOLD,
    COUNT_MODEL_KEYS,
    COUNT_MODEL_LABELS,
    COUNT_SELECTION_RULE,
    EVENT_BASELINE_METHOD_LABEL,
    EVENT_BASELINE_ROLE_LABEL,
    EVENT_CLASSIFIER_ROLE_LABEL,
    EVENT_HEURISTIC_METHOD_LABEL,
    EVENT_HEURISTIC_ROLE_LABEL,
    EVENT_MODEL_LABEL,
    EVENT_PROBABILITY_REASON_SATURATED_EVENT_RATE,
    EVENT_PROBABILITY_REASON_SINGLE_CLASS_EVALUATION,
    EVENT_PROBABILITY_REASON_TOO_FEW_COMPARABLE_WINDOWS,
    EVENT_RATE_SATURATION_MARGIN,
    EVENT_SELECTION_RULE,
    MAX_BACKTEST_POINTS,
    MIN_BACKTEST_POINTS,
    MIN_FEATURE_ROWS,
    MIN_POSITIVE_PREDICTION,
    ROLLING_MIN_TRAIN_ROWS,
)
from .domain_types import (
    BacktestEvaluationRow,
    BacktestFailure,
    BacktestOverview,
    BacktestRunResult,
    BacktestSuccess,
    BacktestWindowRow,
    CountMetrics,
    EventComparisonRow,
    EventMetrics,
    HorizonSummary,
    PredictionIntervalCalibrationByHorizon,
)
from .runtime import MlProgressCallback, _emit_progress
from .training_selection import (
    _available_count_model_labels,
    _build_count_comparison_rows,
    _build_count_selection_details,
    _select_count_method,
)
from .training_dataset import _build_design_matrix, _feature_frame, _prepare_reference_frame
from .training_forecast import (
    _bound_probability,
    _build_recursive_forecast_seed,
    _count_interval,
    _evaluate_prediction_interval_backtest,
    _format_ratio_percent,
    _interval_coverage,
    _simulate_recursive_forecast_path,
)
from .training_models import (
    _fit_count_model_from_design,
    _fit_event_model_from_design,
)
from .training_temperature import _apply_temperature_statistics, _fit_temperature_statistics, _temperature_feature_columns


def _baseline_expected_count(train: pd.DataFrame, target_date: pd.Timestamp) -> float:
    recent_mean = float(train['count'].tail(28).mean()) if not train.empty else 0.0
    same_weekday = train.loc[train['weekday'] == int(target_date.weekday()), 'count'].tail(8)
    if len(same_weekday) >= 3:
        return max(0.0, float(0.6 * same_weekday.mean() + 0.4 * recent_mean))
    return max(0.0, recent_mean)


def _baseline_event_probability(train: pd.DataFrame, target_date: pd.Timestamp) -> Optional[float]:
    if train.empty:
        return None
    recent_rate = float(train['event'].tail(28).mean())
    same_weekday = train.loc[train['weekday'] == int(target_date.weekday()), 'event'].tail(8)
    if len(same_weekday) >= 3:
        probability = 0.6 * float(same_weekday.mean()) + 0.4 * recent_rate
    else:
        probability = recent_rate
    return _bound_probability(probability)


def _optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    return float(value)


def _scenario_reference_forecast(
    train: pd.DataFrame,
    test: pd.DataFrame,
    temperature_stats: Optional[Dict[str, Any]] = None,
) -> Tuple[float, Optional[float]]:
    if train.empty:
        return 0.0, None

    temperature_usable = bool((temperature_stats or {}).get('usable', True))
    temperature_value = None
    if temperature_usable and 'temp_value' in test.columns:
        candidate = test['temp_value'].iloc[0]
        if not pd.isna(candidate):
            temperature_value = float(candidate)

    train_history = [
        {
            'date': pd.Timestamp(row.date),
            'count': float(row.count),
            'avg_temperature': None if (not temperature_usable or pd.isna(row.avg_temperature)) else float(row.avg_temperature),
        }
        for row in train[['date', 'count', 'avg_temperature']].itertuples(index=False)
    ]
    forecast_rows = _build_scenario_forecast_rows(train_history, 1, temperature_value)
    if not forecast_rows:
        target_date = pd.Timestamp(test['date'].iloc[0])
        fallback_count = _baseline_expected_count(train, target_date)
        return fallback_count, _bound_probability(1.0 - math.exp(-max(0.0, fallback_count)))

    row = forecast_rows[0]
    probability = row.get('fire_probability')
    return max(0.0, float(row.get('forecast_value', 0.0))), _bound_probability(probability if probability is not None else 0.0)


def _lead_time_label(horizon_days: int) -> str:
    return '1 day' if int(horizon_days) == 1 else f'{int(horizon_days)} days'


def _lead_time_validation_horizons(max_horizon_days: int) -> List[int]:
    return list(range(1, max(1, int(max_horizon_days)) + 1))


def _enforce_monotonic_horizon_interval_calibrations(
    interval_calibration_by_horizon: Dict[int, Dict[str, Any]],
) -> Dict[int, Dict[str, Any]]:
    monotonic_calibrations: Dict[int, Dict[str, Any]] = {}
    running_floor = 0.0
    for horizon_day in sorted(interval_calibration_by_horizon):
        calibration = dict(interval_calibration_by_horizon[horizon_day])
        original_floor = float(calibration.get('absolute_error_quantile', 0.0) or 0.0)
        horizon_floor = max(running_floor, original_floor)
        running_floor = horizon_floor

        adaptive_bins: List[Dict[str, Any]] = []
        for bin_row in calibration.get('adaptive_bins') or []:
            normalized_bin = dict(bin_row)
            normalized_bin['absolute_error_quantile'] = max(
                horizon_floor,
                float(normalized_bin.get('absolute_error_quantile', 0.0) or 0.0),
            )
            adaptive_bins.append(normalized_bin)

        calibration['absolute_error_quantile'] = horizon_floor
        calibration['adaptive_bins'] = adaptive_bins
        calibration['minimum_absolute_error_quantile'] = horizon_floor
        calibration['monotone_horizon_adjusted'] = not math.isclose(
            original_floor,
            horizon_floor,
            rel_tol=1e-9,
            abs_tol=1e-9,
        )
        monotonic_calibrations[horizon_day] = calibration
    return monotonic_calibrations


def _selected_count_predictions(
    rows: List[BacktestWindowRow],
    selected_count_model_key: str,
) -> np.ndarray:
    return np.asarray(
        [float(_selected_count_prediction(row, selected_count_model_key)) for row in rows],
        dtype=float,
    )


def _optional_float_array(values: Sequence[Any]) -> np.ndarray:
    return np.asarray(
        [np.nan if value is None else float(value) for value in values],
        dtype=float,
    )


def _empty_float_array() -> np.ndarray:
    return np.asarray([], dtype=float)


def _empty_int_array() -> np.ndarray:
    return np.asarray([], dtype=int)


def _nan_float_array(length: int) -> np.ndarray:
    return np.full(length, np.nan, dtype=float)


def _selected_event_probabilities(
    rows: List[BacktestWindowRow],
    selected_count_model_key: str,
) -> np.ndarray:
    return _optional_float_array(
        [row.predicted_event_probabilities.get(selected_count_model_key) for row in rows]
    )


def _selected_count_arrays(
    rows: List[BacktestWindowRow],
    selected_count_model_key: str,
) -> Tuple[np.ndarray, np.ndarray]:
    predictions: List[float] = []
    event_probabilities: List[Optional[float]] = []
    for row in rows:
        predictions.append(float(_selected_count_prediction(row, selected_count_model_key)))
        event_probabilities.append(row.predicted_event_probabilities.get(selected_count_model_key))
    return np.asarray(predictions, dtype=float), _optional_float_array(event_probabilities)


def _optional_probability_from_array(value: Any) -> Optional[float]:
    return None if not np.isfinite(value) else float(value)


@dataclass
class _HorizonBaseArrays:
    actuals: np.ndarray
    baseline_predictions: np.ndarray
    heuristic_predictions: np.ndarray
    actual_events: np.ndarray
    baseline_event_probabilities: np.ndarray
    heuristic_event_probabilities: np.ndarray
    dates: List[str]


@dataclass
class _HorizonEvaluationData:
    rows: List[BacktestWindowRow]
    actuals: np.ndarray
    baseline_predictions: np.ndarray
    heuristic_predictions: np.ndarray
    selected_predictions: np.ndarray
    count_model_predictions: Dict[str, np.ndarray]
    actual_events: np.ndarray
    baseline_event_probabilities: np.ndarray
    heuristic_event_probabilities: np.ndarray
    selected_event_probabilities: np.ndarray
    count_model_event_probabilities: Dict[str, np.ndarray]
    coverage_by_model: Dict[str, _CandidateCoverage]
    dates: List[str]


@dataclass
class _BacktestSelection:
    scored_candidates: _ScoredCandidates
    baseline_metrics: CountMetrics
    heuristic_metrics: CountMetrics
    count_metrics: Dict[str, CountMetrics]
    selected_count_model_key: str
    selected_metrics: CountMetrics
    selection_details: Dict[str, Any]
    overdispersion_ratio: float
    validation_evaluation_data: _HorizonEvaluationData


@dataclass
class _EventMetricInputs:
    common_rows: int
    rows_used: int
    actuals: np.ndarray
    baseline_probabilities: np.ndarray
    heuristic_probabilities: np.ndarray
    classifier_probabilities: np.ndarray
    logistic_available: bool


@dataclass
class _EventMetricMaskContext:
    common_rows: int
    evaluation_mask: np.ndarray
    rows_used: int
    logistic_available: bool


@dataclass
class _EventMetricSelection:
    selected_model_key: str
    selected_model_label: str
    selected_metrics: Dict[str, Any]
    selected_roc_auc: Optional[float]
    selected_log_loss: Optional[float]
    comparison_rows: List[EventComparisonRow]


@dataclass
class _EventProbabilityScores:
    heuristic_metrics: Dict[str, Any]
    baseline_roc_auc: Optional[float]
    heuristic_roc_auc: Optional[float]
    baseline_log_loss: Optional[float]
    heuristic_log_loss: Optional[float]


@dataclass
class _EventMetricContext:
    event_rate: Optional[float]
    evaluation_has_both_classes: bool
    event_probability_informative: bool
    event_probability_note: Optional[str]
    event_probability_reason_code: Optional[str]


@dataclass
class _BacktestEvaluationArtifacts:
    selection: _BacktestSelection
    selected_count_model_key: str
    horizon_evaluation_data: Dict[int, _HorizonEvaluationData]
    interval_calibration_by_horizon: Dict[int, Dict[str, Any]]
    horizon_summaries: Dict[str, HorizonSummary]
    prediction_interval_calibration: Dict[str, Any]
    validation_summary: HorizonSummary
    prediction_interval_coverage: Optional[float]
    backtest_rows: List[BacktestEvaluationRow]
    event_metrics: EventMetrics
    window_rows: List[BacktestWindowRow]


@dataclass
class _BacktestOriginSelection:
    available_backtest_points: int
    selected_origin_dates: List[Any]
    total_windows: int


def _build_count_model_prediction_context_from_buffers(
    *,
    prediction_buffers: Dict[str, List[float]],
    event_probability_buffers: Dict[str, List[Optional[float]]],
    window_count: int,
) -> Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray], Dict[str, _CandidateCoverage]]:
    coverage_by_model: Dict[str, _CandidateCoverage] = {}
    prediction_arrays: Dict[str, np.ndarray] = {}
    for model_key, predictions in prediction_buffers.items():
        covered_window_count = len(predictions)
        coverage_by_model[model_key] = _CandidateCoverage(
            covered_window_count=covered_window_count,
            window_count=window_count,
            window_coverage=float(covered_window_count / window_count) if window_count else 0.0,
        )
        if covered_window_count == window_count:
            prediction_arrays[model_key] = np.asarray(predictions, dtype=float)

    event_probability_arrays = {
        model_key: _optional_float_array(probabilities)
        for model_key, probabilities in event_probability_buffers.items()
    }
    return prediction_arrays, event_probability_arrays, coverage_by_model


def _build_horizon_array_context(
    rows_for_horizon: List[BacktestWindowRow],
    *,
    include_count_model_predictions: bool,
) -> Tuple[_HorizonBaseArrays, Dict[str, np.ndarray], Dict[str, np.ndarray], Dict[str, _CandidateCoverage]]:
    actuals: List[float] = []
    baseline_predictions: List[float] = []
    heuristic_predictions: List[float] = []
    actual_events: List[int] = []
    baseline_event_probabilities: List[float] = []
    heuristic_event_probabilities: List[float] = []
    dates: List[str] = []
    prediction_buffers: Dict[str, List[float]] = {}
    event_probability_buffers: Dict[str, List[Optional[float]]] = {}
    if include_count_model_predictions:
        prediction_buffers = {model_key: [] for model_key in COUNT_MODEL_KEYS}
        event_probability_buffers = {model_key: [] for model_key in COUNT_MODEL_KEYS}
    for row in rows_for_horizon:
        actuals.append(float(row.actual_count))
        baseline_predictions.append(float(row.baseline_count))
        heuristic_predictions.append(float(row.heuristic_count))
        actual_events.append(int(row.actual_event))
        baseline_event_probabilities.append(
            np.nan if row.baseline_event_probability is None else float(row.baseline_event_probability)
        )
        heuristic_event_probabilities.append(
            np.nan if row.heuristic_event_probability is None else float(row.heuristic_event_probability)
        )
        dates.append(row.date)
        if include_count_model_predictions:
            for model_key in COUNT_MODEL_KEYS:
                prediction = row.predictions.get(model_key)
                if prediction is not None:
                    prediction_buffers[model_key].append(float(prediction))
                event_probability_buffers[model_key].append(row.predicted_event_probabilities.get(model_key))

    base_arrays = _HorizonBaseArrays(
        actuals=np.asarray(actuals, dtype=float),
        baseline_predictions=np.asarray(baseline_predictions, dtype=float),
        heuristic_predictions=np.asarray(heuristic_predictions, dtype=float),
        actual_events=np.asarray(actual_events, dtype=int),
        baseline_event_probabilities=np.asarray(baseline_event_probabilities, dtype=float),
        heuristic_event_probabilities=np.asarray(heuristic_event_probabilities, dtype=float),
        dates=dates,
    )
    if not include_count_model_predictions:
        return base_arrays, {}, {}, {}
    count_model_predictions, count_model_event_probabilities, coverage_by_model = _build_count_model_prediction_context_from_buffers(
        prediction_buffers=prediction_buffers,
        event_probability_buffers=event_probability_buffers,
        window_count=len(rows_for_horizon),
    )
    return base_arrays, count_model_predictions, count_model_event_probabilities, coverage_by_model


def _selected_count_arrays_from_evaluation_data(
    evaluation_data: _HorizonEvaluationData,
    selected_count_model_key: Optional[str],
) -> Tuple[np.ndarray, np.ndarray]:
    if selected_count_model_key == 'seasonal_baseline':
        return evaluation_data.baseline_predictions, _nan_float_array(len(evaluation_data.rows))
    if selected_count_model_key == 'heuristic_forecast':
        return evaluation_data.heuristic_predictions, _nan_float_array(len(evaluation_data.rows))
    if selected_count_model_key in evaluation_data.count_model_predictions:
        selected_event_probabilities = evaluation_data.count_model_event_probabilities.get(selected_count_model_key)
        if selected_event_probabilities is None:
            selected_event_probabilities = (
                _selected_event_probabilities(evaluation_data.rows, selected_count_model_key)
                if selected_count_model_key in COUNT_MODEL_KEYS
                else _nan_float_array(len(evaluation_data.rows))
            )
        return evaluation_data.count_model_predictions[selected_count_model_key], selected_event_probabilities
    if selected_count_model_key in evaluation_data.count_model_event_probabilities:
        return (
            _selected_count_predictions(evaluation_data.rows, selected_count_model_key),
            evaluation_data.count_model_event_probabilities[selected_count_model_key],
        )
    if selected_count_model_key in COUNT_MODEL_KEYS:
        return _selected_count_arrays(evaluation_data.rows, selected_count_model_key)
    if selected_count_model_key:
        return (
            _selected_count_predictions(evaluation_data.rows, selected_count_model_key),
            _nan_float_array(len(evaluation_data.rows)),
        )
    return _empty_float_array(), _nan_float_array(len(evaluation_data.rows))


def _with_selected_count_model(
    evaluation_data: _HorizonEvaluationData,
    selected_count_model_key: str,
) -> _HorizonEvaluationData:
    selected_predictions, selected_event_probabilities = _selected_count_arrays_from_evaluation_data(
        evaluation_data,
        selected_count_model_key,
    )
    return _HorizonEvaluationData(
        rows=evaluation_data.rows,
        actuals=evaluation_data.actuals,
        baseline_predictions=evaluation_data.baseline_predictions,
        heuristic_predictions=evaluation_data.heuristic_predictions,
        selected_predictions=selected_predictions,
        count_model_predictions=evaluation_data.count_model_predictions,
        actual_events=evaluation_data.actual_events,
        baseline_event_probabilities=evaluation_data.baseline_event_probabilities,
        heuristic_event_probabilities=evaluation_data.heuristic_event_probabilities,
        selected_event_probabilities=selected_event_probabilities,
        count_model_event_probabilities=evaluation_data.count_model_event_probabilities,
        coverage_by_model=evaluation_data.coverage_by_model,
        dates=evaluation_data.dates,
    )


def _build_horizon_evaluation_data(
    rows_for_horizon: List[BacktestWindowRow],
    selected_count_model_key: Optional[str] = None,
    *,
    include_count_model_predictions: bool = False,
) -> _HorizonEvaluationData:
    (
        base_arrays,
        count_model_predictions,
        count_model_event_probabilities,
        coverage_by_model,
    ) = _build_horizon_array_context(
        rows_for_horizon,
        include_count_model_predictions=include_count_model_predictions,
    )

    evaluation_data = _HorizonEvaluationData(
        rows=rows_for_horizon,
        actuals=base_arrays.actuals,
        baseline_predictions=base_arrays.baseline_predictions,
        heuristic_predictions=base_arrays.heuristic_predictions,
        selected_predictions=_empty_float_array(),
        count_model_predictions=count_model_predictions,
        actual_events=base_arrays.actual_events,
        baseline_event_probabilities=base_arrays.baseline_event_probabilities,
        heuristic_event_probabilities=base_arrays.heuristic_event_probabilities,
        selected_event_probabilities=_empty_float_array(),
        count_model_event_probabilities=count_model_event_probabilities,
        coverage_by_model=coverage_by_model,
        dates=base_arrays.dates,
    )
    if selected_count_model_key:
        return _with_selected_count_model(evaluation_data, selected_count_model_key)
    return evaluation_data


def _build_horizon_evaluation_data_by_horizon(
    horizon_rows: Dict[int, List[BacktestWindowRow]],
    horizon_days: List[int],
    selected_count_model_key: str,
    precomputed: Optional[Dict[int, _HorizonEvaluationData]] = None,
) -> Dict[int, _HorizonEvaluationData]:
    evaluation_data_by_horizon: Dict[int, _HorizonEvaluationData] = {}
    precomputed = precomputed or {}
    for horizon_day in horizon_days:
        evaluation_data = precomputed.get(horizon_day)
        if evaluation_data is None:
            evaluation_data = _build_horizon_evaluation_data(
                horizon_rows[horizon_day],
                selected_count_model_key,
                include_count_model_predictions=True,
            )
        evaluation_data_by_horizon[horizon_day] = evaluation_data
    return evaluation_data_by_horizon


def _prediction_interval_evaluation_slice(calibration: Dict[str, Any], total_rows: int) -> Optional[slice]:
    if not bool(calibration.get('coverage_validated')):
        return None

    calibration_windows = max(0, int(calibration.get('calibration_window_count') or 0))
    evaluation_windows = max(0, int(calibration.get('evaluation_window_count') or 0))
    if evaluation_windows <= 0:
        return None

    start = min(calibration_windows, total_rows)
    end = min(total_rows, start + evaluation_windows)
    if end <= start:
        return None
    return slice(start, end)


def _remeasure_deployed_interval_calibration(
    rows_for_horizon: List[BacktestWindowRow],
    *,
    selected_count_model_key: str,
    calibration: Dict[str, Any],
    evaluation_data: Optional[_HorizonEvaluationData] = None,
) -> Dict[str, Any]:
    updated_calibration = dict(calibration)
    total_rows = len(evaluation_data.rows) if evaluation_data is not None else len(rows_for_horizon)
    evaluation_slice = _prediction_interval_evaluation_slice(updated_calibration, total_rows)
    if evaluation_slice is None:
        return updated_calibration

    actual_values = (
        evaluation_data.actuals
        if evaluation_data is not None
        else np.asarray([row.actual_count for row in rows_for_horizon], dtype=float)
    )
    prediction_values = (
        evaluation_data.selected_predictions
        if evaluation_data is not None
        else _selected_count_predictions(rows_for_horizon, selected_count_model_key)
    )
    deployed_coverage = _interval_coverage(
        actual_values[evaluation_slice],
        prediction_values[evaluation_slice],
        updated_calibration,
    )
    reference_coverage = updated_calibration.get('validated_coverage')

    updated_calibration['validated_coverage_reference'] = reference_coverage
    updated_calibration['validated_coverage_reference_display'] = _format_ratio_percent(reference_coverage)
    updated_calibration['validated_coverage'] = deployed_coverage
    updated_calibration['validated_coverage_scope'] = (
        'deployed_interval_remeasured_after_monotonic_horizon_widening'
        if updated_calibration.get('monotone_horizon_adjusted')
        else 'deployed_interval_remeasured'
    )

    note = str(updated_calibration.get('coverage_note') or '').strip()
    if 'deployed interval is remeasured on the same later evaluation windows' not in note:
        remeasured_note = (
            'Coverage shown for the deployed interval is remeasured on the same later evaluation windows'
        )
        if updated_calibration.get('monotone_horizon_adjusted'):
            remeasured_note = f'{remeasured_note} after monotonic horizon widening.'
        else:
            remeasured_note = f'{remeasured_note}.'
        note = f'{note} {remeasured_note}'.strip() if note else remeasured_note
    updated_calibration['coverage_note'] = note
    return updated_calibration


def _sync_horizon_summary_with_calibration(
    summary: HorizonSummary,
    calibration: Dict[str, Any],
) -> HorizonSummary:
    return summary.clone(
        prediction_interval_coverage=calibration.get('validated_coverage'),
        prediction_interval_coverage_display=_format_ratio_percent(calibration.get('validated_coverage')),
        prediction_interval_coverage_validated=bool(calibration.get('coverage_validated', False)),
        prediction_interval_coverage_note=calibration.get('coverage_note'),
        prediction_interval_validation_scheme_key=calibration.get('validation_scheme_key'),
        prediction_interval_validation_scheme_label=calibration.get('validation_scheme_label'),
        prediction_interval_method_label=calibration.get('method_label'),
    )


def _reconcile_horizon_interval_metadata(
    interval_calibration_by_horizon: Dict[int, Dict[str, Any]],
    horizon_rows: Dict[int, List[BacktestWindowRow]],
    horizon_summaries: Dict[str, HorizonSummary],
    *,
    selected_count_model_key: str,
    evaluation_data_by_horizon: Optional[Dict[int, _HorizonEvaluationData]] = None,
) -> Tuple[Dict[int, Dict[str, Any]], Dict[str, HorizonSummary]]:
    updated_calibrations: Dict[int, Dict[str, Any]] = {}
    updated_summaries: Dict[str, HorizonSummary] = {}
    for horizon_day, calibration in interval_calibration_by_horizon.items():
        evaluation_data = (evaluation_data_by_horizon or {}).get(horizon_day)
        updated_calibration = _remeasure_deployed_interval_calibration(
            horizon_rows.get(horizon_day, []),
            selected_count_model_key=selected_count_model_key,
            calibration=calibration,
            evaluation_data=evaluation_data,
        )
        updated_calibrations[horizon_day] = updated_calibration
        updated_summaries[str(horizon_day)] = _sync_horizon_summary_with_calibration(
            horizon_summaries[str(horizon_day)],
            updated_calibration,
        )
    return updated_calibrations, updated_summaries


def _not_ready_backtest(message: str) -> BacktestFailure:
    return BacktestFailure(message=message)


@dataclass
class _BacktestWindow:
    origin_date: pd.Timestamp
    prepared_train: pd.DataFrame
    future_rows: pd.DataFrame
    feature_columns: List[str]
    model_train_design: pd.DataFrame
    count_targets: np.ndarray
    event_targets: np.ndarray
    temperature_stats: Dict[str, Any]


@dataclass
class _WindowCandidateFits:
    event_bundle: Optional[Dict[str, Any]]
    forecast_paths: Dict[str, Optional[List[Dict[str, Any]]]]


@dataclass
class _CandidateCoverage:
    covered_window_count: int
    window_count: int
    window_coverage: float


@dataclass
class _ScoredCandidates:
    baseline_metrics: CountMetrics
    heuristic_metrics: CountMetrics
    count_metrics: Dict[str, CountMetrics]
    coverage_by_model: Dict[str, _CandidateCoverage]


def _build_window(
    *,
    history_frame: pd.DataFrame,
    history_dates: np.ndarray,
    origin_date: pd.Timestamp,
    max_horizon_days: int,
    min_train_rows: int,
) -> Optional[_BacktestWindow]:
    origin_cutoff = int(np.searchsorted(history_dates, origin_date.to_datetime64(), side='right'))
    reference_train = history_frame.iloc[:origin_cutoff]
    future_rows = history_frame.iloc[origin_cutoff : origin_cutoff + max_horizon_days]
    if reference_train.empty or len(future_rows) < max_horizon_days:
        return None

    window_temperature_stats = _fit_temperature_statistics(reference_train, frame_is_prepared=True)
    prepared_train = _apply_temperature_statistics(
        reference_train,
        window_temperature_stats,
        frame_is_prepared=True,
    )
    window_feature_columns = _temperature_feature_columns(window_temperature_stats)
    featured_train = _feature_frame(prepared_train)
    valid_train_rows = featured_train[window_feature_columns + ['count']].notna().all(axis=1)
    model_train = featured_train.loc[valid_train_rows]
    if len(model_train) < min_train_rows:
        return None

    return _BacktestWindow(
        origin_date=origin_date,
        prepared_train=prepared_train,
        future_rows=future_rows,
        feature_columns=window_feature_columns,
        model_train_design=_build_design_matrix(model_train, feature_columns=window_feature_columns),
        count_targets=model_train['count'].to_numpy(dtype=float),
        event_targets=model_train['event'].to_numpy(dtype=int),
        temperature_stats=window_temperature_stats,
    )


def _simulate_candidate_paths(
    *,
    window: _BacktestWindow,
    count_model_bundles: Dict[str, Optional[Dict[str, Any]]],
    event_bundle: Optional[Dict[str, Any]],
    forecast_days: int,
) -> Dict[str, Optional[List[Dict[str, Any]]]]:
    forecast_paths: Dict[str, Optional[List[Dict[str, Any]]]] = {}
    candidate_specs: List[Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]] = [
        ('seasonal_baseline', None, None),
        ('heuristic_forecast', None, None),
    ]
    candidate_specs.extend(
        (model_key, count_model_bundles.get(model_key), event_bundle)
        for model_key in COUNT_MODEL_KEYS
    )
    simulation_seed = _build_recursive_forecast_seed(window.prepared_train, window.temperature_stats)

    for model_key, count_model, event_model in candidate_specs:
        if model_key in COUNT_MODEL_KEYS and count_model is None:
            forecast_paths[model_key] = None
            continue
        forecast_paths[model_key] = _simulate_recursive_forecast_path(
            frame=window.prepared_train,
            selected_count_model_key=model_key,
            count_model=count_model,
            event_model=event_model,
            forecast_days=forecast_days,
            scenario_temperature=None,
            baseline_expected_count=_baseline_expected_count,
            temperature_stats=window.temperature_stats,
            baseline_event_probability=_baseline_event_probability,
            simulation_seed=simulation_seed,
        )
    return forecast_paths


def _fit_candidates(
    window: _BacktestWindow,
    *,
    forecast_days: int,
) -> _WindowCandidateFits:
    count_model_bundles = {
        model_key: _fit_count_model_from_design(model_key, window.model_train_design, window.count_targets)
        for model_key in COUNT_MODEL_KEYS
    }
    event_bundle = _fit_event_model_from_design(window.model_train_design, window.event_targets)
    return _WindowCandidateFits(
        event_bundle=event_bundle,
        forecast_paths=_simulate_candidate_paths(
            window=window,
            count_model_bundles=count_model_bundles,
            event_bundle=event_bundle,
            forecast_days=forecast_days,
        ),
    )


def _build_window_rows(
    *,
    window: _BacktestWindow,
    candidate_fits: _WindowCandidateFits,
    horizon_days: List[int],
) -> List[BacktestWindowRow]:
    baseline_path = candidate_fits.forecast_paths['seasonal_baseline'] or []
    heuristic_path = candidate_fits.forecast_paths['heuristic_forecast'] or []
    count_model_paths = {
        model_key: candidate_fits.forecast_paths.get(model_key)
        for model_key in COUNT_MODEL_KEYS
    }
    rows: List[BacktestWindowRow] = []
    for horizon_day in horizon_days:
        step_index = horizon_day - 1
        actual_row = window.future_rows.iloc[step_index]
        baseline_step = baseline_path[step_index]
        heuristic_step = heuristic_path[step_index]
        rows.append(
            BacktestWindowRow(
                origin_date=window.origin_date.date().isoformat(),
                date=pd.Timestamp(actual_row['date']).date().isoformat(),
                horizon_days=horizon_day,
                actual_count=float(actual_row['count']),
                baseline_count=float(baseline_step['forecast_value']),
                heuristic_count=float(heuristic_step['forecast_value']),
                actual_event=int(actual_row['event']),
                baseline_event_probability=baseline_step['event_probability'],
                heuristic_event_probability=heuristic_step['event_probability'],
                predictions={
                    model_key: (
                        None
                        if model_path is None
                        else float(model_path[step_index]['forecast_value'])
                    )
                    for model_key, model_path in count_model_paths.items()
                },
                predicted_event_probabilities={
                    model_key: (
                        None
                        if model_path is None
                        else model_path[step_index]['event_probability']
                    )
                    for model_key, model_path in count_model_paths.items()
                },
            )
        )
    return rows


def _score_candidates(evaluation_data: _HorizonEvaluationData) -> _ScoredCandidates:
    baseline_metrics = CountMetrics.coerce(
        compute_count_metrics(evaluation_data.actuals, evaluation_data.baseline_predictions)
    )
    heuristic_metrics = CountMetrics.coerce(
        compute_count_metrics(evaluation_data.actuals, evaluation_data.heuristic_predictions, baseline_metrics)
    )

    count_metrics: Dict[str, CountMetrics] = {}
    for model_key, coverage in evaluation_data.coverage_by_model.items():
        if coverage.covered_window_count != coverage.window_count:
            continue
        predictions = evaluation_data.count_model_predictions[model_key]
        count_metrics[model_key] = CountMetrics.coerce(
            compute_count_metrics(evaluation_data.actuals, predictions, baseline_metrics)
        )

    return _ScoredCandidates(
        baseline_metrics=baseline_metrics,
        heuristic_metrics=heuristic_metrics,
        count_metrics=count_metrics,
        coverage_by_model=evaluation_data.coverage_by_model,
    )


def _select_working_method(
    scored_candidates: _ScoredCandidates,
) -> Tuple[str, CountMetrics, Dict[str, Any]]:
    return _select_count_method(
        scored_candidates.baseline_metrics,
        scored_candidates.heuristic_metrics,
        scored_candidates.count_metrics,
    )


def _evaluate_horizon_rows(
    evaluation_data: _HorizonEvaluationData,
    *,
    horizon_day: int,
) -> Tuple[HorizonSummary, Dict[str, Any]]:
    baseline_metrics_h = CountMetrics.coerce(
        compute_count_metrics(evaluation_data.actuals, evaluation_data.baseline_predictions)
    )
    heuristic_metrics_h = CountMetrics.coerce(
        compute_count_metrics(evaluation_data.actuals, evaluation_data.heuristic_predictions, baseline_metrics_h)
    )
    selected_metrics_h = CountMetrics.coerce(
        compute_count_metrics(evaluation_data.actuals, evaluation_data.selected_predictions, baseline_metrics_h)
    )
    prediction_interval_backtest_h = _evaluate_prediction_interval_backtest(
        evaluation_data.actuals,
        evaluation_data.selected_predictions,
        evaluation_data.dates,
        horizon_days=horizon_day,
    )
    return (
        HorizonSummary(
            horizon_days=horizon_day,
            horizon_label=_lead_time_label(horizon_day),
            folds=len(evaluation_data.rows),
            count_metrics=selected_metrics_h,
            baseline_count_mae=baseline_metrics_h.mae,
            heuristic_count_mae=heuristic_metrics_h.mae,
            prediction_interval_coverage=prediction_interval_backtest_h['coverage'],
            prediction_interval_coverage_display=_format_ratio_percent(prediction_interval_backtest_h['coverage']),
            prediction_interval_coverage_validated=prediction_interval_backtest_h['coverage_validated'],
            prediction_interval_coverage_note=prediction_interval_backtest_h['coverage_note'],
            prediction_interval_validation_scheme_key=prediction_interval_backtest_h.get('validation_scheme_key'),
            prediction_interval_validation_scheme_label=prediction_interval_backtest_h.get('validation_scheme_label'),
            prediction_interval_method_label=prediction_interval_backtest_h['calibration'].get('method_label'),
        ),
        prediction_interval_backtest_h['calibration'],
    )


def _collect_backtest_horizon_rows(
    *,
    history_frame: pd.DataFrame,
    history_dates: np.ndarray,
    selected_origin_dates: Sequence[Any],
    total_windows: int,
    max_horizon_days: int,
    min_train_rows: int,
    horizon_days: List[int],
    progress_callback: MlProgressCallback,
) -> Tuple[Dict[int, List[BacktestWindowRow]], int]:
    horizon_rows: Dict[int, List[BacktestWindowRow]] = {horizon_day: [] for horizon_day in horizon_days}
    comparable_windows = 0

    for completed_windows, origin_date_value in enumerate(selected_origin_dates, start=1):
        origin_date = pd.Timestamp(origin_date_value)
        window = _build_window(
            history_frame=history_frame,
            history_dates=history_dates,
            origin_date=origin_date,
            max_horizon_days=max_horizon_days,
            min_train_rows=min_train_rows,
        )
        if window is None:
            continue

        candidate_fits = _fit_candidates(window, forecast_days=max_horizon_days)

        comparable_windows += 1
        for row in _build_window_rows(
            window=window,
            candidate_fits=candidate_fits,
            horizon_days=horizon_days,
        ):
            horizon_rows[row.horizon_days].append(row)

        if completed_windows == 1 or completed_windows == total_windows or completed_windows % 5 == 0:
            _emit_progress(
                progress_callback,
                'ml_backtest.running',
                f'Backtesting: processed {completed_windows} of {total_windows} rolling origins.',
            )

    return horizon_rows, comparable_windows


def _evaluate_backtest_horizon_metadata(
    *,
    horizon_rows: Dict[int, List[BacktestWindowRow]],
    horizon_days: List[int],
    selected_count_model_key: str,
    evaluation_data_by_horizon: Dict[int, _HorizonEvaluationData],
) -> Tuple[Dict[int, Dict[str, Any]], Dict[str, HorizonSummary]]:
    interval_calibration_by_horizon: Dict[int, Dict[str, Any]] = {}
    horizon_summaries: Dict[str, HorizonSummary] = {}
    for horizon_day in horizon_days:
        horizon_summary, interval_calibration = _evaluate_horizon_rows(
            evaluation_data_by_horizon[horizon_day],
            horizon_day=horizon_day,
        )
        interval_calibration_by_horizon[horizon_day] = interval_calibration
        horizon_summaries[str(horizon_day)] = horizon_summary

    interval_calibration_by_horizon = _enforce_monotonic_horizon_interval_calibrations(interval_calibration_by_horizon)
    return _reconcile_horizon_interval_metadata(
        interval_calibration_by_horizon,
        horizon_rows,
        horizon_summaries,
        selected_count_model_key=selected_count_model_key,
        evaluation_data_by_horizon=evaluation_data_by_horizon,
    )


def _build_backtest_payload_rows(
    *,
    evaluation_data: _HorizonEvaluationData,
    prediction_interval_calibration: Dict[str, Any],
    validation_horizon_days: int,
) -> Tuple[List[BacktestEvaluationRow], List[BacktestWindowRow]]:
    backtest_rows: List[BacktestEvaluationRow] = []
    window_rows: List[BacktestWindowRow] = []
    for row, predicted_value, event_probability in zip(
        evaluation_data.rows,
        evaluation_data.selected_predictions,
        evaluation_data.selected_event_probabilities,
    ):
        predicted_count = float(predicted_value)
        predicted_event_probability = _optional_probability_from_array(event_probability)
        lower_bound, upper_bound = _count_interval(predicted_count, prediction_interval_calibration)
        backtest_rows.append(
            BacktestEvaluationRow(
                origin_date=row.origin_date,
                horizon_days=validation_horizon_days,
                date=row.date,
                actual_count=row.actual_count,
                predicted_count=predicted_count,
                lower_bound=lower_bound,
                upper_bound=upper_bound,
                baseline_count=row.baseline_count,
                heuristic_count=row.heuristic_count,
                actual_event=row.actual_event,
                predicted_event_probability=predicted_event_probability,
                baseline_event_probability=row.baseline_event_probability,
                heuristic_event_probability=row.heuristic_event_probability,
            )
        )
        window_rows.append(row.clone(predicted_event_probability=predicted_event_probability))
    return backtest_rows, window_rows


def _select_backtest_count_model(
    valid_rows: List[BacktestWindowRow],
    dataset: pd.DataFrame,
) -> _BacktestSelection:
    evaluation_data = _build_horizon_evaluation_data(
        valid_rows,
        include_count_model_predictions=True,
    )
    scored_candidates = _score_candidates(evaluation_data)
    selected_count_model_key, selected_metrics, selection_context = _select_working_method(scored_candidates)
    overdispersion_ratio = _estimate_overdispersion_ratio(dataset['count'].to_numpy(dtype=float))
    selection_details = _build_count_selection_details(
        selected_count_model_key=selected_count_model_key,
        selected_metrics=selected_metrics,
        count_metrics=scored_candidates.count_metrics,
        baseline_metrics=scored_candidates.baseline_metrics,
        heuristic_metrics=scored_candidates.heuristic_metrics,
        overdispersion_ratio=overdispersion_ratio,
        raw_best_key=selection_context.get('raw_best_key'),
        tie_break_reason=selection_context.get('tie_break_reason'),
    )
    return _BacktestSelection(
        scored_candidates=scored_candidates,
        baseline_metrics=scored_candidates.baseline_metrics,
        heuristic_metrics=scored_candidates.heuristic_metrics,
        count_metrics=scored_candidates.count_metrics,
        selected_count_model_key=selected_count_model_key,
        selected_metrics=selected_metrics,
        selection_details=selection_details,
        overdispersion_ratio=overdispersion_ratio,
        validation_evaluation_data=_with_selected_count_model(evaluation_data, selected_count_model_key),
    )


def _build_backtest_overview(
    *,
    backtest_rows: List[BacktestEvaluationRow],
    valid_rows: List[BacktestWindowRow],
    min_train_rows: int,
    validation_horizon_days: int,
    max_horizon_days: int,
    horizon_days: List[int],
    selection: _BacktestSelection,
    event_metrics: EventMetrics,
    prediction_interval_calibration: Dict[str, Any],
    prediction_interval_coverage: Optional[float],
    validation_summary: HorizonSummary,
    horizon_summaries: Dict[str, HorizonSummary],
) -> BacktestOverview:
    return BacktestOverview(
        folds=len(backtest_rows),
        min_train_rows=min_train_rows,
        validation_horizon_days=validation_horizon_days,
        validation_horizon_label=_lead_time_label(validation_horizon_days),
        forecast_horizon_days=max_horizon_days,
        forecast_horizon_label=_lead_time_label(max_horizon_days),
        validated_horizon_days=horizon_days,
        selection_rule=COUNT_SELECTION_RULE,
        event_selection_rule=EVENT_SELECTION_RULE,
        classification_threshold=CLASSIFICATION_THRESHOLD,
        event_backtest_event_rate=event_metrics.event_rate,
        event_probability_informative=event_metrics.event_probability_informative,
        event_probability_note=event_metrics.event_probability_note,
        event_probability_reason_code=event_metrics.event_probability_reason_code,
        candidate_model_labels=_available_count_model_labels(selection.count_metrics),
        candidate_window_count=len(valid_rows),
        candidate_covered_window_count_by_model={
            model_key: selection.scored_candidates.coverage_by_model[model_key].covered_window_count
            for model_key in COUNT_MODEL_KEYS
        },
        candidate_window_coverage_by_model={
            model_key: selection.scored_candidates.coverage_by_model[model_key].window_coverage
            for model_key in COUNT_MODEL_KEYS
        },
        dispersion_ratio=selection.overdispersion_ratio,
        prediction_interval_level=prediction_interval_calibration['level'],
        prediction_interval_level_display=prediction_interval_calibration['level_display'],
        prediction_interval_coverage=prediction_interval_coverage,
        prediction_interval_coverage_display=_format_ratio_percent(prediction_interval_coverage),
        prediction_interval_method_label=prediction_interval_calibration['method_label'],
        prediction_interval_coverage_validated=validation_summary.prediction_interval_coverage_validated,
        prediction_interval_coverage_note=validation_summary.prediction_interval_coverage_note,
        prediction_interval_calibration_windows=prediction_interval_calibration['calibration_window_count'],
        prediction_interval_evaluation_windows=prediction_interval_calibration['evaluation_window_count'],
        prediction_interval_validation_scheme_key=prediction_interval_calibration.get('validation_scheme_key'),
        prediction_interval_validation_scheme_label=prediction_interval_calibration.get('validation_scheme_label'),
        prediction_interval_validation_explanation=prediction_interval_calibration.get('validation_scheme_explanation'),
        prediction_interval_calibration_range_label=prediction_interval_calibration['calibration_window_range_label'],
        prediction_interval_evaluation_range_label=prediction_interval_calibration['evaluation_window_range_label'],
        prediction_interval_validated_horizon_days=[
            horizon_day
            for horizon_day in horizon_days
            if horizon_summaries[str(horizon_day)].prediction_interval_coverage_validated
        ],
        prediction_interval_coverage_by_horizon={
            str(horizon_day): horizon_summaries[str(horizon_day)].prediction_interval_coverage
            for horizon_day in horizon_days
        },
        prediction_interval_coverage_display_by_horizon={
            str(horizon_day): horizon_summaries[str(horizon_day)].prediction_interval_coverage_display
            for horizon_day in horizon_days
        },
        rolling_scheme_label=(
            'Rolling-origin backtesting (expanding window, lead-time-aware): '
            f'{len(backtest_rows)} origins, horizons 1-{max_horizon_days} days, '
            f'summary on the {_lead_time_label(validation_horizon_days)} lead'
        ),
    )


def _build_backtest_evaluation_artifacts(
    *,
    horizon_rows: Dict[int, List[BacktestWindowRow]],
    horizon_days: List[int],
    valid_rows: List[BacktestWindowRow],
    dataset: pd.DataFrame,
    validation_horizon_days: int,
) -> _BacktestEvaluationArtifacts:
    selection = _select_backtest_count_model(valid_rows, dataset)
    selected_count_model_key = selection.selected_count_model_key
    horizon_evaluation_data = _build_horizon_evaluation_data_by_horizon(
        horizon_rows,
        horizon_days,
        selected_count_model_key,
        precomputed={validation_horizon_days: selection.validation_evaluation_data},
    )
    interval_calibration_by_horizon, horizon_summaries = _evaluate_backtest_horizon_metadata(
        horizon_rows=horizon_rows,
        horizon_days=horizon_days,
        selected_count_model_key=selected_count_model_key,
        evaluation_data_by_horizon=horizon_evaluation_data,
    )
    prediction_interval_calibration = interval_calibration_by_horizon[validation_horizon_days]
    validation_summary = horizon_summaries[str(validation_horizon_days)]
    validation_evaluation_data = horizon_evaluation_data[validation_horizon_days]
    backtest_rows, window_rows = _build_backtest_payload_rows(
        evaluation_data=validation_evaluation_data,
        prediction_interval_calibration=prediction_interval_calibration,
        validation_horizon_days=validation_horizon_days,
    )
    return _BacktestEvaluationArtifacts(
        selection=selection,
        selected_count_model_key=selected_count_model_key,
        horizon_evaluation_data=horizon_evaluation_data,
        interval_calibration_by_horizon=interval_calibration_by_horizon,
        horizon_summaries=horizon_summaries,
        prediction_interval_calibration=prediction_interval_calibration,
        validation_summary=validation_summary,
        prediction_interval_coverage=validation_summary.prediction_interval_coverage,
        backtest_rows=backtest_rows,
        event_metrics=_compute_event_metrics(validation_evaluation_data),
        window_rows=window_rows,
    )


def _build_backtest_success_result(
    *,
    artifacts: _BacktestEvaluationArtifacts,
    valid_rows: List[BacktestWindowRow],
    min_train_rows: int,
    validation_horizon_days: int,
    max_horizon_days: int,
    horizon_days: List[int],
) -> BacktestSuccess:
    overview = _build_backtest_overview(
        backtest_rows=artifacts.backtest_rows,
        valid_rows=valid_rows,
        min_train_rows=min_train_rows,
        validation_horizon_days=validation_horizon_days,
        max_horizon_days=max_horizon_days,
        horizon_days=horizon_days,
        selection=artifacts.selection,
        event_metrics=artifacts.event_metrics,
        prediction_interval_calibration=artifacts.prediction_interval_calibration,
        prediction_interval_coverage=artifacts.prediction_interval_coverage,
        validation_summary=artifacts.validation_summary,
        horizon_summaries=artifacts.horizon_summaries,
    )
    return BacktestSuccess(
        message='',
        rows=artifacts.backtest_rows,
        window_rows=artifacts.window_rows,
        baseline_metrics=artifacts.selection.baseline_metrics,
        heuristic_metrics=artifacts.selection.heuristic_metrics,
        count_metrics=artifacts.selection.count_metrics,
        count_comparison_rows=_build_count_comparison_rows(
            baseline_metrics=artifacts.selection.baseline_metrics,
            heuristic_metrics=artifacts.selection.heuristic_metrics,
            count_metrics=artifacts.selection.count_metrics,
            selected_count_model_key=artifacts.selected_count_model_key,
        ),
        selected_count_model_key=artifacts.selected_count_model_key,
        selected_count_model_reason=artifacts.selection.selection_details['long'],
        selected_count_model_reason_short=artifacts.selection.selection_details['short'],
        selected_metrics=artifacts.selection.selected_metrics,
        prediction_interval_calibration=artifacts.prediction_interval_calibration,
        prediction_interval_calibration_by_horizon=PredictionIntervalCalibrationByHorizon(
            by_horizon=artifacts.interval_calibration_by_horizon
        ),
        event_metrics=artifacts.event_metrics,
        horizon_summaries=artifacts.horizon_summaries,
        backtest_overview=overview,
    )


def _select_backtest_origins(
    *,
    dataset: pd.DataFrame,
    history_frame: pd.DataFrame,
    min_train_rows: int,
    max_horizon_days: int,
) -> _BacktestOriginSelection:
    latest_origin_date = pd.Timestamp(history_frame['date'].iloc[-(max_horizon_days + 1)])
    eligible_origin_dates = dataset.loc[dataset['date'] <= latest_origin_date, 'date'].iloc[min_train_rows - 1 :].reset_index(drop=True)
    available_backtest_points = len(eligible_origin_dates)
    selected_origin_dates = eligible_origin_dates.iloc[
        -min(MAX_BACKTEST_POINTS, available_backtest_points) :
    ].tolist()
    return _BacktestOriginSelection(
        available_backtest_points=available_backtest_points,
        selected_origin_dates=selected_origin_dates,
        total_windows=len(selected_origin_dates),
    )


def _run_backtest(
    history_frame: pd.DataFrame,
    dataset: pd.DataFrame,
    progress_callback: MlProgressCallback = None,
    validation_horizon_days: int = 1,
    max_horizon_days: Optional[int] = None,
    history_frame_is_prepared: bool = False,
) -> BacktestRunResult:
    perf = current_perf_trace()
    history_frame = history_frame if history_frame_is_prepared else _prepare_reference_frame(history_frame)
    dataset = dataset.sort_values('date').reset_index(drop=True)
    history_dates = history_frame['date'].to_numpy(dtype='datetime64[ns]')
    validation_horizon_days = max(1, int(validation_horizon_days or 1))
    max_horizon_days = max(validation_horizon_days, int(max_horizon_days or validation_horizon_days))
    horizon_days = _lead_time_validation_horizons(max_horizon_days)

    min_train_rows = min(max(ROLLING_MIN_TRAIN_ROWS, MIN_FEATURE_ROWS), len(dataset) - MIN_BACKTEST_POINTS)
    if len(history_frame) <= max_horizon_days or min_train_rows <= 0:
        return _not_ready_backtest(
            f'Lead-time-aware rolling-origin backtesting is unavailable for the {_lead_time_label(validation_horizon_days)} '
            'lead because the history is too short after lagged features are built.'
        )

    origin_selection = _select_backtest_origins(
        dataset=dataset,
        history_frame=history_frame,
        min_train_rows=min_train_rows,
        max_horizon_days=max_horizon_days,
    )

    if perf is not None:
        perf.update(
            history_rows=len(history_frame),
            dataset_rows=len(dataset),
            min_train_rows=min_train_rows,
            available_backtest_points=origin_selection.available_backtest_points,
            validation_horizon_days=validation_horizon_days,
            max_horizon_days=max_horizon_days,
        )

    if origin_selection.available_backtest_points <= 0:
        return _not_ready_backtest(
            f'Lead-time-aware rolling-origin backtesting for the {_lead_time_label(validation_horizon_days)} lead '
            'has no comparable origins after lagged features are built.'
        )

    selected_origin_dates = origin_selection.selected_origin_dates
    total_windows = origin_selection.total_windows
    if perf is not None:
        perf.update(total_windows=total_windows)
    _emit_progress(
        progress_callback,
        'ml_backtest.running',
        (
            f'Backtesting started: {total_windows} rolling origins with lead-time-aware evaluation '
            f'for horizons 1-{max_horizon_days} days.'
        ),
    )

    horizon_rows, comparable_windows = _collect_backtest_horizon_rows(
        history_frame=history_frame,
        history_dates=history_dates,
        selected_origin_dates=selected_origin_dates,
        total_windows=total_windows,
        max_horizon_days=max_horizon_days,
        min_train_rows=min_train_rows,
        horizon_days=horizon_days,
        progress_callback=progress_callback,
    )

    valid_rows = horizon_rows.get(validation_horizon_days, [])
    if perf is not None:
        perf.update(valid_windows=len(valid_rows), comparable_windows=comparable_windows)
    if not valid_rows:
        return _not_ready_backtest(
            f'Lead-time-aware validation collected no comparable origins for the '
            f'{_lead_time_label(validation_horizon_days)} lead.'
        )

    artifacts = _build_backtest_evaluation_artifacts(
        horizon_rows=horizon_rows,
        horizon_days=horizon_days,
        valid_rows=valid_rows,
        dataset=dataset,
        validation_horizon_days=validation_horizon_days,
    )
    _emit_progress(
        progress_callback,
        'ml_backtest.completed',
        (
            f'Backtesting completed: {len(artifacts.backtest_rows)} comparable origins, '
            f'lead-time-aware summary on the {_lead_time_label(validation_horizon_days)} lead.'
        ),
    )

    if perf is not None:
        perf.update(
            completed_windows=total_windows,
            valid_windows=len(valid_rows),
            candidate_models=len(artifacts.selection.count_metrics),
            payload_rows=len(artifacts.backtest_rows),
        )

    return _build_backtest_success_result(
        artifacts=artifacts,
        valid_rows=valid_rows,
        min_train_rows=min_train_rows,
        validation_horizon_days=validation_horizon_days,
        max_horizon_days=max_horizon_days,
        horizon_days=horizon_days,
    )


def _estimate_overdispersion_ratio(counts: np.ndarray) -> float:
    values = np.asarray(counts, dtype=float)
    if values.size == 0:
        return 1.0
    mean_value = max(float(np.mean(values)), MIN_POSITIVE_PREDICTION)
    variance = float(np.var(values, ddof=1)) if values.size > 1 else float(np.var(values))
    return max(variance / mean_value, 1.0)


def _selected_count_prediction(
    row: Dict[str, Any] | BacktestWindowRow,
    selected_count_model_key: str,
) -> Optional[float]:
    if selected_count_model_key == 'seasonal_baseline':
        return row.get('baseline_count')
    if selected_count_model_key == 'heuristic_forecast':
        return row.get('heuristic_count')
    return row.get('predictions', {}).get(selected_count_model_key)


def _event_rate(actuals: np.ndarray) -> Optional[float]:
    if actuals.size == 0:
        return None
    return float(np.mean(actuals))


def _has_both_event_classes(actuals: np.ndarray) -> bool:
    return np.unique(actuals.astype(int, copy=False)).size > 1


def _event_rate_is_saturated(event_rate: Optional[float]) -> bool:
    if event_rate is None:
        return True
    return event_rate <= EVENT_RATE_SATURATION_MARGIN or event_rate >= (1.0 - EVENT_RATE_SATURATION_MARGIN)


def _event_probability_note(
    reason_code: Optional[str],
    *,
    rows_used: int,
    event_rate: Optional[float],
) -> Optional[str]:
    if reason_code == EVENT_PROBABILITY_REASON_TOO_FEW_COMPARABLE_WINDOWS:
        if rows_used > 0:
            return (
                'Вероятностный блок события пожара скрыт: в rolling-origin backtesting доступно только '
                f'{rows_used} сопоставимых окон, где можно корректно сравнить вероятности.'
            )
        return (
            'Вероятностный блок события пожара скрыт: в rolling-origin backtesting слишком мало сопоставимых окон, '
            'где можно корректно сравнить вероятности.'
        )
    if reason_code == EVENT_PROBABILITY_REASON_SINGLE_CLASS_EVALUATION:
        class_note = (
            'только дни с пожаром'
            if event_rate is not None and event_rate >= 0.5
            else 'только дни без пожара'
        )
        if rows_used > 0:
            return (
                'Вероятностный блок события пожара скрыт: '
                f'все {rows_used} evaluation-окон rolling-origin backtesting относятся к одному классу ({class_note}), '
                'поэтому вероятностная валидация некорректна.'
            )
        return (
            'Вероятностный блок события пожара скрыт: в evaluation-окнах rolling-origin backtesting наблюдался '
            f'только один класс ({class_note}), поэтому вероятностная валидация некорректна.'
        )
    if reason_code == EVENT_PROBABILITY_REASON_SATURATED_EVENT_RATE and event_rate is not None:
        return (
            'Вероятность P(>=1 пожара) скрыта: '
            f'доля события в evaluation-окнах rolling-origin backtesting составила {event_rate * 100.0:.1f}%, '
            'поэтому событие почти тривиально и неинформативно.'
        )
    return None


def _empty_event_metrics(
    *,
    rows_used: int,
    event_rate: Optional[float],
    evaluation_has_both_classes: bool,
    reason_code: Optional[str],
) -> EventMetrics:
    return EventMetrics(
        available=False,
        logistic_available=False,
        selected_model_key=None,
        selected_model_label=None,
        comparison_rows=[],
        rows_used=rows_used,
        selection_rule=EVENT_SELECTION_RULE,
        event_rate=event_rate,
        evaluation_has_both_classes=evaluation_has_both_classes,
        event_probability_informative=False,
        event_probability_note=_event_probability_note(
            reason_code,
            rows_used=rows_used,
            event_rate=event_rate,
        ),
        event_probability_reason_code=reason_code,
    )


def _normalized_event_model_label(selected_model_key: Optional[str], fallback_label: Optional[str]) -> Optional[str]:
    if selected_model_key == 'event_baseline':
        return EVENT_BASELINE_METHOD_LABEL
    if selected_model_key == 'heuristic_probability':
        return EVENT_HEURISTIC_METHOD_LABEL
    if selected_model_key == 'logistic_regression':
        return EVENT_MODEL_LABEL
    return fallback_label


def _normalize_event_comparison_rows(
    rows: List[Dict[str, Any] | EventComparisonRow],
) -> List[EventComparisonRow]:
    normalized_rows: List[EventComparisonRow] = []
    for row in rows:
        normalized_row = EventComparisonRow.coerce(row)
        method_key = str(normalized_row.method_key or '')
        if method_key == 'event_baseline':
            normalized_row = normalized_row.clone(
                method_label=EVENT_BASELINE_METHOD_LABEL,
                role_label=EVENT_BASELINE_ROLE_LABEL,
            )
        elif method_key == 'heuristic_probability':
            normalized_row = normalized_row.clone(
                method_label=EVENT_HEURISTIC_METHOD_LABEL,
                role_label=EVENT_HEURISTIC_ROLE_LABEL,
            )
        elif method_key == 'logistic_regression':
            normalized_row = normalized_row.clone(
                method_label=EVENT_MODEL_LABEL,
                role_label=EVENT_CLASSIFIER_ROLE_LABEL,
            )
        normalized_rows.append(normalized_row)
    return normalized_rows


def _event_metric_mask_context(
    *,
    baseline_probabilities: np.ndarray,
    heuristic_probabilities: np.ndarray,
    classifier_probabilities: np.ndarray,
) -> _EventMetricMaskContext:
    common_mask = np.isfinite(baseline_probabilities) & np.isfinite(heuristic_probabilities)
    common_rows = int(np.sum(common_mask))
    if common_rows < MIN_BACKTEST_POINTS:
        return _EventMetricMaskContext(
            common_rows=common_rows,
            evaluation_mask=common_mask,
            rows_used=common_rows,
            logistic_available=False,
        )

    classifier_mask = common_mask & np.isfinite(classifier_probabilities)
    logistic_available = int(np.sum(classifier_mask)) >= MIN_BACKTEST_POINTS
    evaluation_mask = classifier_mask if logistic_available else common_mask
    return _EventMetricMaskContext(
        common_rows=common_rows,
        evaluation_mask=evaluation_mask,
        rows_used=int(np.sum(evaluation_mask)),
        logistic_available=logistic_available,
    )


def _empty_event_metric_inputs(common_rows: int) -> _EventMetricInputs:
    return _EventMetricInputs(
        common_rows=common_rows,
        rows_used=common_rows,
        actuals=_empty_int_array(),
        baseline_probabilities=_empty_float_array(),
        heuristic_probabilities=_empty_float_array(),
        classifier_probabilities=_empty_float_array(),
        logistic_available=False,
    )


def _masked_event_metric_inputs(
    *,
    actual_events: np.ndarray,
    baseline_probabilities: np.ndarray,
    heuristic_probabilities: np.ndarray,
    classifier_probabilities: np.ndarray,
    mask_context: _EventMetricMaskContext,
) -> _EventMetricInputs:
    evaluation_mask = mask_context.evaluation_mask
    return _EventMetricInputs(
        common_rows=mask_context.common_rows,
        rows_used=mask_context.rows_used,
        actuals=actual_events[evaluation_mask].astype(int, copy=False),
        baseline_probabilities=baseline_probabilities[evaluation_mask],
        heuristic_probabilities=heuristic_probabilities[evaluation_mask],
        classifier_probabilities=(
            classifier_probabilities[evaluation_mask] if mask_context.logistic_available else _empty_float_array()
        ),
        logistic_available=mask_context.logistic_available,
    )


def _build_event_metric_inputs_from_arrays(
    *,
    actual_events: np.ndarray,
    baseline_probabilities: np.ndarray,
    heuristic_probabilities: np.ndarray,
    classifier_probabilities: np.ndarray,
) -> _EventMetricInputs:
    mask_context = _event_metric_mask_context(
        baseline_probabilities=baseline_probabilities,
        heuristic_probabilities=heuristic_probabilities,
        classifier_probabilities=classifier_probabilities,
    )
    if mask_context.common_rows < MIN_BACKTEST_POINTS:
        return _empty_event_metric_inputs(mask_context.common_rows)
    return _masked_event_metric_inputs(
        actual_events=actual_events,
        baseline_probabilities=baseline_probabilities,
        heuristic_probabilities=heuristic_probabilities,
        classifier_probabilities=classifier_probabilities,
        mask_context=mask_context,
    )


def _event_metric_inputs_from_horizon(evaluation_data: _HorizonEvaluationData) -> _EventMetricInputs:
    return _build_event_metric_inputs_from_arrays(
        actual_events=evaluation_data.actual_events,
        baseline_probabilities=evaluation_data.baseline_event_probabilities,
        heuristic_probabilities=evaluation_data.heuristic_event_probabilities,
        classifier_probabilities=evaluation_data.selected_event_probabilities,
    )


def _event_metric_inputs_from_rows(
    rows: List[Dict[str, Any] | BacktestEvaluationRow],
) -> _EventMetricInputs:
    actual_events: List[int] = []
    baseline_probabilities: List[Optional[float]] = []
    heuristic_probabilities: List[Optional[float]] = []
    classifier_probabilities: List[Optional[float]] = []
    for row in rows:
        actual_events.append(int(row.get('actual_event', 0)))
        baseline_probabilities.append(row.get('baseline_event_probability'))
        heuristic_probabilities.append(row.get('heuristic_event_probability'))
        classifier_probabilities.append(row.get('predicted_event_probability'))
    return _build_event_metric_inputs_from_arrays(
        actual_events=np.asarray(actual_events, dtype=int),
        baseline_probabilities=_optional_float_array(baseline_probabilities),
        heuristic_probabilities=_optional_float_array(heuristic_probabilities),
        classifier_probabilities=_optional_float_array(classifier_probabilities),
    )


def _event_metric_inputs(
    rows: List[Dict[str, Any] | BacktestEvaluationRow] | _HorizonEvaluationData,
) -> _EventMetricInputs:
    if isinstance(rows, _HorizonEvaluationData):
        return _event_metric_inputs_from_horizon(rows)
    return _event_metric_inputs_from_rows(rows)


def _score_event_probability_candidates(
    event_inputs: _EventMetricInputs,
) -> _EventProbabilityScores:
    actuals = event_inputs.actuals
    heuristic_metrics = compute_classification_metrics(
        actuals,
        event_inputs.heuristic_probabilities,
        event_inputs.baseline_probabilities,
        threshold=CLASSIFICATION_THRESHOLD,
    )
    return _EventProbabilityScores(
        heuristic_metrics=heuristic_metrics,
        baseline_roc_auc=_safe_roc_auc(actuals, event_inputs.baseline_probabilities),
        heuristic_roc_auc=_safe_roc_auc(actuals, event_inputs.heuristic_probabilities),
        baseline_log_loss=_safe_log_loss(actuals, event_inputs.baseline_probabilities),
        heuristic_log_loss=_safe_log_loss(actuals, event_inputs.heuristic_probabilities),
    )


def _initial_event_metric_selection(
    *,
    heuristic_metrics: Dict[str, Any],
    baseline_roc_auc: Optional[float],
    heuristic_roc_auc: Optional[float],
    baseline_log_loss: Optional[float],
    heuristic_log_loss: Optional[float],
) -> _EventMetricSelection:
    return _EventMetricSelection(
        selected_model_key='heuristic_probability',
        selected_model_label=EVENT_HEURISTIC_METHOD_LABEL,
        selected_metrics=heuristic_metrics,
        selected_roc_auc=heuristic_roc_auc,
        selected_log_loss=heuristic_log_loss,
        comparison_rows=[
            EventComparisonRow(
                method_key='event_baseline',
                method_label=EVENT_BASELINE_METHOD_LABEL,
                role_label=EVENT_BASELINE_ROLE_LABEL,
                brier_score=_optional_float(heuristic_metrics.get('baseline_brier_score')),
                roc_auc=baseline_roc_auc,
                f1=_optional_float(heuristic_metrics.get('baseline_f1')),
                log_loss=baseline_log_loss,
                is_selected=False,
            ),
            EventComparisonRow(
                method_key='heuristic_probability',
                method_label=EVENT_HEURISTIC_METHOD_LABEL,
                role_label=EVENT_HEURISTIC_ROLE_LABEL,
                brier_score=_optional_float(heuristic_metrics.get('brier_score')),
                roc_auc=heuristic_roc_auc,
                f1=_optional_float(heuristic_metrics.get('f1')),
                log_loss=heuristic_log_loss,
                is_selected=True,
            ),
        ],
    )


def _with_classifier_event_selection(
    selection: _EventMetricSelection,
    *,
    event_inputs: _EventMetricInputs,
    event_probability_informative: bool,
    heuristic_metrics: Dict[str, Any],
    heuristic_log_loss: Optional[float],
    heuristic_roc_auc: Optional[float],
) -> _EventMetricSelection:
    if not event_inputs.logistic_available:
        return selection

    actuals = event_inputs.actuals
    classifier_metrics = compute_classification_metrics(
        actuals,
        event_inputs.classifier_probabilities,
        event_inputs.baseline_probabilities,
        threshold=CLASSIFICATION_THRESHOLD,
    )
    classifier_roc_auc = _safe_roc_auc(actuals, event_inputs.classifier_probabilities)
    classifier_log_loss = _safe_log_loss(actuals, event_inputs.classifier_probabilities)
    classifier_selected = (
        event_probability_informative
        and bool(classifier_metrics.get('available'))
        and _event_metric_sort_key(
            classifier_metrics.get('brier_score'),
            classifier_log_loss,
            classifier_roc_auc,
            classifier_metrics.get('f1'),
        ) < _event_metric_sort_key(
            heuristic_metrics.get('brier_score'),
            heuristic_log_loss,
            heuristic_roc_auc,
            heuristic_metrics.get('f1'),
        )
    )
    comparison_rows = list(selection.comparison_rows)
    comparison_rows.append(
        EventComparisonRow(
            method_key='logistic_regression',
            method_label=EVENT_MODEL_LABEL,
            role_label=EVENT_CLASSIFIER_ROLE_LABEL,
            brier_score=_optional_float(classifier_metrics.get('brier_score')),
            roc_auc=classifier_roc_auc,
            f1=_optional_float(classifier_metrics.get('f1')),
            log_loss=classifier_log_loss,
            is_selected=classifier_selected,
        )
    )
    if not classifier_selected:
        return _EventMetricSelection(
            selected_model_key=selection.selected_model_key,
            selected_model_label=selection.selected_model_label,
            selected_metrics=selection.selected_metrics,
            selected_roc_auc=selection.selected_roc_auc,
            selected_log_loss=selection.selected_log_loss,
            comparison_rows=comparison_rows,
        )

    comparison_rows[1] = comparison_rows[1].clone(is_selected=False)
    return _EventMetricSelection(
        selected_model_key='logistic_regression',
        selected_model_label=EVENT_MODEL_LABEL,
        selected_metrics=classifier_metrics,
        selected_roc_auc=classifier_roc_auc,
        selected_log_loss=classifier_log_loss,
        comparison_rows=comparison_rows,
    )


def _event_metric_context(event_inputs: _EventMetricInputs) -> _EventMetricContext:
    event_rate = _event_rate(event_inputs.actuals)
    event_probability_informative = not _event_rate_is_saturated(event_rate)
    event_probability_reason_code = (
        EVENT_PROBABILITY_REASON_SATURATED_EVENT_RATE if not event_probability_informative else None
    )
    return _EventMetricContext(
        event_rate=event_rate,
        evaluation_has_both_classes=_has_both_event_classes(event_inputs.actuals),
        event_probability_informative=event_probability_informative,
        event_probability_note=_event_probability_note(
            event_probability_reason_code,
            rows_used=event_inputs.rows_used,
            event_rate=event_rate,
        ),
        event_probability_reason_code=event_probability_reason_code,
    )


def _build_event_metrics_result(
    *,
    event_inputs: _EventMetricInputs,
    context: _EventMetricContext,
    probability_scores: _EventProbabilityScores,
    selection: _EventMetricSelection,
) -> EventMetrics:
    heuristic_metrics = probability_scores.heuristic_metrics
    return EventMetrics(
        available=True,
        logistic_available=event_inputs.logistic_available,
        selected_model_key=selection.selected_model_key,
        selected_model_label=_normalized_event_model_label(selection.selected_model_key, selection.selected_model_label),
        brier_score=_optional_float(selection.selected_metrics.get('brier_score')),
        baseline_brier_score=_optional_float(heuristic_metrics.get('baseline_brier_score')),
        heuristic_brier_score=_optional_float(heuristic_metrics.get('brier_score')),
        roc_auc=selection.selected_roc_auc,
        baseline_roc_auc=probability_scores.baseline_roc_auc,
        heuristic_roc_auc=probability_scores.heuristic_roc_auc,
        f1=_optional_float(selection.selected_metrics.get('f1')),
        baseline_f1=_optional_float(heuristic_metrics.get('baseline_f1')),
        heuristic_f1=_optional_float(heuristic_metrics.get('f1')),
        log_loss=selection.selected_log_loss,
        baseline_log_loss=probability_scores.baseline_log_loss,
        heuristic_log_loss=probability_scores.heuristic_log_loss,
        comparison_rows=_normalize_event_comparison_rows(selection.comparison_rows),
        rows_used=event_inputs.rows_used,
        selection_rule=EVENT_SELECTION_RULE,
        event_rate=context.event_rate,
        evaluation_has_both_classes=context.evaluation_has_both_classes,
        event_probability_informative=context.event_probability_informative,
        event_probability_note=context.event_probability_note,
        event_probability_reason_code=context.event_probability_reason_code,
    )


def _compute_event_metrics(
    rows: List[Dict[str, Any] | BacktestEvaluationRow] | _HorizonEvaluationData,
) -> EventMetrics:
    event_inputs = _event_metric_inputs(rows)
    if event_inputs.common_rows < MIN_BACKTEST_POINTS:
        return _empty_event_metrics(
            rows_used=event_inputs.common_rows,
            event_rate=None,
            evaluation_has_both_classes=False,
            reason_code=EVENT_PROBABILITY_REASON_TOO_FEW_COMPARABLE_WINDOWS,
        )

    context = _event_metric_context(event_inputs)
    if not context.evaluation_has_both_classes:
        return _empty_event_metrics(
            rows_used=event_inputs.rows_used,
            event_rate=context.event_rate,
            evaluation_has_both_classes=False,
            reason_code=EVENT_PROBABILITY_REASON_SINGLE_CLASS_EVALUATION,
        )

    probability_scores = _score_event_probability_candidates(event_inputs)
    selection = _initial_event_metric_selection(
        heuristic_metrics=probability_scores.heuristic_metrics,
        baseline_roc_auc=probability_scores.baseline_roc_auc,
        heuristic_roc_auc=probability_scores.heuristic_roc_auc,
        baseline_log_loss=probability_scores.baseline_log_loss,
        heuristic_log_loss=probability_scores.heuristic_log_loss,
    )
    selection = _with_classifier_event_selection(
        selection,
        event_inputs=event_inputs,
        event_probability_informative=context.event_probability_informative,
        heuristic_metrics=probability_scores.heuristic_metrics,
        heuristic_log_loss=probability_scores.heuristic_log_loss,
        heuristic_roc_auc=probability_scores.heuristic_roc_auc,
    )

    return _build_event_metrics_result(
        event_inputs=event_inputs,
        context=context,
        probability_scores=probability_scores,
        selection=selection,
    )


def _event_metric_sort_key(
    brier_score: Optional[float],
    log_loss_value: Optional[float],
    roc_auc: Optional[float],
    f1_score: Optional[float],
) -> Tuple[float, float, float, float]:
    return (
        brier_score if brier_score is not None else float('inf'),
        log_loss_value if log_loss_value is not None else float('inf'),
        -(roc_auc if roc_auc is not None else -1.0),
        -(f1_score if f1_score is not None else -1.0),
    )


def _safe_roc_auc(actuals: np.ndarray, probabilities: np.ndarray) -> Optional[float]:
    if len(np.unique(actuals)) <= 1:
        return None
    return float(roc_auc_score(actuals, probabilities))


def _safe_log_loss(actuals: np.ndarray, probabilities: np.ndarray) -> Optional[float]:
    if actuals.size == 0:
        return None
    clipped = np.clip(probabilities.astype(float, copy=False), 0.001, 0.999)
    return float(log_loss(actuals, clipped, labels=[0, 1]))
