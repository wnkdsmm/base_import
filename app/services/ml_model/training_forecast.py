from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from app.services.forecasting.data import _build_forecast_rows as _build_scenario_forecast_rows
from app.services.forecasting.utils import _format_number, _format_percent

from .constants import (
    MIN_INTERVAL_BIN_RESIDUALS,
    MIN_INTERVAL_CALIBRATION_WINDOWS,
    MIN_INTERVAL_EVALUATION_WINDOWS,
    PREDICTION_INTERVAL_BLOCKED_CV_LABEL,
    PREDICTION_INTERVAL_CALIBRATION_FRACTION,
    PREDICTION_INTERVAL_FIXED_CHRONO_LABEL,
    PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL,
    PREDICTION_INTERVAL_LEVEL,
    PREDICTION_INTERVAL_METHOD_LABEL,
    PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL,
    PREDICTION_INTERVAL_TARGET_BINS,
)
from .training_dataset import _build_design_row, _prepare_reference_frame
from .training_models import (
    _count_prediction_upper_cap_from_support,
    _predict_count_from_design,
    _predict_event_probability_from_design,
)


def _history_records_from_frame(frame: pd.DataFrame, temperature_usable: bool = True) -> List[Dict[str, Any]]:
    return [
        {
            'date': pd.Timestamp(row.date),
            'count': float(row.count),
            'avg_temperature': None if (not temperature_usable or pd.isna(row.avg_temperature)) else float(row.avg_temperature),
        }
        for row in frame[['date', 'count', 'avg_temperature']].itertuples(index=False)
    ]


def _predict_heuristic_future_step(
    history_records: List[Dict[str, Any]],
    target_date: date,
    temp_value: float,
    temperature_usable: bool,
    baseline_expected_count: Callable[[pd.DataFrame, pd.Timestamp], float],
    reference_train_factory: Optional[Callable[[], pd.DataFrame]] = None,
) -> Tuple[float, Optional[float]]:
    forecast_rows = _build_scenario_forecast_rows(history_records, 1, temp_value if temperature_usable else None)
    if forecast_rows:
        row = forecast_rows[0]
        probability = row.get('fire_probability')
        return (
            max(0.0, float(row.get('forecast_value', 0.0))),
            _bound_probability(probability if probability is not None else 0.0),
        )

    reference_train = (
        reference_train_factory()
        if reference_train_factory is not None
        else _prepare_reference_frame(pd.DataFrame(history_records))
    )
    fallback_count = float(baseline_expected_count(reference_train, pd.Timestamp(target_date)))
    return fallback_count, _bound_probability(1.0 - math.exp(-max(0.0, fallback_count)))


def _predict_future_count(
    selected_count_model_key: str,
    history_records: List[Dict[str, Any]],
    history_counts: List[float],
    target_date: date,
    temp_value: float,
    count_model: Optional[Dict[str, Any]],
    temperature_usable: bool,
    baseline_expected_count: Callable[[pd.DataFrame, pd.Timestamp], float],
    feature_design_row: Optional[pd.DataFrame] = None,
    reference_train_factory: Optional[Callable[[], pd.DataFrame]] = None,
) -> float:
    if selected_count_model_key == 'seasonal_baseline':
        reference_train = (
            reference_train_factory()
            if reference_train_factory is not None
            else _prepare_reference_frame(pd.DataFrame(history_records))
        )
        return float(baseline_expected_count(reference_train, pd.Timestamp(target_date)))

    if selected_count_model_key == 'heuristic_forecast':
        prediction, _ = _predict_heuristic_future_step(
            history_records=history_records,
            target_date=target_date,
            temp_value=temp_value,
            temperature_usable=temperature_usable,
            baseline_expected_count=baseline_expected_count,
            reference_train_factory=reference_train_factory,
        )
        return prediction

    if count_model is None:
        return 0.0

    if feature_design_row is None:
        feature_row = _future_feature_row(history_counts, target_date, temp_value)
        feature_design_row = _build_design_row(feature_row, expected_columns=count_model['columns'])
    return float(_predict_count_from_design(count_model, feature_design_row)[0])


def _sanitize_recursive_count_prediction(prediction: float, history_counts: List[float]) -> float:
    finite_history = np.asarray([float(value) for value in history_counts if np.isfinite(value)], dtype=float)
    recent_support = float(np.max(finite_history[-28:])) if finite_history.size else 0.0
    upper_cap = float(_count_prediction_upper_cap_from_support(recent_support))
    bounded_prediction = min(max(0.0, float(prediction)), upper_cap)
    if not np.isfinite(bounded_prediction):
        return max(0.0, min(upper_cap, recent_support))
    return bounded_prediction


def _simulate_recursive_forecast_path(
    frame: pd.DataFrame,
    selected_count_model_key: str,
    count_model: Optional[Dict[str, Any]],
    event_model: Optional[Dict[str, Any]],
    forecast_days: int,
    scenario_temperature: Optional[float],
    baseline_expected_count: Callable[[pd.DataFrame, pd.Timestamp], float],
    temperature_stats: Optional[Dict[str, Any]] = None,
    baseline_event_probability: Optional[Callable[[pd.DataFrame, pd.Timestamp], Optional[float]]] = None,
) -> List[Dict[str, Any]]:
    temperature_usable = bool((temperature_stats or {}).get('usable', True))
    monthly_temp = frame.groupby(frame['date'].dt.month)['temp_value'].mean().to_dict() if temperature_usable else {}
    overall_temp = float(frame['temp_value'].mean()) if temperature_usable and not frame.empty else 0.0
    history_counts = list(frame['count'].astype(float))
    history_records = _history_records_from_frame(frame, temperature_usable=temperature_usable)
    last_date = frame['date'].dt.date.iloc[-1]

    forecast_path: List[Dict[str, Any]] = []
    for step in range(1, forecast_days + 1):
        target_date = last_date + timedelta(days=step)
        historical_temp_value = (
            float(monthly_temp.get(target_date.month, overall_temp))
            if temperature_usable and (monthly_temp or not frame.empty)
            else None
        )
        temp_value = scenario_temperature if temperature_usable and scenario_temperature is not None else historical_temp_value
        model_temp_value = float(temp_value) if temp_value is not None else 0.0
        feature_row = _future_feature_row(history_counts, target_date, model_temp_value)
        design_rows_by_columns: Dict[Tuple[str, ...], pd.DataFrame] = {}
        reference_train: Optional[pd.DataFrame] = None

        def _design_row_for(columns: Optional[List[str]]) -> pd.DataFrame:
            key = tuple(columns or [])
            design_row = design_rows_by_columns.get(key)
            if design_row is None:
                design_row = _build_design_row(feature_row, expected_columns=list(columns or []))
                design_rows_by_columns[key] = design_row
            return design_row

        def _reference_train() -> pd.DataFrame:
            nonlocal reference_train
            if reference_train is None:
                reference_train = _prepare_reference_frame(pd.DataFrame(history_records))
            return reference_train

        heuristic_probability = None
        if selected_count_model_key == 'heuristic_forecast':
            point_prediction, heuristic_probability = _predict_heuristic_future_step(
                history_records=history_records,
                target_date=target_date,
                temp_value=model_temp_value,
                temperature_usable=temperature_usable,
                baseline_expected_count=baseline_expected_count,
                reference_train_factory=_reference_train,
            )
        else:
            try:
                point_prediction = _predict_future_count(
                    selected_count_model_key=selected_count_model_key,
                    history_records=history_records,
                    history_counts=history_counts,
                    target_date=target_date,
                    temp_value=model_temp_value,
                    count_model=count_model,
                    temperature_usable=temperature_usable,
                    baseline_expected_count=baseline_expected_count,
                    feature_design_row=(
                        None
                        if count_model is None
                        else _design_row_for(list(count_model.get('columns') or []))
                    ),
                    reference_train_factory=_reference_train,
                )
            except Exception:
                point_prediction = float(baseline_expected_count(_reference_train(), pd.Timestamp(target_date)))

        point_prediction = _sanitize_recursive_count_prediction(point_prediction, history_counts)

        event_probability = None
        if event_model is not None:
            try:
                event_probability = float(
                    _predict_event_probability_from_design(
                        event_model,
                        _design_row_for(list(event_model.get('columns') or [])),
                    )[0]
                )
            except Exception:
                event_probability = None
        elif selected_count_model_key == 'heuristic_forecast':
            event_probability = heuristic_probability
        elif selected_count_model_key == 'seasonal_baseline' and baseline_event_probability is not None:
            event_probability = baseline_event_probability(_reference_train(), pd.Timestamp(target_date))

        forecast_path.append(
            {
                'step': step,
                'target_date': target_date,
                'temp_value': temp_value,
                'forecast_value': max(0.0, float(point_prediction)),
                'event_probability': _bound_probability(event_probability) if event_probability is not None else None,
            }
        )
        history_counts.append(point_prediction)
        history_records.append(
            {
                'date': pd.Timestamp(target_date),
                'count': point_prediction,
                'avg_temperature': temp_value if temperature_usable else None,
            }
        )

    return forecast_path


def _resolve_interval_calibration(interval_calibration: Dict[str, Any], horizon_days: int) -> Dict[str, Any]:
    if 'absolute_error_quantile' in interval_calibration:
        return interval_calibration

    calibration_map = interval_calibration.get('by_horizon') if isinstance(interval_calibration, dict) else None
    if calibration_map is None and isinstance(interval_calibration, dict):
        calibration_map = interval_calibration
    if not calibration_map:
        raise KeyError(f'Prediction interval calibration for horizon {horizon_days} is unavailable.')

    direct_match = calibration_map.get(horizon_days) or calibration_map.get(str(horizon_days))
    if direct_match is not None:
        return direct_match

    available_horizons: List[int] = []
    for key in calibration_map.keys():
        try:
            available_horizons.append(int(key))
        except (TypeError, ValueError):
            continue
    if not available_horizons:
        raise KeyError(f'Prediction interval calibration for horizon {horizon_days} is unavailable.')

    fallback_horizon = max((candidate for candidate in available_horizons if candidate <= horizon_days), default=max(available_horizons))
    fallback = calibration_map.get(fallback_horizon) or calibration_map.get(str(fallback_horizon))
    if fallback is None:
        raise KeyError(f'Prediction interval calibration for horizon {horizon_days} is unavailable.')
    return fallback


def _forecast_interval_coverage_metadata(calibration: Dict[str, Any]) -> Dict[str, Any]:
    validated_coverage = calibration.get('validated_coverage')
    return {
        'prediction_interval_coverage_validated': bool(calibration.get('coverage_validated', False)),
        'prediction_interval_coverage': validated_coverage,
        'prediction_interval_coverage_display': _format_ratio_percent(validated_coverage),
    }


def _build_future_forecast_rows(
    frame: pd.DataFrame,
    selected_count_model_key: str,
    count_model: Optional[Dict[str, Any]],
    event_model: Optional[Dict[str, Any]],
    forecast_days: int,
    scenario_temperature: Optional[float],
    interval_calibration: Dict[str, Any],
    baseline_expected_count: Callable[[pd.DataFrame, pd.Timestamp], float],
    temperature_stats: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    history_counts = list(frame['count'].astype(float))
    sorted_history_counts = np.sort(np.asarray(history_counts, dtype=float)) if history_counts else np.asarray([], dtype=float)
    forecast_path = _simulate_recursive_forecast_path(
        frame=frame,
        selected_count_model_key=selected_count_model_key,
        count_model=count_model,
        event_model=event_model,
        forecast_days=forecast_days,
        scenario_temperature=scenario_temperature,
        baseline_expected_count=baseline_expected_count,
        temperature_stats=temperature_stats,
    )
    reference_calibration = _resolve_interval_calibration(interval_calibration, 1)

    interval_label = str(
        reference_calibration.get('level_display')
        or f'{int(round(float(reference_calibration.get("level", PREDICTION_INTERVAL_LEVEL)) * 100.0))}%'
    )
    forecast_rows: List[Dict[str, Any]] = []
    for point in forecast_path:
        step = int(point['step'])
        target_date = point['target_date']
        temp_value = point['temp_value']
        point_prediction = float(point['forecast_value'])
        event_probability = point['event_probability']
        row_interval_calibration = _resolve_interval_calibration(interval_calibration, step)

        lower_bound, upper_bound = _count_interval(point_prediction, row_interval_calibration)
        risk_index = _risk_index(point_prediction, sorted_history_counts)
        risk_level_label, risk_level_tone = _risk_band_from_index(risk_index)

        forecast_rows.append(
            {
                'horizon_days': step,
                'date': target_date.isoformat(),
                'date_display': target_date.strftime('%d.%m.%Y'),
                'forecast_value': round(point_prediction, 3),
                'forecast_value_display': _format_number(point_prediction),
                'lower_bound': round(lower_bound, 3),
                'lower_bound_display': _format_number(lower_bound),
                'upper_bound': round(upper_bound, 3),
                'upper_bound_display': _format_number(upper_bound),
                'range_label': f'{interval_label} interval',
                'range_display': f"{interval_label}: {_format_number(lower_bound)} - {_format_number(upper_bound)} пожара",
                'temperature_display': f"{_format_number(temp_value)} В°C",
                'risk_index': round(risk_index, 1),
                'risk_index_display': f"{int(round(risk_index))} / 100",
                'risk_level_label': risk_level_label,
                'risk_level_tone': risk_level_tone,
                **_forecast_interval_coverage_metadata(row_interval_calibration),
                'event_probability': round(event_probability, 4) if event_probability is not None else None,
                'event_probability_display': _format_probability(event_probability) if event_probability is not None else '—',
            }
        )
    return forecast_rows


def _future_feature_row(history_counts: List[float], target_date: date, temp_value: float) -> Dict[str, float]:
    def lag_value(offset: int) -> float:
        if len(history_counts) >= offset:
            return float(history_counts[-offset])
        return float(np.mean(history_counts)) if history_counts else 0.0

    rolling_7 = float(np.mean(history_counts[-7:])) if history_counts else 0.0
    rolling_28 = float(np.mean(history_counts[-28:])) if history_counts else rolling_7
    return {
        'temp_value': float(temp_value),
        'weekday': float(target_date.weekday()),
        'month': float(target_date.month),
        'lag_1': lag_value(1),
        'lag_7': lag_value(7),
        'lag_14': lag_value(14),
        'rolling_7': rolling_7,
        'rolling_28': rolling_28,
        'trend_gap': rolling_7 - rolling_28,
    }


def _format_ratio_percent(value: Optional[float]) -> str:
    if value is None:
        return '—'
    return f"{_format_number(float(value) * 100.0)}%"


def _prediction_interval_level_display(level: float) -> str:
    return f'{int(round(float(level) * 100.0))}%'


def _prediction_interval_absolute_error_quantile(residuals: np.ndarray, level: float) -> float:
    residual_values = np.sort(np.asarray(residuals, dtype=float))
    residual_count = int(residual_values.size)
    if residual_count == 0:
        return 0.0
    rank = max(1, int(math.ceil((residual_count + 1) * float(level))))
    return float(residual_values[min(residual_count - 1, rank - 1)])


def _build_prediction_interval_bins(
    predictions: np.ndarray,
    residuals: np.ndarray,
    level: float,
    global_quantile: float,
) -> Dict[str, Any]:
    prediction_values = np.asarray(predictions, dtype=float)
    residual_values = np.asarray(residuals, dtype=float)
    residual_count = int(residual_values.size)
    target_bin_count = min(PREDICTION_INTERVAL_TARGET_BINS, max(1, residual_count // MIN_INTERVAL_BIN_RESIDUALS))
    if target_bin_count < 2:
        return {
            'strategy': 'global_absolute_error_quantile',
            'edges': [],
            'bins': [],
            'bin_count': 0,
        }

    raw_edges = np.quantile(
        prediction_values,
        [index / target_bin_count for index in range(1, target_bin_count)],
    )
    edge_values: List[float] = []
    for raw_edge in np.atleast_1d(raw_edges).tolist():
        edge_value = float(raw_edge)
        if edge_values and math.isclose(edge_values[-1], edge_value, rel_tol=1e-9, abs_tol=1e-9):
            continue
        edge_values.append(edge_value)

    if not edge_values:
        return {
            'strategy': 'global_absolute_error_quantile',
            'edges': [],
            'bins': [],
            'bin_count': 0,
        }

    bin_assignments = np.searchsorted(np.asarray(edge_values, dtype=float), prediction_values, side='right')
    bins: List[Dict[str, Any]] = []
    for bin_index in range(len(edge_values) + 1):
        mask = bin_assignments == bin_index
        bin_residuals = residual_values[mask]
        bin_predictions = prediction_values[mask]
        if bin_residuals.size >= MIN_INTERVAL_BIN_RESIDUALS:
            bin_quantile = _prediction_interval_absolute_error_quantile(bin_residuals, level)
            fallback_used = False
        else:
            bin_quantile = float(global_quantile)
            fallback_used = True

        lower_edge = None if bin_index == 0 else float(edge_values[bin_index - 1])
        upper_edge = None if bin_index == len(edge_values) else float(edge_values[bin_index])
        bins.append(
            {
                'bin_index': bin_index,
                'prediction_min': float(np.min(bin_predictions)) if bin_predictions.size else lower_edge,
                'prediction_max': float(np.max(bin_predictions)) if bin_predictions.size else upper_edge,
                'lower_edge': lower_edge,
                'upper_edge': upper_edge,
                'residual_count': int(bin_residuals.size),
                'absolute_error_quantile': float(bin_quantile),
                'fallback_to_global': fallback_used,
            }
        )

    return {
        'strategy': 'predicted_count_quantiles',
        'edges': edge_values,
        'bins': bins,
        'bin_count': len(bins),
    }


def _build_prediction_interval_calibration(
    actuals: np.ndarray,
    predictions: np.ndarray,
    level: float = PREDICTION_INTERVAL_LEVEL,
    method_label: str = PREDICTION_INTERVAL_METHOD_LABEL,
) -> Dict[str, Any]:
    actual_values = np.asarray(actuals, dtype=float)
    prediction_values = np.asarray(predictions, dtype=float)
    residuals = np.abs(actual_values - prediction_values)
    residual_count = int(residuals.size)
    quantile = _prediction_interval_absolute_error_quantile(residuals, level)
    adaptive_binning = _build_prediction_interval_bins(prediction_values, residuals, level, quantile)

    return {
        'level': float(level),
        'level_display': _prediction_interval_level_display(level),
        'absolute_error_quantile': quantile,
        'residual_count': residual_count,
        'adaptive_binning_strategy': adaptive_binning['strategy'],
        'adaptive_bin_count': adaptive_binning['bin_count'],
        'adaptive_bin_edges': adaptive_binning['edges'],
        'adaptive_bins': adaptive_binning['bins'],
        'method_label': method_label,
    }


def _split_prediction_interval_windows(total_windows: int) -> Optional[Tuple[int, int]]:
    minimum_windows = MIN_INTERVAL_CALIBRATION_WINDOWS + MIN_INTERVAL_EVALUATION_WINDOWS
    if total_windows < minimum_windows:
        return None

    calibration_windows = int(math.floor(total_windows * PREDICTION_INTERVAL_CALIBRATION_FRACTION))
    calibration_windows = max(MIN_INTERVAL_CALIBRATION_WINDOWS, calibration_windows)
    calibration_windows = min(calibration_windows, total_windows - MIN_INTERVAL_EVALUATION_WINDOWS)
    evaluation_windows = total_windows - calibration_windows
    if calibration_windows < MIN_INTERVAL_CALIBRATION_WINDOWS or evaluation_windows < MIN_INTERVAL_EVALUATION_WINDOWS:
        return None
    return calibration_windows, evaluation_windows


def _prediction_interval_validation_blocks(calibration_windows: int, total_windows: int) -> List[np.ndarray]:
    evaluation_indices = np.arange(calibration_windows, total_windows, dtype=int)
    evaluation_count = int(evaluation_indices.size)
    if evaluation_count <= 0:
        return []
    if evaluation_count >= 12:
        block_count = 4
    elif evaluation_count >= 6:
        block_count = 3
    else:
        block_count = 2
    return [block for block in np.array_split(evaluation_indices, block_count) if block.size]


def _prediction_interval_window_date_label(window_date: Any) -> str:
    if isinstance(window_date, pd.Timestamp):
        return window_date.date().isoformat()
    if isinstance(window_date, date):
        return window_date.isoformat()
    return str(window_date)


def _prediction_interval_range_labels(
    window_dates: List[Any],
    calibration_windows: int,
    evaluation_prefix: str = 'later',
) -> Tuple[str, str]:
    calibration_end = (
        _prediction_interval_window_date_label(window_dates[calibration_windows - 1])
        if window_dates and calibration_windows > 0
        else None
    )
    evaluation_count = max(0, len(window_dates) - calibration_windows)
    evaluation_start = (
        _prediction_interval_window_date_label(window_dates[calibration_windows])
        if evaluation_count > 0 and len(window_dates) > calibration_windows
        else None
    )
    calibration_label = f'first {calibration_windows} windows'
    if calibration_end:
        calibration_label = f'{calibration_label} through {calibration_end}'
    evaluation_label = f'{evaluation_prefix} {evaluation_count} windows'
    if evaluation_start:
        evaluation_label = f'{evaluation_label} from {evaluation_start}'
    return calibration_label, evaluation_label


def _prediction_interval_coverage_flags(
    actuals: np.ndarray,
    predictions: np.ndarray,
    calibration: Dict[str, Any],
) -> List[bool]:
    actual_values = np.asarray(actuals, dtype=float)
    prediction_values = np.asarray(predictions, dtype=float)
    covered: List[bool] = []
    for actual_value, prediction_value in zip(actual_values, prediction_values):
        lower_bound, upper_bound = _count_interval(float(prediction_value), calibration)
        covered.append(lower_bound <= float(actual_value) <= upper_bound)
    return covered


def _prediction_interval_stability_summary(
    covered_flags: List[bool],
    level: float,
) -> Dict[str, Any]:
    flag_values = np.asarray(covered_flags, dtype=float)
    if flag_values.size == 0:
        return {
            'coverage': None,
            'coverage_gap': float('inf'),
            'segment_coverages': [],
            'segment_coverage_std': float('inf'),
            'stability_score': float('inf'),
        }

    if flag_values.size >= 12:
        segment_count = 4
    elif flag_values.size >= 6:
        segment_count = 3
    else:
        segment_count = 2
    segments = [segment for segment in np.array_split(flag_values, segment_count) if segment.size]
    segment_coverages = [float(np.mean(segment)) for segment in segments]
    coverage = float(np.mean(flag_values))
    coverage_gap = abs(coverage - float(level))
    segment_coverage_std = float(np.std(segment_coverages)) if len(segment_coverages) > 1 else 0.0
    return {
        'coverage': coverage,
        'coverage_gap': coverage_gap,
        'segment_coverages': segment_coverages,
        'segment_coverage_std': segment_coverage_std,
        'stability_score': coverage_gap + segment_coverage_std,
    }


def _build_prediction_interval_candidate(
    scheme_key: str,
    scheme_label: str,
    level: float,
    calibration_windows: int,
    evaluation_windows: int,
    calibration_range_label: str,
    evaluation_range_label: str,
    covered_flags: List[bool],
    calibration_refresh_count: int,
    validation_block_count: int,
) -> Dict[str, Any]:
    summary = _prediction_interval_stability_summary(covered_flags, level)
    return {
        'scheme_key': scheme_key,
        'scheme_label': scheme_label,
        'coverage': summary['coverage'],
        'coverage_display': _format_ratio_percent(summary['coverage']),
        'coverage_gap': summary['coverage_gap'],
        'segment_coverages': summary['segment_coverages'],
        'segment_coverage_std': summary['segment_coverage_std'],
        'stability_score': summary['stability_score'],
        'calibration_window_count': calibration_windows,
        'evaluation_window_count': evaluation_windows,
        'calibration_window_range_label': calibration_range_label,
        'evaluation_window_range_label': evaluation_range_label,
        'calibration_refresh_count': calibration_refresh_count,
        'validation_block_count': validation_block_count,
        'covered_flags': list(covered_flags),
    }


def _evaluate_fixed_chrono_prediction_interval(
    actuals: np.ndarray,
    predictions: np.ndarray,
    window_dates: List[Any],
    calibration_windows: int,
    level: float,
) -> Dict[str, Any]:
    calibration_range_label, evaluation_range_label = _prediction_interval_range_labels(
        window_dates,
        calibration_windows,
    )
    calibration = _build_prediction_interval_calibration(
        actuals[:calibration_windows],
        predictions[:calibration_windows],
        level=level,
        method_label=f'{PREDICTION_INTERVAL_METHOD_LABEL}; validation baseline: {PREDICTION_INTERVAL_FIXED_CHRONO_LABEL}',
    )
    covered_flags = _prediction_interval_coverage_flags(
        actuals[calibration_windows:],
        predictions[calibration_windows:],
        calibration,
    )
    candidate = _build_prediction_interval_candidate(
        scheme_key='fixed_chrono_split',
        scheme_label=PREDICTION_INTERVAL_FIXED_CHRONO_LABEL,
        level=level,
        calibration_windows=calibration_windows,
        evaluation_windows=max(0, len(window_dates) - calibration_windows),
        calibration_range_label=calibration_range_label,
        evaluation_range_label=evaluation_range_label,
        covered_flags=covered_flags,
        calibration_refresh_count=1,
        validation_block_count=1 if covered_flags else 0,
    )
    candidate['calibration'] = calibration
    return candidate


def _evaluate_blocked_prediction_interval(
    actuals: np.ndarray,
    predictions: np.ndarray,
    window_dates: List[Any],
    calibration_windows: int,
    level: float,
) -> Dict[str, Any]:
    calibration_range_label, evaluation_range_label = _prediction_interval_range_labels(
        window_dates,
        calibration_windows,
        evaluation_prefix='blocked evaluation',
    )
    covered_flags: List[bool] = []
    blocks = _prediction_interval_validation_blocks(calibration_windows, len(window_dates))
    for block in blocks:
        block_start = int(block[0])
        calibration = _build_prediction_interval_calibration(
            actuals[:block_start],
            predictions[:block_start],
            level=level,
            method_label=f'{PREDICTION_INTERVAL_METHOD_LABEL}; validation candidate: {PREDICTION_INTERVAL_BLOCKED_CV_LABEL}',
        )
        covered_flags.extend(
            _prediction_interval_coverage_flags(
                actuals[block],
                predictions[block],
                calibration,
            )
        )
    return _build_prediction_interval_candidate(
        scheme_key='blocked_forward_cv',
        scheme_label=PREDICTION_INTERVAL_BLOCKED_CV_LABEL,
        level=level,
        calibration_windows=calibration_windows,
        evaluation_windows=max(0, len(window_dates) - calibration_windows),
        calibration_range_label=calibration_range_label,
        evaluation_range_label=evaluation_range_label,
        covered_flags=covered_flags,
        calibration_refresh_count=len(blocks),
        validation_block_count=len(blocks),
    )


def _evaluate_rolling_prediction_interval(
    actuals: np.ndarray,
    predictions: np.ndarray,
    window_dates: List[Any],
    calibration_windows: int,
    level: float,
) -> Dict[str, Any]:
    calibration_range_label, evaluation_range_label = _prediction_interval_range_labels(
        window_dates,
        calibration_windows,
        evaluation_prefix='rolling evaluation',
    )
    covered_flags: List[bool] = []
    for index in range(calibration_windows, len(window_dates)):
        calibration = _build_prediction_interval_calibration(
            actuals[:index],
            predictions[:index],
            level=level,
            method_label=f'{PREDICTION_INTERVAL_METHOD_LABEL}; validation candidate: {PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL}',
        )
        covered_flags.extend(
            _prediction_interval_coverage_flags(
                actuals[index:index + 1],
                predictions[index:index + 1],
                calibration,
            )
        )
    evaluation_windows = max(0, len(window_dates) - calibration_windows)
    return _build_prediction_interval_candidate(
        scheme_key='rolling_split_conformal',
        scheme_label=PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL,
        level=level,
        calibration_windows=calibration_windows,
        evaluation_windows=evaluation_windows,
        calibration_range_label=calibration_range_label,
        evaluation_range_label=evaluation_range_label,
        covered_flags=covered_flags,
        calibration_refresh_count=evaluation_windows,
        validation_block_count=evaluation_windows,
    )


def _prediction_interval_candidate_sort_key(candidate: Dict[str, Any]) -> Tuple[float, float, float, int]:
    preference_rank = 0 if candidate.get('scheme_key') == 'rolling_split_conformal' else 1
    return (
        float(candidate.get('stability_score', float('inf'))),
        float(candidate.get('segment_coverage_std', float('inf'))),
        float(candidate.get('coverage_gap', float('inf'))),
        preference_rank,
    )


def _prediction_interval_horizon_prefix(horizon_days: Optional[int]) -> str:
    if horizon_days is None:
        return ''
    day_label = '1-day lead' if int(horizon_days) == 1 else f'{int(horizon_days)}-day lead'
    return f'For the {day_label}, '


def _build_prediction_interval_validation_explanation(
    selected_candidate: Dict[str, Any],
    runner_up_candidate: Optional[Dict[str, Any]],
    reference_candidate: Optional[Dict[str, Any]],
    horizon_days: Optional[int] = None,
) -> str:
    selected_label = selected_candidate.get('scheme_label') or PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL
    runner_up_label = runner_up_candidate.get('scheme_label') if runner_up_candidate else None

    if runner_up_candidate is not None:
        score_gap = float(runner_up_candidate.get('stability_score', float('inf'))) - float(
            selected_candidate.get('stability_score', float('inf'))
        )
        if score_gap > 1e-9:
            comparison_text = f'it was more stable on later windows than {runner_up_label}'
        else:
            comparison_text = f'it stayed at least as stable as {runner_up_label} while refreshing calibration more often'
    else:
        comparison_text = 'it gave the most stable forward-only out-of-sample coverage among the available validation schemes'

    reference_text = ''
    if reference_candidate is not None:
        if float(selected_candidate.get('stability_score', float('inf'))) + 1e-9 < float(
            reference_candidate.get('stability_score', float('inf'))
        ):
            reference_text = ' and improved coverage stability versus the previous fixed 60/40 chrono split'
        else:
            reference_text = ' while remaining at least as stable as the previous fixed 60/40 chrono split'

    return (
        f'{_prediction_interval_horizon_prefix(horizon_days)}'
        f'{selected_label} was selected for validated out-of-sample coverage because {comparison_text}{reference_text}. '
        f'{PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL} was not adopted because an honest time-series variant would '
        'require leave-one-block-out refits for every checkpoint.'
    )


def _evaluate_prediction_interval_backtest(
    actuals: np.ndarray,
    predictions: np.ndarray,
    window_dates: List[Any],
    level: float = PREDICTION_INTERVAL_LEVEL,
    horizon_days: Optional[int] = None,
) -> Dict[str, Any]:
    actual_values = np.asarray(actuals, dtype=float)
    prediction_values = np.asarray(predictions, dtype=float)
    normalized_dates = list(window_dates)
    total_windows = min(actual_values.size, prediction_values.size, len(normalized_dates))
    actual_values = actual_values[:total_windows]
    prediction_values = prediction_values[:total_windows]
    normalized_dates = normalized_dates[:total_windows]

    split = _split_prediction_interval_windows(total_windows)
    if split is None:
        calibration = _build_prediction_interval_calibration(
            actual_values,
            prediction_values,
            level=level,
            method_label=f'{PREDICTION_INTERVAL_METHOD_LABEL} (validated out-of-sample coverage unavailable)',
        )
        note = (
            f'{_prediction_interval_horizon_prefix(horizon_days)}'
            'Validated out-of-sample coverage is unavailable because the backtest has too few rolling-origin windows '
            'for forward-only interval validation.'
        )
        calibration.update(
            coverage_validated=False,
            validated_coverage=None,
            coverage_note=note,
            calibration_window_count=total_windows,
            evaluation_window_count=0,
            calibration_window_range_label='all available backtest windows',
            evaluation_window_range_label='not available',
            validation_scheme_key='not_validated',
            validation_scheme_label='validated out-of-sample coverage unavailable',
            validation_scheme_explanation=note,
        )
        return {
            'calibration': calibration,
            'coverage': None,
            'coverage_validated': False,
            'coverage_note': note,
            'calibration_window_count': total_windows,
            'evaluation_window_count': 0,
            'calibration_window_range_label': 'all available backtest windows',
            'evaluation_window_range_label': 'not available',
            'validation_scheme_key': 'not_validated',
            'validation_scheme_label': 'validated out-of-sample coverage unavailable',
            'validation_scheme_explanation': note,
            'reference_candidate': None,
            'runner_up_candidate': None,
        }

    calibration_windows, _ = split
    fixed_candidate = _evaluate_fixed_chrono_prediction_interval(
        actual_values,
        prediction_values,
        normalized_dates,
        calibration_windows,
        level,
    )
    blocked_candidate = _evaluate_blocked_prediction_interval(
        actual_values,
        prediction_values,
        normalized_dates,
        calibration_windows,
        level,
    )
    rolling_candidate = _evaluate_rolling_prediction_interval(
        actual_values,
        prediction_values,
        normalized_dates,
        calibration_windows,
        level,
    )
    selectable_candidates = [blocked_candidate, rolling_candidate]
    ranking = sorted(selectable_candidates, key=_prediction_interval_candidate_sort_key)
    selected_candidate = ranking[0]
    runner_up_candidate = ranking[1] if len(ranking) > 1 else None
    validation_explanation = _build_prediction_interval_validation_explanation(
        selected_candidate,
        runner_up_candidate,
        fixed_candidate,
        horizon_days=horizon_days,
    )

    calibration = _build_prediction_interval_calibration(
        actual_values,
        prediction_values,
        level=level,
        method_label=(
            f'{PREDICTION_INTERVAL_METHOD_LABEL}; validated by {selected_candidate["scheme_label"]}'
        ),
    )
    note = (
        f'{validation_explanation} Coverage is evaluated only on '
        f'{selected_candidate["evaluation_window_range_label"]} after an initial calibration prefix of '
        f'{selected_candidate["calibration_window_range_label"]}. Deployment intervals are recalibrated on all '
        'available rolling-origin residuals after validation.'
    )
    calibration.update(
        coverage_validated=True,
        validated_coverage=selected_candidate['coverage'],
        coverage_note=note,
        calibration_window_count=selected_candidate['calibration_window_count'],
        evaluation_window_count=selected_candidate['evaluation_window_count'],
        calibration_window_range_label=selected_candidate['calibration_window_range_label'],
        evaluation_window_range_label=selected_candidate['evaluation_window_range_label'],
        validation_scheme_key=selected_candidate['scheme_key'],
        validation_scheme_label=selected_candidate['scheme_label'],
        validation_scheme_explanation=validation_explanation,
        reference_scheme_key=fixed_candidate['scheme_key'],
        reference_scheme_label=fixed_candidate['scheme_label'],
    )
    return {
        'calibration': calibration,
        'coverage': selected_candidate['coverage'],
        'coverage_validated': True,
        'coverage_note': note,
        'calibration_window_count': selected_candidate['calibration_window_count'],
        'evaluation_window_count': selected_candidate['evaluation_window_count'],
        'calibration_window_range_label': selected_candidate['calibration_window_range_label'],
        'evaluation_window_range_label': selected_candidate['evaluation_window_range_label'],
        'validation_scheme_key': selected_candidate['scheme_key'],
        'validation_scheme_label': selected_candidate['scheme_label'],
        'validation_scheme_explanation': validation_explanation,
        'reference_candidate': fixed_candidate,
        'runner_up_candidate': runner_up_candidate,
    }


def _prediction_interval_margin(prediction: float, calibration: Dict[str, Any]) -> float:
    center = max(0.0, float(prediction))
    minimum_floor_raw = calibration.get('minimum_absolute_error_quantile')
    minimum_floor = None if minimum_floor_raw is None else max(0.0, float(minimum_floor_raw or 0.0))
    adaptive_bins = calibration.get('adaptive_bins') or []
    edge_values = calibration.get('adaptive_bin_edges') or []
    if adaptive_bins:
        bin_index = int(np.searchsorted(np.asarray(edge_values, dtype=float), center, side='right'))
        bin_index = min(max(bin_index, 0), len(adaptive_bins) - 1)
        bin_quantile = adaptive_bins[bin_index].get('absolute_error_quantile')
        if bin_quantile is not None:
            margin = max(0.0, float(bin_quantile))
            return max(minimum_floor, margin) if minimum_floor is not None else margin

    fallback_margin = max(0.0, float(calibration.get('absolute_error_quantile', 0.0)))
    return max(minimum_floor, fallback_margin) if minimum_floor is not None else fallback_margin


def _count_interval(prediction: float, calibration: Dict[str, Any]) -> Tuple[float, float]:
    margin = _prediction_interval_margin(prediction, calibration)
    center = max(0.0, float(prediction))
    lower = max(0.0, center - margin)
    upper = max(lower, center + margin)
    return lower, upper


def _interval_coverage(
    actuals: np.ndarray,
    predictions: np.ndarray,
    calibration: Dict[str, Any],
) -> Optional[float]:
    actual_values = np.asarray(actuals, dtype=float)
    prediction_values = np.asarray(predictions, dtype=float)
    if actual_values.size == 0 or prediction_values.size == 0 or actual_values.size != prediction_values.size:
        return None

    covered = []
    for actual_value, prediction_value in zip(actual_values, prediction_values):
        lower_bound, upper_bound = _count_interval(float(prediction_value), calibration)
        covered.append(lower_bound <= float(actual_value) <= upper_bound)
    return float(np.mean(covered)) if covered else None


def _risk_index(prediction: float, sorted_history_counts: np.ndarray) -> float:
    if sorted_history_counts.size == 0:
        return 0.0
    rank = float(np.searchsorted(sorted_history_counts, prediction, side='right'))
    return min(100.0, max(0.0, rank / float(sorted_history_counts.size) * 100.0))


def _risk_band_from_index(risk_index: float) -> Tuple[str, str]:
    if risk_index >= 90.0:
        return 'Очень высокий', 'critical'
    if risk_index >= 75.0:
        return 'Высокий', 'high'
    if risk_index >= 50.0:
        return 'Средний', 'medium'
    if risk_index >= 25.0:
        return 'Ниже среднего', 'low'
    return 'Низкий', 'minimal'


def _bound_probability(value: float) -> float:
    return min(1.0, max(0.0, float(value)))


def _format_probability(value: Optional[float]) -> str:
    if value is None:
        return '—'
    return _format_percent(float(value) * 100.0)
