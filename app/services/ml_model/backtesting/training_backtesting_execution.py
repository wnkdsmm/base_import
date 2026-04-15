from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from app.perf import current_perf_trace
from app.services.model_quality import compute_count_metrics

from ..ml_model_config_types import CLASSIFICATION_THRESHOLD, COUNT_MODEL_KEYS, COUNT_SELECTION_RULE, EVENT_SELECTION_RULE, MAX_BACKTEST_POINTS, MIN_BACKTEST_POINTS, MIN_FEATURE_ROWS, MlProgressCallback, ROLLING_MIN_TRAIN_ROWS, _emit_progress
from ..ml_model_result_types import BacktestFailure, BacktestOverview, BacktestRunResult, BacktestSuccess, BacktestWindowRow, CountMetrics, EventMetrics, HorizonSummary, PredictionIntervalCalibrationByHorizon
from .training_backtesting_baselines import _baseline_event_probability, _baseline_expected_count
from .training_backtesting_events import _compute_event_metrics
from .training_backtesting_results import (
    _build_backtest_evaluation_artifacts,
    _build_backtest_success_result,
    _collect_backtest_horizon_rows,
)
from .training_backtesting_horizons import (
    _build_backtest_payload_rows,
    _build_horizon_evaluation_data,
    _build_horizon_evaluation_data_by_horizon,
    _evaluate_backtest_horizon_metadata,
    _with_selected_count_model,
)
from .training_backtesting_support import (
    _estimate_overdispersion_ratio,
    _lead_time_label,
    _lead_time_validation_horizons,
)
from .training_backtesting_types import (
    _BacktestEvaluationArtifacts,
    _BacktestOriginSelection,
    _BacktestRunContext,
    _BacktestSelection,
    _BacktestWindow,
    _HorizonEvaluationData,
    _ScoredCandidates,
    _WindowCandidateFits,
)
from ..training.training_dataset import _build_design_matrix, _feature_frame, _prepare_reference_frame
from ..training.forecast_intervals import _build_recursive_forecast_seed, _simulate_recursive_forecast_path
from ..training.training_selection import (
    _available_count_model_labels,
    _build_count_comparison_rows,
    _build_count_selection_details,
    _select_count_method,
)
from ..training.training_temperature import (
    _apply_temperature_statistics,
    _fit_temperature_statistics,
    _temperature_feature_columns,
)

def _not_ready_backtest(message: str) -> BacktestFailure:
    return BacktestFailure(message=message)

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
    count_model_bundles: Dict[str, Optional[dict[str, Any]]],
    event_bundle: Optional[dict[str, Any]],
    forecast_days: int,
) -> Dict[str, Optional[List[dict[str, Any]]]]:
    forecast_paths: Dict[str, Optional[List[dict[str, Any]]]] = {}
    candidate_specs: List[Tuple[str, Optional[dict[str, Any]], Optional[dict[str, Any]]]] = [
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
    from . import training_backtesting as _backtesting

    count_model_bundles = {
        model_key: _backtesting._fit_count_model_from_design(model_key, window.model_train_design, window.count_targets)
        for model_key in COUNT_MODEL_KEYS
    }
    event_bundle = _backtesting._fit_event_model_from_design(window.model_train_design, window.event_targets)
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
) -> Tuple[str, CountMetrics, dict[str, Any]]:
    return _select_count_method(
        scored_candidates.baseline_metrics,
        scored_candidates.heuristic_metrics,
        scored_candidates.count_metrics,
    )

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

def _prepare_backtest_run_context(
    history_frame: pd.DataFrame,
    dataset: pd.DataFrame,
    *,
    validation_horizon_days: int,
    max_horizon_days: Optional[int],
    history_frame_is_prepared: bool,
) -> _BacktestRunContext:
    prepared_history_frame = (
        history_frame if history_frame_is_prepared else _prepare_reference_frame(history_frame)
    )
    prepared_dataset = dataset.sort_values('date').reset_index(drop=True)
    normalized_validation_horizon = max(1, int(validation_horizon_days or 1))
    normalized_max_horizon = max(
        normalized_validation_horizon,
        int(max_horizon_days or normalized_validation_horizon),
    )
    return _BacktestRunContext(
        history_frame=prepared_history_frame,
        dataset=prepared_dataset,
        history_dates=prepared_history_frame['date'].to_numpy(dtype='datetime64[ns]'),
        validation_horizon_days=normalized_validation_horizon,
        max_horizon_days=normalized_max_horizon,
        horizon_days=_lead_time_validation_horizons(normalized_max_horizon),
        min_train_rows=min(
            max(ROLLING_MIN_TRAIN_ROWS, MIN_FEATURE_ROWS),
            len(prepared_dataset) - MIN_BACKTEST_POINTS,
        ),
    )

def _validate_backtest_run_context(context: _BacktestRunContext) -> Optional[BacktestRunResult]:
    if len(context.history_frame) > context.max_horizon_days and context.min_train_rows > 0:
        return None
    return _not_ready_backtest(
        f'Lead-time-aware rolling-origin backtesting is unavailable for the {_lead_time_label(context.validation_horizon_days)} '
        'lead because the history is too short after lagged features are built.'
    )

def _record_backtest_context_perf(perf: Any, context: _BacktestRunContext, origin_selection: _BacktestOriginSelection) -> None:
    if perf is None:
        return
    perf.update(
        history_rows=len(context.history_frame),
        dataset_rows=len(context.dataset),
        min_train_rows=context.min_train_rows,
        available_backtest_points=origin_selection.available_backtest_points,
        validation_horizon_days=context.validation_horizon_days,
        max_horizon_days=context.max_horizon_days,
    )

def _emit_backtest_start_progress(
    progress_callback: MlProgressCallback,
    *,
    total_windows: int,
    max_horizon_days: int,
) -> None:
    _emit_progress(
        progress_callback,
        'ml_backtest.running',
        (
            f'Backtesting started: {total_windows} rolling origins with lead-time-aware evaluation '
            f'for horizons 1-{max_horizon_days} days.'
        ),
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
    context = _prepare_backtest_run_context(
        history_frame,
        dataset,
        validation_horizon_days=validation_horizon_days,
        max_horizon_days=max_horizon_days,
        history_frame_is_prepared=history_frame_is_prepared,
    )
    invalid_context = _validate_backtest_run_context(context)
    if invalid_context is not None:
        return invalid_context

    origin_selection = _select_backtest_origins(
        dataset=context.dataset,
        history_frame=context.history_frame,
        min_train_rows=context.min_train_rows,
        max_horizon_days=context.max_horizon_days,
    )
    _record_backtest_context_perf(perf, context, origin_selection)

    if origin_selection.available_backtest_points <= 0:
        return _not_ready_backtest(
            f'Lead-time-aware rolling-origin backtesting for the {_lead_time_label(context.validation_horizon_days)} lead '
            'has no comparable origins after lagged features are built.'
        )

    selected_origin_dates = origin_selection.selected_origin_dates
    total_windows = origin_selection.total_windows
    if perf is not None:
        perf.update(total_windows=total_windows)
    _emit_backtest_start_progress(
        progress_callback,
        total_windows=total_windows,
        max_horizon_days=context.max_horizon_days,
    )

    horizon_rows, comparable_windows = _collect_backtest_horizon_rows(
        history_frame=context.history_frame,
        history_dates=context.history_dates,
        selected_origin_dates=selected_origin_dates,
        total_windows=total_windows,
        max_horizon_days=context.max_horizon_days,
        min_train_rows=context.min_train_rows,
        horizon_days=context.horizon_days,
        progress_callback=progress_callback,
    )

    valid_rows = horizon_rows.get(context.validation_horizon_days, [])
    valid_rows_7 = horizon_rows.get(7, [])
    if perf is not None:
        perf.update(valid_windows=len(valid_rows), comparable_windows=comparable_windows)
    if not valid_rows:
        return _not_ready_backtest(
            f'Lead-time-aware validation collected no comparable origins for the '
            f'{_lead_time_label(context.validation_horizon_days)} lead.'
        )

    artifacts = _build_backtest_evaluation_artifacts(
        horizon_rows=horizon_rows,
        horizon_days=context.horizon_days,
        valid_rows=valid_rows,
        dataset=context.dataset,
        validation_horizon_days=context.validation_horizon_days,
    )
    _emit_progress(
        progress_callback,
        'ml_backtest.completed',
        (
            f'Backtesting completed: {len(artifacts.backtest_rows)} comparable origins, '
            f'lead-time-aware summary on the {_lead_time_label(context.validation_horizon_days)} lead.'
        ),
    )

    if perf is not None:
        perf.update(
            completed_windows=total_windows,
            valid_windows=len(valid_rows),
            candidate_models=len(artifacts.selection.count_metrics),
            payload_rows=len(artifacts.backtest_rows),
        )

    horizon_7_mae: Optional[float] = None
    if len(valid_rows_7) >= MIN_BACKTEST_POINTS:
        evaluation_data_7 = _build_horizon_evaluation_data(
            valid_rows_7,
            artifacts.selected_count_model_key,
            include_count_model_predictions=True,
        )
        selected_predictions_7 = evaluation_data_7.selected_predictions
        if selected_predictions_7.size:
            horizon_7_mae = CountMetrics.coerce(
                compute_count_metrics(evaluation_data_7.actuals, selected_predictions_7)
            ).mae

    result = _build_backtest_success_result(
        artifacts=artifacts,
        valid_rows=valid_rows,
        min_train_rows=context.min_train_rows,
        validation_horizon_days=context.validation_horizon_days,
        max_horizon_days=context.max_horizon_days,
        horizon_days=context.horizon_days,
    )
    result.horizon_7_mae = horizon_7_mae
    return result

__all__ = [
    '_not_ready_backtest',
    '_build_window',
    '_simulate_candidate_paths',
    '_fit_candidates',
    '_build_window_rows',
    '_score_candidates',
    '_select_working_method',
    '_select_backtest_count_model',
    '_select_backtest_origins',
    '_prepare_backtest_run_context',
    '_validate_backtest_run_context',
    '_record_backtest_context_perf',
    '_emit_backtest_start_progress',
    '_run_backtest',
]
