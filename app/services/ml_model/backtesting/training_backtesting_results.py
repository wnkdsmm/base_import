from __future__ import annotations

from typing import Any, Sequence

import numpy as np
import pandas as pd

from app.perf import current_perf_trace
from app.services.model_quality import compute_count_metrics

from ..ml_model_config_types import CLASSIFICATION_THRESHOLD, COUNT_MODEL_KEYS, COUNT_SELECTION_RULE, EVENT_SELECTION_RULE, MAX_BACKTEST_POINTS, MIN_BACKTEST_POINTS, MIN_FEATURE_ROWS, MlProgressCallback, ROLLING_MIN_TRAIN_ROWS, _emit_progress
from ..ml_model_result_types import BacktestFailure, BacktestOverview, BacktestRunResult, BacktestSuccess, BacktestWindowRow, CountMetrics, EventMetrics, HorizonSummary, PredictionIntervalCalibrationByHorizon
from .training_backtesting_baselines import _baseline_event_probability, _baseline_expected_count
from .training_backtesting_events import _compute_event_metrics
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


def _collect_backtest_horizon_rows(
    *,
    history_frame: pd.DataFrame,
    history_dates: np.ndarray,
    selected_origin_dates: Sequence[Any],
    total_windows: int,
    max_horizon_days: int,
    min_train_rows: int,
    horizon_days: list[int],
    progress_callback: MlProgressCallback,
) -> tuple[dict[int, list[BacktestWindowRow]], int]:
    from .training_backtesting_execution import (
        _build_window,
        _build_window_rows,
        _fit_candidates,
    )

    horizon_rows: dict[int, list[BacktestWindowRow]] = {horizon_day: [] for horizon_day in horizon_days}
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


def _build_backtest_overview(
    *,
    backtest_rows,
    valid_rows: list[BacktestWindowRow],
    min_train_rows: int,
    validation_horizon_days: int,
    max_horizon_days: int,
    horizon_days: list[int],
    selection: _BacktestSelection,
    event_metrics: EventMetrics,
    prediction_interval_calibration: dict[str, Any],
    validation_summary: HorizonSummary,
    horizon_summaries: dict[str, HorizonSummary],
) -> BacktestOverview:
    prediction_interval_coverage = validation_summary.prediction_interval_coverage
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
        prediction_interval_coverage_display=validation_summary.prediction_interval_coverage_display,
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
    horizon_rows: dict[int, list[BacktestWindowRow]],
    horizon_days: list[int],
    valid_rows: list[BacktestWindowRow],
    dataset: pd.DataFrame,
    validation_horizon_days: int,
) -> _BacktestEvaluationArtifacts:
    from .training_backtesting_execution import _select_backtest_count_model

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
        interval_calibration_by_horizon=interval_calibration_by_horizon,
        horizon_summaries=horizon_summaries,
        prediction_interval_calibration=prediction_interval_calibration,
        validation_summary=validation_summary,
        backtest_rows=backtest_rows,
        event_metrics=_compute_event_metrics(validation_evaluation_data),
        window_rows=window_rows,
    )


def _build_backtest_success_result(
    *,
    artifacts: _BacktestEvaluationArtifacts,
    valid_rows: list[BacktestWindowRow],
    min_train_rows: int,
    validation_horizon_days: int,
    max_horizon_days: int,
    horizon_days: list[int],
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

__all__ = [
    '_collect_backtest_horizon_rows',
    '_build_backtest_overview',
    '_build_backtest_evaluation_artifacts',
    '_build_backtest_success_result',
]
