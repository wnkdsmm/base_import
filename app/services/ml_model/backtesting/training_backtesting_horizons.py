from __future__ import annotations

from dataclasses import replace
import math


import numpy as np

from app.services.model_quality import compute_count_metrics

from ..ml_model_config_types import COUNT_MODEL_KEYS
from ..ml_model_interval_types import PredictionIntervalAdaptiveBin, PredictionIntervalCalibration
from ..ml_model_result_types import BacktestEvaluationRow, BacktestWindowRow, CountMetrics, HorizonSummary
from .training_backtesting_support import (
    _empty_float_array,
    _lead_time_label,
    _nan_float_array,
    _nan_or_float,
    _optional_probability_from_array,
    _selected_count_arrays_from_rows,
)
from .training_backtesting_types import _CandidateCoverage, _HorizonEvaluationData
from ..training.forecast_bounds import (
    _count_interval,
    _format_ratio_percent,
    _interval_coverage,
)
from ..training.forecast_calibration import _evaluate_prediction_interval_backtest


def _enforce_monotonic_horizon_interval_calibrations(
    interval_calibration_by_horizon: dict[int, PredictionIntervalCalibration],
) -> dict[int, PredictionIntervalCalibration]:
    monotonic_calibrations: dict[int, PredictionIntervalCalibration] = {}
    running_floor = 0.0
    for horizon_day in sorted(interval_calibration_by_horizon):
        calibration = dict(interval_calibration_by_horizon[horizon_day])
        original_floor = float(calibration.get('absolute_error_quantile', 0.0) or 0.0)
        horizon_floor = max(running_floor, original_floor)
        running_floor = horizon_floor

        adaptive_bins: list[PredictionIntervalAdaptiveBin] = []
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


def _selected_count_arrays_from_evaluation_data(
    evaluation_data: _HorizonEvaluationData,
    selected_count_model_key: str | None,
) -> tuple[np.ndarray, np.ndarray]:
    row_count = len(evaluation_data.rows)
    if selected_count_model_key == 'seasonal_baseline':
        return evaluation_data.baseline_predictions, _nan_float_array(row_count)
    if selected_count_model_key == 'heuristic_forecast':
        return evaluation_data.heuristic_predictions, _nan_float_array(row_count)
    if not selected_count_model_key:
        return _empty_float_array(), _nan_float_array(row_count)

    selected_predictions = evaluation_data.count_model_predictions.get(selected_count_model_key)
    selected_event_probabilities = evaluation_data.count_model_event_probabilities.get(selected_count_model_key)
    if selected_predictions is None or selected_event_probabilities is None:
        row_predictions, row_event_probabilities = _selected_count_arrays_from_rows(
            evaluation_data.rows,
            selected_count_model_key,
        )
        if selected_predictions is None:
            selected_predictions = row_predictions
        if selected_event_probabilities is None:
            selected_event_probabilities = row_event_probabilities
    if selected_predictions is not None:
        return selected_predictions, selected_event_probabilities
    if selected_count_model_key:
        return (
            _selected_count_arrays_from_rows(
                evaluation_data.rows,
                selected_count_model_key,
                include_event_probabilities=False,
            )[0],
            _nan_float_array(row_count),
        )
    return _empty_float_array(), _nan_float_array(row_count)


def _with_selected_count_model(
    evaluation_data: _HorizonEvaluationData,
    selected_count_model_key: str,
) -> _HorizonEvaluationData:
    selected_predictions, selected_event_probabilities = _selected_count_arrays_from_evaluation_data(
        evaluation_data,
        selected_count_model_key,
    )
    return replace(
        evaluation_data,
        selected_predictions=selected_predictions,
        selected_event_probabilities=selected_event_probabilities,
    )


def _build_horizon_evaluation_data(
    rows_for_horizon: list[BacktestWindowRow],
    selected_count_model_key: str | None = None,
    *,
    include_count_model_predictions: bool = False,
) -> _HorizonEvaluationData:
    actuals: list[float] = []
    baseline_predictions: list[float] = []
    heuristic_predictions: list[float] = []
    actual_events: list[int] = []
    baseline_event_probabilities: list[float] = []
    heuristic_event_probabilities: list[float] = []
    dates: list[str] = []
    prediction_buffers: dict[str, list[float]] = {}
    event_probability_buffers: dict[str, list[float]] = {}
    if include_count_model_predictions:
        prediction_buffers = {model_key: [] for model_key in COUNT_MODEL_KEYS}
        event_probability_buffers = {model_key: [] for model_key in COUNT_MODEL_KEYS}
    for row in rows_for_horizon:
        actuals.append(float(row.actual_count))
        baseline_predictions.append(float(row.baseline_count))
        heuristic_predictions.append(float(row.heuristic_count))
        actual_events.append(int(row.actual_event))
        baseline_event_probabilities.append(_nan_or_float(row.baseline_event_probability))
        heuristic_event_probabilities.append(_nan_or_float(row.heuristic_event_probability))
        dates.append(row.date)
        if include_count_model_predictions:
            for model_key in COUNT_MODEL_KEYS:
                prediction = row.predictions.get(model_key)
                if prediction is not None:
                    prediction_buffers[model_key].append(float(prediction))
                event_probability_buffers[model_key].append(
                    _nan_or_float(row.predicted_event_probabilities.get(model_key))
                )

    count_model_predictions: dict[str, np.ndarray] = {}
    count_model_event_probabilities: dict[str, np.ndarray] = {}
    coverage_by_model: dict[str, _CandidateCoverage] = {}
    if include_count_model_predictions:
        window_count = len(rows_for_horizon)
        for model_key, predictions in prediction_buffers.items():
            covered_window_count = len(predictions)
            coverage_by_model[model_key] = _CandidateCoverage(
                covered_window_count=covered_window_count,
                window_count=window_count,
                window_coverage=float(covered_window_count / window_count) if window_count else 0.0,
            )
            if covered_window_count == window_count:
                count_model_predictions[model_key] = np.asarray(predictions, dtype=float)
        count_model_event_probabilities = {
            model_key: np.asarray(probabilities, dtype=float)
            for model_key, probabilities in event_probability_buffers.items()
        }

    evaluation_data = _HorizonEvaluationData(
        rows=rows_for_horizon,
        actuals=np.asarray(actuals, dtype=float),
        baseline_predictions=np.asarray(baseline_predictions, dtype=float),
        heuristic_predictions=np.asarray(heuristic_predictions, dtype=float),
        selected_predictions=_empty_float_array(),
        count_model_predictions=count_model_predictions,
        actual_events=np.asarray(actual_events, dtype=int),
        baseline_event_probabilities=np.asarray(baseline_event_probabilities, dtype=float),
        heuristic_event_probabilities=np.asarray(heuristic_event_probabilities, dtype=float),
        selected_event_probabilities=_empty_float_array(),
        count_model_event_probabilities=count_model_event_probabilities,
        coverage_by_model=coverage_by_model,
        dates=dates,
    )
    if selected_count_model_key:
        return _with_selected_count_model(evaluation_data, selected_count_model_key)
    return evaluation_data


def _build_horizon_evaluation_data_by_horizon(
    horizon_rows: dict[int, list[BacktestWindowRow]],
    horizon_days: list[int],
    selected_count_model_key: str,
    precomputed: dict[int, _HorizonEvaluationData | None] = None,
) -> dict[int, _HorizonEvaluationData]:
    evaluation_data_by_horizon: dict[int, _HorizonEvaluationData] = {}
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


def _prediction_interval_evaluation_slice(calibration: PredictionIntervalCalibration, total_rows: int) -> slice | None:
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
    rows_for_horizon: list[BacktestWindowRow],
    *,
    selected_count_model_key: str,
    calibration: PredictionIntervalCalibration,
    evaluation_data: _HorizonEvaluationData | None = None,
) -> PredictionIntervalCalibration:
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
        else _selected_count_arrays_from_rows(
            rows_for_horizon,
            selected_count_model_key,
            include_event_probabilities=False,
        )[0]
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
    calibration: PredictionIntervalCalibration,
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
    interval_calibration_by_horizon: dict[int, PredictionIntervalCalibration],
    horizon_rows: dict[int, list[BacktestWindowRow]],
    horizon_summaries: dict[str, HorizonSummary],
    *,
    selected_count_model_key: str,
    evaluation_data_by_horizon: dict[int, _HorizonEvaluationData | None] = None,
) -> tuple[dict[int, PredictionIntervalCalibration], dict[str, HorizonSummary]]:
    updated_calibrations: dict[int, PredictionIntervalCalibration] = {}
    updated_summaries: dict[str, HorizonSummary] = {}
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


def _evaluate_horizon_rows(
    evaluation_data: _HorizonEvaluationData,
    *,
    horizon_day: int,
) -> tuple[HorizonSummary, PredictionIntervalCalibration]:
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


def _evaluate_backtest_horizon_metadata(
    *,
    horizon_rows: dict[int, list[BacktestWindowRow]],
    horizon_days: list[int],
    selected_count_model_key: str,
    evaluation_data_by_horizon: dict[int, _HorizonEvaluationData],
) -> tuple[dict[int, PredictionIntervalCalibration], dict[str, HorizonSummary]]:
    interval_calibration_by_horizon: dict[int, PredictionIntervalCalibration] = {}
    horizon_summaries: dict[str, HorizonSummary] = {}
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
    prediction_interval_calibration: PredictionIntervalCalibration,
    validation_horizon_days: int,
) -> tuple[list[BacktestEvaluationRow], list[BacktestWindowRow]]:
    backtest_rows: list[BacktestEvaluationRow] = []
    window_rows: list[BacktestWindowRow] = []
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
