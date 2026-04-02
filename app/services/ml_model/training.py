from __future__ import annotations

from contextlib import nullcontext
from typing import Any, Dict, Optional

from app.perf import current_perf_trace, profiled

from .constants import COUNT_MODEL_KEYS, COUNT_MODEL_LABELS, MAX_HISTORY_POINTS, MIN_DAILY_HISTORY, MIN_FEATURE_ROWS
from .runtime import MlProgressCallback, _emit_progress
from .training_dataset import (
    _build_backtest_seed_dataset,
    _build_design_matrix,
    _build_history_frame,
    _feature_frame,
    _prepare_reference_frame,
    _prepare_training_dataset,
)
from .training_forecast import (
    _bound_probability,
    _build_future_forecast_rows as _build_future_forecast_rows_impl,
    _build_prediction_interval_calibration,
    _count_interval,
    _evaluate_prediction_interval_backtest,
    _format_probability,
    _format_ratio_percent,
    _future_feature_row,
    _history_records_from_frame,
    _interval_coverage,
    _prediction_interval_margin,
    _predict_future_count,
    _risk_band_from_index,
    _risk_index,
)
from .training_importance import _aggregate_feature_name, _build_feature_importance, _fallback_feature_importance
from . import training_backtesting as _training_backtesting_impl
from . import training_models as _training_models_impl
from .training_result import _assemble_training_result, _empty_ml_result
from .training_temperature import (
    _apply_temperature_statistics,
    _fit_temperature_statistics,
    _temperature_feature_columns,
    _temperature_quality_note,
    _temperature_quality_summary,
    _temperature_source_series,
)

sm = _training_models_impl.sm
ConvergenceWarning = _training_models_impl.ConvergenceWarning
StatsmodelsConvergenceWarning = _training_models_impl.StatsmodelsConvergenceWarning
HessianInversionWarning = _training_models_impl.HessianInversionWarning
PerfectSeparationWarning = _training_models_impl.PerfectSeparationWarning

_build_count_model = _training_models_impl._build_count_model
_count_model_scaled_columns = _training_models_impl._count_model_scaled_columns
_build_count_model_pipeline = _training_models_impl._build_count_model_pipeline
_prepare_statsmodels_count_design = _training_models_impl._prepare_statsmodels_count_design
_warning_indicates_unstable_fit = _training_models_impl._warning_indicates_unstable_fit
_has_warning_instability = _training_models_impl._has_warning_instability
_fit_with_convergence_guard = _training_models_impl._fit_with_convergence_guard
_estimate_negative_binomial_alpha = _training_models_impl._estimate_negative_binomial_alpha
_can_train_event_model = _training_models_impl._can_train_event_model
_baseline_expected_count = _training_backtesting_impl._baseline_expected_count
_baseline_event_probability = _training_backtesting_impl._baseline_event_probability
_scenario_reference_forecast = _training_backtesting_impl._scenario_reference_forecast
_estimate_overdispersion_ratio = _training_backtesting_impl._estimate_overdispersion_ratio
_available_count_model_labels = _training_backtesting_impl._available_count_model_labels
_all_count_metrics = _training_backtesting_impl._all_count_metrics
_metrics_within_selection_tolerance = _training_backtesting_impl._metrics_within_selection_tolerance
_select_count_method = _training_backtesting_impl._select_count_method
_build_count_selection_details = _training_backtesting_impl._build_count_selection_details
_select_count_model = _training_backtesting_impl._select_count_model
_metric_sort_key = _training_backtesting_impl._metric_sort_key
_within_relative_margin = _training_backtesting_impl._within_relative_margin
_build_count_comparison_rows = _training_backtesting_impl._build_count_comparison_rows
_selected_count_prediction = _training_backtesting_impl._selected_count_prediction
_event_rate = _training_backtesting_impl._event_rate
_has_both_event_classes = _training_backtesting_impl._has_both_event_classes
_event_rate_is_saturated = _training_backtesting_impl._event_rate_is_saturated
_event_probability_note = _training_backtesting_impl._event_probability_note
_empty_event_metrics = _training_backtesting_impl._empty_event_metrics
_normalized_event_model_label = _training_backtesting_impl._normalized_event_model_label
_normalize_event_comparison_rows = _training_backtesting_impl._normalize_event_comparison_rows
_compute_event_metrics = _training_backtesting_impl._compute_event_metrics
_event_metric_sort_key = _training_backtesting_impl._event_metric_sort_key
_safe_roc_auc = _training_backtesting_impl._safe_roc_auc
_safe_log_loss = _training_backtesting_impl._safe_log_loss

_fit_count_model_impl = _training_models_impl._fit_count_model
_fit_count_model_from_design_impl = _training_models_impl._fit_count_model_from_design
_fit_negative_binomial_model_from_design_impl = _training_models_impl._fit_negative_binomial_model_from_design
_predict_count_model_impl = _training_models_impl._predict_count_model
_predict_count_from_design = _training_models_impl._predict_count_from_design
_fit_event_model_impl = _training_models_impl._fit_event_model
_fit_event_model_from_design_impl = _training_models_impl._fit_event_model_from_design
_predict_event_probability_impl = _training_models_impl._predict_event_probability
_predict_event_probability_from_design = _training_models_impl._predict_event_probability_from_design
_run_backtest_impl = _training_backtesting_impl._run_backtest


def _sync_model_impl() -> None:
    _training_models_impl.sm = sm
    _training_models_impl._build_design_matrix = _build_design_matrix
    _training_models_impl._build_count_model_pipeline = _build_count_model_pipeline
    _training_models_impl._fit_with_convergence_guard = _fit_with_convergence_guard
    _training_models_impl._fit_negative_binomial_model_from_design = _fit_negative_binomial_model_from_design
    _training_models_impl._prepare_statsmodels_count_design = _prepare_statsmodels_count_design
    _training_models_impl._has_warning_instability = _has_warning_instability
    _training_models_impl._estimate_negative_binomial_alpha = _estimate_negative_binomial_alpha
    _training_models_impl._can_train_event_model = _can_train_event_model
    _training_models_impl._fit_count_model_from_design = _fit_count_model_from_design
    _training_models_impl._fit_event_model_from_design = _fit_event_model_from_design


def _sync_backtest_impl() -> None:
    _training_backtesting_impl._emit_progress = _emit_progress
    _training_backtesting_impl._prepare_reference_frame = _prepare_reference_frame
    _training_backtesting_impl._prepare_training_dataset = _prepare_training_dataset
    _training_backtesting_impl._fit_temperature_statistics = _fit_temperature_statistics
    _training_backtesting_impl._temperature_feature_columns = _temperature_feature_columns
    _training_backtesting_impl._build_design_matrix = _build_design_matrix
    _training_backtesting_impl._fit_count_model_from_design = _fit_count_model_from_design
    _training_backtesting_impl._predict_count_from_design = _predict_count_from_design
    _training_backtesting_impl._fit_event_model_from_design = _fit_event_model_from_design
    _training_backtesting_impl._predict_event_probability_from_design = _predict_event_probability_from_design
    _training_backtesting_impl._baseline_expected_count = _baseline_expected_count
    _training_backtesting_impl._baseline_event_probability = _baseline_event_probability
    _training_backtesting_impl._scenario_reference_forecast = _scenario_reference_forecast
    _training_backtesting_impl._evaluate_prediction_interval_backtest = _evaluate_prediction_interval_backtest
    _training_backtesting_impl._count_interval = _count_interval
    _training_backtesting_impl._format_ratio_percent = _format_ratio_percent


def _fit_count_model(model_key: str, frame, feature_columns=None):
    _sync_model_impl()
    return _fit_count_model_impl(model_key, frame, feature_columns=feature_columns)


def _fit_count_model_from_design(model_key: str, X_train, y_train):
    _sync_model_impl()
    return _fit_count_model_from_design_impl(model_key, X_train, y_train)


def _fit_negative_binomial_model_from_design(X_train, y_train):
    _sync_model_impl()
    return _fit_negative_binomial_model_from_design_impl(X_train, y_train)


def _predict_count_model(model_bundle, frame):
    _sync_model_impl()
    return _predict_count_model_impl(model_bundle, frame)


def _fit_event_model(frame, feature_columns=None):
    _sync_model_impl()
    return _fit_event_model_impl(frame, feature_columns=feature_columns)


def _fit_event_model_from_design(X_train, y_train):
    _sync_model_impl()
    return _fit_event_model_from_design_impl(X_train, y_train)


def _predict_event_probability(model_bundle, frame):
    _sync_model_impl()
    return _predict_event_probability_impl(model_bundle, frame)


@profiled('ml_backtest')
def _run_backtest(
    history_frame,
    dataset,
    progress_callback: MlProgressCallback = None,
) -> Dict[str, Any]:
    _sync_backtest_impl()
    return _run_backtest_impl(history_frame, dataset, progress_callback=progress_callback)


def _build_future_forecast_rows(
    frame,
    selected_count_model_key,
    count_model,
    event_model,
    forecast_days,
    scenario_temperature,
    interval_calibration,
    temperature_stats=None,
):
    return _build_future_forecast_rows_impl(
        frame=frame,
        selected_count_model_key=selected_count_model_key,
        count_model=count_model,
        event_model=event_model,
        forecast_days=forecast_days,
        scenario_temperature=scenario_temperature,
        interval_calibration=interval_calibration,
        baseline_expected_count=_baseline_expected_count,
        temperature_stats=temperature_stats,
    )


def _train_ml_model(
    daily_history,
    forecast_days: int,
    scenario_temperature: Optional[float],
    progress_callback: MlProgressCallback = None,
) -> Dict[str, Any]:
    perf = current_perf_trace()
    if len(daily_history) < MIN_DAILY_HISTORY or forecast_days <= 0:
        return _empty_ml_result(
            f'Для ML-блока нужно минимум {MIN_DAILY_HISTORY} дней непрерывной дневной истории, чтобы выполнить rolling-origin backtesting и обучить модель.'
        )

    _emit_progress(progress_callback, 'ml_model.running', 'Подготавливаем признаки и обучающую выборку для ML-модели.')
    feature_prep_context = perf.span('feature_prep') if perf is not None else nullcontext()
    with feature_prep_context:
        history_tail = daily_history[-MAX_HISTORY_POINTS:]
        frame = _build_history_frame(history_tail)
        dataset = _build_backtest_seed_dataset(frame)
        if perf is not None:
            perf.update(history_points=len(history_tail), feature_rows=len(dataset))
    if len(dataset) < MIN_FEATURE_ROWS:
        return _empty_ml_result(
            f'После формирования лагов и скользящих признаков осталось только {len(dataset)} наблюдений: этого мало для корректного rolling-origin backtesting.'
        )

    _emit_progress(progress_callback, 'ml_backtest.pending', 'Готовим rolling-origin backtesting на выбранной истории.')
    backtest = _run_backtest(frame.copy(), dataset, progress_callback=progress_callback)
    if not backtest['is_ready']:
        _emit_progress(progress_callback, 'ml_backtest.failed', backtest['message'])
        return _empty_ml_result(backtest['message'])

    final_frame, final_dataset, final_temperature_stats = _prepare_training_dataset(frame.copy())
    if len(final_dataset) < MIN_FEATURE_ROWS:
        return _empty_ml_result(
            f'После подготовки полной обучающей выборки осталось только {len(final_dataset)} наблюдений: этого мало для итогового обучения ML-модели.'
        )

    selected_count_model_key = backtest['selected_count_model_key']
    feature_columns = _temperature_feature_columns(final_temperature_stats)
    _emit_progress(
        progress_callback,
        'ml_model.running',
        f"Обучаем итоговую count-модель {COUNT_MODEL_LABELS.get(selected_count_model_key, selected_count_model_key)} на полной истории.",
    )
    final_count_model = (
        _fit_count_model(selected_count_model_key, final_dataset, feature_columns=feature_columns)
        if selected_count_model_key in COUNT_MODEL_KEYS
        else None
    )
    if selected_count_model_key in COUNT_MODEL_KEYS and final_count_model is None:
        return _empty_ml_result('Не удалось обучить итоговую модель по числу пожаров на полной выборке.')

    selected_event_model_key = backtest['event_metrics'].get('selected_model_key')
    classifier_validated = (
        selected_event_model_key == 'logistic_regression'
        and backtest['event_metrics'].get('available', False)
        and backtest['event_metrics'].get('logistic_available', False)
        and backtest['event_metrics'].get('event_probability_informative', False)
        and _can_train_event_model(final_dataset['event'])
    )
    final_event_model = _fit_event_model(final_dataset, feature_columns=feature_columns) if classifier_validated else None

    _emit_progress(progress_callback, 'ml_model.running', 'Строим прогноз по будущим датам и интервалы неопределённости.')
    forecast_rows = _build_future_forecast_rows(
        frame=final_frame,
        selected_count_model_key=selected_count_model_key,
        count_model=final_count_model,
        event_model=final_event_model,
        forecast_days=forecast_days,
        scenario_temperature=scenario_temperature,
        interval_calibration=backtest['prediction_interval_calibration'],
        temperature_stats=final_temperature_stats,
    )

    _emit_progress(progress_callback, 'ml_model.running', 'Оцениваем важность признаков и собираем итоговый ML-отчёт.')
    result_render_context = perf.span('result_render') if perf is not None else nullcontext()
    with result_render_context:
        feature_importance = _build_feature_importance(final_count_model, final_dataset) if final_count_model is not None else []
        if perf is not None:
            perf.update(
                forecast_rows=len(forecast_rows),
                feature_importance_rows=len(feature_importance),
                backtest_rows=len(backtest['rows']),
            )
        return _assemble_training_result(
            backtest=backtest,
            forecast_rows=forecast_rows,
            feature_importance=feature_importance,
            final_temperature_stats=final_temperature_stats,
            final_event_model=final_event_model,
            selected_count_model_key=selected_count_model_key,
        )
