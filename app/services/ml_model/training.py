from __future__ import annotations

from collections import OrderedDict
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from app.perf import current_perf_trace, profiled

from . import training_backtesting as _backtesting
from . import training_dataset as _dataset
from . import training_forecast as _forecast
from . import training_importance as _importance
from . import training_models as _models
from . import training_result as _result
from . import training_selection as _selection
from . import training_temperature as _temperature
from .constants import (
    COUNT_MODEL_KEYS,
    COUNT_MODEL_LABELS,
    EXPLAINABLE_COUNT_MODEL_KEY,
    MAX_HISTORY_POINTS,
    MIN_DAILY_HISTORY,
    MIN_FEATURE_ROWS,
)
from .domain_types import coerce_backtest_result
from .runtime import MlProgressCallback, _emit_progress

# Compatibility exports for tests and legacy imports.
_all_count_metrics = _selection._all_count_metrics
_available_count_model_labels = _selection._available_count_model_labels
_baseline_event_probability = _backtesting._baseline_event_probability
_baseline_expected_count = _backtesting._baseline_expected_count
_bound_probability = _forecast._bound_probability
_build_count_model = _models._build_count_model
_build_count_model_pipeline = _models._build_count_model_pipeline
_build_backtest_seed_dataset = _dataset._build_backtest_seed_dataset
_build_count_comparison_rows = _selection._build_count_comparison_rows
_build_count_selection_details = _selection._build_count_selection_details
_build_design_matrix = _dataset._build_design_matrix
_build_design_row = _dataset._build_design_row
_build_feature_importance = _importance._build_feature_importance
_build_history_frame = _dataset._build_history_frame
_build_prediction_interval_calibration = _forecast._build_prediction_interval_calibration
_can_train_event_model = _models._can_train_event_model
_compute_event_metrics = _backtesting._compute_event_metrics
_count_interval = _forecast._count_interval
_count_model_scaled_columns = _models._count_model_scaled_columns
_empty_event_metrics = _backtesting._empty_event_metrics
_empty_ml_result = _result._empty_ml_result
_estimate_negative_binomial_alpha = _models._estimate_negative_binomial_alpha
_estimate_overdispersion_ratio = _backtesting._estimate_overdispersion_ratio
_evaluate_prediction_interval_backtest = _forecast._evaluate_prediction_interval_backtest
_event_metric_sort_key = _backtesting._event_metric_sort_key
_event_probability_note = _backtesting._event_probability_note
_event_rate = _backtesting._event_rate
_event_rate_is_saturated = _backtesting._event_rate_is_saturated
_feature_frame = _dataset._feature_frame
_fit_count_model = _models._fit_count_model
_fit_count_model_from_design = _models._fit_count_model_from_design
_fit_event_model = _models._fit_event_model
_fit_event_model_from_design = _models._fit_event_model_from_design
_fit_negative_binomial_model_from_design = _models._fit_negative_binomial_model_from_design
_fit_temperature_statistics = _temperature._fit_temperature_statistics
_fit_with_convergence_guard = _models._fit_with_convergence_guard
_aggregate_feature_name = _importance._aggregate_feature_name
_assemble_training_result = _result._assemble_training_result
_apply_temperature_statistics = _temperature._apply_temperature_statistics
_fallback_feature_importance = _importance._fallback_feature_importance
_format_probability = _forecast._format_probability
_format_ratio_percent = _forecast._format_ratio_percent
_future_feature_row = _forecast._future_feature_row
_has_both_event_classes = _backtesting._has_both_event_classes
_has_warning_instability = _models._has_warning_instability
_history_records_from_frame = _forecast._history_records_from_frame
_interval_coverage = _forecast._interval_coverage
_metric_sort_key = _selection._metric_sort_key
_metrics_within_selection_tolerance = _selection._metrics_within_selection_tolerance
_normalize_event_comparison_rows = _backtesting._normalize_event_comparison_rows
_normalized_event_model_label = _backtesting._normalized_event_model_label
_predict_count_from_design = _models._predict_count_from_design
_predict_count_model = _models._predict_count_model
_predict_event_probability = _models._predict_event_probability
_predict_event_probability_from_design = _models._predict_event_probability_from_design
_prediction_interval_margin = _forecast._prediction_interval_margin
_predict_future_count = _forecast._predict_future_count
_prepare_reference_frame = _dataset._prepare_reference_frame
_prepare_statsmodels_count_design = _models._prepare_statsmodels_count_design
_prepare_training_dataset = _dataset._prepare_training_dataset
_risk_band_from_index = _forecast._risk_band_from_index
_risk_index = _forecast._risk_index
_run_backtest = profiled('ml_backtest')(_backtesting._run_backtest)
_safe_log_loss = _backtesting._safe_log_loss
_safe_roc_auc = _backtesting._safe_roc_auc
_scenario_reference_forecast = _backtesting._scenario_reference_forecast
_select_count_method = _selection._select_count_method
_select_count_model = _selection._select_count_model
_selected_count_prediction = _backtesting._selected_count_prediction
_temperature_feature_columns = _temperature._temperature_feature_columns
_temperature_quality_note = _temperature._temperature_quality_note
_temperature_quality_summary = _temperature._temperature_quality_summary
_temperature_source_series = _temperature._temperature_source_series
_warning_indicates_unstable_fit = _models._warning_indicates_unstable_fit

ConvergenceWarning = _models.ConvergenceWarning
HessianInversionWarning = _models.HessianInversionWarning
PerfectSeparationWarning = _models.PerfectSeparationWarning
StatsmodelsConvergenceWarning = _models.StatsmodelsConvergenceWarning
sm = _models.sm


@dataclass
class _TrainingArtifacts:
    final_frame: Any
    final_dataset: Any
    final_temperature_stats: Dict[str, Any]
    backtest: Any
    final_count_model: Optional[Dict[str, Any]]
    final_event_model: Optional[Dict[str, Any]]
    selected_count_model_key: str
    feature_importance: List[Dict[str, Any]]
    feature_importance_source_key: Optional[str]
    feature_importance_source_label: Optional[str]
    feature_importance_note: Optional[str]


_TRAINING_ARTIFACT_CACHE_LIMIT = 4
_TRAINING_ARTIFACT_CACHE: OrderedDict[Tuple[int, Tuple[Tuple[Any, ...], ...]], _TrainingArtifacts] = OrderedDict()


def clear_training_artifact_cache() -> None:
    _TRAINING_ARTIFACT_CACHE.clear()


def _signature_date(value: Any) -> str:
    if hasattr(value, 'isoformat'):
        return str(value.isoformat())
    return str(value)


def _signature_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if numeric_value != numeric_value:
        return None
    return numeric_value


def _daily_history_signature(daily_history: List[Dict[str, Any]]) -> Tuple[Tuple[Any, ...], ...]:
    return tuple(
        (
            _signature_date(item.get('date')),
            _signature_float(item.get('count')),
            _signature_float(item.get('avg_temperature')),
        )
        for item in daily_history[-MAX_HISTORY_POINTS:]
    )


def _training_artifact_cache_key(
    daily_history: List[Dict[str, Any]],
    forecast_days: int,
) -> Tuple[int, Tuple[Tuple[Any, ...], ...]]:
    return (int(forecast_days), _daily_history_signature(daily_history))


def _training_artifact_cache_get(
    cache_key: Tuple[int, Tuple[Tuple[Any, ...], ...]],
) -> Optional[_TrainingArtifacts]:
    artifacts = _TRAINING_ARTIFACT_CACHE.get(cache_key)
    if artifacts is not None:
        _TRAINING_ARTIFACT_CACHE.move_to_end(cache_key)
    return artifacts


def _training_artifact_cache_store(
    cache_key: Tuple[int, Tuple[Tuple[Any, ...], ...]],
    artifacts: _TrainingArtifacts,
) -> _TrainingArtifacts:
    _TRAINING_ARTIFACT_CACHE[cache_key] = artifacts
    _TRAINING_ARTIFACT_CACHE.move_to_end(cache_key)
    while len(_TRAINING_ARTIFACT_CACHE) > _TRAINING_ARTIFACT_CACHE_LIMIT:
        _TRAINING_ARTIFACT_CACHE.popitem(last=False)
    return artifacts


def _forecast_rows_from_training_artifacts(
    artifacts: _TrainingArtifacts,
    *,
    forecast_days: int,
    scenario_temperature: Optional[float],
) -> List[Dict[str, Any]]:
    return _forecast._build_future_forecast_rows(
        frame=artifacts.final_frame,
        selected_count_model_key=artifacts.selected_count_model_key,
        count_model=artifacts.final_count_model,
        event_model=artifacts.final_event_model,
        forecast_days=forecast_days,
        scenario_temperature=scenario_temperature,
        interval_calibration=(
            artifacts.backtest.prediction_interval_calibration_by_horizon.by_horizon
            or artifacts.backtest.prediction_interval_calibration
        ),
        baseline_expected_count=_baseline_expected_count,
        temperature_stats=artifacts.final_temperature_stats,
    )


def _assemble_training_artifacts_result(
    artifacts: _TrainingArtifacts,
    forecast_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return _result._assemble_training_result(
        backtest=artifacts.backtest,
        forecast_rows=forecast_rows,
        feature_importance=[dict(row) for row in artifacts.feature_importance],
        feature_importance_source_key=artifacts.feature_importance_source_key,
        feature_importance_source_label=artifacts.feature_importance_source_label,
        feature_importance_note=artifacts.feature_importance_note,
        final_temperature_stats=artifacts.final_temperature_stats,
        final_event_model=artifacts.final_event_model,
        selected_count_model_key=artifacts.selected_count_model_key,
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

    artifact_cache_key = _training_artifact_cache_key(daily_history, forecast_days)
    cached_artifacts = _training_artifact_cache_get(artifact_cache_key)
    if cached_artifacts is not None:
        result_render_context = perf.span('result_render') if perf is not None else nullcontext()
        with result_render_context:
            forecast_rows = _forecast_rows_from_training_artifacts(
                cached_artifacts,
                forecast_days=forecast_days,
                scenario_temperature=scenario_temperature,
            )
            if perf is not None:
                perf.update(
                    training_artifact_cache_hit=True,
                    history_points=len(artifact_cache_key[1]),
                    feature_rows=len(cached_artifacts.final_dataset),
                    forecast_rows=len(forecast_rows),
                    feature_importance_rows=len(cached_artifacts.feature_importance),
                    backtest_rows=len(cached_artifacts.backtest.rows),
                )
            return _assemble_training_artifacts_result(cached_artifacts, forecast_rows)

    if perf is not None:
        perf.update(training_artifact_cache_hit=False)

    _emit_progress(progress_callback, 'ml_model.running', 'Подготавливаем признаки и обучающую выборку для ML-модели.')
    feature_prep_context = perf.span('feature_prep') if perf is not None else nullcontext()
    with feature_prep_context:
        history_tail = daily_history[-MAX_HISTORY_POINTS:]
        frame = _prepare_reference_frame(_build_history_frame(history_tail))
        dataset = _build_backtest_seed_dataset(frame, frame_is_prepared=True)
        if perf is not None:
            perf.update(history_points=len(history_tail), feature_rows=len(dataset))
    if len(dataset) < MIN_FEATURE_ROWS:
        return _empty_ml_result(
            f'После формирования лагов и скользящих признаков осталось только {len(dataset)} наблюдений: этого мало для корректного rolling-origin backtesting.'
        )

    _emit_progress(progress_callback, 'ml_backtest.pending', 'Готовим rolling-origin backtesting на выбранной истории.')
    backtest = _run_backtest(
        frame,
        dataset,
        progress_callback=progress_callback,
        validation_horizon_days=forecast_days,
        max_horizon_days=forecast_days,
        history_frame_is_prepared=True,
    )
    backtest = coerce_backtest_result(backtest)
    if not backtest.is_ready:
        _emit_progress(progress_callback, 'ml_backtest.failed', backtest.message)
        return _empty_ml_result(backtest.message)

    final_frame, final_dataset, final_temperature_stats = _prepare_training_dataset(frame, frame_is_prepared=True)
    if len(final_dataset) < MIN_FEATURE_ROWS:
        return _empty_ml_result(
            f'После подготовки полной обучающей выборки осталось только {len(final_dataset)} наблюдений: этого мало для итогового обучения ML-модели.'
        )

    selected_count_model_key = backtest.selected_count_model_key
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

    selected_event_model_key = backtest.event_metrics.selected_model_key
    classifier_validated = (
        selected_event_model_key == 'logistic_regression'
        and backtest.event_metrics.available
        and backtest.event_metrics.logistic_available
        and backtest.event_metrics.event_probability_informative
        and _can_train_event_model(final_dataset['event'])
    )
    final_event_model = _fit_event_model(final_dataset, feature_columns=feature_columns) if classifier_validated else None

    _emit_progress(progress_callback, 'ml_model.running', 'Строим прогноз по будущим датам и интервалы неопределённости.')
    forecast_rows = _forecast._build_future_forecast_rows(
        frame=final_frame,
        selected_count_model_key=selected_count_model_key,
        count_model=final_count_model,
        event_model=final_event_model,
        forecast_days=forecast_days,
        scenario_temperature=scenario_temperature,
        interval_calibration=(
            backtest.prediction_interval_calibration_by_horizon.by_horizon
            or backtest.prediction_interval_calibration
        ),
        baseline_expected_count=_baseline_expected_count,
        temperature_stats=final_temperature_stats,
    )

    _emit_progress(progress_callback, 'ml_model.running', 'Оцениваем важность признаков и собираем итоговый ML-отчёт.')
    result_render_context = perf.span('result_render') if perf is not None else nullcontext()
    with result_render_context:
        feature_importance_model_key = selected_count_model_key if selected_count_model_key in COUNT_MODEL_KEYS else None
        feature_importance_model = final_count_model
        if feature_importance_model is None:
            feature_importance_model_key = EXPLAINABLE_COUNT_MODEL_KEY
            feature_importance_model = _fit_count_model(
                EXPLAINABLE_COUNT_MODEL_KEY,
                final_dataset,
                feature_columns=feature_columns,
            )

        feature_importance = (
            _importance._build_feature_importance(feature_importance_model, final_dataset)
            if feature_importance_model is not None
            else []
        )
        feature_importance_source_label = (
            COUNT_MODEL_LABELS.get(feature_importance_model_key, feature_importance_model_key)
            if feature_importance and feature_importance_model_key
            else None
        )
        feature_importance_note = None
        if (
            feature_importance
            and feature_importance_model_key
            and feature_importance_model_key != selected_count_model_key
        ):
            selected_label = COUNT_MODEL_LABELS.get(selected_count_model_key, selected_count_model_key)
            source_label = COUNT_MODEL_LABELS.get(feature_importance_model_key, feature_importance_model_key)
            feature_importance_note = (
                f'Рабочий метод прогноза: {selected_label}. '
                f'Драйверы ниже показаны по {source_label}, потому что это объяснимая ML-модель для разбора факторов.'
            )
        if perf is not None:
            perf.update(
                forecast_rows=len(forecast_rows),
                feature_importance_rows=len(feature_importance),
                backtest_rows=len(backtest.rows),
            )

        _training_artifact_cache_store(
            artifact_cache_key,
            _TrainingArtifacts(
                final_frame=final_frame,
                final_dataset=final_dataset,
                final_temperature_stats=final_temperature_stats,
                backtest=backtest,
                final_count_model=final_count_model,
                final_event_model=final_event_model,
                selected_count_model_key=selected_count_model_key,
                feature_importance=[dict(row) for row in feature_importance],
                feature_importance_source_key=feature_importance_model_key if feature_importance else None,
                feature_importance_source_label=feature_importance_source_label,
                feature_importance_note=feature_importance_note,
            ),
        )
        return _result._assemble_training_result(
            backtest=backtest,
            forecast_rows=forecast_rows,
            feature_importance=feature_importance,
            feature_importance_source_key=feature_importance_model_key if feature_importance else None,
            feature_importance_source_label=feature_importance_source_label,
            feature_importance_note=feature_importance_note,
            final_temperature_stats=final_temperature_stats,
            final_event_model=final_event_model,
            selected_count_model_key=selected_count_model_key,
        )
