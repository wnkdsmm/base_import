from __future__ import annotations

from collections import OrderedDict
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from app.perf import current_perf_trace, profiled

from . import forecast_bounds as _forecast_bounds
from . import forecast_calibration as _forecast_calibration
from . import forecast_intervals as _forecast_intervals
from ..backtesting import training_backtesting as _backtesting
from . import training_dataset as _dataset
from . import training_importance as _importance
from . import training_models as _models
from . import training_result as _result
from . import training_selection as _selection
from . import training_temperature as _temperature
from ..ml_model_config_types import COUNT_MODEL_KEYS, COUNT_MODEL_LABELS, EXPLAINABLE_COUNT_MODEL_KEY, MlProgressCallback, MAX_HISTORY_POINTS, MIN_DAILY_HISTORY, MIN_FEATURE_ROWS, _emit_progress
from ..ml_model_result_types import BacktestSuccess, coerce_backtest_result
from .types import (
    TrainingFeatureImportanceRow,
    TrainingForecastRow,
    TrainingHistoryRecord,
    TrainingModelArtifact,
    TrainingResultPayload,
    TrainingTemperatureStats,
)

# Compatibility exports for tests and legacy imports.
_all_count_metrics = _selection._all_count_metrics
_available_count_model_labels = _selection._available_count_model_labels
_baseline_event_probability = _backtesting._baseline_event_probability
_baseline_expected_count = _backtesting._baseline_expected_count
_bound_probability = _forecast_bounds._bound_probability
_build_count_model = _models._build_count_model
_build_count_model_pipeline = _models._build_count_model_pipeline
_build_backtest_seed_dataset = _dataset._build_backtest_seed_dataset
_build_count_comparison_rows = _selection._build_count_comparison_rows
_build_count_selection_details = _selection._build_count_selection_details
_build_design_matrix = _dataset._build_design_matrix
_build_design_row = _dataset._build_design_row
_build_feature_importance = _importance._build_feature_importance
_build_history_frame = _dataset._build_history_frame
_build_prediction_interval_calibration = _forecast_calibration._build_prediction_interval_calibration
_can_train_event_model = _models._can_train_event_model
_compute_event_metrics = _backtesting._compute_event_metrics
_count_interval = _forecast_bounds._count_interval
_count_model_scaled_columns = _models._count_model_scaled_columns
_empty_event_metrics = _backtesting._empty_event_metrics
_empty_ml_result = _result._empty_ml_result
_estimate_negative_binomial_alpha = _models._estimate_negative_binomial_alpha
_estimate_overdispersion_ratio = _backtesting._estimate_overdispersion_ratio
_evaluate_prediction_interval_backtest = _forecast_calibration._evaluate_prediction_interval_backtest
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
_format_probability = _forecast_bounds._format_probability
_format_ratio_percent = _forecast_bounds._format_ratio_percent
_future_feature_row = _forecast_intervals._future_feature_row
_has_both_event_classes = _backtesting._has_both_event_classes
_has_warning_instability = _models._has_warning_instability
_history_records_from_frame = _forecast_intervals._history_records_from_frame
_interval_coverage = _forecast_bounds._interval_coverage
_metric_sort_key = _selection._metric_sort_key
_metrics_within_selection_tolerance = _selection._metrics_within_selection_tolerance
_normalize_event_comparison_rows = _backtesting._normalize_event_comparison_rows
_normalized_event_model_label = _backtesting._normalized_event_model_label
_predict_count_from_design = _models._predict_count_from_design
_predict_count_model = _models._predict_count_model
_predict_event_probability = _models._predict_event_probability
_predict_event_probability_from_design = _models._predict_event_probability_from_design
_prediction_interval_margin = _forecast_bounds._prediction_interval_margin
_predict_future_count = _forecast_intervals._predict_future_count
_prepare_reference_frame = _dataset._prepare_reference_frame
_prepare_statsmodels_count_design = _models._prepare_statsmodels_count_design
_prepare_training_dataset = _dataset._prepare_training_dataset
_risk_band_from_index = _forecast_bounds._risk_band_from_index
_risk_index = _forecast_bounds._risk_index
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
    final_temperature_stats: TrainingTemperatureStats
    backtest: Any
    final_count_model: Optional[TrainingModelArtifact]
    final_event_model: Optional[TrainingModelArtifact]
    selected_count_model_key: str
    feature_importance: List[TrainingFeatureImportanceRow]
    feature_importance_source_key: Optional[str]
    feature_importance_source_label: Optional[str]
    feature_importance_note: Optional[str]


@dataclass
class _TrainingSeedData:
    history_tail: List[TrainingHistoryRecord]
    frame: Any
    dataset: Any


@dataclass
class _FinalTrainingModels:
    final_frame: Any
    final_dataset: Any
    final_temperature_stats: TrainingTemperatureStats
    feature_columns: List[str]
    backtest: Any
    selected_count_model_key: str
    final_count_model: Optional[TrainingModelArtifact]
    final_event_model: Optional[TrainingModelArtifact]


@dataclass
class _FeatureImportanceArtifacts:
    rows: List[TrainingFeatureImportanceRow]
    source_key: Optional[str]
    source_label: Optional[str]
    note: Optional[str]


_TRAINING_ARTIFACT_CACHE_LIMIT = 32
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


def _daily_history_signature(daily_history: List[TrainingHistoryRecord]) -> Tuple[Tuple[Any, ...], ...]:
    return tuple(
        (
            _signature_date(item.get('date')),
            _signature_float(item.get('count')),
            _signature_float(item.get('avg_temperature')),
        )
        for item in daily_history[-MAX_HISTORY_POINTS:]
    )


def _training_artifact_cache_key(
    daily_history: List[TrainingHistoryRecord],
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
) -> List[TrainingForecastRow]:
    return _forecast_intervals._build_future_forecast_rows(
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
    forecast_rows: List[TrainingForecastRow],
) -> TrainingResultPayload:
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


def _build_training_seed(
    daily_history: List[TrainingHistoryRecord],
    *,
    perf: Any,
) -> _TrainingSeedData:
    history_tail = daily_history[-MAX_HISTORY_POINTS:]
    frame = _prepare_reference_frame(_build_history_frame(history_tail))
    dataset = _build_backtest_seed_dataset(frame, frame_is_prepared=True)
    if perf is not None:
        perf.update(history_points=len(history_tail), feature_rows=len(dataset))
    return _TrainingSeedData(history_tail=history_tail, frame=frame, dataset=dataset)


def _ensure_min_feature_rows(dataset: Any, message: str) -> Optional[TrainingResultPayload]:
    if len(dataset) >= MIN_FEATURE_ROWS:
        return None
    return _empty_ml_result(message.format(rows=len(dataset)))


def _run_training_backtest(
    seed: _TrainingSeedData,
    *,
    forecast_days: int,
    progress_callback: MlProgressCallback,
) -> Any:
    _emit_progress(progress_callback, 'ml_backtest.pending', 'Готовим rolling-origin backtesting на выбранной истории.')
    return coerce_backtest_result(
        _run_backtest(
            seed.frame,
            seed.dataset,
            progress_callback=progress_callback,
            validation_horizon_days=forecast_days,
            max_horizon_days=forecast_days,
            history_frame_is_prepared=True,
        )
    )


def _fit_final_training_models(
    seed: _TrainingSeedData,
    *,
    backtest: BacktestSuccess,
    progress_callback: MlProgressCallback,
) -> _FinalTrainingModels | TrainingResultPayload:
    final_frame, final_dataset, final_temperature_stats = _prepare_training_dataset(
        seed.frame,
        frame_is_prepared=True,
    )
    final_dataset_error = _ensure_min_feature_rows(
        final_dataset,
        'После подготовки полной обучающей выборки осталось только {rows} наблюдений: этого мало для итогового обучения ML-модели.',
    )
    if final_dataset_error is not None:
        return final_dataset_error

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
    return _FinalTrainingModels(
        final_frame=final_frame,
        final_dataset=final_dataset,
        final_temperature_stats=final_temperature_stats,
        feature_columns=feature_columns,
        backtest=backtest,
        selected_count_model_key=selected_count_model_key,
        final_count_model=final_count_model,
        final_event_model=final_event_model,
    )


def _build_feature_importance_artifacts(
    training_models: _FinalTrainingModels,
) -> _FeatureImportanceArtifacts:
    feature_importance_model_key = (
        training_models.selected_count_model_key
        if training_models.selected_count_model_key in COUNT_MODEL_KEYS
        else None
    )
    feature_importance_model = training_models.final_count_model
    if feature_importance_model is None:
        feature_importance_model_key = EXPLAINABLE_COUNT_MODEL_KEY
        feature_importance_model = _fit_count_model(
            EXPLAINABLE_COUNT_MODEL_KEY,
            training_models.final_dataset,
            feature_columns=training_models.feature_columns,
        )

    feature_importance = (
        _importance._build_feature_importance(feature_importance_model, training_models.final_dataset)
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
        and feature_importance_model_key != training_models.selected_count_model_key
    ):
        selected_label = COUNT_MODEL_LABELS.get(
            training_models.selected_count_model_key,
            training_models.selected_count_model_key,
        )
        source_label = COUNT_MODEL_LABELS.get(feature_importance_model_key, feature_importance_model_key)
        feature_importance_note = (
            f'Рабочий метод прогноза: {selected_label}. '
            f'Драйверы ниже показаны по {source_label}, потому что это объяснимая ML-модель для разбора факторов.'
        )
    return _FeatureImportanceArtifacts(
        rows=feature_importance,
        source_key=feature_importance_model_key if feature_importance else None,
        source_label=feature_importance_source_label,
        note=feature_importance_note,
    )


def _store_training_artifacts(
    cache_key: Tuple[int, Tuple[Tuple[Any, ...], ...]],
    *,
    training_models: _FinalTrainingModels,
    feature_importance: _FeatureImportanceArtifacts,
) -> _TrainingArtifacts:
    return _training_artifact_cache_store(
        cache_key,
        _TrainingArtifacts(
            final_frame=training_models.final_frame,
            final_dataset=training_models.final_dataset,
            final_temperature_stats=training_models.final_temperature_stats,
            backtest=training_models.backtest,
            final_count_model=training_models.final_count_model,
            final_event_model=training_models.final_event_model,
            selected_count_model_key=training_models.selected_count_model_key,
            feature_importance=[dict(row) for row in feature_importance.rows],
            feature_importance_source_key=feature_importance.source_key,
            feature_importance_source_label=feature_importance.source_label,
            feature_importance_note=feature_importance.note,
        ),
    )


def _train_ml_model(
    daily_history,
    forecast_days: int,
    scenario_temperature: Optional[float],
    progress_callback: MlProgressCallback = None,
 ) -> TrainingResultPayload:
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
        seed = _build_training_seed(daily_history, perf=perf)
    dataset_error = _ensure_min_feature_rows(
        seed.dataset,
        'После формирования лагов и скользящих признаков осталось только {rows} наблюдений: этого мало для корректного rolling-origin backtesting.',
    )
    if dataset_error is not None:
        return dataset_error

    backtest = _run_training_backtest(
        seed,
        forecast_days=forecast_days,
        progress_callback=progress_callback,
    )
    if not backtest.is_ready:
        _emit_progress(progress_callback, 'ml_backtest.failed', backtest.message)
        return _empty_ml_result(backtest.message)

    training_models = _fit_final_training_models(
        seed,
        backtest=backtest,
        progress_callback=progress_callback,
    )
    if isinstance(training_models, dict):
        return training_models

    _emit_progress(progress_callback, 'ml_model.running', 'Строим прогноз по будущим датам и интервалы неопределённости.')
    forecast_rows = _forecast_intervals._build_future_forecast_rows(
        frame=training_models.final_frame,
        selected_count_model_key=training_models.selected_count_model_key,
        count_model=training_models.final_count_model,
        event_model=training_models.final_event_model,
        forecast_days=forecast_days,
        scenario_temperature=scenario_temperature,
        interval_calibration=(
            training_models.backtest.prediction_interval_calibration_by_horizon.by_horizon
            or training_models.backtest.prediction_interval_calibration
        ),
        baseline_expected_count=_baseline_expected_count,
        temperature_stats=training_models.final_temperature_stats,
    )

    _emit_progress(progress_callback, 'ml_model.running', 'Оцениваем важность признаков и собираем итоговый ML-отчёт.')
    result_render_context = perf.span('result_render') if perf is not None else nullcontext()
    with result_render_context:
        feature_importance = _build_feature_importance_artifacts(training_models)
        if perf is not None:
            perf.update(
                forecast_rows=len(forecast_rows),
                feature_importance_rows=len(feature_importance.rows),
                backtest_rows=len(training_models.backtest.rows),
            )
        artifacts = _store_training_artifacts(
            artifact_cache_key,
            training_models=training_models,
            feature_importance=feature_importance,
        )
        return _assemble_training_artifacts_result(artifacts, forecast_rows)
