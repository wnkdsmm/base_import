from __future__ import annotations

from collections import OrderedDict
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from app.perf import current_perf_trace, profiled

from . import forecast_intervals as _forecast_intervals
from ..backtesting import training_backtesting_execution as _backtesting
from . import training_dataset as _dataset
from . import training_importance as _importance
from . import training_models as _models
from . import training_result as _result
from . import training_temperature as _temperature
from ..caches import MLModelCaches, create_default_caches
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

_run_backtest = profiled('ml_backtest')(_backtesting._run_backtest)


@dataclass


class _TrainingArtifacts:
    final_frame: Any
    final_dataset: Any
    final_temperature_stats: TrainingTemperatureStats
    backtest: Any
    final_count_model: TrainingModelArtifact | None
    final_event_model: TrainingModelArtifact | None
    selected_count_model_key: str
    feature_importance: list[TrainingFeatureImportanceRow]
    feature_importance_source_key: str | None
    feature_importance_source_label: str | None
    feature_importance_note: str | None
    trend_warning: str | None = None


@dataclass


class _TrainingSeedData:
    history_tail: list[TrainingHistoryRecord]
    frame: Any
    dataset: Any


@dataclass


class _FinalTrainingModels:
    final_frame: Any
    final_dataset: Any
    final_temperature_stats: TrainingTemperatureStats
    feature_columns: list[str]
    backtest: Any
    selected_count_model_key: str
    final_count_model: TrainingModelArtifact | None
    final_event_model: TrainingModelArtifact | None


@dataclass


class _FeatureImportanceArtifacts:
    rows: list[TrainingFeatureImportanceRow]
    source_key: str | None
    source_label: str | None
    note: str | None


_TRAINING_ARTIFACT_CACHE_LIMIT = 32
_DEFAULT_CACHES = create_default_caches()


def clear_training_artifact_cache(caches: MLModelCaches | None = None) -> None:
    cache_set = caches or _DEFAULT_CACHES
    cache_set.artifact_cache.clear()


def _signature_date(value: Any) -> str:
    if hasattr(value, 'isoformat'):
        return str(value.isoformat())
    return str(value)


def _signature_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if numeric_value != numeric_value:
        return None
    return numeric_value


def _daily_history_signature(daily_history: list[TrainingHistoryRecord]) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        (
            _signature_date(item.get('date')),
            _signature_float(item.get('count')),
            _signature_float(item.get('avg_temperature')),
        )
        for item in daily_history[-MAX_HISTORY_POINTS:]
    )


def _training_artifact_cache_key(
    daily_history: list[TrainingHistoryRecord],
    forecast_days: int,
) -> tuple[int, tuple[tuple[Any, ...], ...]]:
    return (int(forecast_days), _daily_history_signature(daily_history))


def _training_artifact_cache_get(
    cache_key: tuple[int, tuple[tuple[Any, ...], ...]],
    caches: MLModelCaches,
) -> _TrainingArtifacts | None:
    artifacts = caches.artifact_cache.get(cache_key)
    if artifacts is not None:
        caches.artifact_cache.move_to_end(cache_key)
    return artifacts


def _training_artifact_cache_store(
    cache_key: tuple[int, tuple[tuple[Any, ...], ...]],
    artifacts: _TrainingArtifacts,
    caches: MLModelCaches,
) -> _TrainingArtifacts:
    caches.artifact_cache[cache_key] = artifacts
    caches.artifact_cache.move_to_end(cache_key)
    while len(caches.artifact_cache) > _TRAINING_ARTIFACT_CACHE_LIMIT:
        caches.artifact_cache.popitem(last=False)
    return artifacts


def _forecast_rows_from_training_artifacts(
    artifacts: _TrainingArtifacts,
    *,
    forecast_days: int,
    scenario_temperature: float | None,
) -> list[TrainingForecastRow]:
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
        baseline_expected_count=_backtesting._baseline_expected_count,
        temperature_stats=artifacts.final_temperature_stats,
    )


def _assemble_training_artifacts_result(
    artifacts: _TrainingArtifacts,
    forecast_rows: list[TrainingForecastRow],
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
        trend_warning=artifacts.trend_warning,
    )


def _build_training_seed(
    daily_history: list[TrainingHistoryRecord],
    *,
    perf: Any,
) -> _TrainingSeedData:
    history_tail = daily_history[-MAX_HISTORY_POINTS:]
    frame = _dataset._prepare_reference_frame(_dataset._build_history_frame(history_tail))
    dataset = _dataset._build_backtest_seed_dataset(frame, frame_is_prepared=True)
    if perf is not None:
        perf.update(history_points=len(history_tail), feature_rows=len(dataset))
    return _TrainingSeedData(history_tail=history_tail, frame=frame, dataset=dataset)


def _ensure_min_feature_rows(dataset: Any, message: str) -> TrainingResultPayload | None:
    if len(dataset) >= MIN_FEATURE_ROWS:
        return None
    return _result._empty_ml_result(message.format(rows=len(dataset)))


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
    final_frame, final_dataset, final_temperature_stats = _dataset._prepare_training_dataset(
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
    feature_columns = _temperature._temperature_feature_columns(final_temperature_stats)
    _emit_progress(
        progress_callback,
        'ml_model.running',
        f"Обучаем итоговую count-модель {COUNT_MODEL_LABELS.get(selected_count_model_key, selected_count_model_key)} на полной истории.",
    )
    final_count_model = (
        _models._fit_count_model(selected_count_model_key, final_dataset, feature_columns=feature_columns)
        if selected_count_model_key in COUNT_MODEL_KEYS
        else None
    )
    if selected_count_model_key in COUNT_MODEL_KEYS and final_count_model is None:
        return _result._empty_ml_result('Не удалось обучить итоговую модель по числу пожаров на полной выборке.')

    selected_event_model_key = backtest.event_metrics.selected_model_key
    classifier_validated = (
        selected_event_model_key == 'logistic_regression'
        and backtest.event_metrics.available
        and backtest.event_metrics.logistic_available
        and backtest.event_metrics.event_probability_informative
        and _models._can_train_event_model(final_dataset['event'])
    )
    final_event_model = _models._fit_event_model(final_dataset, feature_columns=feature_columns) if classifier_validated else None
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
        feature_importance_model = _models._fit_count_model(
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
    cache_key: tuple[int, tuple[tuple[Any, ...], ...]],
    *,
    training_models: _FinalTrainingModels,
    feature_importance: _FeatureImportanceArtifacts,
    caches: MLModelCaches,
    trend_warning: str | None = None,
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
            trend_warning=trend_warning,
        ),
        caches=caches,
    )


def _train_ml_model(
    daily_history,
    forecast_days: int,
    scenario_temperature: float | None,
    progress_callback: MlProgressCallback = None,
    caches: MLModelCaches | None = None,
 ) -> TrainingResultPayload:
    cache_set = caches or _DEFAULT_CACHES
    perf = current_perf_trace()
    if len(daily_history) < MIN_DAILY_HISTORY or forecast_days <= 0:
        return _result._empty_ml_result(
            f'Для ML-блока нужно минимум {MIN_DAILY_HISTORY} дней непрерывной дневной истории, чтобы выполнить rolling-origin backtesting и обучить модель.'
        )

    artifact_cache_key = _training_artifact_cache_key(daily_history, forecast_days)
    cached_artifacts = _training_artifact_cache_get(artifact_cache_key, cache_set)
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
    trend_warning = _dataset._detect_trend_warning(seed.dataset)
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
        return _result._empty_ml_result(backtest.message)

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
        baseline_expected_count=_backtesting._baseline_expected_count,
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
            caches=cache_set,
            trend_warning=trend_warning,
        )
        return _assemble_training_artifacts_result(artifacts, forecast_rows)

