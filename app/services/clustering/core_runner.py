from __future__ import annotations

from contextlib import nullcontext
from typing import Any, Callable, Dict, Sequence, Tuple

from app.perf import current_perf_trace, profiled
from config.db import engine

from .constants import MIN_ROWS_PER_CLUSTER
from .core_algorithms import _run_clustering_model_stage
from .core_context import (
    _CLUSTERING_CACHE,
    _build_clustering_cache_key,
    _build_clustering_request_state,
    _empty_clustering_data,
    _normalize_clustering_cache_value,
    _normalize_feature_columns,
    clear_clustering_cache,
    get_clustering_page_context,
    get_clustering_shell_context,
)
from .core_dataset import (
    _append_clustering_feature_notes,
    _build_clustering_feature_context,
    _build_clustering_model_description,
    _build_clustering_model_inputs,
    _emit_clustering_progress,
    _load_clustering_dataset_for_request,
    _prepare_clustering_feature_selection,
)
from .core_results import _build_clustering_quality_stage, _build_clustering_success_payload
from .types import (
    ClusterCountGuidance,
    ClusteringBaseState,
    ClusteringDatasetBundle,
    ClusteringFeatureSelectionContext,
    ClusteringLoadStageResult,
    ClusteringModelBundle,
    ClusteringModelStageInputs,
    ClusteringModelStageResult,
    ClusteringPayload,
    ClusteringSummary,
    FeatureSelectionReport,
)

def _load_clustering_stage(
    *,
    base: ClusteringBaseState,
    selected_table: str,
    requested_sample_limit: int,
    selected_sampling_strategy: str,
    normalized_feature_columns: Sequence[str] | None,
    requested_cluster_count: int,
    perf: Any,
    progress_callback: Callable[[str, str], None] | None,
) -> ClusteringLoadStageResult:
    try:
        dataset = _load_clustering_dataset_for_request(
            selected_table=selected_table,
            requested_sample_limit=requested_sample_limit,
            selected_sampling_strategy=selected_sampling_strategy,
            perf=perf,
            progress_callback=progress_callback,
        )
    except Exception as exc:
        base["notes"].append(str(exc))
        return {"error_payload": base}

    feature_selection = _build_clustering_feature_context(
        base=base,
        dataset=dataset,
        normalized_feature_columns=normalized_feature_columns,
        selected_sampling_strategy=selected_sampling_strategy,
        requested_cluster_count=requested_cluster_count,
        perf=perf,
        progress_callback=progress_callback,
    )
    candidate_features = feature_selection["candidate_features"]
    selected_features = feature_selection["selected_features"]

    if len(candidate_features) < 2:
        base["notes"].append("В таблице не хватило агрегированных признаков с вариативностью, чтобы стабильно кластеризовать территории.")
        _emit_clustering_progress(
            progress_callback,
            "clustering.completed",
            "После агрегации оказалось недостаточно вариативных признаков для устойчивой кластеризации.",
        )
        return {"error_payload": base}

    if len(selected_features) < 2:
        base["notes"].append("Для кластеризации нужно выбрать минимум два агрегированных признака территории.")
        _emit_clustering_progress(
            progress_callback,
            "clustering.completed",
            "Для расчета нужно минимум два агрегированных признака, поэтому результат остался в placeholder-режиме.",
        )
        return {"error_payload": base}

    return {
        "dataset": dataset,
        "feature_selection": feature_selection,
    }

def _build_clustering_model_stage_context(
    *,
    base: ClusteringBaseState,
    summary: ClusteringSummary,
    dataset: ClusteringDatasetBundle,
    feature_selection_report: FeatureSelectionReport,
    selected_features: Sequence[str],
    requested_cluster_count: int,
    cluster_count_is_explicit: bool,
    perf: Any,
    progress_callback: Callable[[str, str], None] | None,
) -> ClusteringModelStageResult:
    model_inputs = _build_clustering_model_inputs(
        summary=summary,
        dataset=dataset,
        selected_features=selected_features,
        requested_cluster_count=requested_cluster_count,
        perf=perf,
        progress_callback=progress_callback,
    )
    cluster_frame = model_inputs["cluster_frame"]
    entity_frame = model_inputs["entity_frame"]
    excluded_entities = model_inputs["excluded_entities"]

    if len(cluster_frame) < max(12, requested_cluster_count * MIN_ROWS_PER_CLUSTER):
        base["notes"].append("После отбора и заполнения пропусков осталось слишком мало территорий для устойчивой кластеризации.")
        _emit_clustering_progress(
            progress_callback,
            "clustering.completed",
            "После подготовки выборки осталось слишком мало территорий для устойчивой кластеризации.",
        )
        return {"error_payload": base}

    requested_working_cluster_count = model_inputs["requested_working_cluster_count"]
    actual_cluster_count = model_inputs["actual_cluster_count"]
    if requested_working_cluster_count != requested_cluster_count:
        base["notes"].append(
            f"Количество кластеров автоматически скорректировано до {actual_cluster_count}, чтобы в каждом типе территорий было достаточно наблюдений."
        )

    model_bundle = _run_clustering_model_stage(
        cluster_frame=cluster_frame,
        entity_frame=entity_frame,
        feature_selection_report=feature_selection_report,
        requested_working_cluster_count=requested_working_cluster_count,
        cluster_count_is_explicit=cluster_count_is_explicit,
        perf=perf,
    )
    if perf is not None:
        perf.update(
            clustered_entities=len(cluster_frame),
            excluded_entities=excluded_entities,
            actual_cluster_count=model_bundle["actual_cluster_count"],
            actual_method_key=model_bundle["actual_method_key"],
            method_comparison_reused=bool(model_bundle.get("method_comparison_reused")),
        )
    return {
        "model_inputs": model_inputs,
        "model_bundle": model_bundle,
    }

def _render_clustering_payload_stage(
    *,
    base: ClusteringBaseState,
    dataset: ClusteringDatasetBundle,
    feature_selection: ClusteringFeatureSelectionContext,
    model_inputs: ClusteringModelStageInputs,
    model_bundle: ClusteringModelBundle,
    cluster_count_guidance: ClusterCountGuidance,
    requested_cluster_count: int,
    cluster_count_is_explicit: bool,
    cache_key: Tuple[str, ...],
    perf: Any,
    progress_callback: Callable[[str, str], None] | None,
) -> ClusteringPayload:
    _emit_clustering_progress(
        progress_callback,
        "clustering.render",
        "Подготавливаем итоговые таблицы, профили кластеров и визуализации.",
    )
    with (perf.span("payload_render") if perf is not None else nullcontext()):
        payload = _build_clustering_success_payload(
            base=base,
            summary=feature_selection["summary"],
            model_description=_build_clustering_model_description(
                cluster_count_guidance=cluster_count_guidance,
                runtime_feature_context=model_bundle["runtime_feature_context"],
            ),
            dataset=dataset,
            selected_features=feature_selection["selected_features"],
            requested_cluster_count=requested_cluster_count,
            requested_working_cluster_count=model_inputs["requested_working_cluster_count"],
            cluster_count_is_explicit=cluster_count_is_explicit,
            cluster_count_guidance=cluster_count_guidance,
            diagnostics=model_bundle["diagnostics"],
            runtime_feature_context=model_bundle["runtime_feature_context"],
            clustering=model_bundle["clustering"],
            method_comparison=model_bundle["method_comparison"],
            actual_cluster_count=model_bundle["actual_cluster_count"],
            profiles=model_bundle["profiles"],
            centroid_columns=model_bundle["centroid_columns"],
            centroid_rows=model_bundle["centroid_rows"],
            representative_columns=model_bundle["representative_columns"],
            representative_rows=model_bundle["representative_rows"],
            labels=model_bundle["labels"],
            cluster_labels=model_bundle["cluster_labels"],
            cluster_frame=model_inputs["cluster_frame"],
            entity_frame=model_inputs["entity_frame"],
        )
        if perf is not None:
            perf.update(
                payload_has_data=bool(payload.get("has_data")),
                payload_notes=len(payload.get("notes") or []),
            )
        result = _CLUSTERING_CACHE.set(cache_key, payload)
        _emit_clustering_progress(
            progress_callback,
            "clustering.completed",
            "Кластеризация завершена, результаты и графики готовы.",
        )
        return result

def get_clustering_data(
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: Sequence[str] | None = None,
    cluster_count_is_explicit: bool = False,
    progress_callback: Callable[[str, str], None] | None = None,
) -> ClusteringPayload:
    perf = current_perf_trace()
    request_state = _build_clustering_request_state(
        table_name=table_name,
        cluster_count=cluster_count,
        sample_limit=sample_limit,
        sampling_strategy=sampling_strategy,
        feature_columns=feature_columns,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    table_options = request_state["table_options"]
    selected_table = request_state["selected_table"]
    requested_cluster_count = request_state["cluster_count"]
    requested_sample_limit = request_state["sample_limit"]
    selected_sampling_strategy = request_state["sampling_strategy"]
    normalized_feature_columns = request_state["feature_columns"]
    cluster_count_is_explicit = request_state["cluster_count_is_explicit"]
    cache_key = request_state["cache_key"]
    if perf is not None:
        perf.update(
            requested_table=table_name,
            selected_table=selected_table,
            cluster_count=requested_cluster_count,
            sample_limit=requested_sample_limit,
            sampling_strategy=selected_sampling_strategy,
        )
    cached_payload = _CLUSTERING_CACHE.get(cache_key)
    if cached_payload is not None:
        if perf is not None:
            perf.update(cache_hit=True, payload_has_data=bool(cached_payload.get("has_data")))
        _emit_clustering_progress(
            progress_callback,
            "clustering.completed",
            "Кластеризация уже была рассчитана ранее и взята из кэша.",
        )
        return cached_payload

    if perf is not None:
        perf.update(cache_hit=False)
    base = _empty_clustering_data(
        table_options=table_options,
        selected_table=selected_table,
        cluster_count=requested_cluster_count,
        sample_limit=requested_sample_limit,
        sampling_strategy=selected_sampling_strategy,
    )
    if not selected_table:
        base["notes"].append("Выберите таблицу, чтобы сгруппировать территории по их пожарному профилю и типу риска.")
        _emit_clustering_progress(
            progress_callback,
            "clustering.completed",
            "Таблица не выбрана, поэтому расчет кластеризации не запускался.",
        )
        return _CLUSTERING_CACHE.set(cache_key, base)

    load_stage = _load_clustering_stage(
        base=base,
        selected_table=selected_table,
        requested_sample_limit=requested_sample_limit,
        selected_sampling_strategy=selected_sampling_strategy,
        normalized_feature_columns=normalized_feature_columns,
        requested_cluster_count=requested_cluster_count,
        perf=perf,
        progress_callback=progress_callback,
    )
    if load_stage.get("error_payload") is not None:
        return _CLUSTERING_CACHE.set(cache_key, load_stage["error_payload"])
    dataset = load_stage["dataset"]
    feature_selection = load_stage["feature_selection"]
    summary = feature_selection["summary"]
    selected_features = feature_selection["selected_features"]
    feature_selection_report = feature_selection["feature_selection_report"]

    model_stage = _build_clustering_model_stage_context(
        base=base,
        summary=summary,
        dataset=dataset,
        feature_selection_report=feature_selection_report,
        selected_features=selected_features,
        requested_cluster_count=requested_cluster_count,
        cluster_count_is_explicit=cluster_count_is_explicit,
        perf=perf,
        progress_callback=progress_callback,
    )
    if model_stage.get("error_payload") is not None:
        return _CLUSTERING_CACHE.set(cache_key, model_stage["error_payload"])
    model_inputs = model_stage["model_inputs"]
    model_bundle = model_stage["model_bundle"]

    cluster_count_guidance = _build_clustering_quality_stage(
        base=base,
        summary=summary,
        model_inputs=model_inputs,
        model_bundle=model_bundle,
        requested_cluster_count=requested_cluster_count,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    return _render_clustering_payload_stage(
        base=base,
        dataset=dataset,
        feature_selection=feature_selection,
        model_inputs=model_inputs,
        model_bundle=model_bundle,
        cluster_count_guidance=cluster_count_guidance,
        requested_cluster_count=requested_cluster_count,
        cluster_count_is_explicit=cluster_count_is_explicit,
        cache_key=cache_key,
        perf=perf,
        progress_callback=progress_callback,
    )
