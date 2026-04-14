from __future__ import annotations

from contextlib import nullcontext
from typing import Any, Callable, Dict, Sequence

from app.services.shared.formatting import _format_integer

from .analysis_features import (
    _build_clustering_mode_context,
    _build_default_feature_selection_analysis,
)
from .constants import CLUSTER_COUNT_OPTIONS, SAMPLING_STRATEGY_OPTIONS
from .data import (
    _build_feature_options,
    _load_territory_dataset,
    _prepare_cluster_frame,
    _resolve_selected_features,
)
from .types import (
    ClusterCountGuidance,
    ClusteringBaseState,
    ClusteringDatasetBundle,
    ClusteringFeatureSelectionContext,
    ClusteringModelStageInputs,
    ClusteringSummary,
    FeatureSelectionReport,
)

def _emit_clustering_progress(
    progress_callback: Callable[[str, str], None] | None,
    phase: str,
    message: str,
) -> None:
    if progress_callback is None:
        return
    progress_callback(phase, message)

def _prepare_clustering_feature_selection(
    *,
    base: ClusteringBaseState,
    dataset: ClusteringDatasetBundle,
    normalized_feature_columns: Sequence[str],
    selected_sampling_strategy: str,
    requested_cluster_count: int,
) -> ClusteringFeatureSelectionContext:
    sampling_strategy_label = next(
        (item["label"] for item in SAMPLING_STRATEGY_OPTIONS if item["value"] == selected_sampling_strategy),
        SAMPLING_STRATEGY_OPTIONS[0]["label"],
    )
    summary = base["summary"]
    summary["total_incidents_display"] = _format_integer(dataset["total_incidents"])
    summary["total_entities_display"] = _format_integer(dataset["total_entities"])
    summary["sampled_entities_display"] = _format_integer(dataset["sampled_entities"])
    summary["sampling_strategy_label"] = sampling_strategy_label

    candidate_features = dataset["candidate_features"]
    feature_names = [item["name"] for item in candidate_features]
    feature_selection_report = None
    if normalized_feature_columns:
        selected_features, selection_note = _resolve_selected_features(
            feature_names,
            normalized_feature_columns,
            feature_frame=dataset["feature_frame"],
            entity_frame=dataset["entity_frame"],
            cluster_count=requested_cluster_count,
        )
    else:
        feature_selection_report = _build_default_feature_selection_analysis(
            feature_frame=dataset["feature_frame"],
            entity_frame=dataset["entity_frame"],
            available_features=feature_names,
            cluster_count=requested_cluster_count,
        )
        selected_features = list(feature_selection_report.get("selected_features") or [])
        selection_note = str(feature_selection_report.get("selection_note") or "")
    feature_selection_report = _build_clustering_mode_context(selected_features, feature_selection_report)
    base["filters"]["feature_columns"] = selected_features
    base["filters"]["available_features"] = _build_feature_options(candidate_features, selected_features)
    summary["candidate_features_display"] = _format_integer(len(candidate_features))
    summary["selected_features_display"] = _format_integer(len(selected_features))

    return {
        "summary": summary,
        "candidate_features": candidate_features,
        "selected_features": selected_features,
        "feature_selection_report": feature_selection_report,
        "selection_note": selection_note,
    }

def _append_clustering_feature_notes(
    base: ClusteringBaseState,
    dataset: ClusteringDatasetBundle,
    selection_note: str,
) -> None:
    base["notes"].extend(dataset["notes"])
    if selection_note:
        base["notes"].append(selection_note)
    if dataset["sampling_note"]:
        base["notes"].append(dataset["sampling_note"])

def _load_clustering_dataset_for_request(
    *,
    selected_table: str,
    requested_sample_limit: int,
    selected_sampling_strategy: str,
    perf: Any,
    progress_callback: Callable[[str, str], None] | None,
) -> ClusteringDatasetBundle:
    _emit_clustering_progress(
        progress_callback,
        "clustering.loading",
        "Загружаем территориальные данные и выбранные параметры кластеризации.",
    )
    aggregation_context = perf.span("aggregation") if perf is not None else nullcontext()
    with aggregation_context:
        dataset = _load_territory_dataset(selected_table, requested_sample_limit, selected_sampling_strategy)
        if perf is not None:
            perf.update(
                input_rows=dataset["total_incidents"],
                total_entities=dataset["total_entities"],
                sampled_entities=dataset["sampled_entities"],
            )
    return dataset

def _build_clustering_feature_context(
    *,
    base: ClusteringBaseState,
    dataset: ClusteringDatasetBundle,
    normalized_feature_columns: Sequence[str] | None,
    selected_sampling_strategy: str,
    requested_cluster_count: int,
    perf: Any,
    progress_callback: Callable[[str, str], None] | None,
) -> ClusteringFeatureSelectionContext:
    _emit_clustering_progress(
        progress_callback,
        "clustering.aggregation",
        "Собираем агрегированные признаки территории и проверяем их заполненность.",
    )
    filter_prep_context = perf.span("filter_prep") if perf is not None else nullcontext()
    with filter_prep_context:
        feature_selection = _prepare_clustering_feature_selection(
            base=base,
            dataset=dataset,
            normalized_feature_columns=normalized_feature_columns,
            selected_sampling_strategy=selected_sampling_strategy,
            requested_cluster_count=requested_cluster_count,
        )
        if perf is not None:
            perf.update(
                candidate_features=len(feature_selection["candidate_features"]),
                selected_features=len(feature_selection["selected_features"]),
            )
    _append_clustering_feature_notes(base, dataset, feature_selection["selection_note"])
    return feature_selection

def _build_clustering_model_inputs(
    *,
    summary: ClusteringSummary,
    dataset: ClusteringDatasetBundle,
    selected_features: Sequence[str],
    requested_cluster_count: int,
    perf: Any,
    progress_callback: Callable[[str, str], None] | None,
) -> ClusteringModelStageInputs:
    _emit_clustering_progress(
        progress_callback,
        "clustering.training",
        "Строим кластеры, сравниваем алгоритмы и оцениваем устойчивость сегментации.",
    )
    clustering_run_context = perf.span("model_training") if perf is not None else nullcontext()
    with clustering_run_context:
        cluster_frame, entity_frame, excluded_entities = _prepare_cluster_frame(
            feature_frame=dataset["feature_frame"],
            entity_frame=dataset["entity_frame"],
            selected_features=selected_features,
        )
        summary["clustered_entities_display"] = _format_integer(len(cluster_frame))
        summary["excluded_entities_display"] = _format_integer(excluded_entities)

    requested_working_cluster_count = min(max(CLUSTER_COUNT_OPTIONS[0], requested_cluster_count), len(cluster_frame) - 1)
    actual_cluster_count = requested_working_cluster_count
    return {
        "cluster_frame": cluster_frame,
        "entity_frame": entity_frame,
        "excluded_entities": excluded_entities,
        "requested_working_cluster_count": requested_working_cluster_count,
        "actual_cluster_count": actual_cluster_count,
    }

def _build_clustering_model_description(
    *,
    cluster_count_guidance: ClusterCountGuidance,
    runtime_feature_context: FeatureSelectionReport,
) -> str:
    return " ".join(
        part
        for part in [
            "Кластеризация строится не по отдельным инцидентам, а по агрегированным территориям и населённым пунктам.",
            "Для территорий с короткой историей долевые признаки считаются через empirical Bayes shrinkage к глобальному среднему.",
            str(cluster_count_guidance.get("model_note") or ""),
            str(runtime_feature_context.get("weighting_note") or ""),
            str(runtime_feature_context.get("volume_note") or ""),
        ]
        if part
    )
