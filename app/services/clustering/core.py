from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime

from app.perf import current_perf_trace, profiled
from app.plotly_bundle import get_plotly_bundle
from app.runtime_cache import CopyingTtlCache
from app.services.charting import build_empty_chart_bundle as _empty_chart_bundle
from typing import Any, Callable, Dict, List, Sequence, Tuple
from config.db import engine

from .analysis import (
    _build_clustering_mode_context,
    _build_runtime_clustering_context,
    _build_default_feature_selection_analysis,
    _build_centroid_table,
    _build_cluster_profiles,
    _build_notes,
    _build_representative_rows,
    _compare_clustering_methods,
    _cluster_labels,
    _evaluate_cluster_counts,
    _run_clustering,
)
from .charts import _build_diagnostics_chart, _build_distribution_chart, _build_scatter_chart
from .constants import (
    CLUSTER_COUNT_OPTIONS,
    MIN_ROWS_PER_CLUSTER,
    SAMPLE_LIMIT_OPTIONS,
    SAMPLING_STRATEGY_OPTIONS,
)
from .data import (
    _build_feature_options,
    _build_table_options,
    _load_territory_dataset,
    _parse_cluster_count,
    _parse_sample_limit,
    _parse_sampling_strategy,
    _prepare_cluster_frame,
    _resolve_selected_features,
    _resolve_selected_table,
)
from .quality import (
    _build_cluster_count_guidance,
    _build_clustering_quality_assessment,
    _empty_clustering_quality_assessment,
)
from .utils import _format_datetime, _format_integer, _format_number, _format_percent

_CLUSTERING_CACHE = CopyingTtlCache(ttl_seconds=120.0)



def clear_clustering_cache() -> None:
    _CLUSTERING_CACHE.clear()



def _normalize_clustering_cache_value(value: str) -> str:
    return str(value or "").strip()



def _build_clustering_cache_key(
    selected_table: str,
    cluster_count: int,
    sample_limit: int,
    sampling_strategy: str,
    feature_columns: Sequence[str] | None,
    cluster_count_is_explicit: bool,
) -> Tuple[str, ...]:
    return (
        selected_table,
        str(cluster_count),
        str(sample_limit),
        _normalize_clustering_cache_value(sampling_strategy),
        "manual_k" if cluster_count_is_explicit else "auto_k",
        *tuple(str(item).strip() for item in (feature_columns or []) if str(item).strip()),
    )


def _normalize_feature_columns(feature_columns: Sequence[str] | None) -> List[str]:
    return [str(item).strip() for item in (feature_columns or []) if str(item).strip()]


def _build_clustering_request_state(
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: Sequence[str] | None = None,
    cluster_count_is_explicit: bool = False,
) -> Dict[str, Any]:
    table_options = _build_table_options()
    selected_table = _resolve_selected_table(table_options, table_name)
    requested_cluster_count = _parse_cluster_count(cluster_count)
    requested_sample_limit = _parse_sample_limit(sample_limit)
    selected_sampling_strategy = _parse_sampling_strategy(sampling_strategy)
    normalized_feature_columns = _normalize_feature_columns(feature_columns)
    cache_key = _build_clustering_cache_key(
        selected_table=selected_table,
        cluster_count=requested_cluster_count,
        sample_limit=requested_sample_limit,
        sampling_strategy=selected_sampling_strategy,
        feature_columns=normalized_feature_columns,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    return {
        "table_options": table_options,
        "selected_table": selected_table,
        "cluster_count": requested_cluster_count,
        "sample_limit": requested_sample_limit,
        "sampling_strategy": selected_sampling_strategy,
        "feature_columns": normalized_feature_columns,
        "cluster_count_is_explicit": bool(cluster_count_is_explicit),
        "cache_key": cache_key,
    }


def _emit_clustering_progress(
    progress_callback: Callable[[str, str], None] | None,
    phase: str,
    message: str,
) -> None:
    if progress_callback is None:
        return
    progress_callback(phase, message)

def _select_render_configuration(
    *,
    requested_cluster_count: int,
    cluster_count_is_explicit: bool,
    diagnostics: Dict[str, Any] | None,
    fallback_weighting_strategy: str,
) -> Dict[str, Any]:
    diagnostics = diagnostics or {}
    if not cluster_count_is_explicit:
        best_configuration = diagnostics.get("best_configuration")
        if best_configuration:
            return dict(best_configuration)

    rows_by_cluster_count = diagnostics.get("method_rows_by_cluster_count") or {}
    method_rows = rows_by_cluster_count.get(int(requested_cluster_count)) or []
    selected_row = next((row for row in method_rows if row.get("is_recommended")), None)
    if selected_row is None:
        selected_row = next((row for row in method_rows if row.get("is_selected")), None)
    if selected_row is not None:
        return {**selected_row, "cluster_count": int(requested_cluster_count)}

    return {
        "cluster_count": int(requested_cluster_count),
        "method_key": f"kmeans_{fallback_weighting_strategy}",
        "algorithm_key": "kmeans",
        "method_label": "KMeans",
        "weighting_strategy": fallback_weighting_strategy,
    }


def _method_comparison_from_diagnostics(
    diagnostics: Dict[str, Any] | None,
    *,
    cluster_count: int,
    selected_method_key: str,
) -> List[Dict[str, Any]] | None:
    rows_by_cluster_count = (diagnostics or {}).get("method_rows_by_cluster_count") or {}
    method_rows = rows_by_cluster_count.get(int(cluster_count)) or []
    if not method_rows:
        return None

    comparison_rows = [dict(row) for row in method_rows]
    has_selected_row = False
    for row in comparison_rows:
        is_selected = str(row.get("method_key") or "") == selected_method_key
        row["is_selected"] = is_selected
        has_selected_row = has_selected_row or is_selected
    return comparison_rows if has_selected_row else None


def get_clustering_page_context(
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: Sequence[str] | None = None,
    cluster_count_is_explicit: bool = False,
) -> Dict[str, Any]:
    initial_data = get_clustering_data(
        table_name=table_name,
        cluster_count=cluster_count,
        sample_limit=sample_limit,
        sampling_strategy=sampling_strategy,
        feature_columns=feature_columns,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    return {
        "generated_at": _format_datetime(datetime.now()),
        "initial_data": initial_data,
        "plotly_js": get_plotly_bundle(),
        "has_data": bool(initial_data["filters"]["available_tables"]),
    }




def get_clustering_shell_context(
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: Sequence[str] | None = None,
    cluster_count_is_explicit: bool = False,
) -> Dict[str, Any]:
    table_options = _build_table_options()
    selected_table = _resolve_selected_table(table_options, table_name)
    requested_cluster_count = _parse_cluster_count(cluster_count)
    requested_sample_limit = _parse_sample_limit(sample_limit)
    selected_sampling_strategy = _parse_sampling_strategy(sampling_strategy)
    initial_data = _empty_clustering_data(
        table_options=table_options,
        selected_table=selected_table,
        cluster_count=requested_cluster_count,
        sample_limit=requested_sample_limit,
        sampling_strategy=selected_sampling_strategy,
    )
    initial_data["bootstrap_mode"] = "deferred"
    if feature_columns:
        initial_data["filters"]["feature_columns"] = [str(item).strip() for item in feature_columns if str(item).strip()]
    return {
        "generated_at": _format_datetime(datetime.now()),
        "initial_data": initial_data,
        "plotly_js": "",
        "has_data": bool(initial_data["filters"]["available_tables"]),
    }


def _prepare_clustering_feature_selection(
    *,
    base: Dict[str, Any],
    dataset: Dict[str, Any],
    normalized_feature_columns: Sequence[str],
    selected_sampling_strategy: str,
    requested_cluster_count: int,
) -> Dict[str, Any]:
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
    base: Dict[str, Any],
    dataset: Dict[str, Any],
    selection_note: str,
) -> None:
    base["notes"].extend(dataset["notes"])
    if selection_note:
        base["notes"].append(selection_note)
    if dataset["sampling_note"]:
        base["notes"].append(dataset["sampling_note"])


def _run_clustering_model_bundle(
    *,
    cluster_frame: Any,
    entity_frame: Any,
    feature_selection_report: Dict[str, Any],
    actual_cluster_count: int,
    actual_method_key: str,
    actual_method_label: str,
    actual_algorithm_key: str,
    actual_weighting_strategy: str,
    method_comparison: Sequence[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    runtime_feature_context = _build_runtime_clustering_context(
        feature_selection_report,
        method_label=actual_method_label,
        algorithm_key=actual_algorithm_key,
        weighting_strategy=actual_weighting_strategy,
    )
    clustering = _run_clustering(
        cluster_frame,
        entity_frame,
        actual_cluster_count,
        weighting_strategy=actual_weighting_strategy,
        algorithm_key=actual_algorithm_key,
        method_key=actual_method_key,
    )
    if method_comparison is None:
        method_comparison = _compare_clustering_methods(
            cluster_frame,
            entity_frame,
            actual_cluster_count,
            weighting_strategy=str(feature_selection_report.get("weighting_strategy") or ""),
            selected_method_key=actual_method_key,
        )
    else:
        method_comparison = [dict(row) for row in method_comparison]
    labels = clustering["labels"]
    cluster_labels = _cluster_labels(actual_cluster_count)
    profiles = _build_cluster_profiles(
        cluster_frame=cluster_frame,
        entity_frame=entity_frame,
        labels=labels,
        raw_centers=clustering["raw_centers"],
        cluster_labels=cluster_labels,
    )
    centroid_columns, centroid_rows = _build_centroid_table(
        cluster_frame=cluster_frame,
        entity_frame=entity_frame,
        labels=labels,
        raw_centers=clustering["raw_centers"],
        cluster_labels=cluster_labels,
        cluster_profiles=profiles,
    )
    representative_columns, representative_rows = _build_representative_rows(
        cluster_frame=cluster_frame,
        entity_frame=entity_frame,
        labels=labels,
        scaled_points=clustering["scaled_points"],
        scaled_centers=clustering["scaled_centers"],
        cluster_labels=cluster_labels,
    )
    return {
        "runtime_feature_context": runtime_feature_context,
        "clustering": clustering,
        "method_comparison": method_comparison,
        "labels": labels,
        "cluster_labels": cluster_labels,
        "profiles": profiles,
        "centroid_columns": centroid_columns,
        "centroid_rows": centroid_rows,
        "representative_columns": representative_columns,
        "representative_rows": representative_rows,
    }


def _run_clustering_diagnostics_bundle(
    *,
    cluster_frame: Any,
    entity_frame: Any,
    feature_selection_report: Dict[str, Any],
    requested_working_cluster_count: int,
    cluster_count_is_explicit: bool,
) -> Dict[str, Any]:
    weighting_strategy = str(feature_selection_report.get("weighting_strategy") or "")
    diagnostics = _evaluate_cluster_counts(
        cluster_frame,
        entity_frame,
        weighting_strategy=weighting_strategy,
    )
    render_configuration = _select_render_configuration(
        requested_cluster_count=requested_working_cluster_count,
        cluster_count_is_explicit=cluster_count_is_explicit,
        diagnostics=diagnostics,
        fallback_weighting_strategy=weighting_strategy,
    )
    actual_cluster_count = int(render_configuration.get("cluster_count") or requested_working_cluster_count)
    actual_method_key = str(render_configuration.get("method_key") or f"kmeans_{weighting_strategy}")
    actual_algorithm_key = str(render_configuration.get("algorithm_key") or "kmeans")
    actual_weighting_strategy = str(render_configuration.get("weighting_strategy") or weighting_strategy)
    method_comparison = _method_comparison_from_diagnostics(
        diagnostics,
        cluster_count=actual_cluster_count,
        selected_method_key=actual_method_key,
    )
    method_comparison_reused = method_comparison is not None
    model_bundle = _run_clustering_model_bundle(
        cluster_frame=cluster_frame,
        entity_frame=entity_frame,
        feature_selection_report=feature_selection_report,
        actual_cluster_count=actual_cluster_count,
        actual_method_key=actual_method_key,
        actual_method_label=str(render_configuration.get("method_label") or "KMeans"),
        actual_algorithm_key=actual_algorithm_key,
        actual_weighting_strategy=actual_weighting_strategy,
        method_comparison=method_comparison,
    )
    return {
        **model_bundle,
        "diagnostics": diagnostics,
        "render_configuration": render_configuration,
        "actual_cluster_count": actual_cluster_count,
        "actual_method_key": actual_method_key,
        "actual_algorithm_key": actual_algorithm_key,
        "actual_weighting_strategy": actual_weighting_strategy,
        "method_comparison_reused": method_comparison_reused,
    }


def _apply_cluster_count_guidance_to_summary(
    *,
    base: Dict[str, Any],
    summary: Dict[str, Any],
    cluster_count_guidance: Dict[str, Any],
    actual_cluster_count: int,
    requested_cluster_count: int,
    diagnostics: Dict[str, Any],
) -> None:
    base["filters"]["cluster_count"] = str(actual_cluster_count)
    summary["cluster_count_display"] = _format_integer(actual_cluster_count)
    summary["cluster_count_requested_display"] = _format_integer(requested_cluster_count)
    summary["cluster_count_note"] = str(cluster_count_guidance.get("current_note") or "")
    summary["suggested_cluster_count_label"] = str(
        cluster_count_guidance.get("suggested_label")
        or "\u0420\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0443\u0435\u043c\u044b\u0439 k"
    )
    summary["suggested_cluster_count_display"] = (
        _format_integer(cluster_count_guidance["recommended_cluster_count"])
        if cluster_count_guidance.get("recommended_cluster_count")
        else "\u2014"
    )
    summary["suggested_cluster_count_note"] = str(cluster_count_guidance.get("suggested_note") or "")
    summary["elbow_cluster_count_display"] = _format_integer(diagnostics["elbow_k"]) if diagnostics.get("elbow_k") else "\u2014"


def _load_clustering_dataset_for_request(
    *,
    selected_table: str,
    requested_sample_limit: int,
    selected_sampling_strategy: str,
    perf: Any,
    progress_callback: Callable[[str, str], None] | None,
) -> Dict[str, Any]:
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
    base: Dict[str, Any],
    dataset: Dict[str, Any],
    normalized_feature_columns: Sequence[str] | None,
    selected_sampling_strategy: str,
    requested_cluster_count: int,
    perf: Any,
    progress_callback: Callable[[str, str], None] | None,
) -> Dict[str, Any]:
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
    summary: Dict[str, Any],
    dataset: Dict[str, Any],
    selected_features: Sequence[str],
    requested_cluster_count: int,
    perf: Any,
    progress_callback: Callable[[str, str], None] | None,
) -> Dict[str, Any]:
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


def _run_clustering_model_stage(
    *,
    cluster_frame: Any,
    entity_frame: Any,
    feature_selection_report: Dict[str, Any],
    requested_working_cluster_count: int,
    cluster_count_is_explicit: bool,
    perf: Any,
) -> Dict[str, Any]:
    with (perf.span("model_training") if perf is not None else nullcontext()):
        return _run_clustering_diagnostics_bundle(
            cluster_frame=cluster_frame,
            entity_frame=entity_frame,
            feature_selection_report=feature_selection_report,
            requested_working_cluster_count=requested_working_cluster_count,
            cluster_count_is_explicit=cluster_count_is_explicit,
        )


def _build_clustering_charts_payload(
    *,
    clustering: Dict[str, Any],
    labels: Sequence[int],
    cluster_labels: Sequence[str],
    cluster_frame: Any,
    entity_frame: Any,
    diagnostics: Dict[str, Any],
    actual_cluster_count: int,
) -> Dict[str, Any]:
    return {
        "scatter": _build_scatter_chart(
            pca_points=clustering["pca_points"],
            labels=labels,
            cluster_labels=cluster_labels,
            cluster_frame=cluster_frame,
            entity_frame=entity_frame,
        ),
        "distribution": _build_distribution_chart(
            labels=labels,
            cluster_labels=cluster_labels,
            total_rows=len(cluster_frame),
            entity_frame=entity_frame,
        ),
        "diagnostics": _build_diagnostics_chart(
            rows=diagnostics["rows"],
            current_cluster_count=actual_cluster_count,
            recommended_cluster_count=diagnostics.get("best_quality_k"),
            best_silhouette_k=diagnostics.get("best_silhouette_k"),
            elbow_k=diagnostics.get("elbow_k"),
        ),
    }


def _build_clustering_model_description(
    *,
    cluster_count_guidance: Dict[str, Any],
    runtime_feature_context: Dict[str, Any],
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


def _build_clustering_success_payload(
    *,
    base: Dict[str, Any],
    summary: Dict[str, Any],
    dataset: Dict[str, Any],
    selected_features: Sequence[str],
    requested_cluster_count: int,
    requested_working_cluster_count: int,
    cluster_count_is_explicit: bool,
    cluster_count_guidance: Dict[str, Any],
    diagnostics: Dict[str, Any],
    runtime_feature_context: Dict[str, Any],
    clustering: Dict[str, Any],
    method_comparison: Sequence[Dict[str, Any]],
    actual_cluster_count: int,
    profiles: Sequence[Dict[str, Any]],
    centroid_columns: Sequence[Dict[str, Any]],
    centroid_rows: Sequence[Dict[str, Any]],
    representative_columns: Sequence[Dict[str, Any]],
    representative_rows: Sequence[Dict[str, Any]],
    labels: Sequence[int],
    cluster_labels: Sequence[str],
    cluster_frame: Any,
    entity_frame: Any,
) -> Dict[str, Any]:
    payload = {
        **base,
        "has_data": True,
        "model_description": _build_clustering_model_description(
            cluster_count_guidance=cluster_count_guidance,
            runtime_feature_context=runtime_feature_context,
        ),
        "summary": summary,
        "quality_assessment": _build_clustering_quality_assessment(
            clustering,
            method_comparison,
            actual_cluster_count,
            selected_features,
            diagnostics=diagnostics,
            support_summary=dataset.get("support_summary"),
            feature_selection_report=runtime_feature_context,
            requested_cluster_count=requested_cluster_count,
            resolved_requested_cluster_count=requested_working_cluster_count,
            cluster_count_is_explicit=cluster_count_is_explicit,
            cluster_count_guidance=cluster_count_guidance,
        ),
        "cluster_profiles": profiles,
        "centroid_columns": centroid_columns,
        "centroid_rows": centroid_rows,
        "representative_columns": representative_columns,
        "representative_rows": representative_rows,
        "charts": _build_clustering_charts_payload(
            clustering=clustering,
            labels=labels,
            cluster_labels=cluster_labels,
            cluster_frame=cluster_frame,
            entity_frame=entity_frame,
            diagnostics=diagnostics,
            actual_cluster_count=actual_cluster_count,
        ),
    }
    payload["notes"].extend(
        _build_notes(
            cluster_profiles=profiles,
            silhouette=clustering["silhouette"],
            selected_features=selected_features,
            diagnostics=diagnostics,
            total_incidents=dataset["total_incidents"],
            total_entities=dataset["total_entities"],
            sampled_entities=dataset["sampled_entities"],
            support_summary=dataset.get("support_summary"),
            stability_ari=clustering.get("stability_ari"),
            feature_selection_report=runtime_feature_context,
        )
    )
    return payload


def _load_clustering_stage(
    *,
    base: Dict[str, Any],
    selected_table: str,
    requested_sample_limit: int,
    selected_sampling_strategy: str,
    normalized_feature_columns: Sequence[str] | None,
    requested_cluster_count: int,
    perf: Any,
    progress_callback: Callable[[str, str], None] | None,
) -> Dict[str, Any]:
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
    base: Dict[str, Any],
    summary: Dict[str, Any],
    dataset: Dict[str, Any],
    feature_selection_report: Dict[str, Any],
    selected_features: Sequence[str],
    requested_cluster_count: int,
    cluster_count_is_explicit: bool,
    perf: Any,
    progress_callback: Callable[[str, str], None] | None,
) -> Dict[str, Any]:
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


def _build_clustering_quality_stage(
    *,
    base: Dict[str, Any],
    summary: Dict[str, Any],
    model_inputs: Dict[str, Any],
    model_bundle: Dict[str, Any],
    requested_cluster_count: int,
    cluster_count_is_explicit: bool,
) -> Dict[str, Any]:
    actual_cluster_count = model_bundle["actual_cluster_count"]
    diagnostics = model_bundle["diagnostics"]
    clustering = model_bundle["clustering"]
    requested_working_cluster_count = model_inputs["requested_working_cluster_count"]
    cluster_count_guidance = _build_cluster_count_guidance(
        requested_cluster_count=requested_cluster_count,
        current_cluster_count=actual_cluster_count,
        diagnostics=diagnostics,
        adjusted_requested_cluster_count=requested_working_cluster_count,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    _apply_cluster_count_guidance_to_summary(
        base=base,
        summary=summary,
        cluster_count_guidance=cluster_count_guidance,
        actual_cluster_count=actual_cluster_count,
        requested_cluster_count=requested_cluster_count,
        diagnostics=diagnostics,
    )
    summary["silhouette_display"] = _format_number(clustering["silhouette"], 3) if clustering["silhouette"] is not None else "—"
    summary["pca_variance_display"] = _format_percent(clustering["explained_variance"])
    summary["inertia_display"] = _format_number(clustering["inertia"], 2)
    if cluster_count_guidance.get("notes_message"):
        base["notes"].append(str(cluster_count_guidance["notes_message"]))
    return cluster_count_guidance


def _render_clustering_payload_stage(
    *,
    base: Dict[str, Any],
    dataset: Dict[str, Any],
    feature_selection: Dict[str, Any],
    model_inputs: Dict[str, Any],
    model_bundle: Dict[str, Any],
    cluster_count_guidance: Dict[str, Any],
    requested_cluster_count: int,
    cluster_count_is_explicit: bool,
    cache_key: Tuple[str, ...],
    perf: Any,
    progress_callback: Callable[[str, str], None] | None,
) -> Dict[str, Any]:
    _emit_clustering_progress(
        progress_callback,
        "clustering.render",
        "Подготавливаем итоговые таблицы, профили кластеров и визуализации.",
    )
    with (perf.span("payload_render") if perf is not None else nullcontext()):
        payload = _build_clustering_success_payload(
            base=base,
            summary=feature_selection["summary"],
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


@profiled("clustering", engine=engine)
def get_clustering_data(
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: Sequence[str] | None = None,
    cluster_count_is_explicit: bool = False,
    progress_callback: Callable[[str, str], None] | None = None,
) -> Dict[str, Any]:
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


def _empty_clustering_data(
    table_options: List[Dict[str, str]],
    selected_table: str,
    cluster_count: int,
    sample_limit: int,
    sampling_strategy: str,
) -> Dict[str, Any]:
    return {
        "generated_at": _format_datetime(datetime.now()),
        "has_data": False,
        "model_description": "",
        "summary": {
            "selected_table_label": selected_table or "Нет таблицы",
            "total_incidents_display": "0",
            "total_entities_display": "0",
            "sampled_entities_display": "0",
            "clustered_entities_display": "0",
            "excluded_entities_display": "0",
            "candidate_features_display": "0",
            "selected_features_display": "0",
            "cluster_count_display": _format_integer(cluster_count),
            "cluster_count_requested_display": _format_integer(cluster_count),
            "cluster_count_note": f"Сейчас основной вывод показан для k={_format_integer(cluster_count)}.",
            "suggested_cluster_count_label": "Рекомендуемый k",
            "suggested_cluster_count_display": "—",
            "suggested_cluster_count_note": "Диагностика k появится, когда хватит данных для сравнения нескольких вариантов.",
            "elbow_cluster_count_display": "—",
            "silhouette_display": "—",
            "pca_variance_display": "0%",
            "inertia_display": "0",
            "sampling_strategy_label": next(
                (item["label"] for item in SAMPLING_STRATEGY_OPTIONS if item["value"] == sampling_strategy),
                SAMPLING_STRATEGY_OPTIONS[0]["label"],
            ),
        },
        "quality_assessment": _empty_clustering_quality_assessment(),
        "cluster_profiles": [],
        "centroid_columns": [],
        "centroid_rows": [],
        "representative_columns": [],
        "representative_rows": [],
        "charts": {
            "scatter": _empty_chart_bundle(
                "Кластеры территорий на двумерной проекции",
                "Недостаточно данных, чтобы показать типы территорий на проекции главных компонент.",
            ),
            "distribution": _empty_chart_bundle(
                "Размеры кластеров по числу территорий",
                "Распределение территорий по типам появится после расчёта.",
            ),
            "diagnostics": _empty_chart_bundle(
                "Подсказка по числу кластеров",
                "Диагностика k появится, когда хватит территорий для сравнения нескольких вариантов.",
            ),
        },
        "notes": [],
        "filters": {
            "table_name": selected_table,
            "cluster_count": str(cluster_count),
            "sample_limit": str(sample_limit),
            "sampling_strategy": sampling_strategy,
            "feature_columns": [],
            "available_tables": table_options,
            "available_cluster_counts": [
                {"value": str(item), "label": f"{item} кластера" if item < 5 else f"{item} кластеров"}
                for item in CLUSTER_COUNT_OPTIONS
            ],
            "available_sample_limits": [
                {"value": str(item), "label": f"до {item} территорий"} for item in SAMPLE_LIMIT_OPTIONS
            ],
            "available_sampling_strategies": SAMPLING_STRATEGY_OPTIONS,
            "available_features": [],
        },
    }
