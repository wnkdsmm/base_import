from __future__ import annotations

from contextlib import nullcontext
from typing import Any, Sequence

from .analysis_features import _build_runtime_clustering_context
from .analysis_metrics import (
    _build_centroid_table,
    _build_cluster_profiles,
    _build_representative_rows,
    _compare_clustering_methods,
    _cluster_labels,
    _evaluate_cluster_counts,
    _run_clustering,
)
from .types import (
    ClusteringDiagnosticsResult,
    ClusteringDiagnosticsRuntimeBundle,
    ClusteringMethodRow,
    ClusteringRuntimeBundle,
    FeatureSelectionReport,
)


def _select_render_configuration(
    *,
    requested_cluster_count: int,
    cluster_count_is_explicit: bool,
    diagnostics: ClusteringDiagnosticsResult | None,
    fallback_weighting_strategy: str,
) -> ClusteringMethodRow:
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
    diagnostics: ClusteringDiagnosticsResult | None,
    *,
    cluster_count: int,
    selected_method_key: str,
) -> list[ClusteringMethodRow] | None:
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


def _run_clustering_model_bundle(
    *,
    cluster_frame: Any,
    entity_frame: Any,
    feature_selection_report: FeatureSelectionReport,
    actual_cluster_count: int,
    actual_method_key: str,
    actual_method_label: str,
    actual_algorithm_key: str,
    actual_weighting_strategy: str,
    method_comparison: Sequence[ClusteringMethodRow] | None = None,
) -> ClusteringRuntimeBundle:
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
        "pca_projection": clustering["pca_projection"],  # New key for thesis-grade 2D projection export.
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
    feature_selection_report: FeatureSelectionReport,
    requested_working_cluster_count: int,
    cluster_count_is_explicit: bool,
) -> ClusteringDiagnosticsRuntimeBundle:
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


def _run_clustering_model_stage(
    *,
    cluster_frame: Any,
    entity_frame: Any,
    feature_selection_report: FeatureSelectionReport,
    requested_working_cluster_count: int,
    cluster_count_is_explicit: bool,
    perf: Any,
) -> ClusteringDiagnosticsRuntimeBundle:
    with (perf.span("model_training") if perf is not None else nullcontext()):
        return _run_clustering_diagnostics_bundle(
            cluster_frame=cluster_frame,
            entity_frame=entity_frame,
            feature_selection_report=feature_selection_report,
            requested_working_cluster_count=requested_working_cluster_count,
            cluster_count_is_explicit=cluster_count_is_explicit,
        )
