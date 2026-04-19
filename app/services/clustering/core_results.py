from __future__ import annotations

from typing import Any, Dict, Sequence

from .analysis_metrics import _build_notes
from .analysis_stats import compute_cluster_risk_scores
from .charts import (
    _build_diagnostics_chart,
    _build_distribution_chart,
    _build_scatter_chart,
    build_feature_importance_chart,
    build_radar_chart,
)
from .count_guidance import _build_cluster_count_guidance
from .quality_silhouette import _build_clustering_quality_assessment
from .types import (
    ClusterCountGuidance,
    ClusterMethod,
    ClusteringDataset,
    ClusteringDiagnostics,
    ClusteringModelBundle,
    ClusteringModelInputs,
    ClusteringModelOutput,
    ClusteringPayload,
    ClusteringSummary,
    FeatureSelectionReport,
)
from .utils import _format_integer, _format_number, _format_percent


def _apply_cluster_count_guidance_to_summary(
    *,
    base: ClusteringPayload,
    summary: ClusteringSummary,
    cluster_count_guidance: ClusterCountGuidance,
    actual_cluster_count: int,
    requested_cluster_count: int,
    diagnostics: ClusteringDiagnostics,
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


def _build_clustering_charts_payload(
    *,
    clustering: ClusteringModelOutput,
    labels: Sequence[int],
    cluster_labels: Sequence[str],
    numeric_profiles: dict[int, dict[str, float]],
    cluster_frame: Any,
    entity_frame: Any,
    diagnostics: ClusteringDiagnostics,
    actual_cluster_count: int,
) -> dict[str, Any]:  # one-off structure: direct chart bundle mapped by chart ids
    feature_labels = [str(column) for column in list(getattr(cluster_frame, "columns", []))]

    return {
        "scatter": _build_scatter_chart(
            pca_points=clustering["pca_points"],
            labels=labels,
            cluster_labels=cluster_labels,
            cluster_frame=cluster_frame,
            entity_frame=entity_frame,
        ),
        "radar_chart": build_radar_chart(
            cluster_profiles=numeric_profiles,
            feature_labels=feature_labels,
        ),
        "feature_importance_chart": build_feature_importance_chart(
            cluster_profiles=numeric_profiles,
            feature_labels=feature_labels,
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


def _build_cluster_numeric_profiles(
    *,
    clustering: ClusteringModelOutput,
    feature_labels: Sequence[str],
    cluster_labels: Sequence[str],
) -> dict[int, dict[str, float]]:
    raw_centers_value = clustering.get("raw_centers")
    raw_centers = list(raw_centers_value) if raw_centers_value is not None else []
    return {
        int(cluster_id): {
            feature_name: float(raw_centers[cluster_id][feature_index])
            for feature_index, feature_name in enumerate(feature_labels)
            if cluster_id < len(raw_centers) and feature_index < len(raw_centers[cluster_id])
        }
        for cluster_id, _cluster_label in enumerate(cluster_labels)
    }


def _build_clustering_success_payload(
    *,
    base: ClusteringPayload,
    summary: ClusteringSummary,
    model_description: str,
    dataset: ClusteringDataset,
    selected_features: Sequence[str],
    requested_cluster_count: int,
    requested_working_cluster_count: int,
    cluster_count_is_explicit: bool,
    cluster_count_guidance: ClusterCountGuidance,
    diagnostics: ClusteringDiagnostics,
    runtime_feature_context: FeatureSelectionReport,
    clustering: ClusteringModelOutput,
    method_comparison: Sequence[ClusterMethod],
    actual_cluster_count: int,
    profiles: Sequence[dict[str, Any]],  # one-off structure: presentation rows from analysis_metrics
    centroid_columns: Sequence[dict[str, Any]],  # one-off structure: dynamic tabular schema
    centroid_rows: Sequence[dict[str, Any]],  # one-off structure: dynamic tabular rows
    representative_columns: Sequence[dict[str, Any]],  # one-off structure: dynamic tabular schema
    representative_rows: Sequence[dict[str, Any]],  # one-off structure: dynamic tabular rows
    labels: Sequence[int],
    cluster_labels: Sequence[str],
    cluster_frame: Any,
    entity_frame: Any,
) -> ClusteringPayload:
    feature_labels = [str(column) for column in list(getattr(cluster_frame, "columns", []))]
    numeric_profiles = _build_cluster_numeric_profiles(
        clustering=clustering,
        feature_labels=feature_labels,
        cluster_labels=cluster_labels,
    )
    cluster_risk_rows = compute_cluster_risk_scores(numeric_profiles)

    payload = {
        **base,
        "has_data": True,
        "model_description": model_description,
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
        "cluster_risk": cluster_risk_rows,
        "pca_projection": clustering.get("pca_projection", {"points": [], "explained_variance": [0.0, 0.0]}),  # New key: serialized PCA 2D points for export/API.
        "charts": _build_clustering_charts_payload(
            clustering=clustering,
            labels=labels,
            cluster_labels=cluster_labels,
            numeric_profiles=numeric_profiles,
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


def _build_clustering_quality_stage(
    *,
    base: ClusteringPayload,
    summary: ClusteringSummary,
    model_inputs: ClusteringModelInputs,
    model_bundle: ClusteringModelBundle,
    requested_cluster_count: int,
    cluster_count_is_explicit: bool,
) -> ClusterCountGuidance:
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
    summary["silhouette_display"] = _format_number(clustering["silhouette"], 3) if clustering["silhouette"] is not None else "\u2014"
    summary["pca_variance_display"] = _format_percent(clustering["explained_variance"])
    summary["inertia_display"] = _format_number(clustering["inertia"], 2)
    if cluster_count_guidance.get("notes_message"):
        base["notes"].append(str(cluster_count_guidance["notes_message"]))
    return cluster_count_guidance
