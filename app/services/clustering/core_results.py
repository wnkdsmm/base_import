from __future__ import annotations

from typing import Any, Dict, Sequence

from .analysis_metrics import _build_notes
from .charts import _build_diagnostics_chart, _build_distribution_chart, _build_scatter_chart
from .quality import _build_cluster_count_guidance, _build_clustering_quality_assessment
from .utils import _format_integer, _format_number, _format_percent


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


def _build_clustering_success_payload(
    *,
    base: Dict[str, Any],
    summary: Dict[str, Any],
    model_description: str,
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
    summary["silhouette_display"] = _format_number(clustering["silhouette"], 3) if clustering["silhouette"] is not None else "\u2014"
    summary["pca_variance_display"] = _format_percent(clustering["explained_variance"])
    summary["inertia_display"] = _format_number(clustering["inertia"], 2)
    if cluster_count_guidance.get("notes_message"):
        base["notes"].append(str(cluster_count_guidance["notes_message"]))
    return cluster_count_guidance
