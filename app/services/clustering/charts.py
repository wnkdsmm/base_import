from __future__ import annotations

from typing import Any, Dict, Sequence

from app.services.charting import (
    build_clustering_diagnostics_chart,
    build_clustering_distribution_chart,
    build_clustering_scatter_chart,
)


def _build_scatter_chart(
    pca_points: Any,
    labels: Any,
    cluster_labels: Sequence[str],
    cluster_frame: Any,
    entity_frame: Any,
) -> Dict[str, Any]:
    return build_clustering_scatter_chart(pca_points, labels, cluster_labels, cluster_frame, entity_frame)


def _build_distribution_chart(
    labels: Any,
    cluster_labels: Sequence[str],
    total_rows: int,
    entity_frame: Any,
) -> Dict[str, Any]:
    return build_clustering_distribution_chart(labels, cluster_labels, total_rows, entity_frame)


def _build_diagnostics_chart(
    rows: Sequence[Dict[str, Any]],
    current_cluster_count: int,
    recommended_cluster_count: int | None,
    best_silhouette_k: int | None,
    elbow_k: int | None,
) -> Dict[str, Any]:
    return build_clustering_diagnostics_chart(
        rows,
        current_cluster_count,
        recommended_cluster_count,
        best_silhouette_k,
        elbow_k,
    )
