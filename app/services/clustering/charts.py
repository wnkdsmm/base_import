from __future__ import annotations

from typing import Any, Dict, Sequence

from app.services.charting import (
    build_reference_annotations,
)
from app.statistics_constants import PLOTLY_PALETTE

from .charts_impl import (
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


def _diagnostic_annotations(
    rows: Sequence[Dict[str, Any]],
    current_cluster_count: int,
    recommended_cluster_count: int | None,
    best_silhouette_k: int | None,
    elbow_k: int | None,
) -> list[Dict[str, Any]]:
    top_silhouette = max((item.get("silhouette") for item in rows if item.get("silhouette") is not None), default=0.0)
    y_anchor = max(0.05, float(top_silhouette))
    return build_reference_annotations(
        y_value=y_anchor,
        references=[
            (current_cluster_count, "Р Р°Р±РѕС‡РёР№ k", PLOTLY_PALETTE["sky"]),
            (recommended_cluster_count, "Р РµРєРѕРјРµРЅРґСѓРµРјС‹Р№ k", PLOTLY_PALETTE["sand"]),
            (best_silhouette_k, "Р›СѓС‡С€РёР№ silhouette", PLOTLY_PALETTE["forest"]),
            (elbow_k, "Р›РѕРєРѕС‚СЊ", PLOTLY_PALETTE["fire"]),
        ],
    )
