from __future__ import annotations

from typing import Any, Dict, Sequence

import numpy as np
import pandas as pd

from app.plotly_bundle import PLOTLY_AVAILABLE, go
from app.services.charting import (
    build_chart_bundle,
    build_empty_chart_bundle as _empty_chart_bundle,
    build_plotly_unavailable_chart_bundle,
    build_plotly_bar_trace,
    build_plotly_line,
    build_plotly_marker,
    build_plotly_scatter_trace,
    build_plotly_scattergl_trace,
)
from app.services.chart_utils import (
    build_horizontal_legend,
    build_plotly_palette,
    build_reference_annotations,
    build_service_plotly_layout,
    build_unique_vertical_reference_lines,
    merge_plotly_layout,
    plotly_layout,
)
from app.statistics_constants import PLOTLY_PALETTE

from .utils import _format_integer, _format_number, _format_percent


def _build_scatter_chart(
    pca_points: np.ndarray,
    labels: np.ndarray,
    cluster_labels: Sequence[str],
    cluster_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
) -> Dict[str, Any]:
    title = "Р С™Р В»Р В°РЎРѓРЎвЂљР ВµРЎР‚РЎвЂ№ РЎвЂљР ВµРЎР‚РЎР‚Р С‘РЎвЂљР С•РЎР‚Р С‘Р в„– Р Р…Р В° Р Т‘Р Р†РЎС“Р СР ВµРЎР‚Р Р…Р С•Р в„– Р С—РЎР‚Р С•Р ВµР С”РЎвЂ Р С‘Р С‘"
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(title, "Plotly Р Р…Р ВµР Т‘Р С•РЎРѓРЎвЂљРЎС“Р С—Р ВµР Р…, Р С—Р С•РЎРЊРЎвЂљР С•Р СРЎС“ Р С–РЎР‚Р В°РЎвЂћР С‘Р С” Р С”Р В»Р В°РЎРѓРЎвЂљР ВµРЎР‚Р С•Р Р† Р Р…Р Вµ Р С—Р С•РЎРѓРЎвЂљРЎР‚Р С•Р ВµР Р….")

    figure = go.Figure()
    palette = build_plotly_palette(
        ["sky", "forest", "sand", "fire", "sky_soft", "forest_soft"],
    )
    preview_columns = list(cluster_frame.columns[:4])

    for cluster_id, cluster_label in enumerate(cluster_labels):
        mask = labels == cluster_id
        hover_texts = []
        for row_index in np.where(mask)[0]:
            entity_row = entity_frame.iloc[row_index]
            details = [
                f"<b>{entity_row.get('Р СћР ВµРЎР‚РЎР‚Р С‘РЎвЂљР С•РЎР‚Р С‘РЎРЏ', 'Р СћР ВµРЎР‚РЎР‚Р С‘РЎвЂљР С•РЎР‚Р С‘РЎРЏ')}</b>",
                f"Р В Р В°Р в„–Р С•Р Р…: {entity_row.get('Р В Р В°Р в„–Р С•Р Р…', 'РІР‚вЂќ')}",
                f"Р С™Р С•Р Р…РЎвЂљР ВµР С”РЎРѓРЎвЂљ: {entity_row.get('Р СћР С‘Р С— РЎвЂљР ВµРЎР‚РЎР‚Р С‘РЎвЂљР С•РЎР‚Р С‘Р С‘', 'РІР‚вЂќ')}",
            ]
            details.extend(
                f"{column}: {_format_metric(column, cluster_frame.iloc[row_index][column])}" for column in preview_columns
            )
            hover_texts.append("<br>".join(details))

        figure.add_trace(
            build_plotly_scattergl_trace(
                x=pca_points[mask, 0],
                y=pca_points[mask, 1],
                mode="markers",
                name=cluster_label,
                text=hover_texts,
                hovertemplate="%{text}<extra></extra>",
                marker=build_plotly_marker(
                    color=palette[cluster_id % len(palette)],
                    size=10,
                    line_color="rgba(255,255,255,0.6)",
                    line_width=0.6,
                    opacity=0.88,
                ),
            )
        )

    figure.update_layout(
        **merge_plotly_layout(
            build_service_plotly_layout("", height=420),
            xaxis={"title": "Р С™Р С•Р СР С—Р С•Р Р…Р ВµР Р…РЎвЂљР В° 1 (PCA)", "showgrid": False, "zeroline": False},
            yaxis={"title": "Р С™Р С•Р СР С—Р С•Р Р…Р ВµР Р…РЎвЂљР В° 2 (PCA)", "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False},
            legend=build_horizontal_legend(y=1.1),
        )
    )
    return build_chart_bundle(title, figure)


def _build_distribution_chart(
    labels: np.ndarray,
    cluster_labels: Sequence[str],
    total_rows: int,
    entity_frame: pd.DataFrame,
) -> Dict[str, Any]:
    title = "Р В Р В°Р В·Р СР ВµРЎР‚РЎвЂ№ Р С”Р В»Р В°РЎРѓРЎвЂљР ВµРЎР‚Р С•Р Р† Р С—Р С• РЎвЂЎР С‘РЎРѓР В»РЎС“ РЎвЂљР ВµРЎР‚РЎР‚Р С‘РЎвЂљР С•РЎР‚Р С‘Р в„–"
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(title, "Plotly Р Р…Р ВµР Т‘Р С•РЎРѓРЎвЂљРЎС“Р С—Р ВµР Р…, Р С—Р С•РЎРЊРЎвЂљР С•Р СРЎС“ РЎР‚Р В°РЎРѓР С—РЎР‚Р ВµР Т‘Р ВµР В»Р ВµР Р…Р С‘Р Вµ Р С”Р В»Р В°РЎРѓРЎвЂљР ВµРЎР‚Р С•Р Р† Р Р…Р Вµ Р С—Р С•РЎРѓРЎвЂљРЎР‚Р С•Р ВµР Р…Р С•.")

    counts = [int(np.sum(labels == cluster_id)) for cluster_id in range(len(cluster_labels))]
    shares = [count / total_rows if total_rows else 0.0 for count in counts]
    fire_totals = [
        int(entity_frame.loc[labels == cluster_id, "Р В§Р С‘РЎРѓР В»Р С• Р С—Р С•Р В¶Р В°РЎР‚Р С•Р Р†"].sum()) if "Р В§Р С‘РЎРѓР В»Р С• Р С—Р С•Р В¶Р В°РЎР‚Р С•Р Р†" in entity_frame.columns else 0
        for cluster_id in range(len(cluster_labels))
    ]
    colors = build_plotly_palette(
        ["sky", "forest", "sand", "fire", "sky_soft", "forest_soft"],
        limit=len(cluster_labels),
    )

    figure = go.Figure(
        data=[
            build_plotly_bar_trace(
                x=list(cluster_labels),
                y=counts,
                text=[_format_percent(share) for share in shares],
                textposition="outside",
                marker={"color": colors[: len(cluster_labels)]},
                customdata=fire_totals,
                hovertemplate="<b>%{x}</b><br>Р СћР ВµРЎР‚РЎР‚Р С‘РЎвЂљР С•РЎР‚Р С‘Р в„–: %{y}<br>Р вЂќР С•Р В»РЎРЏ: %{text}<br>Р СџР С•Р В¶Р В°РЎР‚Р С•Р Р† Р Р† Р С‘РЎРѓРЎвЂљР С•РЎР‚Р С‘Р С‘: %{customdata}<extra></extra>",
            )
        ]
    )
    figure.update_layout(
        **merge_plotly_layout(
            plotly_layout("Р СћР ВµРЎР‚РЎР‚Р С‘РЎвЂљР С•РЎР‚Р С‘Р в„–", height=340),
            updates={"showlegend": False},
        )
    )
    return build_chart_bundle(title, figure)


def _build_diagnostics_chart(
    rows: Sequence[Dict[str, Any]],
    current_cluster_count: int,
    recommended_cluster_count: int | None,
    best_silhouette_k: int | None,
    elbow_k: int | None,
) -> Dict[str, Any]:
    title = "Р СџР С•Р Т‘РЎРѓР С”Р В°Р В·Р С”Р В° Р С—Р С• РЎвЂЎР С‘РЎРѓР В»РЎС“ Р С”Р В»Р В°РЎРѓРЎвЂљР ВµРЎР‚Р С•Р Р†"
    if not rows:
        return _empty_chart_bundle(title, "Р СњР ВµР Т‘Р С•РЎРѓРЎвЂљР В°РЎвЂљР С•РЎвЂЎР Р…Р С• РЎвЂљР ВµРЎР‚РЎР‚Р С‘РЎвЂљР С•РЎР‚Р С‘Р в„–, РЎвЂЎРЎвЂљР С•Р В±РЎвЂ№ РЎРѓРЎР‚Р В°Р Р†Р Р…Р С‘РЎвЂљРЎРЉ Р С”Р С•РЎРЊРЎвЂћРЎвЂћР С‘РЎвЂ Р С‘Р ВµР Р…РЎвЂљ РЎРѓР С‘Р В»РЎС“РЎРЊРЎвЂљР В° Р С‘ Р СР ВµРЎвЂљР С•Р Т‘ Р В»Р С•Р С”РЎвЂљРЎРЏ Р С—Р С• Р Р…Р ВµРЎРѓР С”Р С•Р В»РЎРЉР С”Р С‘Р С Р В·Р Р…Р В°РЎвЂЎР ВµР Р…Р С‘РЎРЏР С k.")
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(title, "Plotly Р Р…Р ВµР Т‘Р С•РЎРѓРЎвЂљРЎС“Р С—Р ВµР Р…, Р С—Р С•РЎРЊРЎвЂљР С•Р СРЎС“ Р Т‘Р С‘Р В°Р С–Р Р…Р С•РЎРѓРЎвЂљР С‘РЎвЂЎР ВµРЎРѓР С”Р С‘Р в„– Р С–РЎР‚Р В°РЎвЂћР С‘Р С” Р Р…Р Вµ Р С—Р С•РЎРѓРЎвЂљРЎР‚Р С•Р ВµР Р….")

    x = [item["cluster_count"] for item in rows]
    silhouette_values = [item["silhouette"] for item in rows]
    inertia_values = [item["inertia"] for item in rows]
    top_silhouette = max((item["silhouette"] for item in rows if item.get("silhouette") is not None), default=0.0)
    y_anchor = max(0.05, top_silhouette) if top_silhouette is not None else 0.05

    figure = go.Figure()
    figure.add_trace(
        build_plotly_scatter_trace(
            x=x,
            y=silhouette_values,
            mode="lines+markers",
            name="Р С™Р С•РЎРЊРЎвЂћРЎвЂћР С‘РЎвЂ Р С‘Р ВµР Р…РЎвЂљ РЎРѓР С‘Р В»РЎС“РЎРЊРЎвЂљР В°",
            marker=build_plotly_marker(color=PLOTLY_PALETTE["forest"], size=8),
            line=build_plotly_line(color=PLOTLY_PALETTE["forest"], width=2),
            hovertemplate="k=%{x}<br>Р С™Р С•РЎРЊРЎвЂћРЎвЂћР С‘РЎвЂ Р С‘Р ВµР Р…РЎвЂљ РЎРѓР С‘Р В»РЎС“РЎРЊРЎвЂљР В°=%{y:.3f}<extra></extra>",
        )
    )
    figure.add_trace(
        build_plotly_scatter_trace(
            x=x,
            y=inertia_values,
            mode="lines+markers",
            name="Р ВР Р…Р ВµРЎР‚РЎвЂ Р С‘РЎРЏ",
            yaxis="y2",
            marker=build_plotly_marker(color=PLOTLY_PALETTE["fire"], size=8),
            line=build_plotly_line(color=PLOTLY_PALETTE["fire"], width=2),
            hovertemplate="k=%{x}<br>Р ВР Р…Р ВµРЎР‚РЎвЂ Р С‘РЎРЏ=%{y:.2f}<extra></extra>",
        )
    )

    figure.update_layout(
        **merge_plotly_layout(
            plotly_layout("Р С™Р С•РЎРЊРЎвЂћРЎвЂћР С‘РЎвЂ Р С‘Р ВµР Р…РЎвЂљ РЎРѓР С‘Р В»РЎС“РЎРЊРЎвЂљР В°", height=340),
            xaxis={"title": "Р В§Р С‘РЎРѓР В»Р С• Р С”Р В»Р В°РЎРѓРЎвЂљР ВµРЎР‚Р С•Р Р†", "tickmode": "array", "tickvals": x},
            yaxis={"title": "Р С™Р С•РЎРЊРЎвЂћРЎвЂћР С‘РЎвЂ Р С‘Р ВµР Р…РЎвЂљ РЎРѓР С‘Р В»РЎС“РЎРЊРЎвЂљР В°", "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False},
            legend=build_horizontal_legend(y=1.12),
            updates={
                "yaxis2": {"title": "Р ВР Р…Р ВµРЎР‚РЎвЂ Р С‘РЎРЏ", "overlaying": "y", "side": "right", "showgrid": False},
                "shapes": build_unique_vertical_reference_lines(
                    [
                        (current_cluster_count, PLOTLY_PALETTE["sky"]),
                        (recommended_cluster_count, PLOTLY_PALETTE["sand"]),
                        (best_silhouette_k, PLOTLY_PALETTE["forest"]),
                        (elbow_k, PLOTLY_PALETTE["fire"]),
                    ]
                ),
                "annotations": build_reference_annotations(
                    y_value=y_anchor,
                    references=[
                        (current_cluster_count, "Р Р°Р±РѕС‡РёР№ k", PLOTLY_PALETTE["sky"]),
                        (recommended_cluster_count, "Р РµРєРѕРјРµРЅРґСѓРµРјС‹Р№ k", PLOTLY_PALETTE["sand"]),
                        (best_silhouette_k, "Р›СѓС‡С€РёР№ silhouette", PLOTLY_PALETTE["forest"]),
                        (elbow_k, "Р›РѕРєРѕС‚СЊ", PLOTLY_PALETTE["fire"]),
                    ],
                ),
            },
        )
    )
    return build_chart_bundle(title, figure)


def _format_metric(column_name: str, value: Any) -> str:
    if column_name.startswith("Р вЂќР С•Р В»РЎРЏ") or column_name.startswith("Р СџР С•Р С”РЎР‚РЎвЂ№РЎвЂљР С‘Р Вµ"):
        return _format_percent(float(value))
    return _format_number(value, 2)


def build_clustering_scatter_chart(
    pca_points: Any,
    labels: Any,
    cluster_labels: Sequence[str],
    cluster_frame: Any,
    entity_frame: Any,
) -> Dict[str, Any]:
    return _build_scatter_chart(pca_points, labels, cluster_labels, cluster_frame, entity_frame)


def build_clustering_distribution_chart(
    labels: Any,
    cluster_labels: Sequence[str],
    total_rows: int,
    entity_frame: Any,
) -> Dict[str, Any]:
    return _build_distribution_chart(labels, cluster_labels, total_rows, entity_frame)


def build_clustering_diagnostics_chart(
    rows: Sequence[Dict[str, Any]],
    current_cluster_count: int,
    recommended_cluster_count: int | None,
    best_silhouette_k: int | None,
    elbow_k: int | None,
) -> Dict[str, Any]:
    return _build_diagnostics_chart(
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


