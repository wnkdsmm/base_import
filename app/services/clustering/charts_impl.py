from __future__ import annotations

from typing import Any, Dict, Sequence

import numpy as np
import pandas as pd

from app.plotly_bundle import PLOTLY_AVAILABLE, go
from app.services.charting import (
    build_chart_bundle,
    build_empty_chart_bundle as _empty_chart_bundle,
    build_horizontal_legend,
    build_plotly_bar_trace,
    build_plotly_line,
    build_plotly_marker,
    build_plotly_palette,
    build_reference_annotations,
    build_plotly_scatter_trace,
    build_plotly_scattergl_trace,
    build_service_plotly_layout,
    build_unique_vertical_reference_lines,
    merge_plotly_layout,
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
    title = "Кластеры территорий на двумерной проекции"
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "Plotly недоступен, поэтому график кластеров не построен.")

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
                f"<b>{entity_row.get('Территория', 'Территория')}</b>",
                f"Район: {entity_row.get('Район', '—')}",
                f"Контекст: {entity_row.get('Тип территории', '—')}",
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
            xaxis={"title": "Компонента 1 (PCA)", "showgrid": False, "zeroline": False},
            yaxis={"title": "Компонента 2 (PCA)", "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False},
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
    title = "Размеры кластеров по числу территорий"
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "Plotly недоступен, поэтому распределение кластеров не построено.")

    counts = [int(np.sum(labels == cluster_id)) for cluster_id in range(len(cluster_labels))]
    shares = [count / total_rows if total_rows else 0.0 for count in counts]
    fire_totals = [
        int(entity_frame.loc[labels == cluster_id, "Число пожаров"].sum()) if "Число пожаров" in entity_frame.columns else 0
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
                hovertemplate="<b>%{x}</b><br>Территорий: %{y}<br>Доля: %{text}<br>Пожаров в истории: %{customdata}<extra></extra>",
            )
        ]
    )
    figure.update_layout(
        **merge_plotly_layout(
            _plotly_layout("Территорий", height=340),
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
    title = "Подсказка по числу кластеров"
    if not rows:
        return _empty_chart_bundle(title, "Недостаточно территорий, чтобы сравнить коэффициент силуэта и метод локтя по нескольким значениям k.")
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "Plotly недоступен, поэтому диагностический график не построен.")

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
            name="Коэффициент силуэта",
            marker=build_plotly_marker(color=PLOTLY_PALETTE["forest"], size=8),
            line=build_plotly_line(color=PLOTLY_PALETTE["forest"], width=2),
            hovertemplate="k=%{x}<br>Коэффициент силуэта=%{y:.3f}<extra></extra>",
        )
    )
    figure.add_trace(
        build_plotly_scatter_trace(
            x=x,
            y=inertia_values,
            mode="lines+markers",
            name="Инерция",
            yaxis="y2",
            marker=build_plotly_marker(color=PLOTLY_PALETTE["fire"], size=8),
            line=build_plotly_line(color=PLOTLY_PALETTE["fire"], width=2),
            hovertemplate="k=%{x}<br>Инерция=%{y:.2f}<extra></extra>",
        )
    )

    figure.update_layout(
        **merge_plotly_layout(
            _plotly_layout("Коэффициент силуэта", height=340),
            xaxis={"title": "Число кластеров", "tickmode": "array", "tickvals": x},
            yaxis={"title": "Коэффициент силуэта", "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False},
            legend=build_horizontal_legend(y=1.12),
            updates={
                "yaxis2": {"title": "Инерция", "overlaying": "y", "side": "right", "showgrid": False},
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
                        (current_cluster_count, "Рабочий k", PLOTLY_PALETTE["sky"]),
                        (recommended_cluster_count, "Рекомендуемый k", PLOTLY_PALETTE["sand"]),
                        (best_silhouette_k, "Лучший silhouette", PLOTLY_PALETTE["forest"]),
                        (elbow_k, "Локоть", PLOTLY_PALETTE["fire"]),
                    ],
                ),
            },
        )
    )
    return build_chart_bundle(title, figure)



def _plotly_layout(yaxis_title: str, height: int = 340) -> Dict[str, Any]:
    return build_service_plotly_layout(yaxis_title, height=height)


def _format_metric(column_name: str, value: Any) -> str:
    if column_name.startswith("Доля") or column_name.startswith("Покрытие"):
        return _format_percent(float(value))
    return _format_number(value, 2)
