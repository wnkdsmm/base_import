from __future__ import annotations

from typing import Any, Dict, Sequence

import numpy as np
import pandas as pd

from app.plotly_bundle import PLOTLY_AVAILABLE, empty_plotly_payload, go, serialize_plotly_figure
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
    palette = [
        PLOTLY_PALETTE["sky"],
        PLOTLY_PALETTE["forest"],
        PLOTLY_PALETTE["sand"],
        PLOTLY_PALETTE["fire"],
        PLOTLY_PALETTE["sky_soft"],
        PLOTLY_PALETTE["forest_soft"],
    ]
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
            go.Scattergl(
                x=pca_points[mask, 0],
                y=pca_points[mask, 1],
                mode="markers",
                name=cluster_label,
                text=hover_texts,
                hovertemplate="%{text}<extra></extra>",
                marker={
                    "size": 10,
                    "color": palette[cluster_id % len(palette)],
                    "line": {"width": 0.6, "color": "rgba(255,255,255,0.6)"},
                    "opacity": 0.88,
                },
            )
        )

    layout = _plotly_layout("", height=420)
    base_xaxis = dict(layout.pop("xaxis", {}))
    base_yaxis = dict(layout.pop("yaxis", {}))
    figure.update_layout(
        **layout,
        xaxis={**base_xaxis, "title": "Компонента 1 (PCA)", "showgrid": False, "zeroline": False},
        yaxis={**base_yaxis, "title": "Компонента 2 (PCA)", "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False},
        legend={"orientation": "h", "y": 1.1, "x": 0},
    )
    return {"title": title, "plotly": _figure_to_dict(figure), "empty_message": ""}



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
    colors = [
        PLOTLY_PALETTE["sky"],
        PLOTLY_PALETTE["forest"],
        PLOTLY_PALETTE["sand"],
        PLOTLY_PALETTE["fire"],
        PLOTLY_PALETTE["sky_soft"],
        PLOTLY_PALETTE["forest_soft"],
    ]

    figure = go.Figure(
        data=[
            go.Bar(
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
    layout = _plotly_layout("Территорий", height=340)
    layout["showlegend"] = False
    figure.update_layout(**layout)
    return {"title": title, "plotly": _figure_to_dict(figure), "empty_message": ""}



def _build_diagnostics_chart(
    rows: Sequence[Dict[str, Any]],
    requested_cluster_count: int,
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

    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=x,
            y=silhouette_values,
            mode="lines+markers",
            name="Коэффициент силуэта",
            marker={"size": 8, "color": PLOTLY_PALETTE["forest"]},
            line={"width": 2, "color": PLOTLY_PALETTE["forest"]},
            hovertemplate="k=%{x}<br>Коэффициент силуэта=%{y:.3f}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=x,
            y=inertia_values,
            mode="lines+markers",
            name="Инерция",
            yaxis="y2",
            marker={"size": 8, "color": PLOTLY_PALETTE["fire"]},
            line={"width": 2, "color": PLOTLY_PALETTE["fire"]},
            hovertemplate="k=%{x}<br>Инерция=%{y:.2f}<extra></extra>",
        )
    )

    layout = _plotly_layout("Коэффициент силуэта", height=340)
    layout.update(
        {
            "xaxis": {"title": "Число кластеров", "tickmode": "array", "tickvals": x},
            "yaxis": {"title": "Коэффициент силуэта", "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False},
            "yaxis2": {"title": "Инерция", "overlaying": "y", "side": "right", "showgrid": False},
            "legend": {"orientation": "h", "y": 1.12, "x": 0},
            "shapes": _diagnostic_shapes(x, requested_cluster_count, best_silhouette_k, elbow_k),
            "annotations": _diagnostic_annotations(rows, requested_cluster_count, best_silhouette_k, elbow_k),
        }
    )
    figure.update_layout(**layout)
    return {"title": title, "plotly": _figure_to_dict(figure), "empty_message": ""}



def _empty_chart_bundle(title: str, message: str) -> Dict[str, Any]:
    return {
        "title": title,
        "plotly": _build_empty_plotly(message),
        "empty_message": message,
    }



def _build_empty_plotly(message: str) -> Dict[str, Any]:
    if not PLOTLY_AVAILABLE:
        return empty_plotly_payload(message)

    figure = go.Figure()
    figure.update_layout(
        height=320,
        paper_bgcolor="rgba(255,255,255,0)",
        plot_bgcolor="rgba(255,255,255,0)",
        margin={"l": 20, "r": 20, "t": 20, "b": 20},
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[
            {
                "text": message,
                "x": 0.5,
                "y": 0.5,
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 16, "color": "#61758d"},
            }
        ],
    )
    payload = _figure_to_dict(figure)
    payload["empty_message"] = message
    return payload



def _plotly_layout(yaxis_title: str, height: int = 340) -> Dict[str, Any]:
    return {
        "height": height,
        "showlegend": True,
        "paper_bgcolor": "rgba(255,255,255,0)",
        "plot_bgcolor": "rgba(255,255,255,0)",
        "font": {"family": 'Bahnschrift, "Segoe UI", "Trebuchet MS", sans-serif', "color": PLOTLY_PALETTE["ink"]},
        "margin": {"l": 52, "r": 42, "t": 24, "b": 48},
        "xaxis": {"showgrid": False, "zeroline": False},
        "yaxis": {"title": yaxis_title, "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False},
        "hoverlabel": {"bgcolor": "#fbfdff", "font": {"color": PLOTLY_PALETTE["ink"]}},
    }



def _figure_to_dict(figure: Any) -> Dict[str, Any]:
    return serialize_plotly_figure(figure)


def _diagnostic_shapes(
    x_values: Sequence[int],
    requested_cluster_count: int,
    best_silhouette_k: int | None,
    elbow_k: int | None,
) -> list[Dict[str, Any]]:
    if not x_values:
        return []
    lines = []
    seen = set()
    for value, color in [
        (requested_cluster_count, PLOTLY_PALETTE["sky"]),
        (best_silhouette_k, PLOTLY_PALETTE["forest"]),
        (elbow_k, PLOTLY_PALETTE["fire"]),
    ]:
        if value is None or value in seen:
            continue
        seen.add(value)
        lines.append(
            {
                "type": "line",
                "xref": "x",
                "yref": "paper",
                "x0": value,
                "x1": value,
                "y0": 0,
                "y1": 1,
                "line": {"color": color, "width": 1.6, "dash": "dot"},
            }
        )
    return lines



def _diagnostic_annotations(
    rows: Sequence[Dict[str, Any]],
    requested_cluster_count: int,
    best_silhouette_k: int | None,
    elbow_k: int | None,
) -> list[Dict[str, Any]]:
    if not rows:
        return []
    top_silhouette = max((item["silhouette"] for item in rows if item.get("silhouette") is not None), default=0.0)
    y_anchor = max(0.05, top_silhouette) if top_silhouette is not None else 0.05
    annotations = []
    for value, text, color in [
        (requested_cluster_count, "Текущий k", PLOTLY_PALETTE["sky"]),
        (best_silhouette_k, "Лучший silhouette", PLOTLY_PALETTE["forest"]),
        (elbow_k, "Локоть", PLOTLY_PALETTE["fire"]),
    ]:
        if value is None:
            continue
        annotations.append(
            {
                "x": value,
                "y": y_anchor,
                "xref": "x",
                "yref": "y",
                "text": text,
                "showarrow": True,
                "arrowhead": 2,
                "ax": 0,
                "ay": -26,
                "font": {"size": 11, "color": color},
                "arrowcolor": color,
                "bgcolor": "rgba(255,255,255,0.75)",
            }
        )
    return annotations



def _format_metric(column_name: str, value: Any) -> str:
    if column_name.startswith("Доля") or column_name.startswith("Покрытие"):
        return _format_percent(float(value))
    return _format_number(value, 2)
