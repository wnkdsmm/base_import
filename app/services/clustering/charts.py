from __future__ import annotations

from typing import Any, Sequence

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

from .types import ClusteringCharts, DiagnosticsRow
from .utils import _format_integer, _format_number, _format_percent


def _build_scatter_chart(
    pca_points: np.ndarray,
    labels: np.ndarray,
    cluster_labels: Sequence[str],
    cluster_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
) -> dict[str, Any]:  # one-off
    title = "Кластеры территорий на двумерной проекции"
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(title, "Plotly недоступен, поэтому график кластеров не построен.")

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
) -> dict[str, Any]:  # one-off
    title = "Размеры кластеров по числу территорий"
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(title, "Plotly недоступен, поэтому распределение кластеров не построено.")

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
            plotly_layout("Территорий", height=340),
            updates={"showlegend": False},
        )
    )
    return build_chart_bundle(title, figure)


def _build_diagnostics_chart(
    rows: Sequence[DiagnosticsRow],
    current_cluster_count: int,
    recommended_cluster_count: int | None,
    best_silhouette_k: int | None,
    elbow_k: int | None,
) -> dict[str, Any]:  # one-off
    title = "Подсказка по числу кластеров"
    if not rows:
        return _empty_chart_bundle(title, "Недостаточно территорий, чтобы сравнить коэффициент силуэта и метод локтя по нескольким значениям k.")
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(title, "Plotly недоступен, поэтому диагностический график не построен.")

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
            plotly_layout("Коэффициент силуэта", height=340),
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


def build_radar_chart(
    cluster_profiles: dict[Any, dict[str, float]],
    feature_labels: Sequence[str],
) -> dict[str, Any]:
    title = "Профили кластеров по признакам"
    if not feature_labels or not cluster_profiles:
        return _empty_chart_bundle(
            title,
            "Недостаточно данных, чтобы показать профили кластеров по признакам.",
        )
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(
            title,
            "Plotly недоступен, поэтому радар-профили кластеров не построены.",
        )

    safe_feature_labels = [str(label) for label in feature_labels]
    cluster_items = list(cluster_profiles.items())
    if not cluster_items:
        return _empty_chart_bundle(
            title,
            "Недостаточно данных, чтобы показать профили кластеров по признакам.",
        )

    feature_min_max: dict[str, tuple[float, float]] = {}
    for feature_name in safe_feature_labels:
        values = [
            float((metrics or {}).get(feature_name, 0.0) or 0.0)
            for _, metrics in cluster_items
        ]
        if not values:
            feature_min_max[feature_name] = (0.0, 0.0)
            continue
        feature_min_max[feature_name] = (min(values), max(values))

    colors = build_plotly_palette(["sky", "forest", "sand", "fire"], limit=len(cluster_items))
    figure = go.Figure()
    theta = [*safe_feature_labels, safe_feature_labels[0]]
    for index, (cluster_id, metrics) in enumerate(cluster_items):
        normalized_values: list[float] = []
        for feature_name in safe_feature_labels:
            raw_value = float((metrics or {}).get(feature_name, 0.0) or 0.0)
            min_value, max_value = feature_min_max[feature_name]
            if max_value > min_value:
                normalized = (raw_value - min_value) / (max_value - min_value)
            else:
                normalized = 0.0
            normalized_values.append(float(np.clip(normalized, 0.0, 1.0)))
        normalized_values.append(normalized_values[0])

        figure.add_trace(
            go.Scatterpolar(
                r=normalized_values,
                theta=theta,
                mode="lines+markers",
                name=str(cluster_id),
                line=build_plotly_line(color=colors[index % len(colors)], width=2),
                marker=build_plotly_marker(color=colors[index % len(colors)], size=6),
                fill="toself",
                fillcolor=colors[index % len(colors)],
                opacity=0.2,
                hovertemplate="<b>%{fullData.name}</b><br>%{theta}: %{r:.2f}<extra></extra>",
            )
        )

    figure.update_layout(
        **merge_plotly_layout(
            build_service_plotly_layout("", height=420),
            legend=build_horizontal_legend(y=1.12),
            updates={
                "polar": {
                    "radialaxis": {
                        "range": [0, 1],
                        "gridcolor": PLOTLY_PALETTE["grid"],
                        "tickformat": ".1f",
                    },
                    "angularaxis": {"gridcolor": PLOTLY_PALETTE["grid"]},
                }
            },
        )
    )
    return build_chart_bundle(title, figure)


def build_feature_importance_chart(
    cluster_profiles: dict[Any, dict[str, float]],
    feature_labels: Sequence[str],
) -> dict[str, Any]:
    title = "Вклад признаков в разделение кластеров"
    if not feature_labels or not cluster_profiles:
        return _empty_chart_bundle(
            title,
            "Недостаточно данных, чтобы оценить вклад признаков в разделение кластеров.",
        )
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(
            title,
            "Plotly недоступен, поэтому график вклада признаков не построен.",
        )

    safe_feature_labels = [str(label) for label in feature_labels]
    cluster_items = list(cluster_profiles.items())
    if not cluster_items:
        return _empty_chart_bundle(
            title,
            "Недостаточно данных, чтобы оценить вклад признаков в разделение кластеров.",
        )

    feature_variances: list[tuple[str, float]] = []
    for feature_name in safe_feature_labels:
        values = [float((metrics or {}).get(feature_name, 0.0) or 0.0) for _, metrics in cluster_items]
        feature_variances.append((feature_name, float(np.var(values)) if values else 0.0))

    total_variance = float(sum(variance for _, variance in feature_variances))
    if total_variance > 0:
        contributions = [(feature_name, variance / total_variance) for feature_name, variance in feature_variances]
    else:
        uniform_share = (1.0 / len(feature_variances)) if feature_variances else 0.0
        contributions = [(feature_name, uniform_share) for feature_name, _variance in feature_variances]

    sorted_contributions = sorted(contributions, key=lambda item: item[1], reverse=True)
    y_values = [feature_name for feature_name, _ in sorted_contributions]
    x_values = [float(value) for _, value in sorted_contributions]
    text_values = [_format_percent(value) for value in x_values]

    figure = go.Figure(
        data=[
            build_plotly_bar_trace(
                x=x_values,
                y=y_values,
                orientation="h",
                marker={"color": PLOTLY_PALETTE["sky"]},
                text=text_values,
                textposition="auto",
                hovertemplate="<b>%{y}</b><br>Вклад: %{x:.1%}<extra></extra>",
            )
        ]
    )
    figure.update_layout(
        **merge_plotly_layout(
            plotly_layout("Вклад признака", height=max(320, 48 * len(y_values) + 120)),
            xaxis={"title": "Доля вклада", "tickformat": ".0%", "rangemode": "tozero"},
            yaxis={"title": "", "automargin": True, "autorange": "reversed"},
            updates={"showlegend": False},
        )
    )
    return build_chart_bundle(title, figure)


def _format_metric(column_name: str, value: Any) -> str:
    if column_name.startswith("Доля") or column_name.startswith("Покрытие"):
        return _format_percent(float(value))
    return _format_number(value, 2)


def build_clustering_scatter_chart(
    pca_points: Any,
    labels: Any,
    cluster_labels: Sequence[str],
    cluster_frame: Any,
    entity_frame: Any,
) -> dict[str, Any]:  # one-off
    return _build_scatter_chart(pca_points, labels, cluster_labels, cluster_frame, entity_frame)


def build_clustering_distribution_chart(
    labels: Any,
    cluster_labels: Sequence[str],
    total_rows: int,
    entity_frame: Any,
) -> dict[str, Any]:  # one-off
    return _build_distribution_chart(labels, cluster_labels, total_rows, entity_frame)


def build_clustering_diagnostics_chart(
    rows: Sequence[DiagnosticsRow],
    current_cluster_count: int,
    recommended_cluster_count: int | None,
    best_silhouette_k: int | None,
    elbow_k: int | None,
) -> dict[str, Any]:  # one-off
    return _build_diagnostics_chart(
        rows,
        current_cluster_count,
        recommended_cluster_count,
        best_silhouette_k,
        elbow_k,
    )


def _diagnostic_annotations(
    rows: Sequence[DiagnosticsRow],
    current_cluster_count: int,
    recommended_cluster_count: int | None,
    best_silhouette_k: int | None,
    elbow_k: int | None,
) -> list[dict[str, Any]]:  # one-off
    top_silhouette = max((item.get("silhouette") for item in rows if item.get("silhouette") is not None), default=0.0)
    y_anchor = max(0.05, float(top_silhouette))
    return build_reference_annotations(
        y_value=y_anchor,
        references=[
            (current_cluster_count, "Рабочий k", PLOTLY_PALETTE["sky"]),
            (recommended_cluster_count, "Рекомендуемый k", PLOTLY_PALETTE["sand"]),
            (best_silhouette_k, "Лучший silhouette", PLOTLY_PALETTE["forest"]),
            (elbow_k, "Локоть", PLOTLY_PALETTE["fire"]),
        ],
    )
