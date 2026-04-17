from __future__ import annotations

from typing import Any, Dict, Sequence

from app.plotly_bundle import PLOTLY_AVAILABLE, go, serialize_plotly_figure
from app.services.charting import (
    build_chart_bundle,
    build_empty_chart_bundle as _empty_chart_bundle,
    build_plotly_marker,
    build_plotly_scattergl_trace,
)
from app.services.chart_utils import (
    build_component_projection,
    build_service_scatter_layout,
)
from app.statistics_constants import PLOTLY_PALETTE

TYPOLOGY_LABELS: Dict[str, str] = {
    "access": "Дальний выезд",
    "water": "Дефицит воды",
    "severity": "Тяжелые последствия",
    "recurrence": "Повторяющийся очаг",
    "needs_data": "Данные неполные",
    "mixed": "Комбинированный риск",
}

TYPOLOGY_COLORS: Dict[str, str] = {
    "access": PLOTLY_PALETTE["fire"],
    "water": PLOTLY_PALETTE["sand"],
    "severity": PLOTLY_PALETTE["forest"],
    "recurrence": PLOTLY_PALETTE["sky"],
    "needs_data": PLOTLY_PALETTE["sky_soft"],
    "mixed": PLOTLY_PALETTE["fire_soft"],
}


def _derive_typology_code(row: dict[str, Any]) -> str:
    explicit = str(row.get("typology_code") or "").strip().lower()
    if explicit in TYPOLOGY_LABELS:
        return explicit
    if row.get("uncertainty_flag"):
        return "needs_data"

    components = {
        "access": float(row.get("access_score") or 0.0),
        "water": float(row.get("water_score") or 0.0),
        "severity": float(row.get("severity_score") or 0.0),
        "recurrence": float(row.get("recurrence_score") or 0.0),
    }
    dominant = max(components, key=components.get)
    if dominant == "access" and components["access"] >= 35.0:
        return "access"
    if dominant == "water" and components["water"] >= 30.0:
        return "water"
    if dominant == "severity" and components["severity"] >= 30.0:
        return "severity"
    if dominant == "recurrence" and components["recurrence"] >= 28.0:
        return "recurrence"
    return "mixed"


def _resolve_typology(row: dict[str, Any]) -> tuple[str, str]:
    code = _derive_typology_code(row)
    label = str(row.get("typology_label") or "").strip() or TYPOLOGY_LABELS.get(code, TYPOLOGY_LABELS["mixed"])
    return code, label


def _build_points_scatter_chart(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    title = "Проблемные точки на двумерной проекции риска"
    if not rows:
        return _empty_chart_bundle(title, "Недостаточно данных, чтобы показать распределение проблемных точек.")
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "Plotly недоступен, поэтому график проблемных точек не построен.")

    figure = go.Figure()
    coordinates = build_component_projection(
        rows,
        component_keys=[
            "access_score",
            "water_score",
            "severity_score",
            "recurrence_score",
            "data_gap_score",
        ],
    )

    typology_order: list[str] = []
    typology_labels: Dict[str, str] = {}
    for row in rows:
        code, label = _resolve_typology(row)
        typology_labels[code] = label
        if code not in typology_order:
            typology_order.append(code)

    for typology_code in typology_order:
        group_indexes = []
        for index, row in enumerate(rows):
            code, _label = _resolve_typology(row)
            if code == typology_code:
                group_indexes.append(index)
        if not group_indexes:
            continue

        hover_texts = []
        x_values = []
        y_values = []
        for row_index in group_indexes:
            row = rows[row_index]
            hover_texts.append(
                "<br>".join(
                    [
                        f"<b>{row.get('label') or 'Точка'}</b>",
                        f"Тип: {row.get('entity_type') or '-'}",
                        f"Район: {row.get('district') or '-'}",
                        f"Итоговый score: {row.get('score_display') or '0'}",
                        f"Пожаров: {row.get('incident_count_display') or '0'}",
                        f"Доступность ПЧ: {row.get('access_score', 0)}",
                        f"\u0412\u043e\u0434\u0430: {row.get('water_score', 0)}",
                        f"Последствия: {row.get('severity_score', 0)}",
                        f"Частота и контекст: {row.get('recurrence_score', 0)}",
                        f"Неполнота данных: {row.get('data_gap_score', 0)}",
                    ]
                )
            )
            x_values.append(float(coordinates[row_index][0]))
            y_values.append(float(coordinates[row_index][1]))

        figure.add_trace(
            build_plotly_scattergl_trace(
                x=x_values,
                y=y_values,
                mode="markers",
                name=typology_labels.get(typology_code, TYPOLOGY_LABELS["mixed"]),
                text=hover_texts,
                hovertemplate="%{text}<extra></extra>",
                marker=build_plotly_marker(
                    color=TYPOLOGY_COLORS.get(typology_code, PLOTLY_PALETTE["sky"]),
                    size=10,
                    line_color="rgba(255,255,255,0.6)",
                    line_width=0.6,
                    opacity=0.88,
                ),
            )
        )

    figure.update_layout(
        **build_service_scatter_layout(
            xaxis_title="Компонента 1",
            yaxis_title="Компонента 2",
            height=420,
            include_xy_axes=False,
            legend_y=1.1,
        )
    )
    return build_chart_bundle(title, figure)


def _build_score_histogram(points: list[dict]) -> dict[str, Any]:
    if not PLOTLY_AVAILABLE:
        return {
            "figure": {"data": [], "layout": {}, "config": {"responsive": True}},
            "plotly": {"data": [], "layout": {}, "config": {"responsive": True}},
            "empty_message": "Plotly недоступен, поэтому гистограмма не построена.",
        }

    scores: list[int] = []
    for point in points:
        raw_value = point.get("total_score")
        try:
            score = int(float(raw_value))
        except (TypeError, ValueError):
            continue
        if score < 0:
            score = 0
        if score > 100:
            score = 100
        scores.append(score)

    if not scores:
        return {
            "figure": {"data": [], "layout": {}, "config": {"responsive": True}},
            "plotly": {"data": [], "layout": {}, "config": {"responsive": True}},
            "empty_message": "Недостаточно данных для построения распределения балла риска.",
        }

    bucket_counts = [0] * 10
    for score in scores:
        bucket_index = min(score // 10, 9)
        bucket_counts[bucket_index] += 1

    x_labels = [f"{index * 10}-{index * 10 + 10}" for index in range(9)] + ["90-100"]
    colors = []
    for index in range(10):
        bucket_start = index * 10
        if bucket_start >= 80:
            colors.append("#ef4444")
        elif bucket_start >= 60:
            colors.append("#f59e0b")
        elif bucket_start >= 40:
            colors.append("#14b8a6")
        else:
            colors.append("#94a3b8")

    figure = go.Figure(
        data=[
            go.Bar(
                x=x_labels,
                y=bucket_counts,
                marker={"color": colors},
                hovertemplate="Диапазон: %{x}<br>Количество точек: %{y}<extra></extra>",
            )
        ]
    )
    figure.update_layout(
        xaxis={"title": "Балл риска"},
        yaxis={"title": "Количество точек"},
        showlegend=False,
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin={"l": 48, "r": 16, "t": 12, "b": 48},
    )

    plotly_payload = serialize_plotly_figure(figure)
    return {
        "figure": figure.to_dict(),
        "plotly": plotly_payload,
        "empty_message": "",
    }


def _build_factor_bar_chart(points: list[dict]) -> dict[str, Any]:
    if not PLOTLY_AVAILABLE:
        return {
            "figure": {"data": [], "layout": {}, "config": {"responsive": True}},
            "plotly": {"data": [], "layout": {}, "config": {"responsive": True}},
            "empty_message": "Plotly недоступен, поэтому график вклада факторов не построен.",
        }

    top_points = list(points[:10])
    if not top_points:
        return {
            "figure": {"data": [], "layout": {}, "config": {"responsive": True}},
            "plotly": {"data": [], "layout": {}, "config": {"responsive": True}},
            "empty_message": "Недостаточно данных для построения вклада факторов.",
        }

    def _safe_score(item: dict[str, Any], key: str) -> float:
        try:
            value = float(item.get(key) or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(100.0, value))

    def _short_label(value: Any) -> str:
        text = str(value or "-")
        return text if len(text) <= 35 else (text[:35] + "...")

    y_labels = [_short_label(point.get("label")) for point in top_points]
    access_values = [_safe_score(point, "access_score") for point in top_points]
    water_values = [_safe_score(point, "water_score") for point in top_points]
    severity_values = [_safe_score(point, "severity_score") for point in top_points]
    recurrence_values = [_safe_score(point, "recurrence_score") for point in top_points]
    data_gap_values = [_safe_score(point, "data_gap_score") for point in top_points]

    figure = go.Figure(
        data=[
            go.Bar(name="Доступность", y=y_labels, x=access_values, orientation="h", marker={"color": "#3b82f6"}),
            go.Bar(name="Водоснабжение", y=y_labels, x=water_values, orientation="h", marker={"color": "#06b6d4"}),
            go.Bar(name="Последствия", y=y_labels, x=severity_values, orientation="h", marker={"color": "#ef4444"}),
            go.Bar(name="Повторяемость", y=y_labels, x=recurrence_values, orientation="h", marker={"color": "#f59e0b"}),
            go.Bar(name="Пропуски", y=y_labels, x=data_gap_values, orientation="h", marker={"color": "#94a3b8"}),
        ]
    )
    figure.update_layout(
        barmode="stack",
        xaxis={"title": "Балл", "range": [0, 100]},
        yaxis={"autorange": "reversed", "showgrid": False},
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=max(280, 60 * min(len(top_points), 10) + 80),
        margin={"l": 140, "r": 24, "t": 20, "b": 48},
    )

    plotly_payload = serialize_plotly_figure(figure)
    return {
        "figure": figure.to_dict(),
        "plotly": plotly_payload,
        "empty_message": "",
    }


def _build_factor_heatmap(points: list[dict]) -> dict[str, Any]:
    if not PLOTLY_AVAILABLE:
        return {
            "figure": {"data": [], "layout": {}, "config": {"responsive": True}},
            "plotly": {"data": [], "layout": {}, "config": {"responsive": True}},
            "empty_message": "Plotly недоступен, поэтому тепловая карта не построена.",
        }

    top_points = list(points[:15])
    if len(top_points) < 3:
        return {
            "figure": {"data": [], "layout": {}, "config": {"responsive": True}},
            "plotly": {"data": [], "layout": {}, "config": {"responsive": True}},
            "empty_message": "Недостаточно данных для построения тепловой карты",
        }

    features = [
        ("access_score", "Доступность"),
        ("water_score", "Вода"),
        ("severity_score", "Последствия"),
        ("recurrence_score", "Повторяемость"),
        ("data_gap_score", "Пропуски"),
    ]

    def _safe_score(item: dict[str, Any], key: str) -> float:
        try:
            value = float(item.get(key) or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(100.0, value))

    x_labels = [label for _key, label in features]
    y_labels = [str((point.get("label") or "-"))[:30] for point in top_points]
    z_values = [
        [_safe_score(point, key) for key, _label in features]
        for point in top_points
    ]
    text_values = [[str(int(round(value))) for value in row] for row in z_values]

    figure = go.Figure(
        data=[
            go.Heatmap(
                z=z_values,
                x=x_labels,
                y=y_labels,
                colorscale="RdYlGn_r",
                zmin=0,
                zmax=100,
                hovertemplate="%{y}<br>%{x}: %{z:.0f}<extra></extra>",
                showscale=False,
            )
        ]
    )

    annotations = []
    for row_index, row in enumerate(text_values):
        for col_index, cell_value in enumerate(row):
            annotations.append(
                {
                    "x": x_labels[col_index],
                    "y": y_labels[row_index],
                    "text": cell_value,
                    "showarrow": False,
                    "font": {"size": 9, "color": "#111827"},
                }
            )

    figure.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=40 * min(15, len(top_points)) + 80,
        margin={"l": 120, "r": 16, "t": 18, "b": 42},
        xaxis={"side": "top"},
        yaxis={"showgrid": False, "autorange": "reversed"},
        annotations=annotations,
    )

    plotly_payload = serialize_plotly_figure(figure)
    return {
        "figure": figure.to_dict(),
        "plotly": plotly_payload,
        "empty_message": "",
    }


def build_access_points_points_scatter_chart(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return _build_points_scatter_chart(rows)

