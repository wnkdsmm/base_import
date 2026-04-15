from __future__ import annotations

from typing import Any, Dict, Sequence

from app.plotly_bundle import PLOTLY_AVAILABLE, go
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


def build_access_points_points_scatter_chart(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return _build_points_scatter_chart(rows)
