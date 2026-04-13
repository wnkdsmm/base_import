from __future__ import annotations

from typing import Any, Dict, Sequence

from app.plotly_bundle import PLOTLY_AVAILABLE, go
from app.services.charting import (
    build_component_projection,
    build_chart_bundle,
    build_empty_chart_bundle as _empty_chart_bundle,
    build_horizontal_legend,
    build_plotly_marker,
    build_plotly_scattergl_trace,
    build_service_plotly_layout,
    merge_plotly_layout,
)
from app.statistics_constants import PLOTLY_PALETTE


def _build_points_scatter_chart(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
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
    typology_palette = {
        "Дальний выезд": PLOTLY_PALETTE["fire"],
        "Дефицит воды": PLOTLY_PALETTE["sand"],
        "Тяжёлые последствия": PLOTLY_PALETTE["forest"],
        "Повторяющийся очаг": PLOTLY_PALETTE["sky"],
        "Данные неполные": PLOTLY_PALETTE["sky_soft"],
        "Комбинированный риск": PLOTLY_PALETTE["fire_soft"],
    }

    typology_order = []
    for row in rows:
        label = str(row.get("typology_label") or "Комбинированный риск")
        if label not in typology_order:
            typology_order.append(label)

    for typology_label in typology_order:
        group_indexes = [
            index
            for index, row in enumerate(rows)
            if str(row.get("typology_label") or "Комбинированный риск") == typology_label
        ]
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
                        f"Тип: {row.get('entity_type') or '—'}",
                        f"Район: {row.get('district') or '—'}",
                        f"Итоговый score: {row.get('score_display') or '0'}",
                        f"Пожаров: {row.get('incident_count_display') or '0'}",
                        f"Доступность ПЧ: {row.get('access_score', 0)}",
                        f"Вода: {row.get('water_score', 0)}",
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
                name=typology_label,
                text=hover_texts,
                hovertemplate="%{text}<extra></extra>",
                marker=build_plotly_marker(
                    color=typology_palette.get(typology_label, PLOTLY_PALETTE["sky"]),
                    size=10,
                    line_color="rgba(255,255,255,0.6)",
                    line_width=0.6,
                    opacity=0.88,
                ),
            )
        )

    figure.update_layout(
        **merge_plotly_layout(
            build_service_plotly_layout(height=420, include_xy_axes=False),
            xaxis={"title": "Компонента 1", "showgrid": False, "zeroline": False},
            yaxis={"title": "Компонента 2", "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False},
            legend=build_horizontal_legend(y=1.1),
        )
    )
    return build_chart_bundle(title, figure)
