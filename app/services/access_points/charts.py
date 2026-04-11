from __future__ import annotations

from typing import Any, Dict, Sequence

import numpy as np

from app.plotly_bundle import PLOTLY_AVAILABLE, go
from app.services.charting import build_chart_bundle, build_empty_chart_bundle as _empty_chart_bundle, build_service_plotly_layout
from app.statistics_constants import PLOTLY_PALETTE


def _build_points_scatter_chart(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    title = "Проблемные точки на двумерной проекции риска"
    if not rows:
        return _empty_chart_bundle(title, "Недостаточно данных, чтобы показать распределение проблемных точек.")
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "Plotly недоступен, поэтому график проблемных точек не построен.")

    figure = go.Figure()
    coordinates = _build_component_projection(rows)
    palette = {
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
        group = [
            (row, coordinates[index])
            for index, row in enumerate(rows)
            if str(row.get("typology_label") or "Комбинированный риск") == typology_label
        ]
        if not group:
            continue
        hover_texts = []
        x_values = []
        y_values = []
        for row, point in group:
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
            x_values.append(float(point[0]))
            y_values.append(float(point[1]))

        figure.add_trace(
            go.Scattergl(
                x=x_values,
                y=y_values,
                mode="markers",
                name=typology_label,
                text=hover_texts,
                hovertemplate="%{text}<extra></extra>",
                marker={
                    "size": 10,
                    "color": palette.get(typology_label, PLOTLY_PALETTE["sky"]),
                    "line": {"width": 0.6, "color": "rgba(255,255,255,0.6)"},
                    "opacity": 0.88,
                },
            )
        )

    layout = build_service_plotly_layout(height=420, include_xy_axes=False)
    figure.update_layout(
        **layout,
        xaxis={
            "title": "Компонента 1",
            "showgrid": False,
            "zeroline": False,
        },
        yaxis={
            "title": "Компонента 2",
            "gridcolor": PLOTLY_PALETTE["grid"],
            "zeroline": False,
        },
        legend={"orientation": "h", "y": 1.1, "x": 0},
    )
    return build_chart_bundle(title, figure)


def _build_component_projection(rows: Sequence[Dict[str, Any]]) -> np.ndarray:
    matrix = np.array(
        [
            [
                float(row.get("access_score") or 0.0),
                float(row.get("water_score") or 0.0),
                float(row.get("severity_score") or 0.0),
                float(row.get("recurrence_score") or 0.0),
                float(row.get("data_gap_score") or 0.0),
            ]
            for row in rows
        ],
        dtype=float,
    )
    if len(rows) <= 1:
        return np.zeros((len(rows), 2), dtype=float)

    centered = matrix - matrix.mean(axis=0, keepdims=True)
    if np.allclose(centered, 0.0):
        return np.zeros((len(rows), 2), dtype=float)

    try:
        _, _, vt = np.linalg.svd(centered, full_matrices=False)
        basis = vt[:2].T if vt.shape[0] >= 2 else np.pad(vt[:1].T, ((0, 0), (0, 1)))
        projected = centered @ basis
    except Exception:
        projected = np.column_stack((centered[:, 0], centered[:, 2] if centered.shape[1] > 2 else np.zeros(len(rows))))

    if projected.shape[1] < 2:
        projected = np.column_stack((projected[:, 0], np.zeros(len(rows))))
    return projected
