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
    "access": "Р”Р°Р»СЊРЅРёР№ РІС‹РµР·Рґ",
    "water": "Р”РµС„РёС†РёС‚ РІРѕРґС‹",
    "severity": "РўСЏР¶РµР»С‹Рµ РїРѕСЃР»РµРґСЃС‚РІРёСЏ",
    "recurrence": "РџРѕРІС‚РѕСЂСЏСЋС‰РёР№СЃСЏ РѕС‡Р°Рі",
    "needs_data": "Р”Р°РЅРЅС‹Рµ РЅРµРїРѕР»РЅС‹Рµ",
    "mixed": "РљРѕРјР±РёРЅРёСЂРѕРІР°РЅРЅС‹Р№ СЂРёСЃРє",
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
    title = "РџСЂРѕР±Р»РµРјРЅС‹Рµ С‚РѕС‡РєРё РЅР° РґРІСѓРјРµСЂРЅРѕР№ РїСЂРѕРµРєС†РёРё СЂРёСЃРєР°"
    if not rows:
        return _empty_chart_bundle(title, "РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С…, С‡С‚РѕР±С‹ РїРѕРєР°Р·Р°С‚СЊ СЂР°СЃРїСЂРµРґРµР»РµРЅРёРµ РїСЂРѕР±Р»РµРјРЅС‹С… С‚РѕС‡РµРє.")
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "Plotly РЅРµРґРѕСЃС‚СѓРїРµРЅ, РїРѕСЌС‚РѕРјСѓ РіСЂР°С„РёРє РїСЂРѕР±Р»РµРјРЅС‹С… С‚РѕС‡РµРє РЅРµ РїРѕСЃС‚СЂРѕРµРЅ.")

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
                        f"<b>{row.get('label') or 'РўРѕС‡РєР°'}</b>",
                        f"РўРёРї: {row.get('entity_type') or '-'}",
                        f"Р Р°Р№РѕРЅ: {row.get('district') or '-'}",
                        f"РС‚РѕРіРѕРІС‹Р№ score: {row.get('score_display') or '0'}",
                        f"РџРѕР¶Р°СЂРѕРІ: {row.get('incident_count_display') or '0'}",
                        f"Р”РѕСЃС‚СѓРїРЅРѕСЃС‚СЊ РџР§: {row.get('access_score', 0)}",
                        f"Р’РѕРґР°: {row.get('water_score', 0)}",
                        f"РџРѕСЃР»РµРґСЃС‚РІРёСЏ: {row.get('severity_score', 0)}",
                        f"Р§Р°СЃС‚РѕС‚Р° Рё РєРѕРЅС‚РµРєСЃС‚: {row.get('recurrence_score', 0)}",
                        f"РќРµРїРѕР»РЅРѕС‚Р° РґР°РЅРЅС‹С…: {row.get('data_gap_score', 0)}",
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
            xaxis_title="РљРѕРјРїРѕРЅРµРЅС‚Р° 1",
            yaxis_title="РљРѕРјРїРѕРЅРµРЅС‚Р° 2",
            height=420,
            include_xy_axes=False,
            legend_y=1.1,
        )
    )
    return build_chart_bundle(title, figure)


def build_access_points_points_scatter_chart(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return _build_points_scatter_chart(rows)
