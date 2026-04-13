from __future__ import annotations

import re
import textwrap
from typing import Any, Dict, List, Optional

from app.services.charting import (
    build_dashboard_plotly_layout,
    build_empty_chart_bundle,
    build_empty_plotly_payload,
    build_horizontal_legend,
    build_item_horizontal_bar_payload,
    build_item_chart_bundle,
    build_item_pie_payload,
    build_item_vertical_bar_payload,
    build_plotly_bar_payload,
    build_plotly_bar_trace,
    build_plotly_line,
    build_plotly_marker,
    build_plotly_palette,
    build_plotly_payload_from_traces,
    build_plotly_scatter_payload,
    build_plotly_scatter_trace,
    merge_plotly_layout,
)
from app.statistics_constants import PLOTLY_PALETTE

def _finalize_chart(
    title: str,
    items: List[Dict[str, Any]],
    empty_message: str,
    plotly: Optional[Dict[str, Any]] = None,
    description: str = "",
) -> Dict[str, Any]:
    return build_item_chart_bundle(
        title,
        items,
        empty_message,
        plotly=plotly,
        description=description,
        annotation_color="#7b6a5a",
    )


def _build_empty_plotly_chart(title: str, empty_message: str) -> Dict[str, Any]:
    return build_empty_chart_bundle(
        title,
        empty_message,
        annotation_color="#7b6a5a",
    )["plotly"]


def _empty_plotly_payload(empty_message: str) -> Dict[str, Any]:
    return build_empty_plotly_payload(empty_message, annotation_color="#7b6a5a")


def _build_yearly_plotly(title: str, items: List[Dict[str, Any]], metric: str, empty_message: str) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)

    x_values = [item["label"] for item in items]
    y_values = [item["value"] for item in items]
    text_values = [item["value_display"] for item in items]

    if metric == "count":
        return build_plotly_bar_payload(
            x=x_values,
            y=y_values,
            text=text_values,
            textposition="outside",
            marker=build_plotly_marker(
                color=PLOTLY_PALETTE["fire"],
                line_color=PLOTLY_PALETTE["fire_soft"],
                line_width=1.5,
            ),
            customdata=text_values,
            hovertemplate="<b>%{x}</b><br>Пожаров: %{customdata}<extra></extra>",
            layout=_plotly_layout("Пожаров", showlegend=False),
        )
    return build_plotly_scatter_payload(
        x=x_values,
        y=y_values,
        mode="lines+markers",
        fill="tozeroy",
        line=build_plotly_line(color=PLOTLY_PALETTE["forest"], width=4),
        marker=build_plotly_marker(color=PLOTLY_PALETTE["forest_soft"], size=9),
        customdata=text_values,
        hovertemplate="<b>%{x}</b><br>Площадь: %{customdata} га<extra></extra>",
        layout=_plotly_layout("Площадь, га", showlegend=False),
    )


def _wrap_plotly_label(value: Any, max_width: int = 34, max_lines: int = 3) -> str:
    normalized = re.sub(r"\s+", " ", str(value or "")).strip()
    if not normalized:
        return ""
    lines = textwrap.wrap(normalized, width=max_width, break_long_words=False, break_on_hyphens=False)
    if not lines:
        return normalized
    if len(lines) > max_lines:
        lines = lines[:max_lines]
        lines[-1] = lines[-1].rstrip(" .,;:") + "..."
    return "<br>".join(lines)


def _build_cause_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)
    ordered_items = list(reversed(items))
    return build_item_horizontal_bar_payload(
        ordered_items,
        layout=_plotly_layout("Количество пожаров", showlegend=False),
        y_values=[_wrap_plotly_label(item["label"], max_width=34, max_lines=2) for item in ordered_items],
        hovertemplate="<b>%{customdata}</b><br>Пожаров: %{text}<extra></extra>",
        color=PLOTLY_PALETTE["fire"],
        line_color=PLOTLY_PALETTE["fire_soft"],
        customdata=[item["label"] for item in ordered_items],
        layout_updates=merge_plotly_layout(
            updates={
                "height": min(620, max(360, 34 * len(items) + 90)),
                "margin": {"l": 320, "r": 72, "t": 20, "b": 36},
                "bargap": 0.62,
            },
            xaxis={"automargin": True},
            yaxis={"automargin": True, "tickfont": {"size": 11}},
        ),
    )


def _build_distribution_pie_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)
    return build_item_pie_payload(
        items,
        layout=_plotly_layout("", showlegend=False),
        colors=build_plotly_palette(["sky", "sky_soft", "forest_soft", "forest", "sand", "fire_soft"]),
        hole=0.45,
        hovertemplate="<b>%{label}</b><br>Записей: %{value}<br>Доля: %{percent}<extra></extra>",
        margin={"l": 24, "r": 24, "t": 12, "b": 12},
    )


def _build_distribution_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)
    ordered_items = list(reversed(items))
    return build_item_horizontal_bar_payload(
        ordered_items,
        layout=_plotly_layout("Количество записей", showlegend=False),
        y_values=[_wrap_plotly_label(item["label"], max_width=26, max_lines=2) for item in ordered_items],
        hovertemplate="<b>%{customdata}</b><br>Записей: %{text}<extra></extra>",
        color=PLOTLY_PALETTE["sky"],
        line_color=PLOTLY_PALETTE["sky_soft"],
        customdata=[item["label"] for item in ordered_items],
        layout_updates=merge_plotly_layout(
            updates={
                "height": max(340, 36 * len(items) + 90),
                "margin": {"l": 220, "r": 36, "t": 20, "b": 40},
            },
            yaxis={"automargin": True},
        ),
    )


def _build_combined_impact_timeline_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)

    x_values = [item["date_value"] for item in items]
    date_labels = [item["label"] for item in items]
    traces = [
        build_plotly_bar_trace(
            x=x_values,
            y=[item["deaths"] for item in items],
            name="Погибшие",
            customdata=date_labels,
            marker={"color": PLOTLY_PALETTE["fire"]},
            hovertemplate="<b>%{customdata}</b><br>Погибшие: %{y}<extra></extra>",
        ),
        build_plotly_bar_trace(
            x=x_values,
            y=[item["injuries"] for item in items],
            name="Травмированные",
            customdata=date_labels,
            marker={"color": PLOTLY_PALETTE["sand"]},
            hovertemplate="<b>%{customdata}</b><br>Травмированные: %{y}<extra></extra>",
        ),
        build_plotly_scatter_trace(
            x=x_values,
            y=[item["evacuated_adults"] for item in items],
            name="Эвакуировано",
            customdata=date_labels,
            mode="lines+markers",
            line=build_plotly_line(color=PLOTLY_PALETTE["sky"], width=3),
            marker=build_plotly_marker(color=PLOTLY_PALETTE["sky_soft"], size=7),
            hovertemplate="<b>%{customdata}</b><br>Эвакуировано: %{y}<extra></extra>",
        ),
        build_plotly_scatter_trace(
            x=x_values,
            y=[item["evacuated_children"] for item in items],
            name="Эвакуировано детей",
            customdata=date_labels,
            mode="lines+markers",
            line=build_plotly_line(color=PLOTLY_PALETTE["forest"], width=3),
            marker=build_plotly_marker(color=PLOTLY_PALETTE["forest_soft"], size=7),
            hovertemplate="<b>%{customdata}</b><br>Эвакуировано детей: %{y}<extra></extra>",
        ),
        build_plotly_scatter_trace(
            x=x_values,
            y=[item["rescued_children"] for item in items],
            name="Спасено детей",
            customdata=date_labels,
            mode="lines+markers",
            line=build_plotly_line(color=PLOTLY_PALETTE["ink"], width=2, dash="dot"),
            marker=build_plotly_marker(color=PLOTLY_PALETTE["fire_soft"], size=6),
            hovertemplate="<b>%{customdata}</b><br>Спасено детей: %{y}<extra></extra>",
        ),
    ]
    return build_plotly_payload_from_traces(
        traces,
        layout=_plotly_layout("Люди", showlegend=True),
        layout_updates=merge_plotly_layout(
            updates={"barmode": "group"},
            xaxis={"type": "date"},
            legend=build_horizontal_legend(y=1.14),
        ),
    )

def _build_damage_overview_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)
    ordered_items = list(reversed(items))
    return build_item_horizontal_bar_payload(
        ordered_items,
        layout=_plotly_layout("Количество пожаров", showlegend=False),
        y_values=[_wrap_plotly_label(item["label"], max_width=24, max_lines=2) for item in ordered_items],
        hovertemplate="<b>%{customdata}</b><br>Пожаров с ненулевым показателем: %{text}<extra></extra>",
        color=PLOTLY_PALETTE["sand"],
        line_color=PLOTLY_PALETTE["sand_soft"],
        customdata=[item["label"] for item in ordered_items],
        layout_updates=merge_plotly_layout(
            updates={
                "height": max(360, 38 * len(items) + 90),
                "margin": {"l": 230, "r": 36, "t": 20, "b": 40},
            },
            yaxis={"automargin": True},
        ),
    )


def _build_damage_pairs_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)

    x_values = [_wrap_plotly_label(item["label"], max_width=16, max_lines=2) for item in items]
    customdata = [item["label"] for item in items]
    traces = [
        build_plotly_bar_trace(
            x=x_values,
            y=[item["destroyed"] for item in items],
            name="Уничтожено",
            customdata=customdata,
            marker={"color": PLOTLY_PALETTE["fire"]},
            hovertemplate="<b>%{customdata}</b><br>Пожаров с уничтожением: %{y}<extra></extra>",
        ),
        build_plotly_bar_trace(
            x=x_values,
            y=[item["damaged"] for item in items],
            name="Повреждено",
            customdata=customdata,
            marker={"color": PLOTLY_PALETTE["sky"]},
            hovertemplate="<b>%{customdata}</b><br>Пожаров с повреждением: %{y}<extra></extra>",
        ),
    ]
    return build_plotly_payload_from_traces(
        traces,
        layout=_plotly_layout("Количество пожаров", showlegend=True),
        layout_updates=merge_plotly_layout(
            updates={"barmode": "group"},
            xaxis={"automargin": True},
            legend=build_horizontal_legend(y=1.12),
        ),
    )


def _build_damage_standalone_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)
    normalized_items = [
        {
            "label": _wrap_plotly_label(item["label"], max_width=16, max_lines=2),
            "value": item["value"],
            "value_display": item["value_display"],
            "original_label": item["label"],
        }
        for item in items
    ]
    return build_item_vertical_bar_payload(
        normalized_items,
        layout=_plotly_layout("Количество пожаров", showlegend=False),
        hovertemplate="<b>%{customdata}</b><br>Пожаров с показателем: %{text}<extra></extra>",
        colors=[
            [PLOTLY_PALETTE["forest"], PLOTLY_PALETTE["sky"], PLOTLY_PALETTE["sand"], PLOTLY_PALETTE["fire_soft"]][
                index % 4
            ]
            for index in range(len(normalized_items))
        ],
        line_color="rgba(255,255,255,0.6)",
        line_width=1.2,
        customdata=[item["original_label"] for item in normalized_items],
        layout_updates=merge_plotly_layout(xaxis={"automargin": True}),
    )

def _build_damage_share_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)
    return build_item_pie_payload(
        items,
        layout=_plotly_layout("", showlegend=False),
        colors=[
            PLOTLY_PALETTE["fire"],
            PLOTLY_PALETTE["fire_soft"],
            PLOTLY_PALETTE["sand"],
            PLOTLY_PALETTE["sand_soft"],
            PLOTLY_PALETTE["sky"],
            PLOTLY_PALETTE["sky_soft"],
            PLOTLY_PALETTE["forest"],
            PLOTLY_PALETTE["forest_soft"],
            "#b99f7a",
            "#8d7763",
        ],
        hole=0.46,
        hovertemplate="<b>%{label}</b><br>Пожаров: %{value}<br>Доля: %{percent}<extra></extra>",
        margin={"l": 24, "r": 24, "t": 12, "b": 12},
    )

def _build_table_breakdown_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)
    ordered_items = list(reversed(items))
    return build_item_horizontal_bar_payload(
        ordered_items,
        layout=_plotly_layout("Количество пожаров", showlegend=False),
        y_values=[item["label"] for item in ordered_items],
        hovertemplate="<b>%{y}</b><br>Пожаров: %{text}<extra></extra>",
        color=PLOTLY_PALETTE["sand"],
        line_color=PLOTLY_PALETTE["sand_soft"],
    )


def _build_monthly_profile_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)
    return build_item_vertical_bar_payload(
        items,
        layout=_plotly_layout("Количество пожаров", showlegend=False),
        hovertemplate="<b>%{x}</b><br>Пожаров: %{text}<extra></extra>",
        colors=[
            PLOTLY_PALETTE["fire_soft"],
            PLOTLY_PALETTE["fire_soft"],
            PLOTLY_PALETTE["fire"],
            PLOTLY_PALETTE["fire"],
            PLOTLY_PALETTE["sand"],
            PLOTLY_PALETTE["sand"],
            PLOTLY_PALETTE["sand_soft"],
            PLOTLY_PALETTE["sand_soft"],
            PLOTLY_PALETTE["forest_soft"],
            PLOTLY_PALETTE["forest"],
            PLOTLY_PALETTE["sky_soft"],
            PLOTLY_PALETTE["sky"],
        ][: len(items)],
        line_color="rgba(255,255,255,0.6)",
        line_width=1,
    )


def _build_area_bucket_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)
    return build_item_pie_payload(
        items,
        layout=_plotly_layout("", showlegend=False),
        colors=[
            PLOTLY_PALETTE["fire"],
            PLOTLY_PALETTE["fire_soft"],
            PLOTLY_PALETTE["sand"],
            PLOTLY_PALETTE["forest"],
            PLOTLY_PALETTE["sky"],
            "#b5aea5",
        ],
        hole=0.58,
        hovertemplate="<b>%{label}</b><br>Пожаров: %{value}<br>Доля: %{percent}<extra></extra>",
        margin={"l": 20, "r": 20, "t": 10, "b": 10},
    )



def _build_sql_widget_bar_plotly(
    title: str,
    items: List[Dict[str, Any]],
    empty_message: str,
    color_key: str,
    value_label: str,
) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)
    ordered_items = list(reversed(items))
    color_value = PLOTLY_PALETTE.get(color_key, PLOTLY_PALETTE["fire"])
    line_color = PLOTLY_PALETTE.get(f"{color_key}_soft", color_value)
    return build_item_horizontal_bar_payload(
        ordered_items,
        layout=_plotly_layout(value_label, showlegend=False),
        y_values=[_wrap_plotly_label(item["label"], max_width=26, max_lines=3) for item in ordered_items],
        hovertemplate=f"<b>%{{customdata}}</b><br>{value_label}: %{{text}}<extra></extra>",
        color=color_value,
        line_color=line_color,
        line_width=1.1,
        customdata=[item["label"] for item in ordered_items],
        layout_updates=merge_plotly_layout(
            updates={
                "height": max(320, 40 * len(items) + 80),
                "margin": {"l": 220, "r": 30, "t": 16, "b": 32},
            },
            yaxis={"automargin": True},
        ),
    )


def _build_sql_widget_season_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _empty_plotly_payload(empty_message)
    return build_item_vertical_bar_payload(
        items,
        layout=_plotly_layout("Пожаров", showlegend=False),
        hovertemplate="<b>%{x}</b><br>Пожаров: %{text}<extra></extra>",
        colors=build_plotly_palette(["sky", "forest", "sand", "fire_soft"], limit=len(items)),
        line_color="rgba(255,255,255,0.7)",
        line_width=1,
    )


def _plotly_layout(yaxis_title: str, showlegend: bool) -> Dict[str, Any]:
    return build_dashboard_plotly_layout(yaxis_title, showlegend=showlegend)

__all__ = [
    "_finalize_chart",
    "_build_yearly_plotly",
    "_wrap_plotly_label",
    "_build_cause_plotly",
    "_build_distribution_pie_plotly",
    "_build_distribution_plotly",
    "_build_combined_impact_timeline_plotly",
    "_build_damage_overview_plotly",
    "_build_damage_pairs_plotly",
    "_build_damage_standalone_plotly",
    "_build_damage_share_plotly",
    "_build_table_breakdown_plotly",
    "_build_monthly_profile_plotly",
    "_build_area_bucket_plotly",
    "_build_sql_widget_bar_plotly",
    "_build_sql_widget_season_plotly",
    "_plotly_layout",
]
