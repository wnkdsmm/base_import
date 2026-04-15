from __future__ import annotations

import re
import textwrap
from typing import Any, Dict, List, Optional, Sequence, TypedDict

from .dashboard_charts_kpi import (
    ChartData,
    ImpactTimelineData,
    PlotlyPayload,
    _empty_plotly_payload,
    _plotly_layout,
    _wrap_plotly_label,
)
from app.services.charting import (
    build_empty_chart_bundle,
    build_empty_plotly_payload,
    build_item_horizontal_bar_payload,
    build_item_chart_bundle,
    build_item_pie_payload,
    build_item_vertical_bar_payload,
    build_plotly_bar_payload,
    build_plotly_bar_trace,
    build_plotly_line,
    build_plotly_marker,
    build_plotly_payload_from_traces,
    build_plotly_scatter_payload,
    build_plotly_scatter_trace,
)
from app.services.chart_utils import (
    build_horizontal_legend,
    build_plotly_layout,
    build_plotly_palette,
    merge_plotly_layout,
)
from app.statistics_constants import PLOTLY_PALETTE

def _build_yearly_plotly(title: str, items: List[ChartData], metric: str, empty_message: str) -> PlotlyPayload:
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

def _build_combined_impact_timeline_plotly(
    title: str, items: List[ImpactTimelineData], empty_message: str
) -> PlotlyPayload:
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

def _build_monthly_profile_plotly(title: str, items: List[ChartData], empty_message: str) -> PlotlyPayload:
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

def _build_area_bucket_plotly(title: str, items: List[ChartData], empty_message: str) -> PlotlyPayload:
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
    items: List[ChartData],
    empty_message: str,
    color_key: str,
    value_label: str,
) -> PlotlyPayload:
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

def _build_sql_widget_season_plotly(title: str, items: List[ChartData], empty_message: str) -> PlotlyPayload:
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

def build_dashboard_yearly_plotly(
    title: str,
    items: Sequence[ChartData],
    metric: str,
    empty_message: str,
) -> PlotlyPayload:
    return _build_yearly_plotly(title, list(items), metric, empty_message)

def build_dashboard_combined_impact_timeline_plotly(
    title: str,
    items: Sequence[ImpactTimelineData],
    empty_message: str,
) -> PlotlyPayload:
    return _build_combined_impact_timeline_plotly(title, list(items), empty_message)

__all__ = [
    '_build_yearly_plotly',
    '_build_combined_impact_timeline_plotly',
    '_build_monthly_profile_plotly',
    '_build_area_bucket_plotly',
    '_build_sql_widget_bar_plotly',
    '_build_sql_widget_season_plotly',
    'build_dashboard_yearly_plotly',
    'build_dashboard_combined_impact_timeline_plotly',
]
