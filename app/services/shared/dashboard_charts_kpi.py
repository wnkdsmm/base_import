from __future__ import annotations

import re
import textwrap
from typing import Any, Dict, List, Optional, Sequence, TypedDict

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

class ChartData(TypedDict):
    """Standard dashboard chart item with label and numeric value."""

    label: str
    value: int | float
    value_display: str

class ImpactTimelineData(TypedDict):
    """Timeline point for combined impact chart metrics."""

    date_value: str
    label: str
    deaths: int | float
    injuries: int | float
    evacuated_adults: int | float
    evacuated_children: int | float
    rescued_children: int | float

class DamagePairData(TypedDict):
    """Damage comparison item with destroyed and damaged counters."""

    label: str
    destroyed: int | float
    damaged: int | float

class PlotlyPayload(TypedDict, total=False):
    """Plotly payload container used by dashboard chart builders."""

    data: List[Any]
    layout: dict[str, Any]
    frames: List[Any]

class ChartResult(TypedDict, total=False):
    """Dashboard chart bundle returned to presentation layer."""

    title: str
    items: List[ChartData]
    empty_message: str
    plotly: PlotlyPayload
    description: str

def build_dashboard_plotly_layout(
    yaxis_title: str = "",
    *,
    showlegend: bool = True,
    height: int = 340,
    margin_left: int = 52,
    margin_right: int = 26,
    margin_top: int = 20,
    margin_bottom: int = 48,
    hover_bgcolor: str = "#fffaf5",
    bargap: float = 0.45,
) -> PlotlyPayload:
    return build_plotly_layout(
        yaxis_title,
        height=height,
        showlegend=showlegend,
        include_xy_axes=True,
        margin_left=margin_left,
        margin_right=margin_right,
        margin_top=margin_top,
        margin_bottom=margin_bottom,
        hover_bgcolor=hover_bgcolor,
        paper_bgcolor=PLOTLY_PALETTE["paper"],
        plot_bgcolor=PLOTLY_PALETTE["paper"],
        bargap=bargap,
        axis_tickfont_size=12,
        axis_title_font_size=12,
    )

def _finalize_chart(
    title: str,
    items: List[ChartData],
    empty_message: str,
    plotly: Optional[PlotlyPayload] = None,
    description: str = "",
) -> ChartResult:
    return build_item_chart_bundle(
        title,
        items,
        empty_message,
        plotly=plotly,
        description=description,
        annotation_color="#7b6a5a",
    )

def _build_empty_plotly_chart(title: str, empty_message: str) -> PlotlyPayload:
    return build_empty_chart_bundle(
        title,
        empty_message,
        annotation_color="#7b6a5a",
    )["plotly"]

def _empty_plotly_payload(empty_message: str) -> PlotlyPayload:
    return build_empty_plotly_payload(empty_message, annotation_color="#7b6a5a")

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

def _build_cause_plotly(title: str, items: List[ChartData], empty_message: str) -> PlotlyPayload:
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

def _build_distribution_pie_plotly(title: str, items: List[ChartData], empty_message: str) -> PlotlyPayload:
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

def _build_distribution_plotly(title: str, items: List[ChartData], empty_message: str) -> PlotlyPayload:
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

def _build_damage_overview_plotly(title: str, items: List[ChartData], empty_message: str) -> PlotlyPayload:
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

def _build_damage_pairs_plotly(title: str, items: List[DamagePairData], empty_message: str) -> PlotlyPayload:
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

def _build_damage_standalone_plotly(title: str, items: List[ChartData], empty_message: str) -> PlotlyPayload:
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

def _build_damage_share_plotly(title: str, items: List[ChartData], empty_message: str) -> PlotlyPayload:
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

def _build_table_breakdown_plotly(title: str, items: List[ChartData], empty_message: str) -> PlotlyPayload:
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

def _plotly_layout(yaxis_title: str, showlegend: bool) -> PlotlyPayload:
    return build_dashboard_plotly_layout(yaxis_title, showlegend=showlegend)

def build_dashboard_finalize_chart(
    title: str,
    items: Sequence[ChartData],
    empty_message: str,
    *,
    plotly: PlotlyPayload | None = None,
    description: str = "",
) -> ChartResult:
    return _finalize_chart(title, list(items), empty_message, plotly=plotly, description=description)

def build_dashboard_empty_plotly_chart(title: str, empty_message: str) -> PlotlyPayload:
    return _build_empty_plotly_chart(title, empty_message)

def build_dashboard_wrap_plotly_label(value: Any, max_width: int = 34, max_lines: int = 3) -> str:
    return _wrap_plotly_label(value, max_width=max_width, max_lines=max_lines)

def build_dashboard_cause_plotly(title: str, items: Sequence[ChartData], empty_message: str) -> PlotlyPayload:
    return _build_cause_plotly(title, list(items), empty_message)

def build_dashboard_distribution_pie_plotly(
    title: str,
    items: Sequence[ChartData],
    empty_message: str,
) -> PlotlyPayload:
    return _build_distribution_pie_plotly(title, list(items), empty_message)

def build_dashboard_distribution_plotly(
    title: str,
    items: Sequence[ChartData],
    empty_message: str,
) -> PlotlyPayload:
    return _build_distribution_plotly(title, list(items), empty_message)

def build_dashboard_damage_overview_plotly(
    title: str,
    items: Sequence[ChartData],
    empty_message: str,
) -> PlotlyPayload:
    return _build_damage_overview_plotly(title, list(items), empty_message)

def build_dashboard_damage_pairs_plotly(
    title: str,
    items: Sequence[DamagePairData],
    empty_message: str,
) -> PlotlyPayload:
    return _build_damage_pairs_plotly(title, list(items), empty_message)

def build_dashboard_damage_standalone_plotly(
    title: str,
    items: Sequence[ChartData],
    empty_message: str,
) -> PlotlyPayload:
    return _build_damage_standalone_plotly(title, list(items), empty_message)

def build_dashboard_damage_share_plotly(
    title: str,
    items: Sequence[ChartData],
    empty_message: str,
) -> PlotlyPayload:
    return _build_damage_share_plotly(title, list(items), empty_message)

def build_dashboard_table_breakdown_plotly(
    title: str,
    items: Sequence[ChartData],
    empty_message: str,
) -> PlotlyPayload:
    return _build_table_breakdown_plotly(title, list(items), empty_message)

__all__ = [
    'ChartData',
    'ImpactTimelineData',
    'DamagePairData',
    'PlotlyPayload',
    'ChartResult',
    'build_dashboard_plotly_layout',
    '_finalize_chart',
    '_build_empty_plotly_chart',
    '_empty_plotly_payload',
    '_wrap_plotly_label',
    '_build_cause_plotly',
    '_build_distribution_pie_plotly',
    '_build_distribution_plotly',
    '_build_damage_overview_plotly',
    '_build_damage_pairs_plotly',
    '_build_damage_standalone_plotly',
    '_build_damage_share_plotly',
    '_build_table_breakdown_plotly',
    '_plotly_layout',
    'build_dashboard_finalize_chart',
    'build_dashboard_empty_plotly_chart',
    'build_dashboard_wrap_plotly_label',
    'build_dashboard_cause_plotly',
    'build_dashboard_distribution_pie_plotly',
    'build_dashboard_distribution_plotly',
    'build_dashboard_damage_overview_plotly',
    'build_dashboard_damage_pairs_plotly',
    'build_dashboard_damage_standalone_plotly',
    'build_dashboard_damage_share_plotly',
    'build_dashboard_table_breakdown_plotly',
]
