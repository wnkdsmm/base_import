from __future__ import annotations

from typing import Any, Dict, Sequence

from app.plotly_bundle import build_empty_plotly_figure_payload, empty_plotly_payload, serialize_plotly_figure
from app.statistics_constants import PLOTLY_PALETTE


def build_chart_bundle(
    title: str,
    figure: Any,
    *,
    empty_message: str = "",
    description: str = "",
) -> Dict[str, Any]:
    bundle = {
        "title": title,
        "plotly": serialize_plotly_figure(figure),
        "empty_message": empty_message,
    }
    if description:
        bundle["description"] = description
    return bundle


def build_empty_plotly_payload(
    message: str,
    *,
    annotation_color: str = "#61758d",
    use_plotly_placeholder: bool = True,
) -> Dict[str, Any]:
    return (
        build_empty_plotly_figure_payload(message, annotation_color=annotation_color)
        if use_plotly_placeholder
        else empty_plotly_payload()
    )


def build_empty_chart_bundle(
    title: str,
    message: str,
    *,
    annotation_color: str = "#61758d",
    use_plotly_placeholder: bool = True,
    description: str = "",
) -> Dict[str, Any]:
    plotly_payload = build_empty_plotly_payload(
        message,
        annotation_color=annotation_color,
        use_plotly_placeholder=use_plotly_placeholder,
    )
    bundle = {
        "title": title,
        "plotly": plotly_payload,
        "empty_message": message,
    }
    if description:
        bundle["description"] = description
    return bundle


def build_item_chart_bundle(
    title: str,
    items: Sequence[Dict[str, Any]],
    empty_message: str,
    *,
    plotly: Dict[str, Any] | None = None,
    description: str = "",
    annotation_color: str = "#7b6a5a",
    min_width_percent: float = 4.0,
    value_key: str = "value",
    width_key: str = "width_percent",
) -> Dict[str, Any]:
    max_value = max((float(item.get(value_key) or 0.0) for item in items), default=0.0)
    normalized_items = []
    for item in items:
        item_value = float(item.get(value_key) or 0.0)
        width_percent = 0.0
        if max_value > 0:
            width_percent = max(min_width_percent, round(item_value / max_value * 100, 2))
        updated = dict(item)
        updated[width_key] = width_percent
        normalized_items.append(updated)
    return {
        **build_empty_chart_bundle(
            title,
            empty_message,
            annotation_color=annotation_color,
            description=description,
        ),
        "items": normalized_items,
        "plotly": plotly or build_empty_plotly_payload(empty_message, annotation_color=annotation_color),
    }


def build_plotly_layout(
    yaxis_title: str = "",
    *,
    height: int = 340,
    showlegend: bool = True,
    include_xy_axes: bool = True,
    margin_left: int = 52,
    margin_right: int = 42,
    margin_top: int = 24,
    margin_bottom: int = 48,
    hover_bgcolor: str = "#fbfdff",
    paper_bgcolor: str = "rgba(255,255,255,0)",
    plot_bgcolor: str = "rgba(255,255,255,0)",
    bargap: float | None = None,
    axis_tickfont_size: int | None = None,
    axis_title_font_size: int | None = None,
) -> Dict[str, Any]:
    layout = {
        "height": height,
        "showlegend": showlegend,
        "paper_bgcolor": paper_bgcolor,
        "plot_bgcolor": plot_bgcolor,
        "font": {"family": 'Bahnschrift, "Segoe UI", "Trebuchet MS", sans-serif', "color": PLOTLY_PALETTE["ink"]},
        "margin": {"l": margin_left, "r": margin_right, "t": margin_top, "b": margin_bottom},
        "hoverlabel": {"bgcolor": hover_bgcolor, "font": {"color": PLOTLY_PALETTE["ink"]}},
    }
    if bargap is not None:
        layout["bargap"] = bargap
    if include_xy_axes:
        xaxis = {"showgrid": False, "zeroline": False}
        yaxis = {"title": yaxis_title, "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False}
        if axis_tickfont_size is not None:
            xaxis["tickfont"] = {"size": axis_tickfont_size}
            yaxis["tickfont"] = {"size": axis_tickfont_size}
        if axis_title_font_size is not None:
            yaxis["title_font"] = {"size": axis_title_font_size}
        layout["xaxis"] = xaxis
        layout["yaxis"] = yaxis
    return layout


def build_service_plotly_layout(
    yaxis_title: str = "",
    *,
    height: int = 340,
    showlegend: bool = True,
    include_xy_axes: bool = True,
    margin_left: int = 52,
    margin_right: int = 42,
    margin_top: int = 24,
    margin_bottom: int = 48,
    hover_bgcolor: str = "#fbfdff",
) -> Dict[str, Any]:
    return build_plotly_layout(
        yaxis_title,
        height=height,
        showlegend=showlegend,
        include_xy_axes=include_xy_axes,
        margin_left=margin_left,
        margin_right=margin_right,
        margin_top=margin_top,
        margin_bottom=margin_bottom,
        hover_bgcolor=hover_bgcolor,
    )


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
) -> Dict[str, Any]:
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
