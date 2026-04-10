from __future__ import annotations

from typing import Any, Dict

from app.plotly_bundle import build_empty_plotly_figure_payload, empty_plotly_payload, serialize_plotly_figure
from app.statistics_constants import PLOTLY_PALETTE


def build_chart_bundle(title: str, figure: Any) -> Dict[str, Any]:
    return {
        "title": title,
        "plotly": serialize_plotly_figure(figure),
        "empty_message": "",
    }


def build_empty_chart_bundle(
    title: str,
    message: str,
    *,
    annotation_color: str = "#61758d",
    use_plotly_placeholder: bool = True,
) -> Dict[str, Any]:
    plotly_payload = (
        build_empty_plotly_figure_payload(message, annotation_color=annotation_color)
        if use_plotly_placeholder
        else empty_plotly_payload()
    )
    return {
        "title": title,
        "plotly": plotly_payload,
        "empty_message": message,
    }


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
    layout = {
        "height": height,
        "showlegend": showlegend,
        "paper_bgcolor": "rgba(255,255,255,0)",
        "plot_bgcolor": "rgba(255,255,255,0)",
        "font": {"family": 'Bahnschrift, "Segoe UI", "Trebuchet MS", sans-serif', "color": PLOTLY_PALETTE["ink"]},
        "margin": {"l": margin_left, "r": margin_right, "t": margin_top, "b": margin_bottom},
        "hoverlabel": {"bgcolor": hover_bgcolor, "font": {"color": PLOTLY_PALETTE["ink"]}},
    }
    if include_xy_axes:
        layout["xaxis"] = {"showgrid": False, "zeroline": False}
        layout["yaxis"] = {"title": yaxis_title, "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False}
    return layout
