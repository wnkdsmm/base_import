from __future__ import annotations

from typing import Any, Dict, Sequence, TypedDict

from app.statistics_constants import PLOTLY_PALETTE


class PlotlyLayout(TypedDict, total=False):
    height: int
    showlegend: bool
    paper_bgcolor: str
    plot_bgcolor: str
    font: dict[str, Any]
    margin: dict[str, Any]
    hoverlabel: dict[str, Any]
    bargap: float
    xaxis: dict[str, Any]
    yaxis: dict[str, Any]
    legend: dict[str, Any]
    yaxis2: dict[str, Any]
    shapes: list[dict[str, Any]]
    annotations: list[dict[str, Any]]


class PlotlyTrace(TypedDict, total=False):
    type: str
    x: Any
    y: Any
    mode: str
    name: str
    text: Any
    labels: Any
    values: Any
    marker: dict[str, Any]
    line: dict[str, Any]
    fill: str
    hovertemplate: str
    customdata: Any
    orientation: str
    textposition: str
    hole: float
    sort: bool


def build_plotly_palette(
    color_keys: Sequence[str],
    *,
    fallback_key: str = "sky",
    limit: int | None = None,
) -> list[str]:
    fallback_color = PLOTLY_PALETTE.get(fallback_key, "#6f92b8")
    values = [PLOTLY_PALETTE.get(color_key, fallback_color) for color_key in color_keys]
    return values[:limit] if limit is not None else values


def build_horizontal_legend(*, y: float = 1.12, x: float = 0.0) -> PlotlyLayout:
    return {"orientation": "h", "y": y, "x": x}


def merge_plotly_layout(
    base_layout: PlotlyLayout | None = None,
    *,
    xaxis: PlotlyTrace | None = None,
    yaxis: PlotlyTrace | None = None,
    legend: PlotlyTrace | None = None,
    updates: PlotlyTrace | None = None,
) -> PlotlyLayout:
    layout = dict(base_layout or {})

    if xaxis is not None:
        current_xaxis = dict(layout.get("xaxis", {}))
        current_xaxis.update(xaxis)
        layout["xaxis"] = current_xaxis
    if yaxis is not None:
        current_yaxis = dict(layout.get("yaxis", {}))
        current_yaxis.update(yaxis)
        layout["yaxis"] = current_yaxis
    if legend is not None:
        current_legend = dict(layout.get("legend", {}))
        current_legend.update(legend)
        layout["legend"] = current_legend
    if updates:
        layout.update(updates)
    return layout


def build_vertical_reference_line(
    x_value: Any,
    color: str,
    *,
    xref: str = "x",
    yref: str = "paper",
    y0: float = 0.0,
    y1: float = 1.0,
    width: float = 1.6,
    dash: str = "dot",
) -> PlotlyTrace:
    return {
        "type": "line",
        "xref": xref,
        "yref": yref,
        "x0": x_value,
        "x1": x_value,
        "y0": y0,
        "y1": y1,
        "line": {"color": color, "width": width, "dash": dash},
    }


def build_plotly_annotation(
    *,
    x: Any,
    y: Any,
    text: str,
    showarrow: bool = True,
    xref: str | None = None,
    yref: str | None = None,
    xanchor: str | None = None,
    yanchor: str | None = None,
    ax: int | None = None,
    ay: int | None = None,
    arrowhead: int | None = None,
    arrowcolor: str | None = None,
    bgcolor: str | None = None,
    font: PlotlyTrace | None = None,
) -> PlotlyTrace:
    annotation: Dict[str, Any] = {
        "x": x,
        "y": y,
        "text": text,
        "showarrow": showarrow,
    }
    if xref is not None:
        annotation["xref"] = xref
    if yref is not None:
        annotation["yref"] = yref
    if xanchor is not None:
        annotation["xanchor"] = xanchor
    if yanchor is not None:
        annotation["yanchor"] = yanchor
    if ax is not None:
        annotation["ax"] = ax
    if ay is not None:
        annotation["ay"] = ay
    if arrowhead is not None:
        annotation["arrowhead"] = arrowhead
    if arrowcolor is not None:
        annotation["arrowcolor"] = arrowcolor
    if bgcolor is not None:
        annotation["bgcolor"] = bgcolor
    if font is not None:
        annotation["font"] = font
    return annotation


def build_component_projection(
    rows: Sequence[PlotlyTrace],
    *,
    component_keys: Sequence[str],
) -> Any:
    import numpy as np

    matrix = np.array(
        [
            [float(row.get(key) or 0.0) for key in component_keys]
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


def build_unique_vertical_reference_lines(
    references: Sequence[tuple[Any, str]],
) -> list[PlotlyTrace]:
    lines: list[Dict[str, Any]] = []
    seen: set[Any] = set()
    for value, color in references:
        if value is None or value in seen:
            continue
        seen.add(value)
        lines.append(build_vertical_reference_line(value, color))
    return lines


def build_reference_annotations(
    *,
    y_value: float,
    references: Sequence[tuple[Any, str, str]],
    xref: str = "x",
    yref: str = "y",
    ay: int = -26,
    arrowhead: int = 2,
    font_size: int = 11,
    bgcolor: str = "rgba(255,255,255,0.75)",
) -> list[PlotlyTrace]:
    annotations: list[Dict[str, Any]] = []
    for value, text, color in references:
        if value is None:
            continue
        annotations.append(
            build_plotly_annotation(
                x=value,
                y=y_value,
                xref=xref,
                yref=yref,
                text=text,
                showarrow=True,
                arrowhead=arrowhead,
                ax=0,
                ay=ay,
                font={"size": font_size, "color": color},
                arrowcolor=color,
                bgcolor=bgcolor,
            )
        )
    return annotations


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
) -> PlotlyLayout:
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
) -> PlotlyLayout:
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


def build_service_scatter_layout(
    *,
    xaxis_title: str,
    yaxis_title: str,
    height: int = 420,
    include_xy_axes: bool = False,
    legend_y: float = 1.1,
    margin_right: int = 42,
    hover_bgcolor: str = "#fbfdff",
) -> PlotlyLayout:
    return merge_plotly_layout(
        build_service_plotly_layout(
            "",
            height=height,
            include_xy_axes=include_xy_axes,
            margin_right=margin_right,
            hover_bgcolor=hover_bgcolor,
        ),
        xaxis={"title": xaxis_title, "showgrid": False, "zeroline": False},
        yaxis={"title": yaxis_title, "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False},
        legend=build_horizontal_legend(y=legend_y),
    )


def plotly_layout(
    yaxis_title: str,
    height: int = 340,
    *,
    margin_right: int | None = None,
    hover_bgcolor: str | None = None,
) -> PlotlyLayout:
    kwargs: Dict[str, Any] = {"height": height}
    if margin_right is not None:
        kwargs["margin_right"] = margin_right
    if hover_bgcolor is not None:
        kwargs["hover_bgcolor"] = hover_bgcolor
    return build_service_plotly_layout(yaxis_title, **kwargs)
