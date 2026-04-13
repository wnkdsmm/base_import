from __future__ import annotations

from typing import Any, Dict, Sequence

from app.plotly_bundle import (
    build_empty_plotly_figure_payload,
    empty_plotly_payload,
    go,
    serialize_plotly_figure,
)
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


def build_plotly_palette(
    color_keys: Sequence[str],
    *,
    fallback_key: str = "sky",
    limit: int | None = None,
) -> list[str]:
    fallback_color = PLOTLY_PALETTE.get(fallback_key, "#6f92b8")
    values = [PLOTLY_PALETTE.get(color_key, fallback_color) for color_key in color_keys]
    return values[:limit] if limit is not None else values


def build_horizontal_legend(*, y: float = 1.12, x: float = 0.0) -> Dict[str, Any]:
    return {"orientation": "h", "y": y, "x": x}


def merge_plotly_layout(
    base_layout: Dict[str, Any] | None = None,
    *,
    xaxis: Dict[str, Any] | None = None,
    yaxis: Dict[str, Any] | None = None,
    legend: Dict[str, Any] | None = None,
    updates: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
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


def build_plotly_marker(
    *,
    color: Any,
    size: Any = None,
    opacity: float | None = None,
    line_color: str | None = None,
    line_width: float | None = None,
    extra: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    marker: Dict[str, Any] = {"color": color}
    if size is not None:
        marker["size"] = size
    if opacity is not None:
        marker["opacity"] = opacity

    line: Dict[str, Any] = {}
    if line_color is not None:
        line["color"] = line_color
    if line_width is not None:
        line["width"] = line_width
    if line:
        marker["line"] = line

    if extra:
        marker.update(extra)
    return marker


def build_plotly_line(
    *,
    color: str,
    width: float | None = None,
    dash: str | None = None,
    extra: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    line: Dict[str, Any] = {"color": color}
    if width is not None:
        line["width"] = width
    if dash is not None:
        line["dash"] = dash
    if extra:
        line.update(extra)
    return line


def build_plotly_bar_trace(**kwargs: Any) -> Any:
    return go.Bar(**kwargs)


def build_plotly_scatter_trace(**kwargs: Any) -> Any:
    return go.Scatter(**kwargs)


def build_plotly_scattergl_trace(**kwargs: Any) -> Any:
    return go.Scattergl(**kwargs)


def build_plotly_pie_trace(**kwargs: Any) -> Any:
    return go.Pie(**kwargs)


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
) -> Dict[str, Any]:
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
    font: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
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


def build_plotly_figure(
    traces: Sequence[Any] | None = None,
    *,
    layout: Dict[str, Any] | None = None,
    layout_updates: Dict[str, Any] | None = None,
) -> Any:
    figure = go.Figure(data=list(traces or ()))
    if layout:
        figure.update_layout(**layout)
    if layout_updates:
        figure.update_layout(**layout_updates)
    return figure


def build_plotly_payload_from_traces(
    traces: Sequence[Any],
    *,
    layout: Dict[str, Any] | None = None,
    layout_updates: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    figure = build_plotly_figure(traces=traces, layout=layout, layout_updates=layout_updates)
    return serialize_plotly_figure(figure)


def build_plotly_bar_payload(
    *,
    x: Sequence[Any],
    y: Sequence[Any],
    layout: Dict[str, Any],
    text: Sequence[Any] | None = None,
    textposition: str | None = "outside",
    name: str | None = None,
    orientation: str = "v",
    marker: Dict[str, Any] | None = None,
    customdata: Sequence[Any] | None = None,
    hovertemplate: str | None = None,
    layout_updates: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    trace_kwargs: Dict[str, Any] = {
        "x": x,
        "y": y,
        "orientation": orientation,
    }
    if text is not None:
        trace_kwargs["text"] = text
    if textposition is not None:
        trace_kwargs["textposition"] = textposition
    if name is not None:
        trace_kwargs["name"] = name
    if marker is not None:
        trace_kwargs["marker"] = marker
    if customdata is not None:
        trace_kwargs["customdata"] = customdata
    if hovertemplate is not None:
        trace_kwargs["hovertemplate"] = hovertemplate
    return build_plotly_payload_from_traces(
        [build_plotly_bar_trace(**trace_kwargs)],
        layout=layout,
        layout_updates=layout_updates,
    )


def build_plotly_scatter_payload(
    *,
    x: Sequence[Any],
    y: Sequence[Any],
    layout: Dict[str, Any],
    mode: str = "lines",
    name: str | None = None,
    fill: str | None = None,
    line: Dict[str, Any] | None = None,
    marker: Dict[str, Any] | None = None,
    text: Sequence[Any] | None = None,
    customdata: Sequence[Any] | None = None,
    hovertemplate: str | None = None,
    layout_updates: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    trace_kwargs: Dict[str, Any] = {
        "x": x,
        "y": y,
        "mode": mode,
    }
    if name is not None:
        trace_kwargs["name"] = name
    if fill is not None:
        trace_kwargs["fill"] = fill
    if line is not None:
        trace_kwargs["line"] = line
    if marker is not None:
        trace_kwargs["marker"] = marker
    if text is not None:
        trace_kwargs["text"] = text
    if customdata is not None:
        trace_kwargs["customdata"] = customdata
    if hovertemplate is not None:
        trace_kwargs["hovertemplate"] = hovertemplate
    return build_plotly_payload_from_traces(
        [build_plotly_scatter_trace(**trace_kwargs)],
        layout=layout,
        layout_updates=layout_updates,
    )


def build_plotly_pie_payload(
    *,
    labels: Sequence[Any],
    values: Sequence[Any],
    layout: Dict[str, Any],
    hole: float = 0.0,
    sort: bool = False,
    marker: Dict[str, Any] | None = None,
    textinfo: str | None = None,
    hovertemplate: str | None = None,
    layout_updates: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    trace_kwargs: Dict[str, Any] = {
        "labels": labels,
        "values": values,
        "hole": hole,
        "sort": sort,
    }
    if marker is not None:
        trace_kwargs["marker"] = marker
    if textinfo is not None:
        trace_kwargs["textinfo"] = textinfo
    if hovertemplate is not None:
        trace_kwargs["hovertemplate"] = hovertemplate
    return build_plotly_payload_from_traces(
        [build_plotly_pie_trace(**trace_kwargs)],
        layout=layout,
        layout_updates=layout_updates,
    )


def build_item_vertical_bar_payload(
    items: Sequence[Dict[str, Any]],
    *,
    layout: Dict[str, Any],
    hovertemplate: str,
    colors: Any,
    line_color: str | None = None,
    line_width: float | None = None,
    customdata: Any = None,
    layout_updates: Dict[str, Any] | None = None,
    label_key: str = "label",
    value_key: str = "value",
    text_key: str = "value_display",
) -> Dict[str, Any]:
    return build_plotly_bar_payload(
        x=[item[label_key] for item in items],
        y=[item[value_key] for item in items],
        text=[item[text_key] for item in items],
        textposition="outside",
        marker=build_plotly_marker(color=colors, line_color=line_color, line_width=line_width),
        customdata=customdata,
        hovertemplate=hovertemplate,
        layout=layout,
        layout_updates=layout_updates,
    )


def build_item_horizontal_bar_payload(
    items: Sequence[Dict[str, Any]],
    *,
    layout: Dict[str, Any],
    y_values: Sequence[Any],
    hovertemplate: str,
    color: Any,
    line_color: str | None = None,
    line_width: float | None = 1.2,
    customdata: Any = None,
    text_values: Sequence[Any] | None = None,
    layout_updates: Dict[str, Any] | None = None,
    value_key: str = "value",
    text_key: str = "value_display",
) -> Dict[str, Any]:
    return build_plotly_bar_payload(
        x=[item[value_key] for item in items],
        y=y_values,
        orientation="h",
        text=list(text_values) if text_values is not None else [item[text_key] for item in items],
        textposition="outside",
        customdata=customdata,
        marker=build_plotly_marker(color=color, line_color=line_color, line_width=line_width),
        hovertemplate=hovertemplate,
        layout=layout,
        layout_updates=layout_updates,
    )


def build_item_pie_payload(
    items: Sequence[Dict[str, Any]],
    *,
    layout: Dict[str, Any],
    colors: Sequence[str],
    hole: float,
    hovertemplate: str,
    margin: Dict[str, int],
    label_key: str = "label",
    value_key: str = "value",
) -> Dict[str, Any]:
    return build_plotly_pie_payload(
        labels=[item[label_key] for item in items],
        values=[item[value_key] for item in items],
        hole=hole,
        sort=False,
        marker={"colors": list(colors)[: len(items)]},
        textinfo="label+percent",
        hovertemplate=hovertemplate,
        layout=layout,
        layout_updates={"margin": margin},
    )


def build_component_projection(
    rows: Sequence[Dict[str, Any]],
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
) -> list[Dict[str, Any]]:
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
) -> list[Dict[str, Any]]:
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
