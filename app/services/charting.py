from __future__ import annotations

from typing import Any, Dict, Sequence, TypedDict

from app.plotly_bundle import (
    build_empty_plotly_figure_payload,
    empty_plotly_payload,
    go,
    serialize_plotly_figure,
)
from app.services.chart_utils import (
    PlotlyLayout,
    PlotlyTrace,
)


class ChartConfig(TypedDict, total=False):
    title: str
    plotly: PlotlyTrace
    empty_message: str
    description: str
    items: Sequence[PlotlyTrace]


def build_chart_bundle(
    title: str,
    figure: Any,
    *,
    empty_message: str = "",
    description: str = "",
) -> ChartConfig:
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
) -> PlotlyTrace:
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
) -> ChartConfig:
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


def build_plotly_unavailable_chart_bundle(
    title: str,
    message: str,
    *,
    annotation_color: str = "#61758d",
    description: str = "",
) -> ChartConfig:
    return build_empty_chart_bundle(
        title,
        message,
        annotation_color=annotation_color,
        use_plotly_placeholder=False,
        description=description,
    )


def build_item_chart_bundle(
    title: str,
    items: Sequence[PlotlyTrace],
    empty_message: str,
    *,
    plotly: PlotlyTrace | None = None,
    description: str = "",
    annotation_color: str = "#7b6a5a",
    min_width_percent: float = 4.0,
    value_key: str = "value",
    width_key: str = "width_percent",
) -> ChartConfig:
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


def build_plotly_marker(
    *,
    color: Any,
    size: Any = None,
    opacity: float | None = None,
    line_color: str | None = None,
    line_width: float | None = None,
    extra: PlotlyTrace | None = None,
) -> PlotlyTrace:
    marker: dict[str, Any] = {"color": color}
    if size is not None:
        marker["size"] = size
    if opacity is not None:
        marker["opacity"] = opacity

    line: dict[str, Any] = {}
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
    extra: PlotlyTrace | None = None,
) -> PlotlyTrace:
    line: dict[str, Any] = {"color": color}
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


def build_plotly_figure(
    traces: Sequence[Any] | None = None,
    *,
    layout: PlotlyLayout | None = None,
    layout_updates: PlotlyLayout | None = None,
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
    layout: PlotlyLayout | None = None,
    layout_updates: PlotlyLayout | None = None,
) -> PlotlyTrace:
    figure = build_plotly_figure(traces=traces, layout=layout, layout_updates=layout_updates)
    return serialize_plotly_figure(figure)


def build_plotly_bar_payload(
    *,
    x: Sequence[Any],
    y: Sequence[Any],
    layout: PlotlyLayout,
    text: Sequence[Any] | None = None,
    textposition: str | None = "outside",
    name: str | None = None,
    orientation: str = "v",
    marker: PlotlyTrace | None = None,
    customdata: Sequence[Any] | None = None,
    hovertemplate: str | None = None,
    layout_updates: PlotlyLayout | None = None,
) -> PlotlyTrace:
    trace_kwargs: dict[str, Any] = {
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
    layout: PlotlyLayout,
    mode: str = "lines",
    name: str | None = None,
    fill: str | None = None,
    line: PlotlyTrace | None = None,
    marker: PlotlyTrace | None = None,
    text: Sequence[Any] | None = None,
    customdata: Sequence[Any] | None = None,
    hovertemplate: str | None = None,
    layout_updates: PlotlyLayout | None = None,
) -> PlotlyTrace:
    trace_kwargs: dict[str, Any] = {
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
    layout: PlotlyLayout,
    hole: float = 0.0,
    sort: bool = False,
    marker: PlotlyTrace | None = None,
    textinfo: str | None = None,
    hovertemplate: str | None = None,
    layout_updates: PlotlyLayout | None = None,
) -> PlotlyTrace:
    trace_kwargs: dict[str, Any] = {
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
    items: Sequence[PlotlyTrace],
    *,
    layout: PlotlyLayout,
    hovertemplate: str,
    colors: Any,
    line_color: str | None = None,
    line_width: float | None = None,
    customdata: Any = None,
    layout_updates: PlotlyLayout | None = None,
    label_key: str = "label",
    value_key: str = "value",
    text_key: str = "value_display",
) -> PlotlyTrace:
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
    items: Sequence[PlotlyTrace],
    *,
    layout: PlotlyLayout,
    y_values: Sequence[Any],
    hovertemplate: str,
    color: Any,
    line_color: str | None = None,
    line_width: float | None = 1.2,
    customdata: Any = None,
    text_values: Sequence[Any] | None = None,
    layout_updates: PlotlyLayout | None = None,
    value_key: str = "value",
    text_key: str = "value_display",
) -> PlotlyTrace:
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
    items: Sequence[PlotlyTrace],
    *,
    layout: PlotlyLayout,
    colors: Sequence[str],
    hole: float,
    hovertemplate: str,
    margin: Dict[str, int],
    label_key: str = "label",
    value_key: str = "value",
) -> PlotlyTrace:
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
