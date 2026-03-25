from __future__ import annotations

import re
import textwrap
from typing import Any, Dict, List, Optional

from app.plotly_bundle import PLOTLY_AVAILABLE, empty_plotly_payload, go, serialize_plotly_figure
from app.statistics_constants import PLOTLY_PALETTE

def _finalize_chart(
    title: str,
    items: List[Dict[str, Any]],
    empty_message: str,
    plotly: Optional[Dict[str, Any]] = None,
    description: str = "",
) -> Dict[str, Any]:
    max_value = max([float(item["value"]) for item in items], default=0)
    normalized_items = []
    for item in items:
        width_percent = 0 if max_value <= 0 else max(4, round(float(item["value"]) / max_value * 100, 2))
        updated = dict(item)
        updated["width_percent"] = width_percent
        normalized_items.append(updated)
    return {
        "title": title,
        "description": description,
        "items": normalized_items,
        "empty_message": empty_message,
        "plotly": plotly or _build_empty_plotly_chart(title, empty_message),
    }

def _build_yearly_plotly(title: str, items: List[Dict[str, Any]], metric: str, empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    x_values = [item["label"] for item in items]
    y_values = [item["value"] for item in items]
    text_values = [item["value_display"] for item in items]

    if metric == "count":
        figure = go.Figure(
            data=[
                go.Bar(
                    x=x_values,
                    y=y_values,
                    text=text_values,
                    textposition="outside",
                    marker=dict(
                        color=PLOTLY_PALETTE["fire"],
                        line=dict(color=PLOTLY_PALETTE["fire_soft"], width=1.5),
                    ),
                    hovertemplate="<b>%{x}</b><br>Пожаров: %{customdata}<extra></extra>",
                    customdata=text_values,
                )
            ]
        )
        figure.update_layout(**_plotly_layout("Пожаров", showlegend=False))
    else:
        figure = go.Figure(
            data=[
                go.Scatter(
                    x=x_values,
                    y=y_values,
                    mode="lines+markers",
                    fill="tozeroy",
                    line=dict(color=PLOTLY_PALETTE["forest"], width=4),
                    marker=dict(size=9, color=PLOTLY_PALETTE["forest_soft"]),
                    hovertemplate="<b>%{x}</b><br>Площадь: %{customdata} га<extra></extra>",
                    customdata=text_values,
                )
            ]
        )
        figure.update_layout(**_plotly_layout("Площадь, га", showlegend=False))

    return _figure_to_dict(figure)


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
        return _build_empty_plotly_chart(title, empty_message)

    ordered_items = list(reversed(items))
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["value"] for item in ordered_items],
                y=[_wrap_plotly_label(item["label"], max_width=34, max_lines=2) for item in ordered_items],
                orientation="h",
                text=[item["value_display"] for item in ordered_items],
                textposition="outside",
                customdata=[item["label"] for item in ordered_items],
                marker=dict(
                    color=PLOTLY_PALETTE["fire"],
                    line=dict(color=PLOTLY_PALETTE["fire_soft"], width=1.2),
                ),
                hovertemplate="<b>%{customdata}</b><br>Пожаров: %{text}<extra></extra>",
            )
        ]
    )
    layout = _plotly_layout("Количество пожаров", showlegend=False)
    layout["height"] = min(620, max(360, 34 * len(items) + 90))
    layout["margin"] = {"l": 320, "r": 72, "t": 20, "b": 36}
    layout["bargap"] = 0.62
    layout["xaxis"]["automargin"] = True
    layout["yaxis"]["automargin"] = True
    layout["yaxis"]["tickfont"] = {"size": 11}
    figure.update_layout(**layout)
    return _figure_to_dict(figure)


def _build_distribution_pie_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure(
        data=[
            go.Pie(
                labels=[item["label"] for item in items],
                values=[item["value"] for item in items],
                hole=0.45,
                sort=False,
                marker=dict(
                    colors=[
                        PLOTLY_PALETTE["sky"],
                        PLOTLY_PALETTE["sky_soft"],
                        PLOTLY_PALETTE["forest_soft"],
                        PLOTLY_PALETTE["forest"],
                        PLOTLY_PALETTE["sand"],
                        PLOTLY_PALETTE["fire_soft"],
                    ][: len(items)]
                ),
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>Записей: %{value}<br>Доля: %{percent}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("", showlegend=False))
    figure.update_layout(margin=dict(l=24, r=24, t=12, b=12))
    return _figure_to_dict(figure)


def _build_distribution_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    ordered_items = list(reversed(items))
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["value"] for item in ordered_items],
                y=[_wrap_plotly_label(item["label"], max_width=26, max_lines=2) for item in ordered_items],
                orientation="h",
                text=[item["value_display"] for item in ordered_items],
                textposition="outside",
                customdata=[item["label"] for item in ordered_items],
                marker=dict(
                    color=PLOTLY_PALETTE["sky"],
                    line=dict(color=PLOTLY_PALETTE["sky_soft"], width=1.2),
                ),
                hovertemplate="<b>%{customdata}</b><br>Записей: %{text}<extra></extra>",
            )
        ]
    )
    layout = _plotly_layout("Количество записей", showlegend=False)
    layout["height"] = max(340, 36 * len(items) + 90)
    layout["margin"] = {"l": 220, "r": 36, "t": 20, "b": 40}
    layout["yaxis"]["automargin"] = True
    figure.update_layout(**layout)
    return _figure_to_dict(figure)


def _build_compact_metric_plotly(title: str, items: List[Dict[str, Any]], empty_message: str, color_key: str, yaxis_title: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    palette = {
        "fire": [PLOTLY_PALETTE["fire"], PLOTLY_PALETTE["fire_soft"]],
        "sky": [PLOTLY_PALETTE["sky"], PLOTLY_PALETTE["sky_soft"]],
        "forest": [PLOTLY_PALETTE["forest"], PLOTLY_PALETTE["forest_soft"]],
    }
    colors = palette.get(color_key, [PLOTLY_PALETTE["sand"], PLOTLY_PALETTE["sand_soft"]])
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["label"] for item in items],
                y=[item["value"] for item in items],
                text=[item["value_display"] for item in items],
                textposition="outside",
                marker=dict(
                    color=[colors[index % len(colors)] for index in range(len(items))],
                    line=dict(color="rgba(255,255,255,0.5)", width=1.2),
                ),
                hovertemplate="<b>%{x}</b><br>Людей: %{text}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout(yaxis_title, showlegend=False))
    return _figure_to_dict(figure)


def _build_evacuation_children_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure(
        data=[
            go.Pie(
                labels=[item["label"] for item in items],
                values=[item["value"] for item in items],
                hole=0.5,
                sort=False,
                marker=dict(
                    colors=[
                        PLOTLY_PALETTE["sky"],
                        PLOTLY_PALETTE["sky_soft"],
                        PLOTLY_PALETTE["forest"],
                        PLOTLY_PALETTE["forest_soft"],
                    ][: len(items)]
                ),
                textinfo="label+value",
                hovertemplate="<b>%{label}</b><br>Людей: %{value}<br>Доля: %{percent}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("", showlegend=False))
    figure.update_layout(margin=dict(l=24, r=24, t=12, b=12))
    return _figure_to_dict(figure)


def _build_combined_impact_timeline_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    x_values = [item["date_value"] for item in items]
    date_labels = [item["label"] for item in items]

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=x_values,
            y=[item["deaths"] for item in items],
            name="Погибшие",
            customdata=date_labels,
            marker=dict(color=PLOTLY_PALETTE["fire"]),
            hovertemplate="<b>%{customdata}</b><br>Погибшие: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=x_values,
            y=[item["injuries"] for item in items],
            name="Травмированные",
            customdata=date_labels,
            marker=dict(color=PLOTLY_PALETTE["sand"]),
            hovertemplate="<b>%{customdata}</b><br>Травмированные: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=x_values,
            y=[item["evacuated_adults"] for item in items],
            name="Эвакуировано",
            customdata=date_labels,
            mode="lines+markers",
            line=dict(color=PLOTLY_PALETTE["sky"], width=3),
            marker=dict(size=7, color=PLOTLY_PALETTE["sky_soft"]),
            hovertemplate="<b>%{customdata}</b><br>Эвакуировано: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=x_values,
            y=[item["evacuated_children"] for item in items],
            name="Эвакуировано детей",
            customdata=date_labels,
            mode="lines+markers",
            line=dict(color=PLOTLY_PALETTE["forest"], width=3),
            marker=dict(size=7, color=PLOTLY_PALETTE["forest_soft"]),
            hovertemplate="<b>%{customdata}</b><br>Эвакуировано детей: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=x_values,
            y=[item["rescued_children"] for item in items],
            name="Спасено детей",
            customdata=date_labels,
            mode="lines+markers",
            line=dict(color=PLOTLY_PALETTE["ink"], width=2, dash="dot"),
            marker=dict(size=6, color=PLOTLY_PALETTE["fire_soft"]),
            hovertemplate="<b>%{customdata}</b><br>Спасено детей: %{y}<extra></extra>",
        )
    )
    layout = _plotly_layout("Люди", showlegend=True)
    layout["barmode"] = "group"
    layout["legend"] = {"orientation": "h", "y": 1.14, "x": 0}
    layout["xaxis"]["type"] = "date"
    figure.update_layout(**layout)
    return _figure_to_dict(figure)

def _build_damage_overview_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    ordered_items = list(reversed(items))
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["value"] for item in ordered_items],
                y=[_wrap_plotly_label(item["label"], max_width=24, max_lines=2) for item in ordered_items],
                orientation="h",
                text=[item["value_display"] for item in ordered_items],
                textposition="outside",
                customdata=[item["label"] for item in ordered_items],
                marker=dict(
                    color=PLOTLY_PALETTE["sand"],
                    line=dict(color=PLOTLY_PALETTE["sand_soft"], width=1.2),
                ),
                hovertemplate="<b>%{customdata}</b><br>Пожаров с ненулевым показателем: %{text}<extra></extra>",
            )
        ]
    )
    layout = _plotly_layout("Количество пожаров", showlegend=False)
    layout["height"] = max(360, 38 * len(items) + 90)
    layout["margin"] = {"l": 230, "r": 36, "t": 20, "b": 40}
    layout["yaxis"]["automargin"] = True
    figure.update_layout(**layout)
    return _figure_to_dict(figure)


def _build_damage_pairs_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=[_wrap_plotly_label(item["label"], max_width=16, max_lines=2) for item in items],
            y=[item["destroyed"] for item in items],
            name="Уничтожено",
            customdata=[item["label"] for item in items],
            marker=dict(color=PLOTLY_PALETTE["fire"]),
            hovertemplate="<b>%{customdata}</b><br>Пожаров с уничтожением: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=[_wrap_plotly_label(item["label"], max_width=16, max_lines=2) for item in items],
            y=[item["damaged"] for item in items],
            name="Повреждено",
            customdata=[item["label"] for item in items],
            marker=dict(color=PLOTLY_PALETTE["sky"]),
            hovertemplate="<b>%{customdata}</b><br>Пожаров с повреждением: %{y}<extra></extra>",
        )
    )
    layout = _plotly_layout("Количество пожаров", showlegend=True)
    layout["barmode"] = "group"
    layout["legend"] = {"orientation": "h", "y": 1.12, "x": 0}
    layout["xaxis"]["automargin"] = True
    figure.update_layout(**layout)
    return _figure_to_dict(figure)


def _build_damage_standalone_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure(
        data=[
            go.Bar(
                x=[_wrap_plotly_label(item["label"], max_width=16, max_lines=2) for item in items],
                y=[item["value"] for item in items],
                text=[item["value_display"] for item in items],
                textposition="outside",
                customdata=[item["label"] for item in items],
                marker=dict(
                    color=[[PLOTLY_PALETTE["forest"], PLOTLY_PALETTE["sky"], PLOTLY_PALETTE["sand"], PLOTLY_PALETTE["fire_soft"]][index % 4] for index in range(len(items))],
                    line=dict(color="rgba(255,255,255,0.6)", width=1.2),
                ),
                hovertemplate="<b>%{customdata}</b><br>Пожаров с показателем: %{text}<extra></extra>",
            )
        ]
    )
    layout = _plotly_layout("Количество пожаров", showlegend=False)
    layout["xaxis"]["automargin"] = True
    figure.update_layout(**layout)
    return _figure_to_dict(figure)

def _build_damage_share_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure(
        data=[
            go.Pie(
                labels=[item["label"] for item in items],
                values=[item["value"] for item in items],
                hole=0.46,
                sort=False,
                marker=dict(
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
                    ][: len(items)]
                ),
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>Пожаров: %{value}<br>Доля: %{percent}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("", showlegend=False))
    figure.update_layout(margin=dict(l=24, r=24, t=12, b=12))
    return _figure_to_dict(figure)

def _build_table_breakdown_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    ordered_items = list(reversed(items))
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["value"] for item in ordered_items],
                y=[item["label"] for item in ordered_items],
                orientation="h",
                text=[item["value_display"] for item in ordered_items],
                textposition="outside",
                marker=dict(
                    color=PLOTLY_PALETTE["sand"],
                    line=dict(color=PLOTLY_PALETTE["sand_soft"], width=1.2),
                ),
                hovertemplate="<b>%{y}</b><br>Пожаров: %{text}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("Количество пожаров", showlegend=False))
    return _figure_to_dict(figure)


def _build_monthly_profile_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["label"] for item in items],
                y=[item["value"] for item in items],
                text=[item["value_display"] for item in items],
                textposition="outside",
                marker=dict(
                    color=[
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
                    line=dict(color="rgba(255,255,255,0.6)", width=1),
                ),
                hovertemplate="<b>%{x}</b><br>Пожаров: %{text}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("Количество пожаров", showlegend=False))
    return _figure_to_dict(figure)


def _build_area_bucket_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure(
        data=[
            go.Pie(
                labels=[item["label"] for item in items],
                values=[item["value"] for item in items],
                hole=0.58,
                sort=False,
                marker=dict(
                    colors=[
                        PLOTLY_PALETTE["fire"],
                        PLOTLY_PALETTE["fire_soft"],
                        PLOTLY_PALETTE["sand"],
                        PLOTLY_PALETTE["forest"],
                        PLOTLY_PALETTE["sky"],
                        "#b5aea5",
                    ][: len(items)]
                ),
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>Пожаров: %{value}<br>Доля: %{percent}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("", showlegend=False))
    figure.update_layout(margin=dict(l=20, r=20, t=10, b=10))
    return _figure_to_dict(figure)



def _build_sql_widget_bar_plotly(
    title: str,
    items: List[Dict[str, Any]],
    empty_message: str,
    color_key: str,
    value_label: str,
) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    ordered_items = list(reversed(items))
    color_value = PLOTLY_PALETTE.get(color_key, PLOTLY_PALETTE["fire"])
    line_color = PLOTLY_PALETTE.get(f"{color_key}_soft", color_value)
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["value"] for item in ordered_items],
                y=[_wrap_plotly_label(item["label"], max_width=26, max_lines=3) for item in ordered_items],
                orientation="h",
                text=[item["value_display"] for item in ordered_items],
                textposition="outside",
                customdata=[item["label"] for item in ordered_items],
                marker=dict(
                    color=color_value,
                    line=dict(color=line_color, width=1.1),
                ),
                hovertemplate=f"<b>%{{customdata}}</b><br>{value_label}: %{{text}}<extra></extra>",
            )
        ]
    )
    layout = _plotly_layout(value_label, showlegend=False)
    layout["height"] = max(320, 40 * len(items) + 80)
    layout["margin"] = {"l": 220, "r": 30, "t": 16, "b": 32}
    layout["yaxis"]["automargin"] = True
    figure.update_layout(**layout)
    return _figure_to_dict(figure)


def _build_sql_widget_season_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    colors = [
        PLOTLY_PALETTE["sky"],
        PLOTLY_PALETTE["forest"],
        PLOTLY_PALETTE["sand"],
        PLOTLY_PALETTE["fire_soft"],
    ]
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["label"] for item in items],
                y=[item["value"] for item in items],
                text=[item["value_display"] for item in items],
                textposition="outside",
                marker=dict(color=colors[: len(items)], line=dict(color="rgba(255,255,255,0.7)", width=1)),
                hovertemplate="<b>%{x}</b><br>Пожаров: %{text}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("Пожаров", showlegend=False))
    return _figure_to_dict(figure)


def _build_impact_yearly_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not PLOTLY_AVAILABLE or not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=[item["label"] for item in items],
            y=[item["deaths"] for item in items],
            name="Погибшие",
            marker=dict(color=PLOTLY_PALETTE["fire"]),
            hovertemplate="<b>%{x}</b><br>Погибшие: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=[item["label"] for item in items],
            y=[item["injuries"] for item in items],
            name="Травмированные",
            marker=dict(color=PLOTLY_PALETTE["sand"]),
            hovertemplate="<b>%{x}</b><br>Травмированные: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=[item["label"] for item in items],
            y=[item["evacuated"] for item in items],
            name="Эвакуировано",
            marker=dict(color=PLOTLY_PALETTE["sky"]),
            hovertemplate="<b>%{x}</b><br>Эвакуировано: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=[item["label"] for item in items],
            y=[item["evacuated_children"] for item in items],
            name="Эвакуировано детей",
            mode="lines+markers",
            line=dict(color=PLOTLY_PALETTE["forest"], width=3),
            marker=dict(size=8, color=PLOTLY_PALETTE["forest_soft"]),
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Эвакуировано детей: %{y}<extra></extra>",
        )
    )
    layout = _plotly_layout("Люди", showlegend=True)
    layout["barmode"] = "group"
    layout["legend"] = {"orientation": "h", "y": 1.12, "x": 0}
    layout["yaxis2"] = {
        "overlaying": "y",
        "side": "right",
        "showgrid": False,
        "title": {"text": "Дети"},
    }
    figure.update_layout(**layout)
    return _figure_to_dict(figure)


def _build_empty_plotly_chart(title: str, message: str) -> Dict[str, Any]:
    if not PLOTLY_AVAILABLE:
        return empty_plotly_payload(message)

    figure = go.Figure()
    layout = _plotly_layout("", showlegend=False)
    layout["annotations"] = [
        dict(
            text=message,
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=16, color="#7b6a5a"),
        )
    ]
    layout["xaxis"] = {"visible": False}
    layout["yaxis"] = {"visible": False}
    layout["margin"] = {"l": 20, "r": 20, "t": 20, "b": 20}
    figure.update_layout(**layout)
    payload = _figure_to_dict(figure)
    payload["empty_message"] = message
    return payload


def _plotly_layout(yaxis_title: str, showlegend: bool) -> Dict[str, Any]:
    return {
        "height": 340,
        "bargap": 0.45,
        "showlegend": showlegend,
        "paper_bgcolor": PLOTLY_PALETTE["paper"],
        "plot_bgcolor": PLOTLY_PALETTE["paper"],
        "font": {"family": 'Bahnschrift, "Segoe UI", "Trebuchet MS", sans-serif', "color": PLOTLY_PALETTE["ink"]},
        "margin": {"l": 52, "r": 26, "t": 20, "b": 48},
        "xaxis": {
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 12},
        },
        "yaxis": {
            "title": yaxis_title,
            "gridcolor": PLOTLY_PALETTE["grid"],
            "zeroline": False,
            "tickfont": {"size": 12},
            "title_font": {"size": 12},
        },
        "hoverlabel": {"bgcolor": "#fffaf5", "font": {"color": PLOTLY_PALETTE["ink"]}},
    }


def _figure_to_dict(figure: Any) -> Dict[str, Any]:
    return serialize_plotly_figure(figure)

__all__ = [
    "_finalize_chart",
    "_build_yearly_plotly",
    "_wrap_plotly_label",
    "_build_cause_plotly",
    "_build_distribution_pie_plotly",
    "_build_distribution_plotly",
    "_build_compact_metric_plotly",
    "_build_evacuation_children_plotly",
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
    "_build_impact_yearly_plotly",
    "_build_empty_plotly_chart",
    "_plotly_layout",
    "_figure_to_dict",
]
