from __future__ import annotations

from statistics import mean
from typing import Any, Dict, List

from app.plotly_bundle import PLOTLY_AVAILABLE, empty_plotly_payload, go, serialize_plotly_figure

from .constants import PLOTLY_PALETTE
from .utils import (
    _probability_from_expected_count,
    _rolling_average,
    _scenario_color,
)


def _build_forecast_chart(daily_history: List[Dict[str, Any]], forecast_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    title = "История и сценарный прогноз"
    if not daily_history:
        return {
            "title": title,
            "plotly": _build_empty_plotly("Нет данных для исторического ряда."),
            "empty_message": "Нет данных для исторического ряда.",
        }

    if not PLOTLY_AVAILABLE:
        return {
            "title": title,
            "plotly": empty_plotly_payload(),
            "empty_message": "Библиотека Plotly не найдена в окружении. Таблица прогноза ниже остаётся доступной.",
        }

    visible_history = daily_history[-90:] if len(daily_history) > 90 else daily_history
    history_x = [item["date"].isoformat() for item in visible_history]
    history_y = [item["count"] for item in visible_history]
    history_avg7 = _rolling_average(history_y, 7)
    forecast_x = [item["date"] for item in forecast_rows]
    forecast_y = [item["forecast_value"] for item in forecast_rows]
    lower_y = [item["lower_bound"] for item in forecast_rows]
    upper_y = [item["upper_bound"] for item in forecast_rows]

    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=history_x,
            y=history_y,
            mode="lines",
            name="Факт по дням",
            line=dict(color=PLOTLY_PALETTE["sand"], width=1.6),
            hovertemplate="<b>%{x}</b><br>Факт: %{y} пожара<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=history_x,
            y=history_avg7,
            mode="lines",
            name="Сглаженный тренд за 7 дней",
            line=dict(color=PLOTLY_PALETTE["fire"], width=3),
            hovertemplate="<b>%{x}</b><br>Среднее за 7 дней: %{y:.1f}<extra></extra>",
        )
    )

    if forecast_rows:
        figure.add_trace(
            go.Scatter(
                x=forecast_x,
                y=upper_y,
                mode="lines",
                line=dict(color="rgba(45,108,143,0)"),
                hoverinfo="skip",
                showlegend=False,
            )
        )
        figure.add_trace(
            go.Scatter(
                x=forecast_x,
                y=lower_y,
                mode="lines",
                line=dict(color="rgba(45,108,143,0)"),
                fill="tonexty",
                fillcolor="rgba(45, 108, 143, 0.16)",
                hoverinfo="skip",
                name="Типичный диапазон",
            )
        )
        figure.add_trace(
            go.Scatter(
                x=forecast_x,
                y=forecast_y,
                mode="lines+markers",
                name="Сценарный прогноз",
                line=dict(color=PLOTLY_PALETTE["sky"], width=3, dash="dash"),
                marker=dict(size=7, color=PLOTLY_PALETTE["sky_soft"]),
                hovertemplate="<b>%{x}</b><br>Ожидаемо: %{y:.1f} пожара<extra></extra>",
            )
        )
        figure.add_shape(
            type="line",
            x0=forecast_x[0],
            x1=forecast_x[0],
            y0=0,
            y1=1,
            xref="x",
            yref="paper",
            line=dict(color="rgba(94, 73, 49, 0.45)", dash="dot", width=2),
        )
        figure.add_annotation(
            x=forecast_x[0],
            y=1,
            xref="x",
            yref="paper",
            text="Начало прогноза",
            showarrow=False,
            xanchor="left",
            yanchor="bottom",
            font={"size": 12, "color": "rgba(94, 73, 49, 0.72)"},
        )

    figure.update_layout(
        height=420,
        showlegend=True,
        paper_bgcolor="rgba(255,255,255,0)",
        plot_bgcolor="rgba(255,255,255,0)",
        font={"family": 'Bahnschrift, "Segoe UI", "Trebuchet MS", sans-serif', "color": PLOTLY_PALETTE["ink"]},
        margin={"l": 52, "r": 24, "t": 24, "b": 50},
        xaxis={"type": "date", "showgrid": False, "zeroline": False, "rangeslider": {"visible": False}},
        yaxis={"title": "Пожаров в день", "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False},
        legend={"orientation": "h", "y": 1.12, "x": 0},
        hoverlabel={"bgcolor": "#fffaf5", "font": {"color": PLOTLY_PALETTE["ink"]}},
    )

    return {"title": title, "plotly": _figure_to_dict(figure), "empty_message": ""}


def _build_forecast_breakdown_chart(forecast_rows: List[Dict[str, Any]], recent_average: float) -> Dict[str, Any]:
    title = "Сценарная вероятность пожара по ближайшим дням"
    if not forecast_rows:
        return _empty_chart_bundle(title, "Нет данных для ближайших дней.")
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "График недоступен без Plotly.", use_plotly=False)

    visible_rows = forecast_rows[:21]
    if len(forecast_rows) > 21:
        title = "Вероятность пожара на ближайшие 21 день"

    colors = [_scenario_color(row.get("scenario_tone", "sky")) for row in visible_rows]
    labels = [row["date_display"] for row in visible_rows]
    values = [float(row.get("fire_probability", 0.0)) * 100.0 for row in visible_rows]
    text_values = [row["fire_probability_display"] for row in visible_rows]

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=labels,
            y=values,
            text=text_values,
            textposition="outside",
            marker=dict(color=colors),
            name="Вероятность пожара",
            customdata=[[row["weekday_label"], row["scenario_hint"]] for row in visible_rows],
            hovertemplate="<b>%{x}</b><br>%{customdata[0]}<br>Вероятность: %{y:.1f}%<br>%{customdata[1]}<extra></extra>",
        )
    )
    if recent_average > 0:
        figure.add_trace(
            go.Scatter(
                x=labels,
                y=[_probability_from_expected_count(recent_average) * 100.0] * len(visible_rows),
                mode="lines",
                name="Недавний обычный уровень",
                line=dict(color="rgba(94, 73, 49, 0.7)", width=2, dash="dot"),
                hovertemplate="Обычная вероятность за последние 4 недели: %{y:.1f}%<extra></extra>",
            )
        )

    figure.update_layout(**_plotly_layout("Вероятность, %", height=360))
    figure.update_layout(
        legend={"orientation": "h", "y": 1.12, "x": 0},
        xaxis={"showgrid": False, "zeroline": False, "tickangle": -35},
    )
    return {"title": title, "plotly": _figure_to_dict(figure), "empty_message": ""}
def _build_weekly_chart(weekly_outlook: List[Dict[str, Any]]) -> Dict[str, Any]:
    title = "Последние недели и ближайшие недели"
    if not weekly_outlook:
        return _empty_chart_bundle(title, "Нет данных по неделям.")
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "График недоступен без Plotly.", use_plotly=False)

    labels = [item["label"] for item in weekly_outlook]
    actual_values = [item["actual"] if not item["is_future"] else None for item in weekly_outlook]
    forecast_values = [item["forecast"] if item["is_future"] else None for item in weekly_outlook]

    figure = go.Figure()
    figure.add_trace(go.Bar(x=labels, y=actual_values, name="Последние фактические недели", marker=dict(color=PLOTLY_PALETTE["sand"]), hovertemplate="<b>%{x}</b><br>Факт: %{y:.1f} пожара<extra></extra>"))
    figure.add_trace(go.Bar(x=labels, y=forecast_values, name="Ближайшие недели сценарного прогноза", marker=dict(color=PLOTLY_PALETTE["sky"]), hovertemplate="<b>%{x}</b><br>Сценарный прогноз: %{y:.1f} пожара<extra></extra>"))
    figure.update_layout(**_plotly_layout("Пожаров за неделю", height=340))
    figure.update_layout(barmode="group", legend={"orientation": "h", "y": 1.12, "x": 0})
    return {"title": title, "plotly": _figure_to_dict(figure), "empty_message": ""}
def _build_monthly_chart(monthly_outlook: List[Dict[str, Any]]) -> Dict[str, Any]:
    title = "Ближайшие месяцы и обычный уровень"
    if not monthly_outlook:
        return _empty_chart_bundle(title, "Нет данных по месяцам.")
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "График недоступен без Plotly.", use_plotly=False)

    figure = go.Figure()
    figure.add_trace(go.Bar(x=[item["label"] for item in monthly_outlook], y=[item["forecast"] for item in monthly_outlook], name="Сценарный прогноз", text=[item["delta_percent_display"] for item in monthly_outlook], textposition="outside", marker=dict(color=PLOTLY_PALETTE["forest"]), customdata=[[item["baseline_display"], item["delta_percent_display"], item["level_label"]] for item in monthly_outlook], hovertemplate="<b>%{x}</b><br>Сценарный прогноз: %{y:.1f} пожара<br>Обычный уровень: %{customdata[0]}<br>Изменение: %{customdata[1]}<br>%{customdata[2]}<extra></extra>"))
    figure.add_trace(go.Scatter(x=[item["label"] for item in monthly_outlook], y=[item["baseline"] for item in monthly_outlook], name="Обычный уровень", mode="lines+markers", line=dict(color=PLOTLY_PALETTE["fire"], width=3), marker=dict(size=7, color=PLOTLY_PALETTE["fire_soft"]), hovertemplate="<b>%{x}</b><br>Обычный уровень: %{y:.1f} пожара<extra></extra>"))
    figure.update_layout(**_plotly_layout("Пожаров за месяц", height=340))
    figure.update_layout(legend={"orientation": "h", "y": 1.12, "x": 0})
    return {"title": title, "plotly": _figure_to_dict(figure), "empty_message": ""}
def _build_weekday_chart(weekday_profile: List[Dict[str, Any]]) -> Dict[str, Any]:
    title = "В какие дни недели пожары случаются чаще"
    if not weekday_profile:
        return _empty_chart_bundle(title, "Нет данных по дням недели.")
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "График недоступен без Plotly.", use_plotly=False)

    overall_average = mean(float(item["avg_value"]) for item in weekday_profile) if weekday_profile else 0.0

    figure = go.Figure()
    figure.add_trace(go.Bar(x=[item["label"] for item in weekday_profile], y=[item["avg_value"] for item in weekday_profile], text=[item["avg_display"] for item in weekday_profile], textposition="outside", marker=dict(color=[PLOTLY_PALETTE["fire_soft"], PLOTLY_PALETTE["sand"], PLOTLY_PALETTE["sand_soft"], PLOTLY_PALETTE["sky_soft"], PLOTLY_PALETTE["sky"], PLOTLY_PALETTE["forest_soft"], PLOTLY_PALETTE["forest"]]), hovertemplate="<b>%{x}</b><br>Среднее: %{y:.1f} пожара<extra></extra>", name="Среднее по дню недели"))
    figure.add_trace(go.Scatter(x=[item["label"] for item in weekday_profile], y=[overall_average] * len(weekday_profile), mode="lines", name="Общий средний уровень", line=dict(color="rgba(94, 73, 49, 0.55)", width=2, dash="dot"), hovertemplate="Средний уровень: %{y:.1f}<extra></extra>"))
    figure.update_layout(**_plotly_layout("Среднее пожаров в день", height=340))
    figure.update_layout(legend={"orientation": "h", "y": 1.12, "x": 0})
    return {"title": title, "plotly": _figure_to_dict(figure), "empty_message": ""}
def _build_geo_chart(geo_prediction: Dict[str, Any]) -> Dict[str, Any]:
    title = "Карта зон риска"
    points = geo_prediction.get("points") or []
    if not points:
        message = "В выбранном срезе нет координат, поэтому карта не построена." if not geo_prediction.get("has_coordinates") else "Для карты пока недостаточно устойчивых зон."
        return _empty_chart_bundle(title, message)
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "Геокарта недоступна без Plotly.", use_plotly=False)

    latitudes = [float(point["latitude"]) for point in points]
    longitudes = [float(point["longitude"]) for point in points]
    min_lat = min(latitudes)
    max_lat = max(latitudes)
    min_lon = min(longitudes)
    max_lon = max(longitudes)
    lat_pad = max(0.08, (max_lat - min_lat) * 0.22 if max_lat > min_lat else 0.15)
    lon_pad = max(0.08, (max_lon - min_lon) * 0.22 if max_lon > min_lon else 0.15)

    figure = go.Figure()
    figure.add_trace(
        go.Scattergeo(
            lon=longitudes,
            lat=latitudes,
            text=[point["short_label"] for point in points],
            customdata=[
                [
                    point["risk_display"],
                    point["incidents_display"],
                    point["last_fire_display"],
                    point["dominant_cause"],
                    point["dominant_object_category"],
                ]
                for point in points
            ],
            mode="markers",
            marker={
                "size": [point["marker_size"] for point in points],
                "color": [point["risk_score"] for point in points],
                "cmin": 0,
                "cmax": 100,
                "colorscale": [
                    [0.0, "#e4c593"],
                    [0.5, "#d95d39"],
                    [1.0, "#8f2d1f"],
                ],
                "opacity": 0.86,
                "line": {"color": "rgba(51, 41, 32, 0.30)", "width": 1},
                "colorbar": {"title": {"text": "Риск"}},
            },
            hovertemplate=(
                "<b>%{text}</b><br>Риск: %{customdata[0]}<br>Пожаров в зоне: %{customdata[1]}<br>"
                "Последний пожар: %{customdata[2]}<br>Причина: %{customdata[3]}<br>"
                "Категория: %{customdata[4]}<extra></extra>"
            ),
        )
    )
    figure.update_layout(
        height=340,
        margin={"l": 24, "r": 24, "t": 24, "b": 24},
        paper_bgcolor="rgba(255,255,255,0)",
        font={"family": 'Bahnschrift, "Segoe UI", "Trebuchet MS", sans-serif', "color": PLOTLY_PALETTE["ink"]},
        geo={
            "projection": {"type": "mercator"},
            "showland": True,
            "landcolor": "#f7f1e7",
            "showocean": True,
            "oceancolor": "#f6fbff",
            "showlakes": True,
            "lakecolor": "#f6fbff",
            "showcountries": True,
            "countrycolor": "rgba(94, 73, 49, 0.18)",
            "showcoastlines": True,
            "coastlinecolor": "rgba(94, 73, 49, 0.18)",
            "bgcolor": "rgba(255,255,255,0)",
            "center": {"lat": (min_lat + max_lat) / 2, "lon": (min_lon + max_lon) / 2},
            "lataxis": {"range": [min_lat - lat_pad, max_lat + lat_pad]},
            "lonaxis": {"range": [min_lon - lon_pad, max_lon + lon_pad]},
        },
        showlegend=False,
        hoverlabel={"bgcolor": "#fffaf5", "font": {"color": PLOTLY_PALETTE["ink"]}},
    )
    return {"title": title, "plotly": _figure_to_dict(figure), "empty_message": ""}

def _empty_chart_bundle(title: str, message: str, use_plotly: bool = True) -> Dict[str, Any]:
    plotly_payload = _build_empty_plotly(message) if use_plotly else empty_plotly_payload()
    return {"title": title, "plotly": plotly_payload, "empty_message": message}


def _plotly_layout(yaxis_title: str, height: int = 340) -> Dict[str, Any]:
    return {
        "height": height,
        "showlegend": True,
        "paper_bgcolor": "rgba(255,255,255,0)",
        "plot_bgcolor": "rgba(255,255,255,0)",
        "font": {"family": 'Bahnschrift, "Segoe UI", "Trebuchet MS", sans-serif', "color": PLOTLY_PALETTE["ink"]},
        "margin": {"l": 52, "r": 24, "t": 24, "b": 48},
        "xaxis": {"showgrid": False, "zeroline": False},
        "yaxis": {"title": yaxis_title, "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False},
        "hoverlabel": {"bgcolor": "#fffaf5", "font": {"color": PLOTLY_PALETTE["ink"]}},
    }


def _build_empty_plotly(message: str) -> Dict[str, Any]:
    if not PLOTLY_AVAILABLE:
        return empty_plotly_payload(message)

    figure = go.Figure()
    figure.update_layout(
        height=320,
        paper_bgcolor="rgba(255,255,255,0)",
        plot_bgcolor="rgba(255,255,255,0)",
        margin={"l": 20, "r": 20, "t": 20, "b": 20},
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[
            {
                "text": message,
                "x": 0.5,
                "y": 0.5,
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 16, "color": "#7b6a5a"},
            }
        ],
    )
    payload = _figure_to_dict(figure)
    payload["empty_message"] = message
    return payload


def _figure_to_dict(figure: Any) -> Dict[str, Any]:
    return serialize_plotly_figure(figure)

