from __future__ import annotations

from statistics import mean
from typing import Any, Dict, List

from app.plotly_bundle import PLOTLY_AVAILABLE, empty_plotly_payload, go
from app.services.charting import (
    build_chart_bundle,
    build_empty_chart_bundle,
    build_empty_plotly_payload,
    build_plotly_bar_trace,
    build_plotly_line,
    build_plotly_marker,
    build_plotly_scatter_trace,
    build_plotly_unavailable_chart_bundle,
)
from app.services.chart_utils import (
    build_horizontal_legend,
    build_plotly_annotation,
    build_vertical_reference_line,
    merge_plotly_layout,
    plotly_layout,
)

from .constants import PLOTLY_PALETTE
from .types import (
    ForecastingDailyHistoryRow,
    ForecastingForecastRow,
    ForecastingGeoPrediction,
    ForecastingPayload,
    ForecastingWeekdayProfileRow,
)
from .utils import (
    _rolling_average,
    _scenario_color,
)


def _build_forecast_chart(daily_history: List[dict[str, Any]], forecast_rows: List[dict[str, Any]]) -> dict[str, Any]:
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
        build_plotly_scatter_trace(
            x=history_x,
            y=history_y,
            mode="lines",
            name="Факт по дням",
            line=build_plotly_line(color=PLOTLY_PALETTE["sand"], width=1.6),
            hovertemplate="<b>%{x}</b><br>Факт: %{y} пожара<extra></extra>",
        )
    )
    figure.add_trace(
        build_plotly_scatter_trace(
            x=history_x,
            y=history_avg7,
            mode="lines",
            name="Сглаженный тренд за 7 дней",
            line=build_plotly_line(color=PLOTLY_PALETTE["fire"], width=3),
            hovertemplate="<b>%{x}</b><br>Среднее за 7 дней: %{y:.1f}<extra></extra>",
        )
    )

    if forecast_rows:
        figure.add_trace(
            build_plotly_scatter_trace(
                x=forecast_x,
                y=upper_y,
                mode="lines",
                line=build_plotly_line(color="rgba(45,108,143,0)"),
                hoverinfo="skip",
                showlegend=False,
            )
        )
        figure.add_trace(
            build_plotly_scatter_trace(
                x=forecast_x,
                y=lower_y,
                mode="lines",
                line=build_plotly_line(color="rgba(45,108,143,0)"),
                fill="tonexty",
                fillcolor="rgba(45, 108, 143, 0.16)",
                hoverinfo="skip",
                name="Типичный диапазон",
            )
        )
        figure.add_trace(
            build_plotly_scatter_trace(
                x=forecast_x,
                y=forecast_y,
                mode="lines+markers",
                name="Сценарный прогноз",
                line=build_plotly_line(color=PLOTLY_PALETTE["sky"], width=3, dash="dash"),
                marker=build_plotly_marker(color=PLOTLY_PALETTE["sky_soft"], size=7),
                hovertemplate="<b>%{x}</b><br>Ожидаемо: %{y:.1f} пожара<extra></extra>",
            )
        )
        figure.add_shape(build_vertical_reference_line(forecast_x[0], "rgba(94, 73, 49, 0.45)", width=2))
        figure.add_annotation(
            build_plotly_annotation(
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
        )

    figure.update_layout(
        **merge_plotly_layout(
            plotly_layout("Пожаров в день", height=420, margin_right=24, hover_bgcolor="#fffaf5"),
            xaxis={"type": "date", "rangeslider": {"visible": False}},
            legend=build_horizontal_legend(y=1.12),
            updates={"margin": {"l": 52, "r": 24, "t": 24, "b": 50}},
        )
    )

    return build_chart_bundle(title, figure)


def _build_forecast_breakdown_chart(forecast_rows: List[dict[str, Any]], recent_average: float) -> dict[str, Any]:
    title = "Сценарная вероятность пожара по ближайшим дням"
    if not forecast_rows:
        return _empty_chart_bundle(title, "Нет данных для ближайших дней.")
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(title, "График недоступен без Plotly.", annotation_color="#7b6a5a")

    visible_rows = forecast_rows[:21]
    if len(forecast_rows) > 21:
        title = "Вероятность пожара на ближайшие 21 день"

    colors = [_scenario_color(row.get("scenario_tone", "sky")) for row in visible_rows]
    labels = [row["date_display"] for row in visible_rows]
    values = [float(row.get("fire_probability", 0.0)) * 100.0 for row in visible_rows]
    text_values = [row["fire_probability_display"] for row in visible_rows]

    figure = go.Figure()
    figure.add_trace(
        build_plotly_bar_trace(
            x=labels,
            y=values,
            text=text_values,
            textposition="outside",
            marker={"color": colors},
            name="Вероятность пожара",
            customdata=[[row["weekday_label"], row["scenario_hint"]] for row in visible_rows],
            hovertemplate="<b>%{x}</b><br>%{customdata[0]}<br>Вероятность: %{y:.1f}%<br>%{customdata[1]}<extra></extra>",
        )
    )
    usual_probability_values = [float(row.get("usual_fire_probability", 0.0)) * 100.0 for row in visible_rows]
    if any(value > 0 for value in usual_probability_values):
        figure.add_trace(
            build_plotly_scatter_trace(
                x=labels,
                y=usual_probability_values,
                mode="lines",
                name="Недавний обычный уровень",
                line=build_plotly_line(color="rgba(94, 73, 49, 0.7)", width=2, dash="dot"),
                hovertemplate="Обычная вероятность за последние 4 недели: %{y:.1f}%<extra></extra>",
            )
        )

    figure.update_layout(
        **merge_plotly_layout(
            plotly_layout("Вероятность, %", height=360, margin_right=24, hover_bgcolor="#fffaf5"),
            xaxis={"tickangle": -35},
            legend=build_horizontal_legend(y=1.12),
        )
    )
    return build_chart_bundle(title, figure)


def _build_weekday_chart(weekday_profile: List[dict[str, Any]]) -> dict[str, Any]:
    title = "В какие дни недели пожары случаются чаще"
    if not weekday_profile:
        return _empty_chart_bundle(title, "Нет данных по дням недели.")
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(title, "График недоступен без Plotly.", annotation_color="#7b6a5a")

    overall_average = mean(float(item["avg_value"]) for item in weekday_profile) if weekday_profile else 0.0

    figure = go.Figure()
    labels = [item["label"] for item in weekday_profile]
    figure.add_trace(
        build_plotly_bar_trace(
            x=labels,
            y=[item["avg_value"] for item in weekday_profile],
            text=[item["avg_display"] for item in weekday_profile],
            textposition="outside",
            marker={
                "color": [
                    PLOTLY_PALETTE["fire_soft"],
                    PLOTLY_PALETTE["sand"],
                    PLOTLY_PALETTE["sand_soft"],
                    PLOTLY_PALETTE["sky_soft"],
                    PLOTLY_PALETTE["sky"],
                    PLOTLY_PALETTE["forest_soft"],
                    PLOTLY_PALETTE["forest"],
                ]
            },
            hovertemplate="<b>%{x}</b><br>Среднее: %{y:.1f} пожара<extra></extra>",
            name="Среднее по дню недели",
        )
    )
    figure.add_trace(
        build_plotly_scatter_trace(
            x=labels,
            y=[overall_average] * len(weekday_profile),
            mode="lines",
            name="Общий средний уровень",
            line=build_plotly_line(color="rgba(94, 73, 49, 0.55)", width=2, dash="dot"),
            hovertemplate="Средний уровень: %{y:.1f}<extra></extra>",
        )
    )
    figure.update_layout(
        **merge_plotly_layout(
            plotly_layout("Среднее пожаров в день", height=340, margin_right=24, hover_bgcolor="#fffaf5"),
            legend=build_horizontal_legend(y=1.12),
        )
    )
    return build_chart_bundle(title, figure)
def _build_geo_chart(geo_prediction: dict[str, Any]) -> dict[str, Any]:
    title = "Карта зон риска"
    points = geo_prediction.get("points") or []
    if not points:
        message = "В выбранном срезе нет координат, поэтому карта не построена." if not geo_prediction.get("has_coordinates") else "Для карты пока недостаточно устойчивых зон."
        return _empty_chart_bundle(title, message)
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(title, "Геокарта недоступна без Plotly.", annotation_color="#7b6a5a")

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
    return build_chart_bundle(title, figure)

def _empty_chart_bundle(title: str, message: str, use_plotly: bool = True) -> dict[str, Any]:
    return build_empty_chart_bundle(
        title,
        message,
        annotation_color="#7b6a5a",
        use_plotly_placeholder=use_plotly,
    )


def _build_empty_plotly(message: str) -> dict[str, Any]:
    return build_empty_plotly_payload(message, annotation_color="#7b6a5a")


def build_forecasting_forecast_chart(
    daily_history: List[ForecastingDailyHistoryRow],
    forecast_rows: List[ForecastingForecastRow],
) -> ForecastingPayload:
    return _build_forecast_chart(daily_history, forecast_rows)


def build_forecasting_forecast_breakdown_chart(
    forecast_rows: List[ForecastingForecastRow],
    recent_average: float,
) -> ForecastingPayload:
    return _build_forecast_breakdown_chart(forecast_rows, recent_average)


def build_forecasting_weekday_chart(weekday_profile: List[ForecastingWeekdayProfileRow]) -> ForecastingPayload:
    return _build_weekday_chart(weekday_profile)


def build_forecasting_geo_chart(geo_prediction: ForecastingGeoPrediction) -> ForecastingPayload:
    return _build_geo_chart(geo_prediction)


