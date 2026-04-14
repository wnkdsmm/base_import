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


def _build_forecast_chart(daily_history: List[Dict[str, Any]], forecast_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    title = "РСЃС‚РѕСЂРёСЏ Рё СЃС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР·"
    if not daily_history:
        return {
            "title": title,
            "plotly": _build_empty_plotly("РќРµС‚ РґР°РЅРЅС‹С… РґР»СЏ РёСЃС‚РѕСЂРёС‡РµСЃРєРѕРіРѕ СЂСЏРґР°."),
            "empty_message": "РќРµС‚ РґР°РЅРЅС‹С… РґР»СЏ РёСЃС‚РѕСЂРёС‡РµСЃРєРѕРіРѕ СЂСЏРґР°.",
        }

    if not PLOTLY_AVAILABLE:
        return {
            "title": title,
            "plotly": empty_plotly_payload(),
            "empty_message": "Р‘РёР±Р»РёРѕС‚РµРєР° Plotly РЅРµ РЅР°Р№РґРµРЅР° РІ РѕРєСЂСѓР¶РµРЅРёРё. РўР°Р±Р»РёС†Р° РїСЂРѕРіРЅРѕР·Р° РЅРёР¶Рµ РѕСЃС‚Р°С‘С‚СЃСЏ РґРѕСЃС‚СѓРїРЅРѕР№.",
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
            name="Р¤Р°РєС‚ РїРѕ РґРЅСЏРј",
            line=build_plotly_line(color=PLOTLY_PALETTE["sand"], width=1.6),
            hovertemplate="<b>%{x}</b><br>Р¤Р°РєС‚: %{y} РїРѕР¶Р°СЂР°<extra></extra>",
        )
    )
    figure.add_trace(
        build_plotly_scatter_trace(
            x=history_x,
            y=history_avg7,
            mode="lines",
            name="РЎРіР»Р°Р¶РµРЅРЅС‹Р№ С‚СЂРµРЅРґ Р·Р° 7 РґРЅРµР№",
            line=build_plotly_line(color=PLOTLY_PALETTE["fire"], width=3),
            hovertemplate="<b>%{x}</b><br>РЎСЂРµРґРЅРµРµ Р·Р° 7 РґРЅРµР№: %{y:.1f}<extra></extra>",
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
                name="РўРёРїРёС‡РЅС‹Р№ РґРёР°РїР°Р·РѕРЅ",
            )
        )
        figure.add_trace(
            build_plotly_scatter_trace(
                x=forecast_x,
                y=forecast_y,
                mode="lines+markers",
                name="РЎС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР·",
                line=build_plotly_line(color=PLOTLY_PALETTE["sky"], width=3, dash="dash"),
                marker=build_plotly_marker(color=PLOTLY_PALETTE["sky_soft"], size=7),
                hovertemplate="<b>%{x}</b><br>РћР¶РёРґР°РµРјРѕ: %{y:.1f} РїРѕР¶Р°СЂР°<extra></extra>",
            )
        )
        figure.add_shape(build_vertical_reference_line(forecast_x[0], "rgba(94, 73, 49, 0.45)", width=2))
        figure.add_annotation(
            build_plotly_annotation(
                x=forecast_x[0],
                y=1,
                xref="x",
                yref="paper",
                text="РќР°С‡Р°Р»Рѕ РїСЂРѕРіРЅРѕР·Р°",
                showarrow=False,
                xanchor="left",
                yanchor="bottom",
                font={"size": 12, "color": "rgba(94, 73, 49, 0.72)"},
            )
        )

    figure.update_layout(
        **merge_plotly_layout(
            plotly_layout("РџРѕР¶Р°СЂРѕРІ РІ РґРµРЅСЊ", height=420, margin_right=24, hover_bgcolor="#fffaf5"),
            xaxis={"type": "date", "rangeslider": {"visible": False}},
            legend=build_horizontal_legend(y=1.12),
            updates={"margin": {"l": 52, "r": 24, "t": 24, "b": 50}},
        )
    )

    return build_chart_bundle(title, figure)


def _build_forecast_breakdown_chart(forecast_rows: List[Dict[str, Any]], recent_average: float) -> Dict[str, Any]:
    title = "РЎС†РµРЅР°СЂРЅР°СЏ РІРµСЂРѕСЏС‚РЅРѕСЃС‚СЊ РїРѕР¶Р°СЂР° РїРѕ Р±Р»РёР¶Р°Р№С€РёРј РґРЅСЏРј"
    if not forecast_rows:
        return _empty_chart_bundle(title, "РќРµС‚ РґР°РЅРЅС‹С… РґР»СЏ Р±Р»РёР¶Р°Р№С€РёС… РґРЅРµР№.")
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(title, "Р“СЂР°С„РёРє РЅРµРґРѕСЃС‚СѓРїРµРЅ Р±РµР· Plotly.", annotation_color="#7b6a5a")

    visible_rows = forecast_rows[:21]
    if len(forecast_rows) > 21:
        title = "Р’РµСЂРѕСЏС‚РЅРѕСЃС‚СЊ РїРѕР¶Р°СЂР° РЅР° Р±Р»РёР¶Р°Р№С€РёРµ 21 РґРµРЅСЊ"

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
            name="Р’РµСЂРѕСЏС‚РЅРѕСЃС‚СЊ РїРѕР¶Р°СЂР°",
            customdata=[[row["weekday_label"], row["scenario_hint"]] for row in visible_rows],
            hovertemplate="<b>%{x}</b><br>%{customdata[0]}<br>Р’РµСЂРѕСЏС‚РЅРѕСЃС‚СЊ: %{y:.1f}%<br>%{customdata[1]}<extra></extra>",
        )
    )
    usual_probability_values = [float(row.get("usual_fire_probability", 0.0)) * 100.0 for row in visible_rows]
    if any(value > 0 for value in usual_probability_values):
        figure.add_trace(
            build_plotly_scatter_trace(
                x=labels,
                y=usual_probability_values,
                mode="lines",
                name="РќРµРґР°РІРЅРёР№ РѕР±С‹С‡РЅС‹Р№ СѓСЂРѕРІРµРЅСЊ",
                line=build_plotly_line(color="rgba(94, 73, 49, 0.7)", width=2, dash="dot"),
                hovertemplate="РћР±С‹С‡РЅР°СЏ РІРµСЂРѕСЏС‚РЅРѕСЃС‚СЊ Р·Р° РїРѕСЃР»РµРґРЅРёРµ 4 РЅРµРґРµР»Рё: %{y:.1f}%<extra></extra>",
            )
        )

    figure.update_layout(
        **merge_plotly_layout(
            plotly_layout("Р’РµСЂРѕСЏС‚РЅРѕСЃС‚СЊ, %", height=360, margin_right=24, hover_bgcolor="#fffaf5"),
            xaxis={"tickangle": -35},
            legend=build_horizontal_legend(y=1.12),
        )
    )
    return build_chart_bundle(title, figure)


def _build_weekday_chart(weekday_profile: List[Dict[str, Any]]) -> Dict[str, Any]:
    title = "Р’ РєР°РєРёРµ РґРЅРё РЅРµРґРµР»Рё РїРѕР¶Р°СЂС‹ СЃР»СѓС‡Р°СЋС‚СЃСЏ С‡Р°С‰Рµ"
    if not weekday_profile:
        return _empty_chart_bundle(title, "РќРµС‚ РґР°РЅРЅС‹С… РїРѕ РґРЅСЏРј РЅРµРґРµР»Рё.")
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(title, "Р“СЂР°С„РёРє РЅРµРґРѕСЃС‚СѓРїРµРЅ Р±РµР· Plotly.", annotation_color="#7b6a5a")

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
            hovertemplate="<b>%{x}</b><br>РЎСЂРµРґРЅРµРµ: %{y:.1f} РїРѕР¶Р°СЂР°<extra></extra>",
            name="РЎСЂРµРґРЅРµРµ РїРѕ РґРЅСЋ РЅРµРґРµР»Рё",
        )
    )
    figure.add_trace(
        build_plotly_scatter_trace(
            x=labels,
            y=[overall_average] * len(weekday_profile),
            mode="lines",
            name="РћР±С‰РёР№ СЃСЂРµРґРЅРёР№ СѓСЂРѕРІРµРЅСЊ",
            line=build_plotly_line(color="rgba(94, 73, 49, 0.55)", width=2, dash="dot"),
            hovertemplate="РЎСЂРµРґРЅРёР№ СѓСЂРѕРІРµРЅСЊ: %{y:.1f}<extra></extra>",
        )
    )
    figure.update_layout(
        **merge_plotly_layout(
            plotly_layout("РЎСЂРµРґРЅРµРµ РїРѕР¶Р°СЂРѕРІ РІ РґРµРЅСЊ", height=340, margin_right=24, hover_bgcolor="#fffaf5"),
            legend=build_horizontal_legend(y=1.12),
        )
    )
    return build_chart_bundle(title, figure)
def _build_geo_chart(geo_prediction: Dict[str, Any]) -> Dict[str, Any]:
    title = "РљР°СЂС‚Р° Р·РѕРЅ СЂРёСЃРєР°"
    points = geo_prediction.get("points") or []
    if not points:
        message = "Р’ РІС‹Р±СЂР°РЅРЅРѕРј СЃСЂРµР·Рµ РЅРµС‚ РєРѕРѕСЂРґРёРЅР°С‚, РїРѕСЌС‚РѕРјСѓ РєР°СЂС‚Р° РЅРµ РїРѕСЃС‚СЂРѕРµРЅР°." if not geo_prediction.get("has_coordinates") else "Р”Р»СЏ РєР°СЂС‚С‹ РїРѕРєР° РЅРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ СѓСЃС‚РѕР№С‡РёРІС‹С… Р·РѕРЅ."
        return _empty_chart_bundle(title, message)
    if not PLOTLY_AVAILABLE:
        return build_plotly_unavailable_chart_bundle(title, "Р“РµРѕРєР°СЂС‚Р° РЅРµРґРѕСЃС‚СѓРїРЅР° Р±РµР· Plotly.", annotation_color="#7b6a5a")

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
                "colorbar": {"title": {"text": "Р РёСЃРє"}},
            },
            hovertemplate=(
                "<b>%{text}</b><br>Р РёСЃРє: %{customdata[0]}<br>РџРѕР¶Р°СЂРѕРІ РІ Р·РѕРЅРµ: %{customdata[1]}<br>"
                "РџРѕСЃР»РµРґРЅРёР№ РїРѕР¶Р°СЂ: %{customdata[2]}<br>РџСЂРёС‡РёРЅР°: %{customdata[3]}<br>"
                "РљР°С‚РµРіРѕСЂРёСЏ: %{customdata[4]}<extra></extra>"
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

def _empty_chart_bundle(title: str, message: str, use_plotly: bool = True) -> Dict[str, Any]:
    return build_empty_chart_bundle(
        title,
        message,
        annotation_color="#7b6a5a",
        use_plotly_placeholder=use_plotly,
    )


def _build_empty_plotly(message: str) -> Dict[str, Any]:
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


