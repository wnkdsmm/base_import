from __future__ import annotations

from collections import Counter
import math
from datetime import timedelta
from typing import Any, Dict, List, Sequence, Tuple

from .constants import *
from .utils import _format_integer, _format_number
def _build_geo_prediction(
    records: List[Dict[str, Any]],
    planning_horizon_days: int,
) -> Dict[str, Any]:
    geo_records = [
        record
        for record in records
        if record.get("latitude") is not None and record.get("longitude") is not None
    ]
    if not geo_records:
        return {
            "has_coordinates": False,
            "model_description": "Карта блока поддержки решений появится, когда в выбранных пожарах есть координаты Широта и Долгота.",
            "coverage_display": "0 с координатами",
            "cell_size_display": "-",
            "top_risk_display": "0 / 100",
            "hotspots_count_display": "0",
            "top_zone_label": "-",
            "top_explanation": "Нет данных для объяснения зоны риска.",
            "legend": _geo_risk_legend(),
            "districts": [],
            "hotspots": [],
            "points": [],
        }

    last_observed_date = max(record["date"] for record in geo_records)
    future_horizon_days = max(1, int(planning_horizon_days or 7))
    future_dates = [last_observed_date + timedelta(days=offset) for offset in range(1, future_horizon_days + 1)]

    future_months = Counter(item.month for item in future_dates)
    future_weekdays = Counter(item.weekday() for item in future_dates)
    future_horizon = max(1, len(future_dates))

    latitudes = [float(record["latitude"]) for record in geo_records]
    longitudes = [float(record["longitude"]) for record in geo_records]
    cell_size = _derive_geo_cell_size(latitudes, longitudes)
    cells: Dict[Tuple[int, int], Dict[str, Any]] = {}

    for record in geo_records:
        latitude = float(record["latitude"])
        longitude = float(record["longitude"])
        key = (math.floor(latitude / cell_size), math.floor(longitude / cell_size))
        cell = cells.setdefault(
            key,
            {
                "score": 0.0,
                "incidents": 0,
                "lat_sum": 0.0,
                "lon_sum": 0.0,
                "last_fire": None,
                "districts": Counter(),
                "causes": Counter(),
                "object_categories": Counter(),
            },
        )

        age_days = max(0, (last_observed_date - record["date"]).days)
        recency_weight = max(0.2, 1 - min(age_days, GEO_LOOKBACK_DAYS) / GEO_LOOKBACK_DAYS)
        month_weight = 1.0 + 0.35 * (future_months.get(record["date"].month, 0) / future_horizon)
        weekday_weight = 1.0 + 0.20 * (future_weekdays.get(record["date"].weekday(), 0) / future_horizon)
        score = recency_weight * month_weight * weekday_weight

        cell["score"] += score
        cell["incidents"] += 1
        cell["lat_sum"] += latitude
        cell["lon_sum"] += longitude
        cell["last_fire"] = record["date"] if cell["last_fire"] is None else max(cell["last_fire"], record["date"])
        if record.get("district"):
            cell["districts"][record["district"]] += 1
        if record.get("cause"):
            cell["causes"][record["cause"]] += 1
        if record.get("object_category"):
            cell["object_categories"][record["object_category"]] += 1

    ranked_cells: List[Dict[str, Any]] = []
    for cell in cells.values():
        freshness_days = min((last_observed_date - cell["last_fire"]).days, GEO_LOOKBACK_DAYS)
        freshness = max(0.0, 1 - freshness_days / GEO_LOOKBACK_DAYS)
        raw_risk = cell["score"] * (1.0 + math.log1p(cell["incidents"]) * 0.22) * (0.85 + 0.15 * freshness)
        centroid_lat = cell["lat_sum"] / cell["incidents"]
        centroid_lon = cell["lon_sum"] / cell["incidents"]
        ranked_cells.append(
            {
                "raw_risk": raw_risk,
                "incidents": cell["incidents"],
                "centroid_lat": round(centroid_lat, 6),
                "centroid_lon": round(centroid_lon, 6),
                "last_fire": cell["last_fire"],
                "freshness_days": freshness_days,
                "dominant_district": _counter_top_label(cell["districts"], "Без района"),
                "dominant_cause": _counter_top_label(cell["causes"], "Не указана"),
                "dominant_object_category": _counter_top_label(cell["object_categories"], "Не указана"),
            }
        )

    ranked_cells.sort(key=lambda item: (item["raw_risk"], item["incidents"]), reverse=True)
    max_risk = ranked_cells[0]["raw_risk"] if ranked_cells else 1.0
    points: List[Dict[str, Any]] = []

    for rank, cell in enumerate(ranked_cells, start=1):
        risk_score = round((cell["raw_risk"] / max_risk) * 100, 1) if max_risk > 0 else 0.0
        risk_level_label, risk_tone = _geo_risk_level(risk_score)
        confidence_score = min(96.0, 42.0 + cell["incidents"] * 7.0 + max(0.0, 25.0 - cell["freshness_days"] * 0.18))
        short_label = cell["dominant_district"] if cell["dominant_district"] != "Без района" else f"Сектор {rank}"
        explanation = (
            f"{_format_integer(cell['incidents'])} пожаров в ячейке, последний очаг {_format_days_ago(cell['freshness_days'])}, "
            f"типовая причина: {cell['dominant_cause']}"
        )
        points.append(
            {
                "rank": rank,
                "short_label": short_label,
                "location_label": f"{short_label} ({cell['centroid_lat']:.3f}, {cell['centroid_lon']:.3f})",
                "risk_score": risk_score,
                "risk_display": f"{_format_number(risk_score)} / 100",
                "risk_level_label": risk_level_label,
                "risk_tone": risk_tone,
                "confidence_display": f"{_format_number(confidence_score)}%",
                "bar_width": f"{max(10, min(100, round(risk_score)))}%",
                "incidents": cell["incidents"],
                "incidents_display": _format_integer(cell["incidents"]),
                "last_fire_display": cell["last_fire"].strftime("%d.%m.%Y") if cell["last_fire"] else "-",
                "last_fire_ago_display": _format_days_ago(cell["freshness_days"]),
                "dominant_district": cell["dominant_district"],
                "dominant_cause": cell["dominant_cause"],
                "dominant_object_category": cell["dominant_object_category"],
                "latitude": cell["centroid_lat"],
                "longitude": cell["centroid_lon"],
                "explanation": explanation,
                "marker_size": round(max(12.0, min(32.0, 10.0 + risk_score / 4.5 + math.log1p(cell["incidents"]) * 3.0)), 1),
            }
        )

    districts_map: Dict[str, Dict[str, float]] = {}
    for point in points:
        district_name = point["dominant_district"] or "Без района"
        bucket = districts_map.setdefault(
            district_name,
            {"zones": 0, "incidents": 0, "peak_risk": 0.0, "risk_sum": 0.0},
        )
        bucket["zones"] += 1
        bucket["incidents"] += int(point["incidents"])
        bucket["peak_risk"] = max(bucket["peak_risk"], float(point["risk_score"]))
        bucket["risk_sum"] += float(point["risk_score"])

    districts = []
    for district_name, bucket in districts_map.items():
        avg_risk = bucket["risk_sum"] / max(1, bucket["zones"])
        districts.append(
            {
                "label": district_name,
                "zones_display": _format_integer(bucket["zones"]),
                "incidents_display": _format_integer(bucket["incidents"]),
                "peak_risk_display": f"{_format_number(bucket['peak_risk'])} / 100",
                "avg_risk_display": f"{_format_number(avg_risk)} / 100",
                "bar_width": f"{max(10, min(100, round(bucket['peak_risk'])))}%",
            }
        )
    districts.sort(
        key=lambda item: (float(item["peak_risk_display"].split(" /")[0].replace(" ", "").replace(",", ".")), item["incidents_display"]),
        reverse=True,
    )

    chart_points = points[:MAX_GEO_CHART_POINTS]
    hotspots = points[:MAX_GEO_HOTSPOTS]
    top_zone_label = hotspots[0]["short_label"] if hotspots else "-"
    top_explanation = hotspots[0]["explanation"] if hotspots else "Нет данных для объяснения зоны риска."
    return {
        "has_coordinates": True,
        "model_description": (
            "Карта относится к блоку поддержки решений и использует пространственную историю очагов, чтобы подсветить зоны повторяемого риска. "
            "Это не ML-прогноз и не дневной сценарный прогноз, а отдельная пространственная оценка для приоритизации территорий."
        ),
        "coverage_display": f"{_format_integer(len(geo_records))} с координатами из {_format_integer(len(records))}",
        "cell_size_display": f"{_format_number(cell_size)}°",
        "top_risk_display": f"{_format_number(hotspots[0]['risk_score'])} / 100" if hotspots else "0 / 100",
        "hotspots_count_display": _format_integer(len(points)),
        "top_zone_label": top_zone_label,
        "top_explanation": top_explanation,
        "legend": _geo_risk_legend(),
        "districts": districts[:6],
        "hotspots": hotspots,
        "points": chart_points,
    }

def _derive_geo_cell_size(latitudes: Sequence[float], longitudes: Sequence[float]) -> float:
    if not latitudes or not longitudes:
        return 0.12
    lat_span = max(latitudes) - min(latitudes)
    lon_span = max(longitudes) - min(longitudes)
    span = max(lat_span, lon_span)
    if span <= 0.35:
        return 0.05
    if span <= 1.20:
        return 0.08
    if span <= 3.00:
        return 0.12
    if span <= 8.00:
        return 0.20
    return round(min(0.60, max(0.12, span / 18.0)), 2)


def _counter_top_label(counter: Counter, fallback: str) -> str:
    if not counter:
        return fallback
    return counter.most_common(1)[0][0]


def _geo_risk_level(value: float) -> Tuple[str, str]:
    if value >= 80:
        return "Критический", "critical"
    if value >= 60:
        return "Высокий", "high"
    if value >= 35:
        return "Средний", "medium"
    return "Наблюдение", "watch"


def _geo_risk_legend() -> List[Dict[str, str]]:
    return [
        {"label": "Критический", "range_label": "80-100", "tone": "critical"},
        {"label": "Высокий", "range_label": "60-79", "tone": "high"},
        {"label": "Средний", "range_label": "35-59", "tone": "medium"},
        {"label": "Наблюдение", "range_label": "0-34", "tone": "watch"},
    ]


def _format_days_ago(days: int) -> str:
    if days <= 0:
        return "сегодня"
    if days == 1:
        return "1 день назад"
    if 2 <= days <= 4:
        return f"{days} дня назад"
    return f"{days} дней назад"


