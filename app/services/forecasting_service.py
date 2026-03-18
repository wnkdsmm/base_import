from __future__ import annotations

from collections import Counter, defaultdict
import math
from datetime import date, datetime, timedelta
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import inspect, text

from app.services.table_options import get_fire_map_table_options, resolve_selected_table
from app.statistics_constants import (
    BUILDING_CAUSE_COLUMN,
    DATE_COLUMN,
    GENERAL_CAUSE_COLUMN,
    OBJECT_CATEGORY_COLUMN,
    OPEN_AREA_CAUSE_COLUMN,
    PLOTLY_PALETTE,
)
from config.db import engine

try:
    import json

    import plotly.graph_objects as go
    from plotly.offline import get_plotlyjs
    from plotly.utils import PlotlyJSONEncoder

    PLOTLY_AVAILABLE = True
except Exception:
    go = None
    get_plotlyjs = None
    PlotlyJSONEncoder = None
    json = None
    PLOTLY_AVAILABLE = False


DISTRICT_COLUMN_CANDIDATES = [
    "Район",
    "Муниципальный район",
    "Муниципальное образование",
    "Административный район",
    "Район выезда подразделения",
    "Район пожара",
    "Территория",
]
TEMPERATURE_COLUMN_CANDIDATES = [
    "Температура",
    "Температура воздуха",
    "Температура воздуха, С",
    "Температура воздуха, C",
    "Температура воздуха, °C",
    "Температура окружающей среды",
]
CAUSE_COLUMN_CANDIDATES = [
    GENERAL_CAUSE_COLUMN,
    OPEN_AREA_CAUSE_COLUMN,
    BUILDING_CAUSE_COLUMN,
    "Причина пожара",
    "Причина",
]
LATITUDE_COLUMN_CANDIDATES = ["Широта", "Latitude", "Lat"]
LONGITUDE_COLUMN_CANDIDATES = ["Долгота", "Longitude", "Lon"]
FORECAST_DAY_OPTIONS = [7, 14, 30, 60]
HISTORY_WINDOW_OPTIONS = [
    {"value": "all", "label": "\u0412\u0441\u0435 \u0433\u043e\u0434\u044b"},
    {"value": "recent_3", "label": "\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0435 3 \u0433\u043e\u0434\u0430"},
    {"value": "recent_5", "label": "\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0435 5 \u043b\u0435\u0442"},
]
GEO_LOOKBACK_DAYS = 180
MAX_GEO_CHART_POINTS = 40
MAX_GEO_HOTSPOTS = 8
WEEKDAY_LABELS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
MONTH_LABELS = {
    1: "Янв",
    2: "Фев",
    3: "Мар",
    4: "Апр",
    5: "Май",
    6: "Июн",
    7: "Июл",
    8: "Авг",
    9: "Сен",
    10: "Окт",
    11: "Ноя",
    12: "Дек",
}


def get_forecasting_page_context(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> Dict[str, Any]:
    initial_data = get_forecasting_data(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
    )
    return {
        "generated_at": _format_datetime(datetime.now()),
        "initial_data": initial_data,
        "plotly_js": _get_plotly_bundle(),
        "has_data": bool(initial_data["filters"]["available_tables"]),
    }


def get_forecasting_data(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> Dict[str, Any]:
    table_options = _build_forecasting_table_options()
    selected_table = _resolve_forecasting_selection(table_options, table_name)
    source_tables = _selected_source_tables(table_options, selected_table)
    days_ahead = _parse_forecast_days(forecast_days)
    selected_history_window = _parse_history_window(history_window)
    temperature_value = _parse_float(temperature)

    base_data = _empty_forecasting_data(table_options, selected_table, days_ahead, temperature, selected_history_window)
    if not source_tables:
        base_data["notes"].append("Нет доступных таблиц для прогнозирования.")
        return base_data

    records, metadata_items, preload_notes = _collect_forecasting_inputs(source_tables)
    scoped_records = _apply_history_window(records, selected_history_window)
    option_catalog = _build_option_catalog(scoped_records)
    selected_cause = _resolve_option_value(option_catalog["causes"], cause)
    selected_object_category = _resolve_option_value(option_catalog["object_categories"], object_category)

    filtered_records = [
        record
        for record in scoped_records
        if (selected_cause == "all" or record["cause"] == selected_cause)
        and (selected_object_category == "all" or record["object_category"] == selected_object_category)
    ]

    daily_history = _build_daily_history(filtered_records)
    forecast_rows = _build_forecast_rows(daily_history, days_ahead, temperature_value)
    weekday_profile = _build_weekday_profile(daily_history)
    history_counts = [float(item["count"]) for item in daily_history]
    recent_counts = history_counts[-28:] if len(history_counts) >= 28 else history_counts
    recent_average = mean(recent_counts) if recent_counts else 0.0
    charts = {
        "daily": _build_forecast_chart(daily_history, forecast_rows),
        "breakdown": _build_forecast_breakdown_chart(forecast_rows, recent_average),
        "weekday": _build_weekday_chart(weekday_profile),
    }
    notes = preload_notes + _build_notes(
        metadata=metadata_items,
        filtered_records=filtered_records,
        daily_history=daily_history,
        temperature_value=temperature_value,
    )
    features = _build_feature_cards(metadata_items)
    insights = _build_insights(daily_history, forecast_rows, weekday_profile)
    summary = _build_summary(
        selected_table=selected_table,
        selected_district="all",
        selected_cause=selected_cause,
        selected_object_category=selected_object_category,
        temperature_value=temperature_value,
        daily_history=daily_history,
        filtered_records=filtered_records,
        forecast_rows=forecast_rows,
        history_window=selected_history_window,
    )

    return {
        "generated_at": _format_datetime(datetime.now()),
        "has_data": bool(filtered_records),
        "model_description": (
            "Прогноз опирается на последние недели истории и повторяющиеся паттерны по дням недели и сезону. "
            "Значения на горизонте считаются от наблюдаемой истории и не разгоняются цепочкой от предыдущего прогноза, "
            "поэтому сценарий стал стабильнее и проще для чтения."
        ),
        "summary": summary,
        "features": features,
        "insights": insights,
        "charts": charts,
        "forecast_rows": forecast_rows,
        "notes": notes,
        "filters": {
            "table_name": selected_table,
            "district": "all",
            "cause": selected_cause,
            "object_category": selected_object_category,
            "temperature": temperature if temperature_value is None else _format_float_for_input(temperature_value),
            "forecast_days": str(days_ahead),
            "history_window": selected_history_window,
            "available_tables": table_options,
            "available_districts": [{"value": "all", "label": "Все районы"}],
            "available_causes": option_catalog["causes"],
            "available_object_categories": option_catalog["object_categories"],
            "available_forecast_days": [{"value": str(option), "label": f"{option} дней"} for option in FORECAST_DAY_OPTIONS],
            "available_history_windows": HISTORY_WINDOW_OPTIONS,
        },
    }
def _empty_forecasting_data(
    table_options: List[Dict[str, str]],
    selected_table: str,
    forecast_days: int,
    temperature: str,
    history_window: str,
) -> Dict[str, Any]:
    return {
        "generated_at": _format_datetime(datetime.now()),
        "has_data": False,
        "model_description": "",
        "summary": {
            "selected_table_label": _table_selection_label(selected_table),
            "slice_label": "Все пожары",
            "history_period_label": "Нет данных",
            "history_window_label": _history_window_label(history_window),
            "fires_count_display": "0",
            "history_days_display": "0",
            "active_days_display": "0",
            "last_observed_date": "-",
            "forecast_days_display": str(forecast_days),
            "predicted_total_display": "0",
            "predicted_average_display": "0",
            "average_probability_display": "0%",
            "historical_average_display": "0",
            "recent_average_display": "0",
            "forecast_vs_recent_display": "0%",
            "forecast_vs_recent_label": "Нет сравнения",
            "active_days_share_display": "0%",
            "peak_forecast_day_display": "-",
            "peak_forecast_value_display": "0",
            "peak_forecast_probability_display": "0%",
            "temperature_scenario_display": temperature.strip() or "Историческая сезонность",
        },
        "features": [],
        "insights": [],
        "charts": {
            "daily": _empty_chart_bundle("Что было и что ожидается", "Недостаточно данных для построения прогноза."),
            "breakdown": _empty_chart_bundle("Вероятность пожара по ближайшим дням", "Нет данных для ближайших дней."),
            "weekday": _empty_chart_bundle("В какие дни недели пожары случаются чаще", "Нет данных по дням недели."),
        },
        "forecast_rows": [],
        "notes": [],
        "filters": {
            "table_name": selected_table,
            "district": "all",
            "cause": "all",
            "object_category": "all",
            "temperature": temperature,
            "forecast_days": str(forecast_days),
            "history_window": history_window,
            "available_tables": table_options,
            "available_districts": [{"value": "all", "label": "Все районы"}],
            "available_causes": [{"value": "all", "label": "Все причины"}],
            "available_object_categories": [{"value": "all", "label": "Все категории"}],
            "available_forecast_days": [{"value": str(option), "label": f"{option} дней"} for option in FORECAST_DAY_OPTIONS],
            "available_history_windows": HISTORY_WINDOW_OPTIONS,
        },
    }
def _build_forecasting_table_options() -> List[Dict[str, str]]:
    options = []
    seen = set()
    for option in get_fire_map_table_options():
        value = str(option.get("value") or "").strip()
        if not value or value == "all" or value in seen:
            continue
        seen.add(value)
        options.append({"value": value, "label": str(option.get("label") or value)})
    return [{"value": "all", "label": "Все таблицы"}] + options


def _resolve_forecasting_selection(table_options: List[Dict[str, str]], table_name: str) -> str:
    values = {option["value"] for option in table_options}
    if table_name in values:
        return table_name
    return "all" if table_options else ""


def _selected_source_tables(table_options: List[Dict[str, str]], selected_table: str) -> List[str]:
    concrete = [option["value"] for option in table_options if option.get("value") and option["value"] != "all"]
    if selected_table == "all":
        return concrete
    return [selected_table] if selected_table in concrete else []


def _collect_forecasting_inputs(source_tables: List[str]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    records: List[Dict[str, Any]] = []
    metadata_items: List[Dict[str, Any]] = []
    notes: List[str] = []
    for source_table in source_tables:
        try:
            metadata = _load_table_metadata(source_table)
            metadata_items.append(metadata)
            records.extend(_load_forecasting_records(source_table, metadata["resolved_columns"]))
        except Exception as exc:
            notes.append(f"{source_table}: {exc}")

    records.sort(key=lambda item: item["date"])
    return records, metadata_items, notes


def _table_selection_label(selected_table: str) -> str:
    if selected_table == "all":
        return "Все таблицы"
    return selected_table or "Нет таблицы"

def _load_table_metadata(table_name: str) -> Dict[str, Any]:
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if table_name not in table_names:
        raise ValueError(f"Таблица '{table_name}' не найдена в базе данных.")

    columns = [column["name"] for column in inspector.get_columns(table_name)]
    resolved_columns = {
        "date": _resolve_column_name(columns, [DATE_COLUMN]),
        "district": _resolve_column_name(columns, DISTRICT_COLUMN_CANDIDATES),
        "temperature": _resolve_column_name(columns, TEMPERATURE_COLUMN_CANDIDATES),
        "cause": _resolve_column_name(columns, CAUSE_COLUMN_CANDIDATES),
        "object_category": _resolve_column_name(columns, [OBJECT_CATEGORY_COLUMN, "Категория объекта пожара"]),
        "latitude": _resolve_column_name(columns, LATITUDE_COLUMN_CANDIDATES),
        "longitude": _resolve_column_name(columns, LONGITUDE_COLUMN_CANDIDATES),
    }
    return {
        "table_name": table_name,
        "columns": columns,
        "resolved_columns": resolved_columns,
    }

def _load_forecasting_records(table_name: str, resolved_columns: Dict[str, str]) -> List[Dict[str, Any]]:
    date_column = resolved_columns["date"]
    if not date_column:
        return []

    select_parts = [f"{_date_expression(date_column)} AS fire_date"]
    if resolved_columns["district"]:
        select_parts.append(f"{_text_expression(resolved_columns['district'])} AS district_value")
    if resolved_columns["cause"]:
        select_parts.append(f"{_text_expression(resolved_columns['cause'])} AS cause_value")
    if resolved_columns["object_category"]:
        select_parts.append(f"{_text_expression(resolved_columns['object_category'])} AS object_category_value")
    if resolved_columns["temperature"]:
        select_parts.append(f"{_numeric_expression_for_column(resolved_columns['temperature'])} AS temperature_value")
    if resolved_columns["latitude"]:
        select_parts.append(f"{_numeric_expression_for_column(resolved_columns['latitude'])} AS latitude_value")
    if resolved_columns["longitude"]:
        select_parts.append(f"{_numeric_expression_for_column(resolved_columns['longitude'])} AS longitude_value")

    query = text(
        f"""
        SELECT {", ".join(select_parts)}
        FROM {_quote_identifier(table_name)}
        WHERE {_date_expression(date_column)} IS NOT NULL
        ORDER BY fire_date
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query).mappings().all()

    records: List[Dict[str, Any]] = []
    for row in rows:
        fire_date = row.get("fire_date")
        if fire_date is None:
            continue

        latitude = _clean_coordinate(row.get("latitude_value"), -90.0, 90.0)
        longitude = _clean_coordinate(row.get("longitude_value"), -180.0, 180.0)
        if latitude is None or longitude is None:
            latitude = None
            longitude = None

        records.append(
            {
                "date": fire_date,
                "district": _clean_option_value(row.get("district_value")),
                "cause": _clean_option_value(row.get("cause_value")),
                "object_category": _clean_option_value(row.get("object_category_value")),
                "temperature": _to_float_or_none(row.get("temperature_value")),
                "latitude": latitude,
                "longitude": longitude,
            }
        )
    return records

def _build_option_catalog(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, str]]]:
    return {
        "districts": _build_options(records, "district", "Все районы"),
        "causes": _build_options(records, "cause", "Все причины"),
        "object_categories": _build_options(records, "object_category", "Все категории"),
    }


def _build_options(records: List[Dict[str, Any]], key: str, default_label: str) -> List[Dict[str, str]]:
    counter = Counter(record[key] for record in records if record.get(key))
    options = [{"value": "all", "label": default_label}]
    for value, count in sorted(counter.items(), key=lambda item: (-item[1], item[0].lower()))[:200]:
        options.append({"value": value, "label": f"{value} ({count})"})
    return options


def _build_daily_history(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not records:
        return []

    counts_by_date: Dict[date, int] = defaultdict(int)
    temps_by_date: Dict[date, List[float]] = defaultdict(list)
    min_date = records[0]["date"]
    max_date = records[-1]["date"]

    for record in records:
        fire_date = record["date"]
        counts_by_date[fire_date] += 1
        if record["temperature"] is not None:
            temps_by_date[fire_date].append(record["temperature"])

    history: List[Dict[str, Any]] = []
    current = min_date
    while current <= max_date:
        day_temps = temps_by_date.get(current, [])
        history.append(
            {
                "date": current,
                "count": counts_by_date.get(current, 0),
                "avg_temperature": round(mean(day_temps), 2) if day_temps else None,
            }
        )
        current += timedelta(days=1)
    return history


def _build_forecast_rows(
    daily_history: List[Dict[str, Any]],
    forecast_days: int,
    temperature_value: Optional[float],
) -> List[Dict[str, Any]]:
    if not daily_history or forecast_days <= 0:
        return []

    history_counts = [float(item["count"]) for item in daily_history]
    history_dates = [item["date"] for item in daily_history]
    overall_average = mean(history_counts) if history_counts else 0.0
    recent_counts = history_counts[-28:] if len(history_counts) >= 28 else history_counts
    very_recent_counts = history_counts[-14:] if len(history_counts) >= 14 else history_counts
    previous_counts = history_counts[-56:-28] if len(history_counts) >= 56 else history_counts[:-len(recent_counts)] if len(history_counts) > len(recent_counts) else []
    recent_average = mean(recent_counts) if recent_counts else overall_average
    very_recent_average = mean(very_recent_counts) if very_recent_counts else recent_average
    previous_average = mean(previous_counts) if previous_counts else recent_average
    trend_ratio = _clamp(((very_recent_average - previous_average) / previous_average) if previous_average > 0 else 0.0, -0.22, 0.22)
    base_recent_level = 0.65 * very_recent_average + 0.35 * recent_average if recent_counts else overall_average

    weekday_factor: Dict[int, float] = {}
    for weekday in range(7):
        weekday_values = [float(item["count"]) for item in daily_history if item["date"].weekday() == weekday]
        weekday_avg = mean(weekday_values) if weekday_values else overall_average
        raw_factor = (weekday_avg / overall_average) if overall_average > 0 else 1.0
        reliability = min(1.0, len(weekday_values) / 12.0)
        weekday_factor[weekday] = 1.0 + (raw_factor - 1.0) * reliability * 0.7

    month_factor: Dict[int, float] = {}
    seasonal_temperature_by_month: Dict[int, float] = {}
    overall_temperature_values = [item["avg_temperature"] for item in daily_history if item["avg_temperature"] is not None]
    overall_temperature_average = mean(overall_temperature_values) if overall_temperature_values else None

    for month in range(1, 13):
        month_values = [float(item["count"]) for item in daily_history if item["date"].month == month]
        month_avg = mean(month_values) if month_values else overall_average
        raw_factor = (month_avg / overall_average) if overall_average > 0 else 1.0
        reliability = min(1.0, len(month_values) / 45.0)
        month_factor[month] = 1.0 + (raw_factor - 1.0) * reliability * 0.55
        month_temps = [item["avg_temperature"] for item in daily_history if item["date"].month == month and item["avg_temperature"] is not None]
        if month_temps:
            seasonal_temperature_by_month[month] = mean(month_temps)

    temperature_pairs = [
        (float(item["avg_temperature"]), float(item["count"]))
        for item in daily_history
        if item["avg_temperature"] is not None
    ]
    temperature_slope = _compute_temperature_slope(temperature_pairs)
    volatility = pstdev(recent_counts) if len(recent_counts) > 1 else pstdev(history_counts) if len(history_counts) > 1 else 0.0
    recent_peak = max(recent_counts) if recent_counts else max(history_counts)
    robust_ceiling = max(
        recent_peak * 1.35,
        base_recent_level * 2.4 + max(1.0, volatility),
        overall_average + 3.5 * max(1.0, volatility),
    )

    forecast_rows: List[Dict[str, Any]] = []
    last_observed_date = history_dates[-1]

    for step in range(1, forecast_days + 1):
        target_date = last_observed_date + timedelta(days=step)
        seasonal_factor = weekday_factor.get(target_date.weekday(), 1.0) * month_factor.get(target_date.month, 1.0)
        usual_for_day = max(0.0, base_recent_level * seasonal_factor)
        trend_effect = base_recent_level * trend_ratio * (0.75 - 0.45 * ((step - 1) / max(1, forecast_days - 1)))

        temperature_for_day = temperature_value
        if temperature_for_day is None:
            temperature_for_day = seasonal_temperature_by_month.get(target_date.month, overall_temperature_average)

        temperature_effect = 0.0
        if (
            temperature_for_day is not None
            and overall_temperature_average is not None
            and temperature_slope is not None
        ):
            seasonal_temperature = seasonal_temperature_by_month.get(target_date.month, overall_temperature_average)
            raw_temperature_effect = temperature_slope * (temperature_for_day - seasonal_temperature) * 0.35
            temperature_cap = max(0.6, volatility)
            temperature_effect = _clamp(raw_temperature_effect, -temperature_cap, temperature_cap)

        estimate = _clamp(usual_for_day + trend_effect + temperature_effect, 0.0, robust_ceiling)
        spread = max(0.75, volatility * (0.95 + step * 0.03))
        lower_bound = max(0.0, estimate - spread)
        upper_bound = min(robust_ceiling + spread, estimate + spread)
        rounded_estimate = round(estimate, 2)
        scenario_label, scenario_tone = _forecast_level_label(estimate, recent_average if recent_average > 0 else overall_average)

        forecast_rows.append(
            {
                "date": target_date.isoformat(),
                "date_display": target_date.strftime("%d.%m.%Y"),
                "weekday_label": WEEKDAY_LABELS[target_date.weekday()],
                "forecast_value": rounded_estimate,
                "forecast_value_display": _format_number(rounded_estimate),
                "forecast_value_human_display": f"около {_format_number(rounded_estimate)}",
                "fire_probability": round(_probability_from_expected_count(estimate), 4),
                "fire_probability_display": _format_probability(_probability_from_expected_count(estimate)),
                "fire_probability_range_display": (
                    f"{_format_probability(_probability_from_expected_count(lower_bound))} - "
                    f"{_format_probability(_probability_from_expected_count(upper_bound))}"
                ),
                "usual_value": round(usual_for_day, 2),
                "usual_value_display": _format_number(usual_for_day),
                "lower_bound": round(lower_bound, 2),
                "lower_bound_display": _format_number(lower_bound),
                "upper_bound": round(upper_bound, 2),
                "upper_bound_display": _format_number(upper_bound),
                "range_display": _format_count_range(lower_bound, upper_bound),
                "temperature_display": (
                    f"{_format_number(temperature_for_day)} °C"
                    if temperature_for_day is not None
                    else "Сезонная средняя"
                ),
                "scenario_label": scenario_label,
                "scenario_tone": scenario_tone,
                "scenario_hint": _relative_delta_text(
                    estimate,
                    usual_for_day,
                    reference_label="к обычному уровню для такого дня",
                ),
            }
        )

    return forecast_rows
def _build_weekday_profile(daily_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for weekday in range(7):
        values = [float(item["count"]) for item in daily_history if item["date"].weekday() == weekday]
        avg_value = mean(values) if values else 0.0
        total_value = sum(values)
        items.append(
            {
                "weekday": weekday,
                "label": WEEKDAY_LABELS[weekday],
                "avg_value": round(avg_value, 2),
                "avg_display": _format_number(avg_value),
                "total_value": round(total_value, 2),
                "total_display": _format_number(total_value),
            }
        )
    return items


def _build_weekly_outlook(daily_history: List[Dict[str, Any]], forecast_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    history_buckets: Dict[date, float] = defaultdict(float)
    for item in daily_history:
        history_buckets[_week_start(item["date"])] += float(item["count"])

    forecast_buckets: Dict[date, float] = defaultdict(float)
    for row in forecast_rows:
        row_date = _parse_iso_date(row["date"])
        forecast_buckets[_week_start(row_date)] += float(row["forecast_value"])

    keys = sorted(set(history_buckets) | set(forecast_buckets))
    if not keys:
        return []

    visible_keys = keys[-10:] if len(keys) > 10 else keys
    last_history_week = _week_start(daily_history[-1]["date"]) if daily_history else None
    items = []
    for bucket_start in visible_keys:
        items.append(
            {
                "week_start": bucket_start,
                "label": bucket_start.strftime("%d.%m"),
                "actual": round(history_buckets.get(bucket_start, 0.0), 2),
                "forecast": round(forecast_buckets.get(bucket_start, 0.0), 2),
                "actual_display": _format_number(history_buckets.get(bucket_start, 0.0)),
                "forecast_display": _format_number(forecast_buckets.get(bucket_start, 0.0)),
                "is_future": bool(last_history_week and bucket_start > last_history_week),
            }
        )
    return items


def _build_monthly_outlook(daily_history: List[Dict[str, Any]], forecast_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    history_by_month_of_year: Dict[int, List[float]] = defaultdict(list)
    if daily_history:
        month_totals: Dict[Tuple[int, int], float] = defaultdict(float)
        for item in daily_history:
            key = (item["date"].year, item["date"].month)
            month_totals[key] += float(item["count"])
        for (year_value, month_value), total_value in month_totals.items():
            history_by_month_of_year[month_value].append(total_value)

    forecast_by_month: Dict[Tuple[int, int], float] = defaultdict(float)
    for row in forecast_rows:
        row_date = _parse_iso_date(row["date"])
        forecast_by_month[(row_date.year, row_date.month)] += float(row["forecast_value"])

    items = []
    for year_value, month_value in sorted(forecast_by_month):
        forecast_total = forecast_by_month[(year_value, month_value)]
        baseline = mean(history_by_month_of_year.get(month_value, [])) if history_by_month_of_year.get(month_value) else 0.0
        delta_ratio = ((forecast_total - baseline) / baseline) if baseline > 0 else 0.0
        level_label, level_tone = _forecast_level_label(forecast_total, baseline)
        items.append(
            {
                "month_key": f"{year_value:04d}-{month_value:02d}",
                "label": f"{MONTH_LABELS[month_value]} {year_value}",
                "forecast": round(forecast_total, 2),
                "forecast_display": _format_number(forecast_total),
                "baseline": round(baseline, 2),
                "baseline_display": _format_number(baseline),
                "delta_percent_display": _format_signed_percent(delta_ratio),
                "level_label": level_label,
                "level_tone": level_tone,
            }
        )
    return items[:4]

def _build_summary(
    selected_table: str,
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    temperature_value: Optional[float],
    daily_history: List[Dict[str, Any]],
    filtered_records: List[Dict[str, Any]],
    forecast_rows: List[Dict[str, Any]],
    history_window: str,
) -> Dict[str, str]:
    history_dates = [item["date"] for item in daily_history]
    history_counts = [float(item["count"]) for item in daily_history]
    active_days = sum(1 for item in daily_history if item["count"] > 0)
    predicted_total = sum(item["forecast_value"] for item in forecast_rows)
    predicted_average = predicted_total / len(forecast_rows) if forecast_rows else 0.0
    average_probability = mean(float(item.get("fire_probability", 0.0)) for item in forecast_rows) if forecast_rows else 0.0
    historical_average = mean(history_counts) if history_counts else 0.0
    recent_counts = history_counts[-28:] if len(history_counts) >= 28 else history_counts
    recent_average = mean(recent_counts) if recent_counts else historical_average
    active_days_share = (active_days / len(daily_history) * 100.0) if daily_history else 0.0
    delta_ratio = ((predicted_average - recent_average) / recent_average) if recent_average > 0 else 0.0
    scenario_label, _scenario_tone = _forecast_level_label(predicted_average, recent_average if recent_average > 0 else historical_average)
    peak_row = max(forecast_rows, key=lambda item: float(item["forecast_value"])) if forecast_rows else None

    slice_parts = []
    if selected_cause != "all":
        slice_parts.append(f"причина: {selected_cause}")
    if selected_object_category != "all":
        slice_parts.append(f"категория: {selected_object_category}")

    return {
        "selected_table_label": _table_selection_label(selected_table),
        "slice_label": " | ".join(slice_parts) if slice_parts else "Все пожары выбранной истории",
        "history_period_label": _format_period(history_dates),
        "history_window_label": _history_window_label(history_window),
        "fires_count_display": _format_integer(len(filtered_records)),
        "history_days_display": _format_integer(len(daily_history)),
        "active_days_display": _format_integer(active_days),
        "active_days_share_display": _format_percent(active_days_share),
        "last_observed_date": history_dates[-1].strftime("%d.%m.%Y") if history_dates else "-",
        "forecast_days_display": _format_integer(len(forecast_rows)),
        "predicted_total_display": _format_number(predicted_total),
        "predicted_average_display": _format_number(predicted_average),
        "average_probability_display": _format_probability(average_probability),
        "historical_average_display": _format_number(historical_average),
        "recent_average_display": _format_number(recent_average),
        "forecast_vs_recent_display": _format_signed_percent(delta_ratio),
        "forecast_vs_recent_label": scenario_label,
        "peak_forecast_day_display": peak_row["date_display"] if peak_row else "-",
        "peak_forecast_value_display": peak_row["forecast_value_display"] if peak_row else "0",
        "peak_forecast_probability_display": peak_row["fire_probability_display"] if peak_row else "0%",
        "temperature_scenario_display": (
            f"{_format_number(temperature_value)} °C" if temperature_value is not None else "Историческая сезонность"
        ),
    }
def _build_feature_cards(metadata: Any) -> List[Dict[str, str]]:
    metadata_items = metadata if isinstance(metadata, list) else [metadata]
    metadata_items = [item for item in metadata_items if item]
    if not metadata_items:
        return []

    total_tables = len(metadata_items)
    feature_config = [
        ("date", "Дата возникновения пожара", "Нужна для построения дневного временного ряда."),
        ("temperature", "Температура", "Используется для температурной поправки, если колонка найдена."),
        ("cause", "Причина", "Позволяет строить сценарий по конкретной причине пожара."),
        ("object_category", "Категория объекта", "Позволяет строить сценарий по типу объекта."),
    ]
    cards = []
    for key, label, description in feature_config:
        sources = []
        found = 0
        for item in metadata_items:
            source_column = item["resolved_columns"].get(key)
            if source_column:
                found += 1
                sources.append(f"{item['table_name']}: {source_column}")
        if found == total_tables:
            status = "used"
            status_label = "Используется"
        elif found > 0:
            status = "partial"
            status_label = f"Частично ({found}/{total_tables})"
        else:
            status = "missing"
            status_label = "Не найдена"
        cards.append(
            {
                "label": label,
                "status": status,
                "status_label": status_label,
                "source": "; ".join(sources[:3]) if sources else "Не найдена",
                "description": description,
            }
        )
    return cards

def _build_insights(
    daily_history: List[Dict[str, Any]],
    forecast_rows: List[Dict[str, Any]],
    weekday_profile: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    insights = []
    if forecast_rows:
        peak_row = max(forecast_rows, key=lambda item: float(item["fire_probability"]))
        insights.append(
            {
                "label": "Самый рискованный день",
                "value": peak_row["date_display"],
                "meta": f"вероятность пожара {peak_row['fire_probability_display']}",
                "tone": "fire",
            }
        )
        average_probability = mean(float(item.get("fire_probability", 0.0)) for item in forecast_rows)
        insights.append(
            {
                "label": "Средняя вероятность пожара",
                "value": _format_probability(average_probability),
                "meta": f"в день на ближайшие {len(forecast_rows)} дней",
                "tone": "forest",
            }
        )
    if daily_history and forecast_rows:
        recent_values = [float(item["count"]) for item in (daily_history[-28:] if len(daily_history) >= 28 else daily_history)]
        recent_average = mean(recent_values) if recent_values else 0.0
        forecast_average = mean(float(item["forecast_value"]) for item in forecast_rows)
        insights.append(
            {
                "label": "Сравнение с недавним уровнем",
                "value": _format_signed_percent(((forecast_average - recent_average) / recent_average) if recent_average > 0 else 0.0),
                "meta": f"{_forecast_level_label(forecast_average, recent_average)[0]} относительно последних 4 недель",
                "tone": "sky",
            }
        )
    if weekday_profile:
        peak_weekday = max(weekday_profile, key=lambda item: float(item["avg_value"]))
        insights.append(
            {
                "label": "Самый активный день недели",
                "value": peak_weekday["label"],
                "meta": f"в истории в среднем {peak_weekday['avg_display']} пожара",
                "tone": "sand",
            }
        )
    stability_label, stability_meta = _forecast_stability_hint(daily_history)
    insights.append(
        {
            "label": "Надежность оценки",
            "value": stability_label,
            "meta": stability_meta,
            "tone": "sky",
        }
    )
    return insights[:4]
def _build_notes(
    metadata: Dict[str, Any],
    filtered_records: List[Dict[str, Any]],
    daily_history: List[Dict[str, Any]],
    temperature_value: Optional[float],
) -> List[str]:
    metadata_items = metadata if isinstance(metadata, list) else [metadata]
    metadata_items = [item for item in metadata_items if item]
    notes: List[str] = []

    if len(metadata_items) > 1:
        notes.append(f"Прогноз собран сразу по {len(metadata_items)} таблицам.")

    if not any(item["resolved_columns"].get("date") for item in metadata_items):
        notes.append("В выбранных таблицах не найдена дата возникновения пожара.")
    if not filtered_records:
        notes.append("По выбранным фильтрам нет пожаров в истории. Попробуйте снять часть ограничений.")
    elif len(daily_history) < 14:
        notes.append("История короткая, поэтому прогноз может быть шумным.")

    if temperature_value is not None and not any(item["resolved_columns"].get("temperature") for item in metadata_items):
        notes.append("Температурный сценарий задан, но колонка температуры не найдена ни в одной таблице.")
    if any(not item["resolved_columns"].get("cause") for item in metadata_items):
        notes.append("Не во всех таблицах найдена причина пожара, поэтому этот фильтр работает частично.")
    if any(not item["resolved_columns"].get("object_category") for item in metadata_items):
        notes.append("Не во всех таблицах найдена категория объекта, поэтому этот фильтр работает частично.")

    notes.append("Прогноз лучше читать как ориентир по уровню нагрузки, а не как точное обещание числа пожаров в каждый день.")
    return notes
def _build_geo_prediction(
    records: List[Dict[str, Any]],
    forecast_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    geo_records = [
        record
        for record in records
        if record.get("latitude") is not None and record.get("longitude") is not None
    ]
    if not geo_records:
        return {
            "has_coordinates": False,
            "model_description": "Для геопрогноза нужны валидные координаты Широта и Долгота в выбранном срезе.",
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
    future_dates = [_parse_iso_date(row["date"]) for row in forecast_rows]
    if not future_dates:
        future_dates = [last_observed_date + timedelta(days=offset) for offset in range(1, 8)]

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
            "Легкая geo + ML модель собирает исторические пожары в координатную сетку и поднимает наверх те зоны, "
            "где точки плотнее, события были недавно и их сезонный профиль ближе к ближайшему горизонту прогноза."
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


def _build_forecast_chart(daily_history: List[Dict[str, Any]], forecast_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    title = "Что было и что ожидается"
    if not daily_history:
        return {
            "title": title,
            "plotly": _build_empty_plotly("Нет данных для исторического ряда."),
            "empty_message": "Нет данных для исторического ряда.",
        }

    if not PLOTLY_AVAILABLE:
        return {
            "title": title,
            "plotly": {"data": [], "layout": {}, "config": {"responsive": True}},
            "empty_message": "Plotly не найден в окружении. Таблица прогноза ниже остается доступной.",
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
                name="Прогноз",
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
    title = "Вероятность пожара по ближайшим дням"
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
    figure.add_trace(go.Bar(x=labels, y=forecast_values, name="Ближайшие прогнозные недели", marker=dict(color=PLOTLY_PALETTE["sky"]), hovertemplate="<b>%{x}</b><br>Прогноз: %{y:.1f} пожара<extra></extra>"))
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
    figure.add_trace(go.Bar(x=[item["label"] for item in monthly_outlook], y=[item["forecast"] for item in monthly_outlook], name="Прогноз", text=[item["delta_percent_display"] for item in monthly_outlook], textposition="outside", marker=dict(color=PLOTLY_PALETTE["forest"]), customdata=[[item["baseline_display"], item["delta_percent_display"], item["level_label"]] for item in monthly_outlook], hovertemplate="<b>%{x}</b><br>Прогноз: %{y:.1f} пожара<br>Обычный уровень: %{customdata[0]}<br>Изменение: %{customdata[1]}<br>%{customdata[2]}<extra></extra>"))
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
    title = "Где вероятнее пожар"
    points = geo_prediction.get("points") or []
    if not points:
        message = "Нет координат для геопрогноза." if not geo_prediction.get("has_coordinates") else "Недостаточно устойчивых зон для карты риска."
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
        height=430,
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
    plotly_payload = _build_empty_plotly(message) if use_plotly else {"data": [], "layout": {}, "config": {"responsive": True}}
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
        return {"data": [], "layout": {}, "config": {"responsive": True}, "empty_message": message}

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
    if not PLOTLY_AVAILABLE or json is None or PlotlyJSONEncoder is None:
        return {"data": [], "layout": {}, "config": {"responsive": True}}

    payload = json.loads(json.dumps(figure, cls=PlotlyJSONEncoder))
    if isinstance(payload.get("layout"), dict):
        payload["layout"].pop("template", None)
    payload["config"] = {
        "responsive": True,
        "displaylogo": False,
        "modeBarButtonsToRemove": [
            "lasso2d",
            "select2d",
            "autoScale2d",
            "toggleSpikelines",
        ],
    }
    return payload


def _get_plotly_bundle() -> str:
    if not PLOTLY_AVAILABLE or get_plotlyjs is None:
        return ""
    return get_plotlyjs()


def _resolve_column_name(columns: Sequence[str], candidates: Sequence[str]) -> str:
    if not columns:
        return ""

    normalized_columns = {column: _normalize_match_text(column) for column in columns}
    normalized_candidates = [_normalize_match_text(candidate) for candidate in candidates if candidate]

    for candidate in candidates:
        if candidate in columns:
            return candidate

    best_match = ""
    best_score = -1
    for column_name, normalized_column in normalized_columns.items():
        for normalized_candidate in normalized_candidates:
            if normalized_column == normalized_candidate:
                return column_name

            score = 0
            if normalized_candidate in normalized_column or normalized_column in normalized_candidate:
                score = min(len(normalized_candidate), len(normalized_column))
            else:
                candidate_tokens = {token for token in normalized_candidate.split(" ") if token}
                column_tokens = {token for token in normalized_column.split(" ") if token}
                common_tokens = candidate_tokens & column_tokens
                if common_tokens:
                    score = sum(len(token) for token in common_tokens)

            if score > best_score:
                best_score = score
                best_match = column_name

    return best_match if best_score >= 8 else ""


def _resolve_option_value(options: List[Dict[str, str]], selected_value: str) -> str:
    available_values = {option["value"] for option in options}
    return selected_value if selected_value in available_values else "all"


def _clean_option_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_history_window(value: str) -> str:
    available_values = {option["value"] for option in HISTORY_WINDOW_OPTIONS}
    return value if value in available_values else "all"


def _history_window_label(value: str) -> str:
    for option in HISTORY_WINDOW_OPTIONS:
        if option["value"] == value:
            return option["label"]
    return "\u0412\u0441\u0435 \u0433\u043e\u0434\u044b"


def _apply_history_window(records: List[Dict[str, Any]], history_window: str) -> List[Dict[str, Any]]:
    if not records or history_window == "all":
        return records

    latest_year = max(record["date"].year for record in records)
    if history_window == "recent_3":
        min_year = latest_year - 2
    elif history_window == "recent_5":
        min_year = latest_year - 4
    else:
        return records

    return [record for record in records if record["date"].year >= min_year]


def _parse_forecast_days(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = 14
    return parsed if parsed in FORECAST_DAY_OPTIONS else 14


def _parse_float(value: str) -> Optional[float]:
    if value is None:
        return None
    normalized = str(value).strip().replace(",", ".")
    if not normalized:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def _to_float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_coordinate(value: Any, minimum: float, maximum: float) -> Optional[float]:
    numeric = _to_float_or_none(value)
    if numeric is None:
        return None
    if numeric < minimum or numeric > maximum:
        return None
    return round(numeric, 6)


def _compute_temperature_slope(pairs: List[tuple[float, float]]) -> Optional[float]:
    if len(pairs) < 5:
        return None
    temperatures = [pair[0] for pair in pairs]
    fire_counts = [pair[1] for pair in pairs]
    avg_temperature = mean(temperatures)
    avg_count = mean(fire_counts)
    variance = sum((value - avg_temperature) ** 2 for value in temperatures)
    if variance <= 0:
        return None
    covariance = sum((temp - avg_temperature) * (count - avg_count) for temp, count in pairs)
    return covariance / variance


def _week_start(value: date) -> date:
    return value - timedelta(days=value.weekday())


def _parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _rolling_average(values: Sequence[float], window: int) -> List[float]:
    items = [float(value) for value in values]
    result: List[float] = []
    for index in range(len(items)):
        start = max(0, index - window + 1)
        subset = items[start:index + 1]
        result.append(round(mean(subset), 2) if subset else 0.0)
    return result


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _scenario_color(tone: str) -> str:
    palette = {
        "fire": PLOTLY_PALETTE["fire"],
        "forest": PLOTLY_PALETTE["forest"],
        "sky": PLOTLY_PALETTE["sky"],
        "sand": PLOTLY_PALETTE["sand"],
    }
    return palette.get(tone, PLOTLY_PALETTE["sky"])


def _forecast_stability_hint(daily_history: List[Dict[str, Any]]) -> Tuple[str, str]:
    total_days = len(daily_history)
    active_days = sum(1 for item in daily_history if float(item["count"]) > 0)
    if total_days >= 180 and active_days >= 45:
        return "Выше средней", "истории достаточно, поэтому прогноз обычно стабильнее"
    if total_days >= 60 and active_days >= 15:
        return "Средняя", "истории хватает для ориентировочного сценария"
    return "Ниже средней", "данных мало или они слишком редкие, поэтому важнее смотреть на общий тренд"


def _probability_from_expected_count(value: float) -> float:
    return _clamp(1.0 - math.exp(-max(0.0, float(value))), 0.0, 0.995)


def _format_probability(value: float) -> str:
    return _format_percent(value * 100.0)


def _forecast_level_label(value: float, reference: float) -> Tuple[str, str]:
    if reference <= 0:
        if value <= 0:
            return "На уровне нуля", "sky"
        return "Выше обычного", "fire"

    ratio = value / reference
    if ratio >= 1.2:
        return "Выше обычного", "fire"
    if ratio <= 0.8:
        return "Ниже обычного", "forest"
    return "Около обычного", "sky"


def _format_count_range(lower: float, upper: float) -> str:
    lower_bound = max(0, int(math.floor(lower)))
    upper_bound = max(lower_bound, int(math.ceil(upper)))
    return f"{_format_integer(lower_bound)}-{_format_integer(upper_bound)} пожара"


def _format_signed_percent(value: float) -> str:
    percent_value = value * 100.0
    if abs(percent_value) < 0.05:
        return "0%"
    rounded = round(percent_value, 1)
    if abs(rounded - round(rounded)) < 1e-9:
        text = str(int(round(rounded)))
    else:
        text = str(rounded).replace(".", ",")
    sign = "+" if rounded > 0 else ""
    return f"{sign}{text}%"


def _format_percent(value: float) -> str:
    rounded = round(value, 1)
    if abs(rounded - round(rounded)) < 1e-9:
        return f"{int(round(rounded))}%"
    return f"{str(rounded).replace('.', ',')}%"


def _relative_delta_text(value: float, reference: float, reference_label: str) -> str:
    if reference <= 0:
        return "нет устойчивой базы для сравнения"
    delta_ratio = (value - reference) / reference
    if abs(delta_ratio) < 0.05:
        return f"почти без изменений {reference_label}"
    return f"{_format_signed_percent(delta_ratio)} {reference_label}"


def _format_period(values: Sequence[date]) -> str:
    if not values:
        return "Нет данных"
    ordered = sorted(values)
    return f"{ordered[0].strftime('%d.%m.%Y')} - {ordered[-1].strftime('%d.%m.%Y')}"


def _format_integer(value: Any) -> str:
    try:
        return f"{int(round(float(value))):,}".replace(",", " ")
    except (TypeError, ValueError):
        return "0"


def _format_number(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "0"
    if abs(numeric - round(numeric)) < 1e-9:
        return f"{int(round(numeric)):,}".replace(",", " ")
    return f"{numeric:,.1f}".replace(",", " ").replace(".", ",")


def _format_float_for_input(value: float) -> str:
    text_value = f"{value:.2f}".rstrip("0").rstrip(".")
    return text_value.replace(",", ".")


def _format_datetime(value: datetime) -> str:
    return value.strftime("%d.%m.%Y %H:%M")


def _quote_identifier(identifier: str) -> str:
    return engine.dialect.identifier_preparer.quote(identifier)


def _normalize_match_text(value: str) -> str:
    normalized = str(value).lower().replace("ё", "е")
    normalized = " ".join(normalized.replace("/", " ").replace("-", " ").split())
    return normalized


def _text_expression(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    return f"NULLIF(TRIM(CAST({column_sql} AS TEXT)), '')"


def _numeric_expression_for_column(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    cleaned = f"NULLIF(REPLACE(REPLACE(REPLACE(CAST({column_sql} AS TEXT), ' ', ''), ',', '.'), CHR(160), ''), '')"
    return f"CASE WHEN {cleaned} ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN ({cleaned})::double precision ELSE NULL END"


def _date_expression(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    text_value = f"TRIM(CAST({column_sql} AS TEXT))"
    return (
        "CASE "
        f"WHEN {text_value} ~ '^[0-9]{{2}}\\.[0-9]{{2}}\\.[0-9]{{4}}$' THEN TO_DATE({text_value}, 'DD.MM.YYYY') "
        f"WHEN {text_value} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' THEN TO_DATE(SUBSTRING({text_value} FROM 1 FOR 10), 'YYYY-MM-DD') "
        f"WHEN {text_value} ~ '^[0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}}' THEN TO_DATE(SUBSTRING({text_value} FROM 1 FOR 10), 'YYYY/MM/DD') "
        "ELSE NULL END"
    )
























