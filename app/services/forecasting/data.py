from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import inspect, text

from app.services.table_options import get_fire_map_table_options
from config.db import engine

from .constants import *
from .utils import (
    _clean_coordinate,
    _clean_option_value,
    _clamp,
    _compute_temperature_slope,
    _date_expression,
    _forecast_level_label,
    _format_count_range,
    _format_number,
    _format_probability,
    _format_signed_percent,
    _numeric_expression_for_column,
    _parse_iso_date,
    _probability_from_expected_count,
    _quote_identifier,
    _relative_delta_text,
    _resolve_column_name,
    _text_expression,
    _to_float_or_none,
    _week_start,
)
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

