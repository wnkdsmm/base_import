from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

from app.db_metadata import get_table_columns_cached
from app.runtime_cache import CopyingTtlCache
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
    _quote_identifier,
    _relative_delta_text,
    _resolve_column_name,
    _text_expression,
    _to_float_or_none,
    _week_start,
)

_FORECASTING_SQL_CACHE = CopyingTtlCache(ttl_seconds=120.0)


def clear_forecasting_sql_cache() -> None:
    _FORECASTING_SQL_CACHE.clear()


def _normalize_filter_value(value: str) -> str:
    normalized = str(value or "").strip()
    return normalized or "all"


def _history_window_year_span(history_window: str) -> int:
    if history_window == "recent_3":
        return 3
    if history_window == "recent_5":
        return 5
    return 0


def _build_sql_cache_key(prefix: str, source_tables: Sequence[str], *parts: Any) -> tuple[Any, ...]:
    return (prefix, *tuple(source_tables), *parts)


def _build_forecasting_table_options() -> List[Dict[str, str]]:
    options = []
    seen = set()
    for option in get_fire_map_table_options():
        value = str(option.get("value") or "").strip()
        if not value or value == "all" or value in seen:
            continue
        seen.add(value)
        options.append({"value": value, "label": str(option.get("label") or value)})
    return [{"value": "all", "label": "\u0412\u0441\u0435 \u0442\u0430\u0431\u043b\u0438\u0446\u044b"}] + options


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


def _collect_forecasting_metadata(source_tables: Sequence[str]) -> tuple[List[Dict[str, Any]], List[str]]:
    metadata_items: List[Dict[str, Any]] = []
    notes: List[str] = []
    for source_table in source_tables:
        try:
            metadata = _load_table_metadata(source_table)
            metadata_items.append(metadata)
        except Exception as exc:
            notes.append(f"{source_table}: {exc}")
    return metadata_items, notes


def _collect_forecasting_inputs(
    source_tables: List[str],
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    history_window: str = "all",
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    records: List[Dict[str, Any]] = []
    metadata_items, notes = _collect_forecasting_metadata(source_tables)
    min_year = _resolve_history_window_min_year(metadata_items, history_window)
    for metadata in metadata_items:
        try:
            records.extend(
                _load_forecasting_records(
                    metadata["table_name"],
                    metadata["resolved_columns"],
                    district=district,
                    cause=cause,
                    object_category=object_category,
                    min_year=min_year,
                )
            )
        except Exception as exc:
            notes.append(f"{metadata['table_name']}: {exc}")

    records.sort(key=lambda item: item["date"])
    return records, metadata_items, notes

def _table_selection_label(selected_table: str) -> str:
    if selected_table == "all":
        return "\u0412\u0441\u0435 \u0442\u0430\u0431\u043b\u0438\u0446\u044b"
    return selected_table or "\u041d\u0435\u0442 \u0442\u0430\u0431\u043b\u0438\u0446\u044b"

def _load_table_metadata(table_name: str) -> Dict[str, Any]:
    try:
        columns = get_table_columns_cached(table_name)
    except ValueError as exc:
        raise ValueError(f"\u0422\u0430\u0431\u043b\u0438\u0446\u0430 '{table_name}' \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430 \u0432 \u0431\u0430\u0437\u0435 \u0434\u0430\u043d\u043d\u044b\u0445.") from exc
    resolved_columns = {
        "date": _resolve_column_name(columns, [DATE_COLUMN]),
        "district": _resolve_column_name(columns, DISTRICT_COLUMN_CANDIDATES),
        "temperature": _resolve_column_name(columns, TEMPERATURE_COLUMN_CANDIDATES),
        "cause": _resolve_column_name(columns, CAUSE_COLUMN_CANDIDATES),
        "object_category": _resolve_column_name(columns, [OBJECT_CATEGORY_COLUMN, "\u041a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u044f \u043e\u0431\u044a\u0435\u043a\u0442\u0430 \u043f\u043e\u0436\u0430\u0440\u0430"]),
        "latitude": _resolve_column_name(columns, LATITUDE_COLUMN_CANDIDATES),
        "longitude": _resolve_column_name(columns, LONGITUDE_COLUMN_CANDIDATES),
    }
    return {
        "table_name": table_name,
        "columns": columns,
        "resolved_columns": resolved_columns,
    }

def _resolve_history_window_min_year(metadata_items: Sequence[Dict[str, Any]], history_window: str) -> Optional[int]:
    year_span = _history_window_year_span(history_window)
    if year_span <= 0 or not metadata_items:
        return None

    source_tables = [str(item.get("table_name") or "") for item in metadata_items if item.get("table_name")]
    cache_key = _build_sql_cache_key("history_window_year", source_tables, history_window)
    cached_year = _FORECASTING_SQL_CACHE.get(cache_key)
    if cached_year is not None:
        return int(cached_year)

    latest_years: List[int] = []
    with engine.connect() as conn:
        for metadata in metadata_items:
            resolved_columns = metadata.get("resolved_columns") or {}
            date_column = resolved_columns.get("date")
            table_name = str(metadata.get("table_name") or "")
            if not date_column or not table_name:
                continue
            date_expression = _date_expression(date_column)
            query = text(
                f"""
                SELECT MAX(EXTRACT(YEAR FROM {date_expression})) AS max_year
                FROM {_quote_identifier(table_name)}
                WHERE {date_expression} IS NOT NULL
                """
            )
            max_year = conn.execute(query).scalar()
            if max_year is not None:
                latest_years.append(int(max_year))

    if not latest_years:
        return None

    min_year = max(latest_years) - (year_span - 1)
    _FORECASTING_SQL_CACHE.set(cache_key, min_year)
    return min_year


def _build_scope_conditions(
    resolved_columns: Dict[str, str],
    min_year: Optional[int] = None,
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
) -> tuple[Optional[str], list[str], Dict[str, Any], bool]:
    date_column = resolved_columns["date"]
    if not date_column:
        return None, [], {}, True

    date_expression = _date_expression(date_column)
    conditions = [f"{date_expression} IS NOT NULL"]
    params: Dict[str, Any] = {}
    if min_year is not None:
        conditions.append(f"EXTRACT(YEAR FROM {date_expression}) >= :min_year")
        params["min_year"] = min_year

    for field_name, selected_value in (
        ("district", _normalize_filter_value(district)),
        ("cause", _normalize_filter_value(cause)),
        ("object_category", _normalize_filter_value(object_category)),
    ):
        if selected_value == "all":
            continue
        column_name = resolved_columns.get(field_name)
        if not column_name:
            return date_expression, conditions, params, False
        conditions.append(f"{_text_expression(column_name)} = :{field_name}")
        params[field_name] = selected_value

    return date_expression, conditions, params, True


def _load_forecasting_records(
    table_name: str,
    resolved_columns: Dict[str, str],
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    min_year: Optional[int] = None,
) -> List[Dict[str, Any]]:
    date_expression, conditions, params, scope_is_valid = _build_scope_conditions(
        resolved_columns,
        min_year=min_year,
        district=district,
        cause=cause,
        object_category=object_category,
    )
    if date_expression is None or not scope_is_valid:
        return []

    select_parts = [f"{date_expression} AS fire_date"]
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
        WHERE {" AND ".join(conditions)}
        ORDER BY fire_date
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()

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


def _load_option_counts(
    table_name: str,
    resolved_columns: Dict[str, str],
    field_name: str,
    min_year: Optional[int] = None,
) -> Counter:
    date_expression, conditions, params, scope_is_valid = _build_scope_conditions(
        resolved_columns,
        min_year=min_year,
    )
    column_name = resolved_columns.get(field_name)
    if date_expression is None or not scope_is_valid or not column_name:
        return Counter()

    value_expression = _text_expression(column_name)
    query = text(
        f"""
        SELECT {value_expression} AS option_value, COUNT(*) AS option_count
        FROM {_quote_identifier(table_name)}
        WHERE {" AND ".join(conditions)} AND {value_expression} IS NOT NULL
        GROUP BY option_value
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()

    counter: Counter = Counter()
    for row in rows:
        option_value = _clean_option_value(row.get("option_value"))
        option_count = int(row.get("option_count") or 0)
        if option_value and option_count > 0:
            counter[option_value] += option_count
    return counter


def _build_options_from_counter(counter: Counter, default_label: str) -> List[Dict[str, str]]:
    options = [{"value": "all", "label": default_label}]
    for value, count in sorted(counter.items(), key=lambda item: (-item[1], item[0].lower()))[:200]:
        options.append({"value": value, "label": f"{value} ({count})"})
    return options


def _build_option_catalog_sql(
    source_tables: Sequence[str],
    history_window: str = "all",
    metadata_items: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, List[Dict[str, str]]]:
    normalized_tables = [str(table_name) for table_name in source_tables if str(table_name or "").strip()]
    cache_key = _build_sql_cache_key("option_catalog", normalized_tables, history_window)
    cached_catalog = _FORECASTING_SQL_CACHE.get(cache_key)
    if cached_catalog is not None:
        return cached_catalog

    local_metadata_items = list(metadata_items) if metadata_items is not None else _collect_forecasting_metadata(normalized_tables)[0]
    min_year = _resolve_history_window_min_year(local_metadata_items, history_window)
    district_counter: Counter = Counter()
    cause_counter: Counter = Counter()
    category_counter: Counter = Counter()

    for metadata in local_metadata_items:
        resolved_columns = metadata.get("resolved_columns") or {}
        table_name = str(metadata.get("table_name") or "")
        if not table_name:
            continue
        district_counter.update(_load_option_counts(table_name, resolved_columns, "district", min_year=min_year))
        cause_counter.update(_load_option_counts(table_name, resolved_columns, "cause", min_year=min_year))
        category_counter.update(_load_option_counts(table_name, resolved_columns, "object_category", min_year=min_year))

    payload = {
        "districts": _build_options_from_counter(district_counter, "\u0412\u0441\u0435 \u0440\u0430\u0439\u043e\u043d\u044b"),
        "causes": _build_options_from_counter(cause_counter, "\u0412\u0441\u0435 \u043f\u0440\u0438\u0447\u0438\u043d\u044b"),
        "object_categories": _build_options_from_counter(category_counter, "\u0412\u0441\u0435 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u0438"),
    }
    return _FORECASTING_SQL_CACHE.set(cache_key, payload)


def _load_daily_history_rows(
    table_name: str,
    resolved_columns: Dict[str, str],
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    min_year: Optional[int] = None,
) -> List[Dict[str, Any]]:
    date_expression, conditions, params, scope_is_valid = _build_scope_conditions(
        resolved_columns,
        min_year=min_year,
        district=district,
        cause=cause,
        object_category=object_category,
    )
    if date_expression is None or not scope_is_valid:
        return []

    select_parts = [
        f"{date_expression} AS fire_date",
        "COUNT(*) AS incident_count",
    ]
    temperature_column = resolved_columns.get("temperature")
    if temperature_column:
        temperature_expression = _numeric_expression_for_column(temperature_column)
        select_parts.append(f"AVG({temperature_expression}) AS avg_temperature")
        select_parts.append(f"COUNT({temperature_expression}) AS temperature_samples")
    else:
        select_parts.append("NULL::double precision AS avg_temperature")
        select_parts.append("0::bigint AS temperature_samples")

    query = text(
        f"""
        SELECT {", ".join(select_parts)}
        FROM {_quote_identifier(table_name)}
        WHERE {" AND ".join(conditions)}
        GROUP BY fire_date
        ORDER BY fire_date
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()

    return [
        {
            "date": row.get("fire_date"),
            "count": int(row.get("incident_count") or 0),
            "avg_temperature": _to_float_or_none(row.get("avg_temperature")),
            "temperature_samples": int(row.get("temperature_samples") or 0),
        }
        for row in rows
        if row.get("fire_date") is not None
    ]


def _build_daily_history_sql(
    source_tables: Sequence[str],
    history_window: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    metadata_items: Optional[Sequence[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    normalized_tables = [str(table_name) for table_name in source_tables if str(table_name or "").strip()]
    cache_key = _build_sql_cache_key(
        "daily_history",
        normalized_tables,
        history_window,
        _normalize_filter_value(district),
        _normalize_filter_value(cause),
        _normalize_filter_value(object_category),
    )
    cached_history = _FORECASTING_SQL_CACHE.get(cache_key)
    if cached_history is not None:
        return cached_history

    local_metadata_items = list(metadata_items) if metadata_items is not None else _collect_forecasting_metadata(normalized_tables)[0]
    min_year = _resolve_history_window_min_year(local_metadata_items, history_window)
    merged_rows: Dict[date, Dict[str, Any]] = {}

    for metadata in local_metadata_items:
        table_name = str(metadata.get("table_name") or "")
        resolved_columns = metadata.get("resolved_columns") or {}
        if not table_name:
            continue
        try:
            table_rows = _load_daily_history_rows(
                table_name,
                resolved_columns,
                district=district,
                cause=cause,
                object_category=object_category,
                min_year=min_year,
            )
        except Exception:
            fallback_records = _load_forecasting_records(
                table_name,
                resolved_columns,
                district=district,
                cause=cause,
                object_category=object_category,
                min_year=min_year,
            )
            table_rows = [
                {
                    "date": item["date"],
                    "count": int(item["count"]),
                    "avg_temperature": item["avg_temperature"],
                    "temperature_samples": 1 if item["avg_temperature"] is not None else 0,
                }
                for item in _build_daily_history(fallback_records)
            ]

        for row in table_rows:
            row_date = row.get("date")
            if row_date is None:
                continue
            bucket = merged_rows.setdefault(
                row_date,
                {
                    "count": 0,
                    "temperature_sum": 0.0,
                    "temperature_samples": 0,
                },
            )
            bucket["count"] += int(row.get("count") or 0)
            sample_count = int(row.get("temperature_samples") or 0)
            avg_temperature = _to_float_or_none(row.get("avg_temperature"))
            if sample_count > 0 and avg_temperature is not None:
                bucket["temperature_sum"] += avg_temperature * sample_count
                bucket["temperature_samples"] += sample_count

    if not merged_rows:
        return _FORECASTING_SQL_CACHE.set(cache_key, [])

    history: List[Dict[str, Any]] = []
    current_date = min(merged_rows)
    max_date = max(merged_rows)
    while current_date <= max_date:
        bucket = merged_rows.get(current_date)
        temperature_samples = int((bucket or {}).get("temperature_samples") or 0)
        temperature_sum = float((bucket or {}).get("temperature_sum") or 0.0)
        history.append(
            {
                "date": current_date,
                "count": int((bucket or {}).get("count") or 0),
                "avg_temperature": round(temperature_sum / temperature_samples, 2) if temperature_samples > 0 else None,
            }
        )
        current_date += timedelta(days=1)

    return _FORECASTING_SQL_CACHE.set(cache_key, history)
def _build_option_catalog(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, str]]]:
    return {
        "districts": _build_options(records, "district", "\u0412\u0441\u0435 \u0440\u0430\u0439\u043e\u043d\u044b"),
        "causes": _build_options(records, "cause", "\u0412\u0441\u0435 \u043f\u0440\u0438\u0447\u0438\u043d\u044b"),
        "object_categories": _build_options(records, "object_category", "\u0412\u0441\u0435 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u0438"),
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
    history_events = [1.0 if value > 0 else 0.0 for value in history_counts]
    overall_average = mean(history_counts) if history_counts else 0.0
    recent_counts = history_counts[-28:] if len(history_counts) >= 28 else history_counts
    recent_events = history_events[-len(recent_counts):] if recent_counts else []
    recent_positive_counts = [value for value in recent_counts if value > 0]
    overall_positive_counts = [value for value in history_counts if value > 0]
    very_recent_counts = history_counts[-14:] if len(history_counts) >= 14 else history_counts
    previous_counts = history_counts[-56:-28] if len(history_counts) >= 56 else history_counts[:-len(recent_counts)] if len(history_counts) > len(recent_counts) else []
    recent_average = mean(recent_counts) if recent_counts else overall_average
    very_recent_average = mean(very_recent_counts) if very_recent_counts else recent_average
    previous_average = mean(previous_counts) if previous_counts else recent_average
    recent_event_rate = mean(recent_events) if recent_events else (mean(history_events) if history_events else 0.0)
    recent_positive_average = mean(recent_positive_counts) if recent_positive_counts else (mean(overall_positive_counts) if overall_positive_counts else max(1.0, overall_average))
    trend_ratio = _clamp(((very_recent_average - previous_average) / previous_average) if previous_average > 0 else 0.0, -0.22, 0.22)
    base_recent_level = 0.65 * very_recent_average + 0.35 * recent_average if recent_counts else overall_average

    weekday_factor: Dict[int, float] = {}
    weekday_event_rate: Dict[int, float] = {}
    weekday_positive_average: Dict[int, float] = {}
    for weekday in range(7):
        weekday_values = [float(item["count"]) for item in daily_history if item["date"].weekday() == weekday]
        weekday_avg = mean(weekday_values) if weekday_values else overall_average
        raw_factor = (weekday_avg / overall_average) if overall_average > 0 else 1.0
        reliability = min(1.0, len(weekday_values) / 12.0)
        weekday_factor[weekday] = 1.0 + (raw_factor - 1.0) * reliability * 0.7
        weekday_event_values = [1.0 if value > 0 else 0.0 for value in weekday_values]
        weekday_positive_values = [value for value in weekday_values if value > 0]
        weekday_event_rate[weekday] = mean(weekday_event_values) if weekday_event_values else recent_event_rate
        weekday_positive_average[weekday] = mean(weekday_positive_values) if weekday_positive_values else recent_positive_average

    month_factor: Dict[int, float] = {}
    month_event_rate: Dict[int, float] = {}
    month_positive_average: Dict[int, float] = {}
    seasonal_temperature_by_month: Dict[int, float] = {}
    overall_temperature_values = [item["avg_temperature"] for item in daily_history if item["avg_temperature"] is not None]
    overall_temperature_average = mean(overall_temperature_values) if overall_temperature_values else None

    for month in range(1, 13):
        month_values = [float(item["count"]) for item in daily_history if item["date"].month == month]
        month_avg = mean(month_values) if month_values else overall_average
        raw_factor = (month_avg / overall_average) if overall_average > 0 else 1.0
        reliability = min(1.0, len(month_values) / 45.0)
        month_factor[month] = 1.0 + (raw_factor - 1.0) * reliability * 0.55
        month_event_values = [1.0 if value > 0 else 0.0 for value in month_values]
        month_positive_values = [value for value in month_values if value > 0]
        month_event_rate[month] = mean(month_event_values) if month_event_values else recent_event_rate
        month_positive_average[month] = mean(month_positive_values) if month_positive_values else recent_positive_average
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

    def _event_probability_for(target_date: date, expected_count: float) -> float:
        numeric_count = max(0.0, float(expected_count))
        if numeric_count <= 0:
            return 0.0

        weekday_base_probability = weekday_event_rate.get(target_date.weekday(), recent_event_rate)
        month_base_probability = month_event_rate.get(target_date.month, recent_event_rate)
        base_probability = _clamp(
            0.55 * weekday_base_probability + 0.20 * month_base_probability + 0.25 * recent_event_rate,
            0.01,
            0.98,
        )

        weekday_positive_level = weekday_positive_average.get(target_date.weekday(), recent_positive_average)
        month_positive_level = month_positive_average.get(target_date.month, recent_positive_average)
        positive_count_scale = max(
            1.0,
            0.55 * weekday_positive_level + 0.20 * month_positive_level + 0.25 * recent_positive_average,
        )
        count_implied_probability = _clamp(numeric_count / positive_count_scale, 0.0, 0.995)
        return _clamp(0.65 * count_implied_probability + 0.35 * base_probability, 0.01, 0.995)

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
        fire_probability = _event_probability_for(target_date, estimate)
        lower_probability = _event_probability_for(target_date, lower_bound)
        upper_probability = _event_probability_for(target_date, upper_bound)
        usual_probability = _event_probability_for(target_date, usual_for_day)

        forecast_rows.append(
            {
                "date": target_date.isoformat(),
                "date_display": target_date.strftime("%d.%m.%Y"),
                "weekday_label": WEEKDAY_LABELS[target_date.weekday()],
                "forecast_value": rounded_estimate,
                "forecast_value_display": _format_number(rounded_estimate),
                "forecast_value_human_display": f"\u043e\u043a\u043e\u043b\u043e {_format_number(rounded_estimate)}",
                "fire_probability": round(fire_probability, 4),
                "fire_probability_display": _format_probability(fire_probability),
                "fire_probability_range_display": (
                    f"{_format_probability(lower_probability)} - "
                    f"{_format_probability(upper_probability)}"
                ),
                "usual_fire_probability": round(usual_probability, 4),
                "usual_fire_probability_display": _format_probability(usual_probability),
                "usual_value": round(usual_for_day, 2),
                "usual_value_display": _format_number(usual_for_day),
                "lower_bound": round(lower_bound, 2),
                "lower_bound_display": _format_number(lower_bound),
                "upper_bound": round(upper_bound, 2),
                "upper_bound_display": _format_number(upper_bound),
                "range_display": _format_count_range(lower_bound, upper_bound),
                "temperature_display": (
                    f"{_format_number(temperature_for_day)} \u00B0C"
                    if temperature_for_day is not None
                    else "\u0421\u0435\u0437\u043e\u043d\u043d\u0430\u044f \u0441\u0440\u0435\u0434\u043d\u044f\u044f"
                ),
                "scenario_label": scenario_label,
                "scenario_tone": scenario_tone,
                "scenario_hint": _relative_delta_text(
                    estimate,
                    usual_for_day,
                    reference_label="\u043a \u043e\u0431\u044b\u0447\u043d\u043e\u043c\u0443 \u0443\u0440\u043e\u0432\u043d\u044e \u0434\u043b\u044f \u0442\u0430\u043a\u043e\u0433\u043e \u0434\u043d\u044f",
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
