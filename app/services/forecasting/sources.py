from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text

from app.db_metadata import get_table_columns_cached
from app.domain.predictive_settings import MIN_TEMPERATURE_COVERAGE, MIN_TEMPERATURE_NON_NULL_DAYS
from config.db import engine

from .constants import (
    CAUSE_COLUMN_CANDIDATES,
    DATE_COLUMN,
    DISTRICT_COLUMN_CANDIDATES,
    LATITUDE_COLUMN_CANDIDATES,
    LONGITUDE_COLUMN_CANDIDATES,
    OBJECT_CATEGORY_COLUMN,
    TEMPERATURE_COLUMN_CANDIDATES,
)
from .selection import _canonicalize_source_tables, _history_window_year_span
from .sql import _FORECASTING_SQL_CACHE, _build_sql_cache_key, _load_daily_history_rows, _load_forecasting_records
from .utils import _date_expression, _quote_identifier, _resolve_column_name, _to_float_or_none


def _collect_forecasting_metadata(source_tables: Sequence[str]) -> tuple[List[Dict[str, Any]], List[str]]:
    metadata_items: List[Dict[str, Any]] = []
    normalized_tables, notes = _canonicalize_source_tables(source_tables)
    for source_table in normalized_tables:
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


def _build_temperature_quality(non_null_days: int, total_days: int) -> Dict[str, Any]:
    normalized_non_null_days = max(0, int(non_null_days))
    normalized_total_days = max(0, int(total_days))
    coverage = (float(normalized_non_null_days) / float(normalized_total_days)) if normalized_total_days > 0 else 0.0
    usable = (
        normalized_total_days > 0
        and normalized_non_null_days >= MIN_TEMPERATURE_NON_NULL_DAYS
        and coverage >= MIN_TEMPERATURE_COVERAGE
    )
    if normalized_non_null_days <= 0:
        quality_key = "missing"
        quality_label = "Нет измерений"
    elif usable:
        quality_key = "good"
        quality_label = "Достаточное покрытие"
    else:
        quality_key = "sparse"
        quality_label = "Низкое покрытие"
    return {
        "non_null_days": normalized_non_null_days,
        "total_days": normalized_total_days,
        "coverage": coverage,
        "usable": usable,
        "quality_key": quality_key,
        "quality_label": quality_label,
    }


def _temperature_quality_from_daily_history(daily_history: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    normalized_history = [item for item in daily_history if item]
    total_days = len(normalized_history)
    non_null_days = sum(
        1
        for item in normalized_history
        if _to_float_or_none(item.get("avg_temperature", item.get("temperature"))) is not None
    )
    return _build_temperature_quality(non_null_days, total_days)


def _temperature_quality_from_daily_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    normalized_rows = [item for item in rows if item and item.get("date") is not None]
    if not normalized_rows:
        return _build_temperature_quality(0, 0)

    min_date = min(item["date"] for item in normalized_rows)
    max_date = max(item["date"] for item in normalized_rows)
    total_days = (max_date - min_date).days + 1
    non_null_days = sum(
        1
        for item in normalized_rows
        if _to_float_or_none(item.get("avg_temperature", item.get("temperature"))) is not None
    )
    return _build_temperature_quality(non_null_days, total_days)


def _load_temperature_quality(table_name: str, resolved_columns: Dict[str, str]) -> Dict[str, Any]:
    date_column = resolved_columns.get("date")
    temperature_column = resolved_columns.get("temperature")
    if not date_column or not temperature_column:
        return _build_temperature_quality(0, 0)

    cache_key = _build_sql_cache_key("temperature_quality", [table_name], date_column, temperature_column)
    cached_quality = _FORECASTING_SQL_CACHE.get(cache_key)
    if isinstance(cached_quality, dict):
        return dict(cached_quality)

    try:
        quality = _temperature_quality_from_daily_rows(_load_daily_history_rows(table_name, resolved_columns))
    except Exception:
        quality = _build_temperature_quality(0, 0)
    _FORECASTING_SQL_CACHE.set(cache_key, quality)
    return quality


def _load_table_metadata(table_name: str) -> Dict[str, Any]:
    try:
        columns = get_table_columns_cached(table_name)
    except ValueError as exc:
        raise ValueError(f"Таблица '{table_name}' не найдена в базе данных.") from exc
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
        "column_quality": {
            "temperature": _load_temperature_quality(table_name, resolved_columns),
        },
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
