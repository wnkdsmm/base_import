from __future__ import annotations

from dataclasses import dataclass
import hashlib
from collections import Counter
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text

from app.perf import current_perf_trace
from app.runtime_cache import CopyingTtlCache
from config.db import engine

from .selection import _canonicalize_source_tables, _normalize_filter_value
from .shaping import _build_daily_history
from .utils import (
    _clean_coordinate,
    _clean_option_value,
    _date_expression,
    _numeric_expression_for_column,
    _quote_identifier,
    _text_expression,
    _to_float_or_none,
)

_FORECASTING_SQL_CACHE = CopyingTtlCache(ttl_seconds=120.0)


def clear_forecasting_sql_cache() -> None:
    _FORECASTING_SQL_CACHE.clear()


def _build_sql_cache_key(prefix: str, source_tables: Sequence[str], *parts: Any) -> tuple[Any, ...]:
    return (prefix, *tuple(source_tables), *parts)


def _filtered_record_count_cache_key(
    source_tables: Sequence[str],
    history_window: str,
    district: str,
    cause: str,
    object_category: str,
) -> tuple[Any, ...]:
    normalized_tables = _canonicalize_source_tables(source_tables)[0]
    return _build_sql_cache_key(
        "filtered_record_count",
        normalized_tables,
        history_window,
        _normalize_filter_value(district),
        _normalize_filter_value(cause),
        _normalize_filter_value(object_category),
    )


def _daily_history_total_count(history: Sequence[Dict[str, Any]]) -> int:
    return sum(int(row.get("count") or 0) for row in history)


def _materialized_view_suffix(value: str) -> str:
    raw_value = str(value or "").strip()
    normalized = "".join(character.lower() if character.isalnum() else "_" for character in raw_value)
    normalized = "_".join(part for part in normalized.split("_") if part)
    if not normalized:
        normalized = "table"
    digest = hashlib.sha1(raw_value.encode("utf-8")).hexdigest()[:8]
    return f"{normalized[:32]}_{digest}"


def _daily_aggregate_view_name(table_name: str) -> str:
    return f"mv_forecasting_daily_{_materialized_view_suffix(table_name)}"


def _daily_aggregate_view_status_map(table_names: Sequence[str]) -> Dict[str, bool]:
    normalized_names = [str(table_name) for table_name in dict.fromkeys(table_names) if str(table_name)]
    if not normalized_names:
        return {}

    status_by_table: Dict[str, bool] = {}
    pending_views: Dict[str, str] = {}
    for table_name in normalized_names:
        cache_key = _build_sql_cache_key("daily_aggregate_view_exists", [table_name])
        cached_value = _FORECASTING_SQL_CACHE.get(cache_key)
        if cached_value is not None:
            status_by_table[table_name] = bool(cached_value)
        else:
            pending_views[_daily_aggregate_view_name(table_name)] = table_name

    if not pending_views:
        return status_by_table

    existing_views: set[str] = set()
    if engine.dialect.name == "postgresql":
        params = {f"view_name_{index}": view_name for index, view_name in enumerate(pending_views)}
        placeholders = ", ".join(f":view_name_{index}" for index in range(len(pending_views)))
        query = text(
            f"""
            SELECT matviewname
            FROM pg_matviews
            WHERE schemaname = current_schema() AND matviewname IN ({placeholders})
            """
        )
        with engine.connect() as conn:
            existing_views = {str(row["matviewname"]) for row in conn.execute(query, params).mappings().all()}

    for view_name, table_name in pending_views.items():
        exists = view_name in existing_views
        _FORECASTING_SQL_CACHE.set(_build_sql_cache_key("daily_aggregate_view_exists", [table_name]), exists)
        status_by_table[table_name] = exists

    return status_by_table


def _daily_aggregate_view_exists(table_name: str) -> bool:
    return _daily_aggregate_view_status_map([table_name]).get(str(table_name), False)


def _build_materialized_scope_conditions(
    resolved_columns: Dict[str, str],
    min_year: Optional[int] = None,
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
) -> tuple[list[str], Dict[str, Any], bool]:
    conditions = ["fire_date IS NOT NULL"]
    params: Dict[str, Any] = {}
    if min_year is not None:
        conditions.append("EXTRACT(YEAR FROM fire_date) >= :min_year")
        params["min_year"] = min_year

    for field_name, selected_value, column_name in (
        ("district", _normalize_filter_value(district), "district_value"),
        ("cause", _normalize_filter_value(cause), "cause_value"),
        ("object_category", _normalize_filter_value(object_category), "object_category_value"),
    ):
        if selected_value == "all":
            continue
        if not resolved_columns.get(field_name):
            return conditions, params, False
        conditions.append(f"{column_name} = :{field_name}")
        params[field_name] = selected_value

    return conditions, params, True


def _build_daily_aggregate_view_sql(table_name: str, resolved_columns: Dict[str, str]) -> str:
    date_column = resolved_columns.get("date")
    if not date_column:
        return ""

    date_expression = _date_expression(date_column)
    district_expression = _text_expression(resolved_columns["district"]) if resolved_columns.get("district") else "NULL::text"
    cause_expression = _text_expression(resolved_columns["cause"]) if resolved_columns.get("cause") else "NULL::text"
    object_expression = _text_expression(resolved_columns["object_category"]) if resolved_columns.get("object_category") else "NULL::text"
    temperature_expression = _numeric_expression_for_column(resolved_columns["temperature"]) if resolved_columns.get("temperature") else None
    avg_temperature_sql = f"AVG({temperature_expression})" if temperature_expression else "NULL::double precision"
    temperature_samples_sql = f"COUNT({temperature_expression})" if temperature_expression else "0::bigint"

    return f"""
        SELECT
            {date_expression} AS fire_date,
            {district_expression} AS district_value,
            {cause_expression} AS cause_value,
            {object_expression} AS object_category_value,
            COUNT(*) AS incident_count,
            {avg_temperature_sql} AS avg_temperature,
            {temperature_samples_sql} AS temperature_samples
        FROM {_quote_identifier(table_name)}
        WHERE {date_expression} IS NOT NULL
        GROUP BY fire_date, district_value, cause_value, object_category_value
    """


def prepare_forecasting_materialized_views(
    source_tables: Optional[Sequence[str]] = None,
    *,
    refresh_existing: bool = True,
) -> List[str]:
    if engine.dialect.name != "postgresql":
        return []

    from .selection import _build_forecasting_table_options
    from .sources import _collect_forecasting_metadata

    table_options = _build_forecasting_table_options()
    available_tables = [option["value"] for option in table_options if option.get("value") and option["value"] != "all"]
    requested_tables = [table_name for table_name in (source_tables or available_tables) if table_name in available_tables]
    target_tables = _canonicalize_source_tables(requested_tables)[0]
    metadata_items, _notes = _collect_forecasting_metadata(target_tables)
    prepared_views: List[str] = []

    with engine.begin() as conn:
        for metadata in metadata_items:
            table_name = str(metadata.get("table_name") or "")
            resolved_columns = metadata.get("resolved_columns") or {}
            view_sql = _build_daily_aggregate_view_sql(table_name, resolved_columns)
            if not table_name or not view_sql:
                continue

            view_name = _daily_aggregate_view_name(table_name)
            exists_query = text(
                """
                SELECT 1
                FROM pg_matviews
                WHERE schemaname = current_schema() AND matviewname = :view_name
                """
            )
            exists = conn.execute(exists_query, {"view_name": view_name}).scalar() is not None
            if not exists:
                conn.execute(text(f"CREATE MATERIALIZED VIEW {_quote_identifier(view_name)} AS {view_sql}"))
                conn.execute(
                    text(
                        f"CREATE INDEX IF NOT EXISTS {_quote_identifier(f'idx_{_materialized_view_suffix(table_name)}_fire_date')} "
                        f"ON {_quote_identifier(view_name)} (fire_date)"
                    )
                )
                conn.execute(
                    text(
                        f"CREATE INDEX IF NOT EXISTS {_quote_identifier(f'idx_{_materialized_view_suffix(table_name)}_filters')} "
                        f"ON {_quote_identifier(view_name)} (fire_date, district_value, cause_value, object_category_value)"
                    )
                )
            elif refresh_existing:
                conn.execute(text(f"REFRESH MATERIALIZED VIEW {_quote_identifier(view_name)}"))
            prepared_views.append(view_name)

    clear_forecasting_sql_cache()
    return prepared_views


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
    if resolved_columns.get(field_name) and _daily_aggregate_view_exists(table_name):
        conditions, params, scope_is_valid = _build_materialized_scope_conditions(resolved_columns, min_year=min_year)
        if not scope_is_valid:
            return Counter()
        query = text(
            f"""
            SELECT {field_name}_value AS option_value, SUM(incident_count) AS option_count
            FROM {_quote_identifier(_daily_aggregate_view_name(table_name))}
            WHERE {" AND ".join(conditions)} AND {field_name}_value IS NOT NULL
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
    from .sources import _collect_forecasting_metadata, _resolve_history_window_min_year

    normalized_tables = _canonicalize_source_tables(source_tables)[0]
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
        "districts": _build_options_from_counter(district_counter, "Все районы"),
        "causes": _build_options_from_counter(cause_counter, "Все причины"),
        "object_categories": _build_options_from_counter(category_counter, "Все категории"),
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
    if _daily_aggregate_view_exists(table_name):
        conditions, params, scope_is_valid = _build_materialized_scope_conditions(
            resolved_columns,
            min_year=min_year,
            district=district,
            cause=cause,
            object_category=object_category,
        )
        if not scope_is_valid:
            return []

        query = text(
            f"""
            SELECT
                fire_date,
                SUM(incident_count) AS incident_count,
                CASE
                    WHEN SUM(temperature_samples) > 0
                    THEN SUM(COALESCE(avg_temperature, 0.0) * temperature_samples) / SUM(temperature_samples)
                    ELSE NULL
                END AS avg_temperature,
                SUM(temperature_samples) AS temperature_samples
            FROM {_quote_identifier(_daily_aggregate_view_name(table_name))}
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

    date_expression, conditions, params, scope_is_valid = _build_scope_conditions(
        resolved_columns,
        min_year=min_year,
        district=district,
        cause=cause,
        object_category=object_category,
    )
    if date_expression is None or not scope_is_valid:
        return []

    select_parts = [f"{date_expression} AS fire_date", "COUNT(*) AS incident_count"]
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


def _daily_history_union_part_sql(
    table_name: str,
    resolved_columns: Dict[str, str],
    params: Dict[str, Any],
    *,
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    min_year: Optional[int] = None,
    has_aggregate_view: Optional[bool] = None,
) -> Optional[str]:
    if not table_name:
        return None

    use_aggregate_view = _daily_aggregate_view_exists(table_name) if has_aggregate_view is None else has_aggregate_view
    if use_aggregate_view:
        conditions, table_params, scope_is_valid = _build_materialized_scope_conditions(
            resolved_columns,
            min_year=min_year,
            district=district,
            cause=cause,
            object_category=object_category,
        )
        if not scope_is_valid:
            return None
        params.update(table_params)
        return f"""
            SELECT
                fire_date,
                SUM(incident_count) AS incident_count,
                SUM(COALESCE(avg_temperature, 0.0) * temperature_samples) AS temperature_sum,
                SUM(temperature_samples) AS temperature_samples
            FROM {_quote_identifier(_daily_aggregate_view_name(table_name))}
            WHERE {" AND ".join(conditions)}
            GROUP BY fire_date
        """

    date_expression, conditions, table_params, scope_is_valid = _build_scope_conditions(
        resolved_columns,
        min_year=min_year,
        district=district,
        cause=cause,
        object_category=object_category,
    )
    if date_expression is None or not scope_is_valid:
        return None

    params.update(table_params)
    temperature_column = resolved_columns.get("temperature")
    if temperature_column:
        temperature_expression = _numeric_expression_for_column(temperature_column)
        temperature_sum_sql = f"COALESCE(SUM({temperature_expression}), 0.0)"
        temperature_samples_sql = f"COUNT({temperature_expression})"
    else:
        temperature_sum_sql = "0.0::double precision"
        temperature_samples_sql = "0::bigint"

    return f"""
        SELECT
            {date_expression} AS fire_date,
            COUNT(*) AS incident_count,
            {temperature_sum_sql} AS temperature_sum,
            {temperature_samples_sql} AS temperature_samples
        FROM {_quote_identifier(table_name)}
        WHERE {" AND ".join(conditions)}
        GROUP BY fire_date
    """


def _load_daily_history_rows_union(
    metadata_items: Sequence[Dict[str, Any]],
    *,
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    min_year: Optional[int] = None,
) -> Optional[List[Dict[str, Any]]]:
    params: Dict[str, Any] = {}
    query_parts: List[str] = []
    view_status = _daily_aggregate_view_status_map(
        [str(metadata.get("table_name") or "") for metadata in metadata_items]
    )
    for metadata in metadata_items:
        table_name = str(metadata.get("table_name") or "")
        resolved_columns = metadata.get("resolved_columns") or {}
        query_part = _daily_history_union_part_sql(
            table_name,
            resolved_columns,
            params,
            district=district,
            cause=cause,
            object_category=object_category,
            min_year=min_year,
            has_aggregate_view=view_status.get(table_name, False),
        )
        if query_part:
            query_parts.append(query_part)

    if not query_parts:
        return []

    union_sql = "\nUNION ALL\n".join(query_parts)
    query = text(
        f"""
        SELECT
            fire_date,
            SUM(incident_count) AS incident_count,
            CASE
                WHEN SUM(temperature_samples) > 0
                THEN SUM(temperature_sum) / SUM(temperature_samples)
                ELSE NULL
            END AS avg_temperature,
            SUM(temperature_samples) AS temperature_samples
        FROM (
            {union_sql}
        ) AS daily_history_union
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


def _merge_daily_history_rows(
    merged_rows: Dict[date, Dict[str, Any]],
    table_rows: Sequence[Dict[str, Any]],
) -> None:
    for row in table_rows:
        row_date = row.get("date")
        if row_date is None:
            continue
        bucket = merged_rows.setdefault(
            row_date,
            {"count": 0, "temperature_sum": 0.0, "temperature_samples": 0},
        )
        bucket["count"] += int(row.get("count") or 0)
        sample_count = int(row.get("temperature_samples") or 0)
        avg_temperature = _to_float_or_none(row.get("avg_temperature"))
        if sample_count > 0 and avg_temperature is not None:
            bucket["temperature_sum"] += avg_temperature * sample_count
            bucket["temperature_samples"] += sample_count


def _load_scope_total_count(
    table_name: str,
    resolved_columns: Dict[str, str],
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    min_year: Optional[int] = None,
) -> int:
    if _daily_aggregate_view_exists(table_name):
        conditions, params, scope_is_valid = _build_materialized_scope_conditions(
            resolved_columns,
            min_year=min_year,
            district=district,
            cause=cause,
            object_category=object_category,
        )
        if not scope_is_valid:
            return 0
        query = text(
            f"""
            SELECT COALESCE(SUM(incident_count), 0) AS total_count
            FROM {_quote_identifier(_daily_aggregate_view_name(table_name))}
            WHERE {" AND ".join(conditions)}
            """
        )
        with engine.connect() as conn:
            return int(conn.execute(query, params).scalar() or 0)

    date_expression, conditions, params, scope_is_valid = _build_scope_conditions(
        resolved_columns,
        min_year=min_year,
        district=district,
        cause=cause,
        object_category=object_category,
    )
    if date_expression is None or not scope_is_valid:
        return 0

    query = text(
        f"""
        SELECT COUNT(*) AS total_count
        FROM {_quote_identifier(table_name)}
        WHERE {" AND ".join(conditions)}
        """
    )
    with engine.connect() as conn:
        return int(conn.execute(query, params).scalar() or 0)


@dataclass
class _DailyHistoryLoadTrace:
    used_union_fast_path: bool = False
    union_fast_path_attempted: bool = False
    union_fast_path_fallback: bool = False
    union_fast_path_error_type: str = ""
    union_fast_path_rows: int = 0


def _daily_history_cache_key(
    normalized_tables: Sequence[str],
    history_window: str,
    district: str,
    cause: str,
    object_category: str,
) -> tuple[Any, ...]:
    return _build_sql_cache_key(
        "daily_history",
        normalized_tables,
        history_window,
        _normalize_filter_value(district),
        _normalize_filter_value(cause),
        _normalize_filter_value(object_category),
    )


def _daily_history_cache_keys(
    normalized_tables: Sequence[str],
    history_window: str,
    district: str,
    cause: str,
    object_category: str,
) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
    return (
        _daily_history_cache_key(
            normalized_tables,
            history_window,
            district,
            cause,
            object_category,
        ),
        _filtered_record_count_cache_key(
            normalized_tables,
            history_window,
            district,
            cause,
            object_category,
        ),
    )


def _record_daily_history_perf(
    perf: Any,
    *,
    cache_hit: bool,
    trace: _DailyHistoryLoadTrace,
    table_count: int,
    row_count: int,
    total_count: int,
) -> None:
    if perf is None:
        return
    perf.update(
        forecasting_daily_history_cache_hit=cache_hit,
        forecasting_daily_history_union_fast_path=trace.used_union_fast_path,
        forecasting_daily_history_union_attempted=trace.union_fast_path_attempted,
        forecasting_daily_history_union_fallback=trace.union_fast_path_fallback,
        forecasting_daily_history_union_error_type=trace.union_fast_path_error_type,
        forecasting_daily_history_union_rows=trace.union_fast_path_rows,
        forecasting_daily_history_tables=table_count,
        forecasting_daily_history_rows=row_count,
        forecasting_daily_history_total_count=total_count,
        forecasting_daily_history_count_cache_populated=True,
    )


def _return_cached_daily_history(
    cached_history: List[Dict[str, Any]],
    count_cache_key: tuple[Any, ...],
    perf: Any,
) -> List[Dict[str, Any]]:
    cached_total_count = _daily_history_total_count(cached_history)
    _FORECASTING_SQL_CACHE.set(count_cache_key, cached_total_count)
    if perf is not None:
        perf.update(
            forecasting_daily_history_cache_hit=True,
            forecasting_daily_history_rows=len(cached_history),
            forecasting_daily_history_total_count=cached_total_count,
            forecasting_daily_history_count_cache_populated=True,
        )
    return cached_history


def _lookup_daily_history_cache(
    cache_key: tuple[Any, ...],
    count_cache_key: tuple[Any, ...],
    perf: Any,
) -> Optional[List[Dict[str, Any]]]:
    cached_history = _FORECASTING_SQL_CACHE.get(cache_key)
    if cached_history is None:
        return None
    return _return_cached_daily_history(cached_history, count_cache_key, perf)


def _load_daily_history_metadata_and_min_year(
    normalized_tables: Sequence[str],
    history_window: str,
    metadata_items: Optional[Sequence[Dict[str, Any]]],
) -> tuple[List[Dict[str, Any]], Optional[int]]:
    from .sources import _collect_forecasting_metadata, _resolve_history_window_min_year

    local_metadata_items = list(metadata_items) if metadata_items is not None else _collect_forecasting_metadata(normalized_tables)[0]
    return local_metadata_items, _resolve_history_window_min_year(local_metadata_items, history_window)


def _load_daily_history_union_path(
    metadata_items: Sequence[Dict[str, Any]],
    trace: _DailyHistoryLoadTrace,
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> Optional[List[Dict[str, Any]]]:
    trace.union_fast_path_attempted = len(metadata_items) > 1
    if not trace.union_fast_path_attempted:
        return None

    try:
        union_rows = _load_daily_history_rows_union(
            metadata_items,
            district=district,
            cause=cause,
            object_category=object_category,
            min_year=min_year,
        )
    except Exception as exc:
        trace.union_fast_path_fallback = True
        trace.union_fast_path_error_type = exc.__class__.__name__
        return None

    if union_rows is None:
        return None

    trace.used_union_fast_path = True
    trace.union_fast_path_rows = len(union_rows)
    return union_rows


def _try_merge_daily_history_union(
    merged_rows: Dict[date, Dict[str, Any]],
    metadata_items: Sequence[Dict[str, Any]],
    trace: _DailyHistoryLoadTrace,
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> bool:
    union_rows = _load_daily_history_union_path(
        metadata_items,
        trace,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )
    if union_rows is None:
        return False

    _merge_daily_history_rows(merged_rows, union_rows)
    return True


def _load_daily_history_from_union_path(
    metadata_items: Sequence[Dict[str, Any]],
    trace: _DailyHistoryLoadTrace,
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> Optional[Dict[date, Dict[str, Any]]]:
    merged_rows: Dict[date, Dict[str, Any]] = {}
    if not _try_merge_daily_history_union(
        merged_rows,
        metadata_items,
        trace,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    ):
        return None
    return merged_rows


def _load_table_daily_history_rows_with_record_fallback(
    table_name: str,
    resolved_columns: Dict[str, str],
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> List[Dict[str, Any]]:
    try:
        return _load_daily_history_rows(
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
        return [
            {
                "date": item["date"],
                "count": int(item["count"]),
                "avg_temperature": item["avg_temperature"],
                "temperature_samples": 1 if item["avg_temperature"] is not None else 0,
            }
            for item in _build_daily_history(fallback_records)
        ]


def _merge_daily_history_per_table_fallback(
    merged_rows: Dict[date, Dict[str, Any]],
    metadata_items: Sequence[Dict[str, Any]],
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> None:
    for metadata in metadata_items:
        table_name = str(metadata.get("table_name") or "")
        resolved_columns = metadata.get("resolved_columns") or {}
        if not table_name:
            continue
        table_rows = _load_table_daily_history_rows_with_record_fallback(
            table_name,
            resolved_columns,
            district=district,
            cause=cause,
            object_category=object_category,
            min_year=min_year,
        )
        _merge_daily_history_rows(merged_rows, table_rows)


def _load_daily_history_per_table_fallback(
    metadata_items: Sequence[Dict[str, Any]],
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> Dict[date, Dict[str, Any]]:
    merged_rows: Dict[date, Dict[str, Any]] = {}
    _merge_daily_history_per_table_fallback(
        merged_rows,
        metadata_items,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )
    return merged_rows


def _merge_daily_history_fast_or_fallback(
    metadata_items: Sequence[Dict[str, Any]],
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> tuple[Dict[date, Dict[str, Any]], _DailyHistoryLoadTrace]:
    trace = _DailyHistoryLoadTrace()
    merged_rows = _load_daily_history_from_union_path(
        metadata_items,
        trace,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )
    if merged_rows is not None:
        return merged_rows, trace

    return (
        _load_daily_history_per_table_fallback(
            metadata_items,
            district=district,
            cause=cause,
            object_category=object_category,
            min_year=min_year,
        ),
        trace,
    )


def _record_daily_history_result_trace(
    count_cache_key: tuple[Any, ...],
    history: List[Dict[str, Any]],
    *,
    perf: Any,
    trace: _DailyHistoryLoadTrace,
    table_count: int,
) -> int:
    total_count = _daily_history_total_count(history)
    _FORECASTING_SQL_CACHE.set(count_cache_key, total_count)
    _record_daily_history_perf(
        perf,
        cache_hit=False,
        trace=trace,
        table_count=table_count,
        row_count=len(history),
        total_count=total_count,
    )
    return total_count


def _load_dense_daily_history_with_trace(
    metadata_items: Sequence[Dict[str, Any]],
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> tuple[List[Dict[str, Any]], _DailyHistoryLoadTrace]:
    merged_rows, trace = _merge_daily_history_fast_or_fallback(
        metadata_items,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )
    return _dense_daily_history_from_merged_rows(merged_rows), trace


def _dense_daily_history_from_merged_rows(
    merged_rows: Dict[date, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not merged_rows:
        return []

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
    return history


def _cache_daily_history_result(
    cache_key: tuple[Any, ...],
    count_cache_key: tuple[Any, ...],
    history: List[Dict[str, Any]],
    *,
    perf: Any,
    trace: _DailyHistoryLoadTrace,
    table_count: int,
) -> List[Dict[str, Any]]:
    _record_daily_history_result_trace(
        count_cache_key,
        history,
        perf=perf,
        trace=trace,
        table_count=table_count,
    )
    return _FORECASTING_SQL_CACHE.set(cache_key, history)


def _build_daily_history_sql(
    source_tables: Sequence[str],
    history_window: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    metadata_items: Optional[Sequence[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    perf = current_perf_trace()
    normalized_tables = _canonicalize_source_tables(source_tables)[0]
    cache_key, count_cache_key = _daily_history_cache_keys(
        normalized_tables,
        history_window,
        district,
        cause,
        object_category,
    )
    cached_history = _lookup_daily_history_cache(cache_key, count_cache_key, perf)
    if cached_history is not None:
        return cached_history

    local_metadata_items, min_year = _load_daily_history_metadata_and_min_year(
        normalized_tables,
        history_window,
        metadata_items,
    )
    history, trace = _load_dense_daily_history_with_trace(
        local_metadata_items,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )

    return _cache_daily_history_result(
        cache_key,
        count_cache_key,
        history,
        perf=perf,
        trace=trace,
        table_count=len(local_metadata_items),
    )


def _count_forecasting_records_sql(
    source_tables: Sequence[str],
    history_window: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    metadata_items: Optional[Sequence[Dict[str, Any]]] = None,
) -> int:
    from .sources import _collect_forecasting_metadata, _resolve_history_window_min_year

    perf = current_perf_trace()
    normalized_tables = _canonicalize_source_tables(source_tables)[0]
    cache_key = _filtered_record_count_cache_key(
        normalized_tables,
        history_window,
        district,
        cause,
        object_category,
    )
    cached_count = _FORECASTING_SQL_CACHE.get(cache_key)
    if cached_count is not None:
        if perf is not None:
            perf.update(
                forecasting_filtered_record_count_cache_hit=True,
                forecasting_filtered_record_count=int(cached_count),
            )
        return int(cached_count)

    local_metadata_items = list(metadata_items) if metadata_items is not None else _collect_forecasting_metadata(normalized_tables)[0]
    min_year = _resolve_history_window_min_year(local_metadata_items, history_window)
    total_count = 0

    for metadata in local_metadata_items:
        table_name = str(metadata.get("table_name") or "")
        resolved_columns = metadata.get("resolved_columns") or {}
        if not table_name:
            continue
        total_count += _load_scope_total_count(
            table_name,
            resolved_columns,
            district=district,
            cause=cause,
            object_category=object_category,
            min_year=min_year,
        )

    cached_total = int(_FORECASTING_SQL_CACHE.set(cache_key, total_count))
    if perf is not None:
        perf.update(
            forecasting_filtered_record_count_cache_hit=False,
            forecasting_filtered_record_count=cached_total,
        )
    return cached_total
