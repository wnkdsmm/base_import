from __future__ import annotations

"""Forecasting SQL aggregation builders with shared SQL-level TTL cache."""

import hashlib
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence

from sqlalchemy import text

from app.cache import CopyingTtlCache
from config.db import engine

from .selection import _canonicalize_source_tables, _normalize_filter_value
from .types import SqlFilters, SqlMaterializedRow, SqlMergedBucket, SqlRow
from .utils import (
    _date_expression,
    _numeric_expression_for_column,
    _quote_identifier,
    _text_expression,
    _to_float_or_none,
)

_FORECASTING_SQL_CACHE = CopyingTtlCache(ttl_seconds=120.0)


class QueryBuilder:
    def __init__(self, hook_resolver: Optional[Callable[[str], Any]] = None) -> None:
        self._hook_resolver = hook_resolver

    @property
    def cache(self) -> CopyingTtlCache:
        return _FORECASTING_SQL_CACHE

    def set_hook_resolver(self, hook_resolver: Optional[Callable[[str], Any]]) -> None:
        self._hook_resolver = hook_resolver

    def _resolve_hook(self, name: str, default: Callable[..., Any]) -> Callable[..., Any]:
        if self._hook_resolver is None:
            return default
        try:
            candidate = self._hook_resolver(name)
        except Exception:
            return default
        return candidate if callable(candidate) else default

    def _build_sql_cache_key(self, prefix: str, source_tables: Sequence[str], *parts: Any) -> tuple[Any, ...]:
        return (prefix, *tuple(source_tables), *parts)

    def _filtered_record_count_cache_key(
        self,
        source_tables: Sequence[str],
        history_window: str,
        district: str,
        cause: str,
        object_category: str,
    ) -> tuple[Any, ...]:
        normalized_tables = _canonicalize_source_tables(source_tables)[0]
        return self._build_sql_cache_key(
            "filtered_record_count",
            normalized_tables,
            history_window,
            _normalize_filter_value(district),
            _normalize_filter_value(cause),
            _normalize_filter_value(object_category),
        )


class AggregationQueryBuilder(QueryBuilder):
    def clear_forecasting_sql_cache(self) -> None:
        self.cache.clear()

    def _daily_history_total_count(self, history: Sequence[SqlRow]) -> int:
        return sum(int(row.get("count") or 0) for row in history)

    def _materialized_view_suffix(self, value: str) -> str:
        raw_value = str(value or "").strip()
        normalized = "".join(character.lower() if character.isalnum() else "_" for character in raw_value)
        normalized = "_".join(part for part in normalized.split("_") if part)
        if not normalized:
            normalized = "table"
        digest = hashlib.sha1(raw_value.encode("utf-8")).hexdigest()[:8]
        return f"{normalized[:32]}_{digest}"

    def _daily_aggregate_view_name(self, table_name: str) -> str:
        return f"mv_forecasting_daily_{self._materialized_view_suffix(table_name)}"

    def _daily_aggregate_view_status_map(self, table_names: Sequence[str]) -> Dict[str, bool]:
        normalized_names = [str(table_name) for table_name in dict.fromkeys(table_names) if str(table_name)]
        if not normalized_names:
            return {}

        status_by_table: Dict[str, bool] = {}
        pending_views: Dict[str, str] = {}
        for table_name in normalized_names:
            cache_key = self._build_sql_cache_key("daily_aggregate_view_exists", [table_name])
            cached_value = self.cache.get(cache_key)
            if cached_value is not None:
                status_by_table[table_name] = bool(cached_value)
            else:
                pending_views[self._daily_aggregate_view_name(table_name)] = table_name

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
            self.cache.set(self._build_sql_cache_key("daily_aggregate_view_exists", [table_name]), exists)
            status_by_table[table_name] = exists

        return status_by_table

    def _daily_aggregate_view_exists(self, table_name: str) -> bool:
        return self._daily_aggregate_view_status_map([table_name]).get(str(table_name), False)

    def _build_materialized_scope_conditions(
        self,
        resolved_columns: Dict[str, str],
        min_year: Optional[int] = None,
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
    ) -> tuple[list[str], SqlFilters, bool]:
        conditions = ["fire_date IS NOT NULL"]
        params: SqlFilters = {}
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

    def _build_daily_aggregate_view_sql(self, table_name: str, resolved_columns: Dict[str, str]) -> str:
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
        self,
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
                view_sql = self._build_daily_aggregate_view_sql(table_name, resolved_columns)
                if not table_name or not view_sql:
                    continue

                view_name = self._daily_aggregate_view_name(table_name)
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
                            f"CREATE INDEX IF NOT EXISTS {_quote_identifier(f'idx_{self._materialized_view_suffix(table_name)}_fire_date')} "
                            f"ON {_quote_identifier(view_name)} (fire_date)"
                        )
                    )
                    conn.execute(
                        text(
                            f"CREATE INDEX IF NOT EXISTS {_quote_identifier(f'idx_{self._materialized_view_suffix(table_name)}_filters')} "
                            f"ON {_quote_identifier(view_name)} (fire_date, district_value, cause_value, object_category_value)"
                        )
                    )
                elif refresh_existing:
                    conn.execute(text(f"REFRESH MATERIALIZED VIEW {_quote_identifier(view_name)}"))
                prepared_views.append(view_name)

        self.clear_forecasting_sql_cache()
        return prepared_views

    def _daily_history_rows_from_query_rows(self, rows: Sequence[Any]) -> List[SqlMaterializedRow]:
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

    def _execute_daily_history_row_query(self, query: Any, params: SqlFilters) -> List[SqlMaterializedRow]:
        with engine.connect() as conn:
            rows = conn.execute(query, params).mappings().all()
        return self._daily_history_rows_from_query_rows(rows)

    def _load_materialized_daily_history_rows(
        self,
        table_name: str,
        resolved_columns: Dict[str, str],
        *,
        district: str,
        cause: str,
        object_category: str,
        min_year: Optional[int],
    ) -> List[SqlMaterializedRow]:
        conditions, params, scope_is_valid = self._build_materialized_scope_conditions(
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
            FROM {_quote_identifier(self._daily_aggregate_view_name(table_name))}
            WHERE {" AND ".join(conditions)}
            GROUP BY fire_date
            ORDER BY fire_date
            """
        )
        return self._execute_daily_history_row_query(query, params)

    def _daily_history_union_materialized_part_sql(
        self,
        table_name: str,
        resolved_columns: Dict[str, str],
        params: SqlFilters,
        *,
        district: str,
        cause: str,
        object_category: str,
        min_year: Optional[int],
    ) -> Optional[str]:
        conditions, table_params, scope_is_valid = self._build_materialized_scope_conditions(
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
            FROM {_quote_identifier(self._daily_aggregate_view_name(table_name))}
            WHERE {" AND ".join(conditions)}
            GROUP BY fire_date
        """

    def _build_daily_history_union_query(self, query_parts: Sequence[str]) -> Any:
        union_sql = "\nUNION ALL\n".join(query_parts)
        return text(
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

    def _merge_daily_history_rows(
        self,
        merged_rows: Dict[date, SqlMergedBucket],
        table_rows: Sequence[SqlMaterializedRow],
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

    def _dense_daily_history_from_merged_rows(
        self,
        merged_rows: Dict[date, SqlMergedBucket],
    ) -> List[SqlRow]:
        if not merged_rows:
            return []

        history: List[SqlRow] = []
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

