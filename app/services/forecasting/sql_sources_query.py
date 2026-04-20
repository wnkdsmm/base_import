from __future__ import annotations

from collections import Counter
from typing import Any, Sequence

from sqlalchemy import text

from config.db import engine

from .selection import _normalize_filter_value
from .utils import (
    _clean_coordinate,
    _clean_option_value,
    _date_expression,
    _numeric_expression_for_column,
    _quote_identifier,
    _text_expression,
    _to_float_or_none,
)


class SourceQuerySqlMixin:
    def _build_scope_conditions(
        self,
        resolved_columns: dict[str, str],
        min_year: int | None = None,
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
    ) -> tuple[str | None, list[str], dict[str, Any], bool]:
        date_column = resolved_columns["date"]
        if not date_column:
            return None, [], {}, True

        date_expression = _date_expression(date_column)
        conditions = [f"{date_expression} IS NOT NULL"]
        params: dict[str, Any] = {}
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
        self,
        table_name: str,
        resolved_columns: dict[str, str],
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
        min_year: int | None = None,
    ) -> list[dict[str, Any]]:
        date_expression, conditions, params, scope_is_valid = self._build_scope_conditions(
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

        records: list[dict[str, Any]] = []
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

    def _load_all_option_counts(
        self,
        table_name: str,
        resolved_columns: dict[str, str],
        min_year: int | None = None,
    ) -> dict[str, Counter]:
        field_names = ("district", "cause", "object_category")
        counters: dict[str, Counter] = {field_name: Counter() for field_name in field_names}
        available_fields = [field_name for field_name in field_names if resolved_columns.get(field_name)]
        if not available_fields:
            return counters

        query_parts: list[str] = []
        params: dict[str, Any] = {}
        if self._aggregations._daily_aggregate_view_exists(table_name):
            conditions, params, scope_is_valid = self._aggregations._build_materialized_scope_conditions(
                resolved_columns,
                min_year=min_year,
            )
            if not scope_is_valid:
                return counters
            view_name = _quote_identifier(self._aggregations._daily_aggregate_view_name(table_name))
            joined_conditions = " AND ".join(conditions)
            for field_name in available_fields:
                query_parts.append(
                    f"""
                    SELECT '{field_name}' AS field_name, {field_name}_value AS option_value, SUM(incident_count) AS option_count
                    FROM {view_name}
                    WHERE {joined_conditions} AND {field_name}_value IS NOT NULL
                    GROUP BY option_value
                    """
                )
        else:
            date_expression, conditions, params, scope_is_valid = self._build_scope_conditions(
                resolved_columns,
                min_year=min_year,
            )
            if date_expression is None or not scope_is_valid:
                return counters
            source_name = _quote_identifier(table_name)
            joined_conditions = " AND ".join(conditions)
            for field_name in available_fields:
                value_expression = _text_expression(resolved_columns[field_name])
                query_parts.append(
                    f"""
                    SELECT '{field_name}' AS field_name, {value_expression} AS option_value, COUNT(*) AS option_count
                    FROM {source_name}
                    WHERE {joined_conditions} AND {value_expression} IS NOT NULL
                    GROUP BY option_value
                    """
                )

        if not query_parts:
            return counters

        query = text("\nUNION ALL\n".join(query_parts))
        with engine.connect() as conn:
            rows = conn.execute(query, params).mappings().all()

        for row in rows:
            field_name = str(row.get("field_name") or "")
            target_counter = counters.get(field_name)
            if target_counter is None:
                continue
            option_value = _clean_option_value(row.get("option_value"))
            option_count = int(row.get("option_count") or 0)
            if option_value and option_count > 0:
                target_counter[option_value] += option_count
        return counters

    def _load_option_counts(
        self,
        table_name: str,
        resolved_columns: dict[str, str],
        field_name: str,
        min_year: int | None = None,
    ) -> Counter:
        if resolved_columns.get(field_name) and self._aggregations._daily_aggregate_view_exists(table_name):
            conditions, params, scope_is_valid = self._aggregations._build_materialized_scope_conditions(
                resolved_columns,
                min_year=min_year,
            )
            if not scope_is_valid:
                return Counter()
            query = text(
                f"""
                SELECT {field_name}_value AS option_value, SUM(incident_count) AS option_count
                FROM {_quote_identifier(self._aggregations._daily_aggregate_view_name(table_name))}
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

        date_expression, conditions, params, scope_is_valid = self._build_scope_conditions(
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

    def _daily_history_source_select_parts(
        self,
        date_expression: str,
        resolved_columns: dict[str, str],
    ) -> list[str]:
        select_parts = [f"{date_expression} AS fire_date", "COUNT(*) AS incident_count"]
        temperature_column = resolved_columns.get("temperature")
        if temperature_column:
            temperature_expression = _numeric_expression_for_column(temperature_column)
            select_parts.append(f"AVG({temperature_expression}) AS avg_temperature")
            select_parts.append(f"COUNT({temperature_expression}) AS temperature_samples")
        else:
            select_parts.append("NULL::double precision AS avg_temperature")
            select_parts.append("0::bigint AS temperature_samples")
        return select_parts

    def _load_source_daily_history_rows(
        self,
        table_name: str,
        resolved_columns: dict[str, str],
        *,
        district: str,
        cause: str,
        object_category: str,
        min_year: int | None,
    ) -> list[dict[str, Any]]:
        date_expression, conditions, params, scope_is_valid = self._build_scope_conditions(
            resolved_columns,
            min_year=min_year,
            district=district,
            cause=cause,
            object_category=object_category,
        )
        if date_expression is None or not scope_is_valid:
            return []

        select_parts = self._daily_history_source_select_parts(date_expression, resolved_columns)

        query = text(
            f"""
            SELECT {", ".join(select_parts)}
            FROM {_quote_identifier(table_name)}
            WHERE {" AND ".join(conditions)}
            GROUP BY fire_date
            ORDER BY fire_date
            """
        )
        return self._aggregations._execute_daily_history_row_query(query, params)

    def _daily_history_union_materialized_part_sql(
        self,
        table_name: str,
        resolved_columns: dict[str, str],
        params: dict[str, Any],
        *,
        district: str,
        cause: str,
        object_category: str,
        min_year: int | None,
    ) -> str | None:
        return self._aggregations._daily_history_union_materialized_part_sql(
            table_name,
            resolved_columns,
            params,
            district=district,
            cause=cause,
            object_category=object_category,
            min_year=min_year,
        )

    def _daily_history_union_source_part_sql(
        self,
        table_name: str,
        resolved_columns: dict[str, str],
        params: dict[str, Any],
        *,
        district: str,
        cause: str,
        object_category: str,
        min_year: int | None,
    ) -> str | None:
        date_expression, conditions, table_params, scope_is_valid = self._build_scope_conditions(
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

    def _load_scope_total_count(
        self,
        table_name: str,
        resolved_columns: dict[str, str],
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
        min_year: int | None = None,
    ) -> int:
        if self._aggregations._daily_aggregate_view_exists(table_name):
            conditions, params, scope_is_valid = self._aggregations._build_materialized_scope_conditions(
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
                FROM {_quote_identifier(self._aggregations._daily_aggregate_view_name(table_name))}
                WHERE {" AND ".join(conditions)}
                """
            )
            with engine.connect() as conn:
                return int(conn.execute(query, params).scalar() or 0)

        date_expression, conditions, params, scope_is_valid = self._build_scope_conditions(
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
