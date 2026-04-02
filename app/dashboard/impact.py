from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.statistics_constants import DATE_COLUMN, MONTH_LABELS
from config.db import engine

from .charts import (
    _build_area_bucket_plotly,
    _build_cause_plotly,
    _build_combined_impact_timeline_plotly,
    _build_monthly_profile_plotly,
    _build_sql_widget_bar_plotly,
    _build_sql_widget_season_plotly,
    _finalize_chart,
)
from .data_access import (
    SEASON_ORDER,
    _area_expression,
    _build_impact_timeline_query,
    _build_year_filter_clause,
    _month_expression,
    _resolve_cause_column,
    _resolve_district_column,
    _year_expression,
)
from .utils import _format_chart_date, _format_number, _quote_identifier


def _collect_cause_counts(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, int]:
    grouped: Dict[str, int] = defaultdict(int)
    with engine.connect() as conn:
        for table in selected_tables:
            cause_column = _resolve_cause_column(table)
            if not cause_column:
                continue
            where_clause = _build_year_filter_clause(table, selected_year)
            if where_clause is None:
                continue
            query = text(
                f"""
                SELECT
                    COALESCE(NULLIF(TRIM(CAST({_quote_identifier(cause_column)} AS TEXT)), ''), 'Не указано') AS label,
                    COUNT(*) AS fire_count
                FROM {_quote_identifier(table['name'])}
                WHERE {where_clause}
                GROUP BY label
                ORDER BY fire_count DESC
                """
            )
            params = {"selected_year": selected_year} if selected_year is not None and DATE_COLUMN in table["column_set"] else {}
            for row in conn.execute(query, params).mappings().all():
                grouped[row["label"]] += int(row["fire_count"] or 0)

    return dict(grouped)


def _build_cause_chart(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    *,
    cause_counts: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    grouped = cause_counts or _collect_cause_counts(selected_tables, selected_year)
    items = [
        {"label": label, "value": value, "value_display": _format_number(value, integer=True)}
        for label, value in sorted(grouped.items(), key=lambda item: item[1], reverse=True)[:12]
    ]
    title = "Причины возгораний"
    empty_message = "Нет данных по причинам возгорания."
    return _finalize_chart(title, items, empty_message, plotly=_build_cause_plotly(title, items, empty_message))


def _build_combined_impact_timeline_chart(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
    grouped: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "date_value": "",
            "label": "",
            "deaths": 0.0,
            "injuries": 0.0,
            "evacuated": 0.0,
            "evacuated_children": 0.0,
            "rescued_children": 0.0,
        }
    )

    with engine.connect() as conn:
        for table in selected_tables:
            query = _build_impact_timeline_query(table, selected_year)
            if query is None:
                continue
            params = {"selected_year": selected_year} if selected_year is not None and DATE_COLUMN in table["column_set"] else {}
            for row in conn.execute(text(query), params).mappings().all():
                date_value = row["date_value"]
                if date_value is None:
                    continue
                date_key = date_value.isoformat() if hasattr(date_value, "isoformat") else str(date_value)
                bucket = grouped[date_key]
                bucket["date_value"] = date_key
                bucket["label"] = _format_chart_date(date_value)
                bucket["deaths"] += float(row["deaths"] or 0)
                bucket["injuries"] += float(row["injuries"] or 0)
                bucket["evacuated"] += float(row["evacuated"] or 0)
                bucket["evacuated_children"] += float(row["evacuated_children"] or 0)
                bucket["rescued_children"] += float(row["rescued_children"] or 0)

    items = []
    for date_key in sorted(grouped):
        bucket = grouped[date_key]
        evacuated_adults = bucket["evacuated"]
        total_value = bucket["deaths"] + bucket["injuries"] + evacuated_adults + bucket["evacuated_children"] + bucket["rescued_children"]
        items.append(
            {
                "label": bucket["label"],
                "date_value": bucket["date_value"],
                "value": total_value,
                "value_display": _format_number(total_value, integer=True),
                "deaths": bucket["deaths"],
                "injuries": bucket["injuries"],
                "evacuated_adults": evacuated_adults,
                "evacuated_children": bucket["evacuated_children"],
                "rescued_children": bucket["rescued_children"],
            }
        )

    title = "Последствия, эвакуация и дети"
    empty_message = "Нет данных по погибшим, травмам и эвакуации."
    return _finalize_chart(
        title,
        items,
        empty_message,
        plotly=_build_combined_impact_timeline_plotly(title, items, empty_message),
    )


def _collect_month_counts(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[int, int]:
    grouped: Dict[int, int] = defaultdict(int)

    with engine.connect() as conn:
        for table in selected_tables:
            if DATE_COLUMN not in table["column_set"]:
                continue
            month_expression = _month_expression(DATE_COLUMN)
            conditions = [f"{month_expression} BETWEEN 1 AND 12"]
            if selected_year is not None:
                conditions.append(f"{_year_expression(DATE_COLUMN)} = :selected_year")
            query = text(
                f"""
                SELECT
                    {month_expression} AS month_value,
                    COUNT(*) AS fire_count
                FROM {_quote_identifier(table['name'])}
                WHERE {' AND '.join(conditions)}
                GROUP BY month_value
                ORDER BY month_value
                """
            )
            params = {"selected_year": selected_year} if selected_year is not None else {}
            for row in conn.execute(query, params).mappings().all():
                month_value = row["month_value"]
                if month_value is None:
                    continue
                grouped[int(month_value)] += int(row["fire_count"] or 0)

    return dict(grouped)


def _build_monthly_profile_chart(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    *,
    month_counts: Optional[Dict[int, int]] = None,
) -> Dict[str, Any]:
    grouped = month_counts or _collect_month_counts(selected_tables, selected_year)
    items = []
    if grouped:
        for month_value in range(1, 13):
            items.append(
                {
                    "label": MONTH_LABELS[month_value],
                    "value": grouped.get(month_value, 0),
                    "value_display": _format_number(grouped.get(month_value, 0), integer=True),
                }
            )

    title = "Сезонность по месяцам"
    empty_message = "Нет данных для сезонного профиля."
    return _finalize_chart(title, items, empty_message, plotly=_build_monthly_profile_plotly(title, items, empty_message))


def _build_area_buckets_chart(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
    grouped: Dict[str, int] = defaultdict(int)
    bucket_order = ["До 1 га", "1-5 га", "5-20 га", "20-100 га", "100+ га", "Не указано"]

    with engine.connect() as conn:
        for table in selected_tables:
            where_clause = _build_year_filter_clause(table, selected_year)
            if where_clause is None:
                continue
            area_expression = _area_expression(table)
            query = text(
                f"""
                SELECT
                    CASE
                        WHEN {area_expression} IS NULL THEN 'Не указано'
                        WHEN {area_expression} < 1 THEN 'До 1 га'
                        WHEN {area_expression} < 5 THEN '1-5 га'
                        WHEN {area_expression} < 20 THEN '5-20 га'
                        WHEN {area_expression} < 100 THEN '20-100 га'
                        ELSE '100+ га'
                    END AS bucket,
                    COUNT(*) AS fire_count
                FROM {_quote_identifier(table['name'])}
                WHERE {where_clause}
                GROUP BY bucket
                """
            )
            params = {"selected_year": selected_year} if selected_year is not None and DATE_COLUMN in table["column_set"] else {}
            for row in conn.execute(query, params).mappings().all():
                grouped[row["bucket"]] += int(row["fire_count"] or 0)

    items = [
        {
            "label": bucket,
            "value": grouped.get(bucket, 0),
            "value_display": _format_number(grouped.get(bucket, 0), integer=True),
        }
        for bucket in bucket_order
        if bucket in grouped
    ]
    title = "Структура по площади пожара"
    empty_message = "Нет данных по площади пожара."
    return _finalize_chart(title, items, empty_message, plotly=_build_area_bucket_plotly(title, items, empty_message))


def _build_sql_widgets(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    *,
    cause_counts: Optional[Dict[str, int]] = None,
    month_counts: Optional[Dict[int, int]] = None,
) -> Dict[str, Dict[str, Any]]:
    return {
        "causes": _build_sql_cause_widget(selected_tables, selected_year, cause_counts=cause_counts),
        "districts": _build_sql_district_widget(selected_tables, selected_year),
        "seasons": _build_sql_season_widget(selected_tables, selected_year, month_counts=month_counts),
    }


def _build_sql_cause_widget(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    *,
    cause_counts: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    grouped = cause_counts or _collect_cause_counts(selected_tables, selected_year)
    items = [
        {"label": label, "value": value, "value_display": _format_number(value, integer=True)}
        for label, value in sorted(grouped.items(), key=lambda item: item[1], reverse=True)[:8]
    ]
    title = "SQL-виджет: причины"
    empty_message = "Нет данных по причинам возгорания."
    return _finalize_chart(
        title,
        items,
        empty_message,
        plotly=_build_sql_widget_bar_plotly(title, items, empty_message, color_key="fire", value_label="Пожаров"),
    )


def _build_sql_district_widget(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
    grouped: Dict[str, int] = defaultdict(int)
    with engine.connect() as conn:
        for table in selected_tables:
            district_column = _resolve_district_column(table)
            if not district_column:
                continue
            where_clause = _build_year_filter_clause(table, selected_year)
            if where_clause is None:
                continue
            query = text(
                f"""
                SELECT
                    COALESCE(NULLIF(TRIM(CAST({_quote_identifier(district_column)} AS TEXT)), ''), 'Не указано') AS label,
                    COUNT(*) AS fire_count
                FROM {_quote_identifier(table['name'])}
                WHERE {where_clause}
                GROUP BY label
                ORDER BY fire_count DESC
                """
            )
            params = {"selected_year": selected_year} if selected_year is not None and DATE_COLUMN in table["column_set"] else {}
            for row in conn.execute(query, params).mappings().all():
                grouped[row["label"]] += int(row["fire_count"] or 0)

    items = [
        {"label": label, "value": value, "value_display": _format_number(value, integer=True)}
        for label, value in sorted(grouped.items(), key=lambda item: item[1], reverse=True)[:8]
    ]
    title = "SQL-виджет: районы"
    empty_message = "В выбранных таблицах не найдено колонок района."
    return _finalize_chart(
        title,
        items,
        empty_message,
        plotly=_build_sql_widget_bar_plotly(title, items, empty_message, color_key="forest", value_label="Пожаров"),
    )


def _build_sql_season_widget(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    *,
    month_counts: Optional[Dict[int, int]] = None,
) -> Dict[str, Any]:
    grouped: Dict[str, int] = defaultdict(int)
    winter_label, spring_label, summer_label, autumn_label = SEASON_ORDER
    for month_value, fire_count in (month_counts or _collect_month_counts(selected_tables, selected_year)).items():
        if month_value in (12, 1, 2):
            season_label = winter_label
        elif month_value in (3, 4, 5):
            season_label = spring_label
        elif month_value in (6, 7, 8):
            season_label = summer_label
        else:
            season_label = autumn_label
        grouped[season_label] += int(fire_count or 0)

    items = [
        {
            "label": season_label,
            "value": grouped.get(season_label, 0),
            "value_display": _format_number(grouped.get(season_label, 0), integer=True),
        }
        for season_label in SEASON_ORDER
        if season_label in grouped
    ]
    title = "SQL-виджет: сезоны"
    empty_message = "Нет данных для сезонного SQL-виджета."
    return _finalize_chart(
        title,
        items,
        empty_message,
        plotly=_build_sql_widget_season_plotly(title, items, empty_message),
    )


__all__ = [
    "_collect_cause_counts",
    "_collect_month_counts",
    "_build_area_buckets_chart",
    "_build_cause_chart",
    "_build_combined_impact_timeline_chart",
    "_build_monthly_profile_chart",
    "_build_sql_widgets",
]
