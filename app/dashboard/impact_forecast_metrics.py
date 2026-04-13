from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text

from app.statistics_constants import DATE_COLUMN
from config.db import engine

from .charts import (
    _build_combined_impact_timeline_plotly,
    _build_sql_widget_bar_plotly,
    _build_sql_widget_season_plotly,
    _finalize_chart,
)
from .data_access import (
    SEASON_ORDER,
    _build_impact_timeline_query,
    _build_year_filter_clause,
    _resolve_district_column,
)
from .impact_fire_metrics import _collect_cause_counts, _collect_month_counts
from .utils import _format_chart_date, _format_number, _quote_identifier


def _build_combined_impact_timeline_chart(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    *,
    impact_timeline_rows: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
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

    rows = (
        list(impact_timeline_rows)
        if impact_timeline_rows is not None
        else _collect_impact_timeline_rows(selected_tables, selected_year)
    )

    for row in rows:
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


def _collect_impact_timeline_rows(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
) -> List[Dict[str, Any]]:
    queries = []
    uses_selected_year_param = False
    for index, table in enumerate(selected_tables):
        query = _build_impact_timeline_query(table, selected_year, include_order_by=False)
        if query is None:
            continue
        queries.append(f"SELECT * FROM ({query}) AS impact_timeline_{index}")
        if selected_year is not None and DATE_COLUMN in table["column_set"]:
            uses_selected_year_param = True

    if not queries:
        return []

    params = {"selected_year": selected_year} if uses_selected_year_param else {}
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(text("\nUNION ALL\n".join(queries)), params).mappings().all()]


def _build_sql_widgets(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    *,
    cause_counts: Optional[Dict[str, int]] = None,
    month_counts: Optional[Dict[int, int]] = None,
    district_counts: Optional[Dict[str, int]] = None,
) -> Dict[str, Dict[str, Any]]:
    return {
        "causes": _build_sql_cause_widget(selected_tables, selected_year, cause_counts=cause_counts),
        "districts": (
            _build_sql_district_widget_from_counts(district_counts)
            if district_counts is not None
            else _build_sql_district_widget(selected_tables, selected_year)
        ),
        "seasons": _build_sql_season_widget(selected_tables, selected_year, month_counts=month_counts),
    }


def _build_sql_cause_widget(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    *,
    cause_counts: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    grouped = cause_counts if cause_counts is not None else _collect_cause_counts(selected_tables, selected_year)
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


def _build_sql_district_widget_from_counts(district_counts: Dict[str, int]) -> Dict[str, Any]:
    items = [
        {"label": label, "value": value, "value_display": _format_number(value, integer=True)}
        for label, value in sorted(district_counts.items(), key=lambda item: item[1], reverse=True)[:8]
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
    resolved_month_counts = month_counts if month_counts is not None else _collect_month_counts(selected_tables, selected_year)
    for month_value, fire_count in resolved_month_counts.items():
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
    "_collect_impact_timeline_rows",
    "_build_combined_impact_timeline_chart",
    "_build_sql_district_widget_from_counts",
    "_build_sql_widgets",
]
