from __future__ import annotations

import json
import re
import time
import textwrap
from collections import defaultdict
from datetime import datetime
from threading import Lock
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import inspect, text

from config.db import engine
from app.statistics_constants import (
    APARTMENTS_DAMAGED_COLUMN,
    APARTMENTS_DESTROYED_COLUMN,
    APART_HOTEL_DAMAGED_COLUMN,
    APART_HOTEL_DESTROYED_COLUMN,
    AREA_COLUMN,
    AREA_DAMAGED_COLUMN,
    AREA_DESTROYED_COLUMN,
    BIRDS_DESTROYED_COLUMN,
    BUILDINGS_DAMAGED_COLUMN,
    BUILDINGS_DESTROYED_COLUMN,
    BUILDING_CATEGORY_COLUMN,
    BUILDING_CAUSE_COLUMN,
    CAUSE_COLUMNS,
    COLUMN_LABELS,
    DAMAGE_GROUP_LABEL,
    DAMAGE_GROUP_OPTION_LABEL,
    DAMAGE_GROUP_OPTION_VALUE,
    DAMAGE_OVERVIEW_LABELS,
    DAMAGE_PAIR_COLUMNS,
    DAMAGE_STANDALONE_COLUMNS,
    DASHBOARD_CACHE_TTL_SECONDS,
    DATE_COLUMN,
    DISTRIBUTION_COLUMNS,
    DISTRIBUTION_GROUPS,
    EXCLUDED_TABLE_PREFIXES,
    FEED_DAMAGED_COLUMN,
    FEED_DESTROYED_COLUMN,
    FIRE_STATION_DISTANCE_COLUMN,
    GENERAL_CAUSE_COLUMN,
    GRAIN_DAMAGED_COLUMN,
    GRAIN_DESTROYED_COLUMN,
    IMPACT_METRIC_CONFIG,
    LARGE_CATTLE_DESTROYED_COLUMN,
    METADATA_CACHE_TTL_SECONDS,
    MONTH_LABELS,
    OBJECT_CATEGORY_COLUMN,
    OBJECT_NAME_COLUMN,
    OPEN_AREA_CAUSE_COLUMN,
    PLOTLY_PALETTE,
    REGISTERED_DAMAGE_COLUMN,
    RISK_CATEGORY_COLUMN,
    SMALL_CATTLE_DESTROYED_COLUMN,
    TECH_CROPS_DAMAGED_COLUMN,
    TECH_CROPS_DESTROYED_COLUMN,
    VEHICLES_DAMAGED_COLUMN,
    VEHICLES_DESTROYED_COLUMN,
)

try:
    import plotly.graph_objects as go
    from plotly.offline import get_plotlyjs
    from plotly.utils import PlotlyJSONEncoder

    PLOTLY_AVAILABLE = True
except Exception:
    go = None
    get_plotlyjs = None
    PlotlyJSONEncoder = None
    PLOTLY_AVAILABLE = False


_CACHE_LOCK = Lock()
_METADATA_CACHE: Dict[str, Any] = {"expires_at": 0.0, "value": None}
_DASHBOARD_CACHE: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
_PLOTLY_BUNDLE_CACHE: Optional[str] = None

DISTRICT_COLUMN_CANDIDATES = [
    "Район",
    "Муниципальный район",
    "Муниципальное образование",
    "Административный район",
    "Район выезда подразделения",
    "Район пожара",
    "Территория",
]
SEASON_ORDER = ["Зима", "Весна", "Лето", "Осень"]


def build_dashboard_context(
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
) -> Dict[str, Any]:
    metadata = _collect_dashboard_metadata_cached()
    initial_data = get_dashboard_data(
        table_name=table_name,
        year=year,
        group_column=group_column or metadata["default_group_column"],

        metadata=metadata,
    )

    return {
        "generated_at": _format_datetime(datetime.now()),
        "filters": {
            "tables": metadata["table_options"],
            "years": initial_data["filters"]["available_years"],
            "group_columns": initial_data["filters"]["available_group_columns"],

        },
        "initial_data": initial_data,
        "errors": list(dict.fromkeys(metadata["errors"] + initial_data.get("notes", []))),
        "has_data": bool(metadata["tables"]),
        "plotly_js": _get_plotly_bundle(),
    }


def get_dashboard_data(
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    try:
        metadata = metadata or _collect_dashboard_metadata_cached()
        tables = metadata["tables"]
        normalized_group_column = group_column or metadata["default_group_column"]

        cache_key = (
            tuple(sorted(table["name"] for table in tables)),
            table_name,
            normalized_group_column,

        )
        cached = _get_dashboard_cache(cache_key)
        if cached is not None:
            return cached

        selected_tables = _resolve_selected_tables(tables, table_name)
        available_years = []
        selected_year = None

        available_group_columns = _collect_group_column_options(selected_tables)

        selected_group_column = _resolve_group_column(
            normalized_group_column,
            available_group_columns,
            metadata["default_group_column"],
        )

        selected_table_name = table_name if any(item["value"] == table_name for item in metadata["table_options"]) else "all"

        summary = _build_summary(selected_tables, selected_year)
        yearly_fires_series = _build_yearly_chart(selected_tables, metric="count")
        table_breakdown_series = _build_table_breakdown_chart(selected_tables, selected_year)
        cause_overview = _build_cause_chart(selected_tables, selected_year)
        if _is_damage_group_selection(selected_group_column):
            distribution = _build_damage_overview_chart(selected_tables, selected_year)
            yearly_area_chart = _build_damage_pairs_chart(selected_tables, selected_year)
            monthly_profile = _build_damage_standalone_chart(selected_tables, selected_year)
            area_buckets = _build_damage_share_chart(selected_tables, selected_year)
        else:
            distribution = _build_distribution_chart(selected_tables, selected_year, selected_group_column)
            yearly_area_chart = _build_combined_impact_timeline_chart(selected_tables, selected_year)
            monthly_profile = _build_monthly_profile_chart(selected_tables, selected_year)
            area_buckets = _build_area_buckets_chart(selected_tables, selected_year)
        trend = _build_trend(yearly_fires_series)
        rankings = _build_rankings(distribution, table_breakdown_series, yearly_fires_series)
        highlights = _build_highlights(summary, yearly_fires_series, cause_overview)
        widgets = _build_sql_widgets(selected_tables, selected_year)
        scope = _build_scope(
            summary=summary,
            metadata=metadata,
            selected_table_label=_find_option_label(metadata["table_options"], selected_table_name, "Все таблицы"),
            selected_group_label=_find_option_label(available_group_columns, selected_group_column, "Нет доступных колонок"),
            available_years=available_years,
        )

        notes = list(metadata["errors"][:5])
        if not PLOTLY_AVAILABLE:
            notes.append("Plotly не найден в окружении. Интерактивные графики не будут показаны.")

        data = {
            "generated_at": _format_datetime(datetime.now()),
            "has_data": bool(selected_tables),
            "summary": summary,
            "scope": scope,
            "trend": trend,
            "highlights": highlights,
            "rankings": rankings,
            "widgets": widgets,
            "charts": {
                "yearly_fires": cause_overview,
                "yearly_area": yearly_area_chart,
                "distribution": distribution,
                "table_breakdown": _finalize_chart("", [], ""),
                "monthly_profile": monthly_profile,
                "area_buckets": area_buckets,
            },
            "filters": {
                "table_name": selected_table_name,
                "year": str(selected_year) if selected_year is not None else "",
                "group_column": selected_group_column,
                "available_tables": metadata["table_options"],
                "available_years": available_years,
                "available_group_columns": available_group_columns,

            },
            "notes": notes,
        }
        _set_dashboard_cache(cache_key, data)
        return data
    except Exception as exc:
        return _empty_dashboard_data(str(exc))


def _empty_dashboard_data(error_message: str = "") -> Dict[str, Any]:
    return {
        "generated_at": _format_datetime(datetime.now()),
        "has_data": False,
        "summary": {
            "fires_count": 0,
            "fires_count_display": "0",
            "total_area": 0,
            "total_area_display": "0",
            "average_area": 0,
            "average_area_display": "0",
            "tables_used": 0,
            "tables_used_display": "0",
            "area_records": 0,
            "area_records_display": "0",
            "area_fill_rate": 0,
            "area_fill_rate_display": "0%",
            "years_covered": 0,
            "years_covered_display": "0",
            "period_label": "Нет данных",
            "year_label": "Все годы",
            "deaths": 0,
            "deaths_display": "0",
            "injuries": 0,
            "injuries_display": "0",
            "evacuated": 0,
            "evacuated_display": "0",
            "evacuated_adults": 0,
            "evacuated_adults_display": "0",
            "evacuated_children": 0,
            "evacuated_children_display": "0",
            "rescued_total": 0,
            "rescued_total_display": "0",
            "rescued_adults": 0,
            "rescued_adults_display": "0",
            "rescued_children": 0,
            "rescued_children_display": "0",
            "children_total": 0,
            "children_total_display": "0",
        },
        "scope": {
            "table_label": "Все таблицы",
            "year_label": "Все годы",
            "group_label": "Нет данных",
            "table_count": 0,
            "table_count_display": "0",
            "database_tables_count": 0,
            "database_tables_count_display": "0",
            "available_years_count": 0,
            "available_years_count_display": "0",
            "period_label": "Нет данных",
        },
        "trend": {
            "title": "Динамика последнего года",
            "current_year": "-",
            "current_value_display": "0",
            "previous_year": "",
            "delta_display": "Нет базы сравнения",
            "direction": "flat",
            "description": "Недостаточно данных для сравнения по годам.",
        },
        "highlights": [],
        "rankings": {
            "top_distribution": [],
            "top_tables": [],
            "recent_years": [],
        },
        "widgets": {
            "causes": _finalize_chart("SQL-виджет: причины", [], "Нет данных по причинам возгорания."),
            "districts": _finalize_chart("SQL-виджет: районы", [], "В выбранных таблицах не найдено колонок района."),
            "seasons": _finalize_chart("SQL-виджет: сезоны", [], "Нет данных для сезонного SQL-виджета."),
        },
        "charts": {
            "yearly_fires": _finalize_chart("Причины возгораний", [], "Нет данных по причинам возгорания."),
            "yearly_area": _finalize_chart("Последствия, эвакуация и дети", [], "Нет данных по погибшим, травмам и эвакуации."),
            "distribution": _finalize_chart("Распределение по колонке", [], "Нет данных для графика."),
            "table_breakdown": _finalize_chart("", [], ""),
            "monthly_profile": _finalize_chart("Сезонность по месяцам", [], "Нет данных для сезонного профиля."),
            "area_buckets": _finalize_chart("Структура по площади пожара", [], "Нет данных по площади пожара."),
        },
        "filters": {
            "table_name": "all",
            "year": "",
            "group_column": "",
            "available_tables": [{"value": "all", "label": "Все таблицы"}],
            "available_years": [],
            "available_group_columns": [],

        },
        "notes": [error_message] if error_message else [],
    }


def _current_dashboard_table_names() -> Tuple[str, ...]:
    inspector = inspect(engine)
    return tuple(sorted(_select_tables(inspector.get_table_names())))



def _metadata_table_names(metadata: Optional[Dict[str, Any]]) -> Tuple[str, ...]:
    if not metadata:
        return ()
    return tuple(sorted(table["name"] for table in metadata.get("tables", [])))



def _invalidate_dashboard_caches() -> None:
    with _CACHE_LOCK:
        _METADATA_CACHE["value"] = None
        _METADATA_CACHE["expires_at"] = 0.0
        _DASHBOARD_CACHE.clear()



def _collect_dashboard_metadata_cached() -> Dict[str, Any]:
    now = time.time()
    current_table_names = _current_dashboard_table_names()

    with _CACHE_LOCK:
        cached_value = _METADATA_CACHE["value"]
        cached_is_fresh = cached_value is not None and float(_METADATA_CACHE["expires_at"]) > now
        if cached_is_fresh and _metadata_table_names(cached_value) == current_table_names:
            return cached_value

    metadata = _collect_dashboard_metadata()
    with _CACHE_LOCK:
        _METADATA_CACHE["value"] = metadata
        _METADATA_CACHE["expires_at"] = now + METADATA_CACHE_TTL_SECONDS
        _DASHBOARD_CACHE.clear()
    return metadata


def _collect_dashboard_metadata() -> Dict[str, Any]:
    inspector = inspect(engine)
    table_names = _select_tables(inspector.get_table_names())

    tables: List[Dict[str, Any]] = []
    errors: List[str] = []

    with engine.connect() as conn:
        for table_name in table_names:
            try:
                columns = [column["name"] for column in inspector.get_columns(table_name)]
                column_set = set(columns)
                table_year = _extract_year_from_name(table_name)
                years = _fetch_table_years(conn, table_name, column_set)
                if not years and table_year is not None:
                    years = [table_year]

                tables.append(
                    {
                        "name": table_name,
                        "column_set": column_set,
                        "years": years,
                        "table_year": table_year,
                    }
                )
            except Exception as exc:
                errors.append(f"{table_name}: {exc}")

    table_options = [{"value": "all", "label": "Все таблицы"}] + [
        {"value": table["name"], "label": table["name"]} for table in tables
    ]
    group_column_options = _collect_group_column_options(tables)
    preferred_default_columns = [
        RISK_CATEGORY_COLUMN,
        BUILDING_CATEGORY_COLUMN,
        GENERAL_CAUSE_COLUMN,
        OPEN_AREA_CAUSE_COLUMN,
        BUILDING_CAUSE_COLUMN,
    ]
    default_group_column = ""
    for candidate in preferred_default_columns:
        if any(item["value"] == candidate for item in group_column_options):
            default_group_column = candidate
            break
    if not default_group_column:
        default_group_column = group_column_options[0]["value"] if group_column_options else ""

    return {
        "tables": tables,
        "table_options": table_options,
        "default_group_column": default_group_column,
        "errors": errors,
    }


def _get_dashboard_cache(cache_key: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    now = time.time()
    with _CACHE_LOCK:
        payload = _DASHBOARD_CACHE.get(cache_key)
        if payload and float(payload["expires_at"]) > now:
            return payload["value"]
        if payload:
            _DASHBOARD_CACHE.pop(cache_key, None)
    return None


def _set_dashboard_cache(cache_key: Tuple[Any, ...], value: Dict[str, Any]) -> None:
    now = time.time()
    with _CACHE_LOCK:
        _DASHBOARD_CACHE[cache_key] = {"expires_at": now + DASHBOARD_CACHE_TTL_SECONDS, "value": value}
        expired_keys = [key for key, payload in _DASHBOARD_CACHE.items() if float(payload["expires_at"]) <= now]
        for key in expired_keys:
            _DASHBOARD_CACHE.pop(key, None)



def _resolve_selected_tables(tables: List[Dict[str, Any]], table_name: str) -> List[Dict[str, Any]]:
    if not table_name or table_name == "all":
        return tables
    return [table for table in tables if table["name"] == table_name]


def _collect_year_options(tables: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    years = sorted({year for table in tables for year in table["years"]}, reverse=True)
    return [{"value": str(year), "label": str(year)} for year in years]


def _collect_group_column_options(tables: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    result = []
    for group_label, columns in DISTRIBUTION_GROUPS:
        if group_label == DAMAGE_GROUP_LABEL:
            result.append({
                "value": DAMAGE_GROUP_OPTION_VALUE,
                "label": DAMAGE_GROUP_OPTION_LABEL,
                "group": group_label,
            })
            continue
        for column_name in columns:
            if not any(_resolve_table_column_name(table, column_name) for table in tables):
                continue
            result.append({
                "value": column_name,
                "label": COLUMN_LABELS.get(column_name, column_name),
                "group": group_label,
            })
    return result


def _resolve_group_column(group_column: str, options: List[Dict[str, str]], default_value: str) -> str:
    available_values = [item["value"] for item in options]
    if group_column in available_values:
        return group_column
    if default_value in available_values:
        return default_value
    return available_values[0] if available_values else ""


def _is_damage_group_selection(group_column: str) -> bool:
    return group_column == DAMAGE_GROUP_OPTION_VALUE


def _build_summary(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
) -> Dict[str, Any]:
    fires_count = 0
    total_area = 0.0
    area_values_count = 0
    tables_used = 0
    years_covered = set()
    impact_totals = {metric_key: 0.0 for metric_key in IMPACT_METRIC_CONFIG}

    with engine.connect() as conn:
        for table in selected_tables:
            where_clause = _build_year_filter_clause(table, selected_year)
            if where_clause is None:
                continue

            tables_used += 1
            years_covered.update(_resolve_years_in_scope(table, selected_year))
            area_expression = _area_expression(table)
            metric_selects = []
            for metric_key in IMPACT_METRIC_CONFIG:
                metric_expression = _metric_expression(table, metric_key)
                metric_selects.append(f"COALESCE(SUM({metric_expression}), 0) AS {metric_key}")
            query = text(
                f"""
                SELECT
                    COUNT(*) AS fire_count,
                    SUM({area_expression}) AS total_area,
                    COUNT({area_expression}) AS area_count,
                    {', '.join(metric_selects)}
                FROM {_quote_identifier(table['name'])}
                WHERE {where_clause}
                """
            )
            params = {"selected_year": selected_year} if selected_year is not None and DATE_COLUMN in table["column_set"] else {}
            row = conn.execute(query, params).mappings().one()
            fires_count += int(row["fire_count"] or 0)
            total_area += float(row["total_area"] or 0)
            area_values_count += int(row["area_count"] or 0)
            for metric_key in IMPACT_METRIC_CONFIG:
                impact_totals[metric_key] += float(row[metric_key] or 0)

    average_area = total_area / area_values_count if area_values_count else 0
    area_fill_rate = (area_values_count / fires_count * 100) if fires_count else 0
    evacuated_adults = impact_totals["evacuated"]
    rescued_adults = impact_totals["rescued_total"]
    children_total = impact_totals["evacuated_children"] + impact_totals["rescued_children"]

    return {
        "fires_count": fires_count,
        "fires_count_display": _format_number(fires_count, integer=True),
        "total_area": total_area,
        "total_area_display": _format_number(total_area),
        "average_area": average_area,
        "average_area_display": _format_number(average_area),
        "tables_used": tables_used,
        "tables_used_display": _format_number(tables_used, integer=True),
        "area_records": area_values_count,
        "area_records_display": _format_number(area_values_count, integer=True),
        "area_fill_rate": area_fill_rate,
        "area_fill_rate_display": _format_percentage(area_fill_rate),
        "years_covered": len(years_covered),
        "years_covered_display": _format_number(len(years_covered), integer=True),
        "period_label": _format_period_label(sorted(years_covered)),
        "year_label": str(selected_year) if selected_year is not None else "Все годы",
        "deaths": impact_totals["deaths"],
        "deaths_display": _format_number(impact_totals["deaths"], integer=True),
        "injuries": impact_totals["injuries"],
        "injuries_display": _format_number(impact_totals["injuries"], integer=True),
        "evacuated": impact_totals["evacuated"],
        "evacuated_display": _format_number(impact_totals["evacuated"], integer=True),
        "evacuated_adults": evacuated_adults,
        "evacuated_adults_display": _format_number(evacuated_adults, integer=True),
        "evacuated_children": impact_totals["evacuated_children"],
        "evacuated_children_display": _format_number(impact_totals["evacuated_children"], integer=True),
        "rescued_total": impact_totals["rescued_total"],
        "rescued_total_display": _format_number(impact_totals["rescued_total"], integer=True),
        "rescued_adults": rescued_adults,
        "rescued_adults_display": _format_number(rescued_adults, integer=True),
        "rescued_children": impact_totals["rescued_children"],
        "rescued_children_display": _format_number(impact_totals["rescued_children"], integer=True),
        "children_total": children_total,
        "children_total_display": _format_number(children_total, integer=True),
    }


def _build_yearly_chart(selected_tables: List[Dict[str, Any]], metric: str) -> Dict[str, Any]:
    grouped: Dict[int, Dict[str, float]] = defaultdict(lambda: {"count": 0.0, "area": 0.0})

    with engine.connect() as conn:
        for table in selected_tables:
            query = _build_yearly_query(table)
            if query is None:
                continue
            for row in conn.execute(text(query)).mappings().all():
                year_value = row["year_value"]
                if year_value is None:
                    continue
                grouped[int(year_value)]["count"] += float(row["fire_count"] or 0)
                grouped[int(year_value)]["area"] += float(row["total_area"] or 0)

    items = []
    for year_value in sorted(grouped):
        raw_value = grouped[year_value][metric]
        items.append(
            {
                "label": str(year_value),
                "value": raw_value,
                "value_display": _format_number(raw_value, integer=(metric == "count")),
            }
        )

    title = "Количество пожаров по годам" if metric == "count" else "Площадь пожаров по годам"
    empty_message = "Недостаточно данных для построения графика по годам."
    return _finalize_chart(title, items, empty_message, plotly=_build_yearly_plotly(title, items, metric, empty_message))


def _build_distribution_chart(selected_tables: List[Dict[str, Any]], selected_year: Optional[int], group_column: str) -> Dict[str, Any]:
    if not group_column:
        empty_message = "Нет доступных колонок для распределения."
        return _finalize_chart(
            "Распределение по колонке",
            [],
            empty_message,
            plotly=_build_empty_plotly_chart("Распределение по колонке", empty_message),
        )

    grouped: Dict[str, int] = defaultdict(int)
    with engine.connect() as conn:
        for table in selected_tables:
            resolved_group_column = _resolve_table_column_name(table, group_column)
            if not resolved_group_column:
                continue
            where_clause = _build_year_filter_clause(table, selected_year)
            if where_clause is None:
                continue
            query = text(
                f"""
                SELECT
                    COALESCE(NULLIF(TRIM(CAST({_quote_identifier(resolved_group_column)} AS TEXT)), ''), 'Не указано') AS label,
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
        for label, value in sorted(grouped.items(), key=lambda item: item[1], reverse=True)[:12]
    ]
    title = f"Распределение: {COLUMN_LABELS.get(group_column, group_column)}"
    empty_message = "Нет данных для выбранной колонки и фильтров."
    plotly_builder = _build_distribution_pie_plotly if group_column == RISK_CATEGORY_COLUMN else _build_distribution_plotly
    return _finalize_chart(title, items, empty_message, plotly=plotly_builder(title, items, empty_message))


def _collect_positive_column_counts(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    columns: Sequence[str],
) -> Dict[str, int]:
    counts = {column_name: 0 for column_name in columns}

    with engine.connect() as conn:
        for table in selected_tables:
            available_columns = [column_name for column_name in columns if column_name in table["column_set"]]
            if not available_columns:
                continue
            where_clause = _build_year_filter_clause(table, selected_year)
            if where_clause is None:
                continue

            selects = []
            alias_to_column: Dict[str, str] = {}
            for index, column_name in enumerate(available_columns):
                alias = f"metric_{index}"
                numeric_expression = _numeric_expression_for_column(column_name)
                selects.append(f"SUM(CASE WHEN COALESCE({numeric_expression}, 0) > 0 THEN 1 ELSE 0 END) AS {alias}")
                alias_to_column[alias] = column_name

            query = text(
                f"""
                SELECT {', '.join(selects)}
                FROM {_quote_identifier(table['name'])}
                WHERE {where_clause}
                """
            )
            params = {"selected_year": selected_year} if selected_year is not None and DATE_COLUMN in table["column_set"] else {}
            row = conn.execute(query, params).mappings().one()
            for alias, column_name in alias_to_column.items():
                counts[column_name] += int(row[alias] or 0)

    return counts


def _build_damage_category_items(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> List[Dict[str, Any]]:
    counts = _collect_positive_column_counts(selected_tables, selected_year, DISTRIBUTION_GROUPS[2][1])
    items: List[Dict[str, Any]] = []
    used_columns = set()

    for label, destroyed_column, damaged_column in DAMAGE_PAIR_COLUMNS:
        destroyed_value = counts.get(destroyed_column, 0)
        damaged_value = counts.get(damaged_column, 0)
        used_columns.add(destroyed_column)
        used_columns.add(damaged_column)
        total_value = destroyed_value + damaged_value
        if total_value <= 0:
            continue
        items.append(
            {
                "label": label,
                "value": total_value,
                "value_display": _format_number(total_value, integer=True),
                "destroyed": destroyed_value,
                "damaged": damaged_value,
            }
        )

    for column_name in DAMAGE_STANDALONE_COLUMNS:
        used_columns.add(column_name)
        value = counts.get(column_name, 0)
        if value <= 0:
            continue
        items.append(
            {
                "label": DAMAGE_OVERVIEW_LABELS.get(column_name, column_name),
                "value": value,
                "value_display": _format_number(value, integer=True),
            }
        )

    for column_name in DISTRIBUTION_GROUPS[2][1]:
        if column_name in used_columns:
            continue
        value = counts.get(column_name, 0)
        if value <= 0:
            continue
        items.append(
            {
                "label": DAMAGE_OVERVIEW_LABELS.get(column_name, column_name),
                "value": value,
                "value_display": _format_number(value, integer=True),
            }
        )

    items.sort(key=lambda item: item["value"], reverse=True)
    return items


def _build_damage_theme_items(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> List[Dict[str, Any]]:
    theme_columns = {
        "Недвижимость": [
            BUILDINGS_DESTROYED_COLUMN,
            BUILDINGS_DAMAGED_COLUMN,
            APARTMENTS_DESTROYED_COLUMN,
            APARTMENTS_DAMAGED_COLUMN,
            APART_HOTEL_DESTROYED_COLUMN,
            APART_HOTEL_DAMAGED_COLUMN,
        ],
        "Площадь пожара": [
            AREA_DESTROYED_COLUMN,
            AREA_DAMAGED_COLUMN,
        ],
        "Техника": [
            VEHICLES_DESTROYED_COLUMN,
            VEHICLES_DAMAGED_COLUMN,
        ],
        "Сельхозпотери": [
            GRAIN_DESTROYED_COLUMN,
            GRAIN_DAMAGED_COLUMN,
            FEED_DESTROYED_COLUMN,
            FEED_DAMAGED_COLUMN,
            TECH_CROPS_DESTROYED_COLUMN,
            TECH_CROPS_DAMAGED_COLUMN,
        ],
        "Животные и птица": [
            LARGE_CATTLE_DESTROYED_COLUMN,
            SMALL_CATTLE_DESTROYED_COLUMN,
            BIRDS_DESTROYED_COLUMN,
        ],
        "Прямой ущерб": [
            REGISTERED_DAMAGE_COLUMN,
        ],
    }
    all_columns = [column_name for columns in theme_columns.values() for column_name in columns]
    counts = _collect_positive_column_counts(selected_tables, selected_year, all_columns)
    items: List[Dict[str, Any]] = []

    for label, columns in theme_columns.items():
        value = sum(int(counts.get(column_name, 0) or 0) for column_name in columns)
        if value <= 0:
            continue
        items.append(
            {
                "label": label,
                "value": value,
                "value_display": _format_number(value, integer=True),
            }
        )

    items.sort(key=lambda item: item["value"], reverse=True)
    return items


def _build_damage_overview_chart(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
    items = _build_damage_category_items(selected_tables, selected_year)
    title = "Ущерб: что страдает чаще всего"
    empty_message = "Нет данных по категориям ущерба."
    description = "Категории потерь по объектам и ресурсам: здания, квартиры, площадь пожара, техника, урожай и другие показатели."
    return _finalize_chart(
        title,
        items[:12],
        empty_message,
        plotly=_build_damage_overview_plotly(title, items[:12], empty_message),
        description=description,
    )


def _build_damage_pairs_chart(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
    items = [item for item in _build_damage_category_items(selected_tables, selected_year) if "destroyed" in item or "damaged" in item]
    title = "Ущерб: уничтожено и повреждено"
    empty_message = "Нет данных по парам показателей ущерба."
    description = "Сравнение по категориям ущерба: где чаще фиксируется уничтожение, а где повреждение имущества и ресурсов."
    return _finalize_chart(
        title,
        items,
        empty_message,
        plotly=_build_damage_pairs_plotly(title, items, empty_message),
        description=description,
    )


def _build_damage_standalone_chart(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
    items = _build_damage_theme_items(selected_tables, selected_year)
    title = "Ущерб: направления потерь"
    empty_message = "Нет данных по укрупненным направлениям ущерба."
    description = "Крупные блоки потерь: недвижимость, площадь пожара, техника, сельхозресурсы, животные и прямой ущерб."
    return _finalize_chart(
        title,
        items,
        empty_message,
        plotly=_build_damage_standalone_plotly(title, items, empty_message),
        description=description,
    )


def _build_damage_share_chart(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
    items = _build_damage_theme_items(selected_tables, selected_year)
    pie_items = [
        {
            "label": item["label"],
            "value": item["value"],
            "value_display": item["value_display"],
        }
        for item in items
    ]
    title = "Ущерб: структура потерь"
    empty_message = "Нет данных для структурного графика по ущербу."
    description = "Доля основных направлений потерь в текущем фильтре: что доминирует в ущербе чаще всего."
    return _finalize_chart(
        title,
        pie_items[:10],
        empty_message,
        plotly=_build_damage_share_plotly(title, pie_items[:10], empty_message),
        description=description,
    )


def _build_table_breakdown_chart(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
    items = []
    with engine.connect() as conn:
        for table in selected_tables:
            where_clause = _build_year_filter_clause(table, selected_year)
            if where_clause is None:
                continue
            query = text(
                f"""
                SELECT COUNT(*) AS fire_count
                FROM {_quote_identifier(table['name'])}
                WHERE {where_clause}
                """
            )
            params = {"selected_year": selected_year} if selected_year is not None and DATE_COLUMN in table["column_set"] else {}
            fire_count = int(conn.execute(query, params).scalar() or 0)
            items.append(
                {
                    "label": table["name"],
                    "value": fire_count,
                    "value_display": _format_number(fire_count, integer=True),
                }
            )

    items.sort(key=lambda item: item["value"], reverse=True)
    items = items[:12]
    empty_message = "Нет данных по выбранным таблицам и году."
    return _finalize_chart(
        "Количество пожаров по таблицам",
        items,
        empty_message,
        plotly=_build_table_breakdown_plotly("Количество пожаров по таблицам", items, empty_message),
    )


def _build_cause_chart(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
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

    items = [
        {"label": label, "value": value, "value_display": _format_number(value, integer=True)}
        for label, value in sorted(grouped.items(), key=lambda item: item[1], reverse=True)[:12]
    ]
    title = "Причины возгораний"
    empty_message = "Нет данных по причинам возгорания."
    return _finalize_chart(title, items, empty_message, plotly=_build_cause_plotly(title, items, empty_message))


def _build_casualties_chart(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
    impact_totals = _collect_impact_totals(selected_tables, selected_year)
    items = [
        {
            "label": "Погибшие",
            "value": impact_totals["deaths"],
            "value_display": _format_number(impact_totals["deaths"], integer=True),
        },
        {
            "label": "Травмированные",
            "value": impact_totals["injuries"],
            "value_display": _format_number(impact_totals["injuries"], integer=True),
        },
    ]
    items = [item for item in items if item["value"] > 0]
    title = "Погибшие и травмированные"
    empty_message = "Нет данных по погибшим и травмированным."
    return _finalize_chart(
        title,
        items,
        empty_message,
        plotly=_build_compact_metric_plotly(title, items, empty_message, color_key="fire", yaxis_title="Люди"),
    )


def _build_evacuation_children_chart(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
    impact_totals = _collect_impact_totals(selected_tables, selected_year)
    evacuated_children = float(impact_totals["evacuated_children"] or 0)
    evacuated_adults = max(float(impact_totals["evacuated"] or 0) - evacuated_children, 0)
    rescued_children = float(impact_totals["rescued_children"] or 0)
    rescued_adults = max(float(impact_totals["rescued_total"] or 0) - rescued_children, 0)

    items = [
        {
            "label": "Эвакуировано взрослых",
            "value": evacuated_adults,
            "value_display": _format_number(evacuated_adults, integer=True),
        },
        {
            "label": "Эвакуировано детей",
            "value": evacuated_children,
            "value_display": _format_number(evacuated_children, integer=True),
        },
        {
            "label": "Спасено взрослых",
            "value": rescued_adults,
            "value_display": _format_number(rescued_adults, integer=True),
        },
        {
            "label": "Спасено детей",
            "value": rescued_children,
            "value_display": _format_number(rescued_children, integer=True),
        },
    ]
    items = [item for item in items if item["value"] > 0]
    title = "Эвакуация и дети"
    empty_message = "Нет данных по эвакуации и спасению детей."
    return _finalize_chart(title, items, empty_message, plotly=_build_evacuation_children_plotly(title, items, empty_message))


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

def _build_monthly_profile_chart(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
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


def _build_sql_widgets(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Dict[str, Any]]:
    return {
        "causes": _build_sql_cause_widget(selected_tables, selected_year),
        "districts": _build_sql_district_widget(selected_tables, selected_year),
        "seasons": _build_sql_season_widget(selected_tables, selected_year),
    }


def _build_sql_cause_widget(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
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


def _build_sql_season_widget(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, Any]:
    grouped: Dict[str, int] = defaultdict(int)

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
                    CASE
                        WHEN {month_expression} IN (12, 1, 2) THEN 'Зима'
                        WHEN {month_expression} IN (3, 4, 5) THEN 'Весна'
                        WHEN {month_expression} IN (6, 7, 8) THEN 'Лето'
                        ELSE 'Осень'
                    END AS label,
                    COUNT(*) AS fire_count
                FROM {_quote_identifier(table['name'])}
                WHERE {' AND '.join(conditions)}
                GROUP BY label
                """
            )
            params = {"selected_year": selected_year} if selected_year is not None else {}
            for row in conn.execute(query, params).mappings().all():
                grouped[row["label"]] += int(row["fire_count"] or 0)

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


def _resolve_district_column(table: Dict[str, Any]) -> str:
    for candidate in DISTRICT_COLUMN_CANDIDATES:
        resolved = _resolve_table_column_name(table, candidate)
        if resolved:
            return resolved
    return ""


def _collect_impact_totals(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, float]:
    totals = {metric_key: 0.0 for metric_key in IMPACT_METRIC_CONFIG}

    with engine.connect() as conn:
        for table in selected_tables:
            where_clause = _build_year_filter_clause(table, selected_year)
            if where_clause is None:
                continue
            metric_selects = []
            for metric_key in IMPACT_METRIC_CONFIG:
                metric_expression = _metric_expression(table, metric_key)
                metric_selects.append(f"COALESCE(SUM({metric_expression}), 0) AS {metric_key}")
            query = text(
                f"""
                SELECT {', '.join(metric_selects)}
                FROM {_quote_identifier(table['name'])}
                WHERE {where_clause}
                """
            )
            params = {"selected_year": selected_year} if selected_year is not None and DATE_COLUMN in table["column_set"] else {}
            row = conn.execute(query, params).mappings().one()
            for metric_key in IMPACT_METRIC_CONFIG:
                totals[metric_key] += float(row[metric_key] or 0)

    return totals


def _build_impact_yearly_query(table: Dict[str, Any], selected_year: Optional[int]) -> Optional[str]:
    year_expression = None
    where_conditions: List[str] = []

    if DATE_COLUMN in table["column_set"]:
        year_expression = _year_expression(DATE_COLUMN)
        if selected_year is None:
            where_conditions.append(f"{year_expression} IS NOT NULL")
        else:
            where_conditions.append(f"{year_expression} = :selected_year")
    elif table["table_year"] is not None:
        if selected_year is not None and table["table_year"] != selected_year:
            return None
        year_expression = str(table["table_year"])
    else:
        return None

    metric_selects = []
    for metric_key in ["deaths", "injuries", "evacuated", "evacuated_children"]:
        metric_expression = _metric_expression(table, metric_key)
        metric_selects.append(f"COALESCE(SUM({metric_expression}), 0) AS {metric_key}")

    where_clause = " AND ".join(where_conditions) if where_conditions else "TRUE"
    return f"""
        SELECT
            {year_expression} AS year_value,
            {', '.join(metric_selects)}
        FROM {_quote_identifier(table['name'])}
        WHERE {where_clause}
        GROUP BY year_value
        ORDER BY year_value
    """


def _build_impact_timeline_query(table: Dict[str, Any], selected_year: Optional[int]) -> Optional[str]:
    where_conditions: List[str] = []

    if DATE_COLUMN in table["column_set"]:
        date_expression = _date_expression(DATE_COLUMN)
        where_conditions.append(f"{date_expression} IS NOT NULL")
        year_clause = _build_year_filter_clause(table, selected_year)
        if year_clause is None:
            return None
        if year_clause != "TRUE":
            where_conditions.append(year_clause)
    elif table["table_year"] is not None:
        if selected_year is not None and table["table_year"] != selected_year:
            return None
        date_expression = f"MAKE_DATE({table['table_year']}, 1, 1)"
    else:
        return None

    where_clause = " AND ".join(where_conditions) if where_conditions else "TRUE"
    return f"""
        SELECT
            {date_expression} AS date_value,
            COALESCE(SUM({_metric_expression(table, "deaths")}), 0) AS deaths,
            COALESCE(SUM({_metric_expression(table, "injuries")}), 0) AS injuries,
            COALESCE(SUM({_metric_expression(table, "evacuated")}), 0) AS evacuated,
            COALESCE(SUM({_metric_expression(table, "evacuated_children")}), 0) AS evacuated_children,
            COALESCE(SUM({_metric_expression(table, "rescued_children")}), 0) AS rescued_children
        FROM {_quote_identifier(table["name"])}
        WHERE {where_clause}
        GROUP BY date_value
        ORDER BY date_value
    """

def _resolve_cause_column(table: Dict[str, Any]) -> str:
    for column_name in CAUSE_COLUMNS:
        resolved_column_name = _resolve_table_column_name(table, column_name)
        if resolved_column_name:
            return resolved_column_name
    return ""


def _resolve_table_column_name(table: Dict[str, Any], expected_name: str) -> str:
    if expected_name in table["column_set"]:
        return expected_name

    normalized_expected = _normalize_match_text(expected_name)
    best_match = ""
    best_score = -1

    for column_name in table["column_set"]:
        normalized_column = _normalize_match_text(column_name)
        if normalized_column == normalized_expected:
            return column_name

        score = -1
        if normalized_expected.startswith(normalized_column):
            score = len(normalized_column)
        elif normalized_column.startswith(normalized_expected):
            score = len(normalized_expected)
        else:
            expected_tokens = [token for token in re.split(r"[^\w]+", normalized_expected) if token]
            column_tokens = [token for token in re.split(r"[^\w]+", normalized_column) if token]
            if expected_tokens and column_tokens and expected_tokens[:len(column_tokens)] == column_tokens:
                score = sum(len(token) for token in column_tokens)

        if score > best_score:
            best_score = score
            best_match = column_name

    return best_match if best_score >= 12 else ""


def _metric_expression(table: Dict[str, Any], metric_key: str) -> str:
    column_name = _find_metric_column(table, metric_key)
    if not column_name:
        return "NULL"
    return _numeric_expression_for_column(column_name)


def _find_metric_column(table: Dict[str, Any], metric_key: str) -> str:
    config = IMPACT_METRIC_CONFIG[metric_key]
    columns = list(table["column_set"])
    normalized_columns = {column_name: _normalize_match_text(column_name) for column_name in columns}
    preferred = [_normalize_match_text(value) for value in config.get("preferred", [])]

    for column_name, normalized_name in normalized_columns.items():
        if normalized_name in preferred:
            return column_name

    best_match = ""
    best_score = -1
    for column_name, normalized_name in normalized_columns.items():
        if any(excluded in normalized_name for excluded in config.get("exclude", [])):
            continue

        score = 0
        for token_group in config.get("include_all", []):
            if all(token in normalized_name for token in token_group):
                score = max(score, 5 + len(token_group))
        for token_group in config.get("include_any", []):
            if all(token in normalized_name for token in token_group):
                score = max(score, 3 + len(token_group))
        if "количество" in normalized_name:
            score += 1
        if score > best_score:
            best_score = score
            best_match = column_name

    return best_match if best_score > 0 else ""


def _numeric_expression_for_column(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    cleaned = f"NULLIF(REPLACE(REPLACE(REPLACE(CAST({column_sql} AS TEXT), ' ', ''), ',', '.'), CHR(160), ''), '')"
    return f"CASE WHEN {cleaned} ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN ({cleaned})::double precision ELSE NULL END"


def _normalize_match_text(value: str) -> str:
    normalized = value.lower().replace("ё", "е")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def _build_yearly_query(table: Dict[str, Any]) -> Optional[str]:
    table_name = _quote_identifier(table["name"])
    area_expression = _area_expression(table)

    if DATE_COLUMN in table["column_set"]:
        year_expression = _year_expression(DATE_COLUMN)
        return f"""
            SELECT
                {year_expression} AS year_value,
                COUNT(*) AS fire_count,
                SUM({area_expression}) AS total_area
            FROM {table_name}
            WHERE {year_expression} IS NOT NULL
            GROUP BY year_value
            ORDER BY year_value
        """

    if table["table_year"] is not None:
        return f"""
            SELECT
                {table['table_year']} AS year_value,
                COUNT(*) AS fire_count,
                SUM({area_expression}) AS total_area
            FROM {table_name}
        """

    return None


def _fetch_table_years(conn: Any, table_name: str, column_set: set) -> List[int]:
    if DATE_COLUMN not in column_set:
        return []
    year_expression = _year_expression(DATE_COLUMN)
    query = text(
        f"""
        SELECT DISTINCT {year_expression} AS year_value
        FROM {_quote_identifier(table_name)}
        WHERE {year_expression} IS NOT NULL
        ORDER BY year_value DESC
        """
    )
    return [int(row["year_value"]) for row in conn.execute(query).mappings().all() if row["year_value"] is not None]


def _build_year_filter_clause(table: Dict[str, Any], selected_year: Optional[int]) -> Optional[str]:
    if selected_year is None:
        return "TRUE"
    if DATE_COLUMN in table["column_set"]:
        return f"{_year_expression(DATE_COLUMN)} = :selected_year"
    if table["table_year"] == selected_year:
        return "TRUE"
    return None


def _year_expression(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    return f"NULLIF(SUBSTRING(CAST({column_sql} AS TEXT) FROM '([0-9]{{4}})'), '')::int"


def _month_expression(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    text_value = f"TRIM(CAST({column_sql} AS TEXT))"
    return (
        "CASE "
        f"WHEN {text_value} ~ '^[0-9]{{4}}[-./][0-9]{{1,2}}[-./][0-9]{{1,2}}' "
        f"THEN NULLIF(SUBSTRING({text_value} FROM '^[0-9]{{4}}[-./]([0-9]{{1,2}})'), '')::int "
        f"WHEN {text_value} ~ '^[0-9]{{1,2}}[-./][0-9]{{1,2}}[-./][0-9]{{4}}' "
        f"THEN NULLIF(SUBSTRING({text_value} FROM '^[0-9]{{1,2}}[-./]([0-9]{{1,2}})'), '')::int "
        "ELSE NULL END"
    )


def _area_expression(table: Dict[str, Any]) -> str:
    if AREA_COLUMN not in table["column_set"]:
        return "NULL"
    column_sql = _quote_identifier(AREA_COLUMN)
    cleaned = f"NULLIF(REPLACE(REPLACE(REPLACE(CAST({column_sql} AS TEXT), ' ', ''), ',', '.'), CHR(160), ''), '')"
    return f"CASE WHEN {cleaned} ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN ({cleaned})::double precision ELSE NULL END"


def _finalize_chart(
    title: str,
    items: List[Dict[str, Any]],
    empty_message: str,
    plotly: Optional[Dict[str, Any]] = None,
    description: str = "",
) -> Dict[str, Any]:
    max_value = max([float(item["value"]) for item in items], default=0)
    normalized_items = []
    for item in items:
        width_percent = 0 if max_value <= 0 else max(4, round(float(item["value"]) / max_value * 100, 2))
        updated = dict(item)
        updated["width_percent"] = width_percent
        normalized_items.append(updated)
    return {
        "title": title,
        "description": description,
        "items": normalized_items,
        "empty_message": empty_message,
        "plotly": plotly or _build_empty_plotly_chart(title, empty_message),
    }


def _build_scope(
    summary: Dict[str, Any],
    metadata: Dict[str, Any],
    selected_table_label: str,
    selected_group_label: str,
    available_years: List[Dict[str, str]],
) -> Dict[str, Any]:
    available_years_count = len(available_years)
    database_tables_count = len(metadata["tables"])
    return {
        "table_label": selected_table_label,
        "year_label": summary["year_label"],
        "group_label": selected_group_label,
        "table_count": summary["tables_used"],
        "table_count_display": summary["tables_used_display"],
        "database_tables_count": database_tables_count,
        "database_tables_count_display": _format_number(database_tables_count, integer=True),
        "available_years_count": available_years_count,
        "available_years_count_display": _format_number(available_years_count, integer=True),
        "period_label": summary["period_label"],
    }


def _build_trend(yearly_fires: Dict[str, Any]) -> Dict[str, Any]:
    items = yearly_fires["items"]
    if not items:
        return {
            "title": "Динамика последнего года",
            "current_year": "-",
            "current_value_display": "0",
            "previous_year": "",
            "delta_display": "Нет базы сравнения",
            "direction": "flat",
            "description": "Недостаточно данных для сравнения по годам.",
        }

    latest = items[-1]
    previous = items[-2] if len(items) > 1 else None
    if previous is None:
        return {
            "title": "Динамика последнего года",
            "current_year": latest["label"],
            "current_value_display": latest["value_display"],
            "previous_year": "",
            "delta_display": "Нет базы сравнения",
            "direction": "flat",
            "description": f"Есть данные только за {latest['label']} год.",
        }

    delta = float(latest["value"]) - float(previous["value"])
    direction = "up" if delta > 0 else "down" if delta < 0 else "flat"
    return {
        "title": "Динамика последнего года",
        "current_year": latest["label"],
        "current_value_display": latest["value_display"],
        "previous_year": previous["label"],
        "delta_display": _format_signed_number(delta, integer=True),
        "direction": direction,
        "description": f"Сравнение {latest['label']} к {previous['label']} году.",
    }


def _build_highlights(
    summary: Dict[str, Any],
    yearly_fires: Dict[str, Any],
    cause_chart: Dict[str, Any],
) -> List[Dict[str, str]]:
    peak_fire = max(yearly_fires["items"], key=lambda item: item["value"], default=None)
    dominant_cause = cause_chart["items"][0] if cause_chart["items"] else None

    return [
        {
            "label": "Пиковый год",
            "value": peak_fire["label"] if peak_fire else "-",
            "meta": f"{peak_fire['value_display']} пожаров" if peak_fire else "Нет данных по годам",
            "tone": "fire",
        },
        {
            "label": "Главная причина",
            "value": dominant_cause["label"] if dominant_cause else "Нет данных",
            "meta": f"{dominant_cause['value_display']} случаев" if dominant_cause else "Причины не заполнены",
            "tone": "group",
        },
        {
            "label": "Погибшие",
            "value": summary["deaths_display"],
            "meta": f"Травмировано: {summary['injuries_display']}",
            "tone": "fire",
        },
        {
            "label": "Эвакуация",
            "value": summary["evacuated_display"],
            "meta": f"Эвакуировано детей: {summary['evacuated_children_display']} | Спасено детей: {summary['rescued_children_display']}",
            "tone": "sky",
        },
    ]


def _build_rankings(
    distribution: Dict[str, Any],
    table_breakdown: Dict[str, Any],
    yearly_fires: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "top_distribution": distribution["items"][:5],
        "top_tables": table_breakdown["items"][:5],
        "recent_years": list(reversed(yearly_fires["items"][-5:])),
    }


def _build_yearly_plotly(title: str, items: List[Dict[str, Any]], metric: str, empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    x_values = [item["label"] for item in items]
    y_values = [item["value"] for item in items]
    text_values = [item["value_display"] for item in items]

    if metric == "count":
        figure = go.Figure(
            data=[
                go.Bar(
                    x=x_values,
                    y=y_values,
                    text=text_values,
                    textposition="outside",
                    marker=dict(
                        color=PLOTLY_PALETTE["fire"],
                        line=dict(color=PLOTLY_PALETTE["fire_soft"], width=1.5),
                    ),
                    hovertemplate="<b>%{x}</b><br>Пожаров: %{customdata}<extra></extra>",
                    customdata=text_values,
                )
            ]
        )
        figure.update_layout(**_plotly_layout("Пожаров", showlegend=False))
    else:
        figure = go.Figure(
            data=[
                go.Scatter(
                    x=x_values,
                    y=y_values,
                    mode="lines+markers",
                    fill="tozeroy",
                    line=dict(color=PLOTLY_PALETTE["forest"], width=4),
                    marker=dict(size=9, color=PLOTLY_PALETTE["forest_soft"]),
                    hovertemplate="<b>%{x}</b><br>Площадь: %{customdata} га<extra></extra>",
                    customdata=text_values,
                )
            ]
        )
        figure.update_layout(**_plotly_layout("Площадь, га", showlegend=False))

    return _figure_to_dict(figure)


def _wrap_plotly_label(value: Any, max_width: int = 34, max_lines: int = 3) -> str:
    normalized = re.sub(r"\s+", " ", str(value or "")).strip()
    if not normalized:
        return ""
    lines = textwrap.wrap(normalized, width=max_width, break_long_words=False, break_on_hyphens=False)
    if not lines:
        return normalized
    if len(lines) > max_lines:
        lines = lines[:max_lines]
        lines[-1] = lines[-1].rstrip(" .,;:") + "..."
    return "<br>".join(lines)


def _build_cause_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    ordered_items = list(reversed(items))
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["value"] for item in ordered_items],
                y=[_wrap_plotly_label(item["label"], max_width=34, max_lines=2) for item in ordered_items],
                orientation="h",
                text=[item["value_display"] for item in ordered_items],
                textposition="outside",
                customdata=[item["label"] for item in ordered_items],
                marker=dict(
                    color=PLOTLY_PALETTE["fire"],
                    line=dict(color=PLOTLY_PALETTE["fire_soft"], width=1.2),
                ),
                hovertemplate="<b>%{customdata}</b><br>Пожаров: %{text}<extra></extra>",
            )
        ]
    )
    layout = _plotly_layout("Количество пожаров", showlegend=False)
    layout["height"] = min(620, max(360, 34 * len(items) + 90))
    layout["margin"] = {"l": 320, "r": 72, "t": 20, "b": 36}
    layout["bargap"] = 0.62
    layout["xaxis"]["automargin"] = True
    layout["yaxis"]["automargin"] = True
    layout["yaxis"]["tickfont"] = {"size": 11}
    figure.update_layout(**layout)
    return _figure_to_dict(figure)


def _build_distribution_pie_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure(
        data=[
            go.Pie(
                labels=[item["label"] for item in items],
                values=[item["value"] for item in items],
                hole=0.45,
                sort=False,
                marker=dict(
                    colors=[
                        PLOTLY_PALETTE["sky"],
                        PLOTLY_PALETTE["sky_soft"],
                        PLOTLY_PALETTE["forest_soft"],
                        PLOTLY_PALETTE["forest"],
                        PLOTLY_PALETTE["sand"],
                        PLOTLY_PALETTE["fire_soft"],
                    ][: len(items)]
                ),
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>Записей: %{value}<br>Доля: %{percent}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("", showlegend=False))
    figure.update_layout(margin=dict(l=24, r=24, t=12, b=12))
    return _figure_to_dict(figure)


def _build_distribution_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    ordered_items = list(reversed(items))
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["value"] for item in ordered_items],
                y=[_wrap_plotly_label(item["label"], max_width=26, max_lines=2) for item in ordered_items],
                orientation="h",
                text=[item["value_display"] for item in ordered_items],
                textposition="outside",
                customdata=[item["label"] for item in ordered_items],
                marker=dict(
                    color=PLOTLY_PALETTE["sky"],
                    line=dict(color=PLOTLY_PALETTE["sky_soft"], width=1.2),
                ),
                hovertemplate="<b>%{customdata}</b><br>Записей: %{text}<extra></extra>",
            )
        ]
    )
    layout = _plotly_layout("Количество записей", showlegend=False)
    layout["height"] = max(340, 36 * len(items) + 90)
    layout["margin"] = {"l": 220, "r": 36, "t": 20, "b": 40}
    layout["yaxis"]["automargin"] = True
    figure.update_layout(**layout)
    return _figure_to_dict(figure)


def _build_compact_metric_plotly(title: str, items: List[Dict[str, Any]], empty_message: str, color_key: str, yaxis_title: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    palette = {
        "fire": [PLOTLY_PALETTE["fire"], PLOTLY_PALETTE["fire_soft"]],
        "sky": [PLOTLY_PALETTE["sky"], PLOTLY_PALETTE["sky_soft"]],
        "forest": [PLOTLY_PALETTE["forest"], PLOTLY_PALETTE["forest_soft"]],
    }
    colors = palette.get(color_key, [PLOTLY_PALETTE["sand"], PLOTLY_PALETTE["sand_soft"]])
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["label"] for item in items],
                y=[item["value"] for item in items],
                text=[item["value_display"] for item in items],
                textposition="outside",
                marker=dict(
                    color=[colors[index % len(colors)] for index in range(len(items))],
                    line=dict(color="rgba(255,255,255,0.5)", width=1.2),
                ),
                hovertemplate="<b>%{x}</b><br>Людей: %{text}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout(yaxis_title, showlegend=False))
    return _figure_to_dict(figure)


def _build_evacuation_children_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure(
        data=[
            go.Pie(
                labels=[item["label"] for item in items],
                values=[item["value"] for item in items],
                hole=0.5,
                sort=False,
                marker=dict(
                    colors=[
                        PLOTLY_PALETTE["sky"],
                        PLOTLY_PALETTE["sky_soft"],
                        PLOTLY_PALETTE["forest"],
                        PLOTLY_PALETTE["forest_soft"],
                    ][: len(items)]
                ),
                textinfo="label+value",
                hovertemplate="<b>%{label}</b><br>Людей: %{value}<br>Доля: %{percent}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("", showlegend=False))
    figure.update_layout(margin=dict(l=24, r=24, t=12, b=12))
    return _figure_to_dict(figure)


def _build_combined_impact_timeline_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    x_values = [item["date_value"] for item in items]
    date_labels = [item["label"] for item in items]

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=x_values,
            y=[item["deaths"] for item in items],
            name="Погибшие",
            customdata=date_labels,
            marker=dict(color=PLOTLY_PALETTE["fire"]),
            hovertemplate="<b>%{customdata}</b><br>Погибшие: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=x_values,
            y=[item["injuries"] for item in items],
            name="Травмированные",
            customdata=date_labels,
            marker=dict(color=PLOTLY_PALETTE["sand"]),
            hovertemplate="<b>%{customdata}</b><br>Травмированные: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=x_values,
            y=[item["evacuated_adults"] for item in items],
            name="Эвакуировано",
            customdata=date_labels,
            mode="lines+markers",
            line=dict(color=PLOTLY_PALETTE["sky"], width=3),
            marker=dict(size=7, color=PLOTLY_PALETTE["sky_soft"]),
            hovertemplate="<b>%{customdata}</b><br>Эвакуировано: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=x_values,
            y=[item["evacuated_children"] for item in items],
            name="Эвакуировано детей",
            customdata=date_labels,
            mode="lines+markers",
            line=dict(color=PLOTLY_PALETTE["forest"], width=3),
            marker=dict(size=7, color=PLOTLY_PALETTE["forest_soft"]),
            hovertemplate="<b>%{customdata}</b><br>Эвакуировано детей: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=x_values,
            y=[item["rescued_children"] for item in items],
            name="Спасено детей",
            customdata=date_labels,
            mode="lines+markers",
            line=dict(color=PLOTLY_PALETTE["ink"], width=2, dash="dot"),
            marker=dict(size=6, color=PLOTLY_PALETTE["fire_soft"]),
            hovertemplate="<b>%{customdata}</b><br>Спасено детей: %{y}<extra></extra>",
        )
    )
    layout = _plotly_layout("Люди", showlegend=True)
    layout["barmode"] = "group"
    layout["legend"] = {"orientation": "h", "y": 1.14, "x": 0}
    layout["xaxis"]["type"] = "date"
    figure.update_layout(**layout)
    return _figure_to_dict(figure)

def _build_damage_overview_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    ordered_items = list(reversed(items))
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["value"] for item in ordered_items],
                y=[_wrap_plotly_label(item["label"], max_width=24, max_lines=2) for item in ordered_items],
                orientation="h",
                text=[item["value_display"] for item in ordered_items],
                textposition="outside",
                customdata=[item["label"] for item in ordered_items],
                marker=dict(
                    color=PLOTLY_PALETTE["sand"],
                    line=dict(color=PLOTLY_PALETTE["sand_soft"], width=1.2),
                ),
                hovertemplate="<b>%{customdata}</b><br>Пожаров с ненулевым показателем: %{text}<extra></extra>",
            )
        ]
    )
    layout = _plotly_layout("Количество пожаров", showlegend=False)
    layout["height"] = max(360, 38 * len(items) + 90)
    layout["margin"] = {"l": 230, "r": 36, "t": 20, "b": 40}
    layout["yaxis"]["automargin"] = True
    figure.update_layout(**layout)
    return _figure_to_dict(figure)


def _build_damage_pairs_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=[_wrap_plotly_label(item["label"], max_width=16, max_lines=2) for item in items],
            y=[item["destroyed"] for item in items],
            name="Уничтожено",
            customdata=[item["label"] for item in items],
            marker=dict(color=PLOTLY_PALETTE["fire"]),
            hovertemplate="<b>%{customdata}</b><br>Пожаров с уничтожением: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=[_wrap_plotly_label(item["label"], max_width=16, max_lines=2) for item in items],
            y=[item["damaged"] for item in items],
            name="Повреждено",
            customdata=[item["label"] for item in items],
            marker=dict(color=PLOTLY_PALETTE["sky"]),
            hovertemplate="<b>%{customdata}</b><br>Пожаров с повреждением: %{y}<extra></extra>",
        )
    )
    layout = _plotly_layout("Количество пожаров", showlegend=True)
    layout["barmode"] = "group"
    layout["legend"] = {"orientation": "h", "y": 1.12, "x": 0}
    layout["xaxis"]["automargin"] = True
    figure.update_layout(**layout)
    return _figure_to_dict(figure)


def _build_damage_standalone_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure(
        data=[
            go.Bar(
                x=[_wrap_plotly_label(item["label"], max_width=16, max_lines=2) for item in items],
                y=[item["value"] for item in items],
                text=[item["value_display"] for item in items],
                textposition="outside",
                customdata=[item["label"] for item in items],
                marker=dict(
                    color=[[PLOTLY_PALETTE["forest"], PLOTLY_PALETTE["sky"], PLOTLY_PALETTE["sand"], PLOTLY_PALETTE["fire_soft"]][index % 4] for index in range(len(items))],
                    line=dict(color="rgba(255,255,255,0.6)", width=1.2),
                ),
                hovertemplate="<b>%{customdata}</b><br>Пожаров с показателем: %{text}<extra></extra>",
            )
        ]
    )
    layout = _plotly_layout("Количество пожаров", showlegend=False)
    layout["xaxis"]["automargin"] = True
    figure.update_layout(**layout)
    return _figure_to_dict(figure)

def _build_damage_share_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure(
        data=[
            go.Pie(
                labels=[item["label"] for item in items],
                values=[item["value"] for item in items],
                hole=0.46,
                sort=False,
                marker=dict(
                    colors=[
                        PLOTLY_PALETTE["fire"],
                        PLOTLY_PALETTE["fire_soft"],
                        PLOTLY_PALETTE["sand"],
                        PLOTLY_PALETTE["sand_soft"],
                        PLOTLY_PALETTE["sky"],
                        PLOTLY_PALETTE["sky_soft"],
                        PLOTLY_PALETTE["forest"],
                        PLOTLY_PALETTE["forest_soft"],
                        "#b99f7a",
                        "#8d7763",
                    ][: len(items)]
                ),
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>Пожаров: %{value}<br>Доля: %{percent}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("", showlegend=False))
    figure.update_layout(margin=dict(l=24, r=24, t=12, b=12))
    return _figure_to_dict(figure)

def _build_table_breakdown_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    ordered_items = list(reversed(items))
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["value"] for item in ordered_items],
                y=[item["label"] for item in ordered_items],
                orientation="h",
                text=[item["value_display"] for item in ordered_items],
                textposition="outside",
                marker=dict(
                    color=PLOTLY_PALETTE["sand"],
                    line=dict(color=PLOTLY_PALETTE["sand_soft"], width=1.2),
                ),
                hovertemplate="<b>%{y}</b><br>Пожаров: %{text}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("Количество пожаров", showlegend=False))
    return _figure_to_dict(figure)


def _build_monthly_profile_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["label"] for item in items],
                y=[item["value"] for item in items],
                text=[item["value_display"] for item in items],
                textposition="outside",
                marker=dict(
                    color=[
                        PLOTLY_PALETTE["fire_soft"],
                        PLOTLY_PALETTE["fire_soft"],
                        PLOTLY_PALETTE["fire"],
                        PLOTLY_PALETTE["fire"],
                        PLOTLY_PALETTE["sand"],
                        PLOTLY_PALETTE["sand"],
                        PLOTLY_PALETTE["sand_soft"],
                        PLOTLY_PALETTE["sand_soft"],
                        PLOTLY_PALETTE["forest_soft"],
                        PLOTLY_PALETTE["forest"],
                        PLOTLY_PALETTE["sky_soft"],
                        PLOTLY_PALETTE["sky"],
                    ][: len(items)],
                    line=dict(color="rgba(255,255,255,0.6)", width=1),
                ),
                hovertemplate="<b>%{x}</b><br>Пожаров: %{text}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("Количество пожаров", showlegend=False))
    return _figure_to_dict(figure)


def _build_area_bucket_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure(
        data=[
            go.Pie(
                labels=[item["label"] for item in items],
                values=[item["value"] for item in items],
                hole=0.58,
                sort=False,
                marker=dict(
                    colors=[
                        PLOTLY_PALETTE["fire"],
                        PLOTLY_PALETTE["fire_soft"],
                        PLOTLY_PALETTE["sand"],
                        PLOTLY_PALETTE["forest"],
                        PLOTLY_PALETTE["sky"],
                        "#b5aea5",
                    ][: len(items)]
                ),
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>Пожаров: %{value}<br>Доля: %{percent}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("", showlegend=False))
    figure.update_layout(margin=dict(l=20, r=20, t=10, b=10))
    return _figure_to_dict(figure)



def _build_sql_widget_bar_plotly(
    title: str,
    items: List[Dict[str, Any]],
    empty_message: str,
    color_key: str,
    value_label: str,
) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    ordered_items = list(reversed(items))
    color_value = PLOTLY_PALETTE.get(color_key, PLOTLY_PALETTE["fire"])
    line_color = PLOTLY_PALETTE.get(f"{color_key}_soft", color_value)
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["value"] for item in ordered_items],
                y=[_wrap_plotly_label(item["label"], max_width=26, max_lines=3) for item in ordered_items],
                orientation="h",
                text=[item["value_display"] for item in ordered_items],
                textposition="outside",
                customdata=[item["label"] for item in ordered_items],
                marker=dict(
                    color=color_value,
                    line=dict(color=line_color, width=1.1),
                ),
                hovertemplate=f"<b>%{{customdata}}</b><br>{value_label}: %{{text}}<extra></extra>",
            )
        ]
    )
    layout = _plotly_layout(value_label, showlegend=False)
    layout["height"] = max(320, 40 * len(items) + 80)
    layout["margin"] = {"l": 220, "r": 30, "t": 16, "b": 32}
    layout["yaxis"]["automargin"] = True
    figure.update_layout(**layout)
    return _figure_to_dict(figure)


def _build_sql_widget_season_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not items:
        return _build_empty_plotly_chart(title, empty_message)

    colors = [
        PLOTLY_PALETTE["sky"],
        PLOTLY_PALETTE["forest"],
        PLOTLY_PALETTE["sand"],
        PLOTLY_PALETTE["fire_soft"],
    ]
    figure = go.Figure(
        data=[
            go.Bar(
                x=[item["label"] for item in items],
                y=[item["value"] for item in items],
                text=[item["value_display"] for item in items],
                textposition="outside",
                marker=dict(color=colors[: len(items)], line=dict(color="rgba(255,255,255,0.7)", width=1)),
                hovertemplate="<b>%{x}</b><br>Пожаров: %{text}<extra></extra>",
            )
        ]
    )
    figure.update_layout(**_plotly_layout("Пожаров", showlegend=False))
    return _figure_to_dict(figure)


def _build_impact_yearly_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    if not PLOTLY_AVAILABLE or not items:
        return _build_empty_plotly_chart(title, empty_message)

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=[item["label"] for item in items],
            y=[item["deaths"] for item in items],
            name="Погибшие",
            marker=dict(color=PLOTLY_PALETTE["fire"]),
            hovertemplate="<b>%{x}</b><br>Погибшие: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=[item["label"] for item in items],
            y=[item["injuries"] for item in items],
            name="Травмированные",
            marker=dict(color=PLOTLY_PALETTE["sand"]),
            hovertemplate="<b>%{x}</b><br>Травмированные: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=[item["label"] for item in items],
            y=[item["evacuated"] for item in items],
            name="Эвакуировано",
            marker=dict(color=PLOTLY_PALETTE["sky"]),
            hovertemplate="<b>%{x}</b><br>Эвакуировано: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=[item["label"] for item in items],
            y=[item["evacuated_children"] for item in items],
            name="Эвакуировано детей",
            mode="lines+markers",
            line=dict(color=PLOTLY_PALETTE["forest"], width=3),
            marker=dict(size=8, color=PLOTLY_PALETTE["forest_soft"]),
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Эвакуировано детей: %{y}<extra></extra>",
        )
    )
    layout = _plotly_layout("Люди", showlegend=True)
    layout["barmode"] = "group"
    layout["legend"] = {"orientation": "h", "y": 1.12, "x": 0}
    layout["yaxis2"] = {
        "overlaying": "y",
        "side": "right",
        "showgrid": False,
        "title": {"text": "Дети"},
    }
    figure.update_layout(**layout)
    return _figure_to_dict(figure)


def _build_empty_plotly_chart(title: str, message: str) -> Dict[str, Any]:
    if not PLOTLY_AVAILABLE:
        return {"data": [], "layout": {}, "config": {"responsive": True}, "empty_message": message}

    figure = go.Figure()
    layout = _plotly_layout("", showlegend=False)
    layout["annotations"] = [
        dict(
            text=message,
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=16, color="#7b6a5a"),
        )
    ]
    layout["xaxis"] = {"visible": False}
    layout["yaxis"] = {"visible": False}
    layout["margin"] = {"l": 20, "r": 20, "t": 20, "b": 20}
    figure.update_layout(**layout)
    payload = _figure_to_dict(figure)
    payload["empty_message"] = message
    return payload


def _plotly_layout(yaxis_title: str, showlegend: bool) -> Dict[str, Any]:
    return {
        "height": 340,
        "bargap": 0.45,
        "showlegend": showlegend,
        "paper_bgcolor": PLOTLY_PALETTE["paper"],
        "plot_bgcolor": PLOTLY_PALETTE["paper"],
        "font": {"family": 'Bahnschrift, "Segoe UI", "Trebuchet MS", sans-serif', "color": PLOTLY_PALETTE["ink"]},
        "margin": {"l": 52, "r": 26, "t": 20, "b": 48},
        "xaxis": {
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 12},
        },
        "yaxis": {
            "title": yaxis_title,
            "gridcolor": PLOTLY_PALETTE["grid"],
            "zeroline": False,
            "tickfont": {"size": 12},
            "title_font": {"size": 12},
        },
        "hoverlabel": {"bgcolor": "#fffaf5", "font": {"color": PLOTLY_PALETTE["ink"]}},
    }


def _figure_to_dict(figure: Any) -> Dict[str, Any]:
    if not PLOTLY_AVAILABLE:
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
    global _PLOTLY_BUNDLE_CACHE
    if not PLOTLY_AVAILABLE or get_plotlyjs is None:
        return ""
    if _PLOTLY_BUNDLE_CACHE is None:
        _PLOTLY_BUNDLE_CACHE = get_plotlyjs()
    return _PLOTLY_BUNDLE_CACHE



def _select_tables(table_names: List[str]) -> List[str]:
    return [name for name in table_names if not name.startswith(EXCLUDED_TABLE_PREFIXES) and not name.startswith("alembic")]


def _extract_year_from_name(table_name: str) -> Optional[int]:
    match = re.search(r"(19|20)\d{2}", table_name)
    return int(match.group(0)) if match else None


def _parse_year(year_value: str) -> Optional[int]:
    if not year_value or year_value == "all":
        return None
    try:
        return int(year_value)
    except ValueError:
        return None


def _resolve_years_in_scope(table: Dict[str, Any], selected_year: Optional[int]) -> List[int]:
    if selected_year is not None:
        return [selected_year] if _build_year_filter_clause(table, selected_year) is not None else []
    if table["years"]:
        return list(table["years"])
    if table["table_year"] is not None:
        return [table["table_year"]]
    return []


def _find_option_label(options: List[Dict[str, str]], value: str, fallback: str) -> str:
    for item in options:
        if item["value"] == value:
            return item["label"]
    return fallback


def _quote_identifier(identifier: str) -> str:
    return engine.dialect.identifier_preparer.quote(identifier)


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


def _format_chart_date(value: Any) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%d.%m.%Y")
    return str(value)

def _format_number(value: Any, integer: bool = False) -> str:
    if value is None:
        return "-"
    numeric_value = float(value)
    if integer:
        return f"{int(round(numeric_value)):,}".replace(",", " ")
    if abs(numeric_value - round(numeric_value)) < 1e-9:
        return f"{int(round(numeric_value)):,}".replace(",", " ")
    return f"{numeric_value:,.2f}".replace(",", " ").replace(".", ",")


def _format_percentage(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return f"{int(round(value))}%"
    return f"{value:.1f}%".replace(".", ",")


def _format_signed_number(value: float, integer: bool = False) -> str:
    if value > 0:
        return f"+{_format_number(value, integer=integer)}"
    return _format_number(value, integer=integer)


def _format_period_label(years: List[int]) -> str:
    if not years:
        return "Нет данных"
    normalized = sorted(set(years))
    if len(normalized) == 1:
        return str(normalized[0])
    return f"{normalized[0]}-{normalized[-1]}"


def _format_datetime(value: datetime) -> str:
    return value.strftime("%d.%m.%Y %H:%M")




















































































