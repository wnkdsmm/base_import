from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text

from app.statistics_constants import (
    APARTMENTS_DAMAGED_COLUMN,
    APARTMENTS_DESTROYED_COLUMN,
    APART_HOTEL_DAMAGED_COLUMN,
    APART_HOTEL_DESTROYED_COLUMN,
    AREA_DAMAGED_COLUMN,
    AREA_DESTROYED_COLUMN,
    BIRDS_DESTROYED_COLUMN,
    BUILDINGS_DAMAGED_COLUMN,
    BUILDINGS_DESTROYED_COLUMN,
    COLUMN_LABELS,
    DAMAGE_OVERVIEW_LABELS,
    DAMAGE_PAIR_COLUMNS,
    DAMAGE_STANDALONE_COLUMNS,
    DATE_COLUMN,
    DISTRIBUTION_GROUPS,
    FEED_DAMAGED_COLUMN,
    FEED_DESTROYED_COLUMN,
    GRAIN_DAMAGED_COLUMN,
    GRAIN_DESTROYED_COLUMN,
    IMPACT_METRIC_CONFIG,
    LARGE_CATTLE_DESTROYED_COLUMN,
    MONTH_LABELS,
    REGISTERED_DAMAGE_COLUMN,
    RISK_CATEGORY_COLUMN,
    SMALL_CATTLE_DESTROYED_COLUMN,
    TECH_CROPS_DAMAGED_COLUMN,
    TECH_CROPS_DESTROYED_COLUMN,
    VEHICLES_DAMAGED_COLUMN,
    VEHICLES_DESTROYED_COLUMN,
)
from config.db import engine

from .charts import (
    _build_area_bucket_plotly,
    _build_cause_plotly,
    _build_combined_impact_timeline_plotly,
    _build_compact_metric_plotly,
    _build_damage_overview_plotly,
    _build_damage_pairs_plotly,
    _build_damage_share_plotly,
    _build_damage_standalone_plotly,
    _build_distribution_pie_plotly,
    _build_distribution_plotly,
    _build_empty_plotly_chart,
    _build_evacuation_children_plotly,
    _build_monthly_profile_plotly,
    _build_sql_widget_bar_plotly,
    _build_sql_widget_season_plotly,
    _build_table_breakdown_plotly,
    _build_yearly_plotly,
    _finalize_chart,
)
from .data_access import (
    SEASON_ORDER,
    _area_expression,
    _build_impact_timeline_query,
    _build_year_filter_clause,
    _build_yearly_query,
    _collect_impact_totals,
    _is_damage_group_selection,
    _metric_expression,
    _month_expression,
    _resolve_cause_column,
    _resolve_district_column,
    _resolve_table_column_name,
    _resolve_years_in_scope,
    _year_expression,
)
from .utils import (
    _format_chart_date,
    _format_number,
    _format_percentage,
    _format_period_label,
    _format_signed_number,
    _quote_identifier,
)

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

__all__ = [
    "_build_summary",
    "_build_yearly_chart",
    "_build_distribution_chart",
    "_collect_positive_column_counts",
    "_build_damage_category_items",
    "_build_damage_theme_items",
    "_build_damage_overview_chart",
    "_build_damage_pairs_chart",
    "_build_damage_standalone_chart",
    "_build_damage_share_chart",
    "_build_table_breakdown_chart",
    "_build_cause_chart",
    "_build_casualties_chart",
    "_build_evacuation_children_chart",
    "_build_combined_impact_timeline_chart",
    "_build_monthly_profile_chart",
    "_build_area_buckets_chart",
    "_build_sql_widgets",
    "_build_sql_cause_widget",
    "_build_sql_district_widget",
    "_build_sql_season_widget",
    "_build_scope",
    "_build_trend",
    "_build_highlights",
    "_build_rankings",
]
