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
    LARGE_CATTLE_DESTROYED_COLUMN,
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
    _build_damage_overview_plotly,
    _build_damage_pairs_plotly,
    _build_damage_share_plotly,
    _build_damage_standalone_plotly,
    _build_distribution_pie_plotly,
    _build_distribution_plotly,
    _build_empty_plotly_chart,
    _build_table_breakdown_plotly,
    _finalize_chart,
)
from .data_access import _build_year_filter_clause, _numeric_expression_for_column, _resolve_table_column_name
from .types import (
    DashboardTableRef,
    DistributionItem,
    DistributionResult,
    SummaryRow,
)
from .utils import _format_number, _quote_identifier

_DAMAGE_THEME_COLUMNS = {
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


def _damage_count_columns() -> List[str]:
    return list(
        dict.fromkeys(
            list(DISTRIBUTION_GROUPS[2][1])
            + [column_name for columns in _DAMAGE_THEME_COLUMNS.values() for column_name in columns]
        )
    )


def _collect_damage_counts(selected_tables: List[DashboardTableRef], selected_year: Optional[int]) -> Dict[str, int]:
    damage_columns = _damage_count_columns()
    return _collect_positive_column_counts(selected_tables, selected_year, damage_columns)


def _resolve_damage_chart_items(
    items: Optional[Sequence[DistributionItem]],
) -> List[DistributionItem]:
    return items if isinstance(items, list) else list(items or [])


def _resolve_damage_category_items(
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    items: Optional[Sequence[DistributionItem]],
) -> List[DistributionItem]:
    if items is not None:
        return _resolve_damage_chart_items(items)
    return _build_damage_category_items(selected_tables, selected_year)


def _resolve_damage_theme_items(
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    items: Optional[Sequence[DistributionItem]],
) -> List[DistributionItem]:
    if items is not None:
        return _resolve_damage_chart_items(items)
    return _build_damage_theme_items(selected_tables, selected_year)


def _build_distribution_chart(
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    group_column: str,
    *,
    grouped_counts: Optional[Dict[str, int]] = None,
) -> DistributionResult:
    if not group_column:
        empty_message = "Нет доступных колонок для распределения."
        return _finalize_chart(
            "Распределение по колонке",
            [],
            empty_message,
            plotly=_build_empty_plotly_chart("Распределение по колонке", empty_message),
        )

    grouped: Dict[str, int] = defaultdict(int)
    if grouped_counts is not None:
        for label, value in grouped_counts.items():
            grouped[label] += int(value or 0)
    else:
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
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    columns: Sequence[str],
) -> Dict[str, int]:
    counts = {column_name: 0 for column_name in columns}
    if not columns:
        return counts

    aliases = [f"metric_{index}" for index, _ in enumerate(columns)]
    query_parts = []
    uses_selected_year_param = False
    for table in selected_tables:
        available_columns = {column_name for column_name in columns if column_name in table["column_set"]}
        if not available_columns:
            continue
        where_clause = _build_year_filter_clause(table, selected_year)
        if where_clause is None:
            continue
        if selected_year is not None and DATE_COLUMN in table["column_set"]:
            uses_selected_year_param = True

        selects = []
        for alias, column_name in zip(aliases, columns):
            if column_name in available_columns:
                numeric_expression = _numeric_expression_for_column(column_name)
                selects.append(f"COALESCE(SUM(CASE WHEN COALESCE({numeric_expression}, 0) > 0 THEN 1 ELSE 0 END), 0) AS {alias}")
            else:
                selects.append(f"0 AS {alias}")

        query_parts.append(
            f"""
            SELECT {', '.join(selects)}
            FROM {_quote_identifier(table['name'])}
            WHERE {where_clause}
            """
        )

    if not query_parts:
        return counts

    query = text(
        f"""
        SELECT {', '.join(f'COALESCE(SUM({alias}), 0) AS {alias}' for alias in aliases)}
        FROM (
            {' UNION ALL '.join(query_parts)}
        ) AS positive_column_counts
        """
    )
    params = {"selected_year": selected_year} if uses_selected_year_param else {}
    with engine.connect() as conn:
        row = conn.execute(query, params).mappings().one()
    for alias, column_name in zip(aliases, columns):
        counts[column_name] += int(row[alias] or 0)

    return counts


def _build_damage_category_items(
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    *,
    counts: Optional[Dict[str, int]] = None,
) -> List[DistributionItem]:
    counts = counts if counts is not None else _collect_damage_counts(selected_tables, selected_year)
    items: List[DistributionItem] = []
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


def _build_damage_theme_items(
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    *,
    counts: Optional[Dict[str, int]] = None,
) -> List[DistributionItem]:
    counts = counts if counts is not None else _collect_damage_counts(selected_tables, selected_year)
    items: List[DistributionItem] = []

    for label, columns in _DAMAGE_THEME_COLUMNS.items():
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


def _build_damage_overview_chart(
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    *,
    items: Optional[Sequence[DistributionItem]] = None,
) -> DistributionResult:
    items = _resolve_damage_category_items(selected_tables, selected_year, items)
    top_items = items[:12]
    title = "Ущерб: что страдает чаще всего"
    empty_message = "Нет данных по категориям ущерба."
    description = "Категории потерь по объектам и ресурсам: здания, квартиры, площадь пожара, техника, урожай и другие показатели."
    return _finalize_chart(
        title,
        top_items,
        empty_message,
        plotly=_build_damage_overview_plotly(title, top_items, empty_message),
        description=description,
    )


def _build_damage_pairs_chart(
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    *,
    items: Optional[Sequence[DistributionItem]] = None,
) -> DistributionResult:
    items = _resolve_damage_category_items(selected_tables, selected_year, items)
    items = [item for item in items if "destroyed" in item or "damaged" in item]
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


def _build_damage_standalone_chart(
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    *,
    items: Optional[Sequence[DistributionItem]] = None,
) -> DistributionResult:
    items = _resolve_damage_theme_items(selected_tables, selected_year, items)
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


def _build_damage_share_chart(
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    *,
    items: Optional[Sequence[DistributionItem]] = None,
) -> DistributionResult:
    items = _resolve_damage_theme_items(selected_tables, selected_year, items)
    pie_items = [
        {
            "label": item["label"],
            "value": item["value"],
            "value_display": item["value_display"],
        }
        for item in items
    ]
    top_pie_items = pie_items[:10]
    title = "Ущерб: структура потерь"
    empty_message = "Нет данных для структурного графика по ущербу."
    description = "Доля основных направлений потерь в текущем фильтре: что доминирует в ущербе чаще всего."
    return _finalize_chart(
        title,
        top_pie_items,
        empty_message,
        plotly=_build_damage_share_plotly(title, top_pie_items, empty_message),
        description=description,
    )


def _build_table_breakdown_chart(
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    *,
    summary_rows: Optional[Sequence[SummaryRow]] = None,
    include_plotly: bool = True,
) -> DistributionResult:
    items = []
    if summary_rows is not None:
        for row in summary_rows:
            fire_count = int(row.get("fire_count") or 0)
            items.append(
                {
                    "label": row.get("table_name") or "",
                    "value": fire_count,
                    "value_display": _format_number(fire_count, integer=True),
                }
            )
    else:
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
        plotly=(
            _build_table_breakdown_plotly("Количество пожаров по таблицам", items, empty_message)
            if include_plotly
            else None
        ),
    )


def _build_rankings(
    distribution: DistributionResult,
    table_breakdown: DistributionResult,
    yearly_fires: DistributionResult,
) -> Dict[str, List[DistributionItem]]:
    return {
        "top_distribution": distribution["items"][:5],
        "top_tables": table_breakdown["items"][:5],
        "recent_years": list(reversed(yearly_fires["items"][-5:])),
    }


__all__ = [
    "_collect_damage_counts",
    "_build_damage_category_items",
    "_build_damage_overview_chart",
    "_build_damage_pairs_chart",
    "_build_damage_share_chart",
    "_build_damage_standalone_chart",
    "_build_damage_theme_items",
    "_build_distribution_chart",
    "_build_rankings",
    "_build_table_breakdown_chart",
    "_collect_positive_column_counts",
    "_damage_count_columns",
]
