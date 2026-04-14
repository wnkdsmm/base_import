from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text

from app.statistics_constants import CAUSE_COLUMNS, DATE_COLUMN, MONTH_LABELS
from config.db import engine

from .charts import (
    _build_area_bucket_plotly,
    _build_cause_plotly,
    _build_monthly_profile_plotly,
    _finalize_chart,
)
from .data_access import (
    _area_expression,
    _build_year_filter_clause,
    _metric_expression,
    _month_expression,
    _numeric_expression_for_column,
    _resolve_cause_column,
    _resolve_district_column,
    _resolve_table_column_name,
    _year_expression,
)
from .types import (
    DashboardGroupedDimensionSql,
    DashboardGroupedQueryContext,
    DashboardGroupedResultSelects,
    DashboardGroupedCounts,
    DashboardTableRef,
    DistributionResult,
    ImpactMetric,
    ImpactTimelineSqlRow,
)
from .utils import _date_expression, _format_number, _quote_identifier

_AREA_BUCKET_ORDER = ["Р”Рѕ 1 РіР°", "1-5 РіР°", "5-20 РіР°", "20-100 РіР°", "100+ РіР°", "РќРµ СѓРєР°Р·Р°РЅРѕ"]
_IMPACT_TIMELINE_METRIC_KEYS = ("deaths", "injuries", "evacuated", "evacuated_children", "rescued_children")


def _collect_cause_counts(selected_tables: List[DashboardTableRef], selected_year: Optional[int]) -> Dict[str, int]:
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
                    COALESCE(NULLIF(TRIM(CAST({_quote_identifier(cause_column)} AS TEXT)), ''), 'РќРµ СѓРєР°Р·Р°РЅРѕ') AS label,
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
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    *,
    cause_counts: Optional[Dict[str, int]] = None,
) -> DistributionResult:
    grouped = cause_counts if cause_counts is not None else _collect_cause_counts(selected_tables, selected_year)
    items = [
        {"label": label, "value": value, "value_display": _format_number(value, integer=True)}
        for label, value in sorted(grouped.items(), key=lambda item: item[1], reverse=True)[:12]
    ]
    title = "РџСЂРёС‡РёРЅС‹ РІРѕР·РіРѕСЂР°РЅРёР№"
    empty_message = "РќРµС‚ РґР°РЅРЅС‹С… РїРѕ РїСЂРёС‡РёРЅР°Рј РІРѕР·РіРѕСЂР°РЅРёСЏ."
    return _finalize_chart(title, items, empty_message, plotly=_build_cause_plotly(title, items, empty_message))


def _collect_month_counts(selected_tables: List[DashboardTableRef], selected_year: Optional[int]) -> Dict[int, int]:
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


def _column_label_expression(column_name: str) -> str:
    return f"COALESCE(NULLIF(TRIM(CAST({_quote_identifier(column_name)} AS TEXT)), ''), 'РќРµ СѓРєР°Р·Р°РЅРѕ')"


def _resolve_grouped_count_query_context(
    table: DashboardTableRef,
    selected_year: Optional[int],
    selected_group_column: Optional[str],
    *,
    include_area_buckets: bool,
    include_impact_timeline: bool,
    include_positive_columns: bool = False,
) -> Optional[DashboardGroupedQueryContext]:
    where_clause = _build_year_filter_clause(table, selected_year)
    if where_clause is None:
        return None

    cause_column = _resolve_cause_column(table)
    distribution_column = (
        _resolve_table_column_name(table, selected_group_column)
        if selected_group_column and selected_group_column not in CAUSE_COLUMNS
        else ""
    )
    district_column = _resolve_district_column(table)
    has_date_column = DATE_COLUMN in table["column_set"]
    has_timeline = include_impact_timeline and (has_date_column or table["table_year"] is not None)
    dimensions: List[tuple[str, str]] = []
    if cause_column:
        dimensions.append(("cause", "cause_label"))
    if distribution_column:
        dimensions.append(("distribution", "distribution_label"))
    if district_column:
        dimensions.append(("district", "district_label"))
    if has_date_column:
        dimensions.append(("month", "month_label"))
    if include_area_buckets:
        dimensions.append(("area_bucket", "area_bucket_label"))
    if has_timeline:
        dimensions.append(("impact_timeline", "date_value"))
    if not dimensions and not include_positive_columns:
        return None

    return {
        "where_clause": where_clause,
        "cause_column": cause_column,
        "distribution_column": distribution_column,
        "district_column": district_column,
        "has_date_column": has_date_column,
        "has_timeline": has_timeline,
        "include_area_buckets": include_area_buckets,
        "dimensions": dimensions,
    }


def _build_grouped_count_dimension_sql(
    dimensions: Sequence[tuple[str, str]],
    *,
    include_positive_columns: bool = False,
) -> Dict[str, str]:
    if not dimensions:
        return {
            "metric_kind_case": "'positive_column_bundle'",
            "label_case": "CAST(NULL AS TEXT)",
            "grouping_sets": "()",
            "having_clause": "TRUE",
            "positive_group_condition": "TRUE",
        }

    metric_kind_case = "CASE\n" + "\n".join(
        f"                    WHEN GROUPING({column_name}) = 0 THEN '{metric_kind}'"
        for metric_kind, column_name in dimensions
    )
    if include_positive_columns:
        metric_kind_case += "\n                    ELSE 'positive_column_bundle'"
    metric_kind_case += "\n                END"
    label_case = "CASE\n" + "\n".join(
        f"                    WHEN GROUPING({column_name}) = 0 THEN {column_name}"
        for metric_kind, column_name in dimensions
        if metric_kind != "impact_timeline"
    ) + "\n                    ELSE CAST(NULL AS TEXT)\n                END"
    grouping_set_items = [f"({column_name})" for _, column_name in dimensions]
    if include_positive_columns:
        grouping_set_items.append("()")
    grouping_sets = ", ".join(grouping_set_items)
    positive_group_condition = " AND ".join(
        f"GROUPING({column_name}) = 1"
        for _, column_name in dimensions
    )
    having_parts = [
        f"(GROUPING({column_name}) = 0 AND {column_name} IS NOT NULL)"
        for _, column_name in dimensions
    ]
    if include_positive_columns and positive_group_condition:
        having_parts.append(f"({positive_group_condition})")
    having_clause = " OR ".join(having_parts)
    return {
        "metric_kind_case": metric_kind_case,
        "label_case": label_case,
        "grouping_sets": grouping_sets,
        "having_clause": having_clause,
        "positive_group_condition": positive_group_condition or "FALSE",
    }


def _build_grouped_count_time_expressions(
    table: DashboardTableRef,
    *,
    has_date_column: bool,
) -> tuple[str, str]:
    if has_date_column:
        month_expression = _month_expression(DATE_COLUMN)
        month_label_expression = (
            f"CASE WHEN {month_expression} BETWEEN 1 AND 12 "
            f"THEN CAST({month_expression} AS TEXT) ELSE NULL END"
        )
        return month_label_expression, _date_expression(DATE_COLUMN)

    date_value_expression = (
        f"MAKE_DATE({int(table['table_year'])}, 1, 1)"
        if table["table_year"] is not None
        else "CAST(NULL AS DATE)"
    )
    return "CAST(NULL AS TEXT)", date_value_expression


def _area_bucket_label_expression(table: DashboardTableRef) -> str:
    area_expression = _area_expression(table)
    return f"""
            CASE
                WHEN {area_expression} IS NULL THEN '{_AREA_BUCKET_ORDER[5]}'
                WHEN {area_expression} < 1 THEN '{_AREA_BUCKET_ORDER[0]}'
                WHEN {area_expression} < 5 THEN '{_AREA_BUCKET_ORDER[1]}'
                WHEN {area_expression} < 20 THEN '{_AREA_BUCKET_ORDER[2]}'
                WHEN {area_expression} < 100 THEN '{_AREA_BUCKET_ORDER[3]}'
                ELSE '{_AREA_BUCKET_ORDER[4]}'
            END
        """


def _build_grouped_count_source_selects(
    table: DashboardTableRef,
    context: DashboardGroupedQueryContext,
    *,
    month_label_expression: str,
    date_value_expression: str,
    positive_count_columns: Sequence[str] = (),
) -> List[str]:
    source_selects: List[str] = []
    if context["cause_column"]:
        source_selects.append(f"{_column_label_expression(context['cause_column'])} AS cause_label")
    if context["distribution_column"]:
        source_selects.append(f"{_column_label_expression(context['distribution_column'])} AS distribution_label")
    if context["district_column"]:
        source_selects.append(f"{_column_label_expression(context['district_column'])} AS district_label")
    if context["has_date_column"]:
        source_selects.append(f"{month_label_expression} AS month_label")
    if context["include_area_buckets"]:
        source_selects.append(f"{_area_bucket_label_expression(table)} AS area_bucket_label")
    if context["has_timeline"]:
        source_selects.append(f"{date_value_expression} AS date_value")
        source_selects.extend(
            f"{_metric_expression(table, metric_key)} AS {metric_key}"
            for metric_key in _IMPACT_TIMELINE_METRIC_KEYS
        )
    for index, column_name in enumerate(positive_count_columns):
        alias = f"positive_metric_{index}"
        if column_name in table["column_set"]:
            numeric_expression = _numeric_expression_for_column(column_name)
            source_selects.append(f"CASE WHEN COALESCE({numeric_expression}, 0) > 0 THEN 1 ELSE 0 END AS {alias}")
        else:
            source_selects.append(f"0 AS {alias}")
    return source_selects


def _build_grouped_count_result_selects(
    has_timeline: bool,
    *,
    positive_count_columns: Sequence[str] = (),
    positive_group_condition: str = "FALSE",
) -> DashboardGroupedResultSelects:
    if not has_timeline:
        return {
            "fire_count_select": "COUNT(*) AS fire_count",
            "date_value_select": "CAST(NULL AS DATE) AS date_value",
            "metric_selects": [f"0.0 AS {metric_key}" for metric_key in _IMPACT_TIMELINE_METRIC_KEYS],
            "positive_metric_selects": [
                f"CASE WHEN {positive_group_condition} THEN COALESCE(SUM(positive_metric_{index}), 0) ELSE 0 END AS positive_metric_{index}"
                for index, _ in enumerate(positive_count_columns)
            ],
        }

    timeline_group = "GROUPING(date_value) = 0"
    return {
        "fire_count_select": f"CASE WHEN {timeline_group} THEN 0 ELSE COUNT(*) END AS fire_count",
        "date_value_select": f"CASE WHEN {timeline_group} THEN date_value ELSE CAST(NULL AS DATE) END AS date_value",
        "metric_selects": [
            f"CASE WHEN {timeline_group} THEN COALESCE(SUM({metric_key}), 0) ELSE 0.0 END AS {metric_key}"
            for metric_key in _IMPACT_TIMELINE_METRIC_KEYS
        ],
        "positive_metric_selects": [
            f"CASE WHEN {positive_group_condition} THEN COALESCE(SUM(positive_metric_{index}), 0) ELSE 0 END AS positive_metric_{index}"
            for index, _ in enumerate(positive_count_columns)
        ],
    }


def _build_dashboard_grouped_counts_query(
    table: DashboardTableRef,
    selected_year: Optional[int],
    selected_group_column: Optional[str],
    query_index: int,
    *,
    include_area_buckets: bool = True,
    include_impact_timeline: bool = True,
    positive_count_columns: Sequence[str] = (),
) -> tuple[Optional[str], bool]:
    positive_columns = tuple(positive_count_columns)
    context = _resolve_grouped_count_query_context(
        table,
        selected_year,
        selected_group_column,
        include_area_buckets=include_area_buckets,
        include_impact_timeline=include_impact_timeline,
        include_positive_columns=bool(positive_columns),
    )
    if context is None:
        return None, False

    dimension_sql = _build_grouped_count_dimension_sql(
        context["dimensions"],
        include_positive_columns=bool(positive_columns),
    )

    month_label_expression, date_value_expression = _build_grouped_count_time_expressions(
        table,
        has_date_column=context["has_date_column"],
    )

    source_selects = _build_grouped_count_source_selects(
        table,
        context,
        month_label_expression=month_label_expression,
        date_value_expression=date_value_expression,
        positive_count_columns=positive_columns,
    )

    result_selects = _build_grouped_count_result_selects(
        context["has_timeline"],
        positive_count_columns=positive_columns,
        positive_group_condition=dimension_sql["positive_group_condition"],
    )
    metric_selects = result_selects["metric_selects"] + result_selects["positive_metric_selects"]

    query = f"""
        SELECT * FROM (
            SELECT
                {dimension_sql['metric_kind_case']} AS metric_kind,
                {dimension_sql['label_case']} AS label,
                {result_selects['fire_count_select']},
                {result_selects['date_value_select']},
                {', '.join(metric_selects)}
            FROM (
                SELECT
                    {', '.join(source_selects)}
                FROM {_quote_identifier(table["name"])}
                WHERE {context['where_clause']}
            ) AS grouped_source
            GROUP BY GROUPING SETS ({dimension_sql['grouping_sets']})
            HAVING {dimension_sql['having_clause']}
        ) AS grouped_counts_bundle_{query_index}
    """
    uses_selected_year_param = selected_year is not None and context["has_date_column"]
    return query, uses_selected_year_param


def _collect_dashboard_grouped_counts(
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    selected_group_column: Optional[str] = None,
    *,
    include_area_buckets: bool = True,
    include_impact_timeline: bool = True,
    positive_count_columns: Optional[Sequence[str]] = None,
) -> DashboardGroupedCounts:
    cause_counts: Dict[str, int] = defaultdict(int)
    district_counts: Dict[str, int] = defaultdict(int)
    month_counts: Dict[int, int] = defaultdict(int)
    area_bucket_counts: Dict[str, int] = defaultdict(int)
    distribution_counts: Dict[str, int] = defaultdict(int)
    positive_columns = tuple(positive_count_columns or ())
    positive_column_counts: Dict[str, int] = {
        column_name: 0
        for column_name in positive_columns
    }
    impact_timeline_rows: List[ImpactTimelineSqlRow] = []
    subqueries: List[str] = []
    uses_selected_year_param = False

    for table in selected_tables:
        query, query_uses_selected_year_param = _build_dashboard_grouped_counts_query(
            table,
            selected_year,
            selected_group_column,
            len(subqueries),
            include_area_buckets=include_area_buckets,
            include_impact_timeline=include_impact_timeline,
            positive_count_columns=positive_columns,
        )
        if query is not None:
            subqueries.append(query)
            uses_selected_year_param = uses_selected_year_param or query_uses_selected_year_param

    if subqueries:
        params = {"selected_year": selected_year} if uses_selected_year_param else {}
        with engine.connect() as conn:
            rows = conn.execute(text("\nUNION ALL\n".join(subqueries)), params).mappings().all()
    else:
        rows = []

    for row in rows:
        metric_kind = str(row["metric_kind"] or "")
        label = row["label"]
        fire_count = int(row["fire_count"] or 0)
        if metric_kind == "cause":
            cause_counts[str(label or "РќРµ СѓРєР°Р·Р°РЅРѕ")] += fire_count
        elif metric_kind == "distribution":
            distribution_counts[str(label or "РќРµ СѓРєР°Р·Р°РЅРѕ")] += fire_count
        elif metric_kind == "district":
            district_counts[str(label or "РќРµ СѓРєР°Р·Р°РЅРѕ")] += fire_count
        elif metric_kind == "month":
            try:
                month_value = int(label)
            except (TypeError, ValueError):
                continue
            if 1 <= month_value <= 12:
                month_counts[month_value] += fire_count
        elif metric_kind == "area_bucket":
            area_bucket_counts[str(label or "РќРµ СѓРєР°Р·Р°РЅРѕ")] += fire_count
        elif metric_kind == "positive_column_bundle":
            for index, column_name in enumerate(positive_columns):
                positive_column_counts[column_name] += int(row[f"positive_metric_{index}"] or 0)
        elif metric_kind == "impact_timeline":
            impact_timeline_rows.append(dict(row))

    return {
        "cause_counts": dict(cause_counts),
        "district_counts": dict(district_counts),
        "distribution_counts": dict(cause_counts) if selected_group_column in CAUSE_COLUMNS else dict(distribution_counts),
        "month_counts": dict(month_counts),
        "area_bucket_counts": dict(area_bucket_counts),
        "positive_column_counts": dict(positive_column_counts),
        "impact_timeline_rows": impact_timeline_rows,
    }


def _build_monthly_profile_chart(
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    *,
    month_counts: Optional[Dict[int, int]] = None,
) -> DistributionResult:
    grouped = month_counts if month_counts is not None else _collect_month_counts(selected_tables, selected_year)
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

    title = "РЎРµР·РѕРЅРЅРѕСЃС‚СЊ РїРѕ РјРµСЃСЏС†Р°Рј"
    empty_message = "РќРµС‚ РґР°РЅРЅС‹С… РґР»СЏ СЃРµР·РѕРЅРЅРѕРіРѕ РїСЂРѕС„РёР»СЏ."
    return _finalize_chart(title, items, empty_message, plotly=_build_monthly_profile_plotly(title, items, empty_message))


def _build_area_buckets_chart(selected_tables: List[DashboardTableRef], selected_year: Optional[int]) -> DistributionResult:
    grouped: Dict[str, int] = defaultdict(int)
    bucket_order = ["Р”Рѕ 1 РіР°", "1-5 РіР°", "5-20 РіР°", "20-100 РіР°", "100+ РіР°", "РќРµ СѓРєР°Р·Р°РЅРѕ"]

    with engine.connect() as conn:
        for table in selected_tables:
            where_clause = _build_year_filter_clause(table, selected_year)
            if where_clause is None:
                continue
            query = text(
                f"""
                SELECT
                    {_area_bucket_label_expression(table)} AS bucket,
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
    title = "РЎС‚СЂСѓРєС‚СѓСЂР° РїРѕ РїР»РѕС‰Р°РґРё РїРѕР¶Р°СЂР°"
    empty_message = "РќРµС‚ РґР°РЅРЅС‹С… РїРѕ РїР»РѕС‰Р°РґРё РїРѕР¶Р°СЂР°."
    return _finalize_chart(title, items, empty_message, plotly=_build_area_bucket_plotly(title, items, empty_message))


def _build_area_buckets_chart_from_counts(bucket_counts: Dict[str, int]) -> DistributionResult:
    items = [
        {
            "label": bucket,
            "value": int(bucket_counts.get(bucket, 0) or 0),
            "value_display": _format_number(bucket_counts.get(bucket, 0), integer=True),
        }
        for bucket in _AREA_BUCKET_ORDER
        if int(bucket_counts.get(bucket, 0) or 0) > 0
    ]
    title = "РЎС‚СЂСѓРєС‚СѓСЂР° РїРѕ РїР»РѕС‰Р°РґРё РїРѕР¶Р°СЂР°"
    empty_message = "РќРµС‚ РґР°РЅРЅС‹С… РїРѕ РїР»РѕС‰Р°РґРё РїРѕР¶Р°СЂР°."
    return _finalize_chart(title, items, empty_message, plotly=_build_area_bucket_plotly(title, items, empty_message))


__all__ = [
    "_collect_dashboard_grouped_counts",
    "_collect_cause_counts",
    "_collect_month_counts",
    "_build_area_buckets_chart",
    "_build_area_buckets_chart_from_counts",
    "_build_cause_chart",
    "_build_monthly_profile_chart",
]
