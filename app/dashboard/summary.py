from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.statistics_constants import DATE_COLUMN, IMPACT_METRIC_CONFIG
from config.db import engine

from .charts import _build_yearly_plotly, _finalize_chart
from .data_access import (
    _area_expression,
    _build_year_filter_clause,
    _build_yearly_query,
    _metric_expression,
    _resolve_years_in_scope,
)
from .utils import (
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


__all__ = [
    "_build_highlights",
    "_build_scope",
    "_build_summary",
    "_build_trend",
    "_build_yearly_chart",
]
