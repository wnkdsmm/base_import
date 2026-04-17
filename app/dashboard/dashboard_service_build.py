from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from app.perf import ensure_sqlalchemy_timing, perf_trace
from app.plotly_bundle import PLOTLY_AVAILABLE, get_plotly_bundle
from app.services.executive_brief import compose_executive_brief_text
from config.db import engine

from .cache import (
    _collect_dashboard_metadata_cached,
    _get_dashboard_cache,
    _metadata_table_names,
    _set_dashboard_cache,
)
from .charts import _finalize_chart
from .distribution import _damage_count_columns
from .distribution_logic import (
    _build_dashboard_widgets,
    _build_damage_dashboard_charts,
    _build_standard_dashboard_charts,
)
from .impact import (
    _build_cause_chart,
    _collect_dashboard_grouped_counts,
)
from .metadata import _collect_group_column_options, _is_damage_group_selection, _resolve_dashboard_filters
from .management import _build_management_snapshot, _empty_management_snapshot
from .summary_logic import (
    _build_dashboard_scope,
    _build_dashboard_summary_metrics,
    _build_dashboard_summary_series,
)
from .types import (
    DashboardAggregation,
    DashboardContext,
    DashboardMetadata,
    DashboardOption,
    DashboardPayload,
    DashboardRequestState,
    DashboardTableRef,
)
from .utils import _find_option_label, _format_datetime

def _build_dashboard_error_context(error_message: str, *, plotly_js: str = "") -> DashboardContext:
    return {
        "generated_at": _format_datetime(datetime.now()),
        "filters": {
            "tables": [{"value": "all", "label": "Все таблицы"}],
            "years": [],
            "group_columns": [],
        },
        "initial_data": get_dashboard_data(),
        "errors": [error_message],
        "has_data": False,
        "plotly_js": plotly_js,
    }

def _build_dashboard_aggregation(
    *,
    metadata: DashboardMetadata,
    selected_tables: list[DashboardTableRef],
    selected_year: Optional[int],
    selected_group_column: str,
    selected_table_name: str,
    available_years: list[DashboardOption],
    available_group_columns: list[DashboardOption],
) -> DashboardAggregation:
    # Compatibility: allow legacy monkeypatches on app.dashboard.service.*
    # to affect the split implementation transparently.
    from . import service as _service_module

    summary_builder = getattr(_service_module, "_build_dashboard_summary_series", _build_dashboard_summary_series)
    grouped_counts_collector = getattr(
        _service_module,
        "_collect_dashboard_grouped_counts",
        _collect_dashboard_grouped_counts,
    )
    damage_count_columns_builder = getattr(_service_module, "_damage_count_columns", _damage_count_columns)
    cause_chart_builder = getattr(_service_module, "_build_cause_chart", _build_cause_chart)
    dashboard_widgets_builder = getattr(_service_module, "_build_dashboard_widgets", _build_dashboard_widgets)
    management_snapshot_builder = getattr(_service_module, "_build_management_snapshot", _build_management_snapshot)
    dashboard_scope_builder = getattr(_service_module, "_build_scope", _build_dashboard_scope)
    trend_builder = getattr(_service_module, "_build_trend", None)
    rankings_builder = getattr(_service_module, "_build_rankings", None)
    highlights_builder = getattr(_service_module, "_build_highlights", None)
    summary_bundle_collector = getattr(_service_module, "_collect_dashboard_summary_bundle", None)
    summary_builder_fn = getattr(_service_module, "_build_summary", None)
    yearly_chart_builder = getattr(_service_module, "_build_yearly_chart", None)
    table_breakdown_builder = getattr(_service_module, "_build_table_breakdown_chart", None)

    if summary_bundle_collector and summary_builder_fn and yearly_chart_builder and table_breakdown_builder:
        summary_bundle = summary_bundle_collector(selected_tables, selected_year)
        summary_rows = summary_bundle["summary_rows"]
        summary = summary_builder_fn(selected_tables, selected_year, summary_rows=summary_rows)
        yearly_fires_series = yearly_chart_builder(
            selected_tables,
            metric="count",
            yearly_grouped=summary_bundle["yearly_grouped"],
            include_plotly=False,
        )
        table_breakdown_series = table_breakdown_builder(
            selected_tables,
            selected_year,
            summary_rows=summary_rows,
            include_plotly=False,
        )
    else:
        summary_series = summary_builder(selected_tables, selected_year)
        summary = summary_series["summary"]
        yearly_fires_series = summary_series["yearly_fires_series"]
        table_breakdown_series = summary_series["table_breakdown_series"]
    is_damage_group = _is_damage_group_selection(selected_group_column)
    if is_damage_group:
        grouped_counts_bundle = grouped_counts_collector(
            selected_tables,
            selected_year,
            selected_group_column,
            include_area_buckets=False,
            include_impact_timeline=False,
            positive_count_columns=damage_count_columns_builder(),
        )
    else:
        grouped_counts_bundle = grouped_counts_collector(
            selected_tables,
            selected_year,
            selected_group_column,
            include_area_buckets=True,
            include_impact_timeline=True,
        )
    cause_counts = grouped_counts_bundle["cause_counts"]
    cause_overview = cause_chart_builder(selected_tables, selected_year, cause_counts=cause_counts)
    dashboard_charts = (
        _build_damage_dashboard_charts(
            selected_tables,
            selected_year,
            damage_counts=grouped_counts_bundle["positive_column_counts"],
        )
        if is_damage_group
        else _build_standard_dashboard_charts(
            selected_tables,
            selected_year,
            selected_group_column,
            grouped_counts_bundle,
        )
    )
    distribution = dashboard_charts["distribution"]
    yearly_area_chart = dashboard_charts["yearly_area_chart"]
    monthly_profile = dashboard_charts["monthly_profile"]
    monthly_heatmap = dashboard_charts["monthly_heatmap"]
    area_buckets = dashboard_charts["area_buckets"]
    cumulative_area = dashboard_charts["cumulative_area"]
    if trend_builder and rankings_builder and highlights_builder:
        trend = trend_builder(yearly_fires_series)
        rankings = rankings_builder(distribution, table_breakdown_series, yearly_fires_series)
        highlights = highlights_builder(summary, yearly_fires_series, cause_overview)
    else:
        summary_metrics = _build_dashboard_summary_metrics(
            summary=summary,
            yearly_fires_series=yearly_fires_series,
            table_breakdown_series=table_breakdown_series,
            distribution=distribution,
            cause_overview=cause_overview,
        )
        trend = summary_metrics["trend"]
        rankings = summary_metrics["rankings"]
        highlights = summary_metrics["highlights"]
    widgets = dashboard_widgets_builder(selected_tables, selected_year, grouped_counts_bundle)
    management = management_snapshot_builder(
        selected_tables=selected_tables,
        selected_year=selected_year,
        summary=summary,
        trend=trend,
        cause_overview=cause_overview,
        district_widget=widgets["districts"],
    )
    try:
        scope = dashboard_scope_builder(
            summary=summary,
            metadata=metadata,
            selected_table_name=selected_table_name,
            selected_group_column=selected_group_column,
            available_group_columns=available_group_columns,
            available_years=available_years,
        )
    except TypeError:
        scope = _build_dashboard_scope(
            summary=summary,
            metadata=metadata,
            selected_table_name=selected_table_name,
            selected_group_column=selected_group_column,
            available_group_columns=available_group_columns,
            available_years=available_years,
        )

    return {
        "summary": summary,
        "yearly_fires_series": yearly_fires_series,
        "cause_overview": cause_overview,
        "distribution": distribution,
        "yearly_area_chart": yearly_area_chart,
        "monthly_profile": monthly_profile,
        "monthly_heatmap": monthly_heatmap,
        "area_buckets": area_buckets,
        "cumulative_area": cumulative_area,
        "trend": trend,
        "rankings": rankings,
        "highlights": highlights,
        "widgets": widgets,
        "management": management,
        "scope": scope,
    }

def _build_dashboard_payload(
    *,
    metadata: DashboardMetadata,
    aggregation: DashboardAggregation,
    selected_tables: list[DashboardTableRef],
    selected_table_name: str,
    selected_year: Optional[int],
    selected_group_column: str,
    available_years: list[DashboardOption],
    available_group_columns: list[DashboardOption],
) -> DashboardPayload:
    summary = aggregation["summary"]
    scope = aggregation["scope"]
    trend = aggregation["trend"]
    management = aggregation["management"]
    cause_overview = aggregation["cause_overview"]
    distribution = aggregation["distribution"]
    yearly_area_chart = aggregation["yearly_area_chart"]
    monthly_profile = aggregation["monthly_profile"]
    monthly_heatmap = aggregation["monthly_heatmap"]
    area_buckets = aggregation["area_buckets"]
    cumulative_area = aggregation["cumulative_area"]
    scope_label = f"Таблица: {scope['table_label']} | Год: {scope['year_label']} | Разрез: {scope['group_label']}"

    export_text = compose_executive_brief_text(
        management.get("brief"),
        scope_label=scope_label,
        generated_at=_format_datetime(datetime.now()),
    )
    management["export_text"] = export_text
    if isinstance(management.get("brief"), dict):
        management["brief"]["export_text"] = export_text

    notes = list(metadata["errors"][:5])
    if not PLOTLY_AVAILABLE:
        notes.append("Библиотека Plotly не найдена в окружении. Интерактивные графики не будут показаны.")

    return {
        "generated_at": _format_datetime(datetime.now()),
        "has_data": bool(selected_tables),
        "summary": summary,
        "scope": scope,
        "trend": trend,
        "management": management,
        "highlights": aggregation["highlights"],
        "rankings": aggregation["rankings"],
        "widgets": aggregation["widgets"],
        "charts": {
            "yearly_fires": cause_overview,
            "yearly_area": yearly_area_chart,
            "distribution": distribution,
            "monthly_heatmap": monthly_heatmap,
            "monthly_profile": monthly_profile,
            "area_buckets": area_buckets,
            "cumulative_area": cumulative_area,
        },
        "filters": {
            "table_name": selected_table_name,
            "year": str(selected_year) if selected_year is not None else "all",
            "group_column": selected_group_column,
            "available_tables": metadata["table_options"],
            "available_years": available_years,
            "available_group_columns": available_group_columns,
        },
        "notes": notes,
    }

def _empty_dashboard_data(error_message: str = "") -> DashboardPayload:
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
        "management": _empty_management_snapshot(),
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

            "monthly_heatmap": _finalize_chart("Сезонность по месяцам и годам", [], "Недостаточно данных для тепловой карты сезонности."),
            "monthly_profile": _finalize_chart("Сезонность по месяцам", [], "Нет данных для сезонного профиля."),
            "area_buckets": _finalize_chart("Структура по площади пожара", [], "Нет данных по площади пожара."),

            "cumulative_area": _finalize_chart("Накопленная площадь по дням года", [], "Недостаточно данных для накопленного графика площади."),
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

__all__ = [
    '_build_dashboard_error_context',
    '_build_dashboard_aggregation',
    '_build_dashboard_payload',
    '_empty_dashboard_data',
]
