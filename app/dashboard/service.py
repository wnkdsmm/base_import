from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from app.perf import ensure_sqlalchemy_timing, perf_trace
from app.plotly_bundle import PLOTLY_AVAILABLE, get_plotly_bundle
from app.services.executive_brief import compose_executive_brief_text
from config.db import engine

from .cache import _collect_dashboard_metadata_cached, _get_dashboard_cache, _set_dashboard_cache
from .charts import _finalize_chart
from .distribution import (
    _build_damage_overview_chart,
    _build_damage_pairs_chart,
    _build_damage_share_chart,
    _build_damage_standalone_chart,
    _build_distribution_chart,
    _build_rankings,
    _build_table_breakdown_chart,
)
from .impact import (
    _build_area_buckets_chart,
    _build_cause_chart,
    _build_combined_impact_timeline_chart,
    _build_monthly_profile_chart,
    _build_sql_widgets,
)
from .metadata import _is_damage_group_selection, _resolve_dashboard_filters
from .management import _build_management_snapshot, _empty_management_snapshot
from .summary import _build_highlights, _build_scope, _build_summary, _build_trend, _build_yearly_chart
from .utils import _find_option_label, _format_datetime

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
        "plotly_js": get_plotly_bundle(),
    }


def get_dashboard_data(
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
    metadata: Optional[Dict[str, Any]] = None,
    allow_fallback: bool = True,
) -> Dict[str, Any]:
    ensure_sqlalchemy_timing(engine)
    with perf_trace(
        "dashboard",
        requested_table=table_name,
        requested_year=year,
        requested_group_column=group_column or "",
        allow_fallback=allow_fallback,
    ) as perf:
        try:
            with perf.span("filter_prep"):
                metadata = metadata or _collect_dashboard_metadata_cached()
                normalized_group_column = group_column or metadata["default_group_column"]

                cache_key = (
                    tuple(sorted(table["name"] for table in metadata["tables"])),
                    table_name,
                    year,
                    normalized_group_column,
                )
                cached = _get_dashboard_cache(cache_key)
                if cached is not None:
                    perf.update(
                        cache_hit=True,
                        available_tables=len(metadata["table_options"]),
                        payload_has_data=bool(cached.get("has_data")),
                        payload_notes=len(cached.get("notes") or []),
                    )
                    return cached

                filter_state = _resolve_dashboard_filters(
                    metadata=metadata,
                    table_name=table_name,
                    year=year,
                    group_column=normalized_group_column,
                )
                selected_tables = filter_state["selected_tables"]
                available_years = filter_state["available_years"]
                selected_year = filter_state["selected_year"]
                available_group_columns = filter_state["available_group_columns"]
                selected_group_column = filter_state["selected_group_column"]
                selected_table_name = filter_state["selected_table_name"]
                perf.update(
                    cache_hit=False,
                    selected_tables=len(selected_tables),
                    available_tables=len(metadata["table_options"]),
                    available_years=len(available_years),
                    available_group_columns=len(available_group_columns),
                )

            with perf.span("aggregation"):
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
                management = _build_management_snapshot(
                    selected_tables=selected_tables,
                    selected_year=selected_year,
                    summary=summary,
                    trend=trend,
                    cause_overview=cause_overview,
                    district_widget=widgets["districts"],
                )
                scope = _build_scope(
                    summary=summary,
                    metadata=metadata,
                    selected_table_label=_find_option_label(metadata["table_options"], selected_table_name, "Все таблицы"),
                    selected_group_label=_find_option_label(available_group_columns, selected_group_column, "Нет доступных колонок"),
                    available_years=available_years,
                )
                perf.update(input_rows=summary.get("fires_count"))

            scope_label = f"Таблица: {scope['table_label']} | Год: {scope['year_label']} | Разрез: {scope['group_label']}"
            with perf.span("payload_render"):
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

                data = {
                    "generated_at": _format_datetime(datetime.now()),
                    "has_data": bool(selected_tables),
                    "summary": summary,
                    "scope": scope,
                    "trend": trend,
                    "management": management,
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
                        "year": str(selected_year) if selected_year is not None else "all",
                        "group_column": selected_group_column,
                        "available_tables": metadata["table_options"],
                        "available_years": available_years,
                        "available_group_columns": available_group_columns,
                    },
                    "notes": notes,
                }
                perf.update(
                    payload_has_data=bool(data["has_data"]),
                    payload_notes=len(notes),
                )

            _set_dashboard_cache(cache_key, data)
            return data
        except Exception as exc:
            if not allow_fallback:
                raise
            perf.fail(exc, status="fallback")
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

__all__ = ["build_dashboard_context", "get_dashboard_data", "_empty_dashboard_data"]
