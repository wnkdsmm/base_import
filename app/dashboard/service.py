from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from app.perf import ensure_sqlalchemy_timing, perf_trace
from app.plotly_bundle import PLOTLY_AVAILABLE, get_plotly_bundle
from app.services.executive_brief import compose_executive_brief_text
from app.statistics_constants import CAUSE_COLUMNS
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
    _build_damage_category_items,
    _build_damage_theme_items,
    _collect_damage_counts,
    _damage_count_columns,
)
from .data_access import _resolve_cause_column, _resolve_table_column_name
from .impact import (
    _build_area_buckets_chart,
    _build_area_buckets_chart_from_counts,
    _build_cause_chart,
    _build_combined_impact_timeline_chart,
    _build_monthly_profile_chart,
    _build_sql_district_widget_from_counts,
    _build_sql_widgets,
    _collect_dashboard_grouped_counts,
    _collect_cause_counts,
    _collect_month_counts,
)
from .metadata import _collect_group_column_options, _is_damage_group_selection, _resolve_dashboard_filters
from .management import _build_management_snapshot, _empty_management_snapshot
from .summary import (
    _build_highlights,
    _build_scope,
    _build_summary,
    _build_trend,
    _build_yearly_chart,
    _collect_dashboard_summary_bundle,
)
from .utils import _find_option_label, _format_datetime


def _can_reuse_distribution_counts(
    selected_tables: list[dict[str, Any]],
    group_column: str,
    grouped_counts: dict[str, int],
) -> bool:
    if not group_column or not grouped_counts:
        return False
    if group_column not in CAUSE_COLUMNS:
        return any(_resolve_table_column_name(table, group_column) for table in selected_tables)
    for table in selected_tables:
        if _resolve_table_column_name(table, group_column) != _resolve_cause_column(table):
            return False
    return True


def _build_dashboard_cache_key(
    metadata: Dict[str, Any],
    table_name: str,
    year: str,
    normalized_group_column: str,
) -> tuple[Any, ...]:
    return (
        tuple(sorted(table["name"] for table in metadata["tables"])),
        table_name,
        year,
        normalized_group_column,
    )


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


def _build_dashboard_error_context(error_message: str, *, plotly_js: str = "") -> Dict[str, Any]:
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


def get_dashboard_page_context(
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
) -> Dict[str, Any]:
    try:
        return build_dashboard_context(table_name=table_name, year=year, group_column=group_column)
    except Exception as exc:
        error_context = _build_dashboard_error_context(str(exc))
        del error_context["plotly_js"]
        return error_context


def get_dashboard_shell_context(
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
) -> Dict[str, Any]:
    try:
        metadata = _collect_dashboard_metadata_cached()
        filter_state = _resolve_dashboard_filters(
            metadata=metadata,
            table_name=table_name,
            year=year,
            group_column=group_column or metadata["default_group_column"],
        )
        resolved_group_columns = filter_state["available_group_columns"]
        available_group_columns = resolved_group_columns or _collect_group_column_options(metadata["tables"])
        selected_group_column = filter_state["selected_group_column"]
        if not resolved_group_columns and available_group_columns:
            has_selected_group = any(item["value"] == selected_group_column for item in available_group_columns)
            if not has_selected_group:
                selected_group_column = available_group_columns[0]["value"]

        initial_data = _empty_dashboard_data()
        initial_data["bootstrap_mode"] = "deferred"
        initial_data["filters"]["table_name"] = filter_state["selected_table_name"]
        initial_data["filters"]["year"] = str(filter_state["selected_year"]) if filter_state["selected_year"] is not None else "all"
        initial_data["filters"]["group_column"] = selected_group_column
        initial_data["filters"]["available_tables"] = metadata["table_options"]
        initial_data["filters"]["available_years"] = filter_state["available_years"]
        initial_data["filters"]["available_group_columns"] = available_group_columns
        initial_data["scope"]["table_label"] = _find_option_label(
            metadata["table_options"],
            filter_state["selected_table_name"],
            "Все таблицы",
        )
        initial_data["scope"]["year_label"] = str(filter_state["selected_year"]) if filter_state["selected_year"] is not None else "Все годы"
        initial_data["scope"]["group_label"] = _find_option_label(
            available_group_columns,
            selected_group_column,
            "Нет данных",
        )

        return {
            "generated_at": _format_datetime(datetime.now()),
            "filters": {
                "tables": metadata["table_options"],
                "years": filter_state["available_years"],
                "group_columns": available_group_columns,
            },
            "initial_data": initial_data,
            "errors": list(dict.fromkeys(metadata["errors"])),
            "has_data": bool(metadata["tables"]),
            "plotly_js": "",
        }
    except Exception as exc:
        return _build_dashboard_error_context(str(exc), plotly_js="")


def _build_dashboard_summary_series(
    selected_tables: list[dict[str, Any]],
    selected_year: Optional[int],
) -> Dict[str, Any]:
    summary_bundle = _collect_dashboard_summary_bundle(selected_tables, selected_year)
    summary_rows = summary_bundle["summary_rows"]
    return {
        "summary": _build_summary(selected_tables, selected_year, summary_rows=summary_rows),
        "yearly_fires_series": _build_yearly_chart(
            selected_tables,
            metric="count",
            yearly_grouped=summary_bundle["yearly_grouped"],
            include_plotly=False,
        ),
        "table_breakdown_series": _build_table_breakdown_chart(
            selected_tables,
            selected_year,
            summary_rows=summary_rows,
            include_plotly=False,
        ),
    }


def _build_damage_dashboard_item_bundle(
    selected_tables: list[dict[str, Any]],
    selected_year: Optional[int],
    damage_counts: Dict[str, int],
) -> Dict[str, list[Dict[str, Any]]]:
    return {
        "category_items": _build_damage_category_items(
            selected_tables,
            selected_year,
            counts=damage_counts,
        ),
        "theme_items": _build_damage_theme_items(
            selected_tables,
            selected_year,
            counts=damage_counts,
        ),
    }


def _build_damage_dashboard_charts(
    selected_tables: list[dict[str, Any]],
    selected_year: Optional[int],
    *,
    damage_counts: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    damage_counts = damage_counts if damage_counts is not None else _collect_damage_counts(selected_tables, selected_year)
    damage_item_bundle = _build_damage_dashboard_item_bundle(selected_tables, selected_year, damage_counts)
    damage_category_items = damage_item_bundle["category_items"]
    damage_theme_items = damage_item_bundle["theme_items"]
    return {
        "distribution": _build_damage_overview_chart(
            selected_tables,
            selected_year,
            items=damage_category_items,
        ),
        "yearly_area_chart": _build_damage_pairs_chart(
            selected_tables,
            selected_year,
            items=damage_category_items,
        ),
        "monthly_profile": _build_damage_standalone_chart(
            selected_tables,
            selected_year,
            items=damage_theme_items,
        ),
        "area_buckets": _build_damage_share_chart(
            selected_tables,
            selected_year,
            items=damage_theme_items,
        ),
    }


def _build_standard_dashboard_charts(
    selected_tables: list[dict[str, Any]],
    selected_year: Optional[int],
    selected_group_column: str,
    grouped_counts_bundle: Dict[str, Any],
) -> Dict[str, Any]:
    distribution_counts = grouped_counts_bundle["distribution_counts"]
    reusable_distribution_counts = (
        distribution_counts
        if _can_reuse_distribution_counts(
            selected_tables,
            selected_group_column,
            distribution_counts,
        )
        else None
    )
    area_bucket_counts = grouped_counts_bundle["area_bucket_counts"]
    return {
        "distribution": _build_distribution_chart(
            selected_tables,
            selected_year,
            selected_group_column,
            grouped_counts=reusable_distribution_counts,
        ),
        "yearly_area_chart": _build_combined_impact_timeline_chart(
            selected_tables,
            selected_year,
            impact_timeline_rows=grouped_counts_bundle["impact_timeline_rows"],
        ),
        "monthly_profile": _build_monthly_profile_chart(
            selected_tables,
            selected_year,
            month_counts=grouped_counts_bundle["month_counts"],
        ),
        "area_buckets": (
            _build_area_buckets_chart_from_counts(area_bucket_counts)
            if area_bucket_counts is not None
            else _build_area_buckets_chart(selected_tables, selected_year)
        ),
    }


def _build_dashboard_widgets(
    selected_tables: list[dict[str, Any]],
    selected_year: Optional[int],
    grouped_counts_bundle: Dict[str, Any],
) -> Dict[str, Any]:
    widgets = _build_sql_widgets(
        selected_tables,
        selected_year,
        cause_counts=grouped_counts_bundle["cause_counts"],
        month_counts=grouped_counts_bundle["month_counts"],
        district_counts=grouped_counts_bundle["district_counts"],
    )
    district_counts = grouped_counts_bundle["district_counts"]
    if district_counts and not widgets.get("districts", {}).get("items"):
        widgets["districts"] = _build_sql_district_widget_from_counts(district_counts)
    return widgets


def _build_dashboard_aggregation(
    *,
    metadata: Dict[str, Any],
    selected_tables: list[dict[str, Any]],
    selected_year: Optional[int],
    selected_group_column: str,
    selected_table_name: str,
    available_years: list[Dict[str, Any]],
    available_group_columns: list[Dict[str, Any]],
) -> Dict[str, Any]:
    summary_series = _build_dashboard_summary_series(selected_tables, selected_year)
    summary = summary_series["summary"]
    yearly_fires_series = summary_series["yearly_fires_series"]
    table_breakdown_series = summary_series["table_breakdown_series"]
    is_damage_group = _is_damage_group_selection(selected_group_column)
    if is_damage_group:
        grouped_counts_bundle = _collect_dashboard_grouped_counts(
            selected_tables,
            selected_year,
            selected_group_column,
            include_area_buckets=False,
            include_impact_timeline=False,
            positive_count_columns=_damage_count_columns(),
        )
    else:
        grouped_counts_bundle = _collect_dashboard_grouped_counts(
            selected_tables,
            selected_year,
            selected_group_column,
            include_area_buckets=True,
            include_impact_timeline=True,
        )
    cause_counts = grouped_counts_bundle["cause_counts"]
    cause_overview = _build_cause_chart(selected_tables, selected_year, cause_counts=cause_counts)
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
    area_buckets = dashboard_charts["area_buckets"]
    trend = _build_trend(yearly_fires_series)
    rankings = _build_rankings(distribution, table_breakdown_series, yearly_fires_series)
    highlights = _build_highlights(summary, yearly_fires_series, cause_overview)
    widgets = _build_dashboard_widgets(selected_tables, selected_year, grouped_counts_bundle)
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

    return {
        "summary": summary,
        "yearly_fires_series": yearly_fires_series,
        "cause_overview": cause_overview,
        "distribution": distribution,
        "yearly_area_chart": yearly_area_chart,
        "monthly_profile": monthly_profile,
        "area_buckets": area_buckets,
        "trend": trend,
        "rankings": rankings,
        "highlights": highlights,
        "widgets": widgets,
        "management": management,
        "scope": scope,
    }


def _build_dashboard_payload(
    *,
    metadata: Dict[str, Any],
    aggregation: Dict[str, Any],
    selected_tables: list[dict[str, Any]],
    selected_table_name: str,
    selected_year: Optional[int],
    selected_group_column: str,
    available_years: list[Dict[str, Any]],
    available_group_columns: list[Dict[str, Any]],
) -> Dict[str, Any]:
    summary = aggregation["summary"]
    scope = aggregation["scope"]
    trend = aggregation["trend"]
    management = aggregation["management"]
    cause_overview = aggregation["cause_overview"]
    distribution = aggregation["distribution"]
    yearly_area_chart = aggregation["yearly_area_chart"]
    monthly_profile = aggregation["monthly_profile"]
    area_buckets = aggregation["area_buckets"]
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

                cache_key = _build_dashboard_cache_key(metadata, table_name, year, normalized_group_column)
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
                aggregation = _build_dashboard_aggregation(
                    metadata=metadata,
                    selected_tables=selected_tables,
                    selected_year=selected_year,
                    selected_group_column=selected_group_column,
                    selected_table_name=selected_table_name,
                    available_years=available_years,
                    available_group_columns=available_group_columns,
                )
                summary = aggregation["summary"]
                perf.update(input_rows=summary.get("fires_count"))

            with perf.span("payload_render"):
                data = _build_dashboard_payload(
                    metadata=metadata,
                    aggregation=aggregation,
                    selected_tables=selected_tables,
                    selected_table_name=selected_table_name,
                    selected_year=selected_year,
                    selected_group_column=selected_group_column,
                    available_years=available_years,
                    available_group_columns=available_group_columns,
                )
                perf.update(
                    payload_has_data=bool(data["has_data"]),
                    payload_notes=len(data.get("notes") or []),
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

__all__ = [
    "_build_dashboard_error_context",
    "_empty_dashboard_data",
    "build_dashboard_context",
    "get_dashboard_data",
    "get_dashboard_page_context",
    "get_dashboard_shell_context",
]
