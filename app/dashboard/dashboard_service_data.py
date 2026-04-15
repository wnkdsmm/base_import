from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from app.perf import ensure_sqlalchemy_timing, perf_trace
from app.plotly_bundle import PLOTLY_AVAILABLE, get_plotly_bundle
from app.services.executive_brief import compose_executive_brief_text
from config.db import engine

from .dashboard_service_build import (
    _build_dashboard_aggregation,
    _build_dashboard_error_context,
    _build_dashboard_payload,
    _empty_dashboard_data,
)
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

def _build_dashboard_cache_key(
    metadata: DashboardMetadata,
    table_name: str,
    year: str,
    normalized_group_column: str,
) -> tuple[Any, ...]:
    return (
        _metadata_table_names(metadata),
        table_name,
        year,
        normalized_group_column,
    )

def _build_resolved_dashboard_cache_key(
    metadata: DashboardMetadata,
    selected_table_name: str,
    selected_year: Optional[int],
    selected_group_column: str,
) -> tuple[Any, ...]:
    return _build_dashboard_cache_key(
        metadata,
        selected_table_name,
        str(selected_year) if selected_year is not None else "all",
        selected_group_column,
    )

def _build_dashboard_context_payload(
    *,
    metadata: DashboardMetadata,
    initial_data: DashboardPayload,
    available_years: list[DashboardOption],
    available_group_columns: list[DashboardOption],
    errors: list[str],
    plotly_js: str,
    has_data: bool | None = None,
) -> DashboardContext:
    return {
        "generated_at": _format_datetime(datetime.now()),
        "filters": {
            "tables": metadata["table_options"],
            "years": available_years,
            "group_columns": available_group_columns,
        },
        "initial_data": initial_data,
        "errors": errors,
        "has_data": bool(metadata["tables"]) if has_data is None else has_data,
        "plotly_js": plotly_js,
    }

def _resolve_shell_group_columns(
    metadata: DashboardMetadata,
    filter_state: DashboardRequestState,
) -> tuple[list[DashboardOption], str]:
    resolved_group_columns = filter_state["available_group_columns"]
    available_group_columns = resolved_group_columns or _collect_group_column_options(metadata["tables"])
    selected_group_column = filter_state["selected_group_column"]
    if not resolved_group_columns and available_group_columns:
        has_selected_group = any(item["value"] == selected_group_column for item in available_group_columns)
        if not has_selected_group:
            selected_group_column = available_group_columns[0]["value"]
    return available_group_columns, selected_group_column

def _build_dashboard_shell_initial_data(
    metadata: DashboardMetadata,
    filter_state: DashboardRequestState,
) -> tuple[DashboardPayload, list[DashboardOption]]:
    available_group_columns, selected_group_column = _resolve_shell_group_columns(metadata, filter_state)
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
    initial_data["scope"]["year_label"] = (
        str(filter_state["selected_year"]) if filter_state["selected_year"] is not None else "Все годы"
    )
    initial_data["scope"]["group_label"] = _find_option_label(
        available_group_columns,
        selected_group_column,
        "Нет данных",
    )
    return initial_data, available_group_columns

def _resolve_requested_dashboard_cache(
    metadata: DashboardMetadata,
    table_name: str,
    year: str,
    group_column: str,
) -> tuple[str, tuple[Any, ...], Optional[DashboardPayload]]:
    normalized_group_column = group_column or metadata["default_group_column"]
    cache_key = _build_dashboard_cache_key(metadata, table_name, year, normalized_group_column)
    return normalized_group_column, cache_key, _get_dashboard_cache(cache_key)

def _build_dashboard_request_state(
    metadata: DashboardMetadata,
    *,
    table_name: str,
    year: str,
    normalized_group_column: str,
) -> DashboardRequestState:
    from . import service as _service_module

    resolve_dashboard_filters = getattr(_service_module, "_resolve_dashboard_filters", _resolve_dashboard_filters)
    filter_state = resolve_dashboard_filters(
        metadata=metadata,
        table_name=table_name,
        year=year,
        group_column=normalized_group_column,
    )
    return {
        **filter_state,
        "resolved_cache_key": _build_resolved_dashboard_cache_key(
            metadata,
            filter_state["selected_table_name"],
            filter_state["selected_year"],
            filter_state["selected_group_column"],
        ),
    }

def _update_dashboard_filter_metrics(
    perf: Any,
    *,
    metadata: DashboardMetadata,
    request_state: DashboardRequestState,
    cached_payload: Optional[DashboardPayload],
    cache_hit: bool,
) -> None:
    perf.update(
        cache_hit=cache_hit,
        cache_key_canonicalized=request_state["resolved_cache_key"] != request_state["cache_key"],
        selected_tables=len(request_state["selected_tables"]),
        available_tables=len(metadata["table_options"]),
        available_years=len(request_state["available_years"]),
        available_group_columns=len(request_state["available_group_columns"]),
        payload_has_data=bool((cached_payload or {}).get("has_data")),
        payload_notes=len((cached_payload or {}).get("notes") or []),
    )

def build_dashboard_context(
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
) -> DashboardContext:
    metadata = _collect_dashboard_metadata_cached()
    initial_data = get_dashboard_data(
        table_name=table_name,
        year=year,
        group_column=group_column or metadata["default_group_column"],
        metadata=metadata,
    )
    return _build_dashboard_context_payload(
        metadata=metadata,
        initial_data=initial_data,
        available_years=initial_data["filters"]["available_years"],
        available_group_columns=initial_data["filters"]["available_group_columns"],
        errors=list(dict.fromkeys(metadata["errors"] + initial_data.get("notes", []))),
        plotly_js=get_plotly_bundle(),
    )

def get_dashboard_page_context(
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
 ) -> DashboardContext:
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
 ) -> DashboardContext:
    try:
        metadata = _collect_dashboard_metadata_cached()
        filter_state = _resolve_dashboard_filters(
            metadata=metadata,
            table_name=table_name,
            year=year,
            group_column=group_column or metadata["default_group_column"],
        )
        initial_data, available_group_columns = _build_dashboard_shell_initial_data(metadata, filter_state)
        return _build_dashboard_context_payload(
            metadata=metadata,
            initial_data=initial_data,
            available_years=filter_state["available_years"],
            available_group_columns=available_group_columns,
            errors=list(dict.fromkeys(metadata["errors"])),
            plotly_js="",
        )
    except Exception as exc:
        return _build_dashboard_error_context(str(exc), plotly_js="")

def get_dashboard_data(
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
    metadata: Optional[DashboardMetadata] = None,
    allow_fallback: bool = True,
) -> DashboardPayload:
    from . import service as _service_module

    collect_dashboard_metadata = getattr(
        _service_module,
        "_collect_dashboard_metadata_cached",
        _collect_dashboard_metadata_cached,
    )
    build_dashboard_cache_key = getattr(_service_module, "_build_dashboard_cache_key", _build_dashboard_cache_key)
    build_dashboard_request_state = getattr(
        _service_module,
        "_build_dashboard_request_state",
        _build_dashboard_request_state,
    )
    get_dashboard_cache = getattr(_service_module, "_get_dashboard_cache", _get_dashboard_cache)
    update_dashboard_filter_metrics = getattr(
        _service_module,
        "_update_dashboard_filter_metrics",
        _update_dashboard_filter_metrics,
    )
    build_dashboard_aggregation = getattr(
        _service_module,
        "_build_dashboard_aggregation",
        _build_dashboard_aggregation,
    )
    build_dashboard_payload = getattr(_service_module, "_build_dashboard_payload", _build_dashboard_payload)
    set_dashboard_cache = getattr(_service_module, "_set_dashboard_cache", _set_dashboard_cache)
    empty_dashboard_data = getattr(_service_module, "_empty_dashboard_data", _empty_dashboard_data)

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
                metadata = metadata or collect_dashboard_metadata()
                normalized_group_column = group_column or metadata["default_group_column"]
                cache_key = build_dashboard_cache_key(metadata, table_name, year, normalized_group_column)
                cached = get_dashboard_cache(cache_key)
                if cached is not None:
                    perf.update(
                        cache_hit=True,
                        available_tables=len(metadata["table_options"]),
                        payload_has_data=bool(cached.get("has_data")),
                        payload_notes=len(cached.get("notes") or []),
                    )
                    return cached

                request_state = build_dashboard_request_state(
                    metadata,
                    table_name=table_name,
                    year=year,
                    normalized_group_column=normalized_group_column,
                )
                request_state["cache_key"] = cache_key
                resolved_cache_key = request_state["resolved_cache_key"]
                cached = get_dashboard_cache(resolved_cache_key)
                if cached is not None:
                    update_dashboard_filter_metrics(
                        perf,
                        metadata=metadata,
                        request_state=request_state,
                        cached_payload=cached,
                        cache_hit=True,
                    )
                    return cached
                update_dashboard_filter_metrics(
                    perf,
                    metadata=metadata,
                    request_state=request_state,
                    cached_payload=None,
                    cache_hit=False,
                )

            with perf.span("aggregation"):
                aggregation = build_dashboard_aggregation(
                    metadata=metadata,
                    selected_tables=request_state["selected_tables"],
                    selected_year=request_state["selected_year"],
                    selected_group_column=request_state["selected_group_column"],
                    selected_table_name=request_state["selected_table_name"],
                    available_years=request_state["available_years"],
                    available_group_columns=request_state["available_group_columns"],
                )
                summary = aggregation["summary"]
                perf.update(input_rows=summary.get("fires_count"))

            with perf.span("payload_render"):
                data = build_dashboard_payload(
                    metadata=metadata,
                    aggregation=aggregation,
                    selected_tables=request_state["selected_tables"],
                    selected_table_name=request_state["selected_table_name"],
                    selected_year=request_state["selected_year"],
                    selected_group_column=request_state["selected_group_column"],
                    available_years=request_state["available_years"],
                    available_group_columns=request_state["available_group_columns"],
                )
                perf.update(
                    payload_has_data=bool(data["has_data"]),
                    payload_notes=len(data.get("notes") or []),
                )

            set_dashboard_cache(request_state["resolved_cache_key"], data)
            return data
        except Exception as exc:
            if not allow_fallback:
                raise
            perf.fail(exc, status="fallback")
            return empty_dashboard_data(str(exc))

__all__ = [
    '_build_dashboard_cache_key',
    '_build_resolved_dashboard_cache_key',
    '_build_dashboard_context_payload',
    '_resolve_shell_group_columns',
    '_build_dashboard_shell_initial_data',
    '_resolve_requested_dashboard_cache',
    '_build_dashboard_request_state',
    '_update_dashboard_filter_metrics',
    'build_dashboard_context',
    'get_dashboard_page_context',
    'get_dashboard_shell_context',
    'get_dashboard_data',
]
