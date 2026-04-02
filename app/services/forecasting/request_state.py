from __future__ import annotations

from typing import Any, Callable, Dict, Sequence


def normalize_forecasting_cache_value(value: str) -> str:
    return str(value or "").strip()


def build_forecasting_cache_key(
    selected_table: str,
    source_tables: Sequence[str],
    district: str,
    cause: str,
    object_category: str,
    temperature: str,
    days_ahead: int,
    history_window: str,
    include_decision_support: bool,
) -> tuple[str, ...]:
    return (
        selected_table,
        *tuple(source_tables),
        normalize_forecasting_cache_value(district),
        normalize_forecasting_cache_value(cause),
        normalize_forecasting_cache_value(object_category),
        normalize_forecasting_cache_value(temperature),
        str(days_ahead),
        history_window,
        "full" if include_decision_support else "core",
    )


def build_forecasting_request_state(
    *,
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
    include_decision_support: bool = False,
    table_options_builder: Callable[[], list[dict[str, Any]]],
    selection_resolver: Callable[[list[dict[str, Any]], str], str],
    source_tables_resolver: Callable[[list[dict[str, Any]], str], list[str]],
    source_notes_resolver: Callable[[list[dict[str, Any]], str], list[str]],
    forecast_days_parser: Callable[[str], int],
    history_window_parser: Callable[[str], str],
) -> Dict[str, Any]:
    table_options = table_options_builder()
    selected_table = selection_resolver(table_options, table_name)
    source_tables = source_tables_resolver(table_options, selected_table)
    source_table_notes = source_notes_resolver(table_options, selected_table)
    days_ahead = forecast_days_parser(forecast_days)
    selected_history_window = history_window_parser(history_window)
    cache_key = build_forecasting_cache_key(
        selected_table=selected_table,
        source_tables=source_tables,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        days_ahead=days_ahead,
        history_window=selected_history_window,
        include_decision_support=include_decision_support,
    )
    return {
        "table_options": table_options,
        "selected_table": selected_table,
        "source_tables": source_tables,
        "source_table_notes": source_table_notes,
        "days_ahead": days_ahead,
        "history_window": selected_history_window,
        "cache_key": cache_key,
    }


def emit_forecasting_progress(
    progress_callback: Callable[[str, str], None] | None,
    phase: str,
    message: str,
) -> None:
    if progress_callback is None:
        return
    progress_callback(phase, message)
