from __future__ import annotations

from typing import Any, Callable, Dict

from .constants import ML_CACHE_SCHEMA_VERSION


def build_ml_request_state(
    *,
    table_name: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
    table_options_builder: Callable[[], list[Dict[str, Any]]],
    selection_resolver: Callable[[list[Dict[str, Any]], str], str],
    source_tables_resolver: Callable[[list[Dict[str, Any]], str], list[str]],
    source_notes_resolver: Callable[[list[Dict[str, Any]], str], list[str]],
    forecast_days_parser: Callable[[str], int],
    history_window_parser: Callable[[str], str],
    temperature_parser: Callable[[str], float | None],
    temperature_formatter: Callable[[float], str],
) -> Dict[str, Any]:
    table_options = table_options_builder()
    selected_table = selection_resolver(table_options, table_name)
    source_tables = source_tables_resolver(table_options, selected_table)
    source_table_notes = source_notes_resolver(table_options, selected_table)
    days_ahead = forecast_days_parser(forecast_days)
    selected_history_window = history_window_parser(history_window)
    scenario_temperature = temperature_parser(temperature)
    cache_key = (
        ML_CACHE_SCHEMA_VERSION,
        selected_table,
        cause or "all",
        object_category or "all",
        temperature_formatter(scenario_temperature) if scenario_temperature is not None else "",
        days_ahead,
        selected_history_window,
    )
    return {
        "table_options": table_options,
        "selected_table": selected_table,
        "source_tables": source_tables,
        "source_table_notes": source_table_notes,
        "days_ahead": days_ahead,
        "selected_history_window": selected_history_window,
        "scenario_temperature": scenario_temperature,
        "cache_key": cache_key,
    }
