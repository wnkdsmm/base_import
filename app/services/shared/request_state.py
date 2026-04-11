from __future__ import annotations

from typing import Any, Callable, Dict, Sequence

TableOptionsBuilder = Callable[[], list[dict[str, Any]]]
SelectionResolver = Callable[[list[dict[str, Any]], str], str]
SourceTablesResolver = Callable[[list[dict[str, Any]], str], list[str]]
SourceNotesResolver = Callable[[list[dict[str, Any]], str], list[str]]
ForecastDaysParser = Callable[[str], int]
HistoryWindowParser = Callable[[str], str]


def normalize_cache_value(value: Any, *, fallback: str = "") -> str:
    normalized = str(value if value is not None else fallback).strip()
    if normalized:
        return normalized
    return fallback


def _build_table_request_context(
    *,
    table_name: str,
    forecast_days: str,
    history_window: str,
    table_options_builder: TableOptionsBuilder,
    selection_resolver: SelectionResolver,
    source_tables_resolver: SourceTablesResolver,
    source_notes_resolver: SourceNotesResolver,
    forecast_days_parser: ForecastDaysParser,
    history_window_parser: HistoryWindowParser,
) -> Dict[str, Any]:
    table_options = table_options_builder()
    selected_table = selection_resolver(table_options, table_name)
    requested_table = normalize_cache_value(table_name, fallback="all")
    if requested_table == "all":
        selected_table = "all"
    source_tables = source_tables_resolver(table_options, selected_table)
    return {
        "table_options": table_options,
        "selected_table": selected_table,
        "source_tables": source_tables,
        "source_table_notes": source_notes_resolver(table_options, selected_table),
        "days_ahead": forecast_days_parser(forecast_days),
        "resolved_history_window": history_window_parser(history_window),
    }


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
        normalize_cache_value(district),
        normalize_cache_value(cause),
        normalize_cache_value(object_category),
        normalize_cache_value(temperature),
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
    table_options_builder: TableOptionsBuilder,
    selection_resolver: SelectionResolver,
    source_tables_resolver: SourceTablesResolver,
    source_notes_resolver: SourceNotesResolver,
    forecast_days_parser: ForecastDaysParser,
    history_window_parser: HistoryWindowParser,
) -> Dict[str, Any]:
    state = _build_table_request_context(
        table_name=table_name,
        forecast_days=forecast_days,
        history_window=history_window,
        table_options_builder=table_options_builder,
        selection_resolver=selection_resolver,
        source_tables_resolver=source_tables_resolver,
        source_notes_resolver=source_notes_resolver,
        forecast_days_parser=forecast_days_parser,
        history_window_parser=history_window_parser,
    )
    resolved_history_window = state.pop("resolved_history_window")
    state["history_window"] = resolved_history_window
    state["cache_key"] = build_forecasting_cache_key(
        selected_table=state["selected_table"],
        source_tables=state["source_tables"],
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        days_ahead=state["days_ahead"],
        history_window=resolved_history_window,
        include_decision_support=include_decision_support,
    )
    return state


def build_ml_cache_key(
    *,
    cache_schema_version: int,
    selected_table: str,
    cause: str,
    object_category: str,
    temperature: str,
    days_ahead: int,
    history_window: str,
) -> tuple[Any, ...]:
    return (
        cache_schema_version,
        selected_table,
        normalize_cache_value(cause, fallback="all"),
        normalize_cache_value(object_category, fallback="all"),
        normalize_cache_value(temperature),
        days_ahead,
        history_window,
    )


def build_ml_request_state(
    *,
    table_name: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
    cache_schema_version: int,
    table_options_builder: TableOptionsBuilder,
    selection_resolver: SelectionResolver,
    source_tables_resolver: SourceTablesResolver,
    source_notes_resolver: SourceNotesResolver,
    forecast_days_parser: ForecastDaysParser,
    history_window_parser: HistoryWindowParser,
    temperature_parser: Callable[[str], float | None],
    temperature_formatter: Callable[[float], str],
) -> Dict[str, Any]:
    state = _build_table_request_context(
        table_name=table_name,
        forecast_days=forecast_days,
        history_window=history_window,
        table_options_builder=table_options_builder,
        selection_resolver=selection_resolver,
        source_tables_resolver=source_tables_resolver,
        source_notes_resolver=source_notes_resolver,
        forecast_days_parser=forecast_days_parser,
        history_window_parser=history_window_parser,
    )
    resolved_history_window = state.pop("resolved_history_window")
    scenario_temperature = temperature_parser(temperature)
    state["selected_history_window"] = resolved_history_window
    state["scenario_temperature"] = scenario_temperature
    state["cache_key"] = build_ml_cache_key(
        cache_schema_version=cache_schema_version,
        selected_table=state["selected_table"],
        cause=cause,
        object_category=object_category,
        temperature=temperature_formatter(scenario_temperature) if scenario_temperature is not None else "",
        days_ahead=state["days_ahead"],
        history_window=resolved_history_window,
    )
    return state


def emit_progress(
    progress_callback: Callable[[str, str], None] | None,
    phase: str,
    message: str,
) -> None:
    if progress_callback is None:
        return
    progress_callback(phase, message)
