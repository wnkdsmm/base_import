from __future__ import annotations

from datetime import datetime

from app.plotly_bundle import get_plotly_bundle
from app.services.forecasting.types import ForecastPayload, ForecastingContext, ForecastingRequestState, TableOption
from app.services.forecasting.payloads import _empty_forecasting_data
from app.services.shared.request_state import (
    build_forecasting_cache_key as _build_forecasting_cache_key,
    build_forecasting_request_state as _build_forecasting_request_state_impl,
)

from .bootstrap import (
    _build_base_forecast_loading_message,
    _build_forecasting_shell_data as _build_forecasting_shell_data_impl,
    _build_metadata_loading_message,
)
from .data import (
    _build_forecasting_table_options,
    _resolve_forecasting_selection,
    _selected_source_table_notes,
    _selected_source_tables,
)
from .utils import (
    _format_datetime,
    _parse_optional_iso_date,
    _parse_forecast_days,
    _parse_history_window,
)

__all__ = [
    "_build_forecasting_context",
    "_build_forecasting_page_fallback_initial_data",
    "_build_forecasting_shell_fallback_initial_data",
    "_finalize_metadata_without_sources",
    "_build_no_source_forecasting_payload",
    "_build_forecasting_request_state",
    "_build_forecasting_shell_data",
]


def _build_forecasting_context(
    initial_data: ForecastPayload,
    *,
    plotly_js: str,
) -> ForecastingContext:
    return {
        "generated_at": _format_datetime(datetime.now()),
        "initial_data": initial_data,
        "plotly_js": plotly_js,
        "has_data": bool(initial_data["filters"]["available_tables"]),
    }


def _build_forecasting_page_fallback_initial_data(
    request_state: ForecastingRequestState,
    *,
    temperature: str,
    exc: Exception,
) -> ForecastPayload:
    return _empty_forecasting_data(
        request_state["table_options"],
        request_state["selected_table"],
        request_state["days_ahead"],
        temperature,
        request_state["history_window"],
    )


def _build_forecasting_shell_fallback_initial_data(
    *,
    table_options: list[TableOption],
    selected_table: str,
    days_ahead: int,
    temperature: str,
    selected_history_window: str,
    source_tables: list[str],
    exc: Exception,
) -> ForecastPayload:
    initial_data = _empty_forecasting_data(
        table_options,
        selected_table,
        days_ahead,
        temperature,
        selected_history_window,
    )
    if source_tables:
        initial_data["bootstrap_mode"] = "deferred"
        initial_data["loading"] = True
        initial_data["deferred"] = True
        initial_data["metadata_pending"] = True
        initial_data["metadata_ready"] = False
        initial_data["metadata_error"] = False
        initial_data["metadata_status_message"] = _build_metadata_loading_message()
        initial_data["base_forecast_pending"] = True
        initial_data["base_forecast_ready"] = False
        initial_data["loading_status_message"] = _build_base_forecast_loading_message()
        initial_data["notes"].append(initial_data["metadata_status_message"])
        initial_data["notes"].append(initial_data["loading_status_message"])
    initial_data["notes"].append(
        "\u0427\u0430\u0441\u0442\u044c \u0431\u044b\u0441\u0442\u0440\u043e\u0433\u043e \u0441\u0442\u0430\u0440\u0442\u043e\u0432\u043e\u0433\u043e \u043a\u043e\u043d\u0442\u0435\u043a\u0441\u0442\u0430 \u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0430, \u043f\u043e\u044d\u0442\u043e\u043c\u0443 \u0441\u0442\u0440\u0430\u043d\u0438\u0446\u0430 \u043e\u0442\u043a\u0440\u044b\u0442\u0430 \u0441 \u0431\u0435\u0437\u043e\u043f\u0430\u0441\u043d\u044b\u043c\u0438 placeholder-\u0434\u0430\u043d\u043d\u044b\u043c\u0438."
    )
    initial_data["notes"].append(f"\u0422\u0435\u0445\u043d\u0438\u0447\u0435\u0441\u043a\u0430\u044f \u043f\u0440\u0438\u0447\u0438\u043d\u0430: {exc}")
    initial_data["model_description"] = SCENARIO_FORECAST_DESCRIPTION
    return initial_data


def _finalize_metadata_without_sources(metadata_payload: ForecastPayload) -> ForecastPayload:
    metadata_payload["metadata_pending"] = False
    metadata_payload["metadata_ready"] = False
    metadata_payload["metadata_error"] = False
    metadata_payload["metadata_status_message"] = ""
    metadata_payload["loading"] = False
    metadata_payload["deferred"] = False
    metadata_payload["base_forecast_pending"] = False
    metadata_payload["loading_status_message"] = ""
    return metadata_payload


def _build_no_source_forecasting_payload(
    base_data: ForecastPayload,
    *,
    include_decision_support: bool,
) -> ForecastPayload:
    base_data["notes"].append("\u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b\u0445 \u0442\u0430\u0431\u043b\u0438\u0446 \u0434\u043b\u044f \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u044f.")
    base_data["bootstrap_mode"] = "full"
    base_data["loading"] = False
    base_data["deferred"] = False
    base_data["metadata_pending"] = False
    base_data["metadata_ready"] = False
    base_data["metadata_error"] = False
    base_data["metadata_status_message"] = ""
    base_data["base_forecast_pending"] = False
    base_data["base_forecast_ready"] = True
    base_data["loading_status_message"] = ""
    base_data["decision_support_pending"] = False
    base_data["decision_support_ready"] = include_decision_support
    base_data["decision_support_error"] = False
    base_data["decision_support_status_message"] = ""
    return base_data


def _build_forecasting_request_state(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
    current_user_date: str = "",
    include_decision_support: bool = False,
) -> ForecastingRequestState:
    parsed_current_user_date = _parse_optional_iso_date(current_user_date)
    normalized_current_user_date = (
        parsed_current_user_date.isoformat() if parsed_current_user_date is not None else ""
    )
    state = _build_forecasting_request_state_impl(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
        current_user_date=normalized_current_user_date,
        include_decision_support=include_decision_support,
        table_options_builder=_build_forecasting_table_options,
        selection_resolver=_resolve_forecasting_selection,
        source_tables_resolver=_selected_source_tables,
        source_notes_resolver=_selected_source_table_notes,
        forecast_days_parser=_parse_forecast_days,
        history_window_parser=_parse_history_window,
    )
    state["current_user_date"] = normalized_current_user_date
    state["current_user_day"] = parsed_current_user_date
    state["cache_key"] = _build_forecasting_cache_key(
        selected_table=state["selected_table"],
        source_tables=state["source_tables"],
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        days_ahead=state["days_ahead"],
        history_window=state["history_window"],
        current_user_date=normalized_current_user_date,
        include_decision_support=include_decision_support,
    )
    return state


def _build_forecasting_shell_data(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> ForecastPayload:
    return _build_forecasting_shell_data_impl(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
        table_options_builder=_build_forecasting_table_options,
        selection_resolver=_resolve_forecasting_selection,
        source_tables_resolver=_selected_source_tables,
        source_notes_resolver=_selected_source_table_notes,
        forecast_days_parser=_parse_forecast_days,
        history_window_parser=_parse_history_window,
    )
