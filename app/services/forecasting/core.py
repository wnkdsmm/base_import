from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime
from typing import Callable

from app.perf import current_perf_trace, profiled
from app.plotly_bundle import get_plotly_bundle
from app.cache import build_immutable_payload_ttl_cache
from app.services.executive_brief import (
    build_executive_brief_from_risk_payload,
    compose_executive_brief_text,
)
from app.services.forecast_risk.core import build_decision_support_payload
from app.services.shared.request_state import (
    build_forecasting_cache_key as _build_forecasting_cache_key,
    build_forecasting_request_state as _build_forecasting_request_state_impl,
    emit_progress as _emit_forecasting_progress,
)
from config.db import engine

from .assembly import (
    build_forecasting_base_payload as _build_forecasting_base_payload_impl,
    build_forecasting_metadata_payload as _build_forecasting_metadata_payload_impl,
    complete_forecasting_decision_support_payload as _complete_forecasting_decision_support_payload_impl,
)
from .bootstrap import (
    _build_base_forecast_loading_message,
    _build_decision_support_followup_message,
    _build_forecasting_shell_data as _build_forecasting_shell_data_impl,
    _build_metadata_loading_message,
    _build_pending_decision_support_payload,
    _build_pending_executive_brief,
    _build_shell_risk_prediction,
    _build_slice_label,
)
from .charts import (
    _build_forecast_breakdown_chart,
    _build_forecast_chart,
    _build_geo_chart,
    _build_weekday_chart,
)
from .constants import FORECAST_DAY_OPTIONS, HISTORY_WINDOW_OPTIONS, SCENARIO_FORECAST_DESCRIPTION
from .data import (
    _build_daily_history_sql,
    _build_forecast_rows,
    _build_forecasting_table_options,
    _build_option_catalog_sql,
    _build_weekday_profile,
    _collect_forecasting_metadata,
    _count_forecasting_records_sql,
    _resolve_forecasting_selection,
    _selected_source_table_notes,
    _selected_source_tables,
    _temperature_quality_from_daily_history,
    clear_forecasting_sql_cache,
)
from .inputs import clear_forecasting_input_cache
from .payloads import _empty_forecasting_data
from .presentation import (
    _build_feature_cards,
    _build_feature_cards_with_quality,
    _build_insights,
    _build_notes,
    _build_summary,
)
from .quality import _build_scenario_quality_assessment, _run_scenario_backtesting
from .types import (
    ForecastPayload,
    ForecastingContext,
    ForecastingDeps,
    ForecastingPayload,
    ForecastingRequestState,
    TableOption,
)
from .utils import (
    _format_datetime,
    _format_float_for_input,
    _history_window_label,
    _parse_float,
    _parse_forecast_days,
    _parse_history_window,
    _resolve_option_value,
)

_FORECASTING_CACHE = build_immutable_payload_ttl_cache(ttl_seconds=120.0)


def clear_forecasting_cache() -> None:
    _FORECASTING_CACHE.clear()
    clear_forecasting_input_cache()
    clear_forecasting_sql_cache()


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


def _resolve_cached_forecasting_shell_payload(
    request_state: ForecastingRequestState,
    *,
    district: str,
    cause: str,
    object_category: str,
    temperature: str,
) -> ForecastPayload | None:
    source_tables = request_state["source_tables"]
    if not source_tables:
        return None
    selected_table = request_state["selected_table"]
    days_ahead = request_state["days_ahead"]
    selected_history_window = request_state["history_window"]
    full_cache_key = _build_forecasting_cache_key(
        selected_table,
        source_tables,
        district,
        cause,
        object_category,
        temperature,
        days_ahead,
        selected_history_window,
        True,
    )
    cached_payload = _FORECASTING_CACHE.get(full_cache_key)
    if cached_payload is not None:
        return cached_payload
    core_cache_key = _build_forecasting_cache_key(
        selected_table,
        source_tables,
        district,
        cause,
        object_category,
        temperature,
        days_ahead,
        selected_history_window,
        False,
    )
    return _FORECASTING_CACHE.get(core_cache_key)


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
    include_decision_support: bool = False,
) -> ForecastingRequestState:
    return _build_forecasting_request_state_impl(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
        include_decision_support=include_decision_support,
        table_options_builder=_build_forecasting_table_options,
        selection_resolver=_resolve_forecasting_selection,
        source_tables_resolver=_selected_source_tables,
        source_notes_resolver=_selected_source_table_notes,
        forecast_days_parser=_parse_forecast_days,
        history_window_parser=_parse_history_window,
    )


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


def get_forecasting_page_context(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> ForecastingContext:
    try:
        initial_data = get_forecasting_data(
            table_name=table_name,
            district=district,
            cause=cause,
            object_category=object_category,
            temperature=temperature,
            forecast_days=forecast_days,
            history_window=history_window,
        )
    except Exception as exc:
        request_state = _build_forecasting_request_state(
            table_name=table_name,
            district=district,
            cause=cause,
            object_category=object_category,
            temperature=temperature,
            forecast_days=forecast_days,
            history_window=history_window,
        )
        initial_data = _build_forecasting_page_fallback_initial_data(
            request_state,
            temperature=temperature,
            exc=exc,
        )
        initial_data["notes"].append(
            "РЎС‚СЂР°РЅРёС†Р° РїСЂРѕРіРЅРѕР·РёСЂРѕРІР°РЅРёСЏ РѕС‚РєСЂС‹С‚Р° РІ Р±РµР·РѕРїР°СЃРЅРѕРј СЂРµР¶РёРјРµ: С‡Р°СЃС‚СЊ СЂР°СЃС‡РµС‚Р° РІСЂРµРјРµРЅРЅРѕ РѕС‚РєР»СЋС‡РµРЅР° РёР·-Р·Р° РІРЅСѓС‚СЂРµРЅРЅРµР№ РѕС€РёР±РєРё."
        )
        initial_data["notes"].append(f"РўРµС…РЅРёС‡РµСЃРєР°СЏ РїСЂРёС‡РёРЅР°: {exc}")
        initial_data["model_description"] = (
            "РЎС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР· РІСЂРµРјРµРЅРЅРѕ РѕС‚РєСЂС‹С‚ Р±РµР· С‡Р°СЃС‚Рё СЂР°СЃС‡РµС‚РѕРІ, С‡С‚РѕР±С‹ СЌРєСЂР°РЅ РѕСЃС‚Р°РІР°Р»СЃСЏ РґРѕСЃС‚СѓРїРµРЅ. Р•РіРѕ Р·Р°РґР°С‡Р° РїРѕ-РїСЂРµР¶РЅРµРјСѓ С‚Р° Р¶Рµ: "
            "РїРѕРєР°Р·Р°С‚СЊ Р±Р»РёР¶Р°Р№С€РёРµ РґРЅРё СЂРёСЃРєР° Рё РЅРµ РїРѕРґРјРµРЅСЏС‚СЊ ML-РїСЂРѕРіРЅРѕР· РѕР¶РёРґР°РµРјРѕРіРѕ С‡РёСЃР»Р° РїРѕР¶Р°СЂРѕРІ."
        )
    return _build_forecasting_context(initial_data, plotly_js=get_plotly_bundle())


@profiled("forecasting.shell", engine=engine)
def get_forecasting_shell_context(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> ForecastingContext:
    perf = current_perf_trace()
    request_state = _build_forecasting_request_state(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
    )
    table_options = request_state["table_options"]
    selected_table = request_state["selected_table"]
    source_tables = request_state["source_tables"]
    days_ahead = request_state["days_ahead"]
    selected_history_window = request_state["history_window"]

    if perf is not None:
        perf.update(
            requested_table=table_name,
            requested_district=district,
            requested_cause=cause,
            requested_object_category=object_category,
            requested_temperature=temperature,
            forecast_horizon_days=days_ahead,
            history_window=selected_history_window,
            selected_table=selected_table,
            source_tables=len(source_tables),
            available_tables=len(table_options),
        )

    cached_payload = _resolve_cached_forecasting_shell_payload(
        request_state,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
    )
    if cached_payload is not None:
        if perf is not None:
            perf.update(
                cache_hit=True,
                payload_has_data=bool(cached_payload.get("has_data")),
                payload_notes=len(cached_payload.get("notes") or []),
                metadata_pending=bool(cached_payload.get("metadata_pending")),
                base_forecast_pending=bool(cached_payload.get("base_forecast_pending")),
            )
        return _build_forecasting_context(cached_payload, plotly_js="")

    if perf is not None:
        perf.update(cache_hit=False)

    try:
        with perf.span("filter_prep") if perf is not None else nullcontext():
            initial_data = _build_forecasting_shell_data(
                table_name=table_name,
                district=district,
                cause=cause,
                object_category=object_category,
                temperature=temperature,
                forecast_days=forecast_days,
                history_window=history_window,
            )
    except Exception as exc:
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
            "Р§Р°СЃС‚СЊ Р±С‹СЃС‚СЂРѕРіРѕ СЃС‚Р°СЂС‚РѕРІРѕРіРѕ РєРѕРЅС‚РµРєСЃС‚Р° РІСЂРµРјРµРЅРЅРѕ РЅРµРґРѕСЃС‚СѓРїРЅР°, РїРѕСЌС‚РѕРјСѓ СЃС‚СЂР°РЅРёС†Р° РѕС‚РєСЂС‹С‚Р° СЃ Р±РµР·РѕРїР°СЃРЅС‹РјРё placeholder-РґР°РЅРЅС‹РјРё."
        )
        initial_data["notes"].append(f"РўРµС…РЅРёС‡РµСЃРєР°СЏ РїСЂРёС‡РёРЅР°: {exc}")
        initial_data["model_description"] = SCENARIO_FORECAST_DESCRIPTION
        if perf is not None:
            perf.fail(
                exc,
                status="fallback",
                selected_table=selected_table,
                source_tables=len(source_tables),
                available_tables=len(table_options),
            )

    with perf.span("payload_render") if perf is not None else nullcontext():
        context = _build_forecasting_context(initial_data, plotly_js="")
        if perf is not None:
            perf.update(
                payload_has_data=bool(context["has_data"]),
                payload_notes=len(initial_data.get("notes") or []),
                metadata_pending=bool(initial_data.get("metadata_pending")),
                base_forecast_pending=bool(initial_data.get("base_forecast_pending")),
            )
        return context


@profiled("forecasting.metadata", engine=engine)
def get_forecasting_metadata(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> ForecastingPayload:
    perf = current_perf_trace()
    with perf.span("filter_prep") if perf is not None else nullcontext():
        metadata_payload = _build_forecasting_shell_data(
            table_name=table_name,
            district=district,
            cause=cause,
            object_category=object_category,
            temperature=temperature,
            forecast_days=forecast_days,
            history_window=history_window,
        )
        source_table_notes = _selected_source_table_notes(
            metadata_payload["filters"]["available_tables"],
            metadata_payload["filters"]["table_name"],
        )
        source_tables = _selected_source_tables(
            metadata_payload["filters"]["available_tables"],
            metadata_payload["filters"]["table_name"],
        )
        if perf is not None:
            perf.update(
                requested_table=table_name,
                selected_table=metadata_payload["filters"]["table_name"],
                source_tables=len(source_tables),
                available_tables=len(metadata_payload["filters"]["available_tables"]),
                history_window=metadata_payload["filters"]["history_window"],
            )

    if not source_tables:
        metadata_payload = _finalize_metadata_without_sources(metadata_payload)
        if perf is not None:
            perf.update(
                payload_has_data=False,
                payload_notes=len(metadata_payload.get("notes") or []),
            )
        return metadata_payload

    payload = _build_forecasting_metadata_payload_impl(
        metadata_payload,
        source_tables=source_tables,
        source_table_notes=source_table_notes,
        selected_history_window=metadata_payload["filters"]["history_window"],
        deps=_forecasting_assembly_dependencies(),
    )
    if perf is not None:
        perf.update(
            payload_has_data=bool(payload.get("filters", {}).get("available_tables")),
            payload_notes=len(payload.get("notes") or []),
        )
    return payload


@profiled("forecasting.base_forecast", engine=engine)
def get_forecasting_data(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
    include_decision_support: bool = True,
) -> ForecastingPayload:
    perf = current_perf_trace()
    request_state = _build_forecasting_request_state(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
        include_decision_support=include_decision_support,
    )
    table_options = request_state["table_options"]
    selected_table = request_state["selected_table"]
    source_tables = request_state["source_tables"]
    source_table_notes = request_state["source_table_notes"]
    days_ahead = request_state["days_ahead"]
    selected_history_window = request_state["history_window"]
    cache_key = request_state["cache_key"]
    temperature_value = _parse_float(temperature)

    if perf is not None:
        perf.update(
            requested_table=table_name,
            requested_district=district,
            requested_cause=cause,
            requested_object_category=object_category,
            requested_temperature=temperature,
            selected_table=selected_table,
            source_tables=len(source_tables),
            forecast_horizon_days=days_ahead,
            history_window=selected_history_window,
            include_decision_support=include_decision_support,
        )

    cached_payload = _FORECASTING_CACHE.get(cache_key)
    if cached_payload is not None:
        if perf is not None:
            perf.update(
                cache_hit=True,
                payload_has_data=bool(cached_payload.get("has_data")),
                payload_notes=len(cached_payload.get("notes") or []),
                input_rows=(cached_payload.get("summary") or {}).get("fires_count"),
            )
        return cached_payload

    if perf is not None:
        perf.update(cache_hit=False)

    base_data = _empty_forecasting_data(
        table_options,
        selected_table,
        days_ahead,
        temperature,
        selected_history_window,
    )
    if not source_tables:
        base_data["notes"].append("РќРµС‚ РґРѕСЃС‚СѓРїРЅС‹С… С‚Р°Р±Р»РёС† РґР»СЏ РїСЂРѕРіРЅРѕР·РёСЂРѕРІР°РЅРёСЏ.")
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
        if perf is not None:
            perf.update(
                payload_has_data=False,
                payload_notes=len(base_data.get("notes") or []),
            )
        _FORECASTING_CACHE.set(cache_key, base_data)
        return base_data

    payload = _build_forecasting_base_payload_impl(
        table_options=table_options,
        selected_table=selected_table,
        source_tables=source_tables,
        source_table_notes=source_table_notes,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        temperature_value=temperature_value,
        days_ahead=days_ahead,
        selected_history_window=selected_history_window,
        include_decision_support=include_decision_support,
        deps=_forecasting_assembly_dependencies(),
    )
    if perf is not None:
        perf.update(
            payload_has_data=bool(payload["has_data"]),
            payload_notes=len(payload.get("notes") or []),
            decision_support_pending=bool(payload.get("decision_support_pending")),
            decision_support_ready=bool(payload.get("decision_support_ready")),
            decision_support_error=bool(payload.get("decision_support_error")),
            input_rows=(payload.get("summary") or {}).get("fires_count_display"),
            forecast_rows=len(payload.get("forecast_rows") or []),
        )
    _FORECASTING_CACHE.set(cache_key, payload)
    return payload


def get_forecasting_decision_support_data(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
    progress_callback: Callable[[str, str], None] | None = None,
) -> ForecastingPayload:
    request_state = _build_forecasting_request_state(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
        include_decision_support=True,
    )
    cached_payload = _FORECASTING_CACHE.get(request_state["cache_key"])
    if cached_payload is not None:
        _emit_forecasting_progress(
            progress_callback,
            "forecasting_decision_support.completed",
            "Р‘Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№ СѓР¶Рµ Р±С‹Р» СЂР°СЃСЃС‡РёС‚Р°РЅ СЂР°РЅРµРµ Рё РІР·СЏС‚ РёР· РєСЌС€Р°.",
        )
        return cached_payload

    _emit_forecasting_progress(
        progress_callback,
        "forecasting_decision_support.loading",
        "РџРѕРґРЅРёРјР°РµРј Р±Р°Р·РѕРІС‹Р№ РїСЂРѕРіРЅРѕР· Рё РїРѕРґРіРѕС‚Р°РІР»РёРІР°РµРј РІС…РѕРґРЅС‹Рµ РґР°РЅРЅС‹Рµ РґР»СЏ Р±Р»РѕРєР° РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№.",
    )
    base_payload = get_forecasting_data(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
        include_decision_support=False,
    )
    payload = _complete_forecasting_decision_support_payload_impl(
        base_payload=base_payload,
        request_state=request_state,
        progress_callback=progress_callback,
        deps=_forecasting_assembly_dependencies(),
    )
    _FORECASTING_CACHE.set(request_state["cache_key"], payload)
    _emit_forecasting_progress(
        progress_callback,
        "forecasting_decision_support.completed",
        "Р‘Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№ РіРѕС‚РѕРІ Рё РїРѕРґСЃС‚Р°РІР»РµРЅ РІ РёС‚РѕРіРѕРІС‹Р№ РїСЂРѕРіРЅРѕР·.",
    )
    return payload


def _forecasting_assembly_dependencies() -> ForecastingDeps:
    return {
        "build_decision_support_followup_message": _build_decision_support_followup_message,
        "build_decision_support_payload": build_decision_support_payload,
        "build_executive_brief_from_risk_payload": build_executive_brief_from_risk_payload,
        "build_feature_cards": _build_feature_cards,
        "build_feature_cards_with_quality": _build_feature_cards_with_quality,
        "build_forecast_breakdown_chart": _build_forecast_breakdown_chart,
        "build_forecast_chart": _build_forecast_chart,
        "build_forecast_rows": _build_forecast_rows,
        "build_geo_chart": _build_geo_chart,
        "build_insights": _build_insights,
        "build_notes": _build_notes,
        "build_option_catalog_sql": _build_option_catalog_sql,
        "build_pending_decision_support_payload": _build_pending_decision_support_payload,
        "build_pending_executive_brief": _build_pending_executive_brief,
        "build_scenario_quality_assessment": _build_scenario_quality_assessment,
        "build_shell_risk_prediction": _build_shell_risk_prediction,
        "build_slice_label": _build_slice_label,
        "build_summary": _build_summary,
        "build_weekday_chart": _build_weekday_chart,
        "build_weekday_profile": _build_weekday_profile,
        "collect_forecasting_metadata": _collect_forecasting_metadata,
        "compose_executive_brief_text": compose_executive_brief_text,
        "count_forecasting_records_sql": _count_forecasting_records_sql,
        "emit_forecasting_progress": _emit_forecasting_progress,
        "forecast_day_options": FORECAST_DAY_OPTIONS,
        "format_datetime": _format_datetime,
        "format_float_for_input": _format_float_for_input,
        "history_window_label": _history_window_label,
        "history_window_options": HISTORY_WINDOW_OPTIONS,
        "resolve_option_value": _resolve_option_value,
        "run_scenario_backtesting": _run_scenario_backtesting,
        "scenario_forecast_description": SCENARIO_FORECAST_DESCRIPTION,
        "selected_source_tables": _selected_source_tables,
        "temperature_quality_from_daily_history": _temperature_quality_from_daily_history,
        "build_daily_history_sql": _build_daily_history_sql,
    }

