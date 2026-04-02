from __future__ import annotations

import copy
from contextlib import nullcontext
from datetime import datetime
from statistics import mean
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.perf import current_perf_trace, profiled
from app.plotly_bundle import get_plotly_bundle
from app.runtime_cache import CopyingTtlCache
from app.services.executive_brief import (
    build_executive_brief_from_risk_payload,
    compose_executive_brief_text,
)
from app.services.forecast_risk.core import build_decision_support_payload
from config.db import engine

from .bootstrap import (
    _build_base_forecast_loading_message,
    _build_decision_support_followup_message,
    _build_forecasting_shell_data as _build_forecasting_shell_data_impl,
    _build_metadata_loading_message,
    _build_pending_decision_support_payload,
    _build_pending_executive_brief,
    _build_shell_filter_options,
    _build_shell_risk_prediction,
    _build_slice_label,
    _normalize_shell_filter_value,
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
from .payloads import _empty_forecasting_data
from .presentation import (
    _build_feature_cards,
    _build_feature_cards_with_quality,
    _build_insights,
    _build_notes,
    _build_summary,
)
from .quality import (
    _build_scenario_quality_assessment,
    _empty_forecast_quality_assessment,
    _run_scenario_backtesting,
    _scenario_baseline_expected_count,
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

_FORECASTING_CACHE = CopyingTtlCache(ttl_seconds=120.0)


def clear_forecasting_cache() -> None:
    _FORECASTING_CACHE.clear()
    clear_forecasting_sql_cache()


def _normalize_forecasting_cache_value(value: str) -> str:
    return str(value or "").strip()


def _build_forecasting_cache_key(
    selected_table: str,
    source_tables: List[str],
    district: str,
    cause: str,
    object_category: str,
    temperature: str,
    days_ahead: int,
    history_window: str,
    include_decision_support: bool,
) -> Tuple[str, ...]:
    return (
        selected_table,
        *tuple(source_tables),
        _normalize_forecasting_cache_value(district),
        _normalize_forecasting_cache_value(cause),
        _normalize_forecasting_cache_value(object_category),
        _normalize_forecasting_cache_value(temperature),
        str(days_ahead),
        history_window,
        "full" if include_decision_support else "core",
    )


def _build_forecasting_request_state(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
    include_decision_support: bool = False,
) -> Dict[str, Any]:
    table_options = _build_forecasting_table_options()
    selected_table = _resolve_forecasting_selection(table_options, table_name)
    source_tables = _selected_source_tables(table_options, selected_table)
    source_table_notes = _selected_source_table_notes(table_options, selected_table)
    days_ahead = _parse_forecast_days(forecast_days)
    selected_history_window = _parse_history_window(history_window)
    cache_key = _build_forecasting_cache_key(
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


def _emit_forecasting_progress(
    progress_callback: Callable[[str, str], None] | None,
    phase: str,
    message: str,
) -> None:
    if progress_callback is None:
        return
    progress_callback(phase, message)


def _build_forecasting_shell_data(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> Dict[str, Any]:
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
) -> Dict[str, Any]:
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
        table_options = _build_forecasting_table_options()
        selected_table = _resolve_forecasting_selection(table_options, table_name)
        days_ahead = _parse_forecast_days(forecast_days)
        selected_history_window = _parse_history_window(history_window)
        initial_data = _empty_forecasting_data(
            table_options,
            selected_table,
            days_ahead,
            temperature,
            selected_history_window,
        )
        initial_data["notes"].append(
            "Страница прогнозирования открыта в безопасном режиме: часть расчета временно отключена из-за внутренней ошибки."
        )
        initial_data["notes"].append(f"Техническая причина: {exc}")
        initial_data["model_description"] = (
            "Сценарный прогноз временно открыт без части расчётов, чтобы экран оставался доступен. Его задача по-прежнему та же: "
            "показать ближайшие дни риска и не подменять ML-прогноз ожидаемого числа пожаров."
        )
    return {
        "generated_at": _format_datetime(datetime.now()),
        "initial_data": initial_data,
        "plotly_js": get_plotly_bundle(),
        "has_data": bool(initial_data["filters"]["available_tables"]),
    }


@profiled("forecasting.shell", engine=engine)
def get_forecasting_shell_context(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> Dict[str, Any]:
    perf = current_perf_trace()
    days_ahead = _parse_forecast_days(forecast_days)
    selected_history_window = _parse_history_window(history_window)
    table_options = _build_forecasting_table_options()
    selected_table = _resolve_forecasting_selection(table_options, table_name)
    source_tables = _selected_source_tables(table_options, selected_table)
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
    if source_tables:
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
        if cached_payload is None:
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
            cached_payload = _FORECASTING_CACHE.get(core_cache_key)
        if cached_payload is not None:
            if perf is not None:
                perf.update(
                    cache_hit=True,
                    payload_has_data=bool(cached_payload.get("has_data")),
                    payload_notes=len(cached_payload.get("notes") or []),
                    metadata_pending=bool(cached_payload.get("metadata_pending")),
                    base_forecast_pending=bool(cached_payload.get("base_forecast_pending")),
                )
            return {
                "generated_at": _format_datetime(datetime.now()),
                "initial_data": cached_payload,
                "plotly_js": "",
                "has_data": bool(cached_payload["filters"]["available_tables"]),
            }
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
            "Часть быстрого стартового контекста временно недоступна, поэтому страница открыта с безопасными placeholder-данными."
        )
        initial_data["notes"].append(f"Техническая причина: {exc}")
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
        context = {
            "generated_at": _format_datetime(datetime.now()),
            "initial_data": initial_data,
            "plotly_js": "",
            "has_data": bool(initial_data["filters"]["available_tables"]),
        }
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
) -> Dict[str, Any]:
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
        metadata_payload["metadata_pending"] = False
        metadata_payload["metadata_ready"] = False
        metadata_payload["metadata_error"] = False
        metadata_payload["metadata_status_message"] = ""
        metadata_payload["loading"] = False
        metadata_payload["deferred"] = False
        metadata_payload["base_forecast_pending"] = False
        metadata_payload["loading_status_message"] = ""
        if perf is not None:
            perf.update(
                payload_has_data=False,
                payload_notes=len(metadata_payload.get("notes") or []),
            )
        return metadata_payload
    selected_history_window = metadata_payload["filters"]["history_window"]
    metadata_items, preload_notes = _collect_forecasting_metadata(source_tables)
    option_catalog = _build_option_catalog_sql(
        source_tables,
        history_window=selected_history_window,
        metadata_items=metadata_items,
    )
    selected_district = _resolve_option_value(
        option_catalog["districts"],
        metadata_payload["filters"]["district"],
    )
    selected_cause = _resolve_option_value(
        option_catalog["causes"],
        metadata_payload["filters"]["cause"],
    )
    selected_object_category = _resolve_option_value(
        option_catalog["object_categories"],
        metadata_payload["filters"]["object_category"],
    )
    feature_cards = _build_feature_cards(metadata_items)
    base_loading_message = "Фильтры и признаки готовы. Запускаем базовый прогноз."
    followup_message = _build_decision_support_followup_message()
    metadata_payload["generated_at"] = _format_datetime(datetime.now())
    metadata_payload["loading"] = True
    metadata_payload["deferred"] = True
    metadata_payload["metadata_pending"] = False
    metadata_payload["metadata_ready"] = True
    metadata_payload["metadata_error"] = False
    metadata_payload["metadata_status_message"] = "Фильтры и признаки готовы."
    metadata_payload["base_forecast_pending"] = True
    metadata_payload["base_forecast_ready"] = False
    metadata_payload["loading_status_message"] = base_loading_message
    metadata_payload["decision_support_pending"] = False
    metadata_payload["decision_support_ready"] = False
    metadata_payload["decision_support_error"] = False
    metadata_payload["decision_support_status_message"] = ""
    metadata_payload["features"] = feature_cards
    metadata_payload["summary"].update(
        {
            "slice_label": _build_slice_label(
                selected_district,
                selected_cause,
                selected_object_category,
            ),
            "history_period_label": "История загружается",
            "history_window_label": _history_window_label(selected_history_window),
        }
    )
    metadata_payload["quality_assessment"]["subtitle"] = (
        "Фильтры и признаки уже готовы. Теперь рассчитываем базовый прогноз, "
        "а метрики качества появятся вместе с ним."
    )
    metadata_payload["quality_assessment"]["dissertation_points"] = [
        metadata_payload["metadata_status_message"],
        base_loading_message,
    ]
    metadata_payload["risk_prediction"] = _build_shell_risk_prediction(
        table_options=metadata_payload["filters"]["available_tables"],
        selected_table=metadata_payload["filters"]["table_name"],
        forecast_days=int(metadata_payload["filters"]["forecast_days"]),
        temperature=metadata_payload["filters"]["temperature"],
        history_window=selected_history_window,
        feature_cards=feature_cards,
        message=base_loading_message,
    )
    metadata_payload["executive_brief"] = _build_pending_executive_brief(base_loading_message)
    metadata_payload["executive_brief"]["notes"] = source_table_notes + [
        metadata_payload["metadata_status_message"],
        base_loading_message,
        followup_message,
    ]
    metadata_payload["executive_brief"]["export_excerpt"] = base_loading_message
    metadata_payload["notes"] = list(
        dict.fromkeys(
            source_table_notes
            + preload_notes
            + [
                metadata_payload["metadata_status_message"],
                base_loading_message,
                followup_message,
            ]
        )
    )
    metadata_payload["filters"].update(
        {
            "district": selected_district,
            "cause": selected_cause,
            "object_category": selected_object_category,
            "available_districts": option_catalog["districts"],
            "available_causes": option_catalog["causes"],
            "available_object_categories": option_catalog["object_categories"],
        }
    )
    return metadata_payload


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
) -> Dict[str, Any]:
    perf = current_perf_trace()
    table_options = _build_forecasting_table_options()
    selected_table = _resolve_forecasting_selection(table_options, table_name)
    source_tables = _selected_source_tables(table_options, selected_table)
    source_table_notes = _selected_source_table_notes(table_options, selected_table)
    days_ahead = _parse_forecast_days(forecast_days)
    selected_history_window = _parse_history_window(history_window)
    temperature_value = _parse_float(temperature)
    cache_key = _build_forecasting_cache_key(
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
        base_data["notes"].append("Нет доступных таблиц для прогнозирования.")
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
        return _FORECASTING_CACHE.set(cache_key, base_data)
    with perf.span("filter_prep") if perf is not None else nullcontext():
        metadata_items, preload_notes = _collect_forecasting_metadata(source_tables)
        feature_cards = _build_feature_cards_with_quality(metadata_items)
        option_catalog = _build_option_catalog_sql(
            source_tables,
            history_window=selected_history_window,
            metadata_items=metadata_items,
        )
        selected_district = _resolve_option_value(option_catalog["districts"], district)
        selected_cause = _resolve_option_value(option_catalog["causes"], cause)
        selected_object_category = _resolve_option_value(option_catalog["object_categories"], object_category)
        if perf is not None:
            perf.update(
                metadata_tables=len(metadata_items),
                available_districts=len(option_catalog["districts"]),
                available_causes=len(option_catalog["causes"]),
                available_object_categories=len(option_catalog["object_categories"]),
                feature_cards=len(feature_cards),
            )
    with perf.span("aggregation") if perf is not None else nullcontext():
        filtered_records_count = _count_forecasting_records_sql(
            source_tables,
            history_window=selected_history_window,
            district=selected_district,
            cause=selected_cause,
            object_category=selected_object_category,
            metadata_items=metadata_items,
        )
        daily_history = _build_daily_history_sql(
            source_tables,
            history_window=selected_history_window,
            district=selected_district,
            cause=selected_cause,
            object_category=selected_object_category,
            metadata_items=metadata_items,
        )
        temperature_quality = _temperature_quality_from_daily_history(daily_history)
        feature_cards = _build_feature_cards_with_quality(
            metadata_items,
            temperature_quality=temperature_quality,
        )
        scenario_backtest = _run_scenario_backtesting(daily_history)
        quality_assessment = _build_scenario_quality_assessment(scenario_backtest)
        forecast_rows = _build_forecast_rows(daily_history, days_ahead, temperature_value)
        weekday_profile = _build_weekday_profile(daily_history)
        history_counts = [float(item["count"]) for item in daily_history]
        recent_counts = history_counts[-28:] if len(history_counts) >= 28 else history_counts
        recent_average = mean(recent_counts) if recent_counts else 0.0
        charts = {
            "daily": _build_forecast_chart(daily_history, forecast_rows),
            "breakdown": _build_forecast_breakdown_chart(forecast_rows, recent_average),
            "weekday": _build_weekday_chart(weekday_profile),
        }
        if perf is not None:
            perf.update(
                input_rows=filtered_records_count,
                history_days=len(daily_history),
                forecast_rows=len(forecast_rows),
                backtest_folds=(scenario_backtest.get("overview") or {}).get("folds"),
            )
    geo_prediction: Dict[str, Any] = {}
    decision_support_pending = not include_decision_support
    decision_support_ready = False
    decision_support_error = False
    decision_support_status_message = ""
    try:
        if not include_decision_support:
            raise RuntimeError("__decision_support_deferred__")
        risk_prediction = build_decision_support_payload(
            source_tables=source_tables,
            selected_district=selected_district,
            selected_cause=selected_cause,
            selected_object_category=selected_object_category,
            history_window=selected_history_window,
            planning_horizon_days=days_ahead,
        )
        risk_prediction["feature_cards"] = feature_cards
        geo_prediction = risk_prediction.get("geo_prediction") or {}
        decision_support_ready = True
        decision_support_status_message = "Блок поддержки решений и рекомендации готовы."
    except Exception as exc:
        risk_prediction = _empty_forecasting_data(
            table_options=table_options,
            selected_table=selected_table,
            forecast_days=days_ahead,
            temperature=temperature,
            history_window=selected_history_window,
        )["risk_prediction"]
        risk_prediction["feature_cards"] = feature_cards
        risk_prediction["notes"] = [
            "Блок поддержки решений по территориям временно недоступен, поэтому сейчас показан только сценарный прогноз по дням.",
            f"Техническая причина: {exc}",
        ]
        if str(exc) == "__decision_support_deferred__":
            decision_support_status_message = (
                "Базовый сценарный прогноз уже показан. Приоритеты территорий, паспорт качества и рекомендации догружаются фоном."
            )
            risk_prediction = _build_pending_decision_support_payload(
                table_options=table_options,
                selected_table=selected_table,
                forecast_days=days_ahead,
                temperature=temperature,
                history_window=selected_history_window,
                feature_cards=feature_cards,
                message=decision_support_status_message,
            )
        else:
            decision_support_error = True
            decision_support_status_message = (
                "Блок приоритетов территорий временно недоступен. Базовый сценарный прогноз показан без него."
            )
            risk_prediction = _build_pending_decision_support_payload(
                table_options=table_options,
                selected_table=selected_table,
                forecast_days=days_ahead,
                temperature=temperature,
                history_window=selected_history_window,
                feature_cards=feature_cards,
                message=decision_support_status_message,
            )
            risk_prediction["notes"].append(f"Техническая причина: {exc}")
    with perf.span("payload_render") if perf is not None else nullcontext():
        charts["geo"] = _build_geo_chart(geo_prediction)
        notes = list(
            dict.fromkeys(
                source_table_notes
                + preload_notes
                + _build_notes(
                    metadata=metadata_items,
                    filtered_records_count=filtered_records_count,
                    daily_history=daily_history,
                    temperature_value=temperature_value,
                )
            )
        )
        features = risk_prediction["feature_cards"] or feature_cards
        insights = _build_insights(daily_history, forecast_rows, weekday_profile)
        summary = _build_summary(
            selected_table=selected_table,
            selected_district=selected_district,
            selected_cause=selected_cause,
            selected_object_category=selected_object_category,
            temperature_value=temperature_value,
            daily_history=daily_history,
            filtered_records_count=filtered_records_count,
            forecast_rows=forecast_rows,
            history_window=selected_history_window,
        )
        generated_at = _format_datetime(datetime.now())
        executive_brief = _build_pending_executive_brief(
            decision_support_status_message or "Короткий вывод будет доступен после расчета блока поддержки решений."
        )
        if decision_support_ready:
            executive_brief = build_executive_brief_from_risk_payload(
                risk_prediction,
                notes=risk_prediction.get("notes"),
            )
        executive_brief["export_text"] = compose_executive_brief_text(
            executive_brief,
            scope_label=(
                f"Таблица: {summary['selected_table_label']} | История: {summary['history_window_label']} | "
                f"Срез: {summary['slice_label']} | Горизонт: {summary['forecast_days_display']} дней"
            ),
            generated_at=generated_at,
        )
        payload = {
            "generated_at": generated_at,
            "has_data": filtered_records_count > 0,
            "bootstrap_mode": "full" if decision_support_ready else "partial",
            "loading": False,
            "deferred": False,
            "metadata_pending": False,
            "metadata_ready": True,
            "metadata_error": False,
            "metadata_status_message": "Фильтры и признаки готовы.",
            "base_forecast_pending": False,
            "base_forecast_ready": True,
            "loading_status_message": "Базовый прогноз готов.",
            "decision_support_pending": decision_support_pending,
            "decision_support_ready": decision_support_ready,
            "decision_support_error": decision_support_error,
            "decision_support_status_message": decision_support_status_message,
            "model_description": SCENARIO_FORECAST_DESCRIPTION,
            "summary": summary,
            "quality_assessment": quality_assessment,
            "features": features,
            "risk_prediction": risk_prediction,
            "executive_brief": executive_brief,
            "insights": insights,
            "charts": charts,
            "forecast_rows": forecast_rows,
            "notes": notes,
            "filters": {
                "table_name": selected_table,
                "district": selected_district,
                "cause": selected_cause,
                "object_category": selected_object_category,
                "temperature": temperature if temperature_value is None else _format_float_for_input(temperature_value),
                "forecast_days": str(days_ahead),
                "history_window": selected_history_window,
                "available_tables": table_options,
                "available_districts": option_catalog["districts"],
                "available_causes": option_catalog["causes"],
                "available_object_categories": option_catalog["object_categories"],
                "available_forecast_days": [
                    {"value": str(option), "label": f"{option} дней"}
                    for option in FORECAST_DAY_OPTIONS
                ],
                "available_history_windows": HISTORY_WINDOW_OPTIONS,
            },
        }
        if perf is not None:
            perf.update(
                payload_has_data=bool(payload["has_data"]),
                payload_notes=len(notes),
                decision_support_pending=decision_support_pending,
                decision_support_ready=decision_support_ready,
                decision_support_error=decision_support_error,
            )
        return _FORECASTING_CACHE.set(cache_key, payload)


def get_forecasting_decision_support_data(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
    progress_callback: Callable[[str, str], None] | None = None,
) -> Dict[str, Any]:
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
            "Блок поддержки решений уже был рассчитан ранее и взят из кэша.",
        )
        return cached_payload
    _emit_forecasting_progress(
        progress_callback,
        "forecasting_decision_support.loading",
        "Поднимаем базовый прогноз и подготавливаем входные данные для блока поддержки решений.",
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
    filters = base_payload.get("filters") or {}
    available_tables = filters.get("available_tables") or request_state["table_options"]
    selected_table = str(filters.get("table_name") or request_state["selected_table"] or "all")
    source_tables = _selected_source_tables(available_tables, selected_table)
    if not source_tables:
        payload = copy.deepcopy(base_payload)
        payload["bootstrap_mode"] = "full"
        payload["decision_support_pending"] = False
        payload["decision_support_ready"] = True
        payload["decision_support_error"] = False
        payload["decision_support_status_message"] = ""
        return _FORECASTING_CACHE.set(request_state["cache_key"], payload)
    _emit_forecasting_progress(
        progress_callback,
        "forecasting_decision_support.aggregation",
        "Собираем ранжирование территорий, паспорт качества и историческую валидацию.",
    )
    risk_prediction = build_decision_support_payload(
        source_tables=source_tables,
        selected_district=str(filters.get("district") or "all"),
        selected_cause=str(filters.get("cause") or "all"),
        selected_object_category=str(filters.get("object_category") or "all"),
        history_window=str(filters.get("history_window") or request_state["history_window"]),
        planning_horizon_days=int(filters.get("forecast_days") or request_state["days_ahead"]),
        progress_callback=progress_callback,
    )
    _emit_forecasting_progress(
        progress_callback,
        "forecasting_decision_support.render",
        "Обновляем короткий вывод, рекомендации и карту риска.",
    )
    payload = copy.deepcopy(base_payload)
    generated_at = _format_datetime(datetime.now())
    charts = dict(payload.get("charts") or {})
    charts["geo"] = _build_geo_chart(risk_prediction.get("geo_prediction") or {})
    executive_brief = build_executive_brief_from_risk_payload(
        risk_prediction,
        notes=risk_prediction.get("notes"),
    )
    summary = payload.get("summary") or {}
    executive_brief["export_text"] = compose_executive_brief_text(
        executive_brief,
        scope_label=(
            f"Таблица: {summary.get('selected_table_label') or '-'} | "
            f"История: {summary.get('history_window_label') or '-'} | "
            f"Срез: {summary.get('slice_label') or '-'} | "
            f"Горизонт: {summary.get('forecast_days_display') or '-'} дней"
        ),
        generated_at=generated_at,
    )
    payload.update(
        generated_at=generated_at,
        bootstrap_mode="full",
        loading=False,
        deferred=False,
        metadata_pending=False,
        metadata_ready=True,
        metadata_error=False,
        metadata_status_message="Фильтры и признаки готовы.",
        base_forecast_pending=False,
        base_forecast_ready=True,
        loading_status_message="Базовый прогноз готов.",
        decision_support_pending=False,
        decision_support_ready=True,
        decision_support_error=False,
        decision_support_status_message="Блок поддержки решений и рекомендации готовы.",
        features=risk_prediction.get("feature_cards") or payload.get("features") or [],
        risk_prediction=risk_prediction,
        executive_brief=executive_brief,
        charts=charts,
    )
    result = _FORECASTING_CACHE.set(request_state["cache_key"], payload)
    _emit_forecasting_progress(
        progress_callback,
        "forecasting_decision_support.completed",
        "Блок поддержки решений готов и подставлен в итоговый прогноз.",
    )
    return result
