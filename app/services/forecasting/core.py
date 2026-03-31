from __future__ import annotations

import copy
from contextlib import nullcontext
from datetime import datetime
from statistics import mean
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.perf import current_perf_trace, profiled
from app.services.executive_brief import (
    build_executive_brief_from_risk_payload,
    compose_executive_brief_text,
    empty_executive_brief,
)
from app.services.forecast_risk.core import build_decision_support_payload
from app.services.model_quality import compute_count_metrics
from app.plotly_bundle import get_plotly_bundle
from app.runtime_cache import CopyingTtlCache
from config.db import engine

from .charts import _build_forecast_breakdown_chart, _build_forecast_chart, _build_geo_chart, _build_weekday_chart, _empty_chart_bundle
from .constants import FORECAST_DAY_OPTIONS, HISTORY_WINDOW_OPTIONS
from .data import _build_daily_history_sql, _build_forecast_rows, _build_forecasting_table_options, _build_option_catalog_sql, _build_weekday_profile, _collect_forecasting_metadata, _count_forecasting_records_sql, _resolve_forecasting_selection, _selected_source_table_notes, _selected_source_tables, _table_selection_label, _temperature_quality_from_daily_history, clear_forecasting_sql_cache
from .utils import (
    _forecast_level_label,
    _forecast_stability_hint,
    _format_datetime,
    _format_float_for_input,
    _format_integer,
    _format_number,
    _format_percent,
    _format_period,
    _format_probability,
    _format_signed_percent,
    _history_window_label,
    _parse_float,
    _parse_forecast_days,
    _parse_history_window,
    _resolve_option_value,
)

SCENARIO_FORECAST_DESCRIPTION = (
    "Это эвристический сценарный прогноз: оценка по исторической частоте, сезонности и повторяющимся паттернам. "
    "Блок не обучается как ML-модель и нужен для ориентира по нагрузке, а не для точного обещания числа пожаров по дням."
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
            "Эвристический сценарный прогноз временно открыт без полного набора расчётов, чтобы интерфейс оставался доступен даже при внутренней ошибке."
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
        with (perf.span("filter_prep") if perf is not None else nullcontext()):
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
    with (perf.span("payload_render") if perf is not None else nullcontext()):
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


def _build_pending_decision_support_payload(
    *,
    table_options: List[Dict[str, str]],
    selected_table: str,
    forecast_days: int,
    temperature: str,
    history_window: str,
    feature_cards: List[Dict[str, str]],
    message: str,
) -> Dict[str, Any]:
    risk_prediction = _empty_forecasting_data(
        table_options=table_options,
        selected_table=selected_table,
        forecast_days=forecast_days,
        temperature=temperature,
        history_window=history_window,
    )["risk_prediction"]
    risk_prediction["feature_cards"] = feature_cards
    risk_prediction["top_territory_label"] = "Подготавливается"
    risk_prediction["top_territory_explanation"] = message
    risk_prediction["top_territory_confidence_label"] = "Подготавливается"
    risk_prediction["top_territory_confidence_score_display"] = "..."
    risk_prediction["top_territory_confidence_note"] = message
    risk_prediction["notes"] = [message]
    risk_prediction["quality_passport"]["confidence_label"] = "Подготавливается"
    risk_prediction["quality_passport"]["confidence_score_display"] = "..."
    risk_prediction["quality_passport"]["confidence_tone"] = "sand"
    risk_prediction["quality_passport"]["validation_label"] = "Фоновая догрузка"
    risk_prediction["quality_passport"]["validation_summary"] = message
    risk_prediction["quality_passport"]["reliability_notes"] = [message]
    risk_prediction["weight_profile"]["status_label"] = "Фоновая догрузка"
    risk_prediction["weight_profile"]["status_tone"] = "sand"
    risk_prediction["weight_profile"]["notes"] = [message]
    risk_prediction["historical_validation"]["status_label"] = "Фоновая догрузка"
    risk_prediction["historical_validation"]["status_tone"] = "sand"
    risk_prediction["historical_validation"]["summary"] = message
    risk_prediction["historical_validation"]["notes"] = [message]
    return risk_prediction


def _build_pending_executive_brief(message: str) -> Dict[str, Any]:
    executive_brief = empty_executive_brief()
    executive_brief["lead"] = message
    executive_brief["top_territory_label"] = "Подготавливается"
    executive_brief["priority_reason"] = message
    executive_brief["why_value"] = "Идет фоновый расчет"
    executive_brief["why_meta"] = "Причина приоритета появится после догрузки ranking-блока."
    executive_brief["action_label"] = "Подготавливается"
    executive_brief["action_detail"] = "Рекомендации появятся сразу после завершения фонового расчета."
    executive_brief["confidence_label"] = "Подготавливается"
    executive_brief["confidence_score_display"] = "..."
    executive_brief["confidence_tone"] = "sand"
    executive_brief["confidence_summary"] = "Паспорт качества и доверие к данным догружаются фоном."
    executive_brief["cards"] = [
        {
            "label": "Где риск выше",
            "value": "Подготавливается",
            "meta": message,
            "tone": "sky",
        },
        {
            "label": "Почему нужен приоритет",
            "value": "Идет фоновый расчет",
            "meta": "Причина приоритета появится после догрузки ranking-блока.",
            "tone": "sand",
        },
        {
            "label": "Доверие к данным",
            "value": "Подготавливается",
            "meta": "Паспорт качества догружается фоном.",
            "tone": "sand",
        },
        {
            "label": "Что сделать первым",
            "value": "Подготавливается",
            "meta": "Рекомендации появятся сразу после завершения фонового расчета.",
            "tone": "forest",
        },
    ]
    executive_brief["notes"] = [message]
    executive_brief["export_excerpt"] = message
    return executive_brief


def _build_slice_label(
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
) -> str:
    slice_parts = []
    if selected_district != "all":
        slice_parts.append(f"район: {selected_district}")
    if selected_cause != "all":
        slice_parts.append(f"причина: {selected_cause}")
    if selected_object_category != "all":
        slice_parts.append(f"категория: {selected_object_category}")
    return " | ".join(slice_parts) if slice_parts else "Все пожары выбранной истории"


def _normalize_shell_filter_value(value: str) -> str:
    normalized = str(value or "").strip()
    return normalized or "all"


def _build_shell_filter_options(selected_value: str, all_label: str) -> List[Dict[str, str]]:
    normalized = _normalize_shell_filter_value(selected_value)
    if normalized == "all":
        return [{"value": "all", "label": all_label}]
    return [
        {"value": normalized, "label": normalized},
        {"value": "all", "label": all_label},
    ]


def _build_metadata_loading_message() -> str:
    return (
        "Страница открыта в быстром режиме: сначала догружаем фильтры и доступные признаки, "
        "затем запускаем базовый прогноз, а после него блок поддержки решений."
    )


def _build_base_forecast_loading_message() -> str:
    return "Фильтры и признаки загружаются. Базовый прогноз стартует сразу после этого этапа."


def _build_decision_support_followup_message() -> str:
    return (
        "Третий этап запускается после базового прогноза: догружаются приоритеты территорий, "
        "паспорт качества и рекомендации."
    )


def _build_shell_risk_prediction(
    *,
    table_options: List[Dict[str, str]],
    selected_table: str,
    forecast_days: int,
    temperature: str,
    history_window: str,
    feature_cards: List[Dict[str, str]],
    message: str,
) -> Dict[str, Any]:
    followup_message = (
        "Приоритеты территорий, паспорт качества и рекомендации запустятся автоматически после базового прогноза."
    )
    risk_prediction = _empty_forecasting_data(
        table_options=table_options,
        selected_table=selected_table,
        forecast_days=forecast_days,
        temperature=temperature,
        history_window=history_window,
    )["risk_prediction"]
    risk_prediction["feature_cards"] = feature_cards
    risk_prediction["top_territory_label"] = "Подготавливаем прогноз"
    risk_prediction["top_territory_explanation"] = message
    risk_prediction["top_territory_confidence_label"] = "Ожидание"
    risk_prediction["top_territory_confidence_score_display"] = "..."
    risk_prediction["top_territory_confidence_note"] = followup_message
    risk_prediction["notes"] = [message, followup_message]
    risk_prediction["quality_passport"]["confidence_label"] = "Ожидание"
    risk_prediction["quality_passport"]["confidence_score_display"] = "..."
    risk_prediction["quality_passport"]["confidence_tone"] = "sand"
    risk_prediction["quality_passport"]["validation_label"] = "Ждет базовый прогноз"
    risk_prediction["quality_passport"]["validation_summary"] = followup_message
    risk_prediction["quality_passport"]["reliability_notes"] = [followup_message]
    risk_prediction["weight_profile"]["status_label"] = "Ждет базовый прогноз"
    risk_prediction["weight_profile"]["status_tone"] = "sand"
    risk_prediction["weight_profile"]["description"] = (
        "Профиль весов станет доступен после базового прогноза и последующей догрузки блока поддержки решений."
    )
    risk_prediction["weight_profile"]["notes"] = [followup_message]
    risk_prediction["weight_profile"]["calibration_notes"] = [followup_message]
    risk_prediction["historical_validation"]["status_label"] = "Ждет базовый прогноз"
    risk_prediction["historical_validation"]["status_tone"] = "sand"
    risk_prediction["historical_validation"]["summary"] = followup_message
    risk_prediction["historical_validation"]["notes"] = [followup_message]
    risk_prediction["geo_summary"]["compact_message"] = "Карта риска появится после базового прогноза."
    risk_prediction["geo_summary"]["coverage_display"] = "Подготавливается"
    risk_prediction["geo_summary"]["top_zone_label"] = "Подготавливается"
    risk_prediction["geo_summary"]["top_risk_display"] = "..."
    risk_prediction["geo_summary"]["hotspots_count_display"] = "..."
    risk_prediction["geo_summary"]["top_explanation"] = followup_message
    return risk_prediction


def _build_forecasting_shell_data(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> Dict[str, Any]:
    table_options = _build_forecasting_table_options()
    selected_table = _resolve_forecasting_selection(table_options, table_name)
    source_tables = _selected_source_tables(table_options, selected_table)
    source_table_notes = _selected_source_table_notes(table_options, selected_table)
    days_ahead = _parse_forecast_days(forecast_days)
    selected_history_window = _parse_history_window(history_window)
    temperature_value = _parse_float(temperature)
    requested_district = _normalize_shell_filter_value(district)
    requested_cause = _normalize_shell_filter_value(cause)
    requested_object_category = _normalize_shell_filter_value(object_category)

    shell_data = _empty_forecasting_data(
        table_options=table_options,
        selected_table=selected_table,
        forecast_days=days_ahead,
        temperature=temperature,
        history_window=selected_history_window,
    )
    if not source_tables:
        shell_data["notes"].extend(source_table_notes)
        shell_data["notes"].append("Нет доступных таблиц для прогнозирования.")
        return shell_data

    metadata_message = _build_metadata_loading_message()
    base_loading_message = _build_base_forecast_loading_message()
    followup_message = _build_decision_support_followup_message()

    shell_data["bootstrap_mode"] = "deferred"
    shell_data["loading"] = True
    shell_data["deferred"] = True
    shell_data["metadata_pending"] = True
    shell_data["metadata_ready"] = False
    shell_data["metadata_error"] = False
    shell_data["metadata_status_message"] = metadata_message
    shell_data["base_forecast_pending"] = True
    shell_data["base_forecast_ready"] = False
    shell_data["loading_status_message"] = base_loading_message
    shell_data["decision_support_pending"] = False
    shell_data["decision_support_ready"] = False
    shell_data["decision_support_error"] = False
    shell_data["decision_support_status_message"] = ""
    shell_data["model_description"] = SCENARIO_FORECAST_DESCRIPTION
    shell_data["summary"].update(
        {
            "selected_table_label": _table_selection_label(selected_table),
            "slice_label": _build_slice_label(requested_district, requested_cause, requested_object_category),
            "history_period_label": "История загружается",
            "history_window_label": _history_window_label(selected_history_window),
            "fires_count_display": "...",
            "history_days_display": "...",
            "active_days_display": "...",
            "last_observed_date": "-",
            "forecast_days_display": str(days_ahead),
            "predicted_total_display": "...",
            "predicted_average_display": "...",
            "average_probability_display": "...",
            "historical_average_display": "...",
            "recent_average_display": "...",
            "forecast_vs_recent_display": "...",
            "forecast_vs_recent_label": "Подготавливается",
            "active_days_share_display": "...",
            "peak_forecast_day_display": "-",
            "peak_forecast_value_display": "...",
            "peak_forecast_probability_display": "...",
            "temperature_scenario_display": f"{_format_number(temperature_value)} °C" if temperature_value is not None else "Историческая сезонность",
        }
    )
    shell_data["quality_assessment"]["subtitle"] = (
        "Сначала догружаем фильтры и признаки, затем считаем базовый прогноз и только после этого собираем метрики качества."
    )
    shell_data["quality_assessment"]["dissertation_points"] = [metadata_message, followup_message]
    shell_data["risk_prediction"] = _build_shell_risk_prediction(
        table_options=table_options,
        selected_table=selected_table,
        forecast_days=days_ahead,
        temperature=temperature,
        history_window=selected_history_window,
        feature_cards=[],
        message=metadata_message,
    )
    shell_data["executive_brief"] = _build_pending_executive_brief(metadata_message)
    shell_data["executive_brief"]["notes"] = source_table_notes + [metadata_message, base_loading_message, followup_message]
    shell_data["executive_brief"]["export_excerpt"] = metadata_message
    shell_data["charts"] = {
        "daily": _empty_chart_bundle("Что было и что ожидается", "Базовый прогноз появится после загрузки фильтров и признаков."),
        "breakdown": _empty_chart_bundle("Сценарная вероятность пожара по ближайшим дням", "Распределение по дням появится на втором этапе после загрузки фильтров и признаков."),
        "weekday": _empty_chart_bundle("В какие дни недели пожары случаются чаще", "Недельный профиль появится после базового прогноза."),
        "geo": _empty_chart_bundle("Карта блока поддержки решений", "Карта риска появится на третьем этапе после блока поддержки решений."),
    }
    shell_data["notes"] = source_table_notes + [metadata_message, base_loading_message, followup_message]
    shell_data["filters"] = {
        "table_name": selected_table,
        "district": requested_district,
        "cause": requested_cause,
        "object_category": requested_object_category,
        "temperature": temperature if temperature_value is None else _format_float_for_input(temperature_value),
        "forecast_days": str(days_ahead),
        "history_window": selected_history_window,
        "available_tables": table_options,
        "available_districts": _build_shell_filter_options(requested_district, "Все районы"),
        "available_causes": _build_shell_filter_options(requested_cause, "Все причины"),
        "available_object_categories": _build_shell_filter_options(requested_object_category, "Все категории"),
        "available_forecast_days": [{"value": str(option), "label": f"{option} дней"} for option in FORECAST_DAY_OPTIONS],
        "available_history_windows": HISTORY_WINDOW_OPTIONS,
    }
    return shell_data


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
    with (perf.span("filter_prep") if perf is not None else nullcontext()):
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
            perf.update(payload_has_data=False, payload_notes=len(metadata_payload.get("notes") or []))
        return metadata_payload

    selected_history_window = metadata_payload["filters"]["history_window"]
    metadata_items, preload_notes = _collect_forecasting_metadata(source_tables)
    option_catalog = _build_option_catalog_sql(
        source_tables,
        history_window=selected_history_window,
        metadata_items=metadata_items,
    )
    selected_district = _resolve_option_value(option_catalog["districts"], metadata_payload["filters"]["district"])
    selected_cause = _resolve_option_value(option_catalog["causes"], metadata_payload["filters"]["cause"])
    selected_object_category = _resolve_option_value(option_catalog["object_categories"], metadata_payload["filters"]["object_category"])
    temperature_quality = _temperature_quality_from_daily_history(
        _build_daily_history_sql(
            source_tables,
            history_window=selected_history_window,
            district=selected_district,
            cause=selected_cause,
            object_category=selected_object_category,
            metadata_items=metadata_items,
        )
    )
    feature_cards = _build_feature_cards_with_quality(metadata_items, temperature_quality=temperature_quality)
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
            "slice_label": _build_slice_label(selected_district, selected_cause, selected_object_category),
            "history_period_label": "История загружается",
            "history_window_label": _history_window_label(selected_history_window),
        }
    )
    metadata_payload["quality_assessment"]["subtitle"] = (
        "Фильтры и признаки уже готовы. Теперь рассчитываем базовый прогноз, а метрики качества появятся вместе с ним."
    )
    metadata_payload["quality_assessment"]["dissertation_points"] = [metadata_payload["metadata_status_message"], base_loading_message]
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
    base_data = _empty_forecasting_data(table_options, selected_table, days_ahead, temperature, selected_history_window)
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
            perf.update(payload_has_data=False, payload_notes=len(base_data.get("notes") or []))
        return _FORECASTING_CACHE.set(cache_key, base_data)

    with (perf.span("filter_prep") if perf is not None else nullcontext()):
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

    with (perf.span("aggregation") if perf is not None else nullcontext()):
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
        feature_cards = _build_feature_cards_with_quality(metadata_items, temperature_quality=temperature_quality)
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
            "Блок поддержки решений по территории временно недоступен, поэтому страница показывает только эвристический сценарный прогноз.",
            f"Техническая причина: {exc}",
        ]
        if str(exc) == "__decision_support_deferred__":
            decision_support_status_message = (
                "Базовый сценарный прогноз уже показан. "
                "Приоритеты территорий, паспорт качества и рекомендации догружаются фоном."
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
                "Блок приоритетов территорий временно недоступен. "
                "Базовый сценарный прогноз показан без него."
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
    with (perf.span("payload_render") if perf is not None else nullcontext()):
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
            decision_support_status_message or "Управленческий бриф будет доступен после расчета ranking-блока."
        )
        if decision_support_ready:
            executive_brief = build_executive_brief_from_risk_payload(
                risk_prediction,
                notes=risk_prediction.get("notes"),
            )
        executive_brief["export_text"] = compose_executive_brief_text(
            executive_brief,
            scope_label=(
                f"Таблица: {summary['selected_table_label']} | "
                f"История: {summary['history_window_label']} | "
                f"Срез: {summary['slice_label']} | "
                f"Горизонт: {summary['forecast_days_display']} дней"
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
                "available_forecast_days": [{"value": str(option), "label": f"{option} дней"} for option in FORECAST_DAY_OPTIONS],
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
        "Собираем ranking территорий, паспорт качества и историческую валидацию.",
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
        "Обновляем управленческий бриф, рекомендации и карту риска.",
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


def _scenario_baseline_expected_count(history: List[Dict[str, Any]], target_date: Any) -> float:
    if not history:
        return 0.0
    history_counts = [float(item["count"]) for item in history]
    recent_mean = mean(history_counts[-28:]) if history_counts else 0.0
    same_weekday = [float(item["count"]) for item in history if item["date"].weekday() == target_date.weekday()]
    if len(same_weekday) >= 3:
        return max(0.0, 0.6 * mean(same_weekday[-8:]) + 0.4 * recent_mean)
    return max(0.0, recent_mean)


def _run_scenario_backtesting(daily_history: List[Dict[str, Any]]) -> Dict[str, Any]:
    if len(daily_history) < 30:
        return {
            "is_ready": False,
            "message": "Для сценарного прогноза пока мало непрерывной дневной истории: проверка на истории включается примерно от 30 дней ряда.",
            "rows": [],
            "model_metrics": {},
            "baseline_metrics": {},
            "overview": {"folds": 0, "min_train_days": 0, "validation_horizon_days": 1},
        }

    min_train_days = min(28, max(14, len(daily_history) // 2))
    available_points = len(daily_history) - min_train_days
    if available_points < 8:
        return {
            "is_ready": False,
            "message": "Для сценарного прогноза пока недостаточно одношаговых исторических окон: нужно хотя бы 8 окон для проверки на истории.",
            "rows": [],
            "model_metrics": {},
            "baseline_metrics": {},
            "overview": {"folds": 0, "min_train_days": min_train_days, "validation_horizon_days": 1},
        }

    start_index = len(daily_history) - min(45, available_points)
    rows: List[Dict[str, Any]] = []
    for index in range(start_index, len(daily_history)):
        train_history = daily_history[:index]
        actual_row = daily_history[index]
        if not train_history:
            continue
        forecast_row = _build_forecast_rows(train_history, 1, None)
        if not forecast_row:
            continue
        point_forecast = forecast_row[0]
        baseline_count = _scenario_baseline_expected_count(train_history, actual_row["date"])
        rows.append(
            {
                "date": actual_row["date"].isoformat(),
                "actual_count": float(actual_row["count"]),
                "predicted_count": float(point_forecast.get("forecast_value", 0.0)),
                "baseline_count": float(baseline_count),
            }
        )

    if len(rows) < 8:
        return {
            "is_ready": False,
            "message": "Не удалось собрать достаточно исторических проверок для сценарного прогноза.",
            "rows": rows,
            "model_metrics": {},
            "baseline_metrics": {},
            "overview": {"folds": len(rows), "min_train_days": min_train_days, "validation_horizon_days": 1},
        }

    actuals = [row["actual_count"] for row in rows]
    predictions = [row["predicted_count"] for row in rows]
    baseline_predictions = [row["baseline_count"] for row in rows]
    baseline_metrics = compute_count_metrics(actuals, baseline_predictions)
    model_metrics = compute_count_metrics(actuals, predictions, baseline_metrics)
    return {
        "is_ready": True,
        "message": "",
        "rows": rows,
        "model_metrics": model_metrics,
        "baseline_metrics": baseline_metrics,
        "overview": {"folds": len(rows), "min_train_days": min_train_days, "validation_horizon_days": 1},
    }


def _empty_forecast_quality_assessment() -> Dict[str, Any]:
    return {
        "ready": False,
        "title": "Оценка качества сценарного прогноза",
        "subtitle": "После накопления истории здесь появится проверка на истории и сравнение с базовой моделью.",
        "metric_cards": [],
        "methodology_items": [],
        "comparison_rows": [],
        "dissertation_points": ["Истории пока недостаточно, чтобы подтвердить качество эвристического сценарного прогноза через проверку на истории."],
    }


def _build_scenario_quality_assessment(backtest: Dict[str, Any]) -> Dict[str, Any]:
    if not backtest.get("is_ready"):
        payload = _empty_forecast_quality_assessment()
        message = backtest.get("message")
        if message:
            payload["dissertation_points"] = [message]
        return payload

    model_metrics = backtest.get("model_metrics", {})
    baseline_metrics = backtest.get("baseline_metrics", {})
    overview = backtest.get("overview", {})
    comparison_rows = [
        {
            "method_label": "Сезонная базовая модель",
            "role_label": "Базовая модель",
            "mae_display": _format_number(baseline_metrics.get("mae")),
            "rmse_display": _format_number(baseline_metrics.get("rmse")),
            "smape_display": f"{_format_number(baseline_metrics.get('smape'))}%",
            "selection_label": "Опорная линия",
            "mae_delta_display": "0%",
        },
        {
            "method_label": "Сценарный прогноз",
            "role_label": "Эвристическая модель",
            "mae_display": _format_number(model_metrics.get("mae")),
            "rmse_display": _format_number(model_metrics.get("rmse")),
            "smape_display": f"{_format_number(model_metrics.get('smape'))}%",
            "selection_label": "Рабочая модель",
            "mae_delta_display": _format_signed_percent(model_metrics.get("mae_delta_vs_baseline")) if model_metrics.get("mae_delta_vs_baseline") is not None else "—",
        },
    ]
    return {
        "ready": True,
        "title": "Оценка качества сценарного прогноза",
        "subtitle": "Эвристический сценарный прогноз проверяется через скользящую одношаговую проверку на истории без использования будущих наблюдений.",
        "metric_cards": [
            {"label": "MAE", "value": _format_number(model_metrics.get("mae")), "meta": f"базовая модель: {_format_number(baseline_metrics.get('mae'))}"},
            {"label": "RMSE", "value": _format_number(model_metrics.get("rmse")), "meta": f"базовая модель: {_format_number(baseline_metrics.get('rmse'))}"},
            {"label": "SMAPE", "value": f"{_format_number(model_metrics.get('smape'))}%", "meta": f"базовая модель: {_format_number(baseline_metrics.get('smape'))}%"},
            {"label": "MAE к базовой модели", "value": _format_signed_percent(model_metrics.get("mae_delta_vs_baseline")) if model_metrics.get("mae_delta_vs_baseline") is not None else "—", "meta": "отрицательное значение лучше базовой модели"},
        ],
        "methodology_items": [
            {"label": "Схема валидации", "value": "Скользящая одношаговая проверка на истории", "meta": "каждое окно использует только прошлую историю"},
            {"label": "Окон проверки", "value": _format_integer(overview.get("folds") or 0), "meta": "одношаговые окна"},
            {"label": "Минимум обучающего окна", "value": _format_integer(overview.get("min_train_days") or 0), "meta": "дней истории на окно"},
            {"label": "Горизонт", "value": _format_integer(overview.get("validation_horizon_days") or 1), "meta": "день вперёд"},
        ],
        "comparison_rows": comparison_rows,
        "dissertation_points": [
            f"Качество эвристического сценарного прогноза оценено через скользящую одношаговую проверку на истории на {_format_integer(overview.get('folds') or 0)} исторических окнах.",
            f"Сценарный прогноз показал MAE {_format_number(model_metrics.get('mae'))}, RMSE {_format_number(model_metrics.get('rmse'))} и SMAPE {_format_number(model_metrics.get('smape'))}%.",
            f"Сезонная базовая модель на тех же окнах дала MAE {_format_number(baseline_metrics.get('mae'))}, RMSE {_format_number(baseline_metrics.get('rmse'))} и SMAPE {_format_number(baseline_metrics.get('smape'))}%.",
            f"Изменение MAE относительно базовой модели составило {_format_signed_percent(model_metrics.get('mae_delta_vs_baseline')) if model_metrics.get('mae_delta_vs_baseline') is not None else '—'}, что позволяет количественно сравнивать сценарный блок с опорной сезонной моделью.",
        ],
    }

def _empty_forecasting_data(
    table_options: List[Dict[str, str]],
    selected_table: str,
    forecast_days: int,
    temperature: str,
    history_window: str,
) -> Dict[str, Any]:
    return {
        "generated_at": _format_datetime(datetime.now()),
        "has_data": False,
        "bootstrap_mode": "empty",
        "loading": False,
        "deferred": False,
        "metadata_pending": False,
        "metadata_ready": False,
        "metadata_error": False,
        "metadata_status_message": "",
        "base_forecast_pending": False,
        "base_forecast_ready": False,
        "loading_status_message": "",
        "decision_support_pending": False,
        "decision_support_ready": False,
        "decision_support_error": False,
        "decision_support_status_message": "",
        "model_description": "",
        "summary": {
            "selected_table_label": _table_selection_label(selected_table),
            "slice_label": "Все пожары",
            "history_period_label": "Нет данных",
            "history_window_label": _history_window_label(history_window),
            "fires_count_display": "0",
            "history_days_display": "0",
            "active_days_display": "0",
            "last_observed_date": "-",
            "forecast_days_display": str(forecast_days),
            "predicted_total_display": "0",
            "predicted_average_display": "0",
            "average_probability_display": "0%",
            "historical_average_display": "0",
            "recent_average_display": "0",
            "forecast_vs_recent_display": "0%",
            "forecast_vs_recent_label": "Нет сравнения",
            "active_days_share_display": "0%",
            "peak_forecast_day_display": "-",
            "peak_forecast_value_display": "0",
            "peak_forecast_probability_display": "0%",
            "temperature_scenario_display": temperature.strip() or "Историческая сезонность",
        },
        "quality_assessment": _empty_forecast_quality_assessment(),
        "features": [],
        "risk_prediction": {
            "has_data": False,
            "title": "Блок поддержки решений: ранжирование территорий",
            "model_description": "Это отдельный блок поддержки решений: он прозрачно раскладывает риск территории на частоту пожаров, тяжесть последствий, долгое прибытие и дефицит водоснабжения, а логистический блок объясняет travel-time, покрытие ПЧ и сервисную зону.",
            "coverage_display": "0 из 0",
            "quality_passport": {
                "title": "Паспорт качества данных",
                "confidence_score": 0,
                "confidence_score_display": "0 / 100",
                "confidence_label": "Ограниченная",
                "confidence_tone": "fire",
                "validation_label": "Валидация ограничена",
                "validation_summary": "Паспорт качества появится после расчета блока поддержки решений.",
                "table_count_display": "0",
                "used_count_display": "0",
                "partial_count_display": "0",
                "missing_count_display": "0",
                "critical_gaps": [],
                "used_labels": [],
                "partial_labels": [],
                "missing_labels": [],
                "reliability_notes": ["Паспорт качества появится после расчета блока поддержки решений."],
            },
            "summary_cards": [],
            "top_territory_label": "-",
            "top_territory_explanation": "Недостаточно данных для ранжирования территорий.",
            "territories": [],
            "feature_cards": [],
            "weight_profile": {
                "mode": "expert",
                "mode_label": "Экспертные веса",
                "status_label": "Активный профиль",
                "status_tone": "forest",
                "description": "После расчета здесь появится прозрачная схема компонентных весов по территориям.",
                "components": [],
                "available_modes": [],
                "notes": ["Веса и сельская поправка появятся после расчета блока поддержки решений."],
                "calibration_ready": True,
                "calibration_targets": [],
                "calibration_notes": ["После расчета здесь появится заготовка под калибровку весов."],
            },
            "historical_validation": {
                "title": "Черновая историческая проверка ранжирования",
                "mode_label": "Экспертные веса",
                "has_metrics": False,
                "status_label": "Пока без проверки",
                "status_tone": "fire",
                "summary": "После расчёта здесь появится заготовка под историческую проверку ранжирования.",
                "metric_cards": [
                    {"label": "Окон оценено", "value": "0", "meta": "Нет данных"},
                    {"label": "Top-1 hit", "value": "0%", "meta": "Нет данных"},
                    {"label": "Top-3 capture", "value": "0%", "meta": "Нет данных"},
                    {"label": "Precision@3", "value": "0%", "meta": "Нет данных"},
                    {"label": "NDCG@3", "value": "0", "meta": "Нет данных"},
                ],
                "notes": ["Метрики появятся автоматически, когда истории станет достаточно."],
                "recent_windows": [],
            },
            "notes": [],
            "geo_summary": {
                "has_coordinates": False,
                "has_map_points": False,
                "compact_message": "В выбранном срезе нет координат, поэтому карта блока поддержки решений сейчас не строится.",
                "model_description": "Если в данных есть координаты, карта блока поддержки решений показывает зоны, где пожары повторялись чаще и ближе по времени.",
                "coverage_display": "0 с координатами",
                "top_zone_label": "-",
                "top_risk_display": "0 / 100",
                "hotspots_count_display": "0",
                "top_explanation": "Нет данных для объяснения зоны риска.",
                "hotspots": [],
                "districts": [],
            },
        },
        "executive_brief": empty_executive_brief(),
        "insights": [],
        "charts": {
            "daily": _empty_chart_bundle("Что было и что ожидается", "Недостаточно данных для построения прогноза."),
            "breakdown": _empty_chart_bundle("Сценарная вероятность пожара по ближайшим дням", "Нет данных для ближайших дней."),
            "weekday": _empty_chart_bundle("В какие дни недели пожары случаются чаще", "Нет данных по дням недели."),
            "geo": _empty_chart_bundle("Карта блока поддержки решений", "В выбранном срезе нет координат, поэтому карта не построена."),
        },
        "forecast_rows": [],
        "notes": [],
        "filters": {
            "table_name": selected_table,
            "district": "all",
            "cause": "all",
            "object_category": "all",
            "temperature": temperature,
            "forecast_days": str(forecast_days),
            "history_window": history_window,
            "available_tables": table_options,
            "available_districts": [{"value": "all", "label": "Все районы"}],
            "available_causes": [{"value": "all", "label": "Все причины"}],
            "available_object_categories": [{"value": "all", "label": "Все категории"}],
            "available_forecast_days": [{"value": str(option), "label": f"{option} дней"} for option in FORECAST_DAY_OPTIONS],
            "available_history_windows": HISTORY_WINDOW_OPTIONS,
        },
    }
def _build_summary(
    selected_table: str,
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    temperature_value: Optional[float],
    daily_history: List[Dict[str, Any]],
    filtered_records_count: int,
    forecast_rows: List[Dict[str, Any]],
    history_window: str,
) -> Dict[str, str]:
    history_dates = [item["date"] for item in daily_history]
    history_counts = [float(item["count"]) for item in daily_history]
    active_days = sum(1 for item in daily_history if item["count"] > 0)
    predicted_total = sum(item["forecast_value"] for item in forecast_rows)
    predicted_average = predicted_total / len(forecast_rows) if forecast_rows else 0.0
    average_probability = mean(float(item.get("fire_probability", 0.0)) for item in forecast_rows) if forecast_rows else 0.0
    historical_average = mean(history_counts) if history_counts else 0.0
    recent_counts = history_counts[-28:] if len(history_counts) >= 28 else history_counts
    recent_average = mean(recent_counts) if recent_counts else historical_average
    active_days_share = (active_days / len(daily_history) * 100.0) if daily_history else 0.0
    delta_ratio = ((predicted_average - recent_average) / recent_average) if recent_average > 0 else 0.0
    scenario_label, _scenario_tone = _forecast_level_label(predicted_average, recent_average if recent_average > 0 else historical_average)
    peak_row = max(forecast_rows, key=lambda item: float(item["forecast_value"])) if forecast_rows else None

    return {
        "selected_table_label": _table_selection_label(selected_table),
        "slice_label": _build_slice_label(selected_district, selected_cause, selected_object_category),
        "history_period_label": _format_period(history_dates),
        "history_window_label": _history_window_label(history_window),
        "fires_count_display": _format_integer(filtered_records_count),
        "history_days_display": _format_integer(len(daily_history)),
        "active_days_display": _format_integer(active_days),
        "active_days_share_display": _format_percent(active_days_share),
        "last_observed_date": history_dates[-1].strftime("%d.%m.%Y") if history_dates else "-",
        "forecast_days_display": _format_integer(len(forecast_rows)),
        "predicted_total_display": _format_number(predicted_total),
        "predicted_average_display": _format_number(predicted_average),
        "average_probability_display": _format_probability(average_probability),
        "historical_average_display": _format_number(historical_average),
        "recent_average_display": _format_number(recent_average),
        "forecast_vs_recent_display": _format_signed_percent(delta_ratio),
        "forecast_vs_recent_label": scenario_label,
        "peak_forecast_day_display": peak_row["date_display"] if peak_row else "-",
        "peak_forecast_value_display": peak_row["forecast_value_display"] if peak_row else "0",
        "peak_forecast_probability_display": peak_row["fire_probability_display"] if peak_row else "0%",
        "temperature_scenario_display": f"{_format_number(temperature_value)} °C" if temperature_value is not None else "Историческая сезонность",
    }

def _build_feature_cards(metadata: Any) -> List[Dict[str, str]]:
    metadata_items = metadata if isinstance(metadata, list) else [metadata]
    metadata_items = [item for item in metadata_items if item]
    if not metadata_items:
        return []

    total_tables = len(metadata_items)
    feature_config = [
        ("date", "Дата возникновения пожара", "Нужна для построения дневного временного ряда."),
        ("temperature", "Температура", "Используется для температурной поправки, если колонка найдена."),
        ("cause", "Причина", "Позволяет строить сценарий по конкретной причине пожара."),
        ("object_category", "Категория объекта", "Позволяет строить сценарий по типу объекта."),
    ]
    cards = []
    for key, label, description in feature_config:
        sources = []
        found = 0
        for item in metadata_items:
            source_column = item["resolved_columns"].get(key)
            if source_column:
                found += 1
                sources.append(f"{item['table_name']}: {source_column}")
        if found == total_tables:
            status = "used"
            status_label = "Используется"
        elif found > 0:
            status = "partial"
            status_label = f"Частично ({found}/{total_tables})"
        else:
            status = "missing"
            status_label = "Не найдена"
        cards.append(
            {
                "label": label,
                "status": status,
                "status_label": status_label,
                "source": "; ".join(sources[:3]) if sources else "Не найдена",
                "description": description,
            }
        )
    return cards


def _build_feature_cards_with_quality(
    metadata: Any,
    temperature_quality: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    metadata_items = metadata if isinstance(metadata, list) else [metadata]
    metadata_items = [item for item in metadata_items if item]
    if not metadata_items:
        return []

    total_tables = len(metadata_items)
    feature_config = [
        ("date", "Дата возникновения пожара", "Нужна для построения дневного временного ряда."),
        ("temperature", "Температура", "Используется для температурной поправки, если колонка найдена."),
        ("cause", "Причина", "Позволяет строить сценарий по конкретной причине пожара."),
        ("object_category", "Категория объекта", "Позволяет строить сценарий по типу объекта."),
    ]
    cards: List[Dict[str, Any]] = []
    for key, label, base_description in feature_config:
        description = base_description
        sources: List[str] = []
        found = 0
        quality_items: List[Dict[str, Any]] = []
        for item in metadata_items:
            source_column = (item.get("resolved_columns") or {}).get(key)
            if not source_column:
                continue
            found += 1
            quality = ((item.get("column_quality") or {}).get(key) or {}) if key == "temperature" else {}
            coverage_suffix = ""
            if key == "temperature" and temperature_quality is None:
                non_null_days = int(quality.get("non_null_days", 0) or 0)
                total_days = int(quality.get("total_days", 0) or 0)
                coverage_value = float(quality.get("coverage", 0.0) or 0.0)
                coverage_suffix = f" | покрытие: {non_null_days}/{total_days} дней ({_format_percent(coverage_value * 100.0)})"
            if key == "temperature":
                quality_items.append(quality)
            sources.append(f"{item['table_name']}: {source_column}{coverage_suffix}")

        coverage_display = None
        quality_status = None
        quality_label = None
        usable = found == total_tables and found > 0
        if key == "temperature" and found > 0:
            non_null_days = sum(int(item.get("non_null_days", 0) or 0) for item in quality_items)
            total_days = sum(int(item.get("total_days", 0) or 0) for item in quality_items)
            coverage_value = (float(non_null_days) / float(total_days)) if total_days > 0 else 0.0
            coverage_display = f"{non_null_days}/{total_days} дней ({_format_percent(coverage_value * 100.0)})"
            usable = all(bool(item.get("usable")) for item in quality_items) and len(quality_items) == found
            quality_status = "good" if usable else ("missing" if non_null_days <= 0 else "sparse")
            quality_label = "Достаточное покрытие" if usable else ("Нет измерений" if non_null_days <= 0 else "Низкое покрытие")
            if not usable:
                description = (
                    "Колонка температуры найдена, но покрытие низкое: температурный признак нельзя считать надёжным для ML и температурной поправки."
                )

        if key == "temperature" and found > 0 and temperature_quality is not None:
            non_null_days = int(temperature_quality.get("non_null_days", 0) or 0)
            total_days = int(temperature_quality.get("total_days", 0) or 0)
            coverage_value = float(temperature_quality.get("coverage", 0.0) or 0.0)
            coverage_display = f"{non_null_days}/{total_days} \u0434\u043d\u0435\u0439 ({_format_percent(coverage_value * 100.0)})"
            usable = bool(temperature_quality.get("usable")) and found == total_tables
            quality_status = str(temperature_quality.get("quality_key") or ("missing" if non_null_days <= 0 else "sparse"))
            quality_label = str(
                temperature_quality.get("quality_label")
                or ("\u041d\u0435\u0442 \u0438\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u0439" if non_null_days <= 0 else "\u041d\u0438\u0437\u043a\u043e\u0435 \u043f\u043e\u043a\u0440\u044b\u0442\u0438\u0435")
            )
            if not usable:
                description = (
                    "\u041a\u043e\u043b\u043e\u043d\u043a\u0430 \u0442\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u044b \u043d\u0430\u0439\u0434\u0435\u043d\u0430, \u043d\u043e \u043f\u043e\u043a\u0440\u044b\u0442\u0438\u0435 \u043d\u0438\u0437\u043a\u043e\u0435: \u0442\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u043d\u044b\u0439 \u043f\u0440\u0438\u0437\u043d\u0430\u043a \u043d\u0435\u043b\u044c\u0437\u044f \u0441\u0447\u0438\u0442\u0430\u0442\u044c \u043d\u0430\u0434\u0451\u0436\u043d\u044b\u043c \u0434\u043b\u044f ML \u0438 \u0442\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u043d\u043e\u0439 \u043f\u043e\u043f\u0440\u0430\u0432\u043a\u0438."
                )
            if sources:
                base_sources = [source.split(" | ", 1)[0] for source in sources[:3]]
                sources = [f"{'; '.join(base_sources)} | \u043f\u043e\u043a\u0440\u044b\u0442\u0438\u0435 \u043f\u043e \u0434\u043d\u0435\u0432\u043d\u043e\u0439 \u0438\u0441\u0442\u043e\u0440\u0438\u0438: {coverage_display}"]

        if found == 0:
            status = "missing"
            status_label = "Не найдена"
            usable = False
        elif key == "temperature" and quality_label is not None:
            status = "used" if usable and found == total_tables else "partial"
            status_label = f"{quality_label} ({coverage_display})"
        elif found == total_tables:
            status = "used"
            status_label = "Используется"
        else:
            status = "partial"
            status_label = f"Частично ({found}/{total_tables})"

        cards.append(
            {
                "label": label,
                "status": status,
                "status_label": status_label,
                "source": "; ".join(sources[:3]) if sources else "Не найдена",
                "description": description,
                "quality_status": quality_status,
                "quality_label": quality_label,
                "coverage_display": coverage_display,
                "usable": usable,
            }
        )
    return cards

def _build_insights(
    daily_history: List[Dict[str, Any]],
    forecast_rows: List[Dict[str, Any]],
    weekday_profile: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    insights = []
    if forecast_rows:
        peak_row = max(forecast_rows, key=lambda item: float(item["fire_probability"]))
        insights.append(
            {
                "label": "Самый рискованный день",
                "value": peak_row["date_display"],
                "meta": f"вероятность пожара {peak_row['fire_probability_display']}",
                "tone": "fire",
            }
        )
        average_probability = mean(float(item.get("fire_probability", 0.0)) for item in forecast_rows)
        insights.append(
            {
                "label": "Средняя вероятность пожара",
                "value": _format_probability(average_probability),
                "meta": f"в день на ближайшие {len(forecast_rows)} дней",
                "tone": "forest",
            }
        )
    if daily_history and forecast_rows:
        recent_values = [float(item["count"]) for item in (daily_history[-28:] if len(daily_history) >= 28 else daily_history)]
        recent_average = mean(recent_values) if recent_values else 0.0
        forecast_average = mean(float(item["forecast_value"]) for item in forecast_rows)
        insights.append(
            {
                "label": "Сравнение с недавним уровнем",
                "value": _format_signed_percent(((forecast_average - recent_average) / recent_average) if recent_average > 0 else 0.0),
                "meta": f"{_forecast_level_label(forecast_average, recent_average)[0]} относительно последних 4 недель",
                "tone": "sky",
            }
        )
    if weekday_profile:
        peak_weekday = max(weekday_profile, key=lambda item: float(item["avg_value"]))
        insights.append(
            {
                "label": "Самый активный день недели",
                "value": peak_weekday["label"],
                "meta": f"в истории в среднем {peak_weekday['avg_display']} пожара",
                "tone": "sand",
            }
        )
    stability_label, stability_meta = _forecast_stability_hint(daily_history)
    insights.append(
        {
            "label": "Надежность оценки",
            "value": stability_label,
            "meta": stability_meta,
            "tone": "sky",
        }
    )
    return insights[:4]
def _build_notes(
    metadata: Dict[str, Any],
    filtered_records_count: int,
    daily_history: List[Dict[str, Any]],
    temperature_value: Optional[float],
) -> List[str]:
    metadata_items = metadata if isinstance(metadata, list) else [metadata]
    metadata_items = [item for item in metadata_items if item]
    notes: List[str] = []

    if len(metadata_items) > 1:
        notes.append(f"Прогноз собран сразу по {len(metadata_items)} таблицам.")

    if not any(item["resolved_columns"].get("date") for item in metadata_items):
        notes.append("В выбранных таблицах не найдена дата возникновения пожара.")
    if filtered_records_count <= 0:
        notes.append("По выбранным фильтрам нет пожаров в истории. Попробуйте снять часть ограничений.")
    elif len(daily_history) < 14:
        notes.append("История короткая, поэтому сценарный прогноз может быть менее устойчивым.")

    if temperature_value is not None and not any(item["resolved_columns"].get("temperature") for item in metadata_items):
        notes.append("Температурный сценарий задан, но колонка температуры не найдена ни в одной таблице.")
    if any(not item["resolved_columns"].get("cause") for item in metadata_items):
        notes.append("Не во всех таблицах найдена причина пожара, поэтому этот фильтр работает частично.")
    if any(not item["resolved_columns"].get("object_category") for item in metadata_items):
        notes.append("Не во всех таблицах найдена категория объекта, поэтому этот фильтр работает частично.")

    notes.append("Сценарный прогноз лучше читать как ориентир по уровню нагрузки и приоритетам, а не как точное обещание числа пожаров в каждый день.")

    return notes
