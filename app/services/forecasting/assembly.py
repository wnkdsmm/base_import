from __future__ import annotations

import copy
from datetime import datetime
from statistics import mean
from typing import Any, Callable, Dict, Sequence

ForecastingDeps = Dict[str, Callable[..., Any]]


def build_forecasting_metadata_payload(
    metadata_payload: Dict[str, Any],
    *,
    source_tables: Sequence[str],
    source_table_notes: Sequence[str],
    selected_history_window: str,
    deps: ForecastingDeps,
) -> Dict[str, Any]:
    metadata_items, preload_notes = deps["collect_forecasting_metadata"](source_tables)
    option_catalog = deps["build_option_catalog_sql"](
        source_tables,
        history_window=selected_history_window,
        metadata_items=metadata_items,
    )
    selected_district = deps["resolve_option_value"](
        option_catalog["districts"],
        metadata_payload["filters"]["district"],
    )
    selected_cause = deps["resolve_option_value"](
        option_catalog["causes"],
        metadata_payload["filters"]["cause"],
    )
    selected_object_category = deps["resolve_option_value"](
        option_catalog["object_categories"],
        metadata_payload["filters"]["object_category"],
    )
    feature_cards = deps["build_feature_cards"](metadata_items)
    base_loading_message = "Фильтры и признаки готовы. Запускаем базовый прогноз."
    followup_message = deps["build_decision_support_followup_message"]()

    metadata_payload["generated_at"] = deps["format_datetime"](datetime.now())
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
            "slice_label": deps["build_slice_label"](
                selected_district,
                selected_cause,
                selected_object_category,
            ),
            "history_period_label": "История загружается",
            "history_window_label": deps["history_window_label"](selected_history_window),
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
    metadata_payload["risk_prediction"] = deps["build_shell_risk_prediction"](
        table_options=metadata_payload["filters"]["available_tables"],
        selected_table=metadata_payload["filters"]["table_name"],
        forecast_days=int(metadata_payload["filters"]["forecast_days"]),
        temperature=metadata_payload["filters"]["temperature"],
        history_window=selected_history_window,
        feature_cards=feature_cards,
        message=base_loading_message,
    )
    metadata_payload["executive_brief"] = deps["build_pending_executive_brief"](base_loading_message)
    metadata_payload["executive_brief"]["notes"] = list(source_table_notes) + [
        metadata_payload["metadata_status_message"],
        base_loading_message,
        followup_message,
    ]
    metadata_payload["executive_brief"]["export_excerpt"] = base_loading_message
    metadata_payload["notes"] = list(
        dict.fromkeys(
            list(source_table_notes)
            + list(preload_notes)
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


def build_forecasting_base_payload(
    *,
    table_options: Sequence[Dict[str, Any]],
    selected_table: str,
    source_tables: Sequence[str],
    source_table_notes: Sequence[str],
    district: str,
    cause: str,
    object_category: str,
    temperature: str,
    temperature_value: float | None,
    days_ahead: int,
    selected_history_window: str,
    include_decision_support: bool,
    deps: ForecastingDeps,
) -> Dict[str, Any]:
    metadata_items, preload_notes = deps["collect_forecasting_metadata"](source_tables)
    feature_cards = deps["build_feature_cards_with_quality"](metadata_items)
    option_catalog = deps["build_option_catalog_sql"](
        source_tables,
        history_window=selected_history_window,
        metadata_items=metadata_items,
    )
    selected_district = deps["resolve_option_value"](option_catalog["districts"], district)
    selected_cause = deps["resolve_option_value"](option_catalog["causes"], cause)
    selected_object_category = deps["resolve_option_value"](
        option_catalog["object_categories"],
        object_category,
    )
    filtered_records_count = deps["count_forecasting_records_sql"](
        source_tables,
        history_window=selected_history_window,
        district=selected_district,
        cause=selected_cause,
        object_category=selected_object_category,
        metadata_items=metadata_items,
    )
    daily_history = deps["build_daily_history_sql"](
        source_tables,
        history_window=selected_history_window,
        district=selected_district,
        cause=selected_cause,
        object_category=selected_object_category,
        metadata_items=metadata_items,
    )
    temperature_quality = deps["temperature_quality_from_daily_history"](daily_history)
    feature_cards = deps["build_feature_cards_with_quality"](
        metadata_items,
        temperature_quality=temperature_quality,
    )
    scenario_backtest = deps["run_scenario_backtesting"](daily_history)
    quality_assessment = deps["build_scenario_quality_assessment"](scenario_backtest)
    forecast_rows = deps["build_forecast_rows"](daily_history, days_ahead, temperature_value)
    weekday_profile = deps["build_weekday_profile"](daily_history)
    history_counts = [float(item["count"]) for item in daily_history]
    recent_counts = history_counts[-28:] if len(history_counts) >= 28 else history_counts
    recent_average = mean(recent_counts) if recent_counts else 0.0
    charts = {
        "daily": deps["build_forecast_chart"](daily_history, forecast_rows),
        "breakdown": deps["build_forecast_breakdown_chart"](forecast_rows, recent_average),
        "weekday": deps["build_weekday_chart"](weekday_profile),
    }
    (
        risk_prediction,
        geo_prediction,
        decision_support_pending,
        decision_support_ready,
        decision_support_error,
        decision_support_status_message,
    ) = _build_decision_support_block(
        table_options=table_options,
        selected_table=selected_table,
        source_tables=source_tables,
        selected_district=selected_district,
        selected_cause=selected_cause,
        selected_object_category=selected_object_category,
        selected_history_window=selected_history_window,
        days_ahead=days_ahead,
        temperature=temperature,
        feature_cards=feature_cards,
        include_decision_support=include_decision_support,
        deps=deps,
    )
    charts["geo"] = deps["build_geo_chart"](geo_prediction)
    notes = list(
        dict.fromkeys(
            list(source_table_notes)
            + list(preload_notes)
            + deps["build_notes"](
                metadata=metadata_items,
                filtered_records_count=filtered_records_count,
                daily_history=daily_history,
                temperature_value=temperature_value,
            )
        )
    )
    features = risk_prediction["feature_cards"] or feature_cards
    insights = deps["build_insights"](daily_history, forecast_rows, weekday_profile)
    summary = deps["build_summary"](
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
    generated_at = deps["format_datetime"](datetime.now())
    executive_brief = deps["build_pending_executive_brief"](
        decision_support_status_message
        or "Короткий вывод будет доступен после расчета блока поддержки решений."
    )
    if decision_support_ready:
        executive_brief = deps["build_executive_brief_from_risk_payload"](
            risk_prediction,
            notes=risk_prediction.get("notes"),
        )
    executive_brief["export_text"] = deps["compose_executive_brief_text"](
        executive_brief,
        scope_label=(
            f"Таблица: {summary['selected_table_label']} | История: {summary['history_window_label']} | "
            f"Срез: {summary['slice_label']} | Горизонт: {summary['forecast_days_display']} дней"
        ),
        generated_at=generated_at,
    )
    return {
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
        "model_description": deps["scenario_forecast_description"],
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
            "temperature": temperature
            if temperature_value is None
            else deps["format_float_for_input"](temperature_value),
            "forecast_days": str(days_ahead),
            "history_window": selected_history_window,
            "available_tables": list(table_options),
            "available_districts": option_catalog["districts"],
            "available_causes": option_catalog["causes"],
            "available_object_categories": option_catalog["object_categories"],
            "available_forecast_days": [
                {"value": str(option), "label": f"{option} дней"}
                for option in deps["forecast_day_options"]
            ],
            "available_history_windows": deps["history_window_options"],
        },
    }


def complete_forecasting_decision_support_payload(
    *,
    base_payload: Dict[str, Any],
    request_state: Dict[str, Any],
    progress_callback: Callable[[str, str], None] | None,
    deps: ForecastingDeps,
) -> Dict[str, Any]:
    filters = base_payload.get("filters") or {}
    available_tables = filters.get("available_tables") or request_state["table_options"]
    selected_table = str(filters.get("table_name") or request_state["selected_table"] or "all")
    source_tables = deps["selected_source_tables"](available_tables, selected_table)
    if not source_tables:
        payload = copy.deepcopy(base_payload)
        payload["bootstrap_mode"] = "full"
        payload["decision_support_pending"] = False
        payload["decision_support_ready"] = True
        payload["decision_support_error"] = False
        payload["decision_support_status_message"] = ""
        return payload

    deps["emit_forecasting_progress"](
        progress_callback,
        "forecasting_decision_support.aggregation",
        "Собираем ранжирование территорий, паспорт качества и историческую валидацию.",
    )
    risk_prediction = deps["build_decision_support_payload"](
        source_tables=source_tables,
        selected_district=str(filters.get("district") or "all"),
        selected_cause=str(filters.get("cause") or "all"),
        selected_object_category=str(filters.get("object_category") or "all"),
        history_window=str(filters.get("history_window") or request_state["history_window"]),
        planning_horizon_days=int(filters.get("forecast_days") or request_state["days_ahead"]),
        progress_callback=progress_callback,
    )
    deps["emit_forecasting_progress"](
        progress_callback,
        "forecasting_decision_support.render",
        "Обновляем короткий вывод, рекомендации и карту риска.",
    )
    payload = copy.deepcopy(base_payload)
    generated_at = deps["format_datetime"](datetime.now())
    charts = dict(payload.get("charts") or {})
    charts["geo"] = deps["build_geo_chart"](risk_prediction.get("geo_prediction") or {})
    executive_brief = deps["build_executive_brief_from_risk_payload"](
        risk_prediction,
        notes=risk_prediction.get("notes"),
    )
    summary = payload.get("summary") or {}
    executive_brief["export_text"] = deps["compose_executive_brief_text"](
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
    return payload


def _build_decision_support_block(
    *,
    table_options: Sequence[Dict[str, Any]],
    selected_table: str,
    source_tables: Sequence[str],
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    selected_history_window: str,
    days_ahead: int,
    temperature: str,
    feature_cards: Sequence[Dict[str, Any]],
    include_decision_support: bool,
    deps: ForecastingDeps,
) -> tuple[Dict[str, Any], Dict[str, Any], bool, bool, bool, str]:
    if not include_decision_support:
        decision_support_status_message = (
            "Базовый сценарный прогноз уже показан. Приоритеты территорий, паспорт качества и рекомендации догружаются фоном."
        )
        risk_prediction = deps["build_pending_decision_support_payload"](
            table_options=table_options,
            selected_table=selected_table,
            forecast_days=days_ahead,
            temperature=temperature,
            history_window=selected_history_window,
            feature_cards=feature_cards,
            message=decision_support_status_message,
        )
        return risk_prediction, {}, True, False, False, decision_support_status_message

    try:
        risk_prediction = deps["build_decision_support_payload"](
            source_tables=source_tables,
            selected_district=selected_district,
            selected_cause=selected_cause,
            selected_object_category=selected_object_category,
            history_window=selected_history_window,
            planning_horizon_days=days_ahead,
        )
        risk_prediction["feature_cards"] = list(feature_cards)
        geo_prediction = risk_prediction.get("geo_prediction") or {}
        return (
            risk_prediction,
            geo_prediction,
            False,
            True,
            False,
            "Блок поддержки решений и рекомендации готовы.",
        )
    except Exception as exc:
        decision_support_status_message = (
            "Блок приоритетов территорий временно недоступен. Базовый сценарный прогноз показан без него."
        )
        risk_prediction = deps["build_pending_decision_support_payload"](
            table_options=table_options,
            selected_table=selected_table,
            forecast_days=days_ahead,
            temperature=temperature,
            history_window=selected_history_window,
            feature_cards=feature_cards,
            message=decision_support_status_message,
        )
        risk_prediction["notes"].append(f"Техническая причина: {exc}")
        return risk_prediction, {}, False, False, True, decision_support_status_message
