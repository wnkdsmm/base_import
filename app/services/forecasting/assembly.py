from __future__ import annotations

from datetime import datetime
from statistics import mean
from typing import Callable, Sequence

from app.cache import clone_mutable_payload

from .inputs import load_base_forecasting_inputs, load_forecasting_metadata_inputs
from .types import (
    ForecastPayload,
    ForecastingBaseArtifacts,
    ForecastingBaseInputs,
    ForecastingBasePresentation,
    ForecastingCharts,
    ForecastingDeps,
    ForecastingDailyHistoryRow,
    ForecastingExecutiveBrief,
    ForecastingFeatureCard,
    ForecastingFilters,
    ForecastingForecastRow,
    ForecastingGeoPrediction,
    ForecastingInsightCard,
    ForecastingOptionCatalog,
    ForecastingQualityAssessment,
    ForecastingRequestState,
    ForecastingRiskPrediction,
    ForecastingSummary,
    ForecastingTableMetadata,
    ForecastingWeekdayProfileRow,
    TableOption,
)


def build_forecasting_metadata_payload(
    metadata_payload: ForecastPayload,
    *,
    source_tables: Sequence[str],
    source_table_notes: Sequence[str],
    selected_history_window: str,
    deps: ForecastingDeps,
) -> ForecastPayload:
    metadata_inputs = load_forecasting_metadata_inputs(
        source_tables=source_tables,
        selected_history_window=selected_history_window,
        district=metadata_payload["filters"]["district"],
        cause=metadata_payload["filters"]["cause"],
        object_category=metadata_payload["filters"]["object_category"],
        deps=deps,
    )
    metadata_items = metadata_inputs["metadata_items"]
    preload_notes = metadata_inputs["preload_notes"]
    option_catalog = metadata_inputs["option_catalog"]
    selected_district = metadata_inputs["selected_district"]
    selected_cause = metadata_inputs["selected_cause"]
    selected_object_category = metadata_inputs["selected_object_category"]
    feature_cards = metadata_inputs["feature_cards"]
    base_loading_message = "Р¤РёР»СЊС‚СЂС‹ Рё РїСЂРёР·РЅР°РєРё РіРѕС‚РѕРІС‹. Р—Р°РїСѓСЃРєР°РµРј Р±Р°Р·РѕРІС‹Р№ РїСЂРѕРіРЅРѕР·."
    followup_message = deps["build_decision_support_followup_message"]()

    metadata_payload["generated_at"] = deps["format_datetime"](datetime.now())
    metadata_payload["loading"] = True
    metadata_payload["deferred"] = True
    metadata_payload["metadata_pending"] = False
    metadata_payload["metadata_ready"] = True
    metadata_payload["metadata_error"] = False
    metadata_payload["metadata_status_message"] = "Р¤РёР»СЊС‚СЂС‹ Рё РїСЂРёР·РЅР°РєРё РіРѕС‚РѕРІС‹."
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
            "history_period_label": "РСЃС‚РѕСЂРёСЏ Р·Р°РіСЂСѓР¶Р°РµС‚СЃСЏ",
            "history_window_label": deps["history_window_label"](selected_history_window),
        }
    )
    metadata_payload["quality_assessment"]["subtitle"] = (
        "Р¤РёР»СЊС‚СЂС‹ Рё РїСЂРёР·РЅР°РєРё СѓР¶Рµ РіРѕС‚РѕРІС‹. РўРµРїРµСЂСЊ СЂР°СЃСЃС‡РёС‚С‹РІР°РµРј Р±Р°Р·РѕРІС‹Р№ РїСЂРѕРіРЅРѕР·, "
        "Р° РјРµС‚СЂРёРєРё РєР°С‡РµСЃС‚РІР° РїРѕСЏРІСЏС‚СЃСЏ РІРјРµСЃС‚Рµ СЃ РЅРёРј."
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


def _recent_average_from_daily_history(daily_history: Sequence[ForecastingDailyHistoryRow]) -> float:
    history_counts = [float(item["count"]) for item in daily_history]
    recent_counts = history_counts[-28:] if len(history_counts) >= 28 else history_counts
    return mean(recent_counts) if recent_counts else 0.0


def _build_base_forecasting_charts(
    *,
    daily_history: Sequence[ForecastingDailyHistoryRow],
    forecast_rows: Sequence[ForecastingForecastRow],
    weekday_profile: Sequence[ForecastingWeekdayProfileRow],
    deps: ForecastingDeps,
) -> ForecastingCharts:
    recent_average = _recent_average_from_daily_history(daily_history)
    return {
        "daily": deps["build_forecast_chart"](daily_history, forecast_rows),
        "breakdown": deps["build_forecast_breakdown_chart"](forecast_rows, recent_average),
        "weekday": deps["build_weekday_chart"](weekday_profile),
    }


def _build_base_forecasting_notes(
    *,
    source_table_notes: Sequence[str],
    preload_notes: Sequence[str],
    metadata_items: Sequence[ForecastingTableMetadata],
    filtered_records_count: int,
    daily_history: Sequence[ForecastingDailyHistoryRow],
    temperature_value: float | None,
    deps: ForecastingDeps,
) -> list[str]:
    return list(
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


def _build_base_forecasting_executive_brief(
    *,
    risk_prediction: ForecastingRiskPrediction,
    decision_support_ready: bool,
    decision_support_status_message: str,
    summary: ForecastingSummary,
    generated_at: str,
    deps: ForecastingDeps,
) -> ForecastingExecutiveBrief:
    executive_brief = deps["build_pending_executive_brief"](
        decision_support_status_message
        or "РљРѕСЂРѕС‚РєРёР№ РІС‹РІРѕРґ Р±СѓРґРµС‚ РґРѕСЃС‚СѓРїРµРЅ РїРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р±Р»РѕРєР° РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№."
    )
    if decision_support_ready:
        executive_brief = deps["build_executive_brief_from_risk_payload"](
            risk_prediction,
            notes=risk_prediction.get("notes"),
        )
    executive_brief["export_text"] = deps["compose_executive_brief_text"](
        executive_brief,
        scope_label=(
            f"РўР°Р±Р»РёС†Р°: {summary['selected_table_label']} | РСЃС‚РѕСЂРёСЏ: {summary['history_window_label']} | "
            f"РЎСЂРµР·: {summary['slice_label']} | Р“РѕСЂРёР·РѕРЅС‚: {summary['forecast_days_display']} РґРЅРµР№"
        ),
        generated_at=generated_at,
    )
    return executive_brief


def _build_base_forecasting_filters(
    *,
    table_options: Sequence[TableOption],
    selected_table: str,
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    temperature: str,
    temperature_value: float | None,
    days_ahead: int,
    selected_history_window: str,
    option_catalog: ForecastingOptionCatalog,
    deps: ForecastingDeps,
) -> ForecastingFilters:
    return {
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
            {"value": str(option), "label": f"{option} РґРЅРµР№"}
            for option in deps["forecast_day_options"]
        ],
        "available_history_windows": deps["history_window_options"],
    }


def _build_base_forecasting_payload_response(
    *,
    generated_at: str,
    filtered_records_count: int,
    decision_support_pending: bool,
    decision_support_ready: bool,
    decision_support_error: bool,
    decision_support_status_message: str,
    summary: ForecastingSummary,
    quality_assessment: ForecastingQualityAssessment,
    features: Sequence[ForecastingFeatureCard],
    risk_prediction: ForecastingRiskPrediction,
    executive_brief: ForecastingExecutiveBrief,
    insights: Sequence[ForecastingInsightCard],
    charts: ForecastingCharts,
    forecast_rows: Sequence[ForecastingForecastRow],
    notes: Sequence[str],
    filters: ForecastingFilters,
    deps: ForecastingDeps,
) -> ForecastPayload:
    return {
        "generated_at": generated_at,
        "has_data": filtered_records_count > 0,
        "bootstrap_mode": "full" if decision_support_ready else "partial",
        "loading": False,
        "deferred": False,
        "metadata_pending": False,
        "metadata_ready": True,
        "metadata_error": False,
        "metadata_status_message": "Р¤РёР»СЊС‚СЂС‹ Рё РїСЂРёР·РЅР°РєРё РіРѕС‚РѕРІС‹.",
        "base_forecast_pending": False,
        "base_forecast_ready": True,
        "loading_status_message": "Р‘Р°Р·РѕРІС‹Р№ РїСЂРѕРіРЅРѕР· РіРѕС‚РѕРІ.",
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
        "filters": filters,
    }


def _load_base_forecasting_inputs(
    *,
    source_tables: Sequence[str],
    selected_history_window: str,
    district: str,
    cause: str,
    object_category: str,
    deps: ForecastingDeps,
) -> ForecastingBaseInputs:
    return load_base_forecasting_inputs(
        source_tables=source_tables,
        selected_history_window=selected_history_window,
        district=district,
        cause=cause,
        object_category=object_category,
        deps=deps,
    )


def _build_base_forecasting_artifacts(
    *,
    daily_history: Sequence[ForecastingDailyHistoryRow],
    days_ahead: int,
    temperature_value: float | None,
    deps: ForecastingDeps,
) -> ForecastingBaseArtifacts:
    scenario_backtest = deps["run_scenario_backtesting"](daily_history)
    quality_assessment = deps["build_scenario_quality_assessment"](scenario_backtest)
    forecast_rows = deps["build_forecast_rows"](daily_history, days_ahead, temperature_value)
    weekday_profile = deps["build_weekday_profile"](daily_history)
    charts = _build_base_forecasting_charts(
        daily_history=daily_history,
        forecast_rows=forecast_rows,
        weekday_profile=weekday_profile,
        deps=deps,
    )
    return {
        "quality_assessment": quality_assessment,
        "forecast_rows": forecast_rows,
        "weekday_profile": weekday_profile,
        "charts": charts,
    }


def _build_base_forecasting_presentation(
    *,
    source_table_notes: Sequence[str],
    preload_notes: Sequence[str],
    metadata_items: Sequence[ForecastingTableMetadata],
    filtered_records_count: int,
    daily_history: Sequence[ForecastingDailyHistoryRow],
    forecast_rows: Sequence[ForecastingForecastRow],
    weekday_profile: Sequence[ForecastingWeekdayProfileRow],
    risk_prediction: ForecastingRiskPrediction,
    feature_cards: Sequence[ForecastingFeatureCard],
    selected_table: str,
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    temperature_value: float | None,
    selected_history_window: str,
    decision_support_ready: bool,
    decision_support_status_message: str,
    table_options: Sequence[TableOption],
    temperature: str,
    days_ahead: int,
    option_catalog: ForecastingOptionCatalog,
    deps: ForecastingDeps,
) -> ForecastingBasePresentation:
    notes = _build_base_forecasting_notes(
        source_table_notes=source_table_notes,
        preload_notes=preload_notes,
        metadata_items=metadata_items,
        filtered_records_count=filtered_records_count,
        daily_history=daily_history,
        temperature_value=temperature_value,
        deps=deps,
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
    executive_brief = _build_base_forecasting_executive_brief(
        risk_prediction=risk_prediction,
        decision_support_ready=decision_support_ready,
        decision_support_status_message=decision_support_status_message,
        summary=summary,
        generated_at=generated_at,
        deps=deps,
    )
    filters = _build_base_forecasting_filters(
        table_options=table_options,
        selected_table=selected_table,
        selected_district=selected_district,
        selected_cause=selected_cause,
        selected_object_category=selected_object_category,
        temperature=temperature,
        temperature_value=temperature_value,
        days_ahead=days_ahead,
        selected_history_window=selected_history_window,
        option_catalog=option_catalog,
        deps=deps,
    )
    return {
        "generated_at": generated_at,
        "notes": notes,
        "features": features,
        "insights": insights,
        "summary": summary,
        "executive_brief": executive_brief,
        "filters": filters,
    }


def build_forecasting_base_payload(
    *,
    table_options: Sequence[TableOption],
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
) -> ForecastPayload:
    inputs = _load_base_forecasting_inputs(
        source_tables=source_tables,
        selected_history_window=selected_history_window,
        district=district,
        cause=cause,
        object_category=object_category,
        deps=deps,
    )
    daily_history = inputs["daily_history"]
    selected_district = inputs["selected_district"]
    selected_cause = inputs["selected_cause"]
    selected_object_category = inputs["selected_object_category"]
    artifacts = _build_base_forecasting_artifacts(
        daily_history=daily_history,
        days_ahead=days_ahead,
        temperature_value=temperature_value,
        deps=deps,
    )
    charts = artifacts["charts"]
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
        feature_cards=inputs["feature_cards"],
        include_decision_support=include_decision_support,
        deps=deps,
    )
    charts["geo"] = deps["build_geo_chart"](geo_prediction)
    presentation = _build_base_forecasting_presentation(
        source_table_notes=source_table_notes,
        preload_notes=inputs["preload_notes"],
        metadata_items=inputs["metadata_items"],
        filtered_records_count=inputs["filtered_records_count"],
        daily_history=daily_history,
        forecast_rows=artifacts["forecast_rows"],
        weekday_profile=artifacts["weekday_profile"],
        risk_prediction=risk_prediction,
        feature_cards=inputs["feature_cards"],
        selected_table=selected_table,
        selected_district=selected_district,
        selected_cause=selected_cause,
        selected_object_category=selected_object_category,
        temperature_value=temperature_value,
        selected_history_window=selected_history_window,
        decision_support_ready=decision_support_ready,
        decision_support_status_message=decision_support_status_message,
        table_options=table_options,
        temperature=temperature,
        days_ahead=days_ahead,
        option_catalog=inputs["option_catalog"],
        deps=deps,
    )
    return _build_base_forecasting_payload_response(
        generated_at=presentation["generated_at"],
        filtered_records_count=inputs["filtered_records_count"],
        decision_support_pending=decision_support_pending,
        decision_support_ready=decision_support_ready,
        decision_support_error=decision_support_error,
        decision_support_status_message=decision_support_status_message,
        summary=presentation["summary"],
        quality_assessment=artifacts["quality_assessment"],
        features=presentation["features"],
        risk_prediction=risk_prediction,
        executive_brief=presentation["executive_brief"],
        insights=presentation["insights"],
        charts=charts,
        forecast_rows=artifacts["forecast_rows"],
        notes=presentation["notes"],
        filters=presentation["filters"],
        deps=deps,
    )


def complete_forecasting_decision_support_payload(
    *,
    base_payload: ForecastPayload,
    request_state: ForecastingRequestState,
    progress_callback: Callable[[str, str], None] | None,
    deps: ForecastingDeps,
) -> ForecastPayload:
    filters = base_payload.get("filters") or {}
    available_tables = filters.get("available_tables") or request_state["table_options"]
    selected_table = str(filters.get("table_name") or request_state["selected_table"] or "all")
    source_tables = deps["selected_source_tables"](available_tables, selected_table)
    if not source_tables:
        payload = clone_mutable_payload(base_payload)
        payload["bootstrap_mode"] = "full"
        payload["decision_support_pending"] = False
        payload["decision_support_ready"] = True
        payload["decision_support_error"] = False
        payload["decision_support_status_message"] = ""
        return payload

    deps["emit_forecasting_progress"](
        progress_callback,
        "forecasting_decision_support.aggregation",
        "РЎРѕР±РёСЂР°РµРј СЂР°РЅР¶РёСЂРѕРІР°РЅРёРµ С‚РµСЂСЂРёС‚РѕСЂРёР№, РїР°СЃРїРѕСЂС‚ РєР°С‡РµСЃС‚РІР° Рё РёСЃС‚РѕСЂРёС‡РµСЃРєСѓСЋ РІР°Р»РёРґР°С†РёСЋ.",
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
        "РћР±РЅРѕРІР»СЏРµРј РєРѕСЂРѕС‚РєРёР№ РІС‹РІРѕРґ, СЂРµРєРѕРјРµРЅРґР°С†РёРё Рё РєР°СЂС‚Сѓ СЂРёСЃРєР°.",
    )
    payload = clone_mutable_payload(base_payload)
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
            f"РўР°Р±Р»РёС†Р°: {summary.get('selected_table_label') or '-'} | "
            f"РСЃС‚РѕСЂРёСЏ: {summary.get('history_window_label') or '-'} | "
            f"РЎСЂРµР·: {summary.get('slice_label') or '-'} | "
            f"Р“РѕСЂРёР·РѕРЅС‚: {summary.get('forecast_days_display') or '-'} РґРЅРµР№"
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
        metadata_status_message="Р¤РёР»СЊС‚СЂС‹ Рё РїСЂРёР·РЅР°РєРё РіРѕС‚РѕРІС‹.",
        base_forecast_pending=False,
        base_forecast_ready=True,
        loading_status_message="Р‘Р°Р·РѕРІС‹Р№ РїСЂРѕРіРЅРѕР· РіРѕС‚РѕРІ.",
        decision_support_pending=False,
        decision_support_ready=True,
        decision_support_error=False,
        decision_support_status_message="Р‘Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№ Рё СЂРµРєРѕРјРµРЅРґР°С†РёРё РіРѕС‚РѕРІС‹.",
        features=risk_prediction.get("feature_cards") or payload.get("features") or [],
        risk_prediction=risk_prediction,
        executive_brief=executive_brief,
        charts=charts,
    )
    return payload


def _build_decision_support_block(
    *,
    table_options: Sequence[TableOption],
    selected_table: str,
    source_tables: Sequence[str],
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    selected_history_window: str,
    days_ahead: int,
    temperature: str,
    feature_cards: Sequence[ForecastingFeatureCard],
    include_decision_support: bool,
    deps: ForecastingDeps,
) -> tuple[ForecastingRiskPrediction, ForecastingGeoPrediction, bool, bool, bool, str]:
    if not include_decision_support:
        decision_support_status_message = (
            "Р‘Р°Р·РѕРІС‹Р№ СЃС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР· СѓР¶Рµ РїРѕРєР°Р·Р°РЅ. РџСЂРёРѕСЂРёС‚РµС‚С‹ С‚РµСЂСЂРёС‚РѕСЂРёР№, РїР°СЃРїРѕСЂС‚ РєР°С‡РµСЃС‚РІР° Рё СЂРµРєРѕРјРµРЅРґР°С†РёРё РґРѕРіСЂСѓР¶Р°СЋС‚СЃСЏ С„РѕРЅРѕРј."
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
            "Р‘Р»РѕРє РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№ Рё СЂРµРєРѕРјРµРЅРґР°С†РёРё РіРѕС‚РѕРІС‹.",
        )
    except Exception as exc:
        decision_support_status_message = (
            "Р‘Р»РѕРє РїСЂРёРѕСЂРёС‚РµС‚РѕРІ С‚РµСЂСЂРёС‚РѕСЂРёР№ РІСЂРµРјРµРЅРЅРѕ РЅРµРґРѕСЃС‚СѓРїРµРЅ. Р‘Р°Р·РѕРІС‹Р№ СЃС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР· РїРѕРєР°Р·Р°РЅ Р±РµР· РЅРµРіРѕ."
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
        risk_prediction["notes"].append(f"РўРµС…РЅРёС‡РµСЃРєР°СЏ РїСЂРёС‡РёРЅР°: {exc}")
        return risk_prediction, {}, False, False, True, decision_support_status_message

