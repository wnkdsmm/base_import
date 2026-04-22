from __future__ import annotations

from datetime import date
from statistics import mean
from typing import Sequence

from .inputs import load_base_forecasting_inputs
from .types import (
    ForecastingBaseArtifacts,
    ForecastingBaseInputs,
    ForecastingCharts,
    ForecastingDailyHistoryRow,
    ForecastingDeps,
    ForecastingFeatureCard,
    ForecastingForecastRow,
    ForecastingGeoPrediction,
    ForecastingQualityAssessment,
    ForecastingRiskPrediction,
    ForecastingWeekdayProfileRow,
    TableOption,
)

__all__ = [
    "_recent_average_from_daily_history",
    "_build_base_forecasting_charts",
    "_load_base_forecasting_inputs",
    "_build_base_forecasting_artifacts",
    "_build_decision_support_block",
]


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
    current_user_date: date | None,
    deps: ForecastingDeps,
) -> ForecastingBaseArtifacts:
    scenario_backtest = deps["run_scenario_backtesting"](daily_history)
    quality_assessment = deps["build_scenario_quality_assessment"](scenario_backtest)
    forecast_rows = deps["build_forecast_rows"](
        daily_history,
        days_ahead,
        temperature_value,
        current_user_date=current_user_date,
    )
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
