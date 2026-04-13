from __future__ import annotations

from typing import Any, Dict, List

from .charts_impl import (
    build_forecasting_forecast_breakdown_chart,
    build_forecasting_forecast_chart,
    build_forecasting_geo_chart,
    build_forecasting_weekday_chart,
)


def _build_forecast_chart(daily_history: List[Dict[str, Any]], forecast_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return build_forecasting_forecast_chart(daily_history, forecast_rows)


def _build_forecast_breakdown_chart(forecast_rows: List[Dict[str, Any]], recent_average: float) -> Dict[str, Any]:
    return build_forecasting_forecast_breakdown_chart(forecast_rows, recent_average)


def _build_weekday_chart(weekday_profile: List[Dict[str, Any]]) -> Dict[str, Any]:
    return build_forecasting_weekday_chart(weekday_profile)


def _build_geo_chart(geo_prediction: Dict[str, Any]) -> Dict[str, Any]:
    return build_forecasting_geo_chart(geo_prediction)
