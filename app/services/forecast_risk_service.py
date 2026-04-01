"""Compatibility facade for legacy forecast-risk imports."""

from app.services.forecast_risk.core import build_decision_support_payload, build_risk_forecast_payload
from app.services.forecast_risk.profiles import (
    DEFAULT_RISK_WEIGHT_MODE,
    build_weight_profile_snapshot,
    get_risk_weight_profile,
    resolve_component_weights,
)
from app.services.forecast_risk.validation import (
    build_historical_validation_payload,
    empty_historical_validation_payload,
    resolve_weight_profile_for_records,
)

__all__ = [
    "DEFAULT_RISK_WEIGHT_MODE",
    "build_decision_support_payload",
    "build_historical_validation_payload",
    "build_risk_forecast_payload",
    "build_weight_profile_snapshot",
    "empty_historical_validation_payload",
    "get_risk_weight_profile",
    "resolve_component_weights",
    "resolve_weight_profile_for_records",
]
