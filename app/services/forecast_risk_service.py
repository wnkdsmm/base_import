from __future__ import annotations

"""Compatibility facade for legacy forecast-risk imports.

Prefer direct imports from ``app.services.forecast_risk`` submodules in new code.
"""

from app.compat import install_lazy_exports

_EXPORTS = {
    "DEFAULT_RISK_WEIGHT_MODE": ("app.services.forecast_risk.profiles", "DEFAULT_RISK_WEIGHT_MODE"),
    "build_decision_support_payload": ("app.services.forecast_risk.core", "build_decision_support_payload"),
    "build_historical_validation_payload": ("app.services.forecast_risk.validation", "build_historical_validation_payload"),
    "build_risk_forecast_payload": ("app.services.forecast_risk.core", "build_risk_forecast_payload"),
    "build_weight_profile_snapshot": ("app.services.forecast_risk.profiles", "build_weight_profile_snapshot"),
    "empty_historical_validation_payload": ("app.services.forecast_risk.validation", "empty_historical_validation_payload"),
    "get_risk_weight_profile": ("app.services.forecast_risk.profiles", "get_risk_weight_profile"),
    "resolve_component_weights": ("app.services.forecast_risk.profiles", "resolve_component_weights"),
    # Keep the validation-module import surface for legacy callers even though
    # the canonical implementation now lives in profile_resolution.
    "resolve_weight_profile_for_records": ("app.services.forecast_risk.validation", "resolve_weight_profile_for_records"),
}

__all__, __getattr__, __dir__ = install_lazy_exports(__name__, globals(), _EXPORTS)
