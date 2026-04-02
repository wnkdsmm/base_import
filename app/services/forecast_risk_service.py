from __future__ import annotations

"""Compatibility facade for legacy forecast-risk imports.

Prefer direct imports from ``app.services.forecast_risk`` submodules in new code.
"""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "DEFAULT_RISK_WEIGHT_MODE": ("app.services.forecast_risk.profiles", "DEFAULT_RISK_WEIGHT_MODE"),
    "build_decision_support_payload": ("app.services.forecast_risk.core", "build_decision_support_payload"),
    "build_historical_validation_payload": ("app.services.forecast_risk.validation", "build_historical_validation_payload"),
    "build_risk_forecast_payload": ("app.services.forecast_risk.core", "build_risk_forecast_payload"),
    "build_weight_profile_snapshot": ("app.services.forecast_risk.profiles", "build_weight_profile_snapshot"),
    "empty_historical_validation_payload": ("app.services.forecast_risk.validation", "empty_historical_validation_payload"),
    "get_risk_weight_profile": ("app.services.forecast_risk.profiles", "get_risk_weight_profile"),
    "resolve_component_weights": ("app.services.forecast_risk.profiles", "resolve_component_weights"),
    "resolve_weight_profile_for_records": ("app.services.forecast_risk.validation", "resolve_weight_profile_for_records"),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    return getattr(import_module(module_name), attr_name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
