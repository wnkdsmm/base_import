"""Training subpackage for ML model service."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_MODULE_EXPORTS = {
    "data_access": ".data_access",
    "data_access_impl": ".data_access_impl",
    "forecast_bounds": ".forecast_bounds",
    "forecast_calibration": ".forecast_calibration",
    "forecast_intervals": ".forecast_intervals",
    "presentation": ".presentation",
    "presentation_backtesting": ".presentation_backtesting",
    "presentation_meta": ".presentation_meta",
    "presentation_training": ".presentation_training",
    "training": ".training",
    "training_dataset": ".training_dataset",
    "training_forecast": ".training_forecast",
    "training_importance": ".training_importance",
    "training_models": ".training_models",
    "training_result": ".training_result",
    "training_selection": ".training_selection",
    "training_temperature": ".training_temperature",
}

_ATTRIBUTE_EXPORTS = {
    "_train_ml_model": (".training", "_train_ml_model"),
    "clear_training_artifact_cache": (".training", "clear_training_artifact_cache"),
    "_empty_ml_result": (".training_result", "_empty_ml_result"),
}

__all__ = sorted([*_MODULE_EXPORTS.keys(), *_ATTRIBUTE_EXPORTS.keys()])


def _load(module_path: str) -> Any:
    return import_module(module_path, __name__)


def __getattr__(name: str) -> Any:
    module_path = _MODULE_EXPORTS.get(name)
    if module_path is not None:
        module = _load(module_path)
        globals()[name] = module
        return module

    attribute_target = _ATTRIBUTE_EXPORTS.get(name)
    if attribute_target is not None:
        target_module_path, attribute_name = attribute_target
        value = getattr(_load(target_module_path), attribute_name)
        globals()[name] = value
        return value

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(__all__))
