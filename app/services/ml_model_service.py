from __future__ import annotations

"""Compatibility facade for legacy ML-model imports.

Prefer direct imports from ``app.services.ml_model.core`` and
``app.services.ml_model.jobs`` in new code.
"""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "clear_ml_model_cache": ("app.services.ml_model.core", "clear_ml_model_cache"),
    "get_ml_job_status": ("app.services.ml_model.jobs", "get_ml_job_status"),
    "get_ml_model_data": ("app.services.ml_model.core", "get_ml_model_data"),
    "get_ml_model_shell_context": ("app.services.ml_model.core", "get_ml_model_shell_context"),
    "start_ml_model_job": ("app.services.ml_model.jobs", "start_ml_model_job"),
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
