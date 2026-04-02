from __future__ import annotations

"""Compatibility facade for legacy forecasting imports.

Prefer direct imports from ``app.services.forecasting.core`` and
``app.services.forecasting.jobs`` in new code.
"""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "clear_forecasting_cache": ("app.services.forecasting.core", "clear_forecasting_cache"),
    "get_forecasting_data": ("app.services.forecasting.core", "get_forecasting_data"),
    "get_forecasting_decision_support_data": ("app.services.forecasting.core", "get_forecasting_decision_support_data"),
    "get_forecasting_decision_support_job_status": ("app.services.forecasting.jobs", "get_forecasting_decision_support_job_status"),
    "get_forecasting_metadata": ("app.services.forecasting.core", "get_forecasting_metadata"),
    "get_forecasting_page_context": ("app.services.forecasting.core", "get_forecasting_page_context"),
    "get_forecasting_shell_context": ("app.services.forecasting.core", "get_forecasting_shell_context"),
    "start_forecasting_decision_support_job": ("app.services.forecasting.jobs", "start_forecasting_decision_support_job"),
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
