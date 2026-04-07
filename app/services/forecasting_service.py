from __future__ import annotations

"""Compatibility facade for legacy forecasting imports.

Prefer direct imports from ``app.services.forecasting.core`` and
``app.services.forecasting.jobs`` in new code.
"""

from app.compat import install_lazy_exports

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

__all__, __getattr__, __dir__ = install_lazy_exports(__name__, globals(), _EXPORTS)
