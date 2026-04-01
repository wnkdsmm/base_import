"""Compatibility facade for legacy forecasting imports."""

from app.services.forecasting.core import (
    clear_forecasting_cache,
    get_forecasting_data,
    get_forecasting_decision_support_data,
    get_forecasting_metadata,
    get_forecasting_page_context,
    get_forecasting_shell_context,
)
from app.services.forecasting.jobs import (
    get_forecasting_decision_support_job_status,
    start_forecasting_decision_support_job,
)

__all__ = [
    "clear_forecasting_cache",
    "get_forecasting_data",
    "get_forecasting_decision_support_data",
    "get_forecasting_decision_support_job_status",
    "get_forecasting_metadata",
    "get_forecasting_page_context",
    "get_forecasting_shell_context",
    "start_forecasting_decision_support_job",
]
