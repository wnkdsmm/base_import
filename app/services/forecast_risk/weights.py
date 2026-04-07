"""Compatibility facade for legacy forecast-risk weight helpers.

Prefer direct imports from ``app.services.forecast_risk.profile_resolution``,
``app.services.forecast_risk.validation``, and
``app.services.forecast_risk.utils`` in new code.
"""

from __future__ import annotations

from app.compat import install_lazy_exports

_EXPORTS = {
    "_format_decimal": ("app.services.forecast_risk.utils", "_format_decimal"),
    "_rerank_predicted_rows_for_profile": ("app.services.forecast_risk.validation", "_rerank_predicted_rows_for_profile"),
    "resolve_weight_profile_for_records": ("app.services.forecast_risk.profile_resolution", "resolve_weight_profile_for_records"),
}

__all__, __getattr__, __dir__ = install_lazy_exports(__name__, globals(), _EXPORTS)
