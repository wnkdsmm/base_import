from __future__ import annotations

"""Compatibility facade for legacy ML-model imports.

Prefer direct imports from ``app.services.ml_model.core`` and
``app.services.ml_model.jobs`` in new code.
"""

from app.compat import install_lazy_exports

_EXPORTS = {
    "clear_ml_model_cache": ("app.services.ml_model.core", "clear_ml_model_cache"),
    "get_ml_job_status": ("app.services.ml_model.jobs", "get_ml_job_status"),
    "get_ml_model_data": ("app.services.ml_model.core", "get_ml_model_data"),
    "get_ml_model_shell_context": ("app.services.ml_model.core", "get_ml_model_shell_context"),
    "start_ml_model_job": ("app.services.ml_model.jobs", "start_ml_model_job"),
}

__all__, __getattr__, __dir__ = install_lazy_exports(__name__, globals(), _EXPORTS)
