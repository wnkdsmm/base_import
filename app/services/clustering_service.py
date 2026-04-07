from __future__ import annotations

"""Compatibility facade for legacy clustering imports.

Prefer direct imports from ``app.services.clustering.core`` and
``app.services.clustering.jobs`` in new code.
"""

from app.compat import install_lazy_exports

_EXPORTS = {
    "get_clustering_data": ("app.services.clustering.core", "get_clustering_data"),
    "get_clustering_job_status": ("app.services.clustering.jobs", "get_clustering_job_status"),
    "get_clustering_page_context": ("app.services.clustering.core", "get_clustering_page_context"),
    "start_clustering_job": ("app.services.clustering.jobs", "start_clustering_job"),
}

__all__, __getattr__, __dir__ = install_lazy_exports(__name__, globals(), _EXPORTS)
