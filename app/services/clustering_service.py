from __future__ import annotations

"""Compatibility facade for legacy clustering imports.

Prefer direct imports from ``app.services.clustering.core`` and
``app.services.clustering.jobs`` in new code.
"""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "get_clustering_data": ("app.services.clustering.core", "get_clustering_data"),
    "get_clustering_job_status": ("app.services.clustering.jobs", "get_clustering_job_status"),
    "get_clustering_page_context": ("app.services.clustering.core", "get_clustering_page_context"),
    "start_clustering_job": ("app.services.clustering.jobs", "start_clustering_job"),
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
