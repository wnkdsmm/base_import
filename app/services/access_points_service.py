from __future__ import annotations

"""Compatibility facade for legacy access-points imports.

Prefer direct imports from ``app.services.access_points.core`` in new code.
"""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "clear_access_points_cache": ("app.services.access_points.core", "clear_access_points_cache"),
    "get_access_points_data": ("app.services.access_points.core", "get_access_points_data"),
    "get_access_points_page_context": ("app.services.access_points.core", "get_access_points_page_context"),
    "get_access_points_shell_context": ("app.services.access_points.core", "get_access_points_shell_context"),
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
