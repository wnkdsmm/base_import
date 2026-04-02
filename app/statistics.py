from __future__ import annotations

"""Compatibility facade for legacy dashboard imports.

Prefer direct imports from ``app.dashboard.service``, ``app.dashboard.cache``,
``app.dashboard.metadata``, and ``app.dashboard.utils`` in new code.
"""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "_collect_dashboard_metadata_cached": ("app.dashboard.cache", "_collect_dashboard_metadata_cached"),
    "_collect_group_column_options": ("app.dashboard.metadata", "_collect_group_column_options"),
    "_collect_year_options": ("app.dashboard.metadata", "_collect_year_options"),
    "_empty_dashboard_data": ("app.dashboard.service", "_empty_dashboard_data"),
    "_find_option_label": ("app.dashboard.utils", "_find_option_label"),
    "_invalidate_dashboard_caches": ("app.dashboard.cache", "_invalidate_dashboard_caches"),
    "_parse_year": ("app.dashboard.utils", "_parse_year"),
    "_resolve_group_column": ("app.dashboard.metadata", "_resolve_group_column"),
    "_resolve_selected_tables": ("app.dashboard.metadata", "_resolve_selected_tables"),
    "build_dashboard_context": ("app.dashboard.service", "build_dashboard_context"),
    "get_dashboard_data": ("app.dashboard.service", "get_dashboard_data"),
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
