from __future__ import annotations

"""Compatibility facade for legacy dashboard-service imports.

Prefer direct imports from ``app.dashboard.service`` in new code.
"""

from app.compat import install_lazy_exports

_EXPORTS = {
    "_build_dashboard_error_context": ("app.dashboard.service", "_build_dashboard_error_context"),
    "_collect_dashboard_metadata_cached": ("app.dashboard.cache", "_collect_dashboard_metadata_cached"),
    "_collect_group_column_options": ("app.dashboard.metadata", "_collect_group_column_options"),
    "_empty_dashboard_data": ("app.dashboard.service", "_empty_dashboard_data"),
    "_find_option_label": ("app.dashboard.utils", "_find_option_label"),
    "_resolve_dashboard_filters": ("app.dashboard.metadata", "_resolve_dashboard_filters"),
    "build_dashboard_context": ("app.dashboard.service", "build_dashboard_context"),
    "get_dashboard_data": ("app.dashboard.service", "get_dashboard_data"),
    "get_dashboard_page_context": ("app.dashboard.service", "get_dashboard_page_context"),
    "get_dashboard_shell_context": ("app.dashboard.service", "get_dashboard_shell_context"),
}

__all__, __getattr__, __dir__ = install_lazy_exports(__name__, globals(), _EXPORTS)
