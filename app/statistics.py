from __future__ import annotations

"""Compatibility facade for legacy dashboard imports.

Prefer direct imports from ``app.dashboard.service``, ``app.dashboard.cache``,
``app.dashboard.metadata``, and ``app.dashboard.utils`` in new code.
"""

from app.dashboard.cache import _collect_dashboard_metadata_cached, _invalidate_dashboard_caches
from app.dashboard.metadata import (
    _collect_group_column_options,
    _collect_year_options,
    _resolve_group_column,
    _resolve_selected_tables,
)
from app.dashboard.service import _empty_dashboard_data, build_dashboard_context, get_dashboard_data
from app.dashboard.utils import _find_option_label, _parse_year

__all__ = [
    "_collect_dashboard_metadata_cached",
    "_collect_group_column_options",
    "_collect_year_options",
    "_empty_dashboard_data",
    "_find_option_label",
    "_invalidate_dashboard_caches",
    "_parse_year",
    "_resolve_group_column",
    "_resolve_selected_tables",
    "build_dashboard_context",
    "get_dashboard_data",
]
