from __future__ import annotations

"""Compatibility facade for legacy access-points imports.

Prefer direct imports from ``app.services.access_points.core`` in new code.
"""

from app.compat import install_lazy_exports

_EXPORTS = {
    "clear_access_points_cache": ("app.services.access_points.core", "clear_access_points_cache"),
    "get_access_points_data": ("app.services.access_points.core", "get_access_points_data"),
    "get_access_points_page_context": ("app.services.access_points.core", "get_access_points_page_context"),
    "get_access_points_shell_context": ("app.services.access_points.core", "get_access_points_shell_context"),
}

__all__, __getattr__, __dir__ = install_lazy_exports(__name__, globals(), _EXPORTS)
