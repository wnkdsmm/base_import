from __future__ import annotations

"""Compatibility package exports for legacy ``app.dashboard`` imports.

Prefer direct imports from ``app.dashboard.service`` in new code.
"""

from app.compat import install_lazy_exports

_EXPORTS = {
    "build_dashboard_context": ("app.dashboard.service", "build_dashboard_context"),
    "get_dashboard_data": ("app.dashboard.service", "get_dashboard_data"),
    "get_dashboard_page_context": ("app.dashboard.service", "get_dashboard_page_context"),
    "get_dashboard_shell_context": ("app.dashboard.service", "get_dashboard_shell_context"),
}

__all__, __getattr__, __dir__ = install_lazy_exports(__name__, globals(), _EXPORTS)
