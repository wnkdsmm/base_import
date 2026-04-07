from __future__ import annotations

"""Compatibility package exports for legacy ``app.services`` imports.

Prefer direct imports from canonical service modules in new code.
"""

from app.compat import install_lazy_exports

_LEGACY_EXPORTS = {
    "build_fire_map_html": ("app.services.fire_map_service", "build_fire_map_html"),
    "get_column_search_table_options": ("app.services.table_options", "get_column_search_table_options"),
    "get_dashboard_page_context": ("app.dashboard.service", "get_dashboard_page_context"),
    "get_fire_map_table_options": ("app.services.table_options", "get_fire_map_table_options"),
    "import_uploaded_data": ("app.services.pipeline_service", "import_uploaded_data"),
    "resolve_selected_table": ("app.services.table_options", "resolve_selected_table"),
    "run_profiling_for_table": ("app.services.pipeline_service", "run_profiling_for_table"),
    "save_uploaded_file": ("app.services.pipeline_service", "save_uploaded_file"),
}

__all__, __getattr__, __dir__ = install_lazy_exports(__name__, globals(), _LEGACY_EXPORTS)
