from __future__ import annotations

from typing import Any

__all__ = [
    "build_fire_map_html",
    "get_column_search_table_options",
    "get_dashboard_page_context",
    "get_fire_map_table_options",
    "import_uploaded_data",
    "resolve_selected_table",
    "run_profiling_for_table",
    "save_uploaded_file",
]


def __getattr__(name: str) -> Any:
    if name == "get_dashboard_page_context":
        from app.services.dashboard_service import get_dashboard_page_context

        return get_dashboard_page_context
    if name == "build_fire_map_html":
        from app.services.fire_map_service import build_fire_map_html

        return build_fire_map_html
    if name in {"import_uploaded_data", "run_profiling_for_table", "save_uploaded_file"}:
        from app.services.pipeline_service import import_uploaded_data, run_profiling_for_table, save_uploaded_file

        return {
            "import_uploaded_data": import_uploaded_data,
            "run_profiling_for_table": run_profiling_for_table,
            "save_uploaded_file": save_uploaded_file,
        }[name]
    if name in {"get_column_search_table_options", "get_fire_map_table_options", "resolve_selected_table"}:
        from app.services.table_options import (
            get_column_search_table_options,
            get_fire_map_table_options,
            resolve_selected_table,
        )

        return {
            "get_column_search_table_options": get_column_search_table_options,
            "get_fire_map_table_options": get_fire_map_table_options,
            "resolve_selected_table": resolve_selected_table,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
