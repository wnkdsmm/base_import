from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from app.dashboard.cache import _collect_dashboard_metadata_cached
from app.dashboard.metadata import _collect_group_column_options, _resolve_dashboard_filters
from app.dashboard.service import _empty_dashboard_data, build_dashboard_context, get_dashboard_data
from app.dashboard.utils import _find_option_label


def _build_dashboard_error_context(error_message: str, *, plotly_js: str = "") -> Dict[str, Any]:
    return {
        "generated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "filters": {
            "tables": [{"value": "all", "label": "Все таблицы"}],
            "years": [],
            "group_columns": [],
        },
        "initial_data": get_dashboard_data(),
        "errors": [error_message],
        "has_data": False,
        "plotly_js": plotly_js,
    }


def get_dashboard_page_context(
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
) -> Dict[str, Any]:
    try:
        return build_dashboard_context(table_name=table_name, year=year, group_column=group_column)
    except Exception as exc:
        error_context = _build_dashboard_error_context(str(exc))
        del error_context["plotly_js"]
        return error_context


def get_dashboard_shell_context(
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
) -> Dict[str, Any]:
    try:
        metadata = _collect_dashboard_metadata_cached()
        filter_state = _resolve_dashboard_filters(
            metadata=metadata,
            table_name=table_name,
            year=year,
            group_column=group_column or metadata["default_group_column"],
        )
        resolved_group_columns = filter_state["available_group_columns"]
        available_group_columns = resolved_group_columns or _collect_group_column_options(metadata["tables"])
        selected_group_column = filter_state["selected_group_column"]
        if not resolved_group_columns and available_group_columns:
            has_selected_group = any(item["value"] == selected_group_column for item in available_group_columns)
            if not has_selected_group:
                selected_group_column = available_group_columns[0]["value"]

        initial_data = _empty_dashboard_data()
        initial_data["bootstrap_mode"] = "deferred"
        initial_data["filters"]["table_name"] = filter_state["selected_table_name"]
        initial_data["filters"]["year"] = str(filter_state["selected_year"]) if filter_state["selected_year"] is not None else "all"
        initial_data["filters"]["group_column"] = selected_group_column
        initial_data["filters"]["available_tables"] = metadata["table_options"]
        initial_data["filters"]["available_years"] = filter_state["available_years"]
        initial_data["filters"]["available_group_columns"] = available_group_columns
        initial_data["scope"]["table_label"] = _find_option_label(
            metadata["table_options"],
            filter_state["selected_table_name"],
            "Все таблицы",
        )
        initial_data["scope"]["year_label"] = str(filter_state["selected_year"]) if filter_state["selected_year"] is not None else "Все годы"
        initial_data["scope"]["group_label"] = _find_option_label(
            available_group_columns,
            selected_group_column,
            "Нет данных",
        )

        return {
            "generated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "filters": {
                "tables": metadata["table_options"],
                "years": filter_state["available_years"],
                "group_columns": available_group_columns,
            },
            "initial_data": initial_data,
            "errors": list(dict.fromkeys(metadata["errors"])),
            "has_data": bool(metadata["tables"]),
            "plotly_js": "",
        }
    except Exception as exc:
        return _build_dashboard_error_context(str(exc), plotly_js="")
