from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from app.statistics import (
    _collect_dashboard_metadata_cached,
    _collect_group_column_options,
    _collect_year_options,
    _empty_dashboard_data,
    _find_option_label,
    _parse_year,
    _resolve_group_column,
    _resolve_selected_tables,
    build_dashboard_context,
    get_dashboard_data,
)


def get_dashboard_page_context(
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
) -> Dict[str, Any]:
    try:
        return build_dashboard_context(table_name=table_name, year=year, group_column=group_column)
    except Exception as exc:
        return {
            "generated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "filters": {
                "tables": [{"value": "all", "label": "Все таблицы"}],
                "years": [],
                "group_columns": [],
            },
            "initial_data": get_dashboard_data(),
            "errors": [str(exc)],
            "has_data": False,
        }


def get_dashboard_shell_context(
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
) -> Dict[str, Any]:
    try:
        metadata = _collect_dashboard_metadata_cached()
        selected_tables = _resolve_selected_tables(metadata["tables"], table_name)
        available_years = _collect_year_options(selected_tables)
        requested_year = _parse_year(year)
        available_year_values = {item["value"] for item in available_years}
        selected_year = requested_year if requested_year is not None and str(requested_year) in available_year_values else None
        available_group_columns = _collect_group_column_options(selected_tables) or _collect_group_column_options(metadata["tables"])
        selected_group_column = _resolve_group_column(
            group_column or metadata["default_group_column"],
            available_group_columns,
            metadata["default_group_column"],
        )
        selected_table_name = table_name if any(item["value"] == table_name for item in metadata["table_options"]) else "all"

        initial_data = _empty_dashboard_data()
        initial_data["bootstrap_mode"] = "deferred"
        initial_data["filters"]["table_name"] = selected_table_name
        initial_data["filters"]["year"] = str(selected_year) if selected_year is not None else "all"
        initial_data["filters"]["group_column"] = selected_group_column
        initial_data["filters"]["available_tables"] = metadata["table_options"]
        initial_data["filters"]["available_years"] = available_years
        initial_data["filters"]["available_group_columns"] = available_group_columns
        initial_data["scope"]["table_label"] = _find_option_label(
            metadata["table_options"],
            selected_table_name,
            "Все таблицы",
        )
        initial_data["scope"]["year_label"] = str(selected_year) if selected_year is not None else "Все годы"
        initial_data["scope"]["group_label"] = _find_option_label(
            available_group_columns,
            selected_group_column,
            "Нет данных",
        )

        return {
            "generated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "filters": {
                "tables": metadata["table_options"],
                "years": available_years,
                "group_columns": available_group_columns,
            },
            "initial_data": initial_data,
            "errors": list(dict.fromkeys(metadata["errors"])),
            "has_data": bool(metadata["tables"]),
            "plotly_js": "",
        }
    except Exception as exc:
        return {
            "generated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "filters": {
                "tables": [{"value": "all", "label": "Все таблицы"}],
                "years": [],
                "group_columns": [],
            },
            "initial_data": get_dashboard_data(),
            "errors": [str(exc)],
            "has_data": False,
            "plotly_js": "",
        }
