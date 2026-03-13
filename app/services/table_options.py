from __future__ import annotations

from typing import Dict, List

from app.db_views import get_all_tables
from app.statistics import get_dashboard_data


EXCLUDED_TABLE_PREFIXES = ("clean_", "final_", "tmp_", "pg_", "sql_")


def _filter_user_tables(table_names: List[str]) -> List[str]:
    return [name for name in table_names if not name.startswith(EXCLUDED_TABLE_PREFIXES)]


def get_user_table_options() -> List[Dict[str, str]]:
    return [{"value": name, "label": name} for name in _filter_user_tables(get_all_tables())]


def get_column_search_table_options() -> List[Dict[str, str]]:
    try:
        options = [
            option
            for option in get_dashboard_data().get("filters", {}).get("available_tables", [])
            if option.get("value") and option.get("value") != "all"
        ]
    except Exception:
        options = []
    return options or get_user_table_options()


def get_fire_map_table_options() -> List[Dict[str, str]]:
    try:
        options = get_dashboard_data().get("filters", {}).get("available_tables", [])
    except Exception:
        options = []

    table_options = []
    seen = set()
    for option in options:
        value = option.get("value")
        if not value or value == "all" or value in seen:
            continue
        seen.add(value)
        table_options.append({"value": value, "label": option.get("label", value)})

    return table_options or get_user_table_options()


def resolve_selected_table(table_options: List[Dict[str, str]], table_name: str = "") -> str:
    available_values = {option["value"] for option in table_options}
    if table_name in available_values:
        return table_name
    return table_options[0]["value"] if table_options else ""
