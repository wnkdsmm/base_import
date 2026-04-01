from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from app.db_metadata import get_table_columns_cached, get_table_signature_cached
from app.statistics_constants import (
    BUILDING_CATEGORY_COLUMN,
    BUILDING_CAUSE_COLUMN,
    COLUMN_LABELS,
    DAMAGE_GROUP_LABEL,
    DAMAGE_GROUP_OPTION_LABEL,
    DAMAGE_GROUP_OPTION_VALUE,
    DISTRIBUTION_GROUPS,
    GENERAL_CAUSE_COLUMN,
    OPEN_AREA_CAUSE_COLUMN,
    RISK_CATEGORY_COLUMN,
)
from config.db import engine

from .data_access import _fetch_table_years, _resolve_table_column_name
from .utils import _extract_year_from_name, _parse_year, _select_tables


def _collect_dashboard_metadata(table_names: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    resolved_table_names = list(table_names) if table_names is not None else list(_select_tables(list(get_table_signature_cached())))

    tables: List[Dict[str, Any]] = []
    errors: List[str] = []

    with engine.connect() as conn:
        for table_name in resolved_table_names:
            try:
                columns = get_table_columns_cached(table_name)
                column_set = set(columns)
                table_year = _extract_year_from_name(table_name)
                years = _fetch_table_years(conn, table_name, column_set)
                if not years and table_year is not None:
                    years = [table_year]

                tables.append(
                    {
                        "name": table_name,
                        "column_set": column_set,
                        "years": years,
                        "table_year": table_year,
                    }
                )
            except Exception as exc:
                errors.append(f"{table_name}: {exc}")

    table_options = [{"value": "all", "label": "Все таблицы"}] + [
        {"value": table["name"], "label": table["name"]} for table in tables
    ]
    group_column_options = _collect_group_column_options(tables)
    preferred_default_columns = [
        RISK_CATEGORY_COLUMN,
        BUILDING_CATEGORY_COLUMN,
        GENERAL_CAUSE_COLUMN,
        OPEN_AREA_CAUSE_COLUMN,
        BUILDING_CAUSE_COLUMN,
    ]
    default_group_column = ""
    for candidate in preferred_default_columns:
        if any(item["value"] == candidate for item in group_column_options):
            default_group_column = candidate
            break
    if not default_group_column:
        default_group_column = group_column_options[0]["value"] if group_column_options else ""

    return {
        "tables": tables,
        "table_options": table_options,
        "default_group_column": default_group_column,
        "errors": errors,
    }


def _resolve_selected_tables(tables: List[Dict[str, Any]], table_name: str) -> List[Dict[str, Any]]:
    if not table_name or table_name == "all":
        return tables
    return [table for table in tables if table["name"] == table_name]


def _collect_year_options(tables: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    years = sorted({year for table in tables for year in table["years"]}, reverse=True)
    return [{"value": str(year), "label": str(year)} for year in years]


def _collect_group_column_options(tables: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    result = []
    for group_label, columns in DISTRIBUTION_GROUPS:
        if group_label == DAMAGE_GROUP_LABEL:
            result.append(
                {
                    "value": DAMAGE_GROUP_OPTION_VALUE,
                    "label": DAMAGE_GROUP_OPTION_LABEL,
                    "group": group_label,
                }
            )
            continue
        for column_name in columns:
            if not any(_resolve_table_column_name(table, column_name) for table in tables):
                continue
            result.append(
                {
                    "value": column_name,
                    "label": COLUMN_LABELS.get(column_name, column_name),
                    "group": group_label,
                }
            )
    return result


def _resolve_group_column(group_column: str, options: List[Dict[str, str]], default_value: str) -> str:
    available_values = [item["value"] for item in options]
    if group_column in available_values:
        return group_column
    if default_value in available_values:
        return default_value
    return available_values[0] if available_values else ""


def _is_damage_group_selection(group_column: str) -> bool:
    return group_column == DAMAGE_GROUP_OPTION_VALUE


def _resolve_dashboard_filters(
    metadata: Dict[str, Any],
    table_name: str,
    year: str,
    group_column: str,
) -> Dict[str, Any]:
    selected_tables = _resolve_selected_tables(metadata["tables"], table_name)
    available_years = _collect_year_options(selected_tables)
    requested_year = _parse_year(year)
    available_year_values = {item["value"] for item in available_years}
    selected_year = requested_year if requested_year is not None and str(requested_year) in available_year_values else None
    available_group_columns = _collect_group_column_options(selected_tables)
    selected_group_column = _resolve_group_column(
        group_column or metadata["default_group_column"],
        available_group_columns,
        metadata["default_group_column"],
    )
    selected_table_name = table_name if any(item["value"] == table_name for item in metadata["table_options"]) else "all"
    return {
        "selected_tables": selected_tables,
        "available_years": available_years,
        "selected_year": selected_year,
        "available_group_columns": available_group_columns,
        "selected_group_column": selected_group_column,
        "selected_table_name": selected_table_name,
    }


__all__ = [
    "_collect_dashboard_metadata",
    "_collect_group_column_options",
    "_collect_year_options",
    "_is_damage_group_selection",
    "_resolve_dashboard_filters",
    "_resolve_group_column",
    "_resolve_selected_tables",
]
