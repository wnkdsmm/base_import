from __future__ import annotations

from typing import Dict, List, Sequence

from app.db_metadata import get_table_names_cached
from app.statistics_constants import EXCLUDED_TABLE_PREFIXES

_ALL_TABLES_LABEL = "Р’СЃРµ С‚Р°Р±Р»РёС†С‹"


def is_user_table_name(table_name: str) -> bool:
    normalized = str(table_name or "").strip()
    if not normalized:
        return False
    return not normalized.startswith(EXCLUDED_TABLE_PREFIXES) and not normalized.startswith("alembic")


def select_user_table_names(table_names: Sequence[str]) -> List[str]:
    return [str(table_name) for table_name in table_names if is_user_table_name(str(table_name))]


def get_user_table_names() -> List[str]:
    return select_user_table_names(get_table_names_cached())


def build_table_options(
    table_names: Sequence[str],
    *,
    include_all: bool = False,
    all_label: str = _ALL_TABLES_LABEL,
) -> List[Dict[str, str]]:
    options = [{"value": table_name, "label": table_name} for table_name in select_user_table_names(table_names)]
    if include_all:
        return [{"value": "all", "label": all_label}, *options]
    return options


def get_user_table_options(
    *,
    include_all: bool = False,
    all_label: str = _ALL_TABLES_LABEL,
) -> List[Dict[str, str]]:
    return build_table_options(
        get_table_names_cached(),
        include_all=include_all,
        all_label=all_label,
    )


def resolve_selected_table_value(
    table_options: Sequence[Dict[str, str]],
    table_name: str,
    *,
    fallback_value: str = "",
) -> str:
    available_values = [str(option.get("value") or "") for option in table_options if option.get("value")]
    normalized = str(table_name or "").strip()
    if normalized in available_values:
        return normalized
    if fallback_value and fallback_value in available_values:
        return fallback_value
    return available_values[0] if available_values else ""
