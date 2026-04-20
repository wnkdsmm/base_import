from __future__ import annotations

from typing import Sequence

from app.db_metadata import get_table_names_cached
from app.statistics_constants import EXCLUDED_TABLE_PREFIXES

_ALL_TABLES_LABEL = "Все таблицы"


def _normalize_table_name(table_name: str) -> str:
    return str(table_name or "").strip()


def is_clean_table_name(table_name: str) -> bool:
    normalized = _normalize_table_name(table_name)
    return normalized.casefold().startswith("clean_") and len(normalized) > len("clean_")


def _table_canonical_key(table_name: str) -> str:
    normalized = _normalize_table_name(table_name)
    if is_clean_table_name(normalized):
        normalized = normalized[len("clean_") :]
    return normalized.casefold()


def is_user_table_name(table_name: str) -> bool:
    normalized = _normalize_table_name(table_name)
    if not normalized:
        return False
    return not normalized.startswith(EXCLUDED_TABLE_PREFIXES) and not normalized.startswith("alembic")


def select_user_table_names(table_names: Sequence[str], *, prefer_clean: bool = False) -> list[str]:
    selected = [_normalize_table_name(table_name) for table_name in table_names if is_user_table_name(str(table_name))]
    if not prefer_clean:
        return selected
    selected_by_key: dict[str, str] = {}
    for table_name in selected:
        key = _table_canonical_key(table_name)
        current = selected_by_key.get(key)
        if current is None:
            selected_by_key[key] = table_name
            continue
        if is_clean_table_name(table_name) and not is_clean_table_name(current):
            selected_by_key[key] = table_name
    return list(selected_by_key.values())


def get_user_table_names(*, prefer_clean: bool = False) -> list[str]:
    return select_user_table_names(get_table_names_cached(), prefer_clean=prefer_clean)


def build_table_options(
    table_names: Sequence[str],
    *,
    include_all: bool = False,
    all_label: str = _ALL_TABLES_LABEL,
    prefer_clean: bool = False,
) -> list[dict[str, str]]:
    options = [
        {"value": table_name, "label": table_name}
        for table_name in select_user_table_names(table_names, prefer_clean=prefer_clean)
    ]
    if include_all:
        return [{"value": "all", "label": all_label}, *options]
    return options


def get_user_table_options(
    *,
    include_all: bool = False,
    all_label: str = _ALL_TABLES_LABEL,
    prefer_clean: bool = False,
) -> list[dict[str, str]]:
    return build_table_options(
        get_table_names_cached(),
        include_all=include_all,
        all_label=all_label,
        prefer_clean=prefer_clean,
    )


def resolve_selected_table_value(
    table_options: Sequence[dict[str, str]],
    table_name: str,
    *,
    fallback_value: str = "",
) -> str:
    available_values = [str(option.get("value") or "") for option in table_options if option.get("value")]
    normalized = _normalize_table_name(table_name)
    if normalized in available_values:
        return normalized
    if normalized:
        requested_key = _table_canonical_key(normalized)
        for value in available_values:
            if _table_canonical_key(value) == requested_key:
                return value
    if fallback_value and fallback_value in available_values:
        return fallback_value
    return available_values[0] if available_values else ""
