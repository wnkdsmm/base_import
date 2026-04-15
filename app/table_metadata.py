from __future__ import annotations

from app.db_metadata import (
    get_table_column_set_cached,
    get_table_columns_cached,
    get_table_names_cached,
    get_table_signature_cached,
    table_exists_cached,
)


def get_all_tables(*, force_refresh: bool = False) -> list[str]:
    return get_table_names_cached(force_refresh=force_refresh)


def get_table_columns(table_name: str, *, force_refresh: bool = False) -> list[str]:
    normalized_name = str(table_name or "").strip()
    if not normalized_name:
        raise ValueError("Invalid table name")
    return get_table_columns_cached(normalized_name, force_refresh=force_refresh)


def get_table_column_set(table_name: str, *, force_refresh: bool = False) -> set[str]:
    normalized_name = str(table_name or "").strip()
    if not normalized_name:
        raise ValueError("Invalid table name")
    return get_table_column_set_cached(normalized_name, force_refresh=force_refresh)


def table_exists(table_name: str) -> bool:
    normalized_name = str(table_name or "").strip()
    if not normalized_name:
        return False
    return table_exists_cached(normalized_name)


def get_table_signature() -> tuple[str, ...]:
    return get_table_signature_cached()


__all__ = [
    "get_all_tables",
    "get_table_column_set",
    "get_table_columns",
    "get_table_signature",
    "table_exists",
]
