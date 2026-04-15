from __future__ import annotations

import re
from typing import Any, Sequence

from sqlalchemy import text

from app.db_metadata import get_table_names_cached
from app.table_invalidation import invalidate_table_related_caches
from app.table_metadata import get_table_columns
from config.db import engine

_NO_SELECTED_COLUMNS_MESSAGE = (
    "\u041d\u0435 \u0432\u044b\u0431\u0440\u0430\u043d\u043e \u043d\u0438 \u043e\u0434\u043d\u043e\u0439 "
    "\u043a\u043e\u043b\u043e\u043d\u043a\u0438 \u0434\u043b\u044f \u043d\u043e\u0432\u043e\u0439 "
    "\u0442\u0430\u0431\u043b\u0438\u0446\u044b"
)
_NO_TABLES_SELECTED_MESSAGE = (
    "\u041d\u0435 \u0432\u044b\u0431\u0440\u0430\u043d\u0430 \u043d\u0438 \u043e\u0434\u043d\u0430 "
    "\u0442\u0430\u0431\u043b\u0438\u0446\u0430 \u0434\u043b\u044f \u0443\u0434\u0430\u043b\u0435\u043d\u0438\u044f"
)
_MISSING_TABLES_MESSAGE = "\u0422\u0430\u0431\u043b\u0438\u0446\u044b \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u044b: "


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _sanitize_table_name(table_name: str) -> str:
    normalized = re.sub(r"\s+", "_", str(table_name).strip())
    normalized = re.sub(r"[^0-9A-Za-z\u0410-\u042f\u0430-\u044f_]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "table"


def build_modified_table_name(source_table: str) -> str:
    base_name = f"modify_{_sanitize_table_name(source_table)}"
    return base_name[:63]


def create_modified_table(
    source_table: str,
    selected_columns: Sequence[Any],
    target_table: str | None = None,
    *,
    db_engine=engine,
) -> dict[str, Any]:
    if not source_table or not isinstance(source_table, str):
        raise ValueError("Invalid source table name")

    source_columns = get_table_columns(source_table)
    selected_set = {str(column) for column in (selected_columns or []) if column}
    ordered_columns = [column for column in source_columns if column in selected_set]
    if not ordered_columns:
        raise ValueError(_NO_SELECTED_COLUMNS_MESSAGE)

    target_table_name = target_table or build_modified_table_name(source_table)
    replaced_existing = target_table_name in set(get_table_names_cached())

    quoted_target = _quote_identifier(target_table_name)
    quoted_source = _quote_identifier(source_table)
    quoted_columns = ", ".join(_quote_identifier(column) for column in ordered_columns)

    with db_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {quoted_target}"))
        conn.execute(text(f"CREATE TABLE {quoted_target} AS SELECT {quoted_columns} FROM {quoted_source}"))

    invalidate_table_related_caches(table_name=target_table_name)

    return {
        "table_name": target_table_name,
        "selected_columns": ordered_columns,
        "replaced_existing": replaced_existing,
    }


def delete_tables(table_names: Sequence[Any], *, db_engine=engine) -> dict[str, Any]:
    normalized_names: list[str] = []
    seen_names: set[str] = set()

    for table_name in table_names or []:
        if not table_name or not isinstance(table_name, str):
            continue

        normalized_name = str(table_name).strip()
        if not normalized_name or normalized_name in seen_names:
            continue

        normalized_names.append(normalized_name)
        seen_names.add(normalized_name)

    if not normalized_names:
        raise ValueError(_NO_TABLES_SELECTED_MESSAGE)

    available_tables = set(get_table_names_cached())
    missing_tables = [table_name for table_name in normalized_names if table_name not in available_tables]
    if missing_tables:
        raise ValueError(_MISSING_TABLES_MESSAGE + ", ".join(missing_tables))

    with db_engine.begin() as conn:
        for table_name in normalized_names:
            conn.execute(text(f"DROP TABLE IF EXISTS {_quote_identifier(table_name)}"))

    invalidate_table_related_caches()

    remaining_tables = get_table_names_cached()
    return {
        "deleted_tables": normalized_names,
        "remaining_tables": remaining_tables,
        "remaining_count": len(remaining_tables),
    }


def delete_table(table_name: str, *, db_engine=engine) -> dict[str, Any]:
    result = delete_tables([table_name], db_engine=db_engine)
    deleted_name = result["deleted_tables"][0]
    return {
        "table_name": deleted_name,
        "remaining_tables": result["remaining_tables"],
        "remaining_count": result["remaining_count"],
    }


__all__ = [
    "build_modified_table_name",
    "create_modified_table",
    "delete_table",
    "delete_tables",
]

