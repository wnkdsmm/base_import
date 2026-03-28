import math
import re
from dataclasses import dataclass
from functools import lru_cache

from sqlalchemy import inspect, text

from app.db_metadata import get_table_columns_cached, get_table_names_cached, invalidate_db_metadata_cache
from config.db import engine


DEFAULT_TABLE_PAGE_SIZE = 100
TABLE_PAGE_SIZE_OPTIONS = (50, 100, 500)
_PHYSICAL_ROW_ORDER_FALLBACKS = {
    "postgresql": "ctid ASC",
    "sqlite": "rowid ASC",
}


@dataclass(frozen=True)
class TableOrderStrategy:
    order_by_sql: str
    source: str
    columns: tuple[str, ...] = ()
    note: str | None = None


def _quote_identifier(identifier):
    return '"' + str(identifier).replace('"', '""') + '"'


def invalidate_table_order_cache(table_name: str | None = None) -> None:
    _get_table_order_strategy_cached.cache_clear()


def _build_order_by_columns_sql(columns) -> str:
    return ", ".join(f"{_quote_identifier(column)} ASC" for column in columns)


def _get_physical_row_order_sql() -> str | None:
    return _PHYSICAL_ROW_ORDER_FALLBACKS.get(engine.dialect.name)


def _get_unique_key_candidates(inspector_obj, table_name: str, available_columns, nullable_by_column):
    candidates = []
    seen_columns = set()

    raw_candidates = []
    try:
        raw_candidates.extend(
            {
                "name": constraint.get("name") or "",
                "columns": tuple(constraint.get("column_names") or ()),
            }
            for constraint in (inspector_obj.get_unique_constraints(table_name) or [])
        )
    except NotImplementedError:
        pass

    try:
        raw_candidates.extend(
            {
                "name": index.get("name") or "",
                "columns": tuple(index.get("column_names") or ()),
            }
            for index in (inspector_obj.get_indexes(table_name) or [])
            if index.get("unique")
        )
    except NotImplementedError:
        pass

    for candidate in raw_candidates:
        columns = tuple(column for column in candidate["columns"] if column)
        if not columns or len(columns) != len(candidate["columns"]):
            continue
        if any(column not in available_columns for column in columns):
            continue
        if columns in seen_columns:
            continue

        seen_columns.add(columns)
        has_nullable_columns = any(nullable_by_column.get(column, True) for column in columns)
        candidates.append(
            {
                "name": candidate["name"],
                "columns": columns,
                "has_nullable_columns": has_nullable_columns,
            }
        )

    candidates.sort(key=lambda item: (item["has_nullable_columns"], len(item["columns"]), item["columns"], item["name"]))
    return candidates


@lru_cache(maxsize=256)
def _get_table_order_strategy_cached(table_name: str) -> TableOrderStrategy:
    available_columns = tuple(get_table_columns_cached(table_name))
    if not available_columns:
        raise ValueError(f"Table '{table_name}' has no columns")

    inspector_obj = inspect(engine)
    column_details = inspector_obj.get_columns(table_name)
    nullable_by_column = {column["name"]: bool(column.get("nullable", True)) for column in column_details}

    primary_key_info = inspector_obj.get_pk_constraint(table_name) or {}
    primary_key_columns = tuple(
        column for column in (primary_key_info.get("constrained_columns") or ()) if column in available_columns
    )
    if primary_key_columns:
        return TableOrderStrategy(
            order_by_sql=_build_order_by_columns_sql(primary_key_columns),
            source="primary_key",
            columns=primary_key_columns,
        )

    unique_key_candidates = _get_unique_key_candidates(
        inspector_obj,
        table_name,
        set(available_columns),
        nullable_by_column,
    )
    physical_row_order_sql = _get_physical_row_order_sql()
    if unique_key_candidates:
        selected_candidate = unique_key_candidates[0]
        order_parts = [_build_order_by_columns_sql(selected_candidate["columns"])]
        note = None
        if selected_candidate["has_nullable_columns"] and physical_row_order_sql:
            order_parts.append(physical_row_order_sql)
            note = "Nullable unique keys are stabilized with a physical row tiebreaker."
        elif selected_candidate["has_nullable_columns"]:
            remaining_columns = tuple(column for column in available_columns if column not in selected_candidate["columns"])
            if remaining_columns:
                order_parts.append(_build_order_by_columns_sql(remaining_columns))
            note = "Nullable unique keys fall back to the remaining columns when no physical row identifier exists."
        return TableOrderStrategy(
            order_by_sql=", ".join(order_parts),
            source="unique_key",
            columns=selected_candidate["columns"],
            note=note,
        )

    if physical_row_order_sql:
        # Explicit last-resort fallback for tables without a primary key or unique index.
        return TableOrderStrategy(
            order_by_sql=physical_row_order_sql,
            source="physical_row_fallback",
            note="Uses the engine's physical row identifier because the table has no logical key.",
        )

    return TableOrderStrategy(
        order_by_sql=_build_order_by_columns_sql(available_columns),
        source="all_columns_fallback",
        columns=available_columns,
        note="Best-effort fallback when the engine has no physical row identifier available.",
    )


def _build_ordered_select_query(table_name: str, columns, limit=None, offset=0):
    quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
    order_strategy = _get_table_order_strategy_cached(table_name)

    query_parts = [
        f"SELECT {quoted_columns}",
        f"FROM {_quote_identifier(table_name)}",
        f"ORDER BY {order_strategy.order_by_sql}",
    ]
    if limit is not None:
        query_parts.append("LIMIT :limit")
    if offset:
        query_parts.append("OFFSET :offset")
    return text(" ".join(query_parts))


def _fetch_ordered_rows(conn, table_name: str, columns, limit=None, offset=0):
    safe_limit = max(1, int(limit)) if limit is not None else None
    safe_offset = max(0, int(offset or 0))
    query = _build_ordered_select_query(table_name, columns, limit=safe_limit, offset=safe_offset)

    params = {}
    if safe_limit is not None:
        params["limit"] = safe_limit
    if safe_offset:
        params["offset"] = safe_offset

    result = conn.execute(query, params)
    return [list(row) for row in result]


def _sanitize_table_name(table_name: str) -> str:
    normalized = re.sub(r"\s+", "_", str(table_name).strip())
    normalized = re.sub(r"[^0-9A-Za-zА-Яа-я_]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "table"


def build_modified_table_name(source_table: str) -> str:
    base_name = f"modify_{_sanitize_table_name(source_table)}"
    return base_name[:63]


def get_all_tables():
    """Получить список всех таблиц в базе данных."""
    return get_table_names_cached()


def get_table_columns(table_name):
    """Получить список колонок для таблицы."""
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Invalid table name")
    return get_table_columns_cached(table_name)


def get_table_preview(table_name, selected_columns, limit=100):
    """Превью выбранных колонок из таблицы."""
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Invalid table name")

    requested_columns = [str(column) for column in (selected_columns or []) if column]
    if not requested_columns:
        return [], []

    table_columns = get_table_columns_cached(table_name)
    available_columns = [column for column in requested_columns if column in table_columns]
    if not available_columns:
        return [], []

    with engine.connect() as conn:
        rows = _fetch_ordered_rows(conn, table_name, available_columns, limit=limit)

    return available_columns, rows


def normalize_table_page_size(page_size):
    if page_size in TABLE_PAGE_SIZE_OPTIONS:
        return int(page_size)

    try:
        numeric_page_size = int(page_size)
    except (TypeError, ValueError):
        return DEFAULT_TABLE_PAGE_SIZE

    if numeric_page_size in TABLE_PAGE_SIZE_OPTIONS:
        return numeric_page_size
    return DEFAULT_TABLE_PAGE_SIZE


def get_table_data(table_name, limit=None, offset=0):
    """Получить данные из таблицы с limit/offset и общим числом строк."""
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Invalid table name")

    try:
        columns = get_table_columns_cached(table_name)
        has_limit = limit is not None
        safe_limit = max(1, int(limit)) if has_limit else None
        safe_offset = max(0, int(offset or 0))

        with engine.connect() as conn:
            total_rows = int(conn.execute(text(f"SELECT COUNT(*) FROM {_quote_identifier(table_name)}")).scalar() or 0)
            rows = _fetch_ordered_rows(conn, table_name, columns, limit=safe_limit if has_limit else None, offset=safe_offset)

        return columns, rows, total_rows
    except Exception as exc:
        raise Exception(f"Error accessing table {table_name}: {str(exc)}") from exc


def get_table_page(table_name, page=1, page_size=DEFAULT_TABLE_PAGE_SIZE):
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Invalid table name")

    try:
        safe_page = max(1, int(page))
    except (TypeError, ValueError):
        safe_page = 1

    safe_page_size = normalize_table_page_size(page_size)
    offset = (safe_page - 1) * safe_page_size
    columns, rows, total_rows = get_table_data(table_name, limit=safe_page_size, offset=offset)

    total_pages = max(1, math.ceil(total_rows / safe_page_size)) if total_rows else 1
    if total_rows and offset >= total_rows and safe_page > 1:
        safe_page = total_pages
        offset = (safe_page - 1) * safe_page_size
        columns, rows, total_rows = get_table_data(table_name, limit=safe_page_size, offset=offset)

    displayed_rows = len(rows)
    page_row_start = offset + 1 if displayed_rows else 0
    page_row_end = offset + displayed_rows

    return {
        "table_name": table_name,
        "columns": columns,
        "rows": rows,
        "total_rows": total_rows,
        "page": safe_page,
        "page_size": safe_page_size,
        "total_pages": total_pages,
        "page_row_start": page_row_start,
        "page_row_end": page_row_end,
        "displayed_rows": displayed_rows,
        "has_previous": safe_page > 1,
        "has_next": safe_page < total_pages,
    }


def create_modified_table(source_table: str, selected_columns, target_table: str | None = None):
    if not source_table or not isinstance(source_table, str):
        raise ValueError("Invalid source table name")

    source_columns = get_table_columns(source_table)
    selected_set = {str(column) for column in (selected_columns or []) if column}
    ordered_columns = [column for column in source_columns if column in selected_set]
    if not ordered_columns:
        raise ValueError("Не выбрано ни одной колонки для новой таблицы")

    target_table_name = target_table or build_modified_table_name(source_table)
    replaced_existing = target_table_name in set(get_table_names_cached())

    quoted_target = _quote_identifier(target_table_name)
    quoted_source = _quote_identifier(source_table)
    quoted_columns = ", ".join(_quote_identifier(column) for column in ordered_columns)

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {quoted_target}"))
        conn.execute(text(f"CREATE TABLE {quoted_target} AS SELECT {quoted_columns} FROM {quoted_source}"))

    invalidate_db_metadata_cache()
    try:
        from app.services.pipeline_service import invalidate_runtime_caches

        invalidate_runtime_caches()
    except Exception:
        pass

    return {
        "table_name": target_table_name,
        "selected_columns": ordered_columns,
        "replaced_existing": replaced_existing,
    }


def delete_tables(table_names):
    normalized_names = []
    seen_names = set()

    for table_name in table_names or []:
        if not table_name or not isinstance(table_name, str):
            continue

        normalized_name = str(table_name).strip()
        if not normalized_name or normalized_name in seen_names:
            continue

        normalized_names.append(normalized_name)
        seen_names.add(normalized_name)

    if not normalized_names:
        raise ValueError("Не выбрана ни одна таблица для удаления")

    available_tables = set(get_table_names_cached())
    missing_tables = [table_name for table_name in normalized_names if table_name not in available_tables]
    if missing_tables:
        raise ValueError("Таблицы не найдены: " + ", ".join(missing_tables))

    with engine.begin() as conn:
        for table_name in normalized_names:
            conn.execute(text(f"DROP TABLE IF EXISTS {_quote_identifier(table_name)}"))

    invalidate_db_metadata_cache()
    try:
        from app.services.pipeline_service import invalidate_runtime_caches

        invalidate_runtime_caches()
    except Exception:
        pass

    remaining_tables = get_table_names_cached()
    return {
        "deleted_tables": normalized_names,
        "remaining_tables": remaining_tables,
        "remaining_count": len(remaining_tables),
    }


def delete_table(table_name: str):
    result = delete_tables([table_name])
    deleted_name = result["deleted_tables"][0]
    return {
        "table_name": deleted_name,
        "remaining_tables": result["remaining_tables"],
        "remaining_count": result["remaining_count"],
    }
