import math
from contextlib import nullcontext
from dataclasses import dataclass
from functools import lru_cache

from sqlalchemy import inspect, text

from app.db_metadata import get_table_columns_cached, register_table_order_cache_invalidator
from app.perf import ensure_sqlalchemy_timing, perf_trace
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


register_table_order_cache_invalidator(_get_table_order_strategy_cached.cache_clear)


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


def get_table_preview(table_name, selected_columns, limit=100):
    ensure_sqlalchemy_timing(engine)
    with perf_trace("table.preview", table_name=table_name, requested_limit=limit) as perf:
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name")

        with perf.span("filter_prep"):
            requested_columns = [str(column) for column in (selected_columns or []) if column]
            if not requested_columns:
                perf.update(requested_columns=0, available_columns=0, returned_rows=0)
                return [], []

            table_columns = get_table_columns_cached(table_name)
            available_columns = [column for column in requested_columns if column in table_columns]
            perf.update(
                requested_columns=len(requested_columns),
                available_columns=len(available_columns),
            )
            if not available_columns:
                perf.update(returned_rows=0)
                return [], []

        with perf.span("payload_render"):
            with engine.connect() as conn:
                rows = _fetch_ordered_rows(conn, table_name, available_columns, limit=limit)
            perf.update(input_rows=len(rows), returned_rows=len(rows))
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


def get_table_data(table_name, limit=None, offset=0, perf=None):
    ensure_sqlalchemy_timing(engine)
    trace = perf or perf_trace("table.data", table_name=table_name, requested_limit=limit, requested_offset=offset)
    trace_context = nullcontext(trace) if perf is not None else trace

    with trace_context as active_perf:
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name")

        try:
            with active_perf.span("filter_prep"):
                columns = get_table_columns_cached(table_name)
                has_limit = limit is not None
                safe_limit = max(1, int(limit)) if has_limit else None
                safe_offset = max(0, int(offset or 0))
                active_perf.update(
                    column_count=len(columns),
                    requested_limit=safe_limit if has_limit else "all",
                    requested_offset=safe_offset,
                )

            with active_perf.span("payload_render"):
                with engine.connect() as conn:
                    total_rows = int(conn.execute(text(f"SELECT COUNT(*) FROM {_quote_identifier(table_name)}")).scalar() or 0)
                    rows = _fetch_ordered_rows(
                        conn,
                        table_name,
                        columns,
                        limit=safe_limit if has_limit else None,
                        offset=safe_offset,
                    )
                active_perf.update(input_rows=total_rows, total_rows=total_rows, returned_rows=len(rows))
                return columns, rows, total_rows
        except Exception as exc:
            if perf is not None:
                active_perf.fail(exc)
            raise Exception(f"Error accessing table {table_name}: {str(exc)}") from exc


def get_table_page(table_name, page=1, page_size=DEFAULT_TABLE_PAGE_SIZE):
    ensure_sqlalchemy_timing(engine)
    with perf_trace("table.page", table_name=table_name, requested_page=page, requested_page_size=page_size) as perf:
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name")

        try:
            safe_page = max(1, int(page))
        except (TypeError, ValueError):
            safe_page = 1

        with perf.span("filter_prep"):
            safe_page_size = normalize_table_page_size(page_size)
            offset = (safe_page - 1) * safe_page_size
            perf.update(page=safe_page, page_size=safe_page_size, requested_offset=offset)

        columns, rows, total_rows = get_table_data(table_name, limit=safe_page_size, offset=offset, perf=perf)

        with perf.span("payload_render"):
            total_pages = max(1, math.ceil(total_rows / safe_page_size)) if total_rows else 1
            if total_rows and offset >= total_rows and safe_page > 1:
                safe_page = total_pages
                offset = (safe_page - 1) * safe_page_size
                columns, rows, total_rows = get_table_data(table_name, limit=safe_page_size, offset=offset, perf=perf)

            displayed_rows = len(rows)
            page_row_start = offset + 1 if displayed_rows else 0
            page_row_end = offset + displayed_rows
            perf.update(
                total_rows=total_rows,
                total_pages=total_pages,
                displayed_rows=displayed_rows,
                page_row_start=page_row_start,
                page_row_end=page_row_end,
            )

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


def build_modified_table_name(source_table: str) -> str:
    from app.table_operations import build_modified_table_name as _build_modified_table_name

    return _build_modified_table_name(source_table)


def get_all_tables():
    from app.table_metadata import get_all_tables as _get_all_tables

    return _get_all_tables()


def get_table_columns(table_name):
    from app.table_metadata import get_table_columns as _get_table_columns

    return _get_table_columns(table_name)


def create_modified_table(source_table: str, selected_columns, target_table: str | None = None):
    from app.table_operations import create_modified_table as _create_modified_table

    return _create_modified_table(
        source_table,
        selected_columns,
        target_table=target_table,
        db_engine=engine,
    )


def delete_tables(table_names):
    from app.table_operations import delete_tables as _delete_tables

    return _delete_tables(table_names, db_engine=engine)


def delete_table(table_name: str):
    from app.table_operations import delete_table as _delete_table

    return _delete_table(table_name, db_engine=engine)
