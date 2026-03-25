import re

from sqlalchemy import text

from app.db_metadata import get_table_columns_cached, get_table_names_cached, invalidate_db_metadata_cache
from config.db import engine



def _quote_identifier(identifier):
    return '"' + str(identifier).replace('"', '""') + '"'



def _sanitize_table_name(table_name: str) -> str:
    normalized = re.sub(r"\s+", "_", str(table_name).strip())
    normalized = re.sub(r"[^0-9A-Za-zА-Яа-я_]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "table"



def build_modified_table_name(source_table: str) -> str:
    base_name = f"modify_{_sanitize_table_name(source_table)}"
    return base_name[:63]



def get_all_tables():
    """Получить список всех таблиц в базе данных"""
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

    quoted_columns = ", ".join(_quote_identifier(column) for column in available_columns)
    query = text(f"SELECT {quoted_columns} FROM {_quote_identifier(table_name)} LIMIT :limit")

    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit})
        rows = [list(row) for row in result]

    return available_columns, rows



def get_table_data(table_name, limit=None):
    """Получить данные из таблицы с ограничением по строкам"""
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Invalid table name")

    try:
        columns = get_table_columns_cached(table_name)
        quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
        has_limit = limit is not None
        safe_limit = max(1, int(limit)) if has_limit else None
        query = text(
            f"SELECT {quoted_columns} FROM {_quote_identifier(table_name)}"
            + (" LIMIT :limit" if has_limit else "")
        )

        with engine.connect() as conn:
            result = conn.execute(query, {"limit": safe_limit} if has_limit else {})
            rows = [list(row) for row in result]

        return columns, rows
    except Exception as exc:
        raise Exception(f"Error accessing table {table_name}: {str(exc)}") from exc



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
