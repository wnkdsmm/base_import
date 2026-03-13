import re

from sqlalchemy import inspect, text

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
    inspector = inspect(engine)
    return inspector.get_table_names()


def get_table_columns(table_name):
    """Получить список колонок для таблицы."""
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Invalid table name")

    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        raise ValueError(f"Table '{table_name}' does not exist")

    return [col["name"] for col in inspector.get_columns(table_name)]


def get_table_preview(table_name, selected_columns, limit=100):
    """Превью выбранных колонок из таблицы."""
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Invalid table name")

    requested_columns = [str(column) for column in (selected_columns or []) if column]
    if not requested_columns:
        return [], []

    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        raise ValueError(f"Table '{table_name}' does not exist")

    table_columns = [col["name"] for col in inspector.get_columns(table_name)]
    available_columns = [column for column in requested_columns if column in table_columns]
    if not available_columns:
        return [], []

    quoted_columns = ", ".join(_quote_identifier(column) for column in available_columns)
    query = text(f"SELECT {quoted_columns} FROM {_quote_identifier(table_name)} LIMIT :limit")

    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit})
        rows = [list(row) for row in result]

    return available_columns, rows


def get_table_data(table_name, limit=100):
    """Получить данные из таблицы с ограничением по строкам"""
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Invalid table name")

    try:
        with engine.connect() as conn:
            inspector = inspect(engine)
            if table_name not in inspector.get_table_names():
                raise ValueError(f"Table '{table_name}' does not exist")

            columns = [col["name"] for col in inspector.get_columns(table_name)]
            query = text(f'SELECT * FROM {_quote_identifier(table_name)} LIMIT :limit')
            result = conn.execute(query, {"limit": limit})
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
    inspector = inspect(engine)
    replaced_existing = target_table_name in inspector.get_table_names()

    quoted_target = _quote_identifier(target_table_name)
    quoted_source = _quote_identifier(source_table)
    quoted_columns = ", ".join(_quote_identifier(column) for column in ordered_columns)

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {quoted_target}"))
        conn.execute(text(f"CREATE TABLE {quoted_target} AS SELECT {quoted_columns} FROM {quoted_source}"))

    return {
        "table_name": target_table_name,
        "selected_columns": ordered_columns,
        "replaced_existing": replaced_existing,
    }
