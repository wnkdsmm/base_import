from __future__ import annotations

from typing import Any, Callable, Iterable, Sequence


def select_expression_or_fallback(
    column_name: str | None,
    expression_builder: Callable[[str], str],
    *,
    fallback: str,
) -> str:
    if not column_name:
        return fallback
    return expression_builder(column_name)


def build_scope_conditions(
    resolved_columns: dict[str, str],
    *,
    date_field: str,
    date_expression_builder: Callable[[str], str],
    text_expression_builder: Callable[[str], str],
    text_filters: Iterable[tuple[str, object]],
    min_year: int | None = None,
    selected_year: int | None = None,
    all_value: str = "all",
) -> tuple[str | None, list[str], dict[str, Any], bool]:
    date_column = resolved_columns.get(date_field)
    if not date_column:
        return None, [], {}, True

    date_expression = date_expression_builder(date_column)
    conditions = [f"{date_expression} IS NOT NULL"]
    params: dict[str, Any] = {}

    if min_year is not None:
        conditions.append(f"EXTRACT(YEAR FROM {date_expression}) >= :min_year")
        params["min_year"] = min_year
    if selected_year is not None:
        conditions.append(f"EXTRACT(YEAR FROM {date_expression}) = :selected_year")
        params["selected_year"] = selected_year

    for field_name, selected_value in text_filters:
        normalized_value = str(selected_value or "").strip() or all_value
        if normalized_value == all_value:
            continue
        column_name = resolved_columns.get(field_name)
        if not column_name:
            return date_expression, conditions, params, False
        conditions.append(f"{text_expression_builder(column_name)} = :{field_name}")
        params[field_name] = normalized_value

    return date_expression, conditions, params, True


def build_select_parts(
    resolved_columns: dict[str, str],
    *,
    text_aliases: dict[str, str],
    numeric_aliases: dict[str, str],
    text_expression_builder: Callable[[str], str],
    numeric_expression_builder: Callable[[str], str],
) -> list[str]:
    select_parts: list[str] = []

    for key, alias in text_aliases.items():
        column_name = resolved_columns.get(key)
        if column_name:
            select_parts.append(f"{text_expression_builder(column_name)} AS {alias}")

    for key, alias in numeric_aliases.items():
        column_name = resolved_columns.get(key)
        if column_name:
            select_parts.append(f"{numeric_expression_builder(column_name)} AS {alias}")

    return select_parts
