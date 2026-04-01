from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.statistics_constants import AREA_COLUMN, CAUSE_COLUMNS, DATE_COLUMN, IMPACT_METRIC_CONFIG

from .utils import _date_expression, _quote_identifier

DISTRICT_COLUMN_CANDIDATES = [
    "Район",
    "Муниципальный район",
    "Муниципальное образование",
    "Административный район",
    "Район выезда подразделения",
    "Район пожара",
    "Территория",
]
SEASON_ORDER = ["Зима", "Весна", "Лето", "Осень"]


def _resolve_district_column(table: Dict[str, Any]) -> str:
    for candidate in DISTRICT_COLUMN_CANDIDATES:
        resolved = _resolve_table_column_name(table, candidate)
        if resolved:
            return resolved
    return ""


def _collect_impact_totals(selected_tables: List[Dict[str, Any]], selected_year: Optional[int]) -> Dict[str, float]:
    from config.db import engine

    totals = {metric_key: 0.0 for metric_key in IMPACT_METRIC_CONFIG}

    with engine.connect() as conn:
        for table in selected_tables:
            where_clause = _build_year_filter_clause(table, selected_year)
            if where_clause is None:
                continue
            metric_selects = []
            for metric_key in IMPACT_METRIC_CONFIG:
                metric_expression = _metric_expression(table, metric_key)
                metric_selects.append(f"COALESCE(SUM({metric_expression}), 0) AS {metric_key}")
            query = text(
                f"""
                SELECT {', '.join(metric_selects)}
                FROM {_quote_identifier(table['name'])}
                WHERE {where_clause}
                """
            )
            params = {"selected_year": selected_year} if selected_year is not None and DATE_COLUMN in table["column_set"] else {}
            row = conn.execute(query, params).mappings().one()
            for metric_key in IMPACT_METRIC_CONFIG:
                totals[metric_key] += float(row[metric_key] or 0)

    return totals


def _build_impact_timeline_query(table: Dict[str, Any], selected_year: Optional[int]) -> Optional[str]:
    where_conditions: List[str] = []

    if DATE_COLUMN in table["column_set"]:
        date_expression = _date_expression(DATE_COLUMN)
        where_conditions.append(f"{date_expression} IS NOT NULL")
        year_clause = _build_year_filter_clause(table, selected_year)
        if year_clause is None:
            return None
        if year_clause != "TRUE":
            where_conditions.append(year_clause)
    elif table["table_year"] is not None:
        if selected_year is not None and table["table_year"] != selected_year:
            return None
        date_expression = f"MAKE_DATE({table['table_year']}, 1, 1)"
    else:
        return None

    where_clause = " AND ".join(where_conditions) if where_conditions else "TRUE"
    return f"""
        SELECT
            {date_expression} AS date_value,
            COALESCE(SUM({_metric_expression(table, "deaths")}), 0) AS deaths,
            COALESCE(SUM({_metric_expression(table, "injuries")}), 0) AS injuries,
            COALESCE(SUM({_metric_expression(table, "evacuated")}), 0) AS evacuated,
            COALESCE(SUM({_metric_expression(table, "evacuated_children")}), 0) AS evacuated_children,
            COALESCE(SUM({_metric_expression(table, "rescued_children")}), 0) AS rescued_children
        FROM {_quote_identifier(table["name"])}
        WHERE {where_clause}
        GROUP BY date_value
        ORDER BY date_value
    """


def _resolve_cause_column(table: Dict[str, Any]) -> str:
    for column_name in CAUSE_COLUMNS:
        resolved_column_name = _resolve_table_column_name(table, column_name)
        if resolved_column_name:
            return resolved_column_name
    return ""


def _resolve_table_column_name(table: Dict[str, Any], expected_name: str) -> str:
    if expected_name in table["column_set"]:
        return expected_name

    normalized_expected = _normalize_match_text(expected_name)
    best_match = ""
    best_score = -1

    for column_name in table["column_set"]:
        normalized_column = _normalize_match_text(column_name)
        if normalized_column == normalized_expected:
            return column_name

        score = -1
        if normalized_expected.startswith(normalized_column):
            score = len(normalized_column)
        elif normalized_column.startswith(normalized_expected):
            score = len(normalized_expected)
        else:
            expected_tokens = [token for token in re.split(r"[^\w]+", normalized_expected) if token]
            column_tokens = [token for token in re.split(r"[^\w]+", normalized_column) if token]
            if expected_tokens and column_tokens and expected_tokens[: len(column_tokens)] == column_tokens:
                score = sum(len(token) for token in column_tokens)

        if score > best_score:
            best_score = score
            best_match = column_name

    return best_match if best_score >= 12 else ""


def _metric_expression(table: Dict[str, Any], metric_key: str) -> str:
    column_name = _find_metric_column(table, metric_key)
    if not column_name:
        return "NULL"
    return _numeric_expression_for_column(column_name)


def _find_metric_column(table: Dict[str, Any], metric_key: str) -> str:
    config = IMPACT_METRIC_CONFIG[metric_key]
    columns = list(table["column_set"])
    normalized_columns = {column_name: _normalize_match_text(column_name) for column_name in columns}
    preferred = [_normalize_match_text(value) for value in config.get("preferred", [])]

    for column_name, normalized_name in normalized_columns.items():
        if normalized_name in preferred:
            return column_name

    best_match = ""
    best_score = -1
    for column_name, normalized_name in normalized_columns.items():
        if any(excluded in normalized_name for excluded in config.get("exclude", [])):
            continue

        score = 0
        for token_group in config.get("include_all", []):
            if all(token in normalized_name for token in token_group):
                score = max(score, 5 + len(token_group))
        for token_group in config.get("include_any", []):
            if all(token in normalized_name for token in token_group):
                score = max(score, 3 + len(token_group))
        if "количество" in normalized_name:
            score += 1
        if score > best_score:
            best_score = score
            best_match = column_name

    return best_match if best_score > 0 else ""


def _numeric_expression_for_column(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    cleaned = f"NULLIF(REPLACE(REPLACE(REPLACE(CAST({column_sql} AS TEXT), ' ', ''), ',', '.'), CHR(160), ''), '')"
    return f"CASE WHEN {cleaned} ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN ({cleaned})::double precision ELSE NULL END"


def _normalize_match_text(value: str) -> str:
    normalized = value.lower().replace("ё", "е")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def _build_yearly_query(table: Dict[str, Any]) -> Optional[str]:
    table_name = _quote_identifier(table["name"])
    area_expression = _area_expression(table)

    if DATE_COLUMN in table["column_set"]:
        year_expression = _year_expression(DATE_COLUMN)
        return f"""
            SELECT
                {year_expression} AS year_value,
                COUNT(*) AS fire_count,
                SUM({area_expression}) AS total_area
            FROM {table_name}
            WHERE {year_expression} IS NOT NULL
            GROUP BY year_value
            ORDER BY year_value
        """

    if table["table_year"] is not None:
        return f"""
            SELECT
                {table['table_year']} AS year_value,
                COUNT(*) AS fire_count,
                SUM({area_expression}) AS total_area
            FROM {table_name}
        """

    return None


def _fetch_table_years(conn: Any, table_name: str, column_set: set) -> List[int]:
    if DATE_COLUMN not in column_set:
        return []
    year_expression = _year_expression(DATE_COLUMN)
    query = text(
        f"""
        SELECT DISTINCT {year_expression} AS year_value
        FROM {_quote_identifier(table_name)}
        WHERE {year_expression} IS NOT NULL
        ORDER BY year_value DESC
        """
    )
    return [int(row["year_value"]) for row in conn.execute(query).mappings().all() if row["year_value"] is not None]


def _build_year_filter_clause(table: Dict[str, Any], selected_year: Optional[int]) -> Optional[str]:
    if selected_year is None:
        return "TRUE"
    if DATE_COLUMN in table["column_set"]:
        return f"{_year_expression(DATE_COLUMN)} = :selected_year"
    if table["table_year"] == selected_year:
        return "TRUE"
    return None


def _year_expression(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    return f"NULLIF(SUBSTRING(CAST({column_sql} AS TEXT) FROM '([0-9]{{4}})'), '')::int"


def _month_expression(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    text_value = f"TRIM(CAST({column_sql} AS TEXT))"
    return (
        "CASE "
        f"WHEN {text_value} ~ '^[0-9]{{4}}[-./][0-9]{{1,2}}[-./][0-9]{{1,2}}' "
        f"THEN NULLIF(SUBSTRING({text_value} FROM '^[0-9]{{4}}[-./]([0-9]{{1,2}})'), '')::int "
        f"WHEN {text_value} ~ '^[0-9]{{1,2}}[-./][0-9]{{1,2}}[-./][0-9]{{4}}' "
        f"THEN NULLIF(SUBSTRING({text_value} FROM '^[0-9]{{1,2}}[-./]([0-9]{{1,2}})'), '')::int "
        "ELSE NULL END"
    )


def _area_expression(table: Dict[str, Any]) -> str:
    if AREA_COLUMN not in table["column_set"]:
        return "NULL"
    column_sql = _quote_identifier(AREA_COLUMN)
    cleaned = f"NULLIF(REPLACE(REPLACE(REPLACE(CAST({column_sql} AS TEXT), ' ', ''), ',', '.'), CHR(160), ''), '')"
    return f"CASE WHEN {cleaned} ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN ({cleaned})::double precision ELSE NULL END"


def _resolve_years_in_scope(table: Dict[str, Any], selected_year: Optional[int]) -> List[int]:
    if selected_year is not None:
        return [selected_year] if _build_year_filter_clause(table, selected_year) is not None else []
    if table["years"]:
        return list(table["years"])
    if table["table_year"] is not None:
        return [table["table_year"]]
    return []


__all__ = [
    "DISTRICT_COLUMN_CANDIDATES",
    "SEASON_ORDER",
    "_area_expression",
    "_build_impact_timeline_query",
    "_build_year_filter_clause",
    "_build_yearly_query",
    "_collect_impact_totals",
    "_fetch_table_years",
    "_metric_expression",
    "_month_expression",
    "_numeric_expression_for_column",
    "_resolve_cause_column",
    "_resolve_district_column",
    "_resolve_table_column_name",
    "_resolve_years_in_scope",
    "_year_expression",
]
