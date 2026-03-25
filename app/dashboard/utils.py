from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.statistics_constants import EXCLUDED_TABLE_PREFIXES
from config.db import engine


def _select_tables(table_names: List[str]) -> List[str]:
    return [name for name in table_names if not name.startswith(EXCLUDED_TABLE_PREFIXES) and not name.startswith("alembic")]


def _extract_year_from_name(table_name: str) -> Optional[int]:
    match = re.search(r"(19|20)\d{2}", table_name)
    return int(match.group(0)) if match else None


def _parse_year(year_value: str) -> Optional[int]:
    if not year_value or year_value == "all":
        return None
    try:
        return int(year_value)
    except ValueError:
        return None


def _find_option_label(options: List[Dict[str, str]], value: str, fallback: str) -> str:
    for item in options:
        if item["value"] == value:
            return item["label"]
    return fallback


def _quote_identifier(identifier: str) -> str:
    return engine.dialect.identifier_preparer.quote(identifier)


def _date_expression(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    text_value = f"TRIM(CAST({column_sql} AS TEXT))"
    return (
        "CASE "
        f"WHEN {text_value} ~ '^[0-9]{{2}}\\.[0-9]{{2}}\\.[0-9]{{4}}$' THEN TO_DATE({text_value}, 'DD.MM.YYYY') "
        f"WHEN {text_value} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' THEN TO_DATE(SUBSTRING({text_value} FROM 1 FOR 10), 'YYYY-MM-DD') "
        f"WHEN {text_value} ~ '^[0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}}' THEN TO_DATE(SUBSTRING({text_value} FROM 1 FOR 10), 'YYYY/MM/DD') "
        "ELSE NULL END"
    )


def _format_chart_date(value: Any) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%d.%m.%Y")
    return str(value)


def _format_number(value: Any, integer: bool = False) -> str:
    if value is None:
        return "-"
    numeric_value = float(value)
    if integer:
        return f"{int(round(numeric_value)):,}".replace(",", " ")
    if abs(numeric_value - round(numeric_value)) < 1e-9:
        return f"{int(round(numeric_value)):,}".replace(",", " ")
    return f"{numeric_value:,.2f}".replace(",", " ").replace(".", ",")


def _format_percentage(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return f"{int(round(value))}%"
    return f"{value:.1f}%".replace(".", ",")


def _format_signed_number(value: float, integer: bool = False) -> str:
    if value > 0:
        return f"+{_format_number(value, integer=integer)}"
    return _format_number(value, integer=integer)


def _format_period_label(years: List[int]) -> str:
    if not years:
        return "Нет данных"
    normalized = sorted(set(years))
    if len(normalized) == 1:
        return str(normalized[0])
    return f"{normalized[0]}-{normalized[-1]}"


def _format_datetime(value: datetime) -> str:
    return value.strftime("%d.%m.%Y %H:%M")


__all__ = [
    "_select_tables",
    "_extract_year_from_name",
    "_parse_year",
    "_find_option_label",
    "_quote_identifier",
    "_date_expression",
    "_format_chart_date",
    "_format_number",
    "_format_percentage",
    "_format_signed_number",
    "_format_period_label",
    "_format_datetime",
]
