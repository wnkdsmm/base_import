from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from app.services.shared.formatting import (
    format_datetime as _format_datetime,
    format_number_two_decimals as _format_number,
    format_percentage as _format_percentage,
)
from app.table_catalog import select_user_table_names
from config.db import engine

DASHBOARD_HORIZON_OPTIONS: tuple[int, ...] = (7, 14, 30)


def build_horizon_day_options() -> list[Dict[str, str]]:
    return [{"value": str(day), "label": f"{day} дней"} for day in DASHBOARD_HORIZON_OPTIONS]


def _select_tables(table_names: List[str]) -> List[str]:
    return select_user_table_names(table_names)


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


__all__ = [
    "DASHBOARD_HORIZON_OPTIONS",
    "build_horizon_day_options",
]
