from __future__ import annotations

from datetime import datetime
from typing import Any


def _format_integer(value: Any) -> str:
    try:
        return f"{int(round(float(value))):,}".replace(",", " ")
    except (TypeError, ValueError):
        return "0"


def _format_number(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "0"
    if abs(numeric - round(numeric)) < 1e-9:
        return f"{int(round(numeric)):,}".replace(",", " ")
    return f"{numeric:,.1f}".replace(",", " ").replace(".", ",")


def _format_percent(value: float) -> str:
    rounded = round(value, 1)
    if abs(rounded - round(rounded)) < 1e-9:
        return f"{int(round(rounded))}%"
    return f"{str(rounded).replace('.', ',')}%"


def _format_datetime(value: datetime) -> str:
    return value.strftime("%d.%m.%Y %H:%M")
