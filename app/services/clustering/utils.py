from __future__ import annotations

from datetime import datetime
import math
from typing import Any


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'



def _format_datetime(value: datetime) -> str:
    return value.strftime("%d.%m.%Y %H:%M")



def _format_integer(value: Any) -> str:
    try:
        return f"{int(round(float(value))):,}".replace(",", " ")
    except Exception:
        return "0"



def _format_number(value: Any, digits: int = 1) -> str:
    try:
        numeric = float(value)
    except Exception:
        return "—"
    if not math.isfinite(numeric):
        return "—"
    if abs(numeric - round(numeric)) < 1e-9:
        return _format_integer(numeric)
    return f"{numeric:.{digits}f}".replace(".", ",")



def _format_percent(value: float) -> str:
    formatted = _format_number(value * 100.0, 1)
    return f"{formatted}%" if formatted != "—" else "—"