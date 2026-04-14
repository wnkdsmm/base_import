from __future__ import annotations

from datetime import date, datetime
from typing import Any, Sequence


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


def _format_signed_percent(value: float) -> str:
    percent_value = value * 100.0
    if abs(percent_value) < 0.05:
        return "0%"
    rounded = round(percent_value, 1)
    if abs(rounded - round(rounded)) < 1e-9:
        text = str(int(round(rounded)))
    else:
        text = str(rounded).replace(".", ",")
    sign = "+" if rounded > 0 else ""
    return f"{sign}{text}%"


def _format_float_for_input(value: float) -> str:
    text_value = f"{value:.2f}".rstrip("0").rstrip(".")
    return text_value.replace(",", ".")


def _format_period(values: Sequence[date]) -> str:
    if not values:
        return "РќРµС‚ РґР°РЅРЅС‹С…"
    ordered = sorted(values)
    return f"{ordered[0].strftime('%d.%m.%Y')} - {ordered[-1].strftime('%d.%m.%Y')}"


def _format_datetime(value: datetime) -> str:
    return value.strftime("%d.%m.%Y %H:%M")
