from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Sequence

MISSING_VALUE = "—"


def format_integer(value: Any) -> str:
    try:
        return f"{int(round(float(value))):,}".replace(",", " ")
    except (TypeError, ValueError):
        return "0"


def format_number(value: Any, digits: int = 1, integer: bool = False) -> str:
    if value is None:
        return MISSING_VALUE
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return MISSING_VALUE
    if not math.isfinite(numeric):
        return MISSING_VALUE
    if integer or abs(numeric - round(numeric)) < 1e-9:
        return format_integer(numeric)
    if digits == 2:
        return f"{numeric:,.2f}".replace(",", " ").replace(".", ",")
    return f"{numeric:.{digits}f}".replace(".", ",")


format_number_two_decimals = lambda v, integer=False: format_number(v, digits=2, integer=integer)
format_number_rounded = lambda v: format_number(v, digits=1).replace(" ", "")


def format_percent(value: float) -> str:
    formatted = format_number(value * 100.0, 1)
    return f"{formatted}%" if formatted != MISSING_VALUE else MISSING_VALUE


def format_percentage(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return f"{int(round(value))}%"
    return f"{value:.1f}%".replace(".", ",")


def format_datetime(value: datetime) -> str:
    return value.strftime("%d.%m.%Y %H:%M")


def format_probability(value: float) -> str:
    return format_percent(value * 100.0)


def format_count_range(lower: float, upper: float) -> str:
    lower_bound = max(0, int(math.floor(lower)))
    upper_bound = max(lower_bound, int(math.ceil(upper)))
    return f"{format_integer(lower_bound)}-{format_integer(upper_bound)} пожара"


def format_percent_ratio(value: float) -> str:
    return f"{format_number_rounded(_clamp(value, 0.0, 1.0) * 100.0)}%"


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


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
        return "Нет данных"
    ordered = sorted(values)
    return f"{ordered[0].strftime('%d.%m.%Y')} - {ordered[-1].strftime('%d.%m.%Y')}"


def _format_datetime(value: datetime) -> str:
    return value.strftime("%d.%m.%Y %H:%M")
