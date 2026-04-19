from __future__ import annotations

from typing import Any, Callable, Optional

import numpy as np

from app.services.forecasting.utils import _format_integer, _format_number, _format_signed_percent

MISSING_DISPLAY = 'вЂ”'


def _is_missing_metric(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ''
    try:
        return bool(np.isnan(value))
    except TypeError:
        return False


def _format_optional_value(value: Any, formatter: Callable[[Any], str]) -> str:
    return formatter(value) if not _is_missing_metric(value) else MISSING_DISPLAY


def _format_optional_number(value: Any) -> str:
    return _format_optional_value(value, lambda item: _format_number(float(item)))


def _format_optional_percent(value: Any) -> str:
    return _format_optional_value(value, lambda item: f"{_format_number(float(item))}%")


def _format_optional_signed_percent(value: Any) -> str:
    return _format_optional_value(value, lambda item: _format_signed_percent(float(item)))


def _format_optional_integer(value: Any) -> str:
    return _format_optional_value(value, lambda item: _format_integer(int(item)))


def _format_optional_text(value: Any) -> str:
    return _format_optional_value(value, lambda item: str(item).strip())


def _first_present(*values: Any) -> Any:
    for value in values:
        if not _is_missing_metric(value):
            return value
    return None


def _format_first_present(formatter: Callable[[Any], str], *values: Any) -> str:
    return _format_optional_value(_first_present(*values), formatter)


def _format_row_display(
    row: Optional[dict[str, Any]],
    display_key: str,
    raw_key: str,
    raw_formatter: Callable[[Any], str],
) -> str:
    if not row:
        return MISSING_DISPLAY
    display_value = row.get(display_key)
    if not _is_missing_metric(display_value):
        return _format_optional_text(display_value)
    return raw_formatter(row.get(raw_key))


__all__ = [
    'MISSING_DISPLAY',
    '_first_present',
    '_format_first_present',
    '_format_optional_integer',
    '_format_optional_number',
    '_format_optional_percent',
    '_format_optional_signed_percent',
    '_format_optional_text',
    '_format_optional_value',
    '_format_row_display',
    '_is_missing_metric',
]
