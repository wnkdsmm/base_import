from __future__ import annotations

from app.services.shared.formatting import (
    format_datetime as _format_datetime,
    format_integer as _format_integer,
    format_number as _format_number,
    format_percent as _format_percent,
)


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'
