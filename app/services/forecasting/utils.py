from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from statistics import mean
from typing import Any, Dict, List, Optional, Sequence, Tuple

from config.db import engine
from app.services.shared.formatting import (
    _format_float_for_input,
    _format_period,
    _format_signed_percent,
    format_count_range as _format_count_range,
    format_datetime as _format_datetime,
    format_integer as _format_integer,
    format_number as _format_number,
    format_probability as _format_probability,
)

from .constants import FORECAST_DAY_OPTIONS, HISTORY_WINDOW_OPTIONS, PLOTLY_PALETTE


def _resolve_column_name(columns: Sequence[str], candidates: Sequence[str]) -> str:
    if not columns:
        return ""

    normalized_columns = {column: _normalize_match_text(column) for column in columns}
    normalized_candidates = [_normalize_match_text(candidate) for candidate in candidates if candidate]

    for candidate in candidates:
        if candidate in columns:
            return candidate

    best_match = ""
    best_score = -1
    for column_name, normalized_column in normalized_columns.items():
        for normalized_candidate in normalized_candidates:
            if normalized_column == normalized_candidate:
                return column_name

            score = 0
            if normalized_candidate in normalized_column or normalized_column in normalized_candidate:
                score = min(len(normalized_candidate), len(normalized_column))
            else:
                candidate_tokens = {token for token in normalized_candidate.split(" ") if token}
                column_tokens = {token for token in normalized_column.split(" ") if token}
                common_tokens = candidate_tokens & column_tokens
                if common_tokens:
                    score = sum(len(token) for token in common_tokens)

            if score > best_score:
                best_score = score
                best_match = column_name

    return best_match if best_score >= 8 else ""


def _resolve_option_value(options: List[Dict[str, str]], selected_value: str) -> str:
    available_values = {option["value"] for option in options}
    return selected_value if selected_value in available_values else "all"


def _clean_option_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_history_window(value: str) -> str:
    available_values = {option["value"] for option in HISTORY_WINDOW_OPTIONS}
    return value if value in available_values else "all"


def _history_window_label(value: str) -> str:
    for option in HISTORY_WINDOW_OPTIONS:
        if option["value"] == value:
            return option["label"]
    return "\u0412\u0441\u0435 \u0433\u043e\u0434\u044b"


def _apply_history_window(records: List[dict[str, Any]], history_window: str) -> List[dict[str, Any]]:
    if not records or history_window == "all":
        return records

    latest_year = max(record["date"].year for record in records)
    if history_window == "recent_3":
        min_year = latest_year - 2
    elif history_window == "recent_5":
        min_year = latest_year - 4
    else:
        return records

    return [record for record in records if record["date"].year >= min_year]


def _parse_forecast_days(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = 14
    return parsed if parsed in FORECAST_DAY_OPTIONS else 14


def _parse_float(value: str) -> Optional[float]:
    if value is None:
        return None
    normalized = str(value).strip().replace(",", ".")
    if not normalized:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def _to_float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_coordinate(value: Any, minimum: float, maximum: float) -> Optional[float]:
    numeric = _to_float_or_none(value)
    if numeric is None:
        return None
    if numeric < minimum or numeric > maximum:
        return None
    return round(numeric, 6)


def _compute_temperature_slope(pairs: List[tuple[float, float]]) -> Optional[float]:
    if len(pairs) < 5:
        return None
    temperatures = [pair[0] for pair in pairs]
    fire_counts = [pair[1] for pair in pairs]
    avg_temperature = mean(temperatures)
    avg_count = mean(fire_counts)
    variance = sum((value - avg_temperature) ** 2 for value in temperatures)
    if variance <= 0:
        return None
    covariance = sum((temp - avg_temperature) * (count - avg_count) for temp, count in pairs)
    return covariance / variance


def _week_start(value: date) -> date:
    return value - timedelta(days=value.weekday())


def _parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _rolling_average(values: Sequence[float], window: int) -> List[float]:
    items = [float(value) for value in values]
    result: List[float] = []
    for index in range(len(items)):
        start = max(0, index - window + 1)
        subset = items[start:index + 1]
        result.append(round(mean(subset), 2) if subset else 0.0)
    return result


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _scenario_color(tone: str) -> str:
    palette = {
        "fire": PLOTLY_PALETTE["fire"],
        "forest": PLOTLY_PALETTE["forest"],
        "sky": PLOTLY_PALETTE["sky"],
        "sand": PLOTLY_PALETTE["sand"],
    }
    return palette.get(tone, PLOTLY_PALETTE["sky"])


def _forecast_stability_hint(daily_history: List[dict[str, Any]]) -> Tuple[str, str]:
    total_days = len(daily_history)
    active_days = sum(1 for item in daily_history if float(item["count"]) > 0)
    if total_days >= 180 and active_days >= 45:
        return "Выше средней", "истории достаточно, поэтому прогноз обычно стабильнее"
    if total_days >= 60 and active_days >= 15:
        return "Средняя", "истории хватает для ориентировочного сценария"
    return "Ниже средней", "данных мало или они слишком редкие, поэтому важнее смотреть на общий тренд"


def _forecast_level_label(value: float, reference: float) -> Tuple[str, str]:
    if reference <= 0:
        if value <= 0:
            return "На уровне нуля", "sky"
        return "Выше обычного", "fire"

    ratio = value / reference
    if ratio >= 1.2:
        return "Выше обычного", "fire"
    if ratio <= 0.8:
        return "Ниже обычного", "forest"
    return "Около обычного", "sky"


def _relative_delta_text(value: float, reference: float, reference_label: str) -> str:
    if reference <= 0:
        return "нет устойчивой базы для сравнения"
    delta_ratio = (value - reference) / reference
    if abs(delta_ratio) < 0.05:
        return f"почти без изменений {reference_label}"
    return f"{_format_signed_percent(delta_ratio)} {reference_label}"


def _quote_identifier(identifier: str) -> str:
    return engine.dialect.identifier_preparer.quote(identifier)


def _normalize_match_text(value: str) -> str:
    normalized = str(value).lower().replace("?", "?")
    normalized = " ".join(normalized.replace("/", " ").replace("-", " ").split())
    return normalized


def _text_expression(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    return f"NULLIF(TRIM(CAST({column_sql} AS TEXT)), '')"


def _numeric_expression_for_column(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    cleaned = f"NULLIF(REPLACE(REPLACE(REPLACE(CAST({column_sql} AS TEXT), ' ', ''), ',', '.'), CHR(160), ''), '')"
    return f"CASE WHEN {cleaned} ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN ({cleaned})::double precision ELSE NULL END"


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


