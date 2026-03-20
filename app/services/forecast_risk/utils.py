from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence

from config.db import engine

from .constants import LONG_RESPONSE_THRESHOLD_MINUTES
def _parse_water_supply_flag(count_value: Optional[float], details: str) -> Optional[bool]:
    if count_value is not None:
        return count_value > 0
    normalized = _normalize_match_text(details)
    if not normalized:
        return None
    if "отсутств" in normalized or ("нет" in normalized and "дан" not in normalized):
        return False
    if any(token in normalized for token in ["вода", "водо", "гидрант", "водоем", "водоисточ", "цистерн"]):
        return True
    return None


def _truthy_value(value: Any) -> Optional[bool]:
    normalized = _normalize_match_text(_clean_text(value))
    if not normalized or normalized in {"нет данных", "не указано", "не указан", "-"}:
        return None
    if normalized in {"да", "true", "истина", "1", "есть"} or normalized.startswith("да") or normalized.startswith("име"):
        return True
    if normalized in {"нет", "false", "ложь", "0"} or normalized.startswith("нет"):
        return False
    return None


def _risk_category_score(value: str) -> float:
    normalized = _normalize_match_text(value)
    if not normalized:
        return 0.26
    if "чрезвычай" in normalized or "очень высокий" in normalized:
        return 0.85
    if "высок" in normalized:
        return 0.68
    if "значител" in normalized:
        return 0.56
    if "средн" in normalized:
        return 0.42
    if "умерен" in normalized:
        return 0.30
    if "низк" in normalized:
        return 0.18
    return 0.26


def _is_heating_season(value: date) -> bool:
    return value.month in {9, 10, 11, 12, 1, 2, 3, 4, 5}


def _is_rural_label(value: str) -> bool:
    normalized = _normalize_match_text(value)
    return any(token in normalized for token in ["сель", "деревн", "посел", "село", "хутор", "станиц", "аул"])


def _calculate_response_minutes(start_time: Optional[datetime], end_time: Optional[datetime]) -> Optional[float]:
    if start_time is None or end_time is None:
        return None
    delta_minutes = (end_time - start_time).total_seconds() / 60.0
    if delta_minutes < 0 and delta_minutes > -1440.0:
        delta_minutes += 1440.0
    if delta_minutes < 0 or delta_minutes > 240.0:
        return None
    return round(delta_minutes, 1)


def _parse_datetime_text(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    for fmt in ["%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%d.%m.%Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(text_value, fmt)
        except ValueError:
            continue
    return None


def _pick_territory_label(primary_value: Any, district_value: str) -> str:
    for raw_value in [primary_value, district_value]:
        value = _clean_text(raw_value)
        if value and _normalize_match_text(value) not in {"нет данных", "не указано", "-"}:
            return value if len(value) <= 72 else value[:69].rstrip() + "..."
    return "Территория не указана"


def _scan_columns(columns: Sequence[str], token_groups: Sequence[Sequence[str]]) -> List[str]:
    matches: List[str] = []
    for column_name in columns:
        normalized_column = _normalize_match_text(column_name)
        if any(all(token in normalized_column for token in group) for group in token_groups):
            matches.append(column_name)
    return matches[:4]


def _counter_top_label(counter: Counter, fallback: str) -> str:
    return counter.most_common(1)[0][0] if counter else fallback


def _apply_history_window(records: Sequence[Dict[str, Any]], history_window: str) -> List[Dict[str, Any]]:
    if not records or history_window == "all":
        return list(records)
    latest_year = max(record["date"].year for record in records)
    if history_window == "recent_3":
        min_year = latest_year - 2
    elif history_window == "recent_5":
        min_year = latest_year - 4
    else:
        return list(records)
    return [record for record in records if record["date"].year >= min_year]


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
                common_tokens = {token for token in normalized_candidate.split(" ") if token} & {token for token in normalized_column.split(" ") if token}
                if common_tokens:
                    score = sum(len(token) for token in common_tokens)
            if score > best_score:
                best_score = score
                best_match = column_name
    return best_match if best_score >= 8 else ""


def _clean_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _to_float_or_none(value: Any) -> Optional[float]:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso_date(value: Any) -> date:
    return datetime.strptime(str(value), "%Y-%m-%d").date()


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
    return f"{int(round(rounded))}%" if abs(rounded - round(rounded)) < 1e-9 else f"{str(rounded).replace('.', ',')}%"


def _format_probability(value: float) -> str:
    return _format_percent(value * 100.0)


def _unique_non_empty(values: Sequence[str]) -> List[str]:
    items: List[str] = []
    for value in values:
        text_value = _clean_text(value)
        if text_value and text_value not in items:
            items.append(text_value)
    return items


def _quote_identifier(identifier: str) -> str:
    return engine.dialect.identifier_preparer.quote(identifier)


def _normalize_match_text(value: str) -> str:
    return " ".join(str(value).lower().replace("ё", "е").replace("/", " ").replace("-", " ").split())


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