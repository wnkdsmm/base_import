from __future__ import annotations

import re
from collections import Counter
from typing import Any, Dict, Iterable, List, Sequence

import pandas as pd

from app.domain.column_matching import MANDATORY_FEATURE_REGISTRY


FEATURE_GROUPS = [
    {
        "id": "time",
        "label": "Время",
        "feature_ids": ["fire_date", "report_time", "arrival_time"],
    },
    {
        "id": "territory",
        "label": "Территория",
        "feature_ids": ["district", "locality", "settlement_type", "coordinates"],
    },
    {
        "id": "incident",
        "label": "Причина и объект",
        "feature_ids": ["cause", "object_category"],
    },
    {
        "id": "response",
        "label": "Реагирование",
        "feature_ids": ["distance_to_fire_station", "water_supply"],
    },
    {
        "id": "consequences",
        "label": "Последствия",
        "feature_ids": ["fatalities", "injuries", "damage"],
    },
]

_EMPTY_TEXT_VALUES = {"", "nan", "nat", "none", "null", "n/a", "na", "-", "—"}


def _normalize_text(value: Any) -> str:
    text = str(value or "").lower().replace("ё", "е")
    text = re.sub(r"[_/#-]+", " ", text)
    text = re.sub(r"[^\w\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_words(value: str) -> List[str]:
    return [word for word in re.findall(r"\w+", value) if word]


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return text.lower() not in _EMPTY_TEXT_VALUES


def _safe_text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def _format_int(value: int) -> str:
    return f"{int(value):,}".replace(",", " ")


def _format_percent(ratio: float) -> str:
    bounded = max(0.0, min(1.0, float(ratio or 0.0)))
    return f"{round(bounded * 100):.0f}%"


def _format_compact_number(value: float) -> str:
    absolute = abs(float(value or 0.0))
    if absolute >= 1_000_000:
        return f"{value / 1_000_000:.1f} млн".replace(".", ",")
    if absolute >= 1_000:
        return f"{value / 1_000:.1f} тыс".replace(".", ",")
    if float(value).is_integer():
        return _format_int(int(value))
    return f"{value:.1f}".replace(".", ",")


def _average(values: Iterable[float]) -> float:
    items = [float(value) for value in values]
    if not items:
        return 0.0
    return sum(items) / len(items)


def _contains_fragment(normalized_name: str, words: Sequence[str], fragment: str) -> bool:
    return bool(
        fragment
        and (
            fragment in normalized_name
            or any(fragment in word for word in words)
        )
    )


def _column_matches_feature(column_name: str, feature: Dict[str, Any]) -> bool:
    normalized_name = _normalize_text(column_name)
    words = _extract_words(normalized_name)
    exclude_tokens = [_normalize_text(token) for token in feature.get("exclude_tokens", []) if token]

    if any(_contains_fragment(normalized_name, words, token) for token in exclude_tokens):
        return False

    for synonym in feature.get("synonyms", []):
        normalized_synonym = _normalize_text(synonym)
        if normalized_name == normalized_synonym:
            return True

    for synonym in feature.get("synonyms", []):
        synonym_tokens = _extract_words(_normalize_text(synonym))
        if len(synonym_tokens) > 1 and all(
            _contains_fragment(normalized_name, words, token) for token in synonym_tokens
        ):
            return True

    for token_set in feature.get("token_sets", []):
        normalized_tokens = [_normalize_text(token) for token in token_set if token]
        if normalized_tokens and all(
            _contains_fragment(normalized_name, words, token) for token in normalized_tokens
        ):
            return True

    return False


def _match_mandatory_features(columns: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    matched: Dict[str, Dict[str, Any]] = {}
    for feature in MANDATORY_FEATURE_REGISTRY:
        feature_id = str(feature["id"])
        matched_columns = [column_name for column_name in columns if _column_matches_feature(column_name, feature)]
        matched[feature_id] = {
            "id": feature_id,
            "label": str(feature["label"]),
            "description": str(feature["description"]),
            "columns": matched_columns,
        }
    return matched


def _coerce_number(value: Any) -> float | None:
    if not _has_value(value):
        return None

    text = str(value).strip().replace("\xa0", "").replace(" ", "")
    if text.count(",") and text.count("."):
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    else:
        text = text.replace(",", ".")

    cleaned = re.sub(r"[^0-9.\-]+", "", text)
    if cleaned in {"", "-", ".", "-."}:
        return None

    try:
        return float(cleaned)
    except ValueError:
        return None


def _summarize_date_range(values: Sequence[Any]) -> str:
    if not values:
        return ""

    series = pd.to_datetime(pd.Series(list(values), dtype="object"), errors="coerce", dayfirst=True)
    valid = series.dropna()
    if valid.empty:
        return ""

    start = valid.min()
    end = valid.max()
    if pd.isna(start) or pd.isna(end):
        return ""

    if start.date() == end.date():
        return start.strftime("%d.%m.%Y")
    return f"{start.strftime('%d.%m.%Y')} - {end.strftime('%d.%m.%Y')}"


def _build_feature_stat(
    feature: Dict[str, Any],
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> Dict[str, Any]:
    row_count = len(rows)
    matched_columns = list(feature.get("columns") or [])
    label = str(feature.get("label") or "Критерий")

    if not matched_columns:
        return {
            "id": feature["id"],
            "label": label,
            "found": False,
            "columns": [],
            "coverage": 0.0,
            "coverage_display": "0%",
            "highlight": "Не распознано",
            "summary": f"{label}: колонка не распознана в текущей таблице.",
            "card_value": "Нет",
            "card_meta": f"{label} не распознан в структуре таблицы.",
            "unique_count": 0,
            "sum_value": 0.0,
            "positive_count": 0,
        }

    column_indexes = [columns.index(column_name) for column_name in matched_columns if column_name in columns]
    non_empty_rows = 0
    observed_values: List[Any] = []
    counter: Counter[str] = Counter()
    numeric_sum = 0.0
    numeric_count = 0
    positive_count = 0

    for row in rows:
        values = [row[index] if index < len(row) else None for index in column_indexes]
        filled_values = [value for value in values if _has_value(value)]
        if filled_values:
            non_empty_rows += 1
            observed_values.extend(filled_values[:1])

        if feature["id"] in {"district", "locality", "settlement_type", "cause", "object_category"} and filled_values:
            counter[_safe_text(filled_values[0])] += 1

        if feature["id"] == "water_supply" and filled_values:
            counter[_safe_text(filled_values[0])] += 1

        if feature["id"] in {"fatalities", "injuries", "damage", "distance_to_fire_station"} and filled_values:
            number = _coerce_number(filled_values[0])
            if number is not None:
                numeric_sum += number
                numeric_count += 1
                if number > 0:
                    positive_count += 1

    coverage = (non_empty_rows / row_count) if row_count else 0.0
    coverage_display = _format_percent(coverage)
    columns_display = ", ".join(matched_columns[:2])
    if len(matched_columns) > 2:
        columns_display += f" и ещё {_format_int(len(matched_columns) - 2)}"

    top_value = ""
    top_share = ""
    unique_count = len(counter)
    if counter:
        top_value, top_count = counter.most_common(1)[0]
        top_share = _format_percent(top_count / max(sum(counter.values()), 1))

    date_range = _summarize_date_range(observed_values) if feature["id"] == "fire_date" else ""
    average_number = (numeric_sum / numeric_count) if numeric_count else 0.0

    highlight = f"{coverage_display} заполнено"
    summary = f"Колонки: {columns_display}. Заполнено {coverage_display} строк."
    card_value = coverage_display
    card_meta = f"{label}: заполнено {coverage_display} строк."

    if feature["id"] == "fire_date" and date_range:
        highlight = date_range
        summary = f"Период: {date_range}. Заполнено {coverage_display} строк."
        card_value = date_range
        card_meta = f"Дата пожара распознана. Заполненность: {coverage_display}."
    elif feature["id"] in {"district", "locality", "settlement_type", "cause", "object_category"} and top_value:
        highlight = top_value
        summary = f"Чаще всего встречается «{top_value}» ({top_share}). Заполнено {coverage_display} строк."
        card_value = top_value
        unique_meta = f"{_format_int(unique_count)} уник." if unique_count else "без разбивки"
        card_meta = f"{unique_meta}; заполнено {coverage_display} строк."
    elif feature["id"] == "coordinates":
        highlight = f"{_format_int(len(matched_columns))} колон."
        summary = f"Найдено {len(matched_columns)} колонок координат. Заполнено {coverage_display} строк."
        card_value = f"{_format_int(len(matched_columns))} колон."
        card_meta = f"Координаты распознаны. Заполненность: {coverage_display}."
    elif feature["id"] == "water_supply" and top_value:
        highlight = coverage_display
        summary = f"Признак водоснабжения найден. Частое значение: «{top_value}». Заполнено {coverage_display} строк."
        card_value = coverage_display
        card_meta = f"Водоснабжение распознано. Заполненность: {coverage_display}."
    elif feature["id"] in {"report_time", "arrival_time"}:
        highlight = coverage_display
        summary = f"{label}: заполнено {coverage_display} строк."
        card_value = coverage_display
        card_meta = f"{label}: данные присутствуют в {coverage_display} строк."
    elif feature["id"] == "distance_to_fire_station" and numeric_count:
        highlight = _format_compact_number(average_number)
        summary = f"Среднее расстояние: {_format_compact_number(average_number)}. Заполнено {coverage_display} строк."
        card_value = _format_compact_number(average_number)
        card_meta = f"Удалённость до пожарной части. Заполненность: {coverage_display}."
    elif feature["id"] in {"fatalities", "injuries"} and numeric_count:
        highlight = _format_int(int(round(numeric_sum)))
        summary = (
            f"Суммарно: {_format_int(int(round(numeric_sum)))}. "
            f"В {_format_int(positive_count)} строках есть ненулевые значения."
        )
        card_value = _format_int(int(round(numeric_sum)))
        card_meta = f"{label}: ненулевые значения в {_format_int(positive_count)} строках."
    elif feature["id"] == "damage" and numeric_count:
        highlight = _format_compact_number(numeric_sum)
        summary = f"Суммарный ущерб: {_format_compact_number(numeric_sum)}. Заполнено {coverage_display} строк."
        card_value = _format_compact_number(numeric_sum)
        card_meta = f"Ущерб распознан. Заполненность: {coverage_display}."

    return {
        "id": feature["id"],
        "label": label,
        "found": True,
        "columns": matched_columns,
        "coverage": coverage,
        "coverage_display": coverage_display,
        "highlight": highlight,
        "summary": summary,
        "card_value": card_value,
        "card_meta": card_meta,
        "unique_count": unique_count,
        "sum_value": numeric_sum,
        "positive_count": positive_count,
    }


def _first_found(feature_stats: Dict[str, Dict[str, Any]], feature_ids: Sequence[str]) -> Dict[str, Any] | None:
    for feature_id in feature_ids:
        stat = feature_stats.get(feature_id)
        if stat and stat["found"]:
            return stat
    return None


# intentionally separate from access_points/presentation.py::_build_summary_cards and
# forecast_risk/reliability.py::_build_summary_cards:
# table-summary cards describe schema/coverage readiness, not risk prioritization.
def _build_summary_cards(
    row_count: int,
    column_count: int,
    feature_stats: Dict[str, Dict[str, Any]],
) -> List[Dict[str, str]]:
    mandatory_total = len(MANDATORY_FEATURE_REGISTRY)
    found_total = sum(1 for item in feature_stats.values() if item["found"])
    average_fill = _average(item["coverage"] for item in feature_stats.values() if item["found"])

    cards: List[Dict[str, str]] = [
        {
            "label": "Размер",
            "value": f"{_format_int(row_count)} x {_format_int(column_count)}",
            "meta": "Строки x колонки в текущем просмотре таблицы.",
        },
        {
            "label": "Обязательные критерии",
            "value": f"{_format_int(found_total)} / {_format_int(mandatory_total)}",
            "meta": "Критерии из сценария очистки, которые удалось распознать в этой таблице.",
        },
        {
            "label": "Заполненность ключевых полей",
            "value": _format_percent(average_fill),
            "meta": "Средняя заполненность распознанных обязательных критериев.",
        },
    ]

    date_stat = feature_stats.get("fire_date")
    if date_stat:
        cards.append(
            {
                "label": "Период",
                "value": date_stat["card_value"],
                "meta": date_stat["card_meta"],
            }
        )

    territory_stat = _first_found(feature_stats, ["district", "locality", "settlement_type"])
    if territory_stat:
        cards.append(
            {
                "label": "Территория",
                "value": territory_stat["card_value"],
                "meta": territory_stat["card_meta"],
            }
        )

    cause_stat = _first_found(feature_stats, ["cause", "object_category"])
    if cause_stat:
        cards.append(
            {
                "label": "Причина / объект",
                "value": cause_stat["card_value"],
                "meta": cause_stat["card_meta"],
            }
        )

    return cards


def _build_group_cards(feature_stats: Dict[str, Dict[str, Any]]) -> List[Dict[str, str]]:
    cards: List[Dict[str, str]] = []
    for group in FEATURE_GROUPS:
        group_stats = [feature_stats[feature_id] for feature_id in group["feature_ids"] if feature_id in feature_stats]
        found_stats = [item for item in group_stats if item["found"]]
        found_count = len(found_stats)
        total_count = len(group_stats)

        if found_stats:
            highlights = [f"{item['label']}: {item['highlight']}" for item in found_stats[:2]]
            if len(found_stats) > 2:
                highlights.append(f"Ещё {_format_int(len(found_stats) - 2)} критерия распознано.")
            meta = " ".join(highlights)
        else:
            meta = "Критерии этой группы пока не распознаны в структуре таблицы."

        cards.append(
            {
                "label": group["label"],
                "value": f"{_format_int(found_count)} / {_format_int(total_count)}",
                "meta": meta,
            }
        )
    return cards


def build_table_summary(table_name: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> Dict[str, Any]:
    del table_name

    column_list = [str(column) for column in (columns or [])]
    row_list = list(rows or [])
    matched_features = _match_mandatory_features(column_list)
    feature_stats = {
        feature_id: _build_feature_stat(feature, column_list, row_list)
        for feature_id, feature in matched_features.items()
    }

    found_total = sum(1 for item in feature_stats.values() if item["found"])
    mandatory_total = len(MANDATORY_FEATURE_REGISTRY)
    average_fill = _average(item["coverage"] for item in feature_stats.values() if item["found"])

    lead = (
        f"Распознано {_format_int(found_total)} из {_format_int(mandatory_total)} обязательных критериев очистки. "
        f"Средняя заполненность ключевых полей: {_format_percent(average_fill)}."
    )
    criteria_lead = (
        "Сначала коротко видно, какие обязательные критерии реально есть в таблице, "
        "а уже потом можно уходить в полный просмотр строк."
    )

    return {
        "lead": lead,
        "criteria_lead": criteria_lead,
        "cards": _build_summary_cards(len(row_list), len(column_list), feature_stats),
        "groups": _build_group_cards(feature_stats),
    }


def build_table_page_summary(
    table_name: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    total_rows: int,
    page_row_start: int,
    page_row_end: int,
) -> Dict[str, Any]:
    summary = build_table_summary(table_name, columns, rows)
    displayed_rows = len(rows or [])

    if total_rows <= 0:
        scope_note = "В таблице пока нет строк."
    elif total_rows <= displayed_rows:
        scope_note = "Сводка рассчитана по всей таблице."
    elif displayed_rows:
        scope_note = (
            f"Эта сводка относится только к текущей странице предпросмотра: "
            f"строки {page_row_start}-{page_row_end} из { _format_int(total_rows) }."
        )
    else:
        scope_note = f"В таблице найдено { _format_int(total_rows) } строк."

    summary["scope_note"] = scope_note
    return summary
