from __future__ import annotations

from typing import Dict, List, Sequence

from app.table_catalog import get_user_table_options, resolve_selected_table_value


def _normalize_filter_value(value: str) -> str:
    normalized = str(value or "").strip()
    return normalized or "all"


def _history_window_year_span(history_window: str) -> int:
    if history_window == "recent_3":
        return 3
    if history_window == "recent_5":
        return 5
    return 0


def _build_forecasting_table_options() -> List[Dict[str, str]]:
    options = []
    seen = set()
    for option in get_user_table_options():
        value = str(option.get("value") or "").strip()
        if not value or value == "all" or value in seen:
            continue
        seen.add(value)
        options.append({"value": value, "label": str(option.get("label") or value)})
    return [{"value": "all", "label": "Р’СЃРµ С‚Р°Р±Р»РёС†С‹"}] + options


def _normalize_source_table_name(table_name: str) -> str:
    return str(table_name or "").strip()


def _is_clean_source_table(table_name: str) -> bool:
    normalized = _normalize_source_table_name(table_name)
    return normalized.casefold().startswith("clean_") and len(normalized) > len("clean_")


def _source_table_canonical_key(table_name: str) -> str:
    normalized = _normalize_source_table_name(table_name)
    if _is_clean_source_table(normalized):
        normalized = normalized[len("clean_") :]
    return normalized.casefold()


def _source_table_deduplication_note(raw_table: str, clean_table: str) -> str:
    return (
        f"РўР°Р±Р»РёС†Р° '{raw_table}' РёСЃРєР»СЋС‡РµРЅР° РєР°Рє РґСѓР±Р»РёРєР°С‚ clean-РІРµСЂСЃРёРё "
        f"'{clean_table}', С‡С‚РѕР±С‹ РёСЃС‚РѕСЂРёСЏ РЅРµ СѓС‡РёС‚С‹РІР°Р»Р°СЃСЊ РґРІР°Р¶РґС‹."
    )


def _unique_notes(notes: Sequence[str]) -> List[str]:
    seen = set()
    unique: List[str] = []
    for note in notes:
        normalized = str(note or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return unique


def _canonicalize_source_tables(source_tables: Sequence[str]) -> tuple[List[str], List[str]]:
    selected_by_key: Dict[str, str] = {}
    deduplication_notes: List[str] = []
    for source_table in source_tables:
        normalized = _normalize_source_table_name(source_table)
        if not normalized:
            continue
        canonical_key = _source_table_canonical_key(normalized)
        current = selected_by_key.get(canonical_key)
        if current is None:
            selected_by_key[canonical_key] = normalized
            continue
        if current == normalized:
            continue

        current_is_clean = _is_clean_source_table(current)
        normalized_is_clean = _is_clean_source_table(normalized)
        if normalized_is_clean and not current_is_clean:
            selected_by_key[canonical_key] = normalized
            deduplication_notes.append(_source_table_deduplication_note(current, normalized))
            continue
        if current_is_clean and not normalized_is_clean:
            deduplication_notes.append(_source_table_deduplication_note(normalized, current))

    return list(selected_by_key.values()), _unique_notes(deduplication_notes)


def _resolve_forecasting_selection(table_options: List[Dict[str, str]], table_name: str) -> str:
    return resolve_selected_table_value(table_options, table_name, fallback_value="all")


def _selected_source_table_notes(table_options: List[Dict[str, str]], selected_table: str) -> List[str]:
    concrete = [option["value"] for option in table_options if option.get("value") and option["value"] != "all"]
    if selected_table != "all":
        return []
    return _canonicalize_source_tables(concrete)[1]


def _selected_source_tables(table_options: List[Dict[str, str]], selected_table: str) -> List[str]:
    concrete = [option["value"] for option in table_options if option.get("value") and option["value"] != "all"]
    if selected_table == "all":
        return _canonicalize_source_tables(concrete)[0]
    return [selected_table] if selected_table in concrete else []


def _table_selection_label(selected_table: str) -> str:
    if selected_table == "all":
        return "Р’СЃРµ С‚Р°Р±Р»РёС†С‹"
    return selected_table or "РќРµС‚ С‚Р°Р±Р»РёС†С‹"
