from __future__ import annotations

from typing import Any, Dict, List, Sequence

from app.services.forecast_risk.utils import _clean_text, _format_integer, _format_number, _unique_non_empty

from .constants import ACCESS_POINTS_DESCRIPTION, ACCESS_POINTS_TITLE, MAX_NOTES


def _selection_label(options: Sequence[Dict[str, str]], selected_value: str, fallback: str) -> str:
    normalized = str(selected_value or "").strip()
    for option in options:
        if str(option.get("value") or "") == normalized:
            return str(option.get("label") or fallback)
    return fallback


def _build_filter_description(selected_table_label: str, selected_district_label: str, selected_year_label: str) -> str:
    parts = [f"таблица: {selected_table_label}"]
    if selected_district_label and selected_district_label != "Все районы":
        parts.append(f"район: {selected_district_label}")
    if selected_year_label and selected_year_label != "Все годы":
        parts.append(f"год: {selected_year_label}")
    return " | ".join(parts)


def _build_top_point_lead(top_point: Dict[str, Any] | None) -> str:
    if not top_point:
        return "Недостаточно данных, чтобы выделить проблемную точку."
    lead = (
        f"{top_point.get('label') or 'Точка'} получает {top_point.get('score_display') or '0'} балла"
        f" и попадает в верх рейтинга как {top_point.get('typology_label') or 'приоритетная точка'}."
    )
    explanation = _clean_text(top_point.get("explanation"))
    if explanation:
        return f"{lead} {explanation}"
    return lead


def _build_summary(
    rows: Sequence[Dict[str, Any]],
    *,
    selected_table_label: str,
    selected_district_label: str,
    selected_year_label: str,
    limit: int,
    total_incidents: int,
    incomplete_points: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    top_point = rows[0] if rows else None
    critical_count = sum(1 for row in rows if float(row.get("score") or 0.0) >= 70.0)
    review_count = sum(1 for row in rows if float(row.get("score") or 0.0) >= 55.0)
    return {
        "selected_table_label": selected_table_label,
        "selected_district_label": selected_district_label,
        "selected_year_label": selected_year_label,
        "limit_display": _format_integer(limit),
        "total_points_display": _format_integer(len(rows)),
        "total_incidents_display": _format_integer(total_incidents),
        "critical_points_display": _format_integer(critical_count),
        "review_points_display": _format_integer(review_count),
        "incomplete_points_display": _format_integer(len(incomplete_points)),
        "top_point_label": str((top_point or {}).get("label") or "-"),
        "top_point_score_display": str((top_point or {}).get("score_display") or "0"),
        "top_point_priority_label": str((top_point or {}).get("priority_label") or "Нет оценки"),
        "filter_description": _build_filter_description(
            selected_table_label=selected_table_label,
            selected_district_label=selected_district_label,
            selected_year_label=selected_year_label,
        ),
    }


def _build_summary_cards(
    rows: Sequence[Dict[str, Any]],
    *,
    total_incidents: int,
    incomplete_points: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    top_point = rows[0] if rows else None
    critical_count = sum(1 for row in rows if float(row.get("score") or 0.0) >= 70.0)
    review_count = sum(1 for row in rows if float(row.get("score") or 0.0) >= 55.0)
    cards = [
        {
            "label": "Уникальные точки",
            "value": _format_integer(len(rows)),
            "meta": f"Инцидентов после фильтров: {_format_integer(total_incidents)}",
            "tone": "normal",
        },
        {
            "label": "Высокий приоритет",
            "value": _format_integer(critical_count),
            "meta": f"Повышенный и выше: {_format_integer(review_count)}",
            "tone": "critical" if critical_count else ("warning" if review_count else "normal"),
        },
        {
            "label": "Точка №1",
            "value": str((top_point or {}).get("score_display") or "0"),
            "meta": str((top_point or {}).get("label") or "Рейтинг появится после расчёта"),
            "tone": str((top_point or {}).get("tone") or "normal"),
        },
        {
            "label": "Нужна верификация",
            "value": _format_integer(len(incomplete_points)),
            "meta": "Точки, где риск проверки растёт из-за пропусков",
            "tone": "watch" if incomplete_points else "normal",
        },
    ]
    return cards


def _build_notes(
    metadata_notes: Sequence[str],
    input_notes: Sequence[str],
    rows: Sequence[Dict[str, Any]],
    incomplete_points: Sequence[Dict[str, Any]],
) -> List[str]:
    notes: List[str] = []
    if rows:
        broad_points = sum(1 for row in rows if str(row.get("entity_code") or "") in {"territory", "district", "unknown"})
        if broad_points:
            notes.append(
                f"Для {_format_integer(broad_points)} точек рейтинг построен на fallback-сущности уровня населённого пункта, территории или района, потому что более точный адрес/объект не найден."
            )
        if len(rows) < 5:
            notes.append("После выбранных фильтров осталось мало уникальных точек, поэтому ranking стоит трактовать как ориентир для просмотра, а не как стабильную типологию.")
        if incomplete_points:
            notes.append("Блок «Данные неполные» показывает точки, где нужны уточнения по воде, времени прибытия или дистанции до ПЧ, прежде чем принимать жёсткие управленческие меры.")
        max_score = max(float(row.get("score") or 0.0) for row in rows)
        if max_score < 40.0:
            notes.append("Даже верхняя часть рейтинга сейчас скорее про наблюдение, чем про критическое перераспределение сил: явных выбросов по score не видно.")
    else:
        notes.append("По выбранному срезу не нашлось инцидентов для построения рейтинга проблемных точек.")

    for item in list(metadata_notes)[:3]:
        text = _clean_text(item)
        if text:
            notes.append(f"Метаданные: {text}")
    for item in list(input_notes)[:3]:
        text = _clean_text(item)
        if text:
            notes.append(f"Загрузка данных: {text}")
    return _unique_non_empty(notes)[:MAX_NOTES]


def _empty_access_points_data(
    *,
    filters: Dict[str, Any],
    summary: Dict[str, Any],
    notes: Sequence[str] | None = None,
    bootstrap_mode: str = "resolved",
) -> Dict[str, Any]:
    resolved_notes = _unique_non_empty(
        list(notes or []) or ["Недостаточно данных для построения рейтинга проблемных точек."]
    )[:MAX_NOTES]
    return {
        "bootstrap_mode": bootstrap_mode,
        "loading": bootstrap_mode == "deferred",
        "has_data": False,
        "title": ACCESS_POINTS_TITLE,
        "model_description": ACCESS_POINTS_DESCRIPTION,
        "filters": filters,
        "summary": summary,
        "summary_cards": _build_summary_cards([], total_incidents=0, incomplete_points=[]),
        "top_point_label": "-",
        "top_point_explanation": "Недостаточно данных для выделения приоритетных точек.",
        "points": [],
        "top_points": [],
        "incomplete_points": [],
        "typology": [],
        "notes": resolved_notes,
    }
