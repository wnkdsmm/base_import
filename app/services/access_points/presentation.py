from __future__ import annotations

from typing import Any, Sequence

from app.services.shared.data_utils import _clean_text, _unique_non_empty
from app.services.shared.formatting import _format_integer
from app.services.shared.summary_cards import build_summary_cards

from .constants import ACCESS_POINTS_DESCRIPTION, ACCESS_POINTS_TITLE, MAX_NOTES
from .types import AccessPointCard, AccessPointFilters, AccessPointPresentation, OptionItem, PointData, PresentationSummary


def _selection_label(options: Sequence[OptionItem], selected_value: str, fallback: str) -> str:
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


def _build_top_point_lead(top_point: PointData | None) -> str:
    if not top_point:
        return "Недостаточно данных, чтобы выделить проблемную точку."

    explanation = _clean_text(top_point.get("human_readable_explanation") or top_point.get("explanation"))
    if explanation:
        return explanation

    label = _clean_text(top_point.get("label")) or "Точка"
    severity_band = _clean_text(top_point.get("severity_band")) or "средний"
    score_display = str(top_point.get("total_score_display") or top_point.get("score_display") or "0")
    typology_label = _clean_text(top_point.get("typology_label")) or "приоритетная точка"
    return f"{label} получает {severity_band} риск со score {score_display} из 100 и попадает в верх рейтинга как {typology_label}."


# intentionally separate from forecasting/presentation.py::_build_summary and
# ml_model/training/presentation_training.py::_build_summary:
# access-points summary has its own point-risk and verification semantics.


def _build_summary(
    rows: Sequence[PointData],
    *,
    selected_table_label: str,
    selected_district_label: str,
    selected_year_label: str,
    limit: int,
    total_incidents: int,
    incomplete_points: Sequence[PointData],
) -> PresentationSummary:
    top_point = rows[0] if rows else None
    critical_count = sum(1 for row in rows if str(row.get("severity_band_code") or "") == "critical")
    high_count = sum(1 for row in rows if str(row.get("severity_band_code") or "") in {"high", "critical"})
    medium_count = sum(1 for row in rows if str(row.get("severity_band_code") or "") == "medium")
    uncertainty_count = sum(1 for row in rows if row.get("uncertainty_flag"))
    return {
        "selected_table_label": selected_table_label,
        "selected_district_label": selected_district_label,
        "selected_year_label": selected_year_label,
        "limit_display": _format_integer(limit),
        "total_points_display": _format_integer(len(rows)),
        "total_incidents_display": _format_integer(total_incidents),
        "critical_points_display": _format_integer(critical_count),
        "high_points_display": _format_integer(high_count),
        "medium_points_display": _format_integer(medium_count),
        "review_points_display": _format_integer(high_count),
        "incomplete_points_display": _format_integer(len(incomplete_points)),
        "uncertainty_points_display": _format_integer(uncertainty_count),
        "top_point_label": str((top_point or {}).get("label") or "-"),
        "top_point_score_display": str((top_point or {}).get("total_score_display") or (top_point or {}).get("score_display") or "0"),
        "top_point_severity_band": str((top_point or {}).get("severity_band") or "нет оценки"),
        "top_point_priority_label": str((top_point or {}).get("priority_label") or "Нет оценки"),
        "filter_description": _build_filter_description(
            selected_table_label=selected_table_label,
            selected_district_label=selected_district_label,
            selected_year_label=selected_year_label,
        ),
    }


# intentionally separate from forecast_risk/reliability.py::_build_summary_cards and
# table_summary.py::_build_summary_cards:
# access-points cards are incident-point prioritization widgets.


def _build_summary_cards(
    rows: Sequence[PointData],
    *,
    total_incidents: int,
    incomplete_points: Sequence[PointData],
) -> list[AccessPointCard]:
    top_point = rows[0] if rows else None
    high_or_above_count = sum(1 for row in rows if str(row.get("severity_band_code") or "") in {"high", "critical"})
    critical_count = sum(1 for row in rows if str(row.get("severity_band_code") or "") == "critical")
    uncertainty_count = sum(1 for row in rows if row.get("uncertainty_flag"))
    return build_summary_cards(
        [
            {
                "label": "Уникальные точки",
                "value": _format_integer(len(rows)),
                "meta": f"Инцидентов после фильтров: {_format_integer(total_incidents)}",
                "tone": "normal",
            },
            {
                "label": "Высокий риск",
                "value": _format_integer(high_or_above_count),
                "meta": f"Критических: {_format_integer(critical_count)}",
                "tone": "critical" if critical_count else ("warning" if high_or_above_count else "normal"),
            },
            {
                "label": "Точка №1",
                "value": str((top_point or {}).get("total_score_display") or (top_point or {}).get("score_display") or "0"),
                "meta": str((top_point or {}).get("label") or "Рейтинг появится после расчёта"),
                "tone": str((top_point or {}).get("tone") or "normal"),
            },
            {
                "label": "Нужна верификация",
                "value": _format_integer(max(len(incomplete_points), uncertainty_count)),
                "meta": "Точки, где risk score требует проверки полноты данных",
                "tone": "watch" if incomplete_points or uncertainty_count else "normal",
            },
        ]
    )


def _build_notes(
    metadata_notes: Sequence[str],
    input_notes: Sequence[str],
    rows: Sequence[PointData],
    incomplete_points: Sequence[PointData],
) -> list[str]:
    notes: list[str] = []
    if rows:
        notes.append(
            "Score 0-100 строится как explainable model: вклад каждого фактора виден отдельно, а неполнота данных даёт ограниченный penalty и не должна доминировать над реальным риском."
        )
        broad_points = sum(1 for row in rows if str(row.get("entity_code") or "") in {"territory", "district", "unknown"})
        if broad_points:
            notes.append(
                f"Для {_format_integer(broad_points)} точек рейтинг построен на fallback-сущности уровня населённого пункта, территории или района, потому что более точный адрес/объект не найден."
            )
        if len(rows) < 5:
            notes.append(
                "После выбранных фильтров осталось мало уникальных точек, поэтому ranking стоит трактовать как ориентир для просмотра, а не как устойчивую типологию."
            )
        if incomplete_points:
            notes.append(
                "Блок «Данные неполные» показывает точки, где нужны уточнения по воде, времени прибытия или дистанции до ПЧ, прежде чем принимать жёсткие управленческие меры."
            )
        max_score = max(float(row.get("total_score") or row.get("score") or 0.0) for row in rows)
        if max_score < 40.0:
            notes.append(
                "Даже верхняя часть рейтинга сейчас скорее про наблюдение, чем про критическое перераспределение сил: явных выбросов по score не видно."
            )
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
    filters: AccessPointFilters,
    summary: PresentationSummary,
    notes: Sequence[str] | None = None,
    bootstrap_mode: str = "resolved",
) -> AccessPointPresentation:
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
        "score_distribution": {
            "average_score_display": "0",
            "median_score_display": "0",
            "bands": [],
            "buckets": [],
        },
        "reason_breakdown": [],
        "incomplete_points": [],
        "typology": [],
        "uncertainty_notes": [],
        "notes": resolved_notes,
    }
