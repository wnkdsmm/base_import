from __future__ import annotations

from typing import Any, Dict, List, Sequence

from app.services.shared.data_utils import _clean_text, _unique_non_empty
from app.services.shared.formatting import _format_integer

from .constants import ACCESS_POINTS_DESCRIPTION, ACCESS_POINTS_TITLE, MAX_NOTES
from .types import AccessPointCard, AccessPointFilters, AccessPointPresentation, OptionItem, PointData, PresentationSummary


def _selection_label(options: Sequence[OptionItem], selected_value: str, fallback: str) -> str:
    normalized = str(selected_value or "").strip()
    for option in options:
        if str(option.get("value") or "") == normalized:
            return str(option.get("label") or fallback)
    return fallback


def _build_filter_description(selected_table_label: str, selected_district_label: str, selected_year_label: str) -> str:
    parts = [f"С‚Р°Р±Р»РёС†Р°: {selected_table_label}"]
    if selected_district_label and selected_district_label != "Р’СЃРµ СЂР°Р№РѕРЅС‹":
        parts.append(f"СЂР°Р№РѕРЅ: {selected_district_label}")
    if selected_year_label and selected_year_label != "Р’СЃРµ РіРѕРґС‹":
        parts.append(f"РіРѕРґ: {selected_year_label}")
    return " | ".join(parts)


def _build_top_point_lead(top_point: PointData | None) -> str:
    if not top_point:
        return "РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С…, С‡С‚РѕР±С‹ РІС‹РґРµР»РёС‚СЊ РїСЂРѕР±Р»РµРјРЅСѓСЋ С‚РѕС‡РєСѓ."

    explanation = _clean_text(top_point.get("human_readable_explanation") or top_point.get("explanation"))
    if explanation:
        return explanation

    label = _clean_text(top_point.get("label")) or "РўРѕС‡РєР°"
    severity_band = _clean_text(top_point.get("severity_band")) or "СЃСЂРµРґРЅРёР№"
    score_display = str(top_point.get("total_score_display") or top_point.get("score_display") or "0")
    typology_label = _clean_text(top_point.get("typology_label")) or "РїСЂРёРѕСЂРёС‚РµС‚РЅР°СЏ С‚РѕС‡РєР°"
    return f"{label} РїРѕР»СѓС‡Р°РµС‚ {severity_band} СЂРёСЃРє СЃРѕ score {score_display} РёР· 100 Рё РїРѕРїР°РґР°РµС‚ РІ РІРµСЂС… СЂРµР№С‚РёРЅРіР° РєР°Рє {typology_label}."


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
        "top_point_severity_band": str((top_point or {}).get("severity_band") or "РЅРµС‚ РѕС†РµРЅРєРё"),
        "top_point_priority_label": str((top_point or {}).get("priority_label") or "РќРµС‚ РѕС†РµРЅРєРё"),
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
) -> List[AccessPointCard]:
    top_point = rows[0] if rows else None
    high_or_above_count = sum(1 for row in rows if str(row.get("severity_band_code") or "") in {"high", "critical"})
    critical_count = sum(1 for row in rows if str(row.get("severity_band_code") or "") == "critical")
    uncertainty_count = sum(1 for row in rows if row.get("uncertainty_flag"))
    return [
        {
            "label": "РЈРЅРёРєР°Р»СЊРЅС‹Рµ С‚РѕС‡РєРё",
            "value": _format_integer(len(rows)),
            "meta": f"РРЅС†РёРґРµРЅС‚РѕРІ РїРѕСЃР»Рµ С„РёР»СЊС‚СЂРѕРІ: {_format_integer(total_incidents)}",
            "tone": "normal",
        },
        {
            "label": "Р’С‹СЃРѕРєРёР№ СЂРёСЃРє",
            "value": _format_integer(high_or_above_count),
            "meta": f"РљСЂРёС‚РёС‡РµСЃРєРёС…: {_format_integer(critical_count)}",
            "tone": "critical" if critical_count else ("warning" if high_or_above_count else "normal"),
        },
        {
            "label": "РўРѕС‡РєР° в„–1",
            "value": str((top_point or {}).get("total_score_display") or (top_point or {}).get("score_display") or "0"),
            "meta": str((top_point or {}).get("label") or "Р РµР№С‚РёРЅРі РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р°"),
            "tone": str((top_point or {}).get("tone") or "normal"),
        },
        {
            "label": "РќСѓР¶РЅР° РІРµСЂРёС„РёРєР°С†РёСЏ",
            "value": _format_integer(max(len(incomplete_points), uncertainty_count)),
            "meta": "РўРѕС‡РєРё, РіРґРµ risk score С‚СЂРµР±СѓРµС‚ РїСЂРѕРІРµСЂРєРё РїРѕР»РЅРѕС‚С‹ РґР°РЅРЅС‹С…",
            "tone": "watch" if incomplete_points or uncertainty_count else "normal",
        },
    ]


def _build_notes(
    metadata_notes: Sequence[str],
    input_notes: Sequence[str],
    rows: Sequence[PointData],
    incomplete_points: Sequence[PointData],
) -> List[str]:
    notes: List[str] = []
    if rows:
        notes.append(
            "Score 0-100 СЃС‚СЂРѕРёС‚СЃСЏ РєР°Рє explainable model: РІРєР»Р°Рґ РєР°Р¶РґРѕРіРѕ С„Р°РєС‚РѕСЂР° РІРёРґРµРЅ РѕС‚РґРµР»СЊРЅРѕ, Р° РЅРµРїРѕР»РЅРѕС‚Р° РґР°РЅРЅС‹С… РґР°С‘С‚ РѕРіСЂР°РЅРёС‡РµРЅРЅС‹Р№ penalty Рё РЅРµ РґРѕР»Р¶РЅР° РґРѕРјРёРЅРёСЂРѕРІР°С‚СЊ РЅР°Рґ СЂРµР°Р»СЊРЅС‹Рј СЂРёСЃРєРѕРј."
        )
        broad_points = sum(1 for row in rows if str(row.get("entity_code") or "") in {"territory", "district", "unknown"})
        if broad_points:
            notes.append(
                f"Р”Р»СЏ {_format_integer(broad_points)} С‚РѕС‡РµРє СЂРµР№С‚РёРЅРі РїРѕСЃС‚СЂРѕРµРЅ РЅР° fallback-СЃСѓС‰РЅРѕСЃС‚Рё СѓСЂРѕРІРЅСЏ РЅР°СЃРµР»С‘РЅРЅРѕРіРѕ РїСѓРЅРєС‚Р°, С‚РµСЂСЂРёС‚РѕСЂРёРё РёР»Рё СЂР°Р№РѕРЅР°, РїРѕС‚РѕРјСѓ С‡С‚Рѕ Р±РѕР»РµРµ С‚РѕС‡РЅС‹Р№ Р°РґСЂРµСЃ/РѕР±СЉРµРєС‚ РЅРµ РЅР°Р№РґРµРЅ."
            )
        if len(rows) < 5:
            notes.append(
                "РџРѕСЃР»Рµ РІС‹Р±СЂР°РЅРЅС‹С… С„РёР»СЊС‚СЂРѕРІ РѕСЃС‚Р°Р»РѕСЃСЊ РјР°Р»Рѕ СѓРЅРёРєР°Р»СЊРЅС‹С… С‚РѕС‡РµРє, РїРѕСЌС‚РѕРјСѓ ranking СЃС‚РѕРёС‚ С‚СЂР°РєС‚РѕРІР°С‚СЊ РєР°Рє РѕСЂРёРµРЅС‚РёСЂ РґР»СЏ РїСЂРѕСЃРјРѕС‚СЂР°, Р° РЅРµ РєР°Рє СѓСЃС‚РѕР№С‡РёРІСѓСЋ С‚РёРїРѕР»РѕРіРёСЋ."
            )
        if incomplete_points:
            notes.append(
                "Р‘Р»РѕРє В«Р”Р°РЅРЅС‹Рµ РЅРµРїРѕР»РЅС‹РµВ» РїРѕРєР°Р·С‹РІР°РµС‚ С‚РѕС‡РєРё, РіРґРµ РЅСѓР¶РЅС‹ СѓС‚РѕС‡РЅРµРЅРёСЏ РїРѕ РІРѕРґРµ, РІСЂРµРјРµРЅРё РїСЂРёР±С‹С‚РёСЏ РёР»Рё РґРёСЃС‚Р°РЅС†РёРё РґРѕ РџР§, РїСЂРµР¶РґРµ С‡РµРј РїСЂРёРЅРёРјР°С‚СЊ Р¶С‘СЃС‚РєРёРµ СѓРїСЂР°РІР»РµРЅС‡РµСЃРєРёРµ РјРµСЂС‹."
            )
        max_score = max(float(row.get("total_score") or row.get("score") or 0.0) for row in rows)
        if max_score < 40.0:
            notes.append(
                "Р”Р°Р¶Рµ РІРµСЂС…РЅСЏСЏ С‡Р°СЃС‚СЊ СЂРµР№С‚РёРЅРіР° СЃРµР№С‡Р°СЃ СЃРєРѕСЂРµРµ РїСЂРѕ РЅР°Р±Р»СЋРґРµРЅРёРµ, С‡РµРј РїСЂРѕ РєСЂРёС‚РёС‡РµСЃРєРѕРµ РїРµСЂРµСЂР°СЃРїСЂРµРґРµР»РµРЅРёРµ СЃРёР»: СЏРІРЅС‹С… РІС‹Р±СЂРѕСЃРѕРІ РїРѕ score РЅРµ РІРёРґРЅРѕ."
            )
    else:
        notes.append("РџРѕ РІС‹Р±СЂР°РЅРЅРѕРјСѓ СЃСЂРµР·Сѓ РЅРµ РЅР°С€Р»РѕСЃСЊ РёРЅС†РёРґРµРЅС‚РѕРІ РґР»СЏ РїРѕСЃС‚СЂРѕРµРЅРёСЏ СЂРµР№С‚РёРЅРіР° РїСЂРѕР±Р»РµРјРЅС‹С… С‚РѕС‡РµРє.")

    for item in list(metadata_notes)[:3]:
        text = _clean_text(item)
        if text:
            notes.append(f"РњРµС‚Р°РґР°РЅРЅС‹Рµ: {text}")
    for item in list(input_notes)[:3]:
        text = _clean_text(item)
        if text:
            notes.append(f"Р—Р°РіСЂСѓР·РєР° РґР°РЅРЅС‹С…: {text}")
    return _unique_non_empty(notes)[:MAX_NOTES]


def _empty_access_points_data(
    *,
    filters: AccessPointFilters,
    summary: PresentationSummary,
    notes: Sequence[str] | None = None,
    bootstrap_mode: str = "resolved",
) -> AccessPointPresentation:
    resolved_notes = _unique_non_empty(
        list(notes or []) or ["РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РїРѕСЃС‚СЂРѕРµРЅРёСЏ СЂРµР№С‚РёРЅРіР° РїСЂРѕР±Р»РµРјРЅС‹С… С‚РѕС‡РµРє."]
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
        "top_point_explanation": "РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РІС‹РґРµР»РµРЅРёСЏ РїСЂРёРѕСЂРёС‚РµС‚РЅС‹С… С‚РѕС‡РµРє.",
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
