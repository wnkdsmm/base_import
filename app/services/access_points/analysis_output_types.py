from __future__ import annotations

from typing import Any, Dict, List, NamedTuple, Sequence

from app.services.shared.formatting import _format_number, _format_percent

from .analysis_factors import (
    CRITICAL_THRESHOLD,
    DISTANCE_CODE,
    HEATING_CODE,
    HIGH_THRESHOLD,
    LONG_ARRIVAL_CODE,
    MEDIUM_THRESHOLD,
    NIGHT_CODE,
    RECURRENCE_CODE,
    RESPONSE_CODE,
    SEVERITY_CODE,
    UNCERTAINTY_CODE,
    WATER_CODE,
)

def _format_coordinate(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.4f}".replace(".", ",")

def _status_for_score(score: float) -> tuple[str, str]:
    if score >= CRITICAL_THRESHOLD:
        return "critical", "РљСЂРёС‚РёС‡РµСЃРєРёР№ РїСЂРёРѕСЂРёС‚РµС‚"
    if score >= HIGH_THRESHOLD:
        return "warning", "РџРѕРІС‹С€РµРЅРЅС‹Р№ РїСЂРёРѕСЂРёС‚РµС‚"
    if score >= MEDIUM_THRESHOLD:
        return "watch", "РќР°Р±Р»СЋРґРµРЅРёРµ"
    return "normal", "РљРѕРЅС‚СЂРѕР»СЊ"

def _component_tone(score: float) -> str:
    if score >= 65.0:
        return "critical"
    if score >= 45.0:
        return "warning"
    if score >= 25.0:
        return "watch"
    return "normal"

def _severity_band_descriptor(score: float) -> Dict[str, str]:
    tone, label = _status_for_score(score)
    if tone == "critical":
        return {"severity_band_code": "critical", "severity_band": "РєСЂРёС‚РёС‡РµСЃРєРёР№", "priority_label": label, "tone": tone}
    if tone == "warning":
        return {"severity_band_code": "high", "severity_band": "РІС‹СЃРѕРєРёР№", "priority_label": label, "tone": tone}
    if tone == "watch":
        return {"severity_band_code": "medium", "severity_band": "СЃСЂРµРґРЅРёР№", "priority_label": label, "tone": tone}
    return {"severity_band_code": "low", "severity_band": "РЅРёР·РєРёР№", "priority_label": label, "tone": tone}

def _factor_label(reason_code: str) -> str:
    return {
        DISTANCE_CODE: "РЈРґР°Р»С‘РЅРЅРѕСЃС‚СЊ РґРѕ РџР§",
        RESPONSE_CODE: "РЎСЂРµРґРЅРµРµ РІСЂРµРјСЏ РїСЂРёР±С‹С‚РёСЏ",
        LONG_ARRIVAL_CODE: "Р”РѕР»СЏ РґРѕР»РіРёС… РїСЂРёР±С‹С‚РёР№",
        WATER_CODE: "РћС‚СЃСѓС‚СЃС‚РІРёРµ РІРѕРґС‹",
        SEVERITY_CODE: "РўСЏР¶С‘Р»С‹Рµ РїРѕСЃР»РµРґСЃС‚РІРёСЏ",
        RECURRENCE_CODE: "РџРѕРІС‚РѕСЂСЏРµРјРѕСЃС‚СЊ РїРѕР¶Р°СЂРѕРІ",
        NIGHT_CODE: "РќРѕС‡РЅРѕР№ РїСЂРѕС„РёР»СЊ",
        HEATING_CODE: "РћС‚РѕРїРёС‚РµР»СЊРЅС‹Р№ СЃРµР·РѕРЅ",
        UNCERTAINTY_CODE: "РќРµРїРѕР»РЅРѕС‚Р° РґР°РЅРЅС‹С…",
    }.get(reason_code, reason_code)

def _make_decomposition_item(
    *,
    code: str,
    factor_score: float,
    weight_points: float,
    contribution_points: float,
    value_display: str,
    is_penalty: bool = False,
) -> dict[str, Any]:
    return {
        "code": code,
        "label": _factor_label(code),
        "factor_score": round(factor_score, 1),
        "factor_score_display": _format_number(factor_score),
        "weight_points": round(weight_points, 2),
        "weight_points_display": _format_number(weight_points),
        "contribution_points": round(contribution_points, 2),
        "contribution_display": f"+{_format_number(contribution_points)}",
        "value_display": value_display,
        "is_penalty": is_penalty,
        "tone": _component_tone(contribution_points * 5.0),
    }

def _distance_value_display(value: Any) -> str:
    return "РЅ/Рґ" if value is None else f"{_format_number(value)} РєРј"

def _response_value_display(value: Any) -> str:
    return "РЅ/Рґ" if value is None else f"{_format_number(value)} РјРёРЅ"

def _share_value_display(value: float) -> str:
    return _format_percent(value * 100.0)

def _build_component_score_item(key: str, label: str, score: float) -> dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "score": round(score, 1),
        "score_display": _format_number(score),
        "tone": _component_tone(score),
    }

def _build_component_scores_from_values(
    *,
    access_score: float,
    water_score: float,
    severity_score: float,
    recurrence_score: float,
    data_gap_score: float,
) -> List[dict[str, Any]]:
    return [
        _build_component_score_item("access", "Р”РѕСЃС‚СѓРїРЅРѕСЃС‚СЊ РџР§", access_score),
        _build_component_score_item("water", "Р’РѕРґРѕСЃРЅР°Р±Р¶РµРЅРёРµ", water_score),
        _build_component_score_item("severity", "РџРѕСЃР»РµРґСЃС‚РІРёСЏ", severity_score),
        _build_component_score_item("recurrence", "Р§Р°СЃС‚РѕС‚Р° Рё РєРѕРЅС‚РµРєСЃС‚", recurrence_score),
        _build_component_score_item("data_gap", "РќРµРїРѕР»РЅРѕС‚Р° РґР°РЅРЅС‹С…", data_gap_score),
    ]

def _build_reason_details(score_decomposition: Sequence[dict[str, Any]]) -> List[dict[str, Any]]:
    ranked = sorted(
        ((item, float(item.get("contribution_points") or 0.0)) for item in score_decomposition),
        key=lambda pair: pair[1],
        reverse=True,
    )
    details = [
        {
            "code": str(item["code"]),
            "label": str(item["label"]),
            "contribution_points": round(contribution, 2),
            "contribution_display": str(item.get("contribution_display") or "0"),
            "value_display": str(item.get("value_display") or ""),
        }
        for item, contribution in ranked
        if contribution > 0
    ]
    return details[:4]

def _build_human_readable_explanation(
    *,
    label: str,
    severity_band: str,
    total_score_display: str,
    reason_details: Sequence[dict[str, Any]],
    uncertainty_flag: bool,
    low_support: bool,
    uncertainty_penalty_display: str,
) -> str:
    details = list(reason_details)
    if not details:
        return "РўРѕС‡РєР° РІРєР»СЋС‡РµРЅР° РІ СЂРµР№С‚РёРЅРі РїРѕ СЃСѓРјРјРµ С„Р°РєС‚РѕСЂРѕРІ СЂРёСЃРєР°."

    lead = f"{label or 'РўРѕС‡РєР°'} РїРѕР»СѓС‡Р°РµС‚ {severity_band or 'СЃСЂРµРґРЅРёР№'} СЂРёСЃРє {total_score_display or '0'} РёР· 100."
    drivers = [item for item in details if item["code"] != UNCERTAINTY_CODE][:2]
    if drivers:
        lead += " РћСЃРЅРѕРІРЅРѕР№ РІРєР»Р°Рґ РґР°Р»Рё " + ", ".join(
            f"{item['label'].lower()} ({item['contribution_display']})" for item in drivers
        ) + "."
    if uncertainty_flag:
        lead += f" РќРµРѕРїСЂРµРґРµР»С‘РЅРЅРѕСЃС‚СЊ РґРѕР±Р°РІР»СЏРµС‚ {uncertainty_penalty_display or '0'} Рї. Рё С‚СЂРµР±СѓРµС‚ РІРµСЂРёС„РёРєР°С†РёРё."
    elif low_support:
        lead += " РўРѕС‡РєР° РЅРёР·РєРѕР№ РѕРїРѕСЂС‹: РґРѕР»РµРІС‹Рµ РїСЂРёР·РЅР°РєРё СЃРіР»Р°Р¶РµРЅС‹, Р° РёС‚РѕРіРѕРІС‹Р№ score РѕСЃР»Р°Р±Р»РµРЅ."
    return lead

def _typology_for_score_values(
    *,
    uncertainty_flag: bool,
    severity_band_code: str,
    access_score: float,
    water_score: float,
    severity_score: float,
    recurrence_score: float,
) -> tuple[str, str]:
    if uncertainty_flag and severity_band_code in {"low", "medium"}:
        return "needs_data", "Р”Р°РЅРЅС‹Рµ РЅРµРїРѕР»РЅС‹Рµ"
    components = {
        "access": access_score,
        "water": water_score,
        "severity": severity_score,
        "recurrence": recurrence_score,
    }
    dominant = max(components, key=components.get)
    if dominant == "access" and components["access"] >= 35.0:
        return "access", "Р”Р°Р»СЊРЅРёР№ РІС‹РµР·Рґ"
    if dominant == "water" and components["water"] >= 30.0:
        return "water", "Р”РµС„РёС†РёС‚ РІРѕРґС‹"
    if dominant == "severity" and components["severity"] >= 30.0:
        return "severity", "РўСЏР¶С‘Р»С‹Рµ РїРѕСЃР»РµРґСЃС‚РІРёСЏ"
    if dominant == "recurrence" and components["recurrence"] >= 28.0:
        return "recurrence", "РџРѕРІС‚РѕСЂСЏСЋС‰РёР№СЃСЏ РѕС‡Р°Рі"
    return "mixed", "РљРѕРјР±РёРЅРёСЂРѕРІР°РЅРЅС‹Р№ СЂРёСЃРє"

class _AccessPointRowMetrics(NamedTuple):
    incident_count: int
    years_observed: int
    incidents_per_year: float
    average_distance: float | None
    average_response: float | None
    long_arrival_share: float
    no_water_share: float
    water_coverage_share: float
    water_unknown_share: float
    severe_share: float
    night_share: float
    heating_share: float
    rural_share: float
    response_coverage_share: float
    distance_coverage_share: float
    arrival_missing_share: float
    distance_missing_share: float
    completeness_share: float
    distance_norm: float
    response_norm: float
    support_weight: float
    severity_factor: float
    recurrence_factor: float
    uncertainty_factor: float
    access_score: float
    water_score: float
    severity_score: float
    recurrence_score: float
    data_gap_score: float
    latitude: float | None
    longitude: float | None

class _AccessPointScoreContext(NamedTuple):
    score_decomposition: List[dict[str, Any]]
    total_score: float
    total_score_payload: float
    total_score_display: str
    uncertainty_penalty: float
    uncertainty_penalty_payload: float
    uncertainty_penalty_display: str
    investigation_score: float
    investigation_score_payload: float
    investigation_score_display: str
    access_score_payload: float
    water_score_payload: float
    severity_score_payload: float
    recurrence_score_payload: float
    data_gap_score_payload: float
    component_scores: List[dict[str, Any]]

class _AccessPointDisplayContext(NamedTuple):
    average_distance_display: str
    average_response_display: str
    response_coverage_display: str
    long_arrival_share_display: str
    no_water_share_display: str
    water_unknown_share_display: str
    water_coverage_display: str
    severe_share_display: str
    night_share_display: str
    heating_share_display: str
    rural_share_display: str
    completeness_display: str

class _AccessPointReasonListContext(NamedTuple):
    top_reason_codes: List[str]
    reasons: List[str]
    reason_chips: List[str]

class _AccessPointReasonContext(NamedTuple):
    reason_details: List[dict[str, Any]]
    reason_lists: _AccessPointReasonListContext
    explanation: str
