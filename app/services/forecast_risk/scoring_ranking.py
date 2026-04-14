from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from .utils import _format_number, _format_percent


def _priority_label(
    risk_score: float,
    component_score_map: Dict[str, Dict[str, float]],
    is_rural: bool,
    thresholds: dict[str, Any],
) -> tuple[str, str]:
    arrival_score = component_score_map.get("long_arrival_risk", {}).get("score", 0.0)
    water_score = component_score_map.get("water_supply_deficit", {}).get("score", 0.0)
    fire_score = component_score_map.get("fire_frequency", {}).get("score", 0.0)
    severe_score = component_score_map.get("consequence_severity", {}).get("score", 0.0)
    priority_thresholds = thresholds.get("priority") or {}
    immediate_threshold = float(priority_thresholds.get("immediate", 70.0))
    targeted_threshold = float(priority_thresholds.get("targeted", 45.0))

    if risk_score >= immediate_threshold or (arrival_score >= 65 and water_score >= 55) or (is_rural and arrival_score >= 62 and fire_score >= 60):
        return "РќСѓР¶РЅС‹ РјРµСЂС‹ СЃРµР№С‡Р°СЃ", "fire"
    if risk_score >= targeted_threshold or max(fire_score, severe_score, arrival_score, water_score) >= 60:
        return "РќСѓР¶РЅС‹ С‚РѕС‡РµС‡РЅС‹Рµ РјРµСЂС‹", "sand"
    return "РџР»Р°РЅРѕРІРѕРµ РЅР°Р±Р»СЋРґРµРЅРёРµ", "sky"

def _risk_class(score: float, thresholds: dict[str, Any]) -> tuple[str, str]:
    risk_thresholds = thresholds.get("risk_class") or {}
    if score >= float(risk_thresholds.get("high", 67.0)):
        return "Р’С‹СЃРѕРєРёР№ СЂРёСЃРє", "high"
    if score >= float(risk_thresholds.get("medium", 43.0)):
        return "РЎСЂРµРґРЅРёР№ СЂРёСЃРє", "medium"
    return "РќРёР·РєРёР№ СЂРёСЃРє", "low"

def _recommended_action(
    risk_score: float,
    component_scores: Sequence[dict[str, Any]],
    context: dict[str, Any],
) -> tuple[str, str, List[Dict[str, str]]]:
    component_map = {item["key"]: item for item in component_scores}
    recommendations: List[Dict[str, str]] = []

    fire_component = component_map.get("fire_frequency", {})
    severe_component = component_map.get("consequence_severity", {})
    arrival_component = component_map.get("long_arrival_risk", {})
    water_component = component_map.get("water_supply_deficit", {})

    if fire_component.get("score", 0.0) >= 55:
        if context["heating_share"] >= 0.45:
            recommendations.append(
                {
                    "label": "РЈСЃРёР»РёС‚СЊ Р°РґСЂРµСЃРЅСѓСЋ РїСЂРѕС„РёР»Р°РєС‚РёРєСѓ РїРѕ РѕС‚РѕРїР»РµРЅРёСЋ Рё СЌР»РµРєС‚СЂРёРєРµ",
                    "detail": "РџРѕР»РµР·РЅРѕ РїСЂРѕРІРµСЂРёС‚СЊ РїРµС‡Рё, СЌР»РµРєС‚СЂРѕС…РѕР·СЏР№СЃС‚РІРѕ Рё РїРѕРІС‚РѕСЂСЏСЋС‰РёРµСЃСЏ Р±С‹С‚РѕРІС‹Рµ РїСЂРёС‡РёРЅС‹ РёРјРµРЅРЅРѕ РЅР° СЌС‚РѕР№ С‚РµСЂСЂРёС‚РѕСЂРёРё РґРѕ СЃР»РµРґСѓСЋС‰РµРіРѕ РїРёРєР° РЅР°РіСЂСѓР·РєРё.",
                }
            )
        else:
            recommendations.append(
                {
                    "label": "Р Р°Р·РѕР±СЂР°С‚СЊ РїРѕРІС‚РѕСЂСЏСЋС‰РёРµСЃСЏ РѕС‡Р°РіРё Рё РїСЂРёС‡РёРЅС‹ РїРѕР¶Р°СЂРѕРІ",
                    "detail": "РЎС„РѕРєСѓСЃРёСЂСѓР№С‚РµСЃСЊ РЅР° Р°РґСЂРµСЃР°С… Рё СЃС†РµРЅР°СЂРёСЏС…, РєРѕС‚РѕСЂС‹Рµ СѓР¶Рµ РїРѕРІС‚РѕСЂСЏР»РёСЃСЊ РІ РёСЃС‚РѕСЂРёРё СЌС‚РѕР№ С‚РµСЂСЂРёС‚РѕСЂРёРё, С‡С‚РѕР±С‹ СЃРЅРёР·РёС‚СЊ РІС…РѕРґСЏС‰РёР№ РїРѕС‚РѕРє РїРѕР¶Р°СЂРѕРІ.",
                }
            )

    if severe_component.get("score", 0.0) >= 55:
        recommendations.append(
            {
                "label": "РџСЂРѕРІРµСЂРёС‚СЊ СѓСЏР·РІРёРјС‹Рµ РѕР±СЉРµРєС‚С‹ Рё РґРѕРјРѕС…РѕР·СЏР№СЃС‚РІР°",
                "detail": "РџСЂРёРѕСЂРёС‚РµС‚РЅРѕ РїСЂРѕР№РґРёС‚Рµ РѕР±СЉРµРєС‚С‹ СЃ РёСЃС‚РѕСЂРёРµР№ СѓС‰РµСЂР±Р°, РѕРґРёРЅРѕРєРѕ РїСЂРѕР¶РёРІР°СЋС‰РёС…, СЃРѕС†РёР°Р»СЊРЅС‹Рµ РѕР±СЉРµРєС‚С‹ Рё СЃРµР»СЊС…РѕР·РѕР±СЉРµРєС‚С‹, РіРґРµ РїРѕСЃР»РµРґСЃС‚РІРёСЏ РјРѕРіСѓС‚ Р±С‹С‚СЊ С‚СЏР¶РµР»РµРµ.",
            }
        )

    if arrival_component.get("score", 0.0) >= 55:
        detail = (
            "РџСЂРѕРІРµСЂСЊС‚Рµ РјР°СЂС€СЂСѓС‚, С„Р°РєС‚РёС‡РµСЃРєРёР№ travel-time, СЂРµР·РµСЂРІ РїСЂРёРєСЂС‹С‚РёСЏ Рё РґРµСЂР¶РёС‚СЃСЏ Р»Рё С‚РµСЂСЂРёС‚РѕСЂРёСЏ РІ СѓСЃС‚РѕР№С‡РёРІРѕР№ Р·РѕРЅРµ РѕР±СЃР»СѓР¶РёРІР°РЅРёСЏ РџР§."
        )
        if context["service_coverage_ratio"] < 0.45:
            detail = (
                "Р”Р»СЏ С‚РµСЂСЂРёС‚РѕСЂРёРё СЃ РґРµС„РёС†РёС‚РѕРј РїСЂРёРєСЂС‹С‚РёСЏ РїРѕР»РµР·РЅРѕ РїРµСЂРµРїСЂРѕРІРµСЂРёС‚СЊ РјР°СЂС€СЂСѓС‚, СЂРµР·РµСЂРІ РїСЂРёРєСЂС‹С‚РёСЏ, РїСЂРѕРјРµР¶СѓС‚РѕС‡РЅРѕРµ СЂР°Р·РјРµС‰РµРЅРёРµ С‚РµС…РЅРёРєРё РёР»Рё Р”РџРљ Рё СЂРµР°Р»СЊРЅС‹Р№ РЅРѕСЂРјР°С‚РёРІ РґРѕРµР·РґР°."
            )
        elif context["avg_distance"] is not None and context["avg_distance"] >= 15.0:
            detail = (
                "Р”Р»СЏ СѓРґР°Р»С‘РЅРЅРѕР№ С‚РµСЂСЂРёС‚РѕСЂРёРё РїРѕР»РµР·РЅРѕ РїРµСЂРµРїСЂРѕРІРµСЂРёС‚СЊ РјР°СЂС€СЂСѓС‚, СЂРµР·РµСЂРІ РїСЂРёРєСЂС‹С‚РёСЏ Рё РІРѕР·РјРѕР¶РЅРѕСЃС‚СЊ РїСЂРѕРјРµР¶СѓС‚РѕС‡РЅРѕРіРѕ СЂР°Р·РјРµС‰РµРЅРёСЏ С‚РµС…РЅРёРєРё РёР»Рё Р”РџРљ."
            )
        recommendations.append(
            {
                "label": "РЎРѕРєСЂР°С‚РёС‚СЊ СЂРёСЃРє РґРѕР»РіРѕРіРѕ РїСЂРёР±С‹С‚РёСЏ",
                "detail": detail,
            }
        )

    if water_component.get("score", 0.0) >= 50:
        recommendations.append(
            {
                "label": "РџРѕРґС‚РІРµСЂРґРёС‚СЊ РІРѕРґСѓ Рё РїРѕРґСЉРµР·Рґ Рє РёСЃС‚РѕС‡РЅРёРєР°Рј",
                "detail": "РџСЂРѕРІРµСЂСЊС‚Рµ РіРёРґСЂР°РЅС‚С‹, Р±Р°С€РЅРё, РІРѕРґРѕС‘РјС‹, СЃСѓС…РёРµ РєРѕР»РѕРґС†С‹ Рё Р·РёРјРЅРёР№/СЂР°СЃРїСѓС‚РёС†РЅС‹Р№ РїРѕРґСЉРµР·Рґ Рє РЅРёРј, С‡С‚РѕР±С‹ РІРѕРґР° Р±С‹Р»Р° СЂРµР°Р»СЊРЅРѕ РґРѕСЃС‚СѓРїРЅР° РЅР° РІС‹РµР·РґРµ.",
            }
        )

    if not recommendations:
        recommendations.append(
            {
                "label": "РћСЃС‚Р°РІРёС‚СЊ С‚РµСЂСЂРёС‚РѕСЂРёСЋ РІ РїР»Р°РЅРѕРІРѕРј РЅР°Р±Р»СЋРґРµРЅРёРё",
                "detail": "РЎРµР№С‡Р°СЃ РґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РѕР±С‹С‡РЅРѕРіРѕ РєРѕРЅС‚СЂРѕР»СЏ, СЃРµР·РѕРЅРЅРѕР№ РїСЂРѕС„РёР»Р°РєС‚РёРєРё Рё РїРµСЂРёРѕРґРёС‡РµСЃРєРѕР№ СЃРІРµСЂРєРё Р»РѕРіРёСЃС‚РёРєРё Рё РёСЃС‚РѕС‡РЅРёРєРѕРІ РІРѕРґС‹.",
            }
        )

    top_component = component_scores[0] if component_scores else {"key": "fire_frequency"}
    action_lookup = {
        "fire_frequency": "РЈСЃРёР»РёС‚СЊ Р°РґСЂРµСЃРЅСѓСЋ РїСЂРѕС„РёР»Р°РєС‚РёРєСѓ",
        "consequence_severity": "РЎРЅРёР·РёС‚СЊ С‚СЏР¶РµСЃС‚СЊ РІРѕР·РјРѕР¶РЅС‹С… РїРѕСЃР»РµРґСЃС‚РІРёР№",
        "long_arrival_risk": "РЎРѕРєСЂР°С‚РёС‚СЊ РІСЂРµРјСЏ РїСЂРёР±С‹С‚РёСЏ",
        "water_supply_deficit": "РџРѕРґС‚РІРµСЂРґРёС‚СЊ РІРѕРґРѕСЃРЅР°Р±Р¶РµРЅРёРµ",
    }
    action_label = action_lookup.get(top_component.get("key"), recommendations[0]["label"])

    if risk_score >= 70 and len(recommendations) >= 2:
        action_hint = f"РЎРЅР°С‡Р°Р»Р° {recommendations[0]['label'].lower()}, Р·Р°С‚РµРј {recommendations[1]['label'].lower()}."
    else:
        action_hint = recommendations[0]["detail"]
    return action_label, action_hint, recommendations[:3]

def _build_formula_display(component_scores: Sequence[dict[str, Any]], risk_score: float) -> str:
    parts = [f"{item['label']} {_format_number(item['contribution'])}" for item in component_scores]
    return f"{' + '.join(parts)} = {_format_number(risk_score)}"

def _attach_ranking_context(territory_rows: List[dict[str, Any]]) -> None:
    if not territory_rows:
        return

    top_score = float(territory_rows[0].get("risk_score") or 0.0)
    for index, item in enumerate(territory_rows):
        next_score = float(territory_rows[index + 1].get("risk_score") or 0.0) if index + 1 < len(territory_rows) else None
        current_score = float(item.get("risk_score") or 0.0)
        gap_to_next = max(0.0, round(current_score - next_score, 1)) if next_score is not None else 0.0
        gap_to_top = max(0.0, round(top_score - current_score, 1))
        strongest_components = [
            f"{component.get('label') or 'РљРѕРјРїРѕРЅРµРЅС‚'} ({component.get('contribution_display') or '0 Р±Р°Р»Р»Р°'})"
            for component in (item.get("component_scores") or [])[:2]
        ]
        component_lead = ", ".join(strongest_components) if strongest_components else "РЅРµС‚ РІС‹СЂР°Р¶РµРЅРЅРѕРіРѕ РґРѕРјРёРЅРёСЂСѓСЋС‰РµРіРѕ РєРѕРјРїРѕРЅРµРЅС‚Р°"
        item.update(
            {
                "ranking_position": index + 1,
                "ranking_position_display": f"в„–{index + 1}",
                "ranking_gap_to_next": gap_to_next,
                "ranking_gap_to_next_display": f"{_format_number(gap_to_next)} Р±Р°Р»Р»Р°" if next_score is not None else "Р·Р°РјС‹РєР°РµС‚ С‚РµРєСѓС‰РёР№ СЃРїРёСЃРѕРє",
                "ranking_gap_to_top": gap_to_top,
                "ranking_gap_to_top_display": f"{_format_number(gap_to_top)} Р±Р°Р»Р»Р°",
                "ranking_component_lead": component_lead,
                "ranking_reason": _build_ranking_reason(index, gap_to_next, gap_to_top, component_lead),
            }
        )

def _build_ranking_reason(index: int, gap_to_next: float, gap_to_top: float, component_lead: str) -> str:
    if index == 0:
        if gap_to_next >= 4.0:
            return f"РўРµСЂСЂРёС‚РѕСЂРёСЏ Р»РёРґРёСЂСѓРµС‚ СЃ Р·Р°РјРµС‚РЅС‹Рј РѕС‚СЂС‹РІРѕРј {_format_number(gap_to_next)} Р±Р°Р»Р»Р°; РѕСЃРЅРѕРІРЅРѕР№ РІРєР»Р°Рґ РґР°СЋС‚ {component_lead}."
        if gap_to_next >= 1.5:
            return f"РўРµСЂСЂРёС‚РѕСЂРёСЏ СѓРґРµСЂР¶РёРІР°РµС‚ РїРµСЂРІРѕРµ РјРµСЃС‚Рѕ СЃ СЂР°Р±РѕС‡РёРј РѕС‚СЂС‹РІРѕРј {_format_number(gap_to_next)} Р±Р°Р»Р»Р°; РѕСЃРЅРѕРІРЅРѕР№ РІРєР»Р°Рґ РґР°СЋС‚ {component_lead}."
        return f"РўРµСЂСЂРёС‚РѕСЂРёСЏ РёРґРµС‚ РїРµСЂРІРѕР№ РІ РїР»РѕС‚РЅРѕР№ РіСЂСѓРїРїРµ; РѕС‚СЂС‹РІ РѕС‚ СЃР»РµРґСѓСЋС‰РµР№ С‚РµСЂСЂРёС‚РѕСЂРёРё {_format_number(gap_to_next)} Р±Р°Р»Р»Р°, РѕСЃРЅРѕРІРЅРѕР№ РІРєР»Р°Рґ РґР°СЋС‚ {component_lead}."

    if gap_to_top <= 2.0:
        return f"РўРµСЂСЂРёС‚РѕСЂРёСЏ РґРµСЂР¶РёС‚СЃСЏ СЂСЏРґРѕРј СЃ Р»РёРґРµСЂРѕРј: РѕС‚СЃС‚Р°РІР°РЅРёРµ {_format_number(gap_to_top)} Р±Р°Р»Р»Р°, РєР»СЋС‡РµРІС‹Рµ РІРєР»Р°РґС‹ {component_lead}."
    if gap_to_top <= 6.0:
        return f"РўРµСЂСЂРёС‚РѕСЂРёСЏ РІС…РѕРґРёС‚ РІ РІРµСЂС…РЅСЋСЋ РіСЂСѓРїРїСѓ: РѕС‚СЃС‚Р°РІР°РЅРёРµ {_format_number(gap_to_top)} Р±Р°Р»Р»Р°, РєР»СЋС‡РµРІС‹Рµ РІРєР»Р°РґС‹ {component_lead}."
    return f"РўРµСЂСЂРёС‚РѕСЂРёСЏ РѕСЃС‚Р°РµС‚СЃСЏ РІ СЃРїРёСЃРєРµ РёР·-Р·Р° РІРєР»Р°РґРѕРІ {component_lead}, С…РѕС‚СЏ РЅРёР¶Рµ Р»РёРґРµСЂР° РЅР° {_format_number(gap_to_top)} Р±Р°Р»Р»Р°."

def _top_territory_lead(top_territory: Optional[dict[str, Any]]) -> str:
    if not top_territory:
        return "РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ Р»РёРґРёСЂСѓСЋС‰РµР№ С‚РµСЂСЂРёС‚РѕСЂРёРё."
    strongest_components = ", ".join(
        f"{item['label']} ({item['contribution_display']})"
        for item in (top_territory.get("component_scores") or [])[:2]
    )
    parts = [
        f"{top_territory['action_label']}. РС‚РѕРіРѕРІС‹Р№ СЂРёСЃРє {top_territory['risk_display']} С„РѕСЂРјРёСЂСѓСЋС‚ РїСЂРµР¶РґРµ РІСЃРµРіРѕ {strongest_components}.",
        f"Р›РѕРіРёСЃС‚РёРєР°: {top_territory.get('travel_time_display') or 'РЅ/Рґ'}, РїРѕРєСЂС‹С‚РёРµ РџР§ {top_territory.get('fire_station_coverage_display') or 'РЅ/Рґ'}, Р·РѕРЅР° {top_territory.get('service_zone_label') or 'РЅРµ РѕРїСЂРµРґРµР»РµРЅР°'}.",
        top_territory.get("ranking_reason") or "",
        top_territory.get("action_hint") or "",
    ]
    return " ".join(part.strip() for part in parts if str(part).strip())

def _water_supply_display(available_count: int, known_count: int) -> str:
    if known_count <= 0:
        return "РЅРµС‚ РїРѕРґС‚РІРµСЂР¶РґРµРЅРЅС‹С… РґР°РЅРЅС‹С…"
    return f"РїРѕРґС‚РІРµСЂР¶РґРµРЅР° РІ {_format_percent(available_count / known_count * 100.0)} СЃР»СѓС‡Р°РµРІ"
