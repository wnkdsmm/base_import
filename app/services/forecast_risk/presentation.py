from __future__ import annotations

from typing import Any, List, Sequence

from .types import (
    FeatureCard,
    FeatureSource,
    GeoSummary,
    QualityPassport,
    RiskPresentation,
    RiskProfile,
    RiskScore,
    TopConfidence,
)
from .utils import _format_integer, _scan_columns


def _table_scope_label(count: int) -> str:
    count_display = _format_integer(count)
    if count == 1:
        return f"Р’ {count_display} С‚Р°Р±Р»РёС†Рµ"
    return f"Р’ {count_display} С‚Р°Р±Р»РёС†Р°С…"


def _compact_feature_sources(sources: Sequence[FeatureSource]) -> str:
    if not sources:
        return "РќРµ РЅР°Р№РґРµРЅР°"

    grouped_sources: List[dict[str, Any]] = []
    for item in sources:
        columns = tuple(item.get("columns") or ())
        if not columns:
            continue
        matched_group = next((group for group in grouped_sources if group["columns"] == columns), None)
        if matched_group is None:
            grouped_sources.append({"columns": columns, "tables": [item.get("table_name") or "РўР°Р±Р»РёС†Р°"]})
            continue
        matched_group["tables"].append(item.get("table_name") or "РўР°Р±Р»РёС†Р°")

    if not grouped_sources:
        return "РќРµ РЅР°Р№РґРµРЅР°"

    parts: List[str] = []
    for group in grouped_sources[:3]:
        columns_text = ", ".join(group["columns"][:4])
        tables = group["tables"]
        if len(tables) == 1:
            parts.append(f"{tables[0]}: {columns_text}")
        else:
            parts.append(f"{_table_scope_label(len(tables))}: {columns_text}")
    remaining_groups = len(grouped_sources) - len(parts)
    if remaining_groups > 0:
        parts.append(f"РµС‰С‘ {_format_integer(remaining_groups)} РЅР°Р±РѕСЂР°")
    return "; ".join(parts)


def _build_feature_cards(metadata_items: Sequence[dict[str, Any]]) -> List[FeatureCard]:
    if not metadata_items:
        return []
    feature_config = [
        {
            "label": "РўРµСЂСЂРёС‚РѕСЂРёСЏ Рё РЅР°СЃРµР»С‘РЅРЅС‹Р№ РїСѓРЅРєС‚",
            "description": "РќСѓР¶РЅС‹ РґР»СЏ СЂР°РЅР¶РёСЂРѕРІР°РЅРёСЏ СЃРµР»СЊСЃРѕРІРµС‚РѕРІ Рё РЅР°СЃРµР»С‘РЅРЅС‹С… РїСѓРЅРєС‚РѕРІ.",
            "resolved_keys": ["territory_label", "district"],
            "minimum_matches": 1,
        },
        {
            "label": "РЈРґР°Р»С‘РЅРЅРѕСЃС‚СЊ РѕС‚ РїРѕР¶Р°СЂРЅРѕР№ С‡Р°СЃС‚Рё",
            "description": "РСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РґР»СЏ РѕС†РµРЅРєРё Р»РѕРіРёСЃС‚РёС‡РµСЃРєРѕРіРѕ СЂРёСЃРєР°.",
            "resolved_keys": ["fire_station_distance"],
            "minimum_matches": 1,
        },
        {
            "label": "Р’СЂРµРјСЏ СЃРѕРѕР±С‰РµРЅРёСЏ Рё РїСЂРёР±С‹С‚РёСЏ",
            "description": "РџРѕРјРѕРіР°РµС‚ РѕС†РµРЅРёРІР°С‚СЊ РІРµСЂРѕСЏС‚РЅРѕСЃС‚СЊ Р±РѕР»СЊС€РѕРіРѕ РІСЂРµРјРµРЅРё РїСЂРёР±С‹С‚РёСЏ РїРѕРґСЂР°Р·РґРµР»РµРЅРёР№.",
            "resolved_keys": ["arrival_time", "report_time", "detection_time"],
            "minimum_matches": 2,
        },
        {
            "label": "РќР°СЂСѓР¶РЅРѕРµ РІРѕРґРѕСЃРЅР°Р±Р¶РµРЅРёРµ",
            "description": "РЈС‡РёС‚С‹РІР°РµС‚СЃСЏ РєР°Рє С„Р°РєС‚РѕСЂ СЃРЅРёР¶РµРЅРёСЏ С‚СЏР¶С‘Р»С‹С… РїРѕСЃР»РµРґСЃС‚РІРёР№.",
            "resolved_keys": ["water_supply_count", "water_supply_details"],
            "minimum_matches": 1,
        },
        {
            "label": "РўРёРї Р·Р°СЃС‚СЂРѕР№РєРё Рё РѕР±СЉРµРєС‚РѕРІ",
            "description": "РљРѕРјР±РёРЅРёСЂСѓРµС‚ С‚РёРї РЅР°СЃРµР»С‘РЅРЅРѕРіРѕ РїСѓРЅРєС‚Р°, РєР°С‚РµРіРѕСЂРёСЋ Р·РґР°РЅРёСЏ Рё РѕР±СЉРµРєС‚Р°.",
            "resolved_keys": ["settlement_type", "building_category", "object_category"],
            "minimum_matches": 2,
        },
        {
            "label": "РџРѕРіРѕРґРЅС‹Рµ СѓСЃР»РѕРІРёСЏ",
            "description": "РЎРµР№С‡Р°СЃ РєР°Рє РїРѕРіРѕРґРЅС‹Р№ СЃРёРіРЅР°Р» РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РґРѕСЃС‚СѓРїРЅР°СЏ С‚РµРјРїРµСЂР°С‚СѓСЂР°.",
            "resolved_keys": ["temperature"],
            "minimum_matches": 1,
        },
        {
            "label": "РСЃС‚РѕСЂРёСЏ РїРѕР¶Р°СЂРѕРІ",
            "description": "Р”Р°С‚Р° Рё РїСЂРёС‡РёРЅС‹ РїРѕРјРѕРіР°СЋС‚ СѓС‡РёС‚С‹РІР°С‚СЊ РїРѕРІС‚РѕСЂСЏРµРјРѕСЃС‚СЊ, СЃРµР·РѕРЅ Рё РїСЂРѕС„РёР»СЊ С‚РµСЂСЂРёС‚РѕСЂРёРё.",
            "resolved_keys": ["date", "cause"],
            "minimum_matches": 1,
        },
        {
            "label": "РўСЏР¶С‘Р»С‹Рµ РїРѕСЃР»РµРґСЃС‚РІРёСЏ",
            "description": "РСЃРїРѕР»СЊР·СѓСЋС‚СЃСЏ РїРѕСЃР»РµРґСЃС‚РІРёСЏ, СѓС‰РµСЂР± Рё РїРѕСЃС‚СЂР°РґР°РІС€РёРµ РґР»СЏ РѕС†РµРЅРєРё С‚СЏР¶РµСЃС‚Рё СЃС†РµРЅР°СЂРёСЏ.",
            "resolved_keys": [
                "consequence",
                "registered_damage",
                "destroyed_buildings",
                "destroyed_area",
                "casualty_flag",
                "injuries",
                "deaths",
            ],
            "minimum_matches": 2,
        },
        {
            "label": "Р’СЂРµРјСЏ СЃСѓС‚РѕРє Рё РѕС‚РѕРїРёС‚РµР»СЊРЅС‹Р№ РїРµСЂРёРѕРґ",
            "description": "РЈС‡РёС‚С‹РІР°СЋС‚СЃСЏ РЅРѕС‡РЅС‹Рµ РёРЅС†РёРґРµРЅС‚С‹, РґРµРЅСЊ РЅРµРґРµР»Рё Рё СЃРµР·РѕРЅ РѕС‚РѕРїР»РµРЅРёСЏ.",
            "resolved_keys": ["date", "report_time", "detection_time", "heating_type"],
            "minimum_matches": 2,
        },
        {
            "label": "РџРѕРґСЉРµР·РґРЅС‹Рµ РїСѓС‚Рё",
            "description": "Р•СЃР»Рё РµСЃС‚СЊ РѕС‚РґРµР»СЊРЅС‹Рµ РєРѕР»РѕРЅРєРё РїРѕ РґРѕСЂРѕРіР°Рј РёР»Рё РїРѕРґСЉРµР·РґСѓ, РѕРЅРё С‚РѕР¶Рµ Р±СѓРґСѓС‚ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊСЃСЏ.",
            "scan_tokens": [["РїРѕРґСЉРµР·Рґ"], ["РґРѕСЂРѕРі"], ["РїСѓС‚СЊ"]],
            "minimum_matches": 1,
        },
        {
            "label": "РџР»РѕС‚РЅРѕСЃС‚СЊ РЅР°СЃРµР»РµРЅРёСЏ",
            "description": "РќСѓР¶РЅР° РґР»СЏ РїСЂСЏРјРѕР№ РѕС†РµРЅРєРё РЅР°РіСЂСѓР·РєРё РЅР° С‚РµСЂСЂРёС‚РѕСЂРёСЋ.",
            "scan_tokens": [["РїР»РѕС‚РЅРѕСЃС‚", "РЅР°СЃРµР»РµРЅ"], ["С‡РёСЃР»РµРЅРЅРѕСЃС‚", "РЅР°СЃРµР»РµРЅ"]],
            "minimum_matches": 1,
        },
    ]
    total_tables = len(metadata_items)
    cards: List[FeatureCard] = []
    for feature in feature_config:
        full_tables = 0
        partial_tables = 0
        sources: List[FeatureSource] = []
        for item in metadata_items:
            found_columns: List[str] = []
            for key in feature.get("resolved_keys", []):
                column_name = item["resolved_columns"].get(key)
                if column_name:
                    found_columns.append(column_name)
            if feature.get("scan_tokens"):
                found_columns.extend(_scan_columns(item["columns"], feature["scan_tokens"]))
            found_columns = list(dict.fromkeys(column_name for column_name in found_columns if column_name))
            if len(found_columns) >= feature.get("minimum_matches", 1):
                full_tables += 1
            elif found_columns:
                partial_tables += 1
            if found_columns:
                sources.append(
                    {
                        "table_name": item["table_name"],
                        "columns": found_columns[:4],
                    }
                )
        if full_tables == total_tables and total_tables > 0:
            status, status_label = ("used", "РСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ")
        elif full_tables > 0 or partial_tables > 0:
            status, status_label = ("partial", f"Р§Р°СЃС‚РёС‡РЅРѕ ({full_tables + partial_tables}/{total_tables})")
        else:
            status, status_label = ("missing", "РќРµ РЅР°Р№РґРµРЅР°")
        cards.append(
            {
                "label": feature["label"],
                "description": feature["description"],
                "status": status,
                "status_label": status_label,
                "source": _compact_feature_sources(sources),
            }
        )
    return cards


def _build_quality_passport(
    feature_cards: Sequence[FeatureCard],
    metadata_items: Sequence[dict[str, Any]],
) -> QualityPassport:
    used_labels = [item["label"] for item in feature_cards if item.get("status") == "used"]
    partial_labels = [item["label"] for item in feature_cards if item.get("status") == "partial"]
    missing_labels = [item["label"] for item in feature_cards if item.get("status") == "missing"]
    total = len(feature_cards)
    used_count = len(used_labels)
    partial_count = len(partial_labels)
    missing_count = len(missing_labels)
    critical_labels = {
        "РўРµСЂСЂРёС‚РѕСЂРёСЏ Рё РЅР°СЃРµР»С‘РЅРЅС‹Р№ РїСѓРЅРєС‚",
        "РСЃС‚РѕСЂРёСЏ РїРѕР¶Р°СЂРѕРІ",
        "Р’СЂРµРјСЏ СЃРѕРѕР±С‰РµРЅРёСЏ Рё РїСЂРёР±С‹С‚РёСЏ",
        "РќР°СЂСѓР¶РЅРѕРµ РІРѕРґРѕСЃРЅР°Р±Р¶РµРЅРёРµ",
        "РўСЏР¶С‘Р»С‹Рµ РїРѕСЃР»РµРґСЃС‚РІРёСЏ",
    }
    critical_gaps = [label for label in missing_labels if label in critical_labels]
    if total > 0:
        raw_score = (used_count + partial_count * 0.55) / total * 100.0
    else:
        raw_score = 0.0
    raw_score -= min(len(critical_gaps), 3) * 8.0
    confidence_score = max(0, min(100, int(round(raw_score))))
    if confidence_score >= 80:
        confidence_label = "Р’С‹СЃРѕРєР°СЏ"
        confidence_tone = "forest"
        validation_label = "Р’Р°Р»РёРґР°С†РёСЏ РґР°РЅРЅС‹С… РїСЂРѕР№РґРµРЅР°"
    elif confidence_score >= 60:
        confidence_label = "Р Р°Р±РѕС‡Р°СЏ"
        confidence_tone = "sky"
        validation_label = "Р’Р°Р»РёРґР°С†РёСЏ РґР°РЅРЅС‹С… РІ РѕСЃРЅРѕРІРЅРѕРј РїСЂРѕР№РґРµРЅР°"
    elif confidence_score >= 40:
        confidence_label = "РЈРјРµСЂРµРЅРЅР°СЏ"
        confidence_tone = "sand"
        validation_label = "Р’Р°Р»РёРґР°С†РёСЏ РґР°РЅРЅС‹С… С‡Р°СЃС‚РёС‡РЅР°СЏ"
    else:
        confidence_label = "РћРіСЂР°РЅРёС‡РµРЅРЅР°СЏ"
        confidence_tone = "fire"
        validation_label = "Р’Р°Р»РёРґР°С†РёСЏ РґР°РЅРЅС‹С… РѕРіСЂР°РЅРёС‡РµРЅР°"
    if critical_gaps:
        validation_summary = (
            "Р§Р°СЃС‚СЊ РєСЂРёС‚РёС‡РЅС‹С… РіСЂСѓРїРї РґР°РЅРЅС‹С… РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚, РїРѕСЌС‚РѕРјСѓ СЂРµРєРѕРјРµРЅРґР°С†РёРё СЃС‚РѕРёС‚ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ РєР°Рє РїСЂРёРѕСЂРёС‚РёР·Р°С†РёСЋ РґР»СЏ РїСЂРѕРІРµСЂРєРё, "
            "Р° РЅРµ РєР°Рє РѕРєРѕРЅС‡Р°С‚РµР»СЊРЅРѕРµ СЂРµС€РµРЅРёРµ."
        )
    elif partial_count:
        validation_summary = (
            "РљР»СЋС‡РµРІС‹Рµ РіСЂСѓРїРїС‹ РґР°РЅРЅС‹С… РІ РѕСЃРЅРѕРІРЅРѕРј РЅР°Р№РґРµРЅС‹, РЅРѕ С‡Р°СЃС‚СЊ РїСЂРёР·РЅР°РєРѕРІ РґРѕСЃС‚СѓРїРЅР° РЅРµ РІРѕ РІСЃРµС… С‚Р°Р±Р»РёС†Р°С…. "
            "Р’С‹РІРѕРґС‹ РїСЂРёРіРѕРґРЅС‹ РґР»СЏ РїСЂР°РєС‚РёС‡РµСЃРєРѕР№ РїСЂРёРѕСЂРёС‚РёР·Р°С†РёРё, РѕРґРЅР°РєРѕ РёС… Р»СѓС‡С€Рµ РїРѕРґС‚РІРµСЂР¶РґР°С‚СЊ Р»РѕРєР°Р»СЊРЅРѕР№ РїСЂРѕРІРµСЂРєРѕР№."
        )
    else:
        validation_summary = (
            "РљР»СЋС‡РµРІС‹Рµ РіСЂСѓРїРїС‹ РґР°РЅРЅС‹С… РЅР°Р№РґРµРЅС‹, РїРѕСЌС‚РѕРјСѓ СЂРµРєРѕРјРµРЅРґР°С†РёРё РјРѕР¶РЅРѕ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ РєР°Рє СЂР°Р±РѕС‡СѓСЋ РѕСЃРЅРѕРІСѓ РґР»СЏ РїСЂРёРѕСЂРёС‚РёР·Р°С†РёРё "
            "С‚РµСЂСЂРёС‚РѕСЂРёР№ Рё РїСЂРѕС„РёР»Р°РєС‚РёС‡РµСЃРєРёС… РјРµСЂ."
        )
    reliability_notes = []
    if metadata_items:
        reliability_notes.append(f"РџР°СЃРїРѕСЂС‚ СЃРѕР±СЂР°РЅ РїРѕ {_format_integer(len(metadata_items))} С‚Р°Р±Р»РёС†Р°Рј Р±Р°Р·С‹.")
    if critical_gaps:
        reliability_notes.append("РљСЂРёС‚РёС‡РЅС‹Рµ РїСЂРѕР±РµР»С‹: " + ", ".join(critical_gaps[:3]) + ".")
    elif missing_count:
        reliability_notes.append("Р•СЃС‚СЊ РЅРµРїСЂСЏРјС‹Рµ РїСЂРѕР±РµР»С‹ РІ РґР°РЅРЅС‹С…, РЅРѕ Р±Р°Р·РѕРІС‹Рµ СЂРµРєРѕРјРµРЅРґР°С†РёРё РїРѕ С‚РµСЂСЂРёС‚РѕСЂРёСЏРј РІСЃС‘ РµС‰С‘ С„РѕСЂРјРёСЂСѓСЋС‚СЃСЏ.")
    else:
        reliability_notes.append("РљСЂРёС‚РёС‡РЅС‹С… РїСЂРѕР±РµР»РѕРІ РІ РєР»СЋС‡РµРІС‹С… РіСЂСѓРїРїР°С… РґР°РЅРЅС‹С… РЅРµ РЅР°Р№РґРµРЅРѕ.")
    return {
        "title": "РџР°СЃРїРѕСЂС‚ РєР°С‡РµСЃС‚РІР° РґР°РЅРЅС‹С…",
        "confidence_score": confidence_score,
        "confidence_score_display": f"{confidence_score} / 100",
        "confidence_label": confidence_label,
        "confidence_tone": confidence_tone,
        "validation_label": validation_label,
        "validation_summary": validation_summary,
        "table_count_display": _format_integer(len(metadata_items)),
        "used_count_display": _format_integer(used_count),
        "partial_count_display": _format_integer(partial_count),
        "missing_count_display": _format_integer(missing_count),
        "critical_gaps": critical_gaps,
        "used_labels": used_labels,
        "partial_labels": partial_labels,
        "missing_labels": missing_labels,
        "reliability_notes": reliability_notes,
    }


def _build_geo_summary(geo_prediction: dict[str, Any]) -> GeoSummary:
    hotspots = [
        {
            "label": item.get("short_label") or item.get("location_label") or "Р—РѕРЅР°",
            "risk_display": item.get("risk_display") or "0 / 100",
            "meta": item.get("explanation") or "РќРµС‚ РїРѕСЏСЃРЅРµРЅРёСЏ РїРѕ Р·РѕРЅРµ.",
        }
        for item in (geo_prediction.get("hotspots") or [])[:5]
    ]
    districts = [
        {
            "label": item.get("label") or "Р Р°Р№РѕРЅ",
            "risk_display": item.get("peak_risk_display") or item.get("avg_risk_display") or "0 / 100",
            "meta": f"Р·РѕРЅ: {item.get('zones_display', '0')} | РїРѕР¶Р°СЂРѕРІ: {item.get('incidents_display', '0')}",
        }
        for item in (geo_prediction.get("districts") or [])[:5]
    ]
    has_coordinates = bool(geo_prediction.get("has_coordinates"))
    has_map_points = bool(geo_prediction.get("points"))
    if not has_coordinates:
        compact_message = "Р’ РІС‹Р±СЂР°РЅРЅРѕРј СЃСЂРµР·Рµ РЅРµС‚ РєРѕРѕСЂРґРёРЅР°С‚, РїРѕСЌС‚РѕРјСѓ РєР°СЂС‚Р° Р·РѕРЅ СЂРёСЃРєР° СЃРµР№С‡Р°СЃ РЅРµ СЃС‚СЂРѕРёС‚СЃСЏ."
    elif not has_map_points:
        compact_message = "РљРѕРѕСЂРґРёРЅР°С‚С‹ РµСЃС‚СЊ, РЅРѕ РґР»СЏ СѓСЃС‚РѕР№С‡РёРІРѕР№ РєР°СЂС‚С‹ Р·РѕРЅ РїРѕРєР° РЅРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РЅР°Р±Р»СЋРґРµРЅРёР№."
    else:
        compact_message = ""
    return {
        "has_coordinates": has_coordinates,
        "has_map_points": has_map_points,
        "compact_message": compact_message,
        "model_description": geo_prediction.get("model_description")
        or "РљР°СЂС‚Р° Р±Р»РѕРєР° РїРѕРґРґРµСЂР¶РєРё СЂРµС€РµРЅРёР№ РїРѕРєР°Р·С‹РІР°РµС‚ РїСЂРѕСЃС‚СЂР°РЅСЃС‚РІРµРЅРЅС‹Рµ Р·РѕРЅС‹ РІРЅРёРјР°РЅРёСЏ РґР»СЏ С‚РµСЂСЂРёС‚РѕСЂРёР°Р»СЊРЅРѕРіРѕ РїСЂРёРѕСЂРёС‚РµС‚Р°. РћРЅР° РЅРµ Р·Р°РјРµРЅСЏРµС‚ РєР°Р»РµРЅРґР°СЂСЊ СЂРёСЃРєР° РїРѕ РґРЅСЏРј.",
        "coverage_display": geo_prediction.get("coverage_display") or "0 СЃ РєРѕРѕСЂРґРёРЅР°С‚Р°РјРё",
        "top_zone_label": geo_prediction.get("top_zone_label") or "-",
        "top_risk_display": geo_prediction.get("top_risk_display") or "0 / 100",
        "hotspots_count_display": geo_prediction.get("hotspots_count_display") or "0",
        "top_explanation": geo_prediction.get("top_explanation") or "РќРµС‚ РґР°РЅРЅС‹С… РґР»СЏ РѕР±СЉСЏСЃРЅРµРЅРёСЏ Р·РѕРЅС‹ СЂРёСЃРєР°.",
        "hotspots": hotspots,
        "districts": districts,
    }


def _build_empty_decision_support_payload(
    *,
    title: str,
    model_description: str,
    coverage_display: str,
    quality_passport: QualityPassport,
    top_confidence: TopConfidence,
    feature_cards: Sequence[FeatureCard],
    weight_profile: RiskProfile,
    historical_validation: dict[str, Any],
    notes: Sequence[str],
    geo_summary: GeoSummary,
    geo_prediction: dict[str, Any],
) -> RiskPresentation:
    return {
        "has_data": False,
        "title": title,
        "model_description": model_description,
        "coverage_display": coverage_display,
        "quality_passport": quality_passport,
        "summary_cards": [],
        "top_territory_label": "-",
        "top_territory_explanation": "РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ СЂР°РЅР¶РёСЂРѕРІР°РЅРёСЏ С‚РµСЂСЂРёС‚РѕСЂРёР№.",
        "top_territory_confidence_label": top_confidence["label"],
        "top_territory_confidence_score_display": top_confidence["score_display"],
        "top_territory_confidence_tone": top_confidence["tone"],
        "top_territory_confidence_note": top_confidence["note"],
        "territories": [],
        "feature_cards": list(feature_cards),
        "weight_profile": weight_profile,
        "historical_validation": historical_validation,
        "notes": list(notes),
        "geo_summary": geo_summary,
        "geo_prediction": geo_prediction,
    }


def _build_decision_support_payload_response(
    *,
    title: str,
    model_description: str,
    coverage_display: str,
    quality_passport: QualityPassport,
    summary_cards: Sequence[dict[str, Any]],
    top_territory_label: str,
    top_territory_explanation: str,
    top_confidence: TopConfidence,
    territories: Sequence[RiskScore],
    feature_cards: Sequence[FeatureCard],
    weight_profile: RiskProfile,
    historical_validation: dict[str, Any],
    notes: Sequence[str],
    geo_summary: GeoSummary,
    geo_prediction: dict[str, Any],
) -> RiskPresentation:
    return {
        "has_data": bool(territories),
        "title": title,
        "model_description": model_description,
        "coverage_display": coverage_display,
        "quality_passport": quality_passport,
        "summary_cards": list(summary_cards),
        "top_territory_label": top_territory_label,
        "top_territory_explanation": top_territory_explanation,
        "top_territory_confidence_label": top_confidence["label"],
        "top_territory_confidence_score_display": top_confidence["score_display"],
        "top_territory_confidence_tone": top_confidence["tone"],
        "top_territory_confidence_note": top_confidence["note"],
        "territories": list(territories),
        "feature_cards": list(feature_cards),
        "weight_profile": weight_profile,
        "historical_validation": historical_validation,
        "notes": list(notes),
        "geo_summary": geo_summary,
        "geo_prediction": geo_prediction,
    }
