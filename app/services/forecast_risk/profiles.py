from __future__ import annotations

from copy import deepcopy
from typing import Any, List

from .types import ComponentWeightRow, RiskProfile
from .utils import _format_decimal, _format_integer

DEFAULT_RISK_WEIGHT_MODE = "adaptive"

EXPERT_RISK_WEIGHT_PROFILE: RiskProfile = {
    "mode": "expert",
    "mode_label": "Р­РєСЃРїРµСЂС‚РЅС‹Рµ РІРµСЃР°",
    "status_label": "Р­РєСЃРїРµСЂС‚РЅС‹Р№ РїСЂРѕС„РёР»СЊ",
    "status_tone": "forest",
    "description": (
        "Р‘Р°Р·РѕРІС‹Р№ РїСЂРѕС„РёР»СЊ РґР»СЏ СЃРµР»СЊСЃРєРёС… С‚РµСЂСЂРёС‚РѕСЂРёР№. РС‚РѕРіРѕРІС‹Р№ СЂРёСЃРє СЃРєР»Р°РґС‹РІР°РµС‚СЃСЏ РёР· С‡РµС‚С‹СЂРµС… РєРѕРјРїРѕРЅРµРЅС‚РѕРІ: "
        "С‡Р°СЃС‚РѕС‚С‹ РїРѕР¶Р°СЂРѕРІ, С‚СЏР¶РµСЃС‚Рё РїРѕСЃР»РµРґСЃС‚РІРёР№, СЂРёСЃРєР° РґРѕР»РіРѕРіРѕ РїСЂРёР±С‹С‚РёСЏ Рё РґРµС„РёС†РёС‚Р° РІРѕРґРѕСЃРЅР°Р±Р¶РµРЅРёСЏ."
    ),
    "component_order": [
        "fire_frequency",
        "consequence_severity",
        "long_arrival_risk",
        "water_supply_deficit",
    ],
    "component_weights": {
        "fire_frequency": 0.34,
        "consequence_severity": 0.24,
        "long_arrival_risk": 0.24,
        "water_supply_deficit": 0.18,
    },
    "rural_weight_shift": {
        "fire_frequency": -0.03,
        "consequence_severity": -0.02,
        "long_arrival_risk": 0.03,
        "water_supply_deficit": 0.02,
    },
    "components": {
        "fire_frequency": {
            "label": "Р§Р°СЃС‚РѕС‚Р° РїРѕР¶Р°СЂРѕРІ",
            "description": "РџРѕРІС‚РѕСЂСЏРµРјРѕСЃС‚СЊ Рё Р±Р»РёР·РѕСЃС‚СЊ РїСЂРѕС€Р»С‹С… РїРѕР¶Р°СЂРѕРІ Рє С‚РµРєСѓС‰РµРјСѓ РіРѕСЂРёР·РѕРЅС‚Сѓ РїР»Р°РЅРёСЂРѕРІР°РЅРёСЏ.",
            "signals": [
                {"key": "predicted_repeat_rate", "label": "РџРѕРІС‚РѕСЂСЏРµРјРѕСЃС‚СЊ РЅР° РіРѕСЂРёР·РѕРЅС‚Рµ", "weight": 0.44},
                {"key": "history_pressure", "label": "РќР°РєРѕРїР»РµРЅРЅР°СЏ РёСЃС‚РѕСЂРёСЏ", "weight": 0.22},
                {"key": "recency_pressure", "label": "РЎРІРµР¶РµСЃС‚СЊ СЃР»СѓС‡Р°РµРІ", "weight": 0.14},
                {"key": "seasonal_alignment", "label": "РЎРµР·РѕРЅРЅРѕРµ СЃРѕРІРїР°РґРµРЅРёРµ", "weight": 0.10},
                {"key": "heating_pressure", "label": "РћС‚РѕРїРёС‚РµР»СЊРЅС‹Р№ РєРѕРЅС‚СѓСЂ", "weight": 0.10},
            ],
        },
        "consequence_severity": {
            "label": "РўСЏР¶РµСЃС‚СЊ РїРѕСЃР»РµРґСЃС‚РІРёР№",
            "description": "РќР°СЃРєРѕР»СЊРєРѕ С‚СЏР¶С‘Р»С‹РјРё Р±С‹Р»Рё РїРѕСЃР»РµРґСЃС‚РІРёСЏ РїРѕР¶Р°СЂРѕРІ РЅР° С‚РµСЂСЂРёС‚РѕСЂРёРё Рё РєР°РєРѕРІ РїСЂРѕС„РёР»СЊ СѓСЏР·РІРёРјРѕСЃС‚Рё.",
            "signals": [
                {"key": "severe_rate", "label": "РўСЏР¶С‘Р»С‹Рµ РїРѕСЃР»РµРґСЃС‚РІРёСЏ", "weight": 0.34},
                {"key": "casualty_pressure", "label": "РџРѕСЃС‚СЂР°РґР°РІС€РёРµ Рё РїРѕРіРёР±С€РёРµ", "weight": 0.24},
                {"key": "damage_pressure", "label": "РЈС‰РµСЂР± Рё СѓРЅРёС‡С‚РѕР¶РµРЅРёРµ", "weight": 0.20},
                {"key": "risk_category_factor", "label": "РљР°С‚РµРіРѕСЂРёСЏ СЂРёСЃРєР°", "weight": 0.14},
                {"key": "heating_pressure", "label": "РћС‚РѕРїРёС‚РµР»СЊРЅС‹Р№ РєРѕРЅС‚СѓСЂ", "weight": 0.08},
            ],
        },
        "long_arrival_risk": {
            "label": "Р РёСЃРє РґРѕР»РіРѕРіРѕ РїСЂРёР±С‹С‚РёСЏ",
            "description": "Р›РѕРіРёСЃС‚РёС‡РµСЃРєРёР№ СЂРёСЃРє: С„Р°РєС‚РёС‡РµСЃРєРѕРµ РїСЂРёР±С‹С‚РёРµ, explainable travel-time, РїРѕРєСЂС‹С‚РёРµ РџР§ Рё СЃРµСЂРІРёСЃРЅР°СЏ Р·РѕРЅР° С‚РµСЂСЂРёС‚РѕСЂРёРё.",
            "signals": [
                {"key": "long_arrival_rate", "label": "Р”РѕР»СЏ РґРѕР»РіРёС… РїСЂРёР±С‹С‚РёР№", "weight": 0.24},
                {"key": "avg_response_pressure", "label": "РЎСЂРµРґРЅРµРµ РІСЂРµРјСЏ РїСЂРёР±С‹С‚РёСЏ", "weight": 0.18},
                {"key": "travel_time_pressure", "label": "Travel-time РґРѕРµР·РґР°", "weight": 0.22},
                {"key": "service_coverage_gap", "label": "Р”РµС„РёС†РёС‚ РїРѕРєСЂС‹С‚РёСЏ РџР§", "weight": 0.20},
                {"key": "service_zone_pressure", "label": "РЎРµСЂРІРёСЃРЅР°СЏ Р·РѕРЅР°", "weight": 0.10},
                {"key": "distance_pressure", "label": "РЈРґР°Р»С‘РЅРЅРѕСЃС‚СЊ РґРѕ РџР§", "weight": 0.06},
            ],
        },
        "water_supply_deficit": {
            "label": "Р”РµС„РёС†РёС‚ РІРѕРґРѕСЃРЅР°Р±Р¶РµРЅРёСЏ",
            "description": "Р РёСЃРє РЅРµРґРѕСЃС‚Р°С‚РєР° РЅР°СЂСѓР¶РЅРѕРіРѕ РІРѕРґРѕСЃРЅР°Р±Р¶РµРЅРёСЏ Рё Р·Р°РІРёСЃРёРјРѕСЃС‚Рё РѕС‚ РїРѕРґРІРѕР·Р° РІРѕРґС‹ РЅР° СѓРґР°Р»С‘РЅРЅРѕР№ С‚РµСЂСЂРёС‚РѕСЂРёРё.",
            "signals": [
                {"key": "water_gap_rate", "label": "РџРѕРґС‚РІРµСЂР¶РґРµРЅРЅС‹Р№ РґРµС„РёС†РёС‚ РІРѕРґС‹", "weight": 0.62},
                {"key": "tanker_dependency", "label": "Р—Р°РІРёСЃРёРјРѕСЃС‚СЊ РѕС‚ РїРѕРґРІРѕР·Р°", "weight": 0.18},
                {"key": "rural_context", "label": "РЎРµР»СЊСЃРєРёР№ РєРѕРЅС‚РµРєСЃС‚", "weight": 0.12},
                {"key": "damage_pressure", "label": "РСЃС‚РѕСЂРёСЏ СѓС‰РµСЂР±Р°", "weight": 0.08},
            ],
        },
    },
    "thresholds": {
        "risk_class": {"high": 67.0, "medium": 43.0},
        "priority": {"immediate": 70.0, "targeted": 45.0},
        "component": {"high": 65.0, "medium": 40.0},
    },
    "defaults": {
        "water_gap_unknown": 0.38,
        "distance_km_baseline": 12.0,
        "distance_pressure_unknown": 0.30,
        "response_pressure_unknown": 0.42,
    },
    "notes": [
        "Р”Р»СЏ СЃРµР»СЊСЃРєРёС… С‚РµСЂСЂРёС‚РѕСЂРёР№ РІРµСЃ Р»РѕРіРёСЃС‚РёРєРё Рё РІРѕРґРѕСЃРЅР°Р±Р¶РµРЅРёСЏ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё РїРѕРІС‹С€Р°РµС‚СЃСЏ.",
        "Р’РµСЃР° РєРѕРјРїРѕРЅРµРЅС‚РѕРІ Рё СЃРёРіРЅР°Р»РѕРІ РІС‹РЅРµСЃРµРЅС‹ РІ РѕС‚РґРµР»СЊРЅСѓСЋ СЃС‚СЂСѓРєС‚СѓСЂСѓ Рё РјРѕРіСѓС‚ РјРµРЅСЏС‚СЊСЃСЏ Р±РµР· РїРµСЂРµРїРёСЃС‹РІР°РЅРёСЏ С„РѕСЂРјСѓР»С‹.",
        "РС‚РѕРіРѕРІС‹Р№ Р±Р°Р»Р» РёРЅС‚РµСЂРїСЂРµС‚РёСЂСѓРµС‚СЃСЏ РєР°Рє СѓРїСЂР°РІР»РµРЅС‡РµСЃРєРёР№ РїСЂРёРѕСЂРёС‚РµС‚ С‚РµСЂСЂРёС‚РѕСЂРёРё, Р° РЅРµ РєР°Рє РїСЂСЏРјРѕР№ РїСЂРѕРіРЅРѕР· С‡РёСЃР»Р° РїРѕР¶Р°СЂРѕРІ.",
    ],
    "calibration": {
        "ready": False,
        "targets": [
            "top1_hit_rate",
            "top3_capture_rate",
            "precision_at_3",
            "ndcg_at_3",
        ],
        "notes": [
            "Р­РєСЃРїРµСЂС‚РЅС‹Р№ РїСЂРѕС„РёР»СЊ РѕСЃС‚Р°РµС‚СЃСЏ СЂРµР·РµСЂРІРЅС‹Рј СЂРµР¶РёРјРѕРј, РµСЃР»Рё РёСЃС‚РѕСЂРёРё РЅРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР»СЏ СѓСЃС‚РѕР№С‡РёРІРѕР№ РєР°Р»РёР±СЂРѕРІРєРё.",
        ],
    },
}

ADAPTIVE_RISK_WEIGHT_PROFILE: RiskProfile = deepcopy(EXPERT_RISK_WEIGHT_PROFILE)
ADAPTIVE_RISK_WEIGHT_PROFILE.update(
    {
        "mode": "adaptive",
        "mode_label": "РђРґР°РїС‚РёРІРЅС‹Рµ РІРµСЃР°",
        "status_label": "РћР¶РёРґР°РµС‚ РєР°Р»РёР±СЂРѕРІРєСѓ",
        "status_tone": "sand",
        "description": (
            "РЎРµСЂРІРёСЃ СЃС‚Р°СЂР°РµС‚СЃСЏ РїРѕРґРѕР±СЂР°С‚СЊ РІРµСЃР° РєРѕРјРїРѕРЅРµРЅС‚РѕРІ РїРѕ РёСЃС‚РѕСЂРёС‡РµСЃРєРёРј РѕРєРЅР°Рј ranking-РєР°С‡РµСЃС‚РІР°, Р° РµСЃР»Рё РґР°РЅРЅС‹С… РјР°Р»Рѕ, "
            "Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё РІРѕР·РІСЂР°С‰Р°РµС‚СЃСЏ Рє СЌРєСЃРїРµСЂС‚РЅРѕРјСѓ РїСЂРѕС„РёР»СЋ Р±РµР· РїРѕС‚РµСЂРё РѕР±СЉСЏСЃРЅРёРјРѕСЃС‚Рё."
        ),
        "notes": [
            "РџРѕРґСЃС‚СЂР°РёРІР°СЋС‚СЃСЏ С‚РѕР»СЊРєРѕ РІРµСЃР° С‡РµС‚С‹СЂРµС… РїРѕРЅСЏС‚РЅС‹С… РєРѕРјРїРѕРЅРµРЅС‚РѕРІ, Р° СЃР°РјРё СЃРёРіРЅР°Р»С‹ РІРЅСѓС‚СЂРё РєРѕРјРїРѕРЅРµРЅС‚РѕРІ РѕСЃС‚Р°СЋС‚СЃСЏ РїСЂРѕР·СЂР°С‡РЅС‹РјРё Рё РґРѕСЃС‚СѓРїРЅС‹ РґР»СЏ СЂР°СЃС€РёС„СЂРѕРІРєРё.",
            "Р•СЃР»Рё РёСЃС‚РѕСЂРёС‡РµСЃРєР°СЏ РїСЂРѕРІРµСЂРєР° РЅРµ РґР°РµС‚ СѓСЃС‚РѕР№С‡РёРІРѕРіРѕ РІС‹РёРіСЂС‹С€Р°, СЃРµСЂРІРёСЃ СѓРґРµСЂР¶РёРІР°РµС‚ СЌРєСЃРїРµСЂС‚РЅС‹Рµ РІРµСЃР° РєР°Рє fallback.",
        ],
        "calibration": {
            "ready": True,
            "targets": [
                "top1_hit_rate",
                "top3_capture_rate",
                "precision_at_3",
                "ndcg_at_3",
            ],
            "notes": [
                "РљР°Р»РёР±СЂРѕРІРєР° РёРґРµС‚ РїРѕ РёСЃС‚РѕСЂРёС‡РµСЃРєРёРј РѕРєРЅР°Рј Р±РµР· РёСЃРїРѕР»СЊР·РѕРІР°РЅРёСЏ Р±СѓРґСѓС‰РёС… РЅР°Р±Р»СЋРґРµРЅРёР№ РІРЅСѓС‚СЂРё РєР°Р¶РґРѕРіРѕ РѕРєРЅР°.",
                "РџРѕРґР±РѕСЂ РѕРїС‚РёРјРёР·РёСЂСѓРµС‚ ranking-РјРµС‚СЂРёРєРё, Р° РЅРµ СЃРєСЂС‹С‚СѓСЋ С‡РµСЂРЅСѓСЋ РєРѕСЂРѕР±РєСѓ РїРѕ С‚РµСЂСЂРёС‚РѕСЂРёСЏРј.",
            ],
        },
    }
)

CALIBRATABLE_RISK_WEIGHT_PROFILE: RiskProfile = deepcopy(ADAPTIVE_RISK_WEIGHT_PROFILE)
CALIBRATABLE_RISK_WEIGHT_PROFILE.update(
    {
        "mode": "calibratable",
        "mode_label": "РЁР°Р±Р»РѕРЅ РґР»СЏ РЅР°СЃС‚СЂРѕР№РєРё",
        "status_label": "Р СѓС‡РЅР°СЏ РЅР°СЃС‚СЂРѕР№РєР°",
        "status_tone": "sky",
        "description": (
            "Р РµР¶РёРј СЃ С‚РѕР№ Р¶Рµ РїСЂРѕР·СЂР°С‡РЅРѕР№ СЃС‚СЂСѓРєС‚СѓСЂРѕР№ РєРѕРјРїРѕРЅРµРЅС‚РѕРІ, РїСЂРµРґРЅР°Р·РЅР°С‡РµРЅРЅС‹Р№ РґР»СЏ СЂСѓС‡РЅРѕР№ РЅР°СЃС‚СЂРѕР№РєРё РёР»Рё СЌРєСЃРїРµСЂРёРјРµРЅС‚Р°Р»СЊРЅРѕРіРѕ "
            "РїРѕРґР±РѕСЂР° РІРµСЃРѕРІ РїРѕ РёСЃС‚РѕСЂРёС‡РµСЃРєРёРј РѕРєРЅР°Рј."
        ),
        "notes": [
            "РСЃРїРѕР»СЊР·СѓРµС‚ С‚Сѓ Р¶Рµ РєРѕРјРїРѕРЅРµРЅС‚РЅСѓСЋ С„РѕСЂРјСѓР»Сѓ, С‡С‚Рѕ Рё СЌРєСЃРїРµСЂС‚РЅС‹Р№ РїСЂРѕС„РёР»СЊ.",
            "РџРѕРґС…РѕРґРёС‚ РґР»СЏ СЂСѓС‡РЅРѕРіРѕ СЃСЂР°РІРЅРµРЅРёСЏ Р°Р»СЊС‚РµСЂРЅР°С‚РёРІРЅС‹С… РЅР°Р±РѕСЂРѕРІ РІРµСЃРѕРІ Р±РµР· СЃРјРµРЅС‹ РёРЅС‚РµСЂС„РµР№СЃР°.",
        ],
    }
)

RISK_WEIGHT_PROFILES: dict[str, RiskProfile] = {
    DEFAULT_RISK_WEIGHT_MODE: ADAPTIVE_RISK_WEIGHT_PROFILE,
    "expert": EXPERT_RISK_WEIGHT_PROFILE,
    "calibratable": CALIBRATABLE_RISK_WEIGHT_PROFILE,
}



def get_risk_weight_profile(mode: str = DEFAULT_RISK_WEIGHT_MODE) -> RiskProfile:
    resolved_mode = mode if mode in RISK_WEIGHT_PROFILES else DEFAULT_RISK_WEIGHT_MODE
    return deepcopy(RISK_WEIGHT_PROFILES[resolved_mode])



def resolve_component_weights(profile: RiskProfile, is_rural: bool) -> List[ComponentWeightRow]:
    base_weights = {key: float(value) for key, value in (profile.get("component_weights") or {}).items()}
    adjusted_weights = dict(base_weights)
    if is_rural:
        for key, shift in (profile.get("rural_weight_shift") or {}).items():
            adjusted_weights[key] = max(0.0, adjusted_weights.get(key, 0.0) + float(shift))
    total_weight = sum(adjusted_weights.values()) or 1.0

    rows: List[ComponentWeightRow] = []
    for key in profile.get("component_order", []):
        spec = (profile.get("components") or {}).get(key, {})
        weight = adjusted_weights.get(key, 0.0) / total_weight
        base_weight = base_weights.get(key, 0.0)
        rows.append(
            {
                "key": key,
                "label": spec.get("label") or key,
                "description": spec.get("description") or "",
                "weight": weight,
                "weight_display": _format_weight(weight),
                "base_weight": base_weight,
                "base_weight_display": _format_weight(base_weight),
                "rural_shift": weight - base_weight,
                "rural_shift_display": _format_shift(weight - base_weight),
            }
        )
    return rows



def build_weight_profile_snapshot(profile: RiskProfile) -> dict[str, Any]:  # one-off
    base_components = resolve_component_weights(profile, is_rural=False)
    rural_components = {item["key"]: item for item in resolve_component_weights(profile, is_rural=True)}
    expert_component_weights = {
        key: float(value)
        for key, value in (profile.get("expert_component_weights") or profile.get("component_weights") or {}).items()
    }

    components: List[dict[str, Any]] = []
    for item in base_components:
        rural_item = rural_components.get(item["key"], item)
        expert_weight = float(expert_component_weights.get(item["key"], item["weight"]))
        calibration_shift = item["weight"] - expert_weight
        components.append(
            {
                **item,
                "expert_weight": expert_weight,
                "expert_weight_display": _format_weight(expert_weight),
                "current_weight_display": item["weight_display"],
                "calibration_shift": calibration_shift,
                "calibration_shift_display": _format_shift(calibration_shift),
                "rural_weight": rural_item.get("weight", item["weight"]),
                "rural_weight_display": rural_item.get("weight_display", item["weight_display"]),
            }
        )

    available_modes: List[Dict[str, str]] = []
    for key, value in RISK_WEIGHT_PROFILES.items():
        available_modes.append(
            {
                "mode": key,
                "label": value.get("mode_label") or key,
                "status_label": "РђРєС‚РёРІРµРЅ" if key == profile.get("mode") else value.get("status_label") or "Р”РѕСЃС‚СѓРїРµРЅ",
                "status_tone": value.get("status_tone") or ("forest" if key == profile.get("mode") else "sky"),
                "description": value.get("description") or "",
            }
        )

    calibration = profile.get("calibration") or {}
    selected_metrics = calibration.get("selected_metrics") or {}
    expert_metrics = calibration.get("expert_metrics") or {}
    comparison = calibration.get("comparison") or {}
    calibration_notes = list(calibration.get("notes") or [])
    summary = str(calibration.get("summary") or "").strip()
    if summary and summary not in calibration_notes:
        calibration_notes.insert(0, summary)
    comparison_summary = str(comparison.get("summary") or "").strip()
    if comparison_summary and comparison_summary not in calibration_notes:
        calibration_notes.insert(1 if calibration_notes else 0, comparison_summary)

    metric_cards: List[Dict[str, str]] = []
    k_value = int(selected_metrics.get("k_value") or 3)
    if selected_metrics:
        metric_cards.append(
            {
                "label": "Top-1 hit",
                "value": _format_probability(float(selected_metrics.get("top1_hit_rate") or 0.0)),
                "meta": "Р”РѕР»СЏ РѕРєРѕРЅ, РіРґРµ С‚РµСЂСЂРёС‚РѕСЂРёСЏ-Р»РёРґРµСЂ РґРµР№СЃС‚РІРёС‚РµР»СЊРЅРѕ РіРѕСЂРµР»Р° РІ СЃР»РµРґСѓСЋС‰РµРј РёРЅС‚РµСЂРІР°Р»Рµ.",
            }
        )
        metric_cards.append(
            {
                "label": f"Top-{k_value} capture",
                "value": _format_probability(float(selected_metrics.get("topk_capture_rate") or 0.0)),
                "meta": "Р”РѕР»СЏ Р±СѓРґСѓС‰РёС… РїРѕР¶Р°СЂРѕРІ, РїРѕРїР°РІС€РёС… РІ РІРµСЂС…РЅРёРµ С‚РµСЂСЂРёС‚РѕСЂРёРё СЂРµР№С‚РёРЅРіР°.",
            }
        )
        metric_cards.append(
            {
                "label": f"Precision@{k_value}",
                "value": _format_probability(float(selected_metrics.get("precision_at_k") or 0.0)),
                "meta": (
                    f"Р­РєСЃРїРµСЂС‚РЅС‹Р№ РїСЂРѕС„РёР»СЊ: {_format_probability(float(expert_metrics.get('precision_at_k') or 0.0))}; О” { _format_shift_probability(float(comparison.get('precision_at_k_delta') or 0.0)) }"
                    if expert_metrics
                    else "РЎРєРѕР»СЊРєРѕ С‚РµСЂСЂРёС‚РѕСЂРёР№ РІ top-k РґРµР№СЃС‚РІРёС‚РµР»СЊРЅРѕ РїРѕРґС‚РІРµСЂРґРёР»РёСЃСЊ РїРѕР¶Р°СЂРѕРј."
                ),
            }
        )
        metric_cards.append(
            {
                "label": f"NDCG@{k_value}",
                "value": _format_decimal(float(selected_metrics.get("ndcg_at_k") or 0.0)),
                "meta": (
                    f"Р­РєСЃРїРµСЂС‚РЅС‹Р№ РїСЂРѕС„РёР»СЊ: {_format_decimal(float(expert_metrics.get('ndcg_at_k') or 0.0))}; О” { _format_signed_decimal(float(comparison.get('ndcg_at_k_delta') or 0.0)) }"
                    if expert_metrics
                    else "РЎСЂР°РІРЅРµРЅРёРµ СЃ СЌРєСЃРїРµСЂС‚РЅС‹Рј РїСЂРѕС„РёР»РµРј РЅРµРґРѕСЃС‚СѓРїРЅРѕ."
                ),
            }
        )

    return {
        "mode": profile.get("mode") or DEFAULT_RISK_WEIGHT_MODE,
        "mode_label": profile.get("mode_label") or "РђРґР°РїС‚РёРІРЅС‹Рµ РІРµСЃР°",
        "status_label": profile.get("status_label") or "РђРєС‚РёРІРЅС‹Р№ РїСЂРѕС„РёР»СЊ",
        "status_tone": profile.get("status_tone") or "forest",
        "description": profile.get("description") or "",
        "components": components,
        "available_modes": available_modes,
        "notes": list(profile.get("notes") or []),
        "calibration_ready": bool(calibration.get("ready")),
        "calibration_targets": list(calibration.get("targets") or []),
        "calibration_notes": calibration_notes,
        "calibration_summary": summary,
        "calibration_windows_display": _format_integer(calibration.get("windows_used") or 0),
        "calibration_candidate_count_display": _format_integer(calibration.get("candidate_count") or 0),
        "uses_fallback": bool(calibration.get("used_fallback")),
        "calibration_comparison": comparison,
        "metric_cards": metric_cards,
    }



def _format_weight(value: float) -> str:
    percent = round(float(value) * 100.0)
    return f"{int(percent)}%"



def _format_shift(value: float) -> str:
    points = round(float(value) * 100.0)
    sign = "+" if points > 0 else ""
    return f"{sign}{int(points)} Рї.Рї."



def _format_probability(value: float) -> str:
    rounded = round(float(value) * 100.0, 1)
    if abs(rounded - round(rounded)) < 1e-9:
        return f"{int(round(rounded))}%"
    return f"{str(rounded).replace('.', ',')}%"



def _format_shift_probability(value: float) -> str:
    numeric = round(float(value) * 100.0, 1)
    sign = "+" if numeric > 0 else ""
    if abs(numeric - round(numeric)) < 1e-9:
        return f"{sign}{int(round(numeric))} Рї.Рї."
    return f"{sign}{str(numeric).replace('.', ',')} Рї.Рї."


def _format_signed_decimal(value: float) -> str:
    numeric = round(float(value), 3)
    sign = "+" if numeric > 0 else ""
    if abs(numeric - round(numeric)) < 1e-9:
        return f"{sign}{int(round(numeric))}"
    return sign + str(numeric).replace('.', ',')
