from __future__ import annotations

from typing import Optional, Sequence

from .types import HistoricalValidationPayload, QualityPassport, RiskProfile, RiskScore, TopConfidence
from .utils import _clamp, _format_integer, _format_number, _format_probability


def _attach_ranking_reliability(
    territories: Sequence[RiskScore],
    quality_passport: QualityPassport,
    historical_validation: HistoricalValidationPayload,
) -> list[RiskScore]:
    if not territories:
        return []

    annotated = [dict(item) for item in territories]
    metrics = historical_validation.get("metrics_raw") or {}
    windows_count = int(metrics.get("windows_count") or 0)
    k_value = int(metrics.get("k_value") or 3)
    validation_ready = bool(historical_validation.get("has_metrics")) and windows_count >= 3
    passport_score = float(quality_passport.get("confidence_score") or 0.0) / 100.0
    objective_score = float(metrics.get("objective_score") or 0.0)
    topk_capture = float(metrics.get("topk_capture_rate") or 0.0)
    precision_at_k = float(metrics.get("precision_at_k") or 0.0)
    ndcg_at_k = float(metrics.get("ndcg_at_k") or 0.0)

    for index, territory in enumerate(annotated):
        history_support = min(1.0, float(territory.get("history_count") or 0.0) / 8.0)
        if index == 0:
            margin_support = min(1.0, float(territory.get("ranking_gap_to_next") or 0.0) / 8.0)
        else:
            margin_support = 1.0 - min(1.0, float(territory.get("ranking_gap_to_top") or 0.0) / 12.0)
        local_support = _clamp(0.58 * margin_support + 0.42 * history_support, 0.15, 1.0)

        if validation_ready:
            confidence_norm = _clamp(0.42 * passport_score + 0.38 * objective_score + 0.20 * local_support, 0.18, 0.96)
        else:
            confidence_norm = _clamp(0.67 * passport_score + 0.33 * local_support, 0.16, 0.88)

        if index == 0:
            confidence_norm = _clamp(confidence_norm + 0.03, 0.18, 0.96)
        elif index >= 3:
            confidence_norm = _clamp(confidence_norm - 0.04, 0.16, 0.92)

        confidence_score = int(round(confidence_norm * 100.0))
        label, tone, prefix = _ranking_confidence_state(confidence_score)

        if validation_ready:
            history_clause = (
                f"rolling-origin РїСЂРѕРІРµСЂРєР° РЅР° {_format_integer(windows_count)} РѕРєРЅР°С… РґР°С‘С‚ Top-{k_value} capture "
                f"{_format_probability(topk_capture)}, Precision@{k_value} {_format_probability(precision_at_k)} "
                f"Рё NDCG@{k_value} {_format_number(ndcg_at_k)}"
            )
        else:
            history_clause = (
                f"РїРѕР»РЅРѕР№ rolling-origin РїСЂРѕРІРµСЂРєРё РїРѕРєР° РЅРµС‚, РїРѕСЌС‚РѕРјСѓ РѕРїРѕСЂР° РёРґС‘С‚ РЅР° РїР°СЃРїРѕСЂС‚ РґР°РЅРЅС‹С… "
                f"{quality_passport.get('confidence_score_display') or '0 / 100'}"
            )

        margin_clause = (
            f"РѕС‚СЂС‹РІ РѕС‚ СЃР»РµРґСѓСЋС‰РµР№ С‚РµСЂСЂРёС‚РѕСЂРёРё {territory.get('ranking_gap_to_next_display') or '0 Р±Р°Р»Р»РѕРІ'}"
            if index == 0
            else f"РѕС‚СЃС‚Р°РІР°РЅРёРµ РѕС‚ Р»РёРґРµСЂР° {territory.get('ranking_gap_to_top_display') or '0 Р±Р°Р»Р»РѕРІ'}"
        )
        component_clause = territory.get("ranking_component_lead") or territory.get("drivers_display") or "РєРѕРјРїРѕРЅРµРЅС‚С‹ СЂРёСЃРєР° С‚РµСЂСЂРёС‚РѕСЂРёРё"
        territory.update(
            {
                "ranking_confidence_score": confidence_score,
                "ranking_confidence_display": f"{confidence_score} / 100",
                "ranking_confidence_label": label,
                "ranking_confidence_tone": tone,
                "ranking_confidence_note": f"{prefix}: {history_clause}; {margin_clause}; РѕСЃРЅРѕРІРЅРѕР№ РІРєР»Р°Рґ РґР°СЋС‚ {component_clause}.",
            }
        )

    return annotated


def _top_territory_confidence_payload(
    top_territory: Optional[RiskScore],
    quality_passport: QualityPassport,
) -> TopConfidence:
    if top_territory:
        return {
            "label": top_territory.get("ranking_confidence_label") or "РЈРјРµСЂРµРЅРЅР°СЏ",
            "score_display": top_territory.get("ranking_confidence_display") or quality_passport.get("confidence_score_display") or "0 / 100",
            "tone": top_territory.get("ranking_confidence_tone") or quality_passport.get("confidence_tone") or "fire",
            "note": top_territory.get("ranking_confidence_note") or quality_passport.get("validation_summary") or "РџРѕСЏСЃРЅРµРЅРёРµ РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р°.",
        }

    return {
        "label": quality_passport.get("confidence_label") or "РћРіСЂР°РЅРёС‡РµРЅРЅР°СЏ",
        "score_display": quality_passport.get("confidence_score_display") or "0 / 100",
        "tone": quality_passport.get("confidence_tone") or "fire",
        "note": quality_passport.get("validation_summary") or "РџРѕСЏСЃРЅРµРЅРёРµ РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р°.",
    }


def _ranking_confidence_state(score: int) -> tuple[str, str, str]:
    if score >= 82:
        return "Р’С‹СЃРѕРєР°СЏ", "forest", "Р’С‹РІРѕРґ РїРѕРґС‚РІРµСЂР¶РґР°РµС‚СЃСЏ СѓРІРµСЂРµРЅРЅРѕ"
    if score >= 64:
        return "Р Р°Р±РѕС‡Р°СЏ", "sky", "Р’С‹РІРѕРґ РїРѕРґС‚РІРµСЂР¶РґР°РµС‚СЃСЏ РЅР° СЂР°Р±РѕС‡РµРј СѓСЂРѕРІРЅРµ"
    if score >= 46:
        return "РЈРјРµСЂРµРЅРЅР°СЏ", "sand", "Р’С‹РІРѕРґ РїРѕР»РµР·РµРЅ РґР»СЏ РїСЂРёРѕСЂРёС‚РёР·Р°С†РёРё, РЅРѕ С‚СЂРµР±СѓРµС‚ Р»РѕРєР°Р»СЊРЅРѕР№ РїСЂРѕРІРµСЂРєРё"
    return "РћРіСЂР°РЅРёС‡РµРЅРЅР°СЏ", "fire", "Р’С‹РІРѕРґ СЃС‚РѕРёС‚ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ РєР°Рє СЃРёРіРЅР°Р» Рє РґРѕРїРѕР»РЅРёС‚РµР»СЊРЅРѕР№ РїСЂРѕРІРµСЂРєРµ"


# intentionally separate from access_points/presentation.py::_build_summary_cards and
# table_summary.py::_build_summary_cards:
# forecast-risk cards combine ranking reliability, calibration and quality signals.
def _build_summary_cards(
    territories: Sequence[RiskScore],
    weight_profile: RiskProfile,
    historical_validation: HistoricalValidationPayload,
    quality_passport: QualityPassport,
) -> list[dict[str, str]]:  # one-off
    if not territories:
        return []

    lead = territories[0]
    cards = [
        {
            "label": "РўРµСЂСЂРёС‚РѕСЂРёСЏ РїРµСЂРІРѕРіРѕ РїСЂРёРѕСЂРёС‚РµС‚Р°",
            "value": lead.get("label") or "-",
            "meta": lead.get("ranking_reason") or lead.get("drivers_display") or "РџРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РѕР±СЉСЏСЃРЅРµРЅРёРµ Р»РёРґРµСЂСЃС‚РІР°.",
            "tone": lead.get("priority_tone") or "sand",
        },
        {
            "label": "РќР°РґС‘Р¶РЅРѕСЃС‚СЊ РІС‹РІРѕРґР°",
            "value": lead.get("ranking_confidence_label") or "РЈРјРµСЂРµРЅРЅР°СЏ",
            "meta": lead.get("ranking_confidence_note") or "РџРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РѕС†РµРЅРєР° РЅР°РґС‘Р¶РЅРѕСЃС‚Рё РІС‹РІРѕРґР° РїРѕ СЂР°РЅР¶РёСЂРѕРІР°РЅРёСЋ.",
            "tone": lead.get("ranking_confidence_tone") or "fire",
        },
        {
            "label": "РџСЂРѕС„РёР»СЊ РІРµСЃРѕРІ",
            "value": weight_profile.get("status_label") or "РђРєС‚РёРІРЅС‹Р№ РїСЂРѕС„РёР»СЊ",
            "meta": weight_profile.get("mode_label") or "РђРґР°РїС‚РёРІРЅС‹Рµ РІРµСЃР°",
            "tone": weight_profile.get("status_tone") or "forest",
        },
        {
            "label": "РљР°С‡РµСЃС‚РІРѕ РґР°РЅРЅС‹С…",
            "value": quality_passport.get("confidence_label") or "РћРіСЂР°РЅРёС‡РµРЅРЅР°СЏ",
            "meta": quality_passport.get("validation_summary") or "РџР°СЃРїРѕСЂС‚ РєР°С‡РµСЃС‚РІР° РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р°.",
            "tone": quality_passport.get("confidence_tone") or "fire",
        },
    ]

    metrics = historical_validation.get("metrics_raw") or {}
    if historical_validation.get("has_metrics"):
        k_value = int(metrics.get("k_value") or 3)
        cards.extend(
            [
                {
                    "label": "Top-1 hit",
                    "value": _format_probability(float(metrics.get("top1_hit_rate") or 0.0)),
                    "meta": "РљР°Рє С‡Р°СЃС‚Рѕ РїРµСЂРІР°СЏ С‚РµСЂСЂРёС‚РѕСЂРёСЏ РґРµР№СЃС‚РІРёС‚РµР»СЊРЅРѕ РіРѕСЂРµР»Р° РІ СЃР»РµРґСѓСЋС‰РµРј РѕРєРЅРµ",
                    "tone": "sky",
                },
                {
                    "label": f"Top-{k_value} capture",
                    "value": _format_probability(float(metrics.get("topk_capture_rate") or 0.0)),
                    "meta": "РљР°РєР°СЏ РґРѕР»СЏ Р±СѓРґСѓС‰РёС… РїРѕР¶Р°СЂРѕРІ РїРѕРїР°РґР°Р»Р° РІ РІРµСЂС…РЅСЋСЋ С‡Р°СЃС‚СЊ СЂРµР№С‚РёРЅРіР°",
                    "tone": "forest",
                },
                {
                    "label": f"Precision@{k_value}",
                    "value": _format_probability(float(metrics.get("precision_at_k") or 0.0)),
                    "meta": "РљР°РєР°СЏ РґРѕР»СЏ С‚РµСЂСЂРёС‚РѕСЂРёР№ РІ РІРµСЂС…РЅРµР№ С‡Р°СЃС‚Рё СЂРµР№С‚РёРЅРіР° РґРµР№СЃС‚РІРёС‚РµР»СЊРЅРѕ РїРѕРґС‚РІРµСЂР¶РґР°Р»Р°СЃСЊ РїРѕР¶Р°СЂРѕРј",
                    "tone": "sky",
                },
                {
                    "label": f"NDCG@{k_value}",
                    "value": _format_number(float(metrics.get("ndcg_at_k") or 0.0)),
                    "meta": "РќР°СЃРєРѕР»СЊРєРѕ РїРѕСЂСЏРґРѕРє С‚РµСЂСЂРёС‚РѕСЂРёР№ СЃРѕРІРїР°РґР°Р» СЃ СЂРµР°Р»СЊРЅРѕР№ РєРѕРЅС†РµРЅС‚СЂР°С†РёРµР№ РїРѕР¶Р°СЂРѕРІ",
                    "tone": "sand",
                },
            ]
        )
    else:
        cards.append(
            {
                "label": "РСЃС‚РѕСЂРёС‡РµСЃРєР°СЏ РїСЂРѕРІРµСЂРєР°",
                "value": historical_validation.get("status_label") or "РџРѕРєР° Р±РµР· РїСЂРѕРІРµСЂРєРё",
                "meta": historical_validation.get("summary") or "РњРµС‚СЂРёРєРё РїРѕСЏРІСЏС‚СЃСЏ РїРѕСЃР»Рµ РЅР°РєРѕРїР»РµРЅРёСЏ РёСЃС‚РѕСЂРёРё.",
                "tone": historical_validation.get("status_tone") or "sand",
            }
        )

    return cards
