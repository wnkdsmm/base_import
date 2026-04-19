from __future__ import annotations

from typing import Any

from .types import (
    ClusterCountGuidance,
    ClusterCountGuidanceContext,
    ClusterCountRecommendationMessages,
    QualityDiagnostics,
)
from .utils import _format_integer

__all__ = [
    "_build_cluster_count_guidance",
]


def _build_cluster_count_guidance_context(
    requested_cluster_count: int,
    current_cluster_count: int,
    diagnostics: QualityDiagnostics | None = None,
    adjusted_requested_cluster_count: int | None = None,
    cluster_count_is_explicit: bool = False,
) -> ClusterCountGuidanceContext:
    raw_recommended_k = (diagnostics or {}).get("best_quality_k")
    best_silhouette_k = (diagnostics or {}).get("best_silhouette_k")
    best_gap_k = (diagnostics or {}).get("best_gap_k")
    requested_cluster_count = int(requested_cluster_count)
    adjusted_requested_cluster_count = int(
        adjusted_requested_cluster_count if adjusted_requested_cluster_count is not None else requested_cluster_count
    )
    current_cluster_count = int(current_cluster_count)
    request_adjusted = requested_cluster_count != adjusted_requested_cluster_count
    recommendation_gap = bool(raw_recommended_k) and int(raw_recommended_k) != current_cluster_count
    has_recommended_k = bool(raw_recommended_k)
    auto_switched_to_recommended = (
        not cluster_count_is_explicit and adjusted_requested_cluster_count != current_cluster_count
    )
    return {
        "recommended_k": int(raw_recommended_k) if has_recommended_k else raw_recommended_k,
        "best_silhouette_k": best_silhouette_k,
        "best_gap_k": best_gap_k,
        "requested_cluster_count": requested_cluster_count,
        "adjusted_requested_cluster_count": adjusted_requested_cluster_count,
        "current_cluster_count": current_cluster_count,
        "request_adjusted": request_adjusted,
        "recommendation_gap": recommendation_gap,
        "has_recommended_k": has_recommended_k,
        "auto_switched_to_recommended": auto_switched_to_recommended,
    }


def _cluster_count_suggested_label(cluster_count_is_explicit: bool) -> str:
    return "РђРІС‚РѕРјР°С‚РёС‡РµСЃРєРёР№ РІС‹Р±РѕСЂ" if not cluster_count_is_explicit else "Р РµРєРѕРјРµРЅРґСѓРµРјРѕРµ Р·РЅР°С‡РµРЅРёРµ"


def _initial_cluster_count_recommendation_messages(
    current_cluster_count: int,
    *,
    cluster_count_is_explicit: bool,
) -> ClusterCountRecommendationMessages:
    return {
        "suggested_label": _cluster_count_suggested_label(cluster_count_is_explicit),
        "suggested_note": "Р РµРєРѕРјРµРЅРґР°С†РёСЏ РїРѕ С‡РёСЃР»Сѓ РіСЂСѓРїРї РїРѕСЏРІРёС‚СЃСЏ, РєРѕРіРґР° С…РІР°С‚РёС‚ РґР°РЅРЅС‹С… РґР»СЏ СЃСЂР°РІРЅРµРЅРёСЏ РЅРµСЃРєРѕР»СЊРєРёС… РІР°СЂРёР°РЅС‚РѕРІ.",
        "current_note": f"РЎРµР№С‡Р°СЃ СЃС‚СЂР°РЅРёС†Р° РїРѕРєР°Р·С‹РІР°РµС‚ {current_cluster_count} РіСЂСѓРїРїС‹.",
        "quality_note": "",
        "notes_message": "",
        "model_note": "",
    }


def _recommended_cluster_count_messages(
    context: ClusterCountGuidanceContext,
    *,
    cluster_count_is_explicit: bool,
) -> ClusterCountRecommendationMessages:
    recommended_k = context["recommended_k"]
    best_gap_k = context["best_gap_k"]
    current_cluster_count = context["current_cluster_count"]
    adjusted_requested_cluster_count = context["adjusted_requested_cluster_count"]
    recommendation_gap = context["recommendation_gap"]
    auto_switched_to_recommended = context["auto_switched_to_recommended"]

    def _append_gap_note(note: str) -> str:
        if best_gap_k is None:
            return note
        if best_gap_k == recommended_k:
            gap_note = f"Gap statistic РїРѕРґС‚РІРµСЂР¶РґР°РµС‚ СЂРµРєРѕРјРµРЅРґР°С†РёСЋ: k={recommended_k}."
        else:
            gap_note = (
                f"РџРѕ РєСЂРёС‚РµСЂРёСЋ Gap statistic РѕРїС‚РёРјР°Р»СЊРЅРѕ k={best_gap_k}, "
                f"РїРѕ СЃРѕРІРѕРєСѓРїРЅРѕРјСѓ РєР°С‡РµСЃС‚РІСѓ вЂ” k={recommended_k}."
            )
        return gap_note if not note else f"{note} {gap_note}".strip()

    if cluster_count_is_explicit and recommendation_gap:
        quality_note = (
            f"Р’С‹Р±СЂР°РЅРЅРѕРµ РІСЂСѓС‡РЅСѓСЋ С‡РёСЃР»Рѕ РіСЂСѓРїРї ({_format_integer(current_cluster_count)}) РЅРµ СЃРѕРІРїР°РґР°РµС‚ СЃ СЂРµРєРѕРјРµРЅРґР°С†РёРµР№; "
            f"РїРѕ СЃРѕРІРѕРєСѓРїРЅРѕСЃС‚Рё РјРµС‚СЂРёРє Р»СѓС‡С€Рµ РІС‹РіР»СЏРґРёС‚ k={_format_integer(recommended_k)}."
        )
        return {
            "suggested_note": (
                f"Р”РёР°РіРЅРѕСЃС‚РёРєР° СЂРµРєРѕРјРµРЅРґСѓРµС‚ {recommended_k} РіСЂСѓРїРїС‹, "
                f"РЅРѕ СЃРµР№С‡Р°СЃ СЃРѕС…СЂР°РЅРµРЅРѕ РІС‹Р±СЂР°РЅРЅРѕРµ РІСЂСѓС‡РЅСѓСЋ Р·РЅР°С‡РµРЅРёРµ: {current_cluster_count}."
            ),
            "current_note": (
                f"РЎРµР№С‡Р°СЃ РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РІС‹Р±СЂР°РЅРЅРѕРµ РІСЂСѓС‡РЅСѓСЋ Р·РЅР°С‡РµРЅРёРµ: {current_cluster_count} РіСЂСѓРїРїС‹; "
                f"РґРёР°РіРЅРѕСЃС‚РёРєР° СЃРѕРІРµС‚СѓРµС‚ {recommended_k}."
            ),
            "quality_note": quality_note,
            "model_note": (
                f"Р§РёСЃР»Рѕ РіСЂСѓРїРї Р·Р°С„РёРєСЃРёСЂРѕРІР°РЅРѕ РІСЂСѓС‡РЅСѓСЋ РЅР° СѓСЂРѕРІРЅРµ {current_cluster_count}, "
                "РїРѕСЌС‚РѕРјСѓ СЃС‚СЂР°РЅРёС†Р° РЅРµ РїРµСЂРµРєР»СЋС‡Р°РµС‚ РµРіРѕ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё."
            ),
            "notes_message": quality_note,
        }

    if cluster_count_is_explicit:
        quality_note = f"Р’С‹Р±СЂР°РЅРЅРѕРµ РІСЂСѓС‡РЅСѓСЋ С‡РёСЃР»Рѕ РіСЂСѓРїРї ({_format_integer(current_cluster_count)}) СЃРѕРІРїР°РґР°РµС‚ СЃ СЂРµРєРѕРјРµРЅРґР°С†РёРµР№."
        return {
            "suggested_note": f"Р”РёР°РіРЅРѕСЃС‚РёРєР° РїРѕРґС‚РІРµСЂР¶РґР°РµС‚ РІС‹Р±СЂР°РЅРЅРѕРµ РІСЂСѓС‡РЅСѓСЋ Р·РЅР°С‡РµРЅРёРµ: {current_cluster_count} РіСЂСѓРїРїС‹.",
            "current_note": f"РЎРµР№С‡Р°СЃ РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РІС‹Р±СЂР°РЅРЅРѕРµ РІСЂСѓС‡РЅСѓСЋ Р·РЅР°С‡РµРЅРёРµ: {current_cluster_count} РіСЂСѓРїРїС‹, Рё РѕРЅРѕ СЃРѕРІРїР°РґР°РµС‚ СЃ СЂРµРєРѕРјРµРЅРґР°С†РёРµР№.",
            "quality_note": quality_note,
            "model_note": f"Р§РёСЃР»Рѕ РіСЂСѓРїРї Р·Р°РґР°РЅРѕ РІСЂСѓС‡РЅСѓСЋ: {current_cluster_count}. Р­С‚Рѕ Р¶Рµ Р·РЅР°С‡РµРЅРёРµ СЂРµРєРѕРјРµРЅРґСѓРµС‚ РґРёР°РіРЅРѕСЃС‚РёРєР°.",
            "notes_message": quality_note,
        }

    suggested_note = f"РђРІС‚РѕРјР°С‚РёС‡РµСЃРєРёР№ РІС‹Р±РѕСЂ РёСЃРїРѕР»СЊР·СѓРµС‚ {current_cluster_count} РіСЂСѓРїРїС‹ РєР°Рє Р»СѓС‡С€РёР№ РІР°СЂРёР°РЅС‚ РїРѕ СЃРѕРІРѕРєСѓРїРЅРѕСЃС‚Рё РјРµС‚СЂРёРє."
    if auto_switched_to_recommended:
        quality_note = (
            "РЎС‚СЂР°РЅРёС†Р° Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё РїРѕРґСЃС‚СЂРѕРёР»Р° С‡РёСЃР»Рѕ РіСЂСѓРїРї РїРѕРґ СЂРµРєРѕРјРµРЅРґР°С†РёСЋ РґРёР°РіРЅРѕСЃС‚РёРєРё: "
            f"РІРјРµСЃС‚Рѕ СЃС‚Р°СЂС‚РѕРІРѕРіРѕ k={_format_integer(adjusted_requested_cluster_count)} "
            f"РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ k={_format_integer(current_cluster_count)}."
        )
        quality_note = _append_gap_note(quality_note)
        model_note = (
            f"РџРѕ СѓРјРѕР»С‡Р°РЅРёСЋ СЃС‚СЂР°РЅРёС†Р° РїРѕРєР°Р·С‹РІР°РµС‚ СЂРµРєРѕРјРµРЅРґРѕРІР°РЅРЅРѕРµ С‡РёСЃР»Рѕ РіСЂСѓРїРї: {current_cluster_count} "
            f"РІРјРµСЃС‚Рѕ СЃС‚Р°СЂС‚РѕРІРѕРіРѕ {adjusted_requested_cluster_count}."
        )
        return {
            "suggested_note": suggested_note,
            "current_note": model_note,
            "quality_note": quality_note,
            "model_note": model_note,
            "notes_message": quality_note,
        }

    quality_note = (
        "РўРµРєСѓС‰РµРµ С‡РёСЃР»Рѕ РіСЂСѓРїРї СѓР¶Рµ СЃРѕРІРїР°РґР°РµС‚ СЃ СЂРµРєРѕРјРµРЅРґР°С†РёРµР№ РґРёР°РіРЅРѕСЃС‚РёРєРё: "
        f"k={_format_integer(current_cluster_count)}."
    )
    quality_note = _append_gap_note(quality_note)
    model_note = f"РџРѕ СѓРјРѕР»С‡Р°РЅРёСЋ СЃС‚СЂР°РЅРёС†Р° РїРѕРєР°Р·С‹РІР°РµС‚ СЂРµРєРѕРјРµРЅРґРѕРІР°РЅРЅРѕРµ С‡РёСЃР»Рѕ РіСЂСѓРїРї: {current_cluster_count}."
    return {
        "suggested_note": suggested_note,
        "current_note": model_note,
        "quality_note": quality_note,
        "model_note": model_note,
        "notes_message": quality_note,
    }


def _build_cluster_count_recommendation_context(
    context: ClusterCountGuidanceContext,
    *,
    cluster_count_is_explicit: bool,
) -> ClusterCountRecommendationMessages:
    current_cluster_count = context["current_cluster_count"]
    messages = _initial_cluster_count_recommendation_messages(
        current_cluster_count,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )

    if context["has_recommended_k"]:
        messages.update(
            _recommended_cluster_count_messages(
                context,
                cluster_count_is_explicit=cluster_count_is_explicit,
            )
        )

    return messages


def _apply_cluster_count_adjustment_warning(
    context: ClusterCountGuidanceContext,
    messages: ClusterCountRecommendationMessages,
) -> ClusterCountRecommendationMessages:
    if not context["request_adjusted"]:
        return messages

    adjusted = dict(messages)
    requested_cluster_count = context["requested_cluster_count"]
    adjusted_requested_cluster_count = context["adjusted_requested_cluster_count"]
    current_note = adjusted["current_note"]
    suggested_note = adjusted["suggested_note"]
    quality_note = adjusted["quality_note"]
    model_note = adjusted["model_note"]
    adjustment_note = (
        f"Р—Р°РїСЂРѕС€РµРЅРЅРѕРµ С‡РёСЃР»Рѕ РіСЂСѓРїРї ({_format_integer(requested_cluster_count)}) Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё СЃРєРѕСЂСЂРµРєС‚РёСЂРѕРІР°РЅРѕ РґРѕ "
        f"{_format_integer(adjusted_requested_cluster_count)} РёР·-Р·Р° РѕРіСЂР°РЅРёС‡РµРЅРёР№ С‚РµРєСѓС‰РµР№ РІС‹Р±РѕСЂРєРё."
    )
    adjusted["current_note"] = adjustment_note if not current_note else f"{adjustment_note} {current_note}".strip()
    adjusted["suggested_note"] = f"{adjustment_note} {suggested_note}".strip()
    adjusted["quality_note"] = adjustment_note if not quality_note else f"{adjustment_note} {quality_note}".strip()
    adjusted["model_note"] = adjustment_note if not model_note else f"{adjustment_note} {model_note}".strip()
    adjusted["notes_message"] = adjusted["quality_note"]
    return adjusted


def _build_cluster_count_guidance(
    requested_cluster_count: int,
    current_cluster_count: int,
    diagnostics: QualityDiagnostics | None = None,
    adjusted_requested_cluster_count: int | None = None,
    cluster_count_is_explicit: bool = False,
) -> ClusterCountGuidance:
    guidance_context = _build_cluster_count_guidance_context(
        requested_cluster_count=requested_cluster_count,
        current_cluster_count=current_cluster_count,
        diagnostics=diagnostics,
        adjusted_requested_cluster_count=adjusted_requested_cluster_count,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    recommendation_context = _build_cluster_count_recommendation_context(
        guidance_context,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    recommendation_context = _apply_cluster_count_adjustment_warning(guidance_context, recommendation_context)
    recommended_k = guidance_context["recommended_k"]
    best_silhouette_k = guidance_context["best_silhouette_k"]
    best_gap_k = guidance_context["best_gap_k"]
    recommendation_gap = guidance_context["recommendation_gap"]
    request_adjusted = guidance_context["request_adjusted"]
    return {
        "recommended_cluster_count": recommended_k,
        "best_silhouette_k": best_silhouette_k,
        "best_gap_k": best_gap_k,
        "has_recommendation_gap": recommendation_gap,
        "request_adjusted": request_adjusted,
        "suggested_label": recommendation_context["suggested_label"],
        "suggested_note": recommendation_context["suggested_note"],
        "current_note": recommendation_context["current_note"],
        "quality_note": recommendation_context["quality_note"],
        "notes_message": recommendation_context["notes_message"],
        "model_note": recommendation_context["model_note"],
    }
