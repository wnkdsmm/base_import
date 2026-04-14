from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence


def _safe_text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def _normalize_tone(value: Any, fallback: str = "sky") -> str:
    tone = _safe_text(value, fallback)
    tone_map = {
        "high": "fire",
        "medium": "sand",
        "low": "sky",
    }
    return tone_map.get(tone, tone or fallback)


def _unique_notes(notes: Iterable[Any]) -> List[str]:
    result: List[str] = []
    for item in notes:
        text = _safe_text(item)
        if text and text not in result:
            result.append(text)
    return result


def empty_executive_brief() -> dict[str, Any]:
    return {
        "lead": "РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РєСЂР°С‚РєРёР№ С‚РµСЂСЂРёС‚РѕСЂРёР°Р»СЊРЅС‹Р№ РІС‹РІРѕРґ: РєР°РєР°СЏ С‚РµСЂСЂРёС‚РѕСЂРёСЏ РёРґРµС‚ РїРµСЂРІРѕР№ РІ РїСЂРёРѕСЂРёС‚РµС‚Рµ, С‡С‚Рѕ РїСЂРѕРІРµСЂРёС‚СЊ СЃРЅР°С‡Р°Р»Р° Рё РЅР°СЃРєРѕР»СЊРєРѕ РЅР°РґРµР¶РµРЅ СЌС‚РѕС‚ РІС‹РІРѕРґ.",
        "top_territory_label": "-",
        "priority_reason": "РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С…, С‡С‚РѕР±С‹ РІС‹РґРµР»РёС‚СЊ С‚РµСЂСЂРёС‚РѕСЂРёСЋ РїРµСЂРІРѕРіРѕ РїСЂРёРѕСЂРёС‚РµС‚Р°.",
        "priority_tone": "sky",
        "why_value": "РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С…",
        "why_meta": "РџСЂРёС‡РёРЅР° РїСЂРёРѕСЂРёС‚РµС‚Р° РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ РЅР°РєРѕРїР»РµРЅРёСЏ РґР°РЅРЅС‹С….",
        "action_label": "РџР»Р°РЅРѕРІРѕРµ РЅР°Р±Р»СЋРґРµРЅРёРµ",
        "action_detail": "Р”РµС‚Р°Р»РёР·Р°С†РёСЏ РґРµР№СЃС‚РІРёСЏ РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р°.",
        "confidence_label": "РћРіСЂР°РЅРёС‡РµРЅРЅР°СЏ",
        "confidence_score_display": "0 / 100",
        "confidence_tone": "fire",
        "confidence_summary": "РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РѕС†РµРЅРєР° РґРѕРІРµСЂРёСЏ Рє РґР°РЅРЅС‹Рј.",
        "cards": [
            {
                "label": "РљР°РєР°СЏ С‚РµСЂСЂРёС‚РѕСЂРёСЏ РїРµСЂРІР°СЏ",
                "value": "-",
                "meta": "РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РїСЂРёРѕСЂРёС‚РµС‚Р°.",
                "tone": "sky",
            },
            {
                "label": "РџРѕС‡РµРјСѓ РѕРЅР° РІС‹С€Рµ",
                "value": "РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С…",
                "meta": "РџСЂРёС‡РёРЅР° РїСЂРёРѕСЂРёС‚РµС‚Р° РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р°.",
                "tone": "sand",
            },
            {
                "label": "РќР°СЃРєРѕР»СЊРєРѕ РЅР°РґРµР¶РµРЅ РїСЂРёРѕСЂРёС‚РµС‚",
                "value": "РћРіСЂР°РЅРёС‡РµРЅРЅР°СЏ",
                "meta": "0 / 100. РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РѕС†РµРЅРєР° РґРѕРІРµСЂРёСЏ Рє РґР°РЅРЅС‹Рј.",
                "tone": "fire",
            },
            {
                "label": "Р§С‚Рѕ СЃРґРµР»Р°С‚СЊ РїРµСЂРІС‹Рј",
                "value": "РџР»Р°РЅРѕРІРѕРµ РЅР°Р±Р»СЋРґРµРЅРёРµ",
                "meta": "Р”РµС‚Р°Р»РёР·Р°С†РёСЏ РґРµР№СЃС‚РІРёСЏ РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р°.",
                "tone": "forest",
            },
        ],
        "territories": [],
        "notes": [],
        "export_title": "РљРѕСЂРѕС‚РєРѕ РґР»СЏ РїРµСЂРµРґР°С‡Рё РґР°Р»СЊС€Рµ",
        "export_excerpt": "РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РєРѕСЂРѕС‚РєР°СЏ СЃРїСЂР°РІРєР° РґР»СЏ СЂСѓРєРѕРІРѕРґРёС‚РµР»СЏ, СЃРјРµРЅС‹ РёР»Рё РґРµР¶СѓСЂРЅРѕРіРѕ.",
        "export_text": "",
    }


def build_executive_brief_from_risk_payload(
    risk_payload: Optional[dict[str, Any]],
    *,
    notes: Sequence[Any] | None = None,
) -> dict[str, Any]:
    if not risk_payload:
        return empty_executive_brief()

    territories = list(risk_payload.get("territories") or [])
    lead = territories[0] if territories else {}
    passport = risk_payload.get("quality_passport") or {}

    top_territory_label = _safe_text(
        lead.get("label") if lead else risk_payload.get("top_territory_label"),
        "-",
    )
    priority_reason = _safe_text(
        lead.get("ranking_reason") if lead else risk_payload.get("top_territory_explanation"),
        "РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С…, С‡С‚РѕР±С‹ РѕР±СЉСЏСЃРЅРёС‚СЊ С‚РµСЂСЂРёС‚РѕСЂРёСЋ РїРµСЂРІРѕРіРѕ РїСЂРёРѕСЂРёС‚РµС‚Р°.",
    )
    top_component = (lead.get("component_scores") or [{}])[0] if lead else {}
    why_value = _safe_text(top_component.get("label"), "РќРµС‚ РґРѕРјРёРЅРёСЂСѓСЋС‰РµРіРѕ С„Р°РєС‚РѕСЂР°")
    why_meta = _safe_text(
        lead.get("drivers_display") if lead else risk_payload.get("top_territory_explanation"),
        "РџСЂРёС‡РёРЅР° РїСЂРёРѕСЂРёС‚РµС‚Р° РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ РЅР°РєРѕРїР»РµРЅРёСЏ РґР°РЅРЅС‹С….",
    )
    action_label = _safe_text(
        lead.get("action_label"),
        "РџР»Р°РЅРѕРІРѕРµ РЅР°Р±Р»СЋРґРµРЅРёРµ",
    )
    action_detail = _safe_text(
        lead.get("action_hint") or ((lead.get("recommendations") or [{}])[0]).get("detail"),
        "РЎРЅР°С‡Р°Р»Р° РїСЂРѕРІРµСЂСЊС‚Рµ Р»РѕРєР°Р»СЊРЅСѓСЋ РѕР±СЃС‚Р°РЅРѕРІРєСѓ Рё РїРѕРґС‚РІРµСЂРґРёС‚Рµ РїСЂРёРѕСЂРёС‚РµС‚ РЅР° РјРµСЃС‚Рµ.",
    )
    confidence_label = _safe_text(
        risk_payload.get("top_territory_confidence_label") or lead.get("ranking_confidence_label") or passport.get("confidence_label"),
        "РћРіСЂР°РЅРёС‡РµРЅРЅР°СЏ",
    )
    confidence_score_display = _safe_text(
        risk_payload.get("top_territory_confidence_score_display") or lead.get("ranking_confidence_display") or passport.get("confidence_score_display"),
        "0 / 100",
    )
    confidence_tone = _normalize_tone(
        risk_payload.get("top_territory_confidence_tone") or lead.get("ranking_confidence_tone") or passport.get("confidence_tone"),
        "fire",
    )
    confidence_summary = _safe_text(
        risk_payload.get("top_territory_confidence_note") or lead.get("ranking_confidence_note") or passport.get("validation_summary"),
        "РџРѕСЏСЃРЅРµРЅРёРµ РїРѕ РЅР°РґРµР¶РЅРѕСЃС‚Рё РІС‹РІРѕРґР° РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р°.",
    )
    priority_tone = _normalize_tone(lead.get("risk_tone"), "sky")

    lead_line = (
        f"РўРµСЂСЂРёС‚РѕСЂРёСЏ РїРµСЂРІРѕРіРѕ РІРЅРёРјР°РЅРёСЏ: {top_territory_label}. {priority_reason} "
        f"РџРµСЂРІРѕРµ РґРµР№СЃС‚РІРёРµ: {action_label}. "
        f"РќР°РґРµР¶РЅРѕСЃС‚СЊ РїСЂРёРѕСЂРёС‚РµС‚Р°: {confidence_label} ({confidence_score_display})."
    )
    export_excerpt = (
        f"{top_territory_label} СЃРµР№С‡Р°СЃ РёРґРµС‚ РїРµСЂРІРѕР№ РІ С‚РµСЂСЂРёС‚РѕСЂРёР°Р»СЊРЅРѕРј РїСЂРёРѕСЂРёС‚РµС‚Рµ. "
        f"{priority_reason} "
        f"Р РµРєРѕРјРµРЅРґСѓРµРјРѕРµ РґРµР№СЃС‚РІРёРµ: {action_label}. {action_detail} "
        f"РќР°РґРµР¶РЅРѕСЃС‚СЊ РїСЂРёРѕСЂРёС‚РµС‚Р°: {confidence_label} ({confidence_score_display})."
    )

    simplified_territories: List[Dict[str, str]] = []
    for item in territories[:3]:
        simplified_territories.append(
            {
                "label": _safe_text(item.get("label"), "РўРµСЂСЂРёС‚РѕСЂРёСЏ"),
                "risk_display": _safe_text(item.get("risk_display"), "0 / 100"),
                "risk_tone": _normalize_tone(item.get("risk_tone"), "sky"),
                "priority_label": _safe_text(item.get("priority_label"), "РџР»Р°РЅРѕРІРѕРµ РЅР°Р±Р»СЋРґРµРЅРёРµ"),
                "reason": _safe_text(
                    item.get("ranking_reason") or item.get("drivers_display"),
                    "РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РѕР±СЉСЏСЃРЅРµРЅРёСЏ РїСЂРёРѕСЂРёС‚РµС‚Р°.",
                ),
                "action_label": _safe_text(item.get("action_label"), "РџР»Р°РЅРѕРІРѕРµ РЅР°Р±Р»СЋРґРµРЅРёРµ"),
                "action_detail": _safe_text(item.get("action_hint"), "Р”РµС‚Р°Р»РёР·Р°С†РёСЏ РґРµР№СЃС‚РІРёСЏ РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р°."),
                "confidence_label": _safe_text(item.get("ranking_confidence_label"), confidence_label),
            }
        )

    brief_notes = _unique_notes(notes or risk_payload.get("notes") or [])

    return {
        "lead": lead_line,
        "top_territory_label": top_territory_label,
        "priority_reason": priority_reason,
        "priority_tone": priority_tone,
        "why_value": why_value,
        "why_meta": why_meta,
        "action_label": action_label,
        "action_detail": action_detail,
        "confidence_label": confidence_label,
        "confidence_score_display": confidence_score_display,
        "confidence_tone": confidence_tone,
        "confidence_summary": confidence_summary,
        "cards": [
            {
                "label": "РљР°РєР°СЏ С‚РµСЂСЂРёС‚РѕСЂРёСЏ РїРµСЂРІР°СЏ",
                "value": top_territory_label,
                "meta": priority_reason,
                "tone": priority_tone,
            },
            {
                "label": "РџРѕС‡РµРјСѓ РѕРЅР° РІС‹С€Рµ",
                "value": why_value,
                "meta": why_meta,
                "tone": "sand",
            },
            {
                "label": "РќР°СЃРєРѕР»СЊРєРѕ РЅР°РґРµР¶РµРЅ РїСЂРёРѕСЂРёС‚РµС‚",
                "value": confidence_label,
                "meta": f"{confidence_score_display}. {confidence_summary}",
                "tone": confidence_tone,
            },
            {
                "label": "Р§С‚Рѕ СЃРґРµР»Р°С‚СЊ РїРµСЂРІС‹Рј",
                "value": action_label,
                "meta": action_detail,
                "tone": "forest",
            },
        ],
        "territories": simplified_territories,
        "notes": brief_notes[:3],
        "export_title": "РљРѕСЂРѕС‚РєРѕ РґР»СЏ РїРµСЂРµРґР°С‡Рё РґР°Р»СЊС€Рµ",
        "export_excerpt": export_excerpt,
        "export_text": "",
    }


def compose_executive_brief_text(
    brief: Optional[dict[str, Any]],
    *,
    scope_label: str = "",
    generated_at: str = "",
) -> str:
    safe_brief = brief or empty_executive_brief()
    notes = list(safe_brief.get("notes") or [])
    territories = list(safe_brief.get("territories") or [])

    lines = ["РљРѕСЂРѕС‚РєРёР№ РІС‹РІРѕРґ РїРѕ С‚РµСЂСЂРёС‚РѕСЂРёР°Р»СЊРЅРѕРјСѓ РїСЂРёРѕСЂРёС‚РµС‚Сѓ"]
    if _safe_text(generated_at):
        lines.append(f"РЎС„РѕСЂРјРёСЂРѕРІР°РЅРѕ: {_safe_text(generated_at)}")
    if _safe_text(scope_label):
        lines.append(f"РЎСЂРµР·: {_safe_text(scope_label)}")

    lines.extend(
        [
            "",
            f"РљР°РєР°СЏ С‚РµСЂСЂРёС‚РѕСЂРёСЏ РїРµСЂРІР°СЏ: {_safe_text(safe_brief.get('top_territory_label'), '-')}",
            f"РџРѕС‡РµРјСѓ РѕРЅР° РІС‹С€Рµ: {_safe_text(safe_brief.get('priority_reason'), 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РѕР±СЉСЏСЃРЅРµРЅРёСЏ РїСЂРёРѕСЂРёС‚РµС‚Р°.')}",
            f"РќР°СЃРєРѕР»СЊРєРѕ РЅР°РґРµР¶РµРЅ РїСЂРёРѕСЂРёС‚РµС‚: {_safe_text(safe_brief.get('confidence_label'), 'РћРіСЂР°РЅРёС‡РµРЅРЅР°СЏ')} ({_safe_text(safe_brief.get('confidence_score_display'), '0 / 100')})",
            f"РџРѕС‡РµРјСѓ СѓСЂРѕРІРµРЅСЊ РґРѕРІРµСЂРёСЏ С‚Р°РєРѕР№: {_safe_text(safe_brief.get('confidence_summary'), 'РџРѕСЏСЃРЅРµРЅРёРµ РїРѕ РЅР°РґРµР¶РЅРѕСЃС‚Рё РІС‹РІРѕРґР° РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р°.')}",
            f"Р§С‚Рѕ СЃРґРµР»Р°С‚СЊ РїРµСЂРІС‹Рј: {_safe_text(safe_brief.get('action_label'), 'РџР»Р°РЅРѕРІРѕРµ РЅР°Р±Р»СЋРґРµРЅРёРµ')}",
            f"Р”РµС‚Р°Р»СЊ РґРµР№СЃС‚РІРёСЏ: {_safe_text(safe_brief.get('action_detail'), 'Р”РµС‚Р°Р»РёР·Р°С†РёСЏ РґРµР№СЃС‚РІРёСЏ РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р°.')}",
            "",
            "РљРѕСЂРѕС‚РєРѕ РґР»СЏ РїРµСЂРµРґР°С‡Рё РґР°Р»СЊС€Рµ:",
            _safe_text(
                safe_brief.get("export_excerpt"),
                "РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РєРѕСЂРѕС‚РєР°СЏ СЃРїСЂР°РІРєР° РґР»СЏ РїРµСЂРµРґР°С‡Рё РІ СЃРјРµРЅСѓ РёР»Рё СЂСѓРєРѕРІРѕРґРёС‚РµР»СЋ.",
            ),
        ]
    )

    if territories:
        lines.append("")
        lines.append("РЎР»РµРґСѓСЋС‰РёРµ С‚РµСЂСЂРёС‚РѕСЂРёРё РІ РїСЂРёРѕСЂРёС‚РµС‚Рµ:")
        for index, item in enumerate(territories[:3], start=1):
            lines.append(
                f"{index}. {_safe_text(item.get('label'), 'РўРµСЂСЂРёС‚РѕСЂРёСЏ')} | "
                f"{_safe_text(item.get('risk_display'), '0 / 100')} | "
                f"{_safe_text(item.get('priority_label'), 'РџР»Р°РЅРѕРІРѕРµ РЅР°Р±Р»СЋРґРµРЅРёРµ')}"
            )

    lines.append("")
    lines.append("РћРіСЂР°РЅРёС‡РµРЅРёСЏ Рё РїСЂРёРјРµС‡Р°РЅРёСЏ:")
    if notes:
        for index, note in enumerate(notes, start=1):
            lines.append(f"{index}. {note}")
    else:
        lines.append("1. РЎСѓС‰РµСЃС‚РІРµРЅРЅС‹С… РѕРіСЂР°РЅРёС‡РµРЅРёР№ РІ С‚РµРєСѓС‰РµРј СЃСЂРµР·Рµ РЅРµ Р·Р°С„РёРєСЃРёСЂРѕРІР°РЅРѕ.")

    return "\r\n".join(lines)


__all__ = [
    "build_executive_brief_from_risk_payload",
    "compose_executive_brief_text",
    "empty_executive_brief",
]
