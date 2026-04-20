from __future__ import annotations

from typing import Any, Iterable, Sequence


def _maybe_fix_mojibake(text: str) -> str:
    """Best-effort repair for cp1251/utf-8 mojibake like 'По...'."""
    if not text:
        return text
    if "Р" not in text and "С" not in text:
        return text
    try:
        repaired = text.encode("cp1251").decode("utf-8")
    except Exception:
        return text
    if repaired.count("Р") + repaired.count("С") < text.count("Р") + text.count("С"):
        return repaired
    return text


def _safe_text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    text = _maybe_fix_mojibake(text)
    return text or fallback


def _normalize_tone(value: Any, fallback: str = "sky") -> str:
    tone = _safe_text(value, fallback)
    tone_map = {
        "high": "fire",
        "medium": "sand",
        "low": "sky",
    }
    return tone_map.get(tone, tone or fallback)


def _unique_notes(notes: Iterable[Any]) -> list[str]:
    result: list[str] = []
    for item in notes:
        text = _safe_text(item)
        if text and text not in result:
            result.append(text)
    return result


def empty_executive_brief() -> dict[str, Any]:
    return {
        "lead": "После расчета здесь появится краткий территориальный вывод: какая территория идет первой в приоритете, что проверить сначала и насколько надежен этот вывод.",
        "top_territory_label": "-",
        "priority_reason": "Недостаточно данных, чтобы выделить территорию первого приоритета.",
        "priority_tone": "sky",
        "why_value": "Недостаточно данных",
        "why_meta": "Причина приоритета появится после накопления данных.",
        "action_label": "Плановое наблюдение",
        "action_detail": "Детализация действия появится после расчета.",
        "confidence_label": "Ограниченная",
        "confidence_score_display": "0 / 100",
        "confidence_tone": "fire",
        "confidence_summary": "После расчета здесь появится оценка доверия к данным.",
        "cards": [
            {
                "label": "Какая территория первая",
                "value": "-",
                "meta": "Недостаточно данных для приоритета.",
                "tone": "sky",
            },
            {
                "label": "Почему она выше",
                "value": "Недостаточно данных",
                "meta": "Причина приоритета появится после расчета.",
                "tone": "sand",
            },
            {
                "label": "Насколько надежен приоритет",
                "value": "Ограниченная",
                "meta": "0 / 100. После расчета здесь появится оценка доверия к данным.",
                "tone": "fire",
            },
            {
                "label": "Что сделать первым",
                "value": "Плановое наблюдение",
                "meta": "Детализация действия появится после расчета.",
                "tone": "forest",
            },
        ],
        "territories": [],
        "notes": [],
        "export_title": "Коротко для передачи дальше",
        "export_excerpt": "После расчета здесь появится короткая справка для руководителя, смены или дежурного.",
        "export_text": "",
    }


def build_executive_brief_from_risk_payload(
    risk_payload: dict[str, Any] | None,
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
        "Недостаточно данных, чтобы объяснить территорию первого приоритета.",
    )
    top_component = (lead.get("component_scores") or [{}])[0] if lead else {}
    why_value = _safe_text(top_component.get("label"), "Нет доминирующего фактора")
    why_meta = _safe_text(
        lead.get("drivers_display") if lead else risk_payload.get("top_territory_explanation"),
        "Причина приоритета появится после накопления данных.",
    )
    action_label = _safe_text(
        lead.get("action_label"),
        "Плановое наблюдение",
    )
    action_detail = _safe_text(
        lead.get("action_hint") or ((lead.get("recommendations") or [{}])[0]).get("detail"),
        "Сначала проверьте локальную обстановку и подтвердите приоритет на месте.",
    )
    confidence_label = _safe_text(
        risk_payload.get("top_territory_confidence_label") or lead.get("ranking_confidence_label") or passport.get("confidence_label"),
        "Ограниченная",
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
        "Пояснение по надежности вывода появится после расчета.",
    )
    priority_tone = _normalize_tone(lead.get("risk_tone"), "sky")

    lead_line = (
        f"Территория первого внимания: {top_territory_label}. {priority_reason} "
        f"Первое действие: {action_label}. "
        f"Надежность приоритета: {confidence_label} ({confidence_score_display})."
    )
    export_excerpt = (
        f"{top_territory_label} сейчас идет первой в территориальном приоритете. "
        f"{priority_reason} "
        f"Рекомендуемое действие: {action_label}. {action_detail} "
        f"Надежность приоритета: {confidence_label} ({confidence_score_display})."
    )

    simplified_territories: list[dict[str, str]] = []
    for item in territories[:3]:
        simplified_territories.append(
            {
                "label": _safe_text(item.get("label"), "Территория"),
                "risk_display": _safe_text(item.get("risk_display"), "0 / 100"),
                "risk_tone": _normalize_tone(item.get("risk_tone"), "sky"),
                "priority_label": _safe_text(item.get("priority_label"), "Плановое наблюдение"),
                "reason": _safe_text(
                    item.get("ranking_reason") or item.get("drivers_display"),
                    "Недостаточно данных для объяснения приоритета.",
                ),
                "action_label": _safe_text(item.get("action_label"), "Плановое наблюдение"),
                "action_detail": _safe_text(item.get("action_hint"), "Детализация действия появится после расчета."),
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
                "label": "Какая территория первая",
                "value": top_territory_label,
                "meta": priority_reason,
                "tone": priority_tone,
            },
            {
                "label": "Почему она выше",
                "value": why_value,
                "meta": why_meta,
                "tone": "sand",
            },
            {
                "label": "Насколько надежен приоритет",
                "value": confidence_label,
                "meta": f"{confidence_score_display}. {confidence_summary}",
                "tone": confidence_tone,
            },
            {
                "label": "Что сделать первым",
                "value": action_label,
                "meta": action_detail,
                "tone": "forest",
            },
        ],
        "territories": simplified_territories,
        "notes": brief_notes[:3],
        "export_title": "Коротко для передачи дальше",
        "export_excerpt": export_excerpt,
        "export_text": "",
    }


def compose_executive_brief_text(
    brief: dict[str, Any] | None,
    *,
    scope_label: str = "",
    generated_at: str = "",
) -> str:
    safe_brief = brief or empty_executive_brief()
    notes = list(safe_brief.get("notes") or [])
    territories = list(safe_brief.get("territories") or [])

    lines = ["Короткий вывод по территориальному приоритету"]
    if _safe_text(generated_at):
        lines.append(f"Сформировано: {_safe_text(generated_at)}")
    if _safe_text(scope_label):
        lines.append(f"Срез: {_safe_text(scope_label)}")

    lines.extend(
        [
            "",
            f"Какая территория первая: {_safe_text(safe_brief.get('top_territory_label'), '-')}",
            f"Почему она выше: {_safe_text(safe_brief.get('priority_reason'), 'Недостаточно данных для объяснения приоритета.')}",
            f"Насколько надежен приоритет: {_safe_text(safe_brief.get('confidence_label'), 'Ограниченная')} ({_safe_text(safe_brief.get('confidence_score_display'), '0 / 100')})",
            f"Почему уровень доверия такой: {_safe_text(safe_brief.get('confidence_summary'), 'Пояснение по надежности вывода появится после расчета.')}",
            f"Что сделать первым: {_safe_text(safe_brief.get('action_label'), 'Плановое наблюдение')}",
            f"Деталь действия: {_safe_text(safe_brief.get('action_detail'), 'Детализация действия появится после расчета.')}",
            "",
            "Коротко для передачи дальше:",
            _safe_text(
                safe_brief.get("export_excerpt"),
                "После расчета здесь появится короткая справка для передачи в смену или руководителю.",
            ),
        ]
    )

    if territories:
        lines.append("")
        lines.append("Следующие территории в приоритете:")
        for index, item in enumerate(territories[:3], start=1):
            lines.append(
                f"{index}. {_safe_text(item.get('label'), 'Территория')} | "
                f"{_safe_text(item.get('risk_display'), '0 / 100')} | "
                f"{_safe_text(item.get('priority_label'), 'Плановое наблюдение')}"
            )

    lines.append("")
    lines.append("Ограничения и примечания:")
    if notes:
        for index, note in enumerate(notes, start=1):
            lines.append(f"{index}. {note}")
    else:
        lines.append("1. Существенных ограничений в текущем срезе не зафиксировано.")

    return "\r\n".join(lines)


__all__ = [
    "build_executive_brief_from_risk_payload",
    "compose_executive_brief_text",
    "empty_executive_brief",
]
