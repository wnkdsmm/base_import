from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.services.forecast_risk.core import build_decision_support_payload

def _empty_management_snapshot() -> Dict[str, Any]:
    return {
        "summary_line": "После загрузки данных здесь появится управленческая сводка: где риск, почему риск и что делать в первую очередь.",
        "priority_territory_label": "-",
        "priority_reason": "Недостаточно данных, чтобы выделить территорию первого приоритета.",
        "priority_tone": "sky",
        "confidence_label": "Ограниченная",
        "confidence_score_display": "0 / 100",
        "confidence_tone": "fire",
        "confidence_summary": "После загрузки данных здесь появится уровень доверия к сводке.",
        "brief_cards": [],
        "territories": [],
        "actions": [],
        "notes": [],
    }


def _build_management_snapshot(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    summary: Dict[str, Any],
    trend: Dict[str, Any],
    cause_overview: Dict[str, Any],
    district_widget: Dict[str, Any],
) -> Dict[str, Any]:
    if not selected_tables:
        return _empty_management_snapshot()

    tone_map = {"high": "fire", "medium": "sand", "low": "sky"}
    try:
        risk_payload = build_decision_support_payload(
            source_tables=[table["name"] for table in selected_tables],
            selected_district="all",
            selected_cause="all",
            selected_object_category="all",
            history_window="all",
            planning_horizon_days=14,
            selected_year=selected_year,
        )
    except Exception as exc:
        fallback = _empty_management_snapshot()
        fallback["summary_line"] = "Управленческий бриф временно недоступен; ниже остаются базовые показатели и графики."
        fallback["notes"] = [f"Управленческий бриф временно недоступен: {exc}"]
        return fallback
    passport = risk_payload.get("quality_passport") or {}
    risk_territories = risk_payload.get("territories") or []
    dominant_cause = cause_overview["items"][0] if cause_overview.get("items") else None
    top_district = district_widget["items"][0] if district_widget.get("items") else None
    top_territory = risk_territories[0] if risk_territories else None

    territories: List[Dict[str, str]] = []
    for item in risk_territories[:3]:
        components = item.get("component_scores") or []
        top_component = components[0] if components else {}
        territories.append(
            {
                "label": item.get("label") or "Территория",
                "risk_display": item.get("risk_display") or "0 / 100",
                "risk_class_label": item.get("risk_class_label") or "Нет оценки",
                "priority_label": item.get("priority_label") or "Плановое наблюдение",
                "risk_tone": tone_map.get(item.get("risk_tone"), "sky"),
                "drivers_display": item.get("drivers_display") or "Недостаточно данных для объяснения приоритета.",
                "action_label": item.get("action_label") or "Оставить территорию в плановом наблюдении",
                "action_hint": item.get("action_hint") or "Сверьте приоритет с локальной оперативной обстановкой.",
                "context_label": item.get("settlement_context_label") or "Контекст не указан",
                "last_fire_display": item.get("last_fire_display") or "-",
                "top_component_label": top_component.get("label") or "Нет доминирующего фактора",
                "top_component_score": top_component.get("score_display") or "0 / 100",
            }
        )

    actions: List[Dict[str, str]] = []
    if top_territory:
        for recommendation in (top_territory.get("recommendations") or [])[:3]:
            label = str(recommendation.get("label") or "").strip()
            detail = str(recommendation.get("detail") or "").strip()
            if label and detail:
                actions.append({"label": label, "detail": detail})

    if not actions:
        if dominant_cause:
            actions.append(
                {
                    "label": "Сделать адресную профилактику по главной причине",
                    "detail": f"В текущем срезе чаще всего фиксируется причина «{dominant_cause['label']}» ({dominant_cause['value_display']}).",
                }
            )
        if top_district:
            actions.append(
                {
                    "label": "Усилить контроль в лидирующей территории",
                    "detail": f"{top_district['label']} остаётся лидером по числу пожаров: {top_district['value_display']}.",
                }
            )
        if trend.get("direction") == "up":
            actions.append(
                {
                    "label": "Проверить источники роста к прошлому году",
                    "detail": f"{trend.get('description') or 'Динамика требует уточнения.'} Изменение: {trend.get('delta_display') or '0'}.",
                }
            )

    if not actions:
        actions.append(
            {
                "label": "Сохранить плановое наблюдение",
                "detail": "Резкого ухудшения по текущему срезу не видно, но приоритетные территории стоит держать в регулярном контроле.",
            }
        )

    confidence_label = passport.get("confidence_label") or "Ограниченная"
    confidence_score_display = passport.get("confidence_score_display") or "0 / 100"
    confidence_tone = passport.get("confidence_tone") or "fire"
    confidence_summary = passport.get("validation_summary") or "После загрузки данных здесь появится уровень доверия к сводке."

    priority_territory_label = (
        top_territory.get("label")
        if top_territory
        else top_district.get("label")
        if top_district
        else "-"
    )
    priority_reason = risk_payload.get("top_territory_explanation") or ""
    if not priority_reason and top_district:
        priority_reason = f"{top_district['label']} лидирует по числу пожаров: {top_district['value_display']}."
    if not priority_reason:
        priority_reason = "Недостаточно данных, чтобы объяснить территорию первого приоритета."

    lead_action = actions[0] if actions else {"label": "Плановое наблюдение", "detail": ""}
    why_value = (
        territories[0]["top_component_label"]
        if territories
        else dominant_cause["label"]
        if dominant_cause
        else "Недостаточно данных"
    )
    why_meta_parts = []
    if territories:
        why_meta_parts.append(territories[0]["drivers_display"])
    if dominant_cause:
        why_meta_parts.append(f"Главная причина в срезе: {dominant_cause['label']}.")
    if not why_meta_parts:
        why_meta_parts.append("Причины приоритета появятся после накопления данных.")

    trend_value = trend.get("current_value_display") or "0"
    if trend.get("current_year") and trend.get("current_year") != "-":
        trend_value = f"{trend_value} в {trend['current_year']}"

    summary_line = (
        f"Первый приоритет: {priority_territory_label}. "
        f"Что делать: {lead_action['label']}. "
        f"Надёжность данных: {confidence_label} ({confidence_score_display})."
    )

    notes = []
    for note in (risk_payload.get("notes") or [])[:3]:
        text = str(note or "").strip()
        if text and text not in notes:
            notes.append(text)
    if not notes:
        notes.append(confidence_summary)

    return {
        "summary_line": summary_line,
        "priority_territory_label": priority_territory_label,
        "priority_reason": priority_reason,
        "priority_tone": tone_map.get(top_territory.get("risk_tone") if top_territory else "low", "sky"),
        "confidence_label": confidence_label,
        "confidence_score_display": confidence_score_display,
        "confidence_tone": confidence_tone,
        "confidence_summary": confidence_summary,
        "brief_cards": [
            {
                "label": "Где риск выше",
                "value": priority_territory_label,
                "meta": priority_reason,
                "tone": tone_map.get(top_territory.get("risk_tone") if top_territory else "low", "sky"),
            },
            {
                "label": "Почему нужен приоритет",
                "value": why_value,
                "meta": " ".join(why_meta_parts),
                "tone": "sand",
            },
            {
                "label": "Что сделать сегодня",
                "value": lead_action["label"],
                "meta": lead_action["detail"] or "Детализация действия появится после расчёта.",
                "tone": "forest",
            },
            {
                "label": "Как меняется обстановка",
                "value": trend_value,
                "meta": trend.get("description") or "Недостаточно данных для сравнения по годам.",
                "tone": "fire" if trend.get("direction") == "up" else "sky",
            },
        ],
        "territories": territories,
        "actions": actions[:3],
        "notes": notes,
    }

__all__ = ["_empty_management_snapshot", "_build_management_snapshot"]
