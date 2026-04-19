from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.services.executive_brief import (
    build_executive_brief_from_risk_payload,
    empty_executive_brief,
)
from app.services.forecast_risk.core import build_decision_support_payload
from config.constants import PRIORITY_HORIZON_DAYS

from .types import DashboardSection, DashboardTableRef, DistributionResult, SummaryResult


def _build_management_snapshot_payload(
    brief: dict[str, Any],  # one-off
    *,
    territories: Optional[List[Dict[str, str]]] = None,
    actions: Optional[List[Dict[str, str]]] = None,
    notes: Optional[List[str]] = None,
    priority_horizon_days: int = PRIORITY_HORIZON_DAYS,
) -> dict[str, Any]:  # one-off
    resolved_notes = list(notes if notes is not None else brief.get("notes") or [])
    return {
        "summary_line": brief["lead"],
        "priority_territory_label": brief["top_territory_label"],
        "priority_reason": brief["priority_reason"],
        "priority_tone": brief["priority_tone"],
        "confidence_label": brief["confidence_label"],
        "confidence_score_display": brief["confidence_score_display"],
        "confidence_tone": brief["confidence_tone"],
        "confidence_summary": brief["confidence_summary"],
        "recommended_action_label": brief["action_label"],
        "recommended_action_detail": brief["action_detail"],
        "brief_cards": list(brief["cards"]),
        "brief": brief,
        "territories": list(territories or []),
        "actions": list(actions or []),
        "notes": resolved_notes,
        "priority_horizon_days": priority_horizon_days,
        "export_title": brief["export_title"],
        "export_excerpt": brief["export_excerpt"],
        "export_text": "",
    }


def _empty_management_snapshot(priority_horizon_days: int = PRIORITY_HORIZON_DAYS) -> dict[str, Any]:  # one-off
    return _build_management_snapshot_payload(
        empty_executive_brief(),
        priority_horizon_days=priority_horizon_days,
    )


def _build_management_snapshot(
    selected_tables: List[DashboardTableRef],
    selected_year: Optional[int],
    summary: SummaryResult,
    trend: DashboardSection,
    cause_overview: DistributionResult,
    district_widget: DistributionResult,
    planning_horizon_days: int = PRIORITY_HORIZON_DAYS,
) -> dict[str, Any]:  # one-off
    if not selected_tables:
        return _empty_management_snapshot(priority_horizon_days=planning_horizon_days)
    tone_map = {"high": "fire", "medium": "sand", "low": "sky"}
    try:
        risk_payload = build_decision_support_payload(
            source_tables=[table["name"] for table in selected_tables],
            selected_district="all",
            selected_cause="all",
            selected_object_category="all",
            history_window="all",
            planning_horizon_days=planning_horizon_days,
            selected_year=selected_year,
            include_geo_prediction=False,
            include_historical_validation=False,
        )
    except Exception as exc:
        fallback = _empty_management_snapshot(priority_horizon_days=planning_horizon_days)
        fallback["summary_line"] = "Краткий территориальный вывод временно недоступен; ниже остаются базовые показатели и графики по выбранному срезу."
        fallback["notes"] = [f"Короткий вывод временно недоступен: {exc}"]
        fallback["brief"]["notes"] = list(fallback["notes"])
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
                    "detail": f"{top_district['label']} остается лидером по числу пожаров: {top_district['value_display']}.",
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

    confidence_summary = passport.get("validation_summary") or "После загрузки данных здесь появится уровень доверия к сводке."
    horizon_note = (
        f"Важно: это краткий территориальный приоритет на ближайшие {planning_horizon_days} дней, "
        "а не календарь риска по датам и не прогноз ожидаемого числа пожаров."
    )
    notes: List[str] = []
    for note in (risk_payload.get("notes") or [])[:3]:
        text = str(note or "").strip()
        if text and text not in notes:
            notes.append(text)
    if horizon_note not in notes:
        notes.insert(0, horizon_note)
    if len(notes) == 1 and confidence_summary not in notes:
        notes.append(confidence_summary)

    brief = build_executive_brief_from_risk_payload(risk_payload, notes=notes)
    if horizon_note not in brief["notes"]:
        brief["notes"] = [horizon_note, *list(brief["notes"] or [])][:3]
    brief["export_excerpt"] = (
        f"{brief['export_excerpt']} Приоритет и действия рассчитаны на ближайшие {planning_horizon_days} дней."
    )
    brief["export_text"] = ""

    return _build_management_snapshot_payload(
        brief,
        territories=territories,
        actions=actions[:3],
        notes=list(brief["notes"] or notes),
        priority_horizon_days=planning_horizon_days,
    )


__all__ = ["_empty_management_snapshot", "_build_management_snapshot"]
