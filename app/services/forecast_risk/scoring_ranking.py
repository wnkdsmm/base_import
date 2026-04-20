from __future__ import annotations

from typing import Any, Sequence

from .utils import _format_number, _format_percent


def _priority_label(
    risk_score: float,
    component_score_map: dict[str, dict[str, float]],
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
        return "Нужны меры сейчас", "fire"
    if risk_score >= targeted_threshold or max(fire_score, severe_score, arrival_score, water_score) >= 60:
        return "Нужны точечные меры", "sand"
    return "Плановое наблюдение", "sky"


def _risk_class(score: float, thresholds: dict[str, Any]) -> tuple[str, str]:
    risk_thresholds = thresholds.get("risk_class") or {}
    if score >= float(risk_thresholds.get("high", 67.0)):
        return "Высокий риск", "high"
    if score >= float(risk_thresholds.get("medium", 43.0)):
        return "Средний риск", "medium"
    return "Низкий риск", "low"


def _recommended_action(
    risk_score: float,
    component_scores: Sequence[dict[str, Any]],
    context: dict[str, Any],
) -> tuple[str, str, list[dict[str, str]]]:
    component_map = {item["key"]: item for item in component_scores}
    recommendations: list[dict[str, str]] = []

    fire_component = component_map.get("fire_frequency", {})
    severe_component = component_map.get("consequence_severity", {})
    arrival_component = component_map.get("long_arrival_risk", {})
    water_component = component_map.get("water_supply_deficit", {})

    if fire_component.get("score", 0.0) >= 55:
        if context["heating_share"] >= 0.45:
            recommendations.append(
                {
                    "label": "Усилить адресную профилактику по отоплению и электрике",
                    "detail": "Полезно проверить печи, электрохозяйство и повторяющиеся бытовые причины именно на этой территории до следующего пика нагрузки.",
                }
            )
        else:
            recommendations.append(
                {
                    "label": "Разобрать повторяющиеся очаги и причины пожаров",
                    "detail": "Сфокусируйтесь на адресах и сценариях, которые уже повторялись в истории этой территории, чтобы снизить входящий поток пожаров.",
                }
            )

    if severe_component.get("score", 0.0) >= 55:
        recommendations.append(
            {
                "label": "Проверить уязвимые объекты и домохозяйства",
                "detail": "Приоритетно пройдите объекты с историей ущерба, одиноко проживающих, социальные объекты и сельхозобъекты, где последствия могут быть тяжелее.",
            }
        )

    if arrival_component.get("score", 0.0) >= 55:
        detail = (
            "Проверьте маршрут, фактический travel-time, резерв прикрытия и держится ли территория в устойчивой зоне обслуживания ПЧ."
        )
        if context["service_coverage_ratio"] < 0.45:
            detail = (
                "Для территории с дефицитом прикрытия полезно перепроверить маршрут, резерв прикрытия, промежуточное размещение техники или ДПК и реальный норматив доезда."
            )
        elif context["avg_distance"] is not None and context["avg_distance"] >= 15.0:
            detail = (
                "Для удалённой территории полезно перепроверить маршрут, резерв прикрытия и возможность промежуточного размещения техники или ДПК."
            )
        recommendations.append(
            {
                "label": "Сократить риск долгого прибытия",
                "detail": detail,
            }
        )

    if water_component.get("score", 0.0) >= 50:
        recommendations.append(
            {
                "label": "Подтвердить воду и подъезд к источникам",
                "detail": "Проверьте гидранты, башни, водоёмы, сухие колодцы и зимний/распутицный подъезд к ним, чтобы вода была реально доступна на выезде.",
            }
        )

    if not recommendations:
        recommendations.append(
            {
                "label": "Оставить территорию в плановом наблюдении",
                "detail": "Сейчас достаточно обычного контроля, сезонной профилактики и периодической сверки логистики и источников воды.",
            }
        )

    top_component = component_scores[0] if component_scores else {"key": "fire_frequency"}
    action_lookup = {
        "fire_frequency": "Усилить адресную профилактику",
        "consequence_severity": "Снизить тяжесть возможных последствий",
        "long_arrival_risk": "Сократить время прибытия",
        "water_supply_deficit": "Подтвердить водоснабжение",
    }
    action_label = action_lookup.get(top_component.get("key"), recommendations[0]["label"])

    if risk_score >= 70 and len(recommendations) >= 2:
        action_hint = f"Сначала {recommendations[0]['label'].lower()}, затем {recommendations[1]['label'].lower()}."
    else:
        action_hint = recommendations[0]["detail"]
    return action_label, action_hint, recommendations[:3]


def _build_formula_display(component_scores: Sequence[dict[str, Any]], risk_score: float) -> str:
    parts = [f"{item['label']} {_format_number(item['contribution'])}" for item in component_scores]
    return f"{' + '.join(parts)} = {_format_number(risk_score)}"


def _attach_ranking_context(territory_rows: list[dict[str, Any]]) -> None:
    if not territory_rows:
        return

    top_score = float(territory_rows[0].get("risk_score") or 0.0)
    for index, item in enumerate(territory_rows):
        next_score = float(territory_rows[index + 1].get("risk_score") or 0.0) if index + 1 < len(territory_rows) else None
        current_score = float(item.get("risk_score") or 0.0)
        gap_to_next = max(0.0, round(current_score - next_score, 1)) if next_score is not None else 0.0
        gap_to_top = max(0.0, round(top_score - current_score, 1))
        strongest_components = [
            f"{component.get('label') or 'Компонент'} ({component.get('contribution_display') or '0 балла'})"
            for component in (item.get("component_scores") or [])[:2]
        ]
        component_lead = ", ".join(strongest_components) if strongest_components else "нет выраженного доминирующего компонента"
        item.update(
            {
                "ranking_position": index + 1,
                "ranking_position_display": f"пїЅ{index + 1}",
                "ranking_gap_to_next": gap_to_next,
                "ranking_gap_to_next_display": f"{_format_number(gap_to_next)} балла" if next_score is not None else "замыкает текущий список",
                "ranking_gap_to_top": gap_to_top,
                "ranking_gap_to_top_display": f"{_format_number(gap_to_top)} балла",
                "ranking_component_lead": component_lead,
                "ranking_reason": _build_ranking_reason(index, gap_to_next, gap_to_top, component_lead),
            }
        )


def _build_ranking_reason(index: int, gap_to_next: float, gap_to_top: float, component_lead: str) -> str:
    if index == 0:
        if gap_to_next >= 4.0:
            return f"Территория лидирует с заметным отрывом {_format_number(gap_to_next)} балла; основной вклад дают {component_lead}."
        if gap_to_next >= 1.5:
            return f"Территория удерживает первое место с рабочим отрывом {_format_number(gap_to_next)} балла; основной вклад дают {component_lead}."
        return f"Территория идет первой в плотной группе; отрыв от следующей территории {_format_number(gap_to_next)} балла, основной вклад дают {component_lead}."

    if gap_to_top <= 2.0:
        return f"Территория держится рядом с лидером: отставание {_format_number(gap_to_top)} балла, ключевые вклады {component_lead}."
    if gap_to_top <= 6.0:
        return f"Территория входит в верхнюю группу: отставание {_format_number(gap_to_top)} балла, ключевые вклады {component_lead}."
    return f"Территория остается в списке из-за вкладов {component_lead}, хотя ниже лидера на {_format_number(gap_to_top)} балла."


def _top_territory_lead(top_territory: dict[str, Any | None]) -> str:
    if not top_territory:
        return "Недостаточно данных для лидирующей территории."
    strongest_components = ", ".join(
        f"{item['label']} ({item['contribution_display']})"
        for item in (top_territory.get("component_scores") or [])[:2]
    )
    parts = [
        f"{top_territory['action_label']}. �?тоговый риск {top_territory['risk_display']} формируют прежде всего {strongest_components}.",
        f"Логистика: {top_territory.get('travel_time_display') or 'н/д'}, покрытие ПЧ {top_territory.get('fire_station_coverage_display') or 'н/д'}, зона {top_territory.get('service_zone_label') or 'не определена'}.",
        top_territory.get("ranking_reason") or "",
        top_territory.get("action_hint") or "",
    ]
    return " ".join(part.strip() for part in parts if str(part).strip())


def _water_supply_display(available_count: int, known_count: int) -> str:
    if known_count <= 0:
        return "нет подтвержденных данных"
    return f"подтверждена в {_format_percent(available_count / known_count * 100.0)} случаев"
