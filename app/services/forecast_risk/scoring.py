from __future__ import annotations

from collections import Counter
from datetime import timedelta
import math
from typing import Any, Dict, List, Optional, Sequence

from .constants import LONG_RESPONSE_THRESHOLD_MINUTES
from .profiles import DEFAULT_RISK_WEIGHT_MODE, get_risk_weight_profile, resolve_component_weights
from .utils import (
    _clamp,
    _counter_top_label,
    _format_integer,
    _format_number,
    _format_percent,
    _format_probability,
    _is_heating_season,
    _is_rural_label,
)


def _build_territory_rows(
    records: Sequence[Dict[str, Any]],
    planning_horizon_days: int,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
) -> List[Dict[str, Any]]:
    if not records:
        return []

    profile = get_risk_weight_profile(weight_mode)
    thresholds = profile.get("thresholds") or {}
    defaults = profile.get("defaults") or {}

    history_start = min(record["date"] for record in records)
    history_end = max(record["date"] for record in records)
    history_days = max(1, (history_end - history_start).days + 1)
    horizon_days = max(1, int(planning_horizon_days or 14))
    future_dates = [history_end + timedelta(days=offset) for offset in range(1, horizon_days + 1)]
    future_months = Counter(item.month for item in future_dates)
    future_weekdays = Counter(item.weekday() for item in future_dates)
    future_heating_share = sum(1 for item in future_dates if _is_heating_season(item)) / horizon_days

    recent_window_days = max(1, min(history_days, 90))
    recent_window_start = history_end - timedelta(days=recent_window_days - 1)
    recent_incidents = sum(1 for record in records if record["date"] >= recent_window_start)
    base_fire_signal = _clamp(1.0 - math.exp(-(recent_incidents / recent_window_days)), 0.08, 0.72)

    territories: Dict[str, Dict[str, Any]] = {}
    for record in records:
        label = record["territory_label"] or record["district"] or "Территория не указана"
        bucket = territories.setdefault(
            label,
            {
                "label": label,
                "incidents": 0,
                "weighted_history": 0.0,
                "seasonal_month_sum": 0.0,
                "seasonal_weekday_sum": 0.0,
                "last_fire": None,
                "response_sum": 0.0,
                "response_count": 0,
                "long_arrivals": 0,
                "distance_sum": 0.0,
                "distance_count": 0,
                "water_known": 0,
                "water_available": 0,
                "severe": 0,
                "victims": 0,
                "major_damage": 0,
                "night_incidents": 0,
                "heating_incidents": 0,
                "risk_score_sum": 0.0,
                "risk_score_count": 0,
                "causes": Counter(),
                "object_categories": Counter(),
                "settlement_types": Counter(),
            },
        )
        age_days = max(0, (history_end - record["date"]).days)
        month_alignment = future_months.get(record["date"].month, 0) / horizon_days
        weekday_alignment = future_weekdays.get(record["date"].weekday(), 0) / horizon_days
        recency_weight = max(0.25, 1.0 - age_days / max(210.0, float(history_days)))
        history_weight = recency_weight * (1.0 + 0.40 * month_alignment) * (1.0 + 0.18 * weekday_alignment)

        bucket["incidents"] += 1
        bucket["weighted_history"] += history_weight
        bucket["seasonal_month_sum"] += month_alignment
        bucket["seasonal_weekday_sum"] += weekday_alignment
        bucket["last_fire"] = record["date"] if bucket["last_fire"] is None else max(bucket["last_fire"], record["date"])
        if record["response_minutes"] is not None:
            bucket["response_sum"] += float(record["response_minutes"])
            bucket["response_count"] += 1
            if record["long_arrival"]:
                bucket["long_arrivals"] += 1
        if record["fire_station_distance"] is not None:
            bucket["distance_sum"] += float(record["fire_station_distance"])
            bucket["distance_count"] += 1
        if record["has_water_supply"] is not None:
            bucket["water_known"] += 1
            if record["has_water_supply"]:
                bucket["water_available"] += 1
        if record["severe_consequence"]:
            bucket["severe"] += 1
        if record["victims_present"]:
            bucket["victims"] += 1
        if record["major_damage"]:
            bucket["major_damage"] += 1
        if record["night_incident"]:
            bucket["night_incidents"] += 1
        if record["heating_season"]:
            bucket["heating_incidents"] += 1
        bucket["risk_score_sum"] += float(record["risk_category_score"])
        bucket["risk_score_count"] += 1
        if record["cause"]:
            bucket["causes"][record["cause"]] += 1
        if record["object_category"]:
            bucket["object_categories"][record["object_category"]] += 1
        if record["settlement_type"]:
            bucket["settlement_types"][record["settlement_type"]] += 1

    max_incidents = max(bucket["incidents"] for bucket in territories.values())
    max_weighted = max(bucket["weighted_history"] for bucket in territories.values())

    territory_rows: List[Dict[str, Any]] = []
    for bucket in territories.values():
        incidents = bucket["incidents"]
        dominant_object_category = _counter_top_label(bucket["object_categories"], "Не указано")
        dominant_settlement_type = _counter_top_label(bucket["settlement_types"], "Не указано")
        is_rural = _is_rural_label(dominant_settlement_type) or _is_rural_label(bucket["label"])
        settlement_context_label = "Сельская территория" if is_rural else "Территория без выраженного сельского профиля"
        component_weights = resolve_component_weights(profile, is_rural=is_rural)

        history_pressure = incidents / max(1, max_incidents)
        recency_pressure = bucket["weighted_history"] / max(1.0, max_weighted)
        seasonal_alignment = _clamp(
            0.62 * (bucket["seasonal_month_sum"] / incidents) + 0.38 * (bucket["seasonal_weekday_sum"] / incidents),
            0.0,
            1.0,
        )
        expected_value = (bucket["weighted_history"] / history_days) * horizon_days * (0.72 + base_fire_signal)
        fire_probability = _clamp(
            max(
                1.0 - math.exp(-max(0.0, expected_value)),
                base_fire_signal * min(0.94, 0.22 + history_pressure * 0.52),
            ),
            0.02,
            0.995,
        )

        severe_rate = bucket["severe"] / incidents
        victims_rate = bucket["victims"] / incidents
        damage_rate = bucket["major_damage"] / incidents
        heating_share = bucket["heating_incidents"] / incidents
        heating_pressure = _clamp(heating_share * future_heating_share, 0.0, 1.0)
        night_share = bucket["night_incidents"] / incidents
        risk_factor = bucket["risk_score_sum"] / bucket["risk_score_count"] if bucket["risk_score_count"] else 0.26

        casualty_pressure = _clamp(victims_rate * 1.8, 0.0, 1.0)
        damage_pressure = _clamp(0.70 * damage_rate + 0.30 * severe_rate, 0.0, 1.0)
        severe_probability = _clamp(
            0.46 * severe_rate + 0.26 * casualty_pressure + 0.18 * damage_pressure + 0.10 * risk_factor,
            0.02,
            0.98,
        )

        avg_response = bucket["response_sum"] / bucket["response_count"] if bucket["response_count"] else None
        avg_distance = bucket["distance_sum"] / bucket["distance_count"] if bucket["distance_count"] else None
        distance_score = _clamp(
            ((avg_distance or float(defaults.get("distance_km_baseline", 12.0))) - 6.0) / 24.0,
            0.0,
            1.0,
        )
        long_arrival_rate = (
            bucket["long_arrivals"] / bucket["response_count"] if bucket["response_count"] else _clamp(distance_score * 0.55, 0.05, 0.75)
        )
        response_pressure = (
            _clamp((avg_response - 12.0) / 18.0, 0.0, 1.0)
            if avg_response is not None
            else _clamp(max(float(defaults.get("response_pressure_unknown", 0.42)), distance_score * 0.72), 0.0, 1.0)
        )
        arrival_probability = _clamp(0.42 * long_arrival_rate + 0.34 * response_pressure + 0.24 * distance_score, 0.03, 0.98)

        water_gap_rate = (
            1.0 - (bucket["water_available"] / bucket["water_known"])
            if bucket["water_known"]
            else float(defaults.get("water_gap_unknown", 0.38))
        )
        tanker_dependency = _clamp(0.58 * distance_score + 0.42 * response_pressure, 0.0, 1.0)
        water_deficit_probability = _clamp(0.76 * water_gap_rate + 0.24 * tanker_dependency, 0.02, 0.99)
        rural_context = 1.0 if is_rural else 0.0

        signal_values = {
            "predicted_repeat_rate": fire_probability,
            "history_pressure": history_pressure,
            "recency_pressure": recency_pressure,
            "seasonal_alignment": seasonal_alignment,
            "heating_pressure": heating_pressure,
            "severe_rate": severe_rate,
            "casualty_pressure": casualty_pressure,
            "damage_pressure": damage_pressure,
            "risk_category_factor": risk_factor,
            "long_arrival_rate": long_arrival_rate,
            "avg_response_pressure": response_pressure,
            "distance_pressure": distance_score,
            "night_pressure": night_share,
            "water_gap_rate": water_gap_rate,
            "tanker_dependency": tanker_dependency,
            "rural_context": rural_context,
        }

        context = {
            "incidents": incidents,
            "history_pressure": history_pressure,
            "recency_pressure": recency_pressure,
            "seasonal_alignment": seasonal_alignment,
            "fire_probability": fire_probability,
            "severe_rate": severe_rate,
            "victims_rate": victims_rate,
            "damage_rate": damage_rate,
            "risk_factor": risk_factor,
            "avg_response": avg_response,
            "avg_distance": avg_distance,
            "long_arrival_rate": long_arrival_rate,
            "night_share": night_share,
            "water_gap_rate": water_gap_rate,
            "water_known": bucket["water_known"],
            "water_available": bucket["water_available"],
            "tanker_dependency": tanker_dependency,
            "is_rural": is_rural,
            "settlement_context_label": settlement_context_label,
            "heating_share": heating_share,
        }

        component_scores: List[Dict[str, Any]] = []
        for component_weight in component_weights:
            component_scores.append(
                _score_component(
                    component_weight=component_weight,
                    component_spec=(profile.get("components") or {}).get(component_weight["key"], {}),
                    signal_values=signal_values,
                    thresholds=thresholds,
                    context=context,
                )
            )

        component_scores.sort(key=lambda item: item["contribution"], reverse=True)
        component_score_map = {
            item["key"]: {
                "score": item["score"],
                "contribution": item["contribution"],
                "weight": item["weight"],
            }
            for item in component_scores
        }
        risk_score = _clamp(sum(item["contribution"] for item in component_scores), 1.0, 99.0)
        risk_class_label, risk_tone = _risk_class(risk_score, thresholds)
        priority_label, priority_tone = _priority_label(risk_score, component_score_map, is_rural, thresholds)
        drivers = _build_risk_drivers(component_scores)
        drivers_display = ", ".join(drivers)
        action_label, action_hint, recommendations = _recommended_action(
            risk_score=risk_score,
            component_scores=component_scores,
            context=context,
        )
        formula_display = _build_formula_display(component_scores, risk_score)

        territory_rows.append(
            {
                "label": bucket["label"],
                "risk_score": round(risk_score, 1),
                "risk_display": f"{_format_number(risk_score)} / 100",
                "risk_formula_display": formula_display,
                "risk_class_label": risk_class_label,
                "risk_tone": risk_tone,
                "priority_label": priority_label,
                "priority_tone": priority_tone,
                "weight_mode": profile.get("mode") or DEFAULT_RISK_WEIGHT_MODE,
                "weight_mode_label": profile.get("mode_label") or "Экспертные веса",
                "component_scores": component_scores,
                "component_score_map": component_score_map,
                "fire_probability": fire_probability,
                "severe_probability": severe_probability,
                "arrival_probability": arrival_probability,
                "water_deficit_probability": water_deficit_probability,
                "fire_probability_display": _format_probability(fire_probability),
                "severe_probability_display": _format_probability(severe_probability),
                "arrival_probability_display": _format_probability(arrival_probability),
                "water_deficit_display": _format_probability(water_deficit_probability),
                "history_count": incidents,
                "history_count_display": _format_integer(incidents),
                "last_fire_display": bucket["last_fire"].strftime("%d.%m.%Y") if bucket["last_fire"] else "-",
                "avg_response_minutes": round(avg_response, 1) if avg_response is not None else None,
                "response_time_display": f"{_format_number(avg_response)} мин" if avg_response is not None else "Нет данных",
                "avg_distance_km": round(avg_distance, 1) if avg_distance is not None else None,
                "distance_display": f"{_format_number(avg_distance)} км" if avg_distance is not None else "Нет данных",
                "water_availability_share": round(bucket["water_available"] / bucket["water_known"], 4) if bucket["water_known"] else None,
                "water_supply_display": _water_supply_display(bucket["water_available"], bucket["water_known"]),
                "dominant_object_category": dominant_object_category,
                "dominant_settlement_type": dominant_settlement_type,
                "settlement_context_label": settlement_context_label,
                "is_rural": is_rural,
                "drivers_display": drivers_display,
                "action_label": action_label,
                "action_hint": action_hint,
                "recommendations": recommendations,
                "explanation": (
                    f"Итоговый риск { _format_number(risk_score) } / 100. "
                    f"Формула: {formula_display}. "
                    f"Ключевые причины: {drivers_display}."
                ),
                "bar_width": f"{max(10, min(100, round(risk_score)))}%",
                "history_pressure": round(history_pressure, 3),
            }
        )

    territory_rows.sort(key=lambda item: (item["risk_score"], item["history_pressure"]), reverse=True)
    return territory_rows


def _score_component(
    component_weight: Dict[str, Any],
    component_spec: Dict[str, Any],
    signal_values: Dict[str, float],
    thresholds: Dict[str, Any],
    context: Dict[str, Any],
) -> Dict[str, Any]:
    signal_rows: List[Dict[str, Any]] = []
    weighted_sum = 0.0
    total_signal_weight = 0.0

    for signal in component_spec.get("signals", []):
        signal_weight = float(signal.get("weight", 0.0))
        signal_value = _clamp(float(signal_values.get(signal.get("key"), 0.0)), 0.0, 1.0)
        weighted_value = signal_value * signal_weight
        weighted_sum += weighted_value
        total_signal_weight += signal_weight
        signal_rows.append(
            {
                "key": signal.get("key") or "",
                "label": signal.get("label") or signal.get("key") or "Сигнал",
                "value": round(signal_value, 4),
                "value_display": f"{_format_number(signal_value * 100.0)} / 100",
                "weight": signal_weight,
                "weight_display": f"{_format_number(signal_weight * 100.0)}%",
                "weighted_value": round(weighted_value, 4),
            }
        )

    signal_rows.sort(key=lambda item: item["weighted_value"], reverse=True)
    score = 100.0 * weighted_sum / total_signal_weight if total_signal_weight > 0 else 0.0
    contribution = score * float(component_weight.get("weight", 0.0))
    tone = _component_tone(score, thresholds)

    result = {
        "key": component_weight.get("key") or "component",
        "label": component_weight.get("label") or component_spec.get("label") or "Компонент",
        "description": component_weight.get("description") or component_spec.get("description") or "",
        "score": round(score, 1),
        "score_display": f"{_format_number(score)} / 100",
        "weight": round(float(component_weight.get("weight", 0.0)), 4),
        "weight_display": component_weight.get("weight_display") or "0%",
        "contribution": round(contribution, 1),
        "contribution_display": f"{_format_number(contribution)} балла",
        "tone": tone,
        "signals": signal_rows,
        "bar_width": f"{max(12, min(100, round(score)))}%",
    }
    result["summary"] = f"Вес {result['weight_display']}, вклад {result['contribution_display']}."
    result["rationale"] = _component_rationale(result["key"], score, context)
    result["driver_text"] = _component_driver_text(result["key"], score, context)
    return result


def _component_tone(score: float, thresholds: Dict[str, Any]) -> str:
    component_thresholds = thresholds.get("component") or {}
    if score >= float(component_thresholds.get("high", 65.0)):
        return "high"
    if score >= float(component_thresholds.get("medium", 40.0)):
        return "medium"
    return "low"


def _component_rationale(component_key: str, score: float, context: Dict[str, Any]) -> str:
    if component_key == "fire_frequency":
        parts = [f"В истории {_format_integer(context['incidents'])} пожаров."]
        if context["history_pressure"] >= 0.65:
            parts.append("Территория уже накапливала много случаев относительно выбранного среза.")
        if context["recency_pressure"] >= 0.60:
            parts.append("Часть пожаров свежая и влияет на ближайший горизонт.")
        if context["seasonal_alignment"] >= 0.55:
            parts.append("Профиль хорошо совпадает с текущим сезонным окном.")
        if context["heating_share"] >= 0.50 and context["is_rural"]:
            parts.append("Для сельской территории заметен отопительный контур риска.")
        if score < 35:
            parts.append("Повторяемость пока умеренная.")
        return " ".join(parts[:4])

    if component_key == "consequence_severity":
        parts = []
        if context["severe_rate"] >= 0.25:
            parts.append(f"Тяжёлые последствия были в {_format_probability(context['severe_rate'])} случаев.")
        if context["victims_rate"] >= 0.08:
            parts.append("В истории есть пострадавшие или погибшие.")
        if context["damage_rate"] >= 0.30:
            parts.append("Ущерб и уничтожение фиксировались часто.")
        if context["risk_factor"] >= 0.56:
            parts.append("Категория риска по объектам выше средней.")
        if not parts:
            parts.append("История тяжёлых последствий пока умеренная.")
        return " ".join(parts[:4])

    if component_key == "long_arrival_risk":
        parts = []
        if context["avg_response"] is not None:
            parts.append(f"Среднее прибытие {_format_number(context['avg_response'])} мин.")
        else:
            parts.append("Фактическое время прибытия не найдено, поэтому компонент опирается на удалённость.")
        if context["long_arrival_rate"] >= 0.25:
            parts.append(f"Долгие прибытия были в {_format_probability(context['long_arrival_rate'])} случаев.")
        if context["avg_distance"] is not None and context["avg_distance"] >= 15.0:
            parts.append(f"Удалённость до ПЧ {_format_number(context['avg_distance'])} км.")
        if context["is_rural"]:
            parts.append("Для сельской территории логистика критичнее среднего.")
        return " ".join(parts[:4])

    parts = []
    if context["water_known"] > 0:
        water_share = context["water_available"] / max(1, context["water_known"])
        parts.append(f"Подтверждённая вода есть только в {_format_probability(water_share)} случаев.")
    else:
        parts.append("Подтверждённых записей о воде нет, поэтому использован осторожный базовый уровень риска.")
    if context["tanker_dependency"] >= 0.55:
        parts.append("Из-за удалённости территория сильнее зависит от подвоза воды.")
    if context["is_rural"]:
        parts.append("Для сельской территории запас воды и подъезд к источникам особенно критичны.")
    return " ".join(parts[:4])


def _component_driver_text(component_key: str, score: float, context: Dict[str, Any]) -> str:
    if score < 38:
        return ""
    if component_key == "fire_frequency":
        if context["heating_share"] >= 0.50 and context["is_rural"]:
            return "пожары здесь повторяются и усиливаются в отопительный период"
        return "пожары здесь повторяются чаще фонового уровня"
    if component_key == "consequence_severity":
        return "история последствий здесь тяжелее среднего"
    if component_key == "long_arrival_risk":
        if context["avg_distance"] is not None and context["avg_distance"] >= 15.0:
            return "есть риск долгого прибытия из-за удалённости"
        return "логистика выезда может удлинять прибытие"
    return "не подтверждён стабильный доступ к воде для тушения"


def _build_risk_drivers(component_scores: Sequence[Dict[str, Any]]) -> List[str]:
    drivers = [item.get("driver_text") or "" for item in component_scores if item.get("driver_text")]
    if not drivers:
        return ["профиль риска пока умеренный и без явного доминирующего фактора"]
    return drivers[:3]


def _priority_label(
    risk_score: float,
    component_score_map: Dict[str, Dict[str, float]],
    is_rural: bool,
    thresholds: Dict[str, Any],
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


def _risk_class(score: float, thresholds: Dict[str, Any]) -> tuple[str, str]:
    risk_thresholds = thresholds.get("risk_class") or {}
    if score >= float(risk_thresholds.get("high", 67.0)):
        return "Высокий риск", "high"
    if score >= float(risk_thresholds.get("medium", 43.0)):
        return "Средний риск", "medium"
    return "Низкий риск", "low"


def _recommended_action(
    risk_score: float,
    component_scores: Sequence[Dict[str, Any]],
    context: Dict[str, Any],
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
        detail = "Уточните маршрут, резерв прикрытия, точки разворота и фактическое время доезда."
        if context["avg_distance"] is not None and context["avg_distance"] >= 15.0:
            detail = "Для удалённой территории полезно перепроверить маршрут, резерв прикрытия и возможность промежуточного размещения техники или ДПК."
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


def _build_formula_display(component_scores: Sequence[Dict[str, Any]], risk_score: float) -> str:
    parts = [f"{item['label']} {_format_number(item['contribution'])}" for item in component_scores]
    return f"{' + '.join(parts)} = {_format_number(risk_score)}"

def _top_territory_lead(top_territory: Optional[Dict[str, Any]]) -> str:
    if not top_territory:
        return "Недостаточно данных для лидирующей территории."
    strongest_components = ", ".join(
        f"{item['label']} ({item['contribution_display']})"
        for item in (top_territory.get("component_scores") or [])[:2]
    )
    return (
        f"{top_territory['action_label']}. Итоговый риск {top_territory['risk_display']} формируют прежде всего {strongest_components}. "
        f"{top_territory['action_hint']}"
    )


def _water_supply_display(available_count: int, known_count: int) -> str:
    if known_count <= 0:
        return "нет подтвержденных данных"
    return f"подтверждена в {_format_percent(available_count / known_count * 100.0)} случаев"
