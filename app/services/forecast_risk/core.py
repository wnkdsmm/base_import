from __future__ import annotations

from statistics import mean
from typing import Any, Dict, Optional, Sequence

from .constants import MAX_TERRITORIES
from .data import _collect_risk_inputs
from .presentation import _build_feature_cards, _build_geo_summary, _build_quality_passport, _build_risk_notes
from .profiles import DEFAULT_RISK_WEIGHT_MODE, build_weight_profile_snapshot, get_risk_weight_profile
from .scoring import _build_territory_rows, _top_territory_lead
from .utils import _apply_history_window, _format_integer, _format_number, _unique_non_empty
from .validation import build_historical_validation_payload, empty_historical_validation_payload

DECISION_SUPPORT_TITLE = "Блок поддержки решений: ранжирование территорий"
DECISION_SUPPORT_DESCRIPTION = (
    "Это прозрачный decision-support инструмент для сельских территорий. Итоговый риск раскладывается на четыре "
    "компонента: частоту пожаров, тяжесть последствий, риск долгого прибытия и дефицит водоснабжения. "
    "Компонентные веса вынесены в отдельный профиль и не смешиваются в одну непрозрачную формулу."
)


def build_decision_support_payload(
    source_tables: Sequence[str],
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    history_window: str,
    planning_horizon_days: int,
    geo_prediction: Optional[Dict[str, Any]] = None,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
) -> Dict[str, Any]:
    metadata_items, records, preload_notes = _collect_risk_inputs(source_tables)
    feature_cards = _build_feature_cards(metadata_items)
    quality_passport = _build_quality_passport(feature_cards, metadata_items)
    coverage_display = f"{sum(1 for item in feature_cards if item['status'] != 'missing')} из {len(feature_cards)}" if feature_cards else "0 из 0"
    geo_summary = _build_geo_summary(geo_prediction or {})
    profile = get_risk_weight_profile(weight_mode)
    weight_profile = build_weight_profile_snapshot(profile)

    scoped_records = _apply_history_window(records, history_window)
    filtered_records = [
        record
        for record in scoped_records
        if (selected_district == "all" or record["district"] == selected_district)
        and (selected_cause == "all" or record["cause"] == selected_cause)
        and (selected_object_category == "all" or record["object_category"] == selected_object_category)
    ]

    if not filtered_records:
        notes = _unique_non_empty(list(preload_notes[:2]) + ["После выбранных фильтров не осталось записей для ранжирования территорий."])
        return {
            "has_data": False,
            "title": DECISION_SUPPORT_TITLE,
            "model_description": DECISION_SUPPORT_DESCRIPTION,
            "coverage_display": coverage_display,
            "quality_passport": quality_passport,
            "summary_cards": [],
            "top_territory_label": "-",
            "top_territory_explanation": "Недостаточно данных для ранжирования территорий.",
            "territories": [],
            "feature_cards": feature_cards,
            "weight_profile": weight_profile,
            "historical_validation": empty_historical_validation_payload(weight_profile.get("mode_label") or "Экспертные веса"),
            "notes": notes,
            "geo_summary": geo_summary,
        }

    territories = _build_territory_rows(filtered_records, planning_horizon_days, weight_mode=weight_mode)
    top_territory = territories[0] if territories else None
    historical_validation = build_historical_validation_payload(filtered_records, planning_horizon_days, weight_mode=weight_mode)

    return {
        "has_data": bool(territories),
        "title": DECISION_SUPPORT_TITLE,
        "model_description": DECISION_SUPPORT_DESCRIPTION,
        "coverage_display": coverage_display,
        "quality_passport": quality_passport,
        "summary_cards": _build_summary_cards(territories, weight_profile),
        "top_territory_label": top_territory["label"] if top_territory else "-",
        "top_territory_explanation": _top_territory_lead(top_territory),
        "territories": territories[:MAX_TERRITORIES],
        "feature_cards": feature_cards,
        "weight_profile": weight_profile,
        "historical_validation": historical_validation,
        "notes": _build_risk_notes(feature_cards, preload_notes, weight_profile, historical_validation),
        "geo_summary": geo_summary,
    }



def build_risk_forecast_payload(
    source_tables: Sequence[str],
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    history_window: str,
    forecast_rows: Sequence[Dict[str, Any]],
    geo_prediction: Optional[Dict[str, Any]] = None,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
) -> Dict[str, Any]:
    return build_decision_support_payload(
        source_tables=source_tables,
        selected_district=selected_district,
        selected_cause=selected_cause,
        selected_object_category=selected_object_category,
        history_window=history_window,
        planning_horizon_days=max(1, len(forecast_rows) or 14),
        geo_prediction=geo_prediction,
        weight_mode=weight_mode,
    )



def _build_summary_cards(territories: Sequence[Dict[str, Any]], weight_profile: Dict[str, Any]) -> list[Dict[str, str]]:
    if not territories:
        return []

    component_meta = {item["key"]: item for item in (weight_profile.get("components") or [])}
    cards = [
        {
            "label": "Оценено территорий",
            "value": _format_integer(len(territories)),
            "meta": f"Режим: {weight_profile.get('mode_label') or 'Экспертные веса'}",
            "tone": "sky",
        }
    ]

    tone_map = {
        "fire_frequency": "fire",
        "consequence_severity": "sand",
        "long_arrival_risk": "forest",
        "water_supply_deficit": "sky",
    }
    for key in ["fire_frequency", "consequence_severity", "long_arrival_risk", "water_supply_deficit"]:
        scores = [float(item.get("component_score_map", {}).get(key, {}).get("score", 0.0)) for item in territories]
        meta = component_meta.get(key, {})
        cards.append(
            {
                "label": meta.get("label") or key,
                "value": f"{_format_number(mean(scores))} / 100",
                "meta": f"Базовый вес: {meta.get('weight_display', '0%')} | сельская поправка: {meta.get('rural_weight_display', '0%')}",
                "tone": tone_map.get(key, "sky"),
            }
        )

    cards.append(
        {
            "label": "Калибровка",
            "value": "Готова" if weight_profile.get("calibration_ready") else "Пока нет",
            "meta": "Структура совместима с будущей настройкой весов по историческим окнам",
            "tone": "forest" if weight_profile.get("calibration_ready") else "sand",
        }
    )
    return cards
