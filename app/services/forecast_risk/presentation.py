from __future__ import annotations

from typing import Any, List, Sequence

from .types import (
    FeatureCard,
    FeatureSource,
    GeoSummary,
    QualityPassport,
    RiskPresentation,
    RiskProfile,
    RiskScore,
    TopConfidence,
)
from .utils import _format_integer, _scan_columns


def _table_scope_label(count: int) -> str:
    count_display = _format_integer(count)
    if count == 1:
        return f"В {count_display} таблице"
    return f"В {count_display} таблицах"


def _compact_feature_sources(sources: Sequence[FeatureSource]) -> str:
    if not sources:
        return "Не найдена"

    grouped_sources: List[dict[str, Any]] = []
    for item in sources:
        columns = tuple(item.get("columns") or ())
        if not columns:
            continue
        matched_group = next((group for group in grouped_sources if group["columns"] == columns), None)
        if matched_group is None:
            grouped_sources.append({"columns": columns, "tables": [item.get("table_name") or "Таблица"]})
            continue
        matched_group["tables"].append(item.get("table_name") or "Таблица")

    if not grouped_sources:
        return "Не найдена"

    parts: List[str] = []
    for group in grouped_sources[:3]:
        columns_text = ", ".join(group["columns"][:4])
        tables = group["tables"]
        if len(tables) == 1:
            parts.append(f"{tables[0]}: {columns_text}")
        else:
            parts.append(f"{_table_scope_label(len(tables))}: {columns_text}")
    remaining_groups = len(grouped_sources) - len(parts)
    if remaining_groups > 0:
        parts.append(f"ещё {_format_integer(remaining_groups)} набора")
    return "; ".join(parts)


def _build_feature_cards(metadata_items: Sequence[dict[str, Any]]) -> List[FeatureCard]:
    if not metadata_items:
        return []
    feature_config = [
        {
            "label": "Территория и населённый пункт",
            "description": "Нужны для ранжирования сельсоветов и населённых пунктов.",
            "resolved_keys": ["territory_label", "district"],
            "minimum_matches": 1,
        },
        {
            "label": "Удалённость от пожарной части",
            "description": "Используется для оценки логистического риска.",
            "resolved_keys": ["fire_station_distance"],
            "minimum_matches": 1,
        },
        {
            "label": "Время сообщения и прибытия",
            "description": "Помогает оценивать вероятность большого времени прибытия подразделений.",
            "resolved_keys": ["arrival_time", "report_time", "detection_time"],
            "minimum_matches": 2,
        },
        {
            "label": "Наружное водоснабжение",
            "description": "Учитывается как фактор снижения тяжёлых последствий.",
            "resolved_keys": ["water_supply_count", "water_supply_details"],
            "minimum_matches": 1,
        },
        {
            "label": "Тип застройки и объектов",
            "description": "Комбинирует тип населённого пункта, категорию здания и объекта.",
            "resolved_keys": ["settlement_type", "building_category", "object_category"],
            "minimum_matches": 2,
        },
        {
            "label": "Погодные условия",
            "description": "Сейчас как погодный сигнал используется доступная температура.",
            "resolved_keys": ["temperature"],
            "minimum_matches": 1,
        },
        {
            "label": "История пожаров",
            "description": "Дата и причины помогают учитывать повторяемость, сезон и профиль территории.",
            "resolved_keys": ["date", "cause"],
            "minimum_matches": 1,
        },
        {
            "label": "Тяжёлые последствия",
            "description": "Используются последствия, ущерб и пострадавшие для оценки тяжести сценария.",
            "resolved_keys": [
                "consequence",
                "registered_damage",
                "destroyed_buildings",
                "destroyed_area",
                "casualty_flag",
                "injuries",
                "deaths",
            ],
            "minimum_matches": 2,
        },
        {
            "label": "Время суток и отопительный период",
            "description": "Учитываются ночные инциденты, день недели и сезон отопления.",
            "resolved_keys": ["date", "report_time", "detection_time", "heating_type"],
            "minimum_matches": 2,
        },
        {
            "label": "Подъездные пути",
            "description": "Если есть отдельные колонки по дорогам или подъезду, они тоже будут использоваться.",
            "scan_tokens": [["подъезд"], ["дорог"], ["путь"]],
            "minimum_matches": 1,
        },
        {
            "label": "Плотность населения",
            "description": "Нужна для прямой оценки нагрузки на территорию.",
            "scan_tokens": [["плотност", "населен"], ["численност", "населен"]],
            "minimum_matches": 1,
        },
    ]
    total_tables = len(metadata_items)
    cards: List[FeatureCard] = []
    for feature in feature_config:
        full_tables = 0
        partial_tables = 0
        sources: List[FeatureSource] = []
        for item in metadata_items:
            found_columns: List[str] = []
            for key in feature.get("resolved_keys", []):
                column_name = item["resolved_columns"].get(key)
                if column_name:
                    found_columns.append(column_name)
            if feature.get("scan_tokens"):
                found_columns.extend(_scan_columns(item["columns"], feature["scan_tokens"]))
            found_columns = list(dict.fromkeys(column_name for column_name in found_columns if column_name))
            if len(found_columns) >= feature.get("minimum_matches", 1):
                full_tables += 1
            elif found_columns:
                partial_tables += 1
            if found_columns:
                sources.append(
                    {
                        "table_name": item["table_name"],
                        "columns": found_columns[:4],
                    }
                )
        if full_tables == total_tables and total_tables > 0:
            status, status_label = ("used", "Используется")
        elif full_tables > 0 or partial_tables > 0:
            status, status_label = ("partial", f"Частично ({full_tables + partial_tables}/{total_tables})")
        else:
            status, status_label = ("missing", "Не найдена")
        cards.append(
            {
                "label": feature["label"],
                "description": feature["description"],
                "status": status,
                "status_label": status_label,
                "source": _compact_feature_sources(sources),
            }
        )
    return cards


def _build_quality_passport(
    feature_cards: Sequence[FeatureCard],
    metadata_items: Sequence[dict[str, Any]],
) -> QualityPassport:
    used_labels = [item["label"] for item in feature_cards if item.get("status") == "used"]
    partial_labels = [item["label"] for item in feature_cards if item.get("status") == "partial"]
    missing_labels = [item["label"] for item in feature_cards if item.get("status") == "missing"]
    total = len(feature_cards)
    used_count = len(used_labels)
    partial_count = len(partial_labels)
    missing_count = len(missing_labels)
    critical_labels = {
        "Территория и населённый пункт",
        "История пожаров",
        "Время сообщения и прибытия",
        "Наружное водоснабжение",
        "Тяжёлые последствия",
    }
    critical_gaps = [label for label in missing_labels if label in critical_labels]
    if total > 0:
        raw_score = (used_count + partial_count * 0.55) / total * 100.0
    else:
        raw_score = 0.0
    raw_score -= min(len(critical_gaps), 3) * 8.0
    confidence_score = max(0, min(100, int(round(raw_score))))
    if confidence_score >= 80:
        confidence_label = "Высокая"
        confidence_tone = "forest"
        validation_label = "Валидация данных пройдена"
    elif confidence_score >= 60:
        confidence_label = "Рабочая"
        confidence_tone = "sky"
        validation_label = "Валидация данных в основном пройдена"
    elif confidence_score >= 40:
        confidence_label = "Умеренная"
        confidence_tone = "sand"
        validation_label = "Валидация данных частичная"
    else:
        confidence_label = "Ограниченная"
        confidence_tone = "fire"
        validation_label = "Валидация данных ограничена"
    if critical_gaps:
        validation_summary = (
            "Часть критичных групп данных отсутствует, поэтому рекомендации стоит использовать как приоритизацию для проверки, "
            "а не как окончательное решение."
        )
    elif partial_count:
        validation_summary = (
            "Ключевые группы данных в основном найдены, но часть признаков доступна не во всех таблицах. "
            "Выводы пригодны для практической приоритизации, однако их лучше подтверждать локальной проверкой."
        )
    else:
        validation_summary = (
            "Ключевые группы данных найдены, поэтому рекомендации можно использовать как рабочую основу для приоритизации "
            "территорий и профилактических мер."
        )
    reliability_notes = []
    if metadata_items:
        reliability_notes.append(f"Паспорт собран по {_format_integer(len(metadata_items))} таблицам базы.")
    if critical_gaps:
        reliability_notes.append("Критичные пробелы: " + ", ".join(critical_gaps[:3]) + ".")
    elif missing_count:
        reliability_notes.append("Есть непрямые пробелы в данных, но базовые рекомендации по территориям всё ещё формируются.")
    else:
        reliability_notes.append("Критичных пробелов в ключевых группах данных не найдено.")
    return {
        "title": "Паспорт качества данных",
        "confidence_score": confidence_score,
        "confidence_score_display": f"{confidence_score} / 100",
        "confidence_label": confidence_label,
        "confidence_tone": confidence_tone,
        "validation_label": validation_label,
        "validation_summary": validation_summary,
        "table_count_display": _format_integer(len(metadata_items)),
        "used_count_display": _format_integer(used_count),
        "partial_count_display": _format_integer(partial_count),
        "missing_count_display": _format_integer(missing_count),
        "critical_gaps": critical_gaps,
        "used_labels": used_labels,
        "partial_labels": partial_labels,
        "missing_labels": missing_labels,
        "reliability_notes": reliability_notes,
    }


def _build_geo_summary(geo_prediction: dict[str, Any]) -> GeoSummary:
    hotspots = [
        {
            "label": item.get("short_label") or item.get("location_label") or "Зона",
            "risk_display": item.get("risk_display") or "0 / 100",
            "meta": item.get("explanation") or "Нет пояснения по зоне.",
        }
        for item in (geo_prediction.get("hotspots") or [])[:5]
    ]
    districts = [
        {
            "label": item.get("label") or "Район",
            "risk_display": item.get("peak_risk_display") or item.get("avg_risk_display") or "0 / 100",
            "meta": f"зон: {item.get('zones_display', '0')} | пожаров: {item.get('incidents_display', '0')}",
        }
        for item in (geo_prediction.get("districts") or [])[:5]
    ]
    has_coordinates = bool(geo_prediction.get("has_coordinates"))
    has_map_points = bool(geo_prediction.get("points"))
    if not has_coordinates:
        compact_message = "В выбранном срезе нет координат, поэтому карта зон риска сейчас не строится."
    elif not has_map_points:
        compact_message = "Координаты есть, но для устойчивой карты зон пока недостаточно наблюдений."
    else:
        compact_message = ""
    return {
        "has_coordinates": has_coordinates,
        "has_map_points": has_map_points,
        "compact_message": compact_message,
        "model_description": geo_prediction.get("model_description")
        or "Карта блока поддержки решений показывает пространственные зоны внимания для территориального приоритета. Она не заменяет календарь риска по дням.",
        "coverage_display": geo_prediction.get("coverage_display") or "0 с координатами",
        "top_zone_label": geo_prediction.get("top_zone_label") or "-",
        "top_risk_display": geo_prediction.get("top_risk_display") or "0 / 100",
        "hotspots_count_display": geo_prediction.get("hotspots_count_display") or "0",
        "top_explanation": geo_prediction.get("top_explanation") or "Нет данных для объяснения зоны риска.",
        "hotspots": hotspots,
        "districts": districts,
    }


def _build_empty_decision_support_payload(
    *,
    title: str,
    model_description: str,
    coverage_display: str,
    quality_passport: QualityPassport,
    top_confidence: TopConfidence,
    feature_cards: Sequence[FeatureCard],
    weight_profile: RiskProfile,
    historical_validation: dict[str, Any],
    notes: Sequence[str],
    geo_summary: GeoSummary,
    geo_prediction: dict[str, Any],
) -> RiskPresentation:
    return {
        "has_data": False,
        "title": title,
        "model_description": model_description,
        "coverage_display": coverage_display,
        "quality_passport": quality_passport,
        "summary_cards": [],
        "top_territory_label": "-",
        "top_territory_explanation": "Недостаточно данных для ранжирования территорий.",
        "top_territory_confidence_label": top_confidence["label"],
        "top_territory_confidence_score_display": top_confidence["score_display"],
        "top_territory_confidence_tone": top_confidence["tone"],
        "top_territory_confidence_note": top_confidence["note"],
        "territories": [],
        "feature_cards": list(feature_cards),
        "weight_profile": weight_profile,
        "historical_validation": historical_validation,
        "notes": list(notes),
        "geo_summary": geo_summary,
        "geo_prediction": geo_prediction,
    }


def _build_decision_support_payload_response(
    *,
    title: str,
    model_description: str,
    coverage_display: str,
    quality_passport: QualityPassport,
    summary_cards: Sequence[dict[str, Any]],
    top_territory_label: str,
    top_territory_explanation: str,
    top_confidence: TopConfidence,
    territories: Sequence[RiskScore],
    feature_cards: Sequence[FeatureCard],
    weight_profile: RiskProfile,
    historical_validation: dict[str, Any],
    notes: Sequence[str],
    geo_summary: GeoSummary,
    geo_prediction: dict[str, Any],
) -> RiskPresentation:
    return {
        "has_data": bool(territories),
        "title": title,
        "model_description": model_description,
        "coverage_display": coverage_display,
        "quality_passport": quality_passport,
        "summary_cards": list(summary_cards),
        "top_territory_label": top_territory_label,
        "top_territory_explanation": top_territory_explanation,
        "top_territory_confidence_label": top_confidence["label"],
        "top_territory_confidence_score_display": top_confidence["score_display"],
        "top_territory_confidence_tone": top_confidence["tone"],
        "top_territory_confidence_note": top_confidence["note"],
        "territories": list(territories),
        "feature_cards": list(feature_cards),
        "weight_profile": weight_profile,
        "historical_validation": historical_validation,
        "notes": list(notes),
        "geo_summary": geo_summary,
        "geo_prediction": geo_prediction,
    }
