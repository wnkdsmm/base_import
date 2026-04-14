from __future__ import annotations

from statistics import mean
from typing import Any, Optional

from .bootstrap import _build_slice_label
from .selection import _table_selection_label
from .types import (
    ForecastingDailyHistoryRow,
    ForecastingFeatureCard,
    ForecastingForecastRow,
    ForecastingTableMetadata,
    ForecastingTemperatureQuality,
    ForecastingWeekdayProfileRow,
)
from .utils import (
    _forecast_level_label,
    _forecast_stability_hint,
    _format_integer,
    _format_number,
    _format_percent,
    _format_period,
    _format_probability,
    _format_signed_percent,
    _history_window_label,
)


# intentionally separate from access_points/presentation.py::_build_summary and
# ml_model/training/presentation_training.py::_build_summary:
# forecasting summary is scenario-level (time-series probability) and lightweight.
def _build_summary(
    selected_table: str,
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    temperature_value: Optional[float],
    daily_history: list[ForecastingDailyHistoryRow],
    filtered_records_count: int,
    forecast_rows: list[ForecastingForecastRow],
    history_window: str,
) -> Dict[str, str]:
    history_dates = [item["date"] for item in daily_history]
    history_counts = [float(item["count"]) for item in daily_history]
    active_days = sum(1 for item in daily_history if item["count"] > 0)
    predicted_total = sum(item["forecast_value"] for item in forecast_rows)
    predicted_average = predicted_total / len(forecast_rows) if forecast_rows else 0.0
    average_probability = mean(float(item.get("fire_probability", 0.0)) for item in forecast_rows) if forecast_rows else 0.0
    historical_average = mean(history_counts) if history_counts else 0.0
    recent_counts = history_counts[-28:] if len(history_counts) >= 28 else history_counts
    recent_average = mean(recent_counts) if recent_counts else historical_average
    active_days_share = active_days / len(daily_history) * 100.0 if daily_history else 0.0
    delta_ratio = (predicted_average - recent_average) / recent_average if recent_average > 0 else 0.0
    scenario_label, _scenario_tone = _forecast_level_label(
        predicted_average,
        recent_average if recent_average > 0 else historical_average,
    )
    peak_row = max(forecast_rows, key=lambda item: float(item["forecast_value"])) if forecast_rows else None
    hero_summary = (
        f"РџРёРєРѕРІР°СЏ РґР°С‚Р° СЃС†РµРЅР°СЂРёСЏ: {peak_row.get('date_display', '-')} вЂ” РІРµСЂРѕСЏС‚РЅРѕСЃС‚СЊ РїРѕР¶Р°СЂР° {peak_row.get('fire_probability_display', '0%')}. "
        f"РЎСЂРµРґРЅСЏСЏ РІРµСЂРѕСЏС‚РЅРѕСЃС‚СЊ РїРѕ РІС‹Р±СЂР°РЅРЅРѕРјСѓ РіРѕСЂРёР·РѕРЅС‚Сѓ: {_format_probability(average_probability)}."
        if peak_row
        else "РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РєСЂР°С‚РєРёР№ РІС‹РІРѕРґ РїРѕ РґР°С‚Р°Рј, РіРґРµ СЃС†РµРЅР°СЂРёР№ РІС‹РіР»СЏРґРёС‚ РЅР°РїСЂСЏР¶С‘РЅРЅРµРµ."
    )
    return {
        "selected_table_label": _table_selection_label(selected_table),
        "slice_label": _build_slice_label(selected_district, selected_cause, selected_object_category),
        "hero_summary": hero_summary,
        "history_period_label": _format_period(history_dates),
        "history_window_label": _history_window_label(history_window),
        "fires_count_display": _format_integer(filtered_records_count),
        "history_days_display": _format_integer(len(daily_history)),
        "active_days_display": _format_integer(active_days),
        "active_days_share_display": _format_percent(active_days_share),
        "last_observed_date": history_dates[-1].strftime("%d.%m.%Y") if history_dates else "-",
        "forecast_days_display": _format_integer(len(forecast_rows)),
        "predicted_total_display": _format_number(predicted_total),
        "predicted_average_display": _format_number(predicted_average),
        "average_probability_display": _format_probability(average_probability),
        "historical_average_display": _format_number(historical_average),
        "recent_average_display": _format_number(recent_average),
        "forecast_vs_recent_display": _format_signed_percent(delta_ratio),
        "forecast_vs_recent_label": scenario_label,
        "peak_forecast_day_display": peak_row["date_display"] if peak_row else "-",
        "peak_forecast_value_display": peak_row["forecast_value_display"] if peak_row else "0",
        "peak_forecast_probability_display": peak_row["fire_probability_display"] if peak_row else "0%",
        "temperature_scenario_display": f"{_format_number(temperature_value)} В°C" if temperature_value is not None else "РСЃС‚РѕСЂРёС‡РµСЃРєР°СЏ СЃРµР·РѕРЅРЅРѕСЃС‚СЊ",
    }


def _build_feature_cards(metadata: Any) -> list[dict[str, str]]:
    metadata_items = metadata if isinstance(metadata, list) else [metadata]
    metadata_items = [item for item in metadata_items if item]
    if not metadata_items:
        return []
    total_tables = len(metadata_items)
    feature_config = [
        ("date", "Р”Р°С‚Р° РІРѕР·РЅРёРєРЅРѕРІРµРЅРёСЏ РїРѕР¶Р°СЂР°", "РќСѓР¶РЅР° РґР»СЏ РїРѕСЃС‚СЂРѕРµРЅРёСЏ РґРЅРµРІРЅРѕРіРѕ РІСЂРµРјРµРЅРЅРѕРіРѕ СЂСЏРґР°."),
        ("temperature", "РўРµРјРїРµСЂР°С‚СѓСЂР°", "РСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РґР»СЏ С‚РµРјРїРµСЂР°С‚СѓСЂРЅРѕР№ РїРѕРїСЂР°РІРєРё, РµСЃР»Рё РєРѕР»РѕРЅРєР° РЅР°Р№РґРµРЅР°."),
        ("cause", "РџСЂРёС‡РёРЅР°", "РџРѕР·РІРѕР»СЏРµС‚ СЃС‚СЂРѕРёС‚СЊ СЃС†РµРЅР°СЂРёР№ РїРѕ РєРѕРЅРєСЂРµС‚РЅРѕР№ РїСЂРёС‡РёРЅРµ РїРѕР¶Р°СЂР°."),
        ("object_category", "РљР°С‚РµРіРѕСЂРёСЏ РѕР±СЉРµРєС‚Р°", "РџРѕР·РІРѕР»СЏРµС‚ СЃС‚СЂРѕРёС‚СЊ СЃС†РµРЅР°СЂРёР№ РїРѕ С‚РёРїСѓ РѕР±СЉРµРєС‚Р°."),
    ]
    cards = []
    for key, label, description in feature_config:
        sources = []
        found = 0
        for item in metadata_items:
            source_column = item["resolved_columns"].get(key)
            if source_column:
                found += 1
                sources.append(f"{item['table_name']}: {source_column}")
        if found == total_tables:
            status = "used"
            status_label = "РСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ"
        elif found > 0:
            status = "partial"
            status_label = f"Р§Р°СЃС‚РёС‡РЅРѕ ({found}/{total_tables})"
        else:
            status = "missing"
            status_label = "РќРµ РЅР°Р№РґРµРЅР°"
        cards.append(
            {
                "label": label,
                "status": status,
                "status_label": status_label,
                "source": "; ".join(sources[:3]) if sources else "РќРµ РЅР°Р№РґРµРЅР°",
                "description": description,
            }
        )
    return cards


def _build_feature_cards_with_quality(
    metadata: Any,
    temperature_quality: Optional[ForecastingTemperatureQuality] = None,
) -> list[ForecastingFeatureCard]:
    metadata_items = metadata if isinstance(metadata, list) else [metadata]
    metadata_items = [item for item in metadata_items if item]
    if not metadata_items:
        return []
    total_tables = len(metadata_items)
    feature_config = [
        ("date", "Р”Р°С‚Р° РІРѕР·РЅРёРєРЅРѕРІРµРЅРёСЏ РїРѕР¶Р°СЂР°", "РќСѓР¶РЅР° РґР»СЏ РїРѕСЃС‚СЂРѕРµРЅРёСЏ РґРЅРµРІРЅРѕРіРѕ РІСЂРµРјРµРЅРЅРѕРіРѕ СЂСЏРґР°."),
        ("temperature", "РўРµРјРїРµСЂР°С‚СѓСЂР°", "РСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РґР»СЏ С‚РµРјРїРµСЂР°С‚СѓСЂРЅРѕР№ РїРѕРїСЂР°РІРєРё, РµСЃР»Рё РєРѕР»РѕРЅРєР° РЅР°Р№РґРµРЅР°."),
        ("cause", "РџСЂРёС‡РёРЅР°", "РџРѕР·РІРѕР»СЏРµС‚ СЃС‚СЂРѕРёС‚СЊ СЃС†РµРЅР°СЂРёР№ РїРѕ РєРѕРЅРєСЂРµС‚РЅРѕР№ РїСЂРёС‡РёРЅРµ РїРѕР¶Р°СЂР°."),
        ("object_category", "РљР°С‚РµРіРѕСЂРёСЏ РѕР±СЉРµРєС‚Р°", "РџРѕР·РІРѕР»СЏРµС‚ СЃС‚СЂРѕРёС‚СЊ СЃС†РµРЅР°СЂРёР№ РїРѕ С‚РёРїСѓ РѕР±СЉРµРєС‚Р°."),
    ]
    cards: list[ForecastingFeatureCard] = []
    for key, label, base_description in feature_config:
        description = base_description
        sources: List[str] = []
        found = 0
        quality_items: list[ForecastingTemperatureQuality] = []
        for item in metadata_items:
            source_column = (item.get("resolved_columns") or {}).get(key)
            if not source_column:
                continue
            found += 1
            quality = (item.get("column_quality") or {}).get(key) or {} if key == "temperature" else {}
            coverage_suffix = ""
            if key == "temperature" and temperature_quality is None:
                non_null_days = int(quality.get("non_null_days", 0) or 0)
                total_days = int(quality.get("total_days", 0) or 0)
                coverage_value = float(quality.get("coverage", 0.0) or 0.0)
                coverage_suffix = f" | РїРѕРєСЂС‹С‚РёРµ: {non_null_days}/{total_days} РґРЅРµР№ ({_format_percent(coverage_value * 100.0)})"
            if key == "temperature":
                quality_items.append(quality)
            sources.append(f"{item['table_name']}: {source_column}{coverage_suffix}")
        coverage_display = None
        quality_status = None
        quality_label = None
        usable = found == total_tables and found > 0
        if key == "temperature" and found > 0:
            non_null_days = sum(int(item.get("non_null_days", 0) or 0) for item in quality_items)
            total_days = sum(int(item.get("total_days", 0) or 0) for item in quality_items)
            coverage_value = float(non_null_days) / float(total_days) if total_days > 0 else 0.0
            coverage_display = f"{non_null_days}/{total_days} РґРЅРµР№ ({_format_percent(coverage_value * 100.0)})"
            usable = all(bool(item.get("usable")) for item in quality_items) and len(quality_items) == found
            quality_status = "good" if usable else "missing" if non_null_days <= 0 else "sparse"
            quality_label = "Р”РѕСЃС‚Р°С‚РѕС‡РЅРѕРµ РїРѕРєСЂС‹С‚РёРµ" if usable else "РќРµС‚ РёР·РјРµСЂРµРЅРёР№" if non_null_days <= 0 else "РќРёР·РєРѕРµ РїРѕРєСЂС‹С‚РёРµ"
            if not usable:
                description = "РљРѕР»РѕРЅРєР° С‚РµРјРїРµСЂР°С‚СѓСЂС‹ РЅР°Р№РґРµРЅР°, РЅРѕ РїРѕРєСЂС‹С‚РёРµ РЅРёР·РєРѕРµ: С‚РµРјРїРµСЂР°С‚СѓСЂРЅС‹Р№ РїСЂРёР·РЅР°Рє РЅРµР»СЊР·СЏ СЃС‡РёС‚Р°С‚СЊ РЅР°РґС‘Р¶РЅС‹Рј РґР»СЏ ML Рё С‚РµРјРїРµСЂР°С‚СѓСЂРЅРѕР№ РїРѕРїСЂР°РІРєРё."
        if key == "temperature" and found > 0 and temperature_quality is not None:
            non_null_days = int(temperature_quality.get("non_null_days", 0) or 0)
            total_days = int(temperature_quality.get("total_days", 0) or 0)
            coverage_value = float(temperature_quality.get("coverage", 0.0) or 0.0)
            coverage_display = f"{non_null_days}/{total_days} РґРЅРµР№ ({_format_percent(coverage_value * 100.0)})"
            usable = bool(temperature_quality.get("usable")) and found == total_tables
            quality_status = str(temperature_quality.get("quality_key") or ("missing" if non_null_days <= 0 else "sparse"))
            quality_label = str(temperature_quality.get("quality_label") or ("РќРµС‚ РёР·РјРµСЂРµРЅРёР№" if non_null_days <= 0 else "РќРёР·РєРѕРµ РїРѕРєСЂС‹С‚РёРµ"))
            if not usable:
                description = "РљРѕР»РѕРЅРєР° С‚РµРјРїРµСЂР°С‚СѓСЂС‹ РЅР°Р№РґРµРЅР°, РЅРѕ РїРѕРєСЂС‹С‚РёРµ РЅРёР·РєРѕРµ: С‚РµРјРїРµСЂР°С‚СѓСЂРЅС‹Р№ РїСЂРёР·РЅР°Рє РЅРµР»СЊР·СЏ СЃС‡РёС‚Р°С‚СЊ РЅР°РґС‘Р¶РЅС‹Рј РґР»СЏ ML Рё С‚РµРјРїРµСЂР°С‚СѓСЂРЅРѕР№ РїРѕРїСЂР°РІРєРё."
            if sources:
                base_sources = [source.split(" | ", 1)[0] for source in sources[:3]]
                sources = [f"{'; '.join(base_sources)} | РїРѕРєСЂС‹С‚РёРµ РїРѕ РґРЅРµРІРЅРѕР№ РёСЃС‚РѕСЂРёРё: {coverage_display}"]
        if found == 0:
            status = "missing"
            status_label = "РќРµ РЅР°Р№РґРµРЅР°"
            usable = False
        elif key == "temperature" and quality_label is not None:
            status = "used" if usable and found == total_tables else "partial"
            status_label = f"{quality_label} ({coverage_display})"
        elif found == total_tables:
            status = "used"
            status_label = "РСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ"
        else:
            status = "partial"
            status_label = f"Р§Р°СЃС‚РёС‡РЅРѕ ({found}/{total_tables})"
        cards.append(
            {
                "label": label,
                "status": status,
                "status_label": status_label,
                "source": "; ".join(sources[:3]) if sources else "РќРµ РЅР°Р№РґРµРЅР°",
                "description": description,
                "quality_status": quality_status,
                "quality_label": quality_label,
                "coverage_display": coverage_display,
                "usable": usable,
            }
        )
    return cards


def _build_insights(
    daily_history: list[ForecastingDailyHistoryRow],
    forecast_rows: list[ForecastingForecastRow],
    weekday_profile: list[ForecastingWeekdayProfileRow],
) -> list[dict[str, str]]:
    insights = []
    if forecast_rows:
        peak_row = max(forecast_rows, key=lambda item: float(item["fire_probability"]))
        insights.append(
            {
                "label": "РЎР°РјС‹Р№ СЂРёСЃРєРѕРІР°РЅРЅС‹Р№ РґРµРЅСЊ",
                "value": peak_row["date_display"],
                "meta": f"РІРµСЂРѕСЏС‚РЅРѕСЃС‚СЊ РїРѕР¶Р°СЂР° {peak_row['fire_probability_display']}",
                "tone": "fire",
            }
        )
        average_probability = mean(float(item.get("fire_probability", 0.0)) for item in forecast_rows)
        insights.append(
            {
                "label": "РЎСЂРµРґРЅСЏСЏ РІРµСЂРѕСЏС‚РЅРѕСЃС‚СЊ РїРѕР¶Р°СЂР°",
                "value": _format_probability(average_probability),
                "meta": f"РІ РґРµРЅСЊ РЅР° Р±Р»РёР¶Р°Р№С€РёРµ {len(forecast_rows)} РґРЅРµР№",
                "tone": "forest",
            }
        )
    if daily_history and forecast_rows:
        recent_values = [float(item["count"]) for item in (daily_history[-28:] if len(daily_history) >= 28 else daily_history)]
        recent_average = mean(recent_values) if recent_values else 0.0
        forecast_average = mean(float(item["forecast_value"]) for item in forecast_rows)
        insights.append(
            {
                "label": "РЎСЂР°РІРЅРµРЅРёРµ СЃ РЅРµРґР°РІРЅРёРј СѓСЂРѕРІРЅРµРј",
                "value": _format_signed_percent((forecast_average - recent_average) / recent_average if recent_average > 0 else 0.0),
                "meta": f"{_forecast_level_label(forecast_average, recent_average)[0]} РѕС‚РЅРѕСЃРёС‚РµР»СЊРЅРѕ РїРѕСЃР»РµРґРЅРёС… 4 РЅРµРґРµР»СЊ",
                "tone": "sky",
            }
        )
    if weekday_profile:
        peak_weekday = max(weekday_profile, key=lambda item: float(item["avg_value"]))
        insights.append(
            {
                "label": "РЎР°РјС‹Р№ Р°РєС‚РёРІРЅС‹Р№ РґРµРЅСЊ РЅРµРґРµР»Рё",
                "value": peak_weekday["label"],
                "meta": f"РІ РёСЃС‚РѕСЂРёРё РІ СЃСЂРµРґРЅРµРј {peak_weekday['avg_display']} РїРѕР¶Р°СЂР°",
                "tone": "sand",
            }
        )
    stability_label, stability_meta = _forecast_stability_hint(daily_history)
    insights.append(
        {
            "label": "РќР°РґРµР¶РЅРѕСЃС‚СЊ РѕС†РµРЅРєРё",
            "value": stability_label,
            "meta": stability_meta,
            "tone": "sky",
        }
    )
    return insights[:4]


def _build_notes(
    metadata: ForecastingTableMetadata | list[ForecastingTableMetadata],  # one-off
    filtered_records_count: int,
    daily_history: list[ForecastingDailyHistoryRow],
    temperature_value: Optional[float],
) -> list[str]:
    metadata_items = metadata if isinstance(metadata, list) else [metadata]
    metadata_items = [item for item in metadata_items if item]
    notes: List[str] = []
    if len(metadata_items) > 1:
        notes.append(f"РџСЂРѕРіРЅРѕР· СЃРѕР±СЂР°РЅ СЃСЂР°Р·Сѓ РїРѕ {len(metadata_items)} С‚Р°Р±Р»РёС†Р°Рј.")
    if not any(item["resolved_columns"].get("date") for item in metadata_items):
        notes.append("Р’ РІС‹Р±СЂР°РЅРЅС‹С… С‚Р°Р±Р»РёС†Р°С… РЅРµ РЅР°Р№РґРµРЅР° РґР°С‚Р° РІРѕР·РЅРёРєРЅРѕРІРµРЅРёСЏ РїРѕР¶Р°СЂР°.")
    if filtered_records_count <= 0:
        notes.append("РџРѕ РІС‹Р±СЂР°РЅРЅС‹Рј С„РёР»СЊС‚СЂР°Рј РЅРµС‚ РїРѕР¶Р°СЂРѕРІ РІ РёСЃС‚РѕСЂРёРё. РџРѕРїСЂРѕР±СѓР№С‚Рµ СЃРЅСЏС‚СЊ С‡Р°СЃС‚СЊ РѕРіСЂР°РЅРёС‡РµРЅРёР№.")
    elif len(daily_history) < 14:
        notes.append("РСЃС‚РѕСЂРёСЏ РєРѕСЂРѕС‚РєР°СЏ, РїРѕСЌС‚РѕРјСѓ СЃС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР· РјРѕР¶РµС‚ Р±С‹С‚СЊ РјРµРЅРµРµ СѓСЃС‚РѕР№С‡РёРІС‹Рј.")
    if temperature_value is not None and not any(item["resolved_columns"].get("temperature") for item in metadata_items):
        notes.append("РўРµРјРїРµСЂР°С‚СѓСЂРЅС‹Р№ СЃС†РµРЅР°СЂРёР№ Р·Р°РґР°РЅ, РЅРѕ РєРѕР»РѕРЅРєР° С‚РµРјРїРµСЂР°С‚СѓСЂС‹ РЅРµ РЅР°Р№РґРµРЅР° РЅРё РІ РѕРґРЅРѕР№ С‚Р°Р±Р»РёС†Рµ.")
    if any(not item["resolved_columns"].get("cause") for item in metadata_items):
        notes.append("РќРµ РІРѕ РІСЃРµС… С‚Р°Р±Р»РёС†Р°С… РЅР°Р№РґРµРЅР° РїСЂРёС‡РёРЅР° РїРѕР¶Р°СЂР°, РїРѕСЌС‚РѕРјСѓ СЌС‚РѕС‚ С„РёР»СЊС‚СЂ СЂР°Р±РѕС‚Р°РµС‚ С‡Р°СЃС‚РёС‡РЅРѕ.")
    if any(not item["resolved_columns"].get("object_category") for item in metadata_items):
        notes.append("РќРµ РІРѕ РІСЃРµС… С‚Р°Р±Р»РёС†Р°С… РЅР°Р№РґРµРЅР° РєР°С‚РµРіРѕСЂРёСЏ РѕР±СЉРµРєС‚Р°, РїРѕСЌС‚РѕРјСѓ СЌС‚РѕС‚ С„РёР»СЊС‚СЂ СЂР°Р±РѕС‚Р°РµС‚ С‡Р°СЃС‚РёС‡РЅРѕ.")
    notes.append(
        "РЎС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР· Р»СѓС‡С€Рµ С‡РёС‚Р°С‚СЊ РєР°Рє РєР°Р»РµРЅРґР°СЂСЊ РІРµСЂРѕСЏС‚РЅРѕСЃС‚Рё РїРѕР¶Р°СЂР° РїРѕ РґРЅСЏРј, Р° РЅРµ РєР°Рє С‚РѕС‡РЅРѕРµ РѕР±РµС‰Р°РЅРёРµ С‡РёСЃР»Р° РїРѕР¶Р°СЂРѕРІ. Р•СЃР»Рё РЅСѓР¶РµРЅ РїСЂРѕРіРЅРѕР· РєРѕР»РёС‡РµСЃС‚РІР°, РёСЃРїРѕР»СЊР·СѓР№С‚Рµ ML-РїСЂРѕРіРЅРѕР·."
    )
    return notes
