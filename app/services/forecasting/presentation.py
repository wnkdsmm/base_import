from __future__ import annotations

from statistics import mean
from typing import Any, Dict, List, Optional

from app.services.shared.formatting import format_percent as _format_percent

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
        f"Пиковая дата сценария: {peak_row.get('date_display', '-')} — вероятность пожара {peak_row.get('fire_probability_display', '0%')}. "
        f"Средняя вероятность по выбранному горизонту: {_format_probability(average_probability)}."
        if peak_row
        else "После расчета здесь появится краткий вывод по датам, где сценарий выглядит напряжённее."
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
        "temperature_scenario_display": f"{_format_number(temperature_value)} °C" if temperature_value is not None else "Историческая сезонность",
    }


def _build_feature_cards(metadata: Any) -> list[dict[str, str]]:
    metadata_items = metadata if isinstance(metadata, list) else [metadata]
    metadata_items = [item for item in metadata_items if item]
    if not metadata_items:
        return []
    total_tables = len(metadata_items)
    feature_config = [
        ("date", "Дата возникновения пожара", "Нужна для построения дневного временного ряда."),
        ("temperature", "Температура", "Используется для температурной поправки, если колонка найдена."),
        ("cause", "Причина", "Позволяет строить сценарий по конкретной причине пожара."),
        ("object_category", "Категория объекта", "Позволяет строить сценарий по типу объекта."),
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
            status_label = "Используется"
        elif found > 0:
            status = "partial"
            status_label = f"Частично ({found}/{total_tables})"
        else:
            status = "missing"
            status_label = "Не найдена"
        cards.append(
            {
                "label": label,
                "status": status,
                "status_label": status_label,
                "source": "; ".join(sources[:3]) if sources else "Не найдена",
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
        ("date", "Дата возникновения пожара", "Нужна для построения дневного временного ряда."),
        ("temperature", "Температура", "Используется для температурной поправки, если колонка найдена."),
        ("cause", "Причина", "Позволяет строить сценарий по конкретной причине пожара."),
        ("object_category", "Категория объекта", "Позволяет строить сценарий по типу объекта."),
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
                coverage_suffix = f" | покрытие: {non_null_days}/{total_days} дней ({_format_percent(coverage_value * 100.0)})"
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
            coverage_display = f"{non_null_days}/{total_days} дней ({_format_percent(coverage_value * 100.0)})"
            usable = all(bool(item.get("usable")) for item in quality_items) and len(quality_items) == found
            quality_status = "good" if usable else "missing" if non_null_days <= 0 else "sparse"
            quality_label = "Достаточное покрытие" if usable else "Нет измерений" if non_null_days <= 0 else "Низкое покрытие"
            if not usable:
                description = "Колонка температуры найдена, но покрытие низкое: температурный признак нельзя считать надёжным для ML и температурной поправки."
        if key == "temperature" and found > 0 and temperature_quality is not None:
            non_null_days = int(temperature_quality.get("non_null_days", 0) or 0)
            total_days = int(temperature_quality.get("total_days", 0) or 0)
            coverage_value = float(temperature_quality.get("coverage", 0.0) or 0.0)
            coverage_display = f"{non_null_days}/{total_days} дней ({_format_percent(coverage_value * 100.0)})"
            usable = bool(temperature_quality.get("usable")) and found == total_tables
            quality_status = str(temperature_quality.get("quality_key") or ("missing" if non_null_days <= 0 else "sparse"))
            quality_label = str(temperature_quality.get("quality_label") or ("Нет измерений" if non_null_days <= 0 else "Низкое покрытие"))
            if not usable:
                description = "Колонка температуры найдена, но покрытие низкое: температурный признак нельзя считать надёжным для ML и температурной поправки."
            if sources:
                base_sources = [source.split(" | ", 1)[0] for source in sources[:3]]
                sources = [f"{'; '.join(base_sources)} | покрытие по дневной истории: {coverage_display}"]
        if found == 0:
            status = "missing"
            status_label = "Не найдена"
            usable = False
        elif key == "temperature" and quality_label is not None:
            status = "used" if usable and found == total_tables else "partial"
            status_label = f"{quality_label} ({coverage_display})"
        elif found == total_tables:
            status = "used"
            status_label = "Используется"
        else:
            status = "partial"
            status_label = f"Частично ({found}/{total_tables})"
        cards.append(
            {
                "label": label,
                "status": status,
                "status_label": status_label,
                "source": "; ".join(sources[:3]) if sources else "Не найдена",
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
                "label": "Самый рискованный день",
                "value": peak_row["date_display"],
                "meta": f"вероятность пожара {peak_row['fire_probability_display']}",
                "tone": "fire",
            }
        )
        average_probability = mean(float(item.get("fire_probability", 0.0)) for item in forecast_rows)
        insights.append(
            {
                "label": "Средняя вероятность пожара",
                "value": _format_probability(average_probability),
                "meta": f"в день на ближайшие {len(forecast_rows)} дней",
                "tone": "forest",
            }
        )
    if daily_history and forecast_rows:
        recent_values = [float(item["count"]) for item in (daily_history[-28:] if len(daily_history) >= 28 else daily_history)]
        recent_average = mean(recent_values) if recent_values else 0.0
        forecast_average = mean(float(item["forecast_value"]) for item in forecast_rows)
        insights.append(
            {
                "label": "Сравнение с недавним уровнем",
                "value": _format_signed_percent((forecast_average - recent_average) / recent_average if recent_average > 0 else 0.0),
                "meta": f"{_forecast_level_label(forecast_average, recent_average)[0]} относительно последних 4 недель",
                "tone": "sky",
            }
        )
    if weekday_profile:
        peak_weekday = max(weekday_profile, key=lambda item: float(item["avg_value"]))
        insights.append(
            {
                "label": "Самый активный день недели",
                "value": peak_weekday["label"],
                "meta": f"в истории в среднем {peak_weekday['avg_display']} пожара",
                "tone": "sand",
            }
        )
    stability_label, stability_meta = _forecast_stability_hint(daily_history)
    insights.append(
        {
            "label": "Надежность оценки",
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
        notes.append(f"Прогноз собран сразу по {len(metadata_items)} таблицам.")
    if not any(item["resolved_columns"].get("date") for item in metadata_items):
        notes.append("В выбранных таблицах не найдена дата возникновения пожара.")
    if filtered_records_count <= 0:
        notes.append("По выбранным фильтрам нет пожаров в истории. Попробуйте снять часть ограничений.")
    elif len(daily_history) < 14:
        notes.append("История короткая, поэтому сценарный прогноз может быть менее устойчивым.")
    if temperature_value is not None and not any(item["resolved_columns"].get("temperature") for item in metadata_items):
        notes.append("Температурный сценарий задан, но колонка температуры не найдена ни в одной таблице.")
    if any(not item["resolved_columns"].get("cause") for item in metadata_items):
        notes.append("Не во всех таблицах найдена причина пожара, поэтому этот фильтр работает частично.")
    if any(not item["resolved_columns"].get("object_category") for item in metadata_items):
        notes.append("Не во всех таблицах найдена категория объекта, поэтому этот фильтр работает частично.")
    notes.append(
        "Сценарный прогноз лучше читать как календарь вероятности пожара по дням, а не как точное обещание числа пожаров. Если нужен прогноз количества, используйте ML-прогноз."
    )
    return notes
