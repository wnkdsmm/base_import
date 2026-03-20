from __future__ import annotations

from datetime import datetime
from statistics import mean
from typing import Any, Dict, List, Optional

from app.services.forecast_risk.core import build_decision_support_payload

from .charts import _build_forecast_breakdown_chart, _build_forecast_chart, _build_geo_chart, _build_weekday_chart, _empty_chart_bundle, _get_plotly_bundle
from .constants import FORECAST_DAY_OPTIONS, HISTORY_WINDOW_OPTIONS
from .data import _build_daily_history, _build_forecast_rows, _build_forecasting_table_options, _build_option_catalog, _build_weekday_profile, _collect_forecasting_inputs, _resolve_forecasting_selection, _selected_source_tables, _table_selection_label
from .geo import _build_geo_prediction
from .utils import (
    _apply_history_window,
    _forecast_level_label,
    _forecast_stability_hint,
    _format_datetime,
    _format_float_for_input,
    _format_integer,
    _format_number,
    _format_percent,
    _format_period,
    _format_probability,
    _format_signed_percent,
    _history_window_label,
    _parse_float,
    _parse_forecast_days,
    _parse_history_window,
    _resolve_option_value,
)

SCENARIO_FORECAST_DESCRIPTION = (
    "Это heuristic / scenario forecast: эвристическая сценарная оценка по исторической частоте, сезонности и повторяющимся паттернам. "
    "Блок не обучается как ML-модель и нужен для ориентира по нагрузке, а не для точного обещания числа пожаров по дням."
)
def get_forecasting_page_context(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> Dict[str, Any]:
    try:
        initial_data = get_forecasting_data(
            table_name=table_name,
            district=district,
            cause=cause,
            object_category=object_category,
            temperature=temperature,
            forecast_days=forecast_days,
            history_window=history_window,
        )
    except Exception as exc:
        table_options = _build_forecasting_table_options()
        selected_table = _resolve_forecasting_selection(table_options, table_name)
        days_ahead = _parse_forecast_days(forecast_days)
        selected_history_window = _parse_history_window(history_window)
        initial_data = _empty_forecasting_data(
            table_options,
            selected_table,
            days_ahead,
            temperature,
            selected_history_window,
        )
        initial_data["notes"].append(
            "Страница прогнозирования открыта в безопасном режиме: часть расчета временно отключена из-за внутренней ошибки."
        )
        initial_data["notes"].append(f"Техническая причина: {exc}")
        initial_data["model_description"] = (
            "Heuristic / scenario forecast временно открыт без полного набора расчетов, чтобы интерфейс оставался доступен даже при внутренней ошибке."
        )
    return {
        "generated_at": _format_datetime(datetime.now()),
        "initial_data": initial_data,
        "plotly_js": _get_plotly_bundle(),
        "has_data": bool(initial_data["filters"]["available_tables"]),
    }


def get_forecasting_data(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> Dict[str, Any]:
    table_options = _build_forecasting_table_options()
    selected_table = _resolve_forecasting_selection(table_options, table_name)
    source_tables = _selected_source_tables(table_options, selected_table)
    days_ahead = _parse_forecast_days(forecast_days)
    selected_history_window = _parse_history_window(history_window)
    temperature_value = _parse_float(temperature)

    base_data = _empty_forecasting_data(table_options, selected_table, days_ahead, temperature, selected_history_window)
    if not source_tables:
        base_data["notes"].append("Нет доступных таблиц для прогнозирования.")
        return base_data

    records, metadata_items, preload_notes = _collect_forecasting_inputs(source_tables)
    scoped_records = _apply_history_window(records, selected_history_window)
    option_catalog = _build_option_catalog(scoped_records)
    selected_district = _resolve_option_value(option_catalog["districts"], district)
    selected_cause = _resolve_option_value(option_catalog["causes"], cause)
    selected_object_category = _resolve_option_value(option_catalog["object_categories"], object_category)

    filtered_records = [
        record
        for record in scoped_records
        if (selected_district == "all" or record["district"] == selected_district)
        and (selected_cause == "all" or record["cause"] == selected_cause)
        and (selected_object_category == "all" or record["object_category"] == selected_object_category)
    ]

    daily_history = _build_daily_history(filtered_records)
    forecast_rows = _build_forecast_rows(daily_history, days_ahead, temperature_value)
    weekday_profile = _build_weekday_profile(daily_history)
    history_counts = [float(item["count"]) for item in daily_history]
    recent_counts = history_counts[-28:] if len(history_counts) >= 28 else history_counts
    recent_average = mean(recent_counts) if recent_counts else 0.0
    charts = {
        "daily": _build_forecast_chart(daily_history, forecast_rows),
        "breakdown": _build_forecast_breakdown_chart(forecast_rows, recent_average),
        "weekday": _build_weekday_chart(weekday_profile),
    }
    geo_prediction = _build_geo_prediction(filtered_records, days_ahead)
    charts["geo"] = _build_geo_chart(geo_prediction)
    try:
        risk_prediction = build_decision_support_payload(
            source_tables=source_tables,
            selected_district=selected_district,
            selected_cause=selected_cause,
            selected_object_category=selected_object_category,
            history_window=selected_history_window,
            planning_horizon_days=days_ahead,
            geo_prediction=geo_prediction,
        )
    except Exception as exc:
        risk_prediction = _empty_forecasting_data(
            table_options=table_options,
            selected_table=selected_table,
            forecast_days=days_ahead,
            temperature=temperature,
            history_window=selected_history_window,
        )["risk_prediction"]
        risk_prediction["feature_cards"] = _build_feature_cards(metadata_items)
        risk_prediction["notes"] = [
            "Блок поддержки решений по территориям временно недоступен, поэтому страница показывает только heuristic / scenario forecast.",
            f"Техническая причина: {exc}",
        ]
    notes = preload_notes + _build_notes(
        metadata=metadata_items,
        filtered_records=filtered_records,
        daily_history=daily_history,
        temperature_value=temperature_value,
    )
    features = risk_prediction["feature_cards"] or _build_feature_cards(metadata_items)
    insights = _build_insights(daily_history, forecast_rows, weekday_profile)
    summary = _build_summary(
        selected_table=selected_table,
        selected_district=selected_district,
        selected_cause=selected_cause,
        selected_object_category=selected_object_category,
        temperature_value=temperature_value,
        daily_history=daily_history,
        filtered_records=filtered_records,
        forecast_rows=forecast_rows,
        history_window=selected_history_window,
    )

    return {
        "generated_at": _format_datetime(datetime.now()),
        "has_data": bool(filtered_records),
        "model_description": SCENARIO_FORECAST_DESCRIPTION,
        "summary": summary,
        "features": features,
        "risk_prediction": risk_prediction,
        "insights": insights,
        "charts": charts,
        "forecast_rows": forecast_rows,
        "notes": notes,
        "filters": {
            "table_name": selected_table,
            "district": selected_district,
            "cause": selected_cause,
            "object_category": selected_object_category,
            "temperature": temperature if temperature_value is None else _format_float_for_input(temperature_value),
            "forecast_days": str(days_ahead),
            "history_window": selected_history_window,
            "available_tables": table_options,
            "available_districts": option_catalog["districts"],
            "available_causes": option_catalog["causes"],
            "available_object_categories": option_catalog["object_categories"],
            "available_forecast_days": [{"value": str(option), "label": f"{option} дней"} for option in FORECAST_DAY_OPTIONS],
            "available_history_windows": HISTORY_WINDOW_OPTIONS,
        },
    }

def _empty_forecasting_data(
    table_options: List[Dict[str, str]],
    selected_table: str,
    forecast_days: int,
    temperature: str,
    history_window: str,
) -> Dict[str, Any]:
    return {
        "generated_at": _format_datetime(datetime.now()),
        "has_data": False,
        "model_description": "",
        "summary": {
            "selected_table_label": _table_selection_label(selected_table),
            "slice_label": "Все пожары",
            "history_period_label": "Нет данных",
            "history_window_label": _history_window_label(history_window),
            "fires_count_display": "0",
            "history_days_display": "0",
            "active_days_display": "0",
            "last_observed_date": "-",
            "forecast_days_display": str(forecast_days),
            "predicted_total_display": "0",
            "predicted_average_display": "0",
            "average_probability_display": "0%",
            "historical_average_display": "0",
            "recent_average_display": "0",
            "forecast_vs_recent_display": "0%",
            "forecast_vs_recent_label": "Нет сравнения",
            "active_days_share_display": "0%",
            "peak_forecast_day_display": "-",
            "peak_forecast_value_display": "0",
            "peak_forecast_probability_display": "0%",
            "temperature_scenario_display": temperature.strip() or "Историческая сезонность",
        },
        "features": [],
        "risk_prediction": {
            "has_data": False,
            "title": "Блок поддержки решений: ранжирование территорий",
            "model_description": "Это отдельный блок поддержки решений: он прозрачно раскладывает риск территории на частоту пожаров, тяжесть последствий, долгое прибытие и дефицит водоснабжения.",
            "coverage_display": "0 из 0",
            "quality_passport": {
                "title": "Паспорт качества данных",
                "confidence_score": 0,
                "confidence_score_display": "0 / 100",
                "confidence_label": "Ограниченная",
                "confidence_tone": "fire",
                "validation_label": "Валидация ограничена",
                "validation_summary": "Паспорт качества появится после расчета блока поддержки решений.",
                "table_count_display": "0",
                "used_count_display": "0",
                "partial_count_display": "0",
                "missing_count_display": "0",
                "critical_gaps": [],
                "used_labels": [],
                "partial_labels": [],
                "missing_labels": [],
                "reliability_notes": ["Паспорт качества появится после расчета блока поддержки решений."],
            },
            "summary_cards": [],
            "top_territory_label": "-",
            "top_territory_explanation": "Недостаточно данных для ранжирования территорий.",
            "territories": [],
            "feature_cards": [],
            "weight_profile": {
                "mode": "expert",
                "mode_label": "Экспертные веса",
                "status_label": "Активный профиль",
                "status_tone": "forest",
                "description": "После расчета здесь появится прозрачная схема компонентных весов по территориям.",
                "components": [],
                "available_modes": [],
                "notes": ["Веса и сельская поправка появятся после расчета блока поддержки решений."],
                "calibration_ready": True,
                "calibration_targets": [],
                "calibration_notes": ["После расчета здесь появится заготовка под калибровку весов."],
            },
            "historical_validation": {
                "title": "Черновая историческая проверка ranking",
                "mode_label": "Экспертные веса",
                "has_metrics": False,
                "status_label": "Пока без проверки",
                "status_tone": "fire",
                "summary": "После расчета здесь появится заготовка под историческую проверку ranking.",
                "metric_cards": [
                    {"label": "Окон оценено", "value": "0", "meta": "Нет данных"},
                    {"label": "Top-1 hit", "value": "0%", "meta": "Нет данных"},
                    {"label": "Top-3 capture", "value": "0%", "meta": "Нет данных"},
                    {"label": "Precision high risk", "value": "0%", "meta": "Нет данных"},
                ],
                "notes": ["Метрики появятся автоматически, когда истории станет достаточно."],
                "recent_windows": [],
            },
            "notes": [],
            "geo_summary": {
                "has_coordinates": False,
                "has_map_points": False,
                "compact_message": "В выбранном срезе нет координат, поэтому карта блока поддержки решений сейчас не строится.",
                "model_description": "Если в данных есть координаты, карта блока поддержки решений показывает зоны, где пожары повторялись чаще и ближе по времени.",
                "coverage_display": "0 с координатами",
                "top_zone_label": "-",
                "top_risk_display": "0 / 100",
                "hotspots_count_display": "0",
                "top_explanation": "Нет данных для объяснения зоны риска.",
                "hotspots": [],
                "districts": [],
            },
        },
        "insights": [],
        "charts": {
            "daily": _empty_chart_bundle("История и heuristic / scenario forecast", "Недостаточно данных для построения прогноза."),
            "breakdown": _empty_chart_bundle("Сценарная вероятность пожара по ближайшим дням", "Нет данных для ближайших дней."),
            "weekday": _empty_chart_bundle("В какие дни недели пожары случаются чаще", "Нет данных по дням недели."),
            "geo": _empty_chart_bundle("Карта блока поддержки решений", "В выбранном срезе нет координат, поэтому карта не построена."),
        },
        "forecast_rows": [],
        "notes": [],
        "filters": {
            "table_name": selected_table,
            "district": "all",
            "cause": "all",
            "object_category": "all",
            "temperature": temperature,
            "forecast_days": str(forecast_days),
            "history_window": history_window,
            "available_tables": table_options,
            "available_districts": [{"value": "all", "label": "Все районы"}],
            "available_causes": [{"value": "all", "label": "Все причины"}],
            "available_object_categories": [{"value": "all", "label": "Все категории"}],
            "available_forecast_days": [{"value": str(option), "label": f"{option} дней"} for option in FORECAST_DAY_OPTIONS],
            "available_history_windows": HISTORY_WINDOW_OPTIONS,
        },
    }
def _build_summary(
    selected_table: str,
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    temperature_value: Optional[float],
    daily_history: List[Dict[str, Any]],
    filtered_records: List[Dict[str, Any]],
    forecast_rows: List[Dict[str, Any]],
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
    active_days_share = (active_days / len(daily_history) * 100.0) if daily_history else 0.0
    delta_ratio = ((predicted_average - recent_average) / recent_average) if recent_average > 0 else 0.0
    scenario_label, _scenario_tone = _forecast_level_label(predicted_average, recent_average if recent_average > 0 else historical_average)
    peak_row = max(forecast_rows, key=lambda item: float(item["forecast_value"])) if forecast_rows else None

    slice_parts = []
    if selected_district != "all":
        slice_parts.append(f"район: {selected_district}")
    if selected_cause != "all":
        slice_parts.append(f"причина: {selected_cause}")
    if selected_object_category != "all":
        slice_parts.append(f"категория: {selected_object_category}")

    return {
        "selected_table_label": _table_selection_label(selected_table),
        "slice_label": " | ".join(slice_parts) if slice_parts else "Все пожары выбранной истории",
        "history_period_label": _format_period(history_dates),
        "history_window_label": _history_window_label(history_window),
        "fires_count_display": _format_integer(len(filtered_records)),
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

def _build_feature_cards(metadata: Any) -> List[Dict[str, str]]:
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

def _build_insights(
    daily_history: List[Dict[str, Any]],
    forecast_rows: List[Dict[str, Any]],
    weekday_profile: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
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
                "value": _format_signed_percent(((forecast_average - recent_average) / recent_average) if recent_average > 0 else 0.0),
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
    metadata: Dict[str, Any],
    filtered_records: List[Dict[str, Any]],
    daily_history: List[Dict[str, Any]],
    temperature_value: Optional[float],
) -> List[str]:
    metadata_items = metadata if isinstance(metadata, list) else [metadata]
    metadata_items = [item for item in metadata_items if item]
    notes: List[str] = []

    if len(metadata_items) > 1:
        notes.append(f"Прогноз собран сразу по {len(metadata_items)} таблицам.")

    if not any(item["resolved_columns"].get("date") for item in metadata_items):
        notes.append("В выбранных таблицах не найдена дата возникновения пожара.")
    if not filtered_records:
        notes.append("По выбранным фильтрам нет пожаров в истории. Попробуйте снять часть ограничений.")
    elif len(daily_history) < 14:
        notes.append("История короткая, поэтому прогноз может быть шумным.")

    if temperature_value is not None and not any(item["resolved_columns"].get("temperature") for item in metadata_items):
        notes.append("Температурный сценарий задан, но колонка температуры не найдена ни в одной таблице.")
    if any(not item["resolved_columns"].get("cause") for item in metadata_items):
        notes.append("Не во всех таблицах найдена причина пожара, поэтому этот фильтр работает частично.")
    if any(not item["resolved_columns"].get("object_category") for item in metadata_items):
        notes.append("Не во всех таблицах найдена категория объекта, поэтому этот фильтр работает частично.")

    notes.append("Heuristic / scenario forecast лучше читать как сценарный ориентир по уровню нагрузки и приоритетам, а не как точное обещание числа пожаров в каждый день.")

    return notes




