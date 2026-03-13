from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import inspect, text

from app.services.table_options import get_fire_map_table_options, resolve_selected_table
from app.statistics_constants import (
    BUILDING_CAUSE_COLUMN,
    DATE_COLUMN,
    GENERAL_CAUSE_COLUMN,
    OBJECT_CATEGORY_COLUMN,
    OPEN_AREA_CAUSE_COLUMN,
    PLOTLY_PALETTE,
)
from config.db import engine

try:
    import json

    import plotly.graph_objects as go
    from plotly.offline import get_plotlyjs
    from plotly.utils import PlotlyJSONEncoder

    PLOTLY_AVAILABLE = True
except Exception:
    go = None
    get_plotlyjs = None
    PlotlyJSONEncoder = None
    json = None
    PLOTLY_AVAILABLE = False


DISTRICT_COLUMN_CANDIDATES = [
    "Район",
    "Муниципальный район",
    "Муниципальное образование",
    "Административный район",
    "Район выезда подразделения",
    "Район пожара",
    "Территория",
]
TEMPERATURE_COLUMN_CANDIDATES = [
    "Температура",
    "Температура воздуха",
    "Температура воздуха, С",
    "Температура воздуха, C",
    "Температура воздуха, °C",
    "Температура окружающей среды",
]
CAUSE_COLUMN_CANDIDATES = [
    GENERAL_CAUSE_COLUMN,
    OPEN_AREA_CAUSE_COLUMN,
    BUILDING_CAUSE_COLUMN,
    "Причина пожара",
    "Причина",
]
FORECAST_DAY_OPTIONS = [7, 14, 30, 60]
WEEKDAY_LABELS = [\"Пн\", \"Вт\", \"Ср\", \"Чт\", \"Пт\", \"Сб\", \"Вс\"]
MONTH_LABELS = {
    1: \"Янв\",
    2: \"Фев\",
    3: \"Мар\",
    4: \"Апр\",
    5: \"Май\",
    6: \"Июн\",
    7: \"Июл\",
    8: \"Авг\",
    9: \"Сен\",
    10: \"Окт\",
    11: \"Ноя\",
    12: \"Дек\",
}


def get_forecasting_page_context(
    table_name: str = "",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
) -> Dict[str, Any]:
    initial_data = get_forecasting_data(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
    )
    return {
        "generated_at": _format_datetime(datetime.now()),
        "initial_data": initial_data,
        "plotly_js": _get_plotly_bundle(),
        "has_data": bool(initial_data["filters"]["available_tables"]),
    }


def get_forecasting_data(
    table_name: str = "",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
) -> Dict[str, Any]:
    table_options = get_fire_map_table_options()
    selected_table = resolve_selected_table(table_options, table_name)
    days_ahead = _parse_forecast_days(forecast_days)
    temperature_value = _parse_float(temperature)

    base_data = _empty_forecasting_data(table_options, selected_table, days_ahead, temperature)
    if not selected_table:
        base_data["notes"].append("Нет доступных таблиц для прогнозирования.")
        return base_data

    try:
        metadata = _load_table_metadata(selected_table)
    except Exception as exc:
        base_data["notes"].append(str(exc))
        return base_data

    resolved_columns = metadata["resolved_columns"]
    raw_records = _load_forecasting_records(selected_table, resolved_columns)
    option_catalog = _build_option_catalog(raw_records)

    selected_district = _resolve_option_value(option_catalog["districts"], district)
    selected_cause = _resolve_option_value(option_catalog["causes"], cause)
    selected_object_category = _resolve_option_value(option_catalog["object_categories"], object_category)

    filtered_records = [
        record
        for record in raw_records
        if (selected_district == "all" or record["district"] == selected_district)
        and (selected_cause == "all" or record["cause"] == selected_cause)
        and (selected_object_category == "all" or record["object_category"] == selected_object_category)
    ]

    daily_history = _build_daily_history(filtered_records)
    forecast_rows = _build_forecast_rows(daily_history, days_ahead, temperature_value)
    weekday_profile = _build_weekday_profile(daily_history)
    weekly_outlook = _build_weekly_outlook(daily_history, forecast_rows)
    monthly_outlook = _build_monthly_outlook(daily_history, forecast_rows)
    charts = {
        "daily": _build_forecast_chart(daily_history, forecast_rows),
        "weekly": _build_weekly_chart(weekly_outlook),
        "monthly": _build_monthly_chart(monthly_outlook),
        "weekday": _build_weekday_chart(weekday_profile),
    }
    notes = _build_notes(metadata, filtered_records, daily_history, temperature_value, weekly_outlook, monthly_outlook)
    features = _build_feature_cards(metadata)
    insights = _build_insights(forecast_rows, weekday_profile, monthly_outlook)
    summary = _build_summary(
        selected_table=selected_table,
        selected_district=selected_district,
        selected_cause=selected_cause,
        selected_object_category=selected_object_category,
        temperature_value=temperature_value,
        daily_history=daily_history,
        filtered_records=filtered_records,
        forecast_rows=forecast_rows,
        weekly_outlook=weekly_outlook,
        monthly_outlook=monthly_outlook,
    )

    return {
        "generated_at": _format_datetime(datetime.now()),
        "has_data": bool(filtered_records),
        "model_description": (
            "Прогноз строится по дневному ряду пожаров: учитывает сезонность по дню недели и месяцу, "
            "тренд последних недель, а при наличии колонки температуры добавляет температурную поправку. "
            "Ниже дополнительно показаны недельный и месячный сценарии, чтобы оценивать не только отдельные даты, но и общий ритм риска."
        ),
        "summary": summary,
        "features": features,
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
            "available_tables": table_options,
            "available_districts": option_catalog["districts"],
            "available_causes": option_catalog["causes"],
            "available_object_categories": option_catalog["object_categories"],
            "available_forecast_days": [
                {"value": str(option), "label": f"{option} дней"} for option in FORECAST_DAY_OPTIONS
            ],
        },
    }


def _empty_forecasting_data(
    table_options: List[Dict[str, str]],
    selected_table: str,
    forecast_days: int,
    temperature: str,
) -> Dict[str, Any]:
    return {
        "generated_at": _format_datetime(datetime.now()),
        "has_data": False,
        "model_description": "",
        "summary": {
            "selected_table_label": selected_table or "Нет таблицы",
            "slice_label": "Все пожары",
            "history_period_label": "Нет данных",
            "fires_count_display": "0",
            "history_days_display": "0",
            "active_days_display": "0",
            "last_observed_date": "-",
            "forecast_days_display": str(forecast_days),
            "predicted_total_display": "0",
            "predicted_average_display": "0",
            "temperature_scenario_display": temperature.strip() or "Историческая сезонность",
            "weekly_forecast_display": "0",
            "monthly_forecast_display": "0",
        },
        "features": [],
        "insights": [],
        "charts": {
            "daily": _empty_chart_bundle("Прогноз количества пожаров по датам", "Недостаточно данных для построения прогноза."),
            "weekly": _empty_chart_bundle("Недельный прогноз", "Нет данных по неделям."),
            "monthly": _empty_chart_bundle("Месячный сценарий", "Нет данных по месяцам."),
            "weekday": _empty_chart_bundle("Ритм по дням недели", "Нет данных по дням недели."),
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
            "available_tables": table_options,
            "available_districts": [{"value": "all", "label": "Все районы"}],
            "available_causes": [{"value": "all", "label": "Все причины"}],
            "available_object_categories": [{"value": "all", "label": "Все категории"}],
            "available_forecast_days": [{"value": str(option), "label": f"{option} дней"} for option in FORECAST_DAY_OPTIONS],
        },
    }


def _load_table_metadata(table_name: str) -> Dict[str, Any]:
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if table_name not in table_names:
        raise ValueError(f"Таблица '{table_name}' не найдена в базе данных.")

    columns = [column["name"] for column in inspector.get_columns(table_name)]
    resolved_columns = {
        "date": _resolve_column_name(columns, [DATE_COLUMN]),
        "district": _resolve_column_name(columns, DISTRICT_COLUMN_CANDIDATES),
        "temperature": _resolve_column_name(columns, TEMPERATURE_COLUMN_CANDIDATES),
        "cause": _resolve_column_name(columns, CAUSE_COLUMN_CANDIDATES),
        "object_category": _resolve_column_name(columns, [OBJECT_CATEGORY_COLUMN, "Категория объекта пожара"]),
    }
    return {
        "table_name": table_name,
        "columns": columns,
        "resolved_columns": resolved_columns,
    }


def _load_forecasting_records(table_name: str, resolved_columns: Dict[str, str]) -> List[Dict[str, Any]]:
    date_column = resolved_columns["date"]
    if not date_column:
        return []

    select_parts = [f"{_date_expression(date_column)} AS fire_date"]
    if resolved_columns["district"]:
        select_parts.append(f"{_text_expression(resolved_columns['district'])} AS district_value")
    if resolved_columns["cause"]:
        select_parts.append(f"{_text_expression(resolved_columns['cause'])} AS cause_value")
    if resolved_columns["object_category"]:
        select_parts.append(f"{_text_expression(resolved_columns['object_category'])} AS object_category_value")
    if resolved_columns["temperature"]:
        select_parts.append(f"{_numeric_expression_for_column(resolved_columns['temperature'])} AS temperature_value")

    query = text(
        f"""
        SELECT {", ".join(select_parts)}
        FROM {_quote_identifier(table_name)}
        WHERE {_date_expression(date_column)} IS NOT NULL
        ORDER BY fire_date
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query).mappings().all()

    records: List[Dict[str, Any]] = []
    for row in rows:
        fire_date = row.get("fire_date")
        if fire_date is None:
            continue
        records.append(
            {
                "date": fire_date,
                "district": _clean_option_value(row.get("district_value")),
                "cause": _clean_option_value(row.get("cause_value")),
                "object_category": _clean_option_value(row.get("object_category_value")),
                "temperature": _to_float_or_none(row.get("temperature_value")),
            }
        )
    return records


def _build_option_catalog(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, str]]]:
    return {
        "districts": _build_options(records, "district", "Все районы"),
        "causes": _build_options(records, "cause", "Все причины"),
        "object_categories": _build_options(records, "object_category", "Все категории"),
    }


def _build_options(records: List[Dict[str, Any]], key: str, default_label: str) -> List[Dict[str, str]]:
    counter = Counter(record[key] for record in records if record.get(key))
    options = [{"value": "all", "label": default_label}]
    for value, count in sorted(counter.items(), key=lambda item: (-item[1], item[0].lower()))[:200]:
        options.append({"value": value, "label": f"{value} ({count})"})
    return options


def _build_daily_history(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not records:
        return []

    counts_by_date: Dict[date, int] = defaultdict(int)
    temps_by_date: Dict[date, List[float]] = defaultdict(list)
    min_date = records[0]["date"]
    max_date = records[-1]["date"]

    for record in records:
        fire_date = record["date"]
        counts_by_date[fire_date] += 1
        if record["temperature"] is not None:
            temps_by_date[fire_date].append(record["temperature"])

    history: List[Dict[str, Any]] = []
    current = min_date
    while current <= max_date:
        day_temps = temps_by_date.get(current, [])
        history.append(
            {
                "date": current,
                "count": counts_by_date.get(current, 0),
                "avg_temperature": round(mean(day_temps), 2) if day_temps else None,
            }
        )
        current += timedelta(days=1)
    return history


def _build_forecast_rows(
    daily_history: List[Dict[str, Any]],
    forecast_days: int,
    temperature_value: Optional[float],
) -> List[Dict[str, Any]]:
    if not daily_history or forecast_days <= 0:
        return []

    history_counts = [float(item["count"]) for item in daily_history]
    history_dates = [item["date"] for item in daily_history]
    overall_average = mean(history_counts) if history_counts else 0.0
    recent_counts = history_counts[-28:] if len(history_counts) >= 28 else history_counts
    previous_counts = history_counts[-56:-28] if len(history_counts) >= 56 else history_counts[:-len(recent_counts)] if len(history_counts) > len(recent_counts) else []
    recent_average = mean(recent_counts) if recent_counts else overall_average
    previous_average = mean(previous_counts) if previous_counts else recent_average
    if previous_average > 0:
        trend_factor = 1 + max(-0.35, min(0.35, (recent_average - previous_average) / previous_average))
    else:
        trend_factor = 1.0

    weekday_average: Dict[int, float] = {}
    for weekday in range(7):
        weekday_values = [float(item["count"]) for item in daily_history if item["date"].weekday() == weekday]
        weekday_average[weekday] = mean(weekday_values) if weekday_values else overall_average

    month_average: Dict[int, float] = {}
    seasonal_temperature_by_month: Dict[int, float] = {}
    overall_temperature_values = [item["avg_temperature"] for item in daily_history if item["avg_temperature"] is not None]
    overall_temperature_average = mean(overall_temperature_values) if overall_temperature_values else None

    for month in range(1, 13):
        month_values = [float(item["count"]) for item in daily_history if item["date"].month == month]
        month_average[month] = mean(month_values) if month_values else overall_average
        month_temps = [item["avg_temperature"] for item in daily_history if item["date"].month == month and item["avg_temperature"] is not None]
        if month_temps:
            seasonal_temperature_by_month[month] = mean(month_temps)

    temperature_pairs = [
        (float(item["avg_temperature"]), float(item["count"]))
        for item in daily_history
        if item["avg_temperature"] is not None
    ]
    temperature_slope = _compute_temperature_slope(temperature_pairs)
    volatility = pstdev(recent_counts) if len(recent_counts) > 1 else pstdev(history_counts) if len(history_counts) > 1 else 0.0

    rolling_series = list(history_counts)
    forecast_rows: List[Dict[str, Any]] = []
    last_observed_date = history_dates[-1]

    for step in range(1, forecast_days + 1):
        target_date = last_observed_date + timedelta(days=step)
        weekday_component = weekday_average.get(target_date.weekday(), recent_average)
        month_component = month_average.get(target_date.month, overall_average)
        rolling_average = mean(rolling_series[-14:]) if rolling_series else overall_average
        base_forecast = 0.45 * weekday_component + 0.35 * month_component + 0.20 * rolling_average

        temperature_for_day = temperature_value
        if temperature_for_day is None:
            temperature_for_day = seasonal_temperature_by_month.get(target_date.month, overall_temperature_average)

        if (
            temperature_for_day is not None
            and overall_temperature_average is not None
            and temperature_slope is not None
        ):
            temperature_adjusted = overall_average + temperature_slope * (temperature_for_day - overall_temperature_average)
            estimate = base_forecast * 0.72 + max(0.0, temperature_adjusted) * 0.28
        else:
            estimate = base_forecast

        estimate = max(0.0, estimate * trend_factor)
        lower_bound = max(0.0, estimate - volatility)
        upper_bound = estimate + volatility
        rounded_estimate = round(estimate, 2)

        forecast_rows.append(
            {
                "date": target_date.isoformat(),
                "date_display": target_date.strftime("%d.%m.%Y"),
                "forecast_value": rounded_estimate,
                "forecast_value_display": _format_number(rounded_estimate),
                "lower_bound": round(lower_bound, 2),
                "lower_bound_display": _format_number(lower_bound),
                "upper_bound": round(upper_bound, 2),
                "upper_bound_display": _format_number(upper_bound),
                "temperature_display": _format_number(temperature_for_day) if temperature_for_day is not None else "ист. ср.",
            }
        )
        rolling_series.append(estimate)

    return forecast_rows


def _build_weekday_profile(daily_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for weekday in range(7):
        values = [float(item["count"]) for item in daily_history if item["date"].weekday() == weekday]
        avg_value = mean(values) if values else 0.0
        total_value = sum(values)
        items.append(
            {
                "weekday": weekday,
                "label": WEEKDAY_LABELS[weekday],
                "avg_value": round(avg_value, 2),
                "avg_display": _format_number(avg_value),
                "total_value": round(total_value, 2),
                "total_display": _format_number(total_value),
            }
        )
    return items


def _build_weekly_outlook(daily_history: List[Dict[str, Any]], forecast_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    history_buckets: Dict[date, float] = defaultdict(float)
    for item in daily_history:
        history_buckets[_week_start(item["date"])] += float(item["count"])

    forecast_buckets: Dict[date, float] = defaultdict(float)
    for row in forecast_rows:
        row_date = _parse_iso_date(row["date"])
        forecast_buckets[_week_start(row_date)] += float(row["forecast_value"])

    keys = sorted(set(history_buckets) | set(forecast_buckets))
    if not keys:
        return []

    visible_keys = keys[-10:] if len(keys) > 10 else keys
    last_history_week = _week_start(daily_history[-1]["date"]) if daily_history else None
    items = []
    for bucket_start in visible_keys:
        items.append(
            {
                "week_start": bucket_start,
                "label": bucket_start.strftime("%d.%m"),
                "actual": round(history_buckets.get(bucket_start, 0.0), 2),
                "forecast": round(forecast_buckets.get(bucket_start, 0.0), 2),
                "actual_display": _format_number(history_buckets.get(bucket_start, 0.0)),
                "forecast_display": _format_number(forecast_buckets.get(bucket_start, 0.0)),
                "is_future": bool(last_history_week and bucket_start > last_history_week),
            }
        )
    return items


def _build_monthly_outlook(daily_history: List[Dict[str, Any]], forecast_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    history_by_month_of_year: Dict[int, List[float]] = defaultdict(list)
    if daily_history:
        month_totals: Dict[Tuple[int, int], float] = defaultdict(float)
        for item in daily_history:
            key = (item["date"].year, item["date"].month)
            month_totals[key] += float(item["count"])
        for (year_value, month_value), total_value in month_totals.items():
            history_by_month_of_year[month_value].append(total_value)

    forecast_by_month: Dict[Tuple[int, int], float] = defaultdict(float)
    for row in forecast_rows:
        row_date = _parse_iso_date(row["date"])
        forecast_by_month[(row_date.year, row_date.month)] += float(row["forecast_value"])

    items = []
    for year_value, month_value in sorted(forecast_by_month):
        baseline = mean(history_by_month_of_year.get(month_value, [])) if history_by_month_of_year.get(month_value) else 0.0
        items.append(
            {
                "month_key": f"{year_value:04d}-{month_value:02d}",
                "label": f"{MONTH_LABELS[month_value]} {year_value}",
                "forecast": round(forecast_by_month[(year_value, month_value)], 2),
                "forecast_display": _format_number(forecast_by_month[(year_value, month_value)]),
                "baseline": round(baseline, 2),
                "baseline_display": _format_number(baseline),
            }
        )
    return items[:4]


def _build_summary(
    selected_table: str,
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    temperature_value: Optional[float],
    daily_history: List[Dict[str, Any]],
    filtered_records: List[Dict[str, Any]],
    forecast_rows: List[Dict[str, Any]],
    weekly_outlook: List[Dict[str, Any]],
    monthly_outlook: List[Dict[str, Any]],
) -> Dict[str, str]:
    history_dates = [item["date"] for item in daily_history]
    active_days = sum(1 for item in daily_history if item["count"] > 0)
    predicted_total = sum(item["forecast_value"] for item in forecast_rows)
    predicted_average = predicted_total / len(forecast_rows) if forecast_rows else 0.0
    weekly_forecast = sum(item["forecast"] for item in weekly_outlook)
    monthly_forecast = monthly_outlook[0]["forecast"] if monthly_outlook else 0.0

    slice_parts = []
    if selected_district != "all":
        slice_parts.append(f"район: {selected_district}")
    if selected_cause != "all":
        slice_parts.append(f"причина: {selected_cause}")
    if selected_object_category != "all":
        slice_parts.append(f"категория: {selected_object_category}")

    return {
        "selected_table_label": selected_table,
        "slice_label": " | ".join(slice_parts) if slice_parts else "Все пожары выбранной таблицы",
        "history_period_label": _format_period(history_dates),
        "fires_count_display": _format_integer(len(filtered_records)),
        "history_days_display": _format_integer(len(daily_history)),
        "active_days_display": _format_integer(active_days),
        "last_observed_date": history_dates[-1].strftime("%d.%m.%Y") if history_dates else "-",
        "forecast_days_display": _format_integer(len(forecast_rows)),
        "predicted_total_display": _format_number(predicted_total),
        "predicted_average_display": _format_number(predicted_average),
        "temperature_scenario_display": (
            f"{_format_number(temperature_value)} °C" if temperature_value is not None else "Историческая сезонность"
        ),
        "weekly_forecast_display": _format_number(weekly_forecast),
        "monthly_forecast_display": _format_number(monthly_forecast),
    }


def _build_feature_cards(metadata: Dict[str, Any]) -> List[Dict[str, str]]:
    resolved_columns = metadata["resolved_columns"]
    feature_config = [
        ("date", "Дата возникновения пожара", "Нужна для построения дневного временного ряда."),
        ("district", "Район", "Работает как сценарный фильтр для выбранной территории."),
        ("temperature", "Температура", "Используется для температурной поправки в прогнозе."),
        ("cause", "Причина", "Работает как сценарный фильтр по типу причины."),
        ("object_category", "Категория объекта", "Фильтрует прогноз под выбранный тип объекта."),
    ]
    cards = []
    for key, label, description in feature_config:
        source_column = resolved_columns.get(key) or "Не найдена"
        status = "used" if resolved_columns.get(key) else "missing"
        cards.append(
            {
                "label": label,
                "status": status,
                "status_label": "Используется" if status == "used" else "Не найдена",
                "source": source_column,
                "description": description,
            }
        )
    return cards


def _build_insights(
    forecast_rows: List[Dict[str, Any]],
    weekday_profile: List[Dict[str, Any]],
    monthly_outlook: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    insights = []
    if forecast_rows:
        peak_row = max(forecast_rows, key=lambda item: float(item["forecast_value"]))
        insights.append(
            {
                "label": "Пиковый день прогноза",
                "value": peak_row["date_display"],
                "meta": f"ожидается {peak_row['forecast_value_display']} пожара",
                "tone": "fire",
            }
        )
    if weekday_profile:
        peak_weekday = max(weekday_profile, key=lambda item: float(item["avg_value"]))
        insights.append(
            {
                "label": "Самый активный день недели",
                "value": peak_weekday["label"],
                "meta": f"среднее {peak_weekday['avg_display']} пожара",
                "tone": "sky",
            }
        )
    if monthly_outlook:
        peak_month = max(monthly_outlook, key=lambda item: float(item["forecast"]))
        insights.append(
            {
                "label": "Сильнейший месяц сценария",
                "value": peak_month["label"],
                "meta": f"прогноз {peak_month['forecast_display']} пожара",
                "tone": "forest",
            }
        )
    return insights


def _build_notes(
    metadata: Dict[str, Any],
    filtered_records: List[Dict[str, Any]],
    daily_history: List[Dict[str, Any]],
    temperature_value: Optional[float],
    weekly_outlook: List[Dict[str, Any]],
    monthly_outlook: List[Dict[str, Any]],
) -> List[str]:
    notes: List[str] = []
    resolved_columns = metadata["resolved_columns"]
    if not resolved_columns.get("date"):
        notes.append("В таблице не найдена колонка с датой возникновения пожара, поэтому прогноз не построен.")
    for key, label in (
        ("district", "района"),
        ("cause", "причины"),
        ("object_category", "категории объекта"),
        ("temperature", "температуры"),
    ):
        if not resolved_columns.get(key):
            notes.append(f"Колонка для {label} не найдена. Эта часть модели отключена.")

    if filtered_records and len(daily_history) < 14:
        notes.append("История слишком короткая: прогноз строится по очень малой выборке и может быть шумным.")
    elif not filtered_records:
        notes.append("По выбранным фильтрам нет пожаров в истории. Попробуйте снять часть ограничений.")

    if temperature_value is not None and not resolved_columns.get("temperature"):
        notes.append("Вы указали температурный сценарий, но колонка температуры в таблице не найдена.")
    if weekly_outlook:
        notes.append("Недельный график суммирует историю и прогноз по календарным неделям, чтобы было легче оценить нагрузку на ближайшие недели.")
    if monthly_outlook:
        notes.append("Месячный сценарий сравнивает прогноз будущих месяцев с типичным историческим уровнем для того же месяца года.")
    if filtered_records:
        notes.append(
            "Район, причина и категория объекта работают как сценарные фильтры: прогноз считается по выбранному поднабору пожаров."
        )
    return notes


def _build_forecast_chart(daily_history: List[Dict[str, Any]], forecast_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    title = "Прогноз количества пожаров по датам"
    if not daily_history:
        return {
            "title": title,
            "plotly": _build_empty_plotly("Нет данных для исторического ряда."),
            "empty_message": "Нет данных для исторического ряда.",
        }

    if not PLOTLY_AVAILABLE:
        return {
            "title": title,
            "plotly": {"data": [], "layout": {}, "config": {"responsive": True}},
            "empty_message": "Plotly не найден в окружении. Таблица прогноза ниже остается доступной.",
        }

    history_x = [item["date"].isoformat() for item in daily_history]
    history_y = [item["count"] for item in daily_history]
    forecast_x = [item["date"] for item in forecast_rows]
    forecast_y = [item["forecast_value"] for item in forecast_rows]
    lower_y = [item["lower_bound"] for item in forecast_rows]
    upper_y = [item["upper_bound"] for item in forecast_rows]

    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=history_x,
            y=history_y,
            mode="lines",
            name="История",
            line=dict(color=PLOTLY_PALETTE["fire"], width=3),
            hovertemplate="<b>%{x}</b><br>Пожаров: %{y}<extra></extra>",
        )
    )

    if forecast_rows:
        figure.add_trace(
            go.Scatter(
                x=forecast_x,
                y=upper_y,
                mode="lines",
                line=dict(color="rgba(45,108,143,0)"),
                hoverinfo="skip",
                showlegend=False,
            )
        )
        figure.add_trace(
            go.Scatter(
                x=forecast_x,
                y=lower_y,
                mode="lines",
                line=dict(color="rgba(45,108,143,0)"),
                fill="tonexty",
                fillcolor="rgba(45, 108, 143, 0.16)",
                hoverinfo="skip",
                name="Диапазон",
            )
        )
        figure.add_trace(
            go.Scatter(
                x=forecast_x,
                y=forecast_y,
                mode="lines+markers",
                name="Прогноз",
                line=dict(color=PLOTLY_PALETTE["sky"], width=3, dash="dash"),
                marker=dict(size=7, color=PLOTLY_PALETTE["sky_soft"]),
                hovertemplate="<b>%{x}</b><br>Прогноз: %{y}<extra></extra>",
            )
        )

    figure.update_layout(
        height=420,
        showlegend=True,
        paper_bgcolor="rgba(255,255,255,0)",
        plot_bgcolor="rgba(255,255,255,0)",
        font={"family": 'Bahnschrift, "Segoe UI", "Trebuchet MS", sans-serif', "color": PLOTLY_PALETTE["ink"]},
        margin={"l": 52, "r": 24, "t": 24, "b": 50},
        xaxis={
            "type": "date",
            "showgrid": False,
            "zeroline": False,
        },
        yaxis={
            "title": "Количество пожаров",
            "gridcolor": PLOTLY_PALETTE["grid"],
            "zeroline": False,
        },
        legend={"orientation": "h", "y": 1.12, "x": 0},
        hoverlabel={"bgcolor": "#fffaf5", "font": {"color": PLOTLY_PALETTE["ink"]}},
    )

    return {
        "title": title,
        "plotly": _figure_to_dict(figure),
        "empty_message": "",
    }


def _build_weekly_chart(weekly_outlook: List[Dict[str, Any]]) -> Dict[str, Any]:
    title = "Недельный прогноз"
    if not weekly_outlook:
        return _empty_chart_bundle(title, "Нет данных по неделям.")
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "График недоступен без Plotly.", use_plotly=False)

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=[item["label"] for item in weekly_outlook],
            y=[item["actual"] for item in weekly_outlook],
            name="История",
            marker=dict(color=PLOTLY_PALETTE["sand"]),
            hovertemplate="<b>%{x}</b><br>История: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=[item["label"] for item in weekly_outlook],
            y=[item["forecast"] for item in weekly_outlook],
            name="Прогноз",
            marker=dict(color=PLOTLY_PALETTE["sky"]),
            hovertemplate="<b>%{x}</b><br>Прогноз: %{y}<extra></extra>",
        )
    )
    figure.update_layout(**_plotly_layout("Пожаров за неделю", height=340))
    figure.update_layout(barmode="group", legend={"orientation": "h", "y": 1.12, "x": 0})
    return {"title": title, "plotly": _figure_to_dict(figure), "empty_message": ""}


def _build_monthly_chart(monthly_outlook: List[Dict[str, Any]]) -> Dict[str, Any]:
    title = "Месячный сценарий"
    if not monthly_outlook:
        return _empty_chart_bundle(title, "Нет данных по месяцам.")
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "График недоступен без Plotly.", use_plotly=False)

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=[item["label"] for item in monthly_outlook],
            y=[item["forecast"] for item in monthly_outlook],
            name="Прогноз",
            marker=dict(color=PLOTLY_PALETTE["forest"]),
            hovertemplate="<b>%{x}</b><br>Прогноз: %{y}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=[item["label"] for item in monthly_outlook],
            y=[item["baseline"] for item in monthly_outlook],
            name="Ист. норма",
            mode="lines+markers",
            line=dict(color=PLOTLY_PALETTE["fire"], width=3),
            marker=dict(size=7, color=PLOTLY_PALETTE["fire_soft"]),
            hovertemplate="<b>%{x}</b><br>Ист. норма: %{y}<extra></extra>",
        )
    )
    figure.update_layout(**_plotly_layout("Пожаров в месяц", height=340))
    figure.update_layout(legend={"orientation": "h", "y": 1.12, "x": 0})
    return {"title": title, "plotly": _figure_to_dict(figure), "empty_message": ""}


def _build_weekday_chart(weekday_profile: List[Dict[str, Any]]) -> Dict[str, Any]:
    title = "Ритм по дням недели"
    if not weekday_profile:
        return _empty_chart_bundle(title, "Нет данных по дням недели.")
    if not PLOTLY_AVAILABLE:
        return _empty_chart_bundle(title, "График недоступен без Plotly.", use_plotly=False)

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=[item["label"] for item in weekday_profile],
            y=[item["avg_value"] for item in weekday_profile],
            marker=dict(color=[
                PLOTLY_PALETTE["fire_soft"],
                PLOTLY_PALETTE["sand"],
                PLOTLY_PALETTE["sand_soft"],
                PLOTLY_PALETTE["sky_soft"],
                PLOTLY_PALETTE["sky"],
                PLOTLY_PALETTE["forest_soft"],
                PLOTLY_PALETTE["forest"],
            ]),
            hovertemplate="<b>%{x}</b><br>Среднее: %{y}<extra></extra>",
        )
    )
    figure.update_layout(**_plotly_layout("Среднее пожаров", height=340))
    return {"title": title, "plotly": _figure_to_dict(figure), "empty_message": ""}


def _empty_chart_bundle(title: str, message: str, use_plotly: bool = True) -> Dict[str, Any]:
    plotly_payload = _build_empty_plotly(message) if use_plotly else {"data": [], "layout": {}, "config": {"responsive": True}}
    return {"title": title, "plotly": plotly_payload, "empty_message": message}


def _plotly_layout(yaxis_title: str, height: int = 340) -> Dict[str, Any]:
    return {
        "height": height,
        "showlegend": True,
        "paper_bgcolor": "rgba(255,255,255,0)",
        "plot_bgcolor": "rgba(255,255,255,0)",
        "font": {"family": 'Bahnschrift, "Segoe UI", "Trebuchet MS", sans-serif', "color": PLOTLY_PALETTE["ink"]},
        "margin": {"l": 52, "r": 24, "t": 24, "b": 48},
        "xaxis": {"showgrid": False, "zeroline": False},
        "yaxis": {"title": yaxis_title, "gridcolor": PLOTLY_PALETTE["grid"], "zeroline": False},
        "hoverlabel": {"bgcolor": "#fffaf5", "font": {"color": PLOTLY_PALETTE["ink"]}},
    }


def _build_empty_plotly(message: str) -> Dict[str, Any]:
    if not PLOTLY_AVAILABLE:
        return {"data": [], "layout": {}, "config": {"responsive": True}, "empty_message": message}

    figure = go.Figure()
    figure.update_layout(
        height=320,
        paper_bgcolor="rgba(255,255,255,0)",
        plot_bgcolor="rgba(255,255,255,0)",
        margin={"l": 20, "r": 20, "t": 20, "b": 20},
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[
            {
                "text": message,
                "x": 0.5,
                "y": 0.5,
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 16, "color": "#7b6a5a"},
            }
        ],
    )
    payload = _figure_to_dict(figure)
    payload["empty_message"] = message
    return payload


def _figure_to_dict(figure: Any) -> Dict[str, Any]:
    if not PLOTLY_AVAILABLE or json is None or PlotlyJSONEncoder is None:
        return {"data": [], "layout": {}, "config": {"responsive": True}}

    payload = json.loads(json.dumps(figure, cls=PlotlyJSONEncoder))
    if isinstance(payload.get("layout"), dict):
        payload["layout"].pop("template", None)
    payload["config"] = {
        "responsive": True,
        "displaylogo": False,
        "modeBarButtonsToRemove": [
            "lasso2d",
            "select2d",
            "autoScale2d",
            "toggleSpikelines",
        ],
    }
    return payload


def _get_plotly_bundle() -> str:
    if not PLOTLY_AVAILABLE or get_plotlyjs is None:
        return ""
    return get_plotlyjs()


def _resolve_column_name(columns: Sequence[str], candidates: Sequence[str]) -> str:
    if not columns:
        return ""

    normalized_columns = {column: _normalize_match_text(column) for column in columns}
    normalized_candidates = [_normalize_match_text(candidate) for candidate in candidates if candidate]

    for candidate in candidates:
        if candidate in columns:
            return candidate

    best_match = ""
    best_score = -1
    for column_name, normalized_column in normalized_columns.items():
        for normalized_candidate in normalized_candidates:
            if normalized_column == normalized_candidate:
                return column_name

            score = 0
            if normalized_candidate in normalized_column or normalized_column in normalized_candidate:
                score = min(len(normalized_candidate), len(normalized_column))
            else:
                candidate_tokens = {token for token in normalized_candidate.split(" ") if token}
                column_tokens = {token for token in normalized_column.split(" ") if token}
                common_tokens = candidate_tokens & column_tokens
                if common_tokens:
                    score = sum(len(token) for token in common_tokens)

            if score > best_score:
                best_score = score
                best_match = column_name

    return best_match if best_score >= 8 else ""


def _resolve_option_value(options: List[Dict[str, str]], selected_value: str) -> str:
    available_values = {option["value"] for option in options}
    return selected_value if selected_value in available_values else "all"


def _clean_option_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_forecast_days(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = 14
    return parsed if parsed in FORECAST_DAY_OPTIONS else 14


def _parse_float(value: str) -> Optional[float]:
    if value is None:
        return None
    normalized = str(value).strip().replace(",", ".")
    if not normalized:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def _to_float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compute_temperature_slope(pairs: List[tuple[float, float]]) -> Optional[float]:
    if len(pairs) < 5:
        return None
    temperatures = [pair[0] for pair in pairs]
    fire_counts = [pair[1] for pair in pairs]
    avg_temperature = mean(temperatures)
    avg_count = mean(fire_counts)
    variance = sum((value - avg_temperature) ** 2 for value in temperatures)
    if variance <= 0:
        return None
    covariance = sum((temp - avg_temperature) * (count - avg_count) for temp, count in pairs)
    return covariance / variance


def _week_start(value: date) -> date:
    return value - timedelta(days=value.weekday())


def _parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _format_period(values: Sequence[date]) -> str:
    if not values:
        return "Нет данных"
    ordered = sorted(values)
    return f"{ordered[0].strftime('%d.%m.%Y')} - {ordered[-1].strftime('%d.%m.%Y')}"


def _format_integer(value: Any) -> str:
    try:
        return f"{int(round(float(value))):,}".replace(",", " ")
    except (TypeError, ValueError):
        return "0"


def _format_number(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "0"
    if abs(numeric - round(numeric)) < 1e-9:
        return f"{int(round(numeric)):,}".replace(",", " ")
    return f"{numeric:,.1f}".replace(",", " ").replace(".", ",")


def _format_float_for_input(value: float) -> str:
    text_value = f"{value:.2f}".rstrip("0").rstrip(".")
    return text_value.replace(",", ".")


def _format_datetime(value: datetime) -> str:
    return value.strftime("%d.%m.%Y %H:%M")


def _quote_identifier(identifier: str) -> str:
    return engine.dialect.identifier_preparer.quote(identifier)


def _normalize_match_text(value: str) -> str:
    normalized = str(value).lower().replace("ё", "е")
    normalized = " ".join(normalized.replace("/", " ").replace("-", " ").split())
    return normalized


def _text_expression(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    return f"NULLIF(TRIM(CAST({column_sql} AS TEXT)), '')"


def _numeric_expression_for_column(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    cleaned = f"NULLIF(REPLACE(REPLACE(REPLACE(CAST({column_sql} AS TEXT), ' ', ''), ',', '.'), CHR(160), ''), '')"
    return f"CASE WHEN {cleaned} ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN ({cleaned})::double precision ELSE NULL END"


def _date_expression(column_name: str) -> str:
    column_sql = _quote_identifier(column_name)
    text_value = f"TRIM(CAST({column_sql} AS TEXT))"
    return (
        "CASE "
        f"WHEN {text_value} ~ '^[0-9]{{2}}\\.[0-9]{{2}}\\.[0-9]{{4}}$' THEN TO_DATE({text_value}, 'DD.MM.YYYY') "
        f"WHEN {text_value} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' THEN TO_DATE(SUBSTRING({text_value} FROM 1 FOR 10), 'YYYY-MM-DD') "
        f"WHEN {text_value} ~ '^[0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}}' THEN TO_DATE(SUBSTRING({text_value} FROM 1 FOR 10), 'YYYY/MM/DD') "
        "ELSE NULL END"
    )










