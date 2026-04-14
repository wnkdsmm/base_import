from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta
from statistics import mean, pstdev
from typing import Dict, List, Optional, Tuple

from .constants import MONTH_LABELS, WEEKDAY_LABELS
from .utils import (
    _clamp,
    _compute_temperature_slope,
    _forecast_level_label,
    _format_count_range,
    _format_number,
    _format_probability,
    _format_signed_percent,
    _parse_iso_date,
    _relative_delta_text,
    _week_start,
)


def _build_option_catalog(records: List[Dict[str, object]]) -> Dict[str, List[Dict[str, str]]]:
    return {
        "districts": _build_options(records, "district", "Р’СЃРµ СЂР°Р№РѕРЅС‹"),
        "causes": _build_options(records, "cause", "Р’СЃРµ РїСЂРёС‡РёРЅС‹"),
        "object_categories": _build_options(records, "object_category", "Р’СЃРµ РєР°С‚РµРіРѕСЂРёРё"),
    }


def _build_options(records: List[Dict[str, object]], key: str, default_label: str) -> List[Dict[str, str]]:
    counter = Counter(record[key] for record in records if record.get(key))
    options = [{"value": "all", "label": default_label}]
    for value, count in sorted(counter.items(), key=lambda item: (-item[1], str(item[0]).lower()))[:200]:
        options.append({"value": str(value), "label": f"{value} ({count})"})
    return options


def _build_daily_history(records: List[Dict[str, object]]) -> List[Dict[str, object]]:
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

    history: List[Dict[str, object]] = []
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


def _build_forecast_history_stats(daily_history: List[Dict[str, object]]) -> Dict[str, object]:
    history_counts = [float(item["count"]) for item in daily_history]
    history_dates = [item["date"] for item in daily_history]
    history_events = [1.0 if value > 0 else 0.0 for value in history_counts]
    overall_average = mean(history_counts) if history_counts else 0.0
    recent_counts = history_counts[-28:] if len(history_counts) >= 28 else history_counts
    recent_events = history_events[-len(recent_counts) :] if recent_counts else []
    recent_positive_counts = [value for value in recent_counts if value > 0]
    overall_positive_counts = [value for value in history_counts if value > 0]
    very_recent_counts = history_counts[-14:] if len(history_counts) >= 14 else history_counts
    previous_counts = (
        history_counts[-56:-28]
        if len(history_counts) >= 56
        else history_counts[: -len(recent_counts)]
        if len(history_counts) > len(recent_counts)
        else []
    )
    recent_average = mean(recent_counts) if recent_counts else overall_average
    very_recent_average = mean(very_recent_counts) if very_recent_counts else recent_average
    previous_average = mean(previous_counts) if previous_counts else recent_average
    recent_event_rate = mean(recent_events) if recent_events else (mean(history_events) if history_events else 0.0)
    recent_positive_average = (
        mean(recent_positive_counts)
        if recent_positive_counts
        else (mean(overall_positive_counts) if overall_positive_counts else max(1.0, overall_average))
    )
    trend_ratio = _clamp(
        ((very_recent_average - previous_average) / previous_average) if previous_average > 0 else 0.0,
        -0.22,
        0.22,
    )
    base_recent_level = 0.65 * very_recent_average + 0.35 * recent_average if recent_counts else overall_average
    volatility = pstdev(recent_counts) if len(recent_counts) > 1 else pstdev(history_counts) if len(history_counts) > 1 else 0.0
    recent_peak = max(recent_counts) if recent_counts else max(history_counts)
    robust_ceiling = max(
        recent_peak * 1.35,
        base_recent_level * 2.4 + max(1.0, volatility),
        overall_average + 3.5 * max(1.0, volatility),
    )
    return {
        "history_dates": history_dates,
        "history_counts": history_counts,
        "overall_average": overall_average,
        "recent_counts": recent_counts,
        "recent_average": recent_average,
        "recent_event_rate": recent_event_rate,
        "recent_positive_average": recent_positive_average,
        "trend_ratio": trend_ratio,
        "base_recent_level": base_recent_level,
        "volatility": volatility,
        "robust_ceiling": robust_ceiling,
    }


def _build_weekday_forecast_factors(
    daily_history: List[Dict[str, object]],
    *,
    overall_average: float,
    recent_event_rate: float,
    recent_positive_average: float,
) -> Tuple[Dict[int, float], Dict[int, float], Dict[int, float]]:
    weekday_factor: Dict[int, float] = {}
    weekday_event_rate: Dict[int, float] = {}
    weekday_positive_average: Dict[int, float] = {}
    for weekday in range(7):
        weekday_values = [float(item["count"]) for item in daily_history if item["date"].weekday() == weekday]
        weekday_avg = mean(weekday_values) if weekday_values else overall_average
        raw_factor = (weekday_avg / overall_average) if overall_average > 0 else 1.0
        reliability = min(1.0, len(weekday_values) / 12.0)
        weekday_factor[weekday] = 1.0 + (raw_factor - 1.0) * reliability * 0.7
        weekday_event_values = [1.0 if value > 0 else 0.0 for value in weekday_values]
        weekday_positive_values = [value for value in weekday_values if value > 0]
        weekday_event_rate[weekday] = mean(weekday_event_values) if weekday_event_values else recent_event_rate
        weekday_positive_average[weekday] = mean(weekday_positive_values) if weekday_positive_values else recent_positive_average
    return weekday_factor, weekday_event_rate, weekday_positive_average


def _build_month_forecast_factors(
    daily_history: List[Dict[str, object]],
    *,
    overall_average: float,
    recent_event_rate: float,
    recent_positive_average: float,
) -> Tuple[Dict[int, float], Dict[int, float], Dict[int, float], Dict[int, float], Optional[float]]:
    month_factor: Dict[int, float] = {}
    month_event_rate: Dict[int, float] = {}
    month_positive_average: Dict[int, float] = {}
    seasonal_temperature_by_month: Dict[int, float] = {}
    overall_temperature_values = [item["avg_temperature"] for item in daily_history if item["avg_temperature"] is not None]
    overall_temperature_average = mean(overall_temperature_values) if overall_temperature_values else None

    for month in range(1, 13):
        month_values = [float(item["count"]) for item in daily_history if item["date"].month == month]
        month_avg = mean(month_values) if month_values else overall_average
        raw_factor = (month_avg / overall_average) if overall_average > 0 else 1.0
        reliability = min(1.0, len(month_values) / 45.0)
        month_factor[month] = 1.0 + (raw_factor - 1.0) * reliability * 0.55
        month_event_values = [1.0 if value > 0 else 0.0 for value in month_values]
        month_positive_values = [value for value in month_values if value > 0]
        month_event_rate[month] = mean(month_event_values) if month_event_values else recent_event_rate
        month_positive_average[month] = mean(month_positive_values) if month_positive_values else recent_positive_average
        month_temps = [
            item["avg_temperature"]
            for item in daily_history
            if item["date"].month == month and item["avg_temperature"] is not None
        ]
        if month_temps:
            seasonal_temperature_by_month[month] = mean(month_temps)
    return month_factor, month_event_rate, month_positive_average, seasonal_temperature_by_month, overall_temperature_average


def _forecast_event_probability(
    target_date: date,
    expected_count: float,
    *,
    weekday_event_rate: Dict[int, float],
    month_event_rate: Dict[int, float],
    weekday_positive_average: Dict[int, float],
    month_positive_average: Dict[int, float],
    recent_event_rate: float,
    recent_positive_average: float,
) -> float:
    numeric_count = max(0.0, float(expected_count))
    if numeric_count <= 0:
        return 0.0

    weekday_base_probability = weekday_event_rate.get(target_date.weekday(), recent_event_rate)
    month_base_probability = month_event_rate.get(target_date.month, recent_event_rate)
    base_probability = _clamp(
        0.55 * weekday_base_probability + 0.20 * month_base_probability + 0.25 * recent_event_rate,
        0.01,
        0.98,
    )

    weekday_positive_level = weekday_positive_average.get(target_date.weekday(), recent_positive_average)
    month_positive_level = month_positive_average.get(target_date.month, recent_positive_average)
    positive_count_scale = max(
        1.0,
        0.55 * weekday_positive_level + 0.20 * month_positive_level + 0.25 * recent_positive_average,
    )
    count_implied_probability = _clamp(numeric_count / positive_count_scale, 0.0, 0.995)
    return _clamp(0.65 * count_implied_probability + 0.35 * base_probability, 0.01, 0.995)


def _forecast_temperature_effect(
    target_date: date,
    temperature_value: Optional[float],
    *,
    seasonal_temperature_by_month: Dict[int, float],
    overall_temperature_average: Optional[float],
    temperature_slope: Optional[float],
    volatility: float,
) -> Tuple[Optional[float], float]:
    temperature_for_day = temperature_value
    if temperature_for_day is None:
        temperature_for_day = seasonal_temperature_by_month.get(target_date.month, overall_temperature_average)

    temperature_effect = 0.0
    if (
        temperature_for_day is not None
        and overall_temperature_average is not None
        and temperature_slope is not None
    ):
        seasonal_temperature = seasonal_temperature_by_month.get(target_date.month, overall_temperature_average)
        raw_temperature_effect = temperature_slope * (temperature_for_day - seasonal_temperature) * 0.35
        temperature_cap = max(0.6, volatility)
        temperature_effect = _clamp(raw_temperature_effect, -temperature_cap, temperature_cap)
    return temperature_for_day, temperature_effect


def _build_forecast_probability_context(
    *,
    weekday_event_rate: Dict[int, float],
    month_event_rate: Dict[int, float],
    weekday_positive_average: Dict[int, float],
    month_positive_average: Dict[int, float],
    recent_event_rate: float,
    recent_positive_average: float,
) -> Dict[str, object]:
    return {
        "weekday_event_rate": weekday_event_rate,
        "month_event_rate": month_event_rate,
        "weekday_positive_average": weekday_positive_average,
        "month_positive_average": month_positive_average,
        "recent_event_rate": recent_event_rate,
        "recent_positive_average": recent_positive_average,
    }


def _forecast_probability_bundle(
    target_date: date,
    *,
    estimate: float,
    lower_bound: float,
    upper_bound: float,
    usual_for_day: float,
    probability_context: Dict[str, object],
) -> Dict[str, float]:
    return {
        "fire": _forecast_event_probability(target_date, estimate, **probability_context),
        "lower": _forecast_event_probability(target_date, lower_bound, **probability_context),
        "upper": _forecast_event_probability(target_date, upper_bound, **probability_context),
        "usual": _forecast_event_probability(target_date, usual_for_day, **probability_context),
    }


def _build_forecast_row_payload(
    *,
    target_date: date,
    estimate: float,
    rounded_estimate: float,
    lower_bound: float,
    upper_bound: float,
    usual_for_day: float,
    temperature_for_day: Optional[float],
    scenario_label: str,
    scenario_tone: str,
    probabilities: Dict[str, float],
) -> Dict[str, object]:
    return {
        "date": target_date.isoformat(),
        "date_display": target_date.strftime("%d.%m.%Y"),
        "weekday_label": WEEKDAY_LABELS[target_date.weekday()],
        "forecast_value": rounded_estimate,
        "forecast_value_display": _format_number(rounded_estimate),
        "forecast_value_human_display": f"РѕРєРѕР»Рѕ {_format_number(rounded_estimate)}",
        "fire_probability": round(probabilities["fire"], 4),
        "fire_probability_display": _format_probability(probabilities["fire"]),
        "fire_probability_range_display": f"{_format_probability(probabilities['lower'])} - {_format_probability(probabilities['upper'])}",
        "usual_fire_probability": round(probabilities["usual"], 4),
        "usual_fire_probability_display": _format_probability(probabilities["usual"]),
        "usual_value": round(usual_for_day, 2),
        "usual_value_display": _format_number(usual_for_day),
        "lower_bound": round(lower_bound, 2),
        "lower_bound_display": _format_number(lower_bound),
        "upper_bound": round(upper_bound, 2),
        "upper_bound_display": _format_number(upper_bound),
        "range_display": _format_count_range(lower_bound, upper_bound),
        "temperature_display": f"{_format_number(temperature_for_day)} В°C" if temperature_for_day is not None else "РЎРµР·РѕРЅРЅР°СЏ СЃСЂРµРґРЅСЏСЏ",
        "scenario_label": scenario_label,
        "scenario_tone": scenario_tone,
        "scenario_hint": _relative_delta_text(estimate, usual_for_day, reference_label="Рє РѕР±С‹С‡РЅРѕРјСѓ СѓСЂРѕРІРЅСЋ РґР»СЏ С‚Р°РєРѕРіРѕ РґРЅСЏ"),
    }


def _build_forecast_rows(
    daily_history: List[Dict[str, object]],
    forecast_days: int,
    temperature_value: Optional[float],
) -> List[Dict[str, object]]:
    if not daily_history or forecast_days <= 0:
        return []

    stats = _build_forecast_history_stats(daily_history)
    history_dates = stats["history_dates"]
    history_counts = stats["history_counts"]
    overall_average = stats["overall_average"]
    recent_average = stats["recent_average"]
    recent_event_rate = stats["recent_event_rate"]
    recent_positive_average = stats["recent_positive_average"]
    trend_ratio = stats["trend_ratio"]
    base_recent_level = stats["base_recent_level"]
    volatility = stats["volatility"]
    robust_ceiling = stats["robust_ceiling"]
    weekday_factor, weekday_event_rate, weekday_positive_average = _build_weekday_forecast_factors(
        daily_history,
        overall_average=overall_average,
        recent_event_rate=recent_event_rate,
        recent_positive_average=recent_positive_average,
    )
    (
        month_factor,
        month_event_rate,
        month_positive_average,
        seasonal_temperature_by_month,
        overall_temperature_average,
    ) = _build_month_forecast_factors(
        daily_history,
        overall_average=overall_average,
        recent_event_rate=recent_event_rate,
        recent_positive_average=recent_positive_average,
    )
    temperature_pairs = [
        (float(item["avg_temperature"]), float(item["count"]))
        for item in daily_history
        if item["avg_temperature"] is not None
    ]
    temperature_slope = _compute_temperature_slope(temperature_pairs)

    forecast_rows: List[Dict[str, object]] = []
    last_observed_date = history_dates[-1]
    probability_context = _build_forecast_probability_context(
        weekday_event_rate=weekday_event_rate,
        month_event_rate=month_event_rate,
        weekday_positive_average=weekday_positive_average,
        month_positive_average=month_positive_average,
        recent_event_rate=recent_event_rate,
        recent_positive_average=recent_positive_average,
    )

    for step in range(1, forecast_days + 1):
        target_date = last_observed_date + timedelta(days=step)
        seasonal_factor = weekday_factor.get(target_date.weekday(), 1.0) * month_factor.get(target_date.month, 1.0)
        usual_for_day = max(0.0, base_recent_level * seasonal_factor)
        trend_effect = base_recent_level * trend_ratio * (0.75 - 0.45 * ((step - 1) / max(1, forecast_days - 1)))

        temperature_for_day, temperature_effect = _forecast_temperature_effect(
            target_date,
            temperature_value,
            seasonal_temperature_by_month=seasonal_temperature_by_month,
            overall_temperature_average=overall_temperature_average,
            temperature_slope=temperature_slope,
            volatility=volatility,
        )

        estimate = _clamp(usual_for_day + trend_effect + temperature_effect, 0.0, robust_ceiling)
        spread = max(0.75, volatility * (0.95 + step * 0.03))
        lower_bound = max(0.0, estimate - spread)
        upper_bound = min(robust_ceiling + spread, estimate + spread)
        rounded_estimate = round(estimate, 2)
        scenario_label, scenario_tone = _forecast_level_label(estimate, recent_average if recent_average > 0 else overall_average)
        probabilities = _forecast_probability_bundle(
            target_date,
            estimate=estimate,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            usual_for_day=usual_for_day,
            probability_context=probability_context,
        )

        forecast_rows.append(
            _build_forecast_row_payload(
                target_date=target_date,
                estimate=estimate,
                rounded_estimate=rounded_estimate,
                lower_bound=lower_bound,
                upper_bound=upper_bound,
                usual_for_day=usual_for_day,
                temperature_for_day=temperature_for_day,
                scenario_label=scenario_label,
                scenario_tone=scenario_tone,
                probabilities=probabilities,
            )
        )

    return forecast_rows


def _build_weekday_profile(daily_history: List[Dict[str, object]]) -> List[Dict[str, object]]:
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


def _build_weekly_outlook(daily_history: List[Dict[str, object]], forecast_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
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


def _build_monthly_outlook(daily_history: List[Dict[str, object]], forecast_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    history_by_month_of_year: Dict[int, List[float]] = defaultdict(list)
    if daily_history:
        month_totals: Dict[Tuple[int, int], float] = defaultdict(float)
        for item in daily_history:
            key = (item["date"].year, item["date"].month)
            month_totals[key] += float(item["count"])
        for (_year_value, month_value), total_value in month_totals.items():
            history_by_month_of_year[month_value].append(total_value)

    forecast_by_month: Dict[Tuple[int, int], float] = defaultdict(float)
    for row in forecast_rows:
        row_date = _parse_iso_date(row["date"])
        forecast_by_month[(row_date.year, row_date.month)] += float(row["forecast_value"])

    items = []
    for year_value, month_value in sorted(forecast_by_month):
        forecast_total = forecast_by_month[(year_value, month_value)]
        baseline = mean(history_by_month_of_year.get(month_value, [])) if history_by_month_of_year.get(month_value) else 0.0
        delta_ratio = ((forecast_total - baseline) / baseline) if baseline > 0 else 0.0
        level_label, level_tone = _forecast_level_label(forecast_total, baseline)
        items.append(
            {
                "month_key": f"{year_value:04d}-{month_value:02d}",
                "label": f"{MONTH_LABELS[month_value]} {year_value}",
                "forecast": round(forecast_total, 2),
                "forecast_display": _format_number(forecast_total),
                "baseline": round(baseline, 2),
                "baseline_display": _format_number(baseline),
                "delta_percent_display": _format_signed_percent(delta_ratio),
                "level_label": level_label,
                "level_tone": level_tone,
            }
        )
    return items[:4]
