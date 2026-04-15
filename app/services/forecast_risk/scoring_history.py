from __future__ import annotations

from collections import Counter
from datetime import timedelta
from typing import Any, Dict, List, Sequence, cast

from .profiles import resolve_component_weights
from .types import ComponentWeightRow, HorizonContext, RiskEventRecord, RiskProfile, TerritoryBucket
from .utils import _is_heating_season


def _component_weights_for_rural(
    profile: RiskProfile,
    cache: Dict[bool, List[ComponentWeightRow]],
    *,
    is_rural: bool,
) -> List[ComponentWeightRow]:
    rural_key = bool(is_rural)
    component_weights = cache.get(rural_key)
    if component_weights is None:
        component_weights = cast(List[ComponentWeightRow], resolve_component_weights(profile, is_rural=rural_key))
        cache[rural_key] = component_weights
    return component_weights

def _history_date_bounds(records: Sequence[RiskEventRecord]) -> tuple[Any, Any]:
    record_iterator = iter(records)
    first_record = next(record_iterator)
    history_start = first_record["date"]
    history_end = first_record["date"]
    for record in record_iterator:
        record_date = record["date"]
        if record_date < history_start:
            history_start = record_date
        if record_date > history_end:
            history_end = record_date
    return history_start, history_end

def _territory_label(record: RiskEventRecord) -> str:
    return record["territory_label"] or record["district"] or "Территория не указана"

def _horizon_context(
    records: Sequence[RiskEventRecord],
    planning_horizon_days: int,
) -> HorizonContext:
    history_start, history_end = _history_date_bounds(records)
    history_days = max(1, (history_end - history_start).days + 1)
    horizon_days = max(1, int(planning_horizon_days or 14))
    future_dates = [history_end + timedelta(days=offset) for offset in range(1, horizon_days + 1)]
    recent_window_days = max(1, min(history_days, 90))
    return {
        "history_end": history_end,
        "history_days": history_days,
        "horizon_days": horizon_days,
        "future_months": Counter(item.month for item in future_dates),
        "future_weekdays": Counter(item.weekday() for item in future_dates),
        "future_heating_share": sum(1 for item in future_dates if _is_heating_season(item)) / horizon_days,
        "recent_window_days": recent_window_days,
        "recent_window_start": history_end - timedelta(days=recent_window_days - 1),
    }

def _empty_territory_bucket(label: str) -> TerritoryBucket:
    return {
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
    }

def _history_weight_for_record(record_date: Any, horizon: HorizonContext) -> tuple[float, float, float]:
    horizon_days = horizon["horizon_days"]
    age_days = max(0, (horizon["history_end"] - record_date).days)
    month_alignment = horizon["future_months"].get(record_date.month, 0) / horizon_days
    weekday_alignment = horizon["future_weekdays"].get(record_date.weekday(), 0) / horizon_days
    recency_weight = max(0.25, 1.0 - age_days / max(210.0, float(horizon["history_days"])))
    history_weight = recency_weight * (1.0 + 0.40 * month_alignment) * (1.0 + 0.18 * weekday_alignment)
    return month_alignment, weekday_alignment, history_weight

def _update_territory_bucket(
    bucket: TerritoryBucket,
    record: RiskEventRecord,
    *,
    month_alignment: float,
    weekday_alignment: float,
    history_weight: float,
) -> None:
    record_date = record["date"]
    bucket["incidents"] += 1
    bucket["weighted_history"] += history_weight
    bucket["seasonal_month_sum"] += month_alignment
    bucket["seasonal_weekday_sum"] += weekday_alignment
    bucket["last_fire"] = record_date if bucket["last_fire"] is None else max(bucket["last_fire"], record_date)
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

def _collect_territory_buckets(
    records: Sequence[RiskEventRecord],
    horizon: HorizonContext,
) -> tuple[Dict[str, TerritoryBucket], int]:
    recent_incidents = 0
    territories: Dict[str, TerritoryBucket] = {}
    for record in records:
        record_date = record["date"]
        if record_date >= horizon["recent_window_start"]:
            recent_incidents += 1
        label = _territory_label(record)
        bucket = territories.get(label)
        if bucket is None:
            bucket = _empty_territory_bucket(label)
            territories[label] = bucket
        month_alignment, weekday_alignment, history_weight = _history_weight_for_record(record_date, horizon)
        _update_territory_bucket(
            bucket,
            record,
            month_alignment=month_alignment,
            weekday_alignment=weekday_alignment,
            history_weight=history_weight,
        )
    return territories, recent_incidents
