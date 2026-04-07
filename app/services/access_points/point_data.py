from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from app.services.forecast_risk.utils import _clean_text, _is_rural_label, _normalize_match_text, _unique_non_empty

from .constants import GENERIC_OBJECT_TOKENS, MIN_ACCESS_POINT_SUPPORT, POINT_FEATURE_COLUMNS
from .data import _collect_access_point_inputs
from .numeric import _finite_numeric_frame, _normalize_coordinate, _safe_mean, _share


def _normalize_identity_token(value: Any) -> str:
    return _normalize_match_text(_clean_text(value))


def _is_generic_object_name(value: str) -> bool:
    normalized = _normalize_identity_token(value)
    if not normalized or len(normalized) < 4:
        return True
    return any(token in normalized for token in GENERIC_OBJECT_TOKENS)


def _smooth_share(successes: int, observations: int, prior_mean: float, minimum_support: int) -> float:
    if observations <= 0:
        return 0.0
    support_gap = max(0, int(minimum_support) - int(observations))
    if support_gap <= 0:
        return successes / float(observations)
    return (successes + (prior_mean * support_gap)) / float(observations + support_gap)


def _resolve_point_identity(record: Dict[str, Any]) -> Dict[str, Any]:
    address = _clean_text(record.get("address"))
    address_comment = _clean_text(record.get("address_comment"))
    object_name = _clean_text(record.get("object_name"))
    settlement = _clean_text(record.get("settlement"))
    territory_label = _clean_text(record.get("territory_label"))
    district = _clean_text(record.get("district"))
    latitude = _normalize_coordinate(record.get("latitude"), -90.0, 90.0)
    longitude = _normalize_coordinate(record.get("longitude"), -180.0, 180.0)

    normalized_address = _normalize_identity_token(address)
    normalized_comment = _normalize_identity_token(address_comment)
    normalized_object = _normalize_identity_token(object_name)
    normalized_settlement = _normalize_identity_token(settlement)
    normalized_territory = _normalize_identity_token(territory_label)
    normalized_district = _normalize_identity_token(district)
    meaningful_object = bool(object_name and not _is_generic_object_name(object_name))
    normalized_address_object = normalized_object if meaningful_object else ""

    if address:
        label = address
        if meaningful_object and normalized_object not in normalized_address:
            label = f"{object_name}, {address}"
        if address_comment and normalized_comment not in normalized_address:
            label = f"{label} ({address_comment})"
        return {
            "point_id": f"address:{normalized_address}|{normalized_address_object}",
            "label": label,
            "entity_type": "Объект / адрес",
            "entity_code": "address",
            "granularity_rank": 5,
        }

    if meaningful_object:
        return {
            "point_id": f"object:{normalized_object}",
            "label": object_name,
            "entity_type": "Объект",
            "entity_code": "object",
            "granularity_rank": 4,
        }

    if latitude is not None and longitude is not None:
        rounded_lat = round(float(latitude), 4)
        rounded_lon = round(float(longitude), 4)
        base_label = settlement or territory_label or district or "Координатная точка"
        return {
            "point_id": f"coords:{rounded_lat:.4f}:{rounded_lon:.4f}",
            "label": f"{base_label} ({rounded_lat:.4f}, {rounded_lon:.4f})",
            "entity_type": "Точная локация",
            "entity_code": "coordinates",
            "granularity_rank": 4,
        }

    if settlement:
        return {
            "point_id": f"settlement:{normalized_settlement}",
            "label": settlement,
            "entity_type": "Населённый пункт",
            "entity_code": "settlement",
            "granularity_rank": 3,
        }

    if territory_label:
        return {
            "point_id": f"territory:{normalized_territory}",
            "label": territory_label,
            "entity_type": "Territory label",
            "entity_code": "territory",
            "granularity_rank": 2,
        }

    if district:
        return {
            "point_id": f"district:{normalized_district}",
            "label": district,
            "entity_type": "Район",
            "entity_code": "district",
            "granularity_rank": 1,
        }

    return {
        "point_id": "unknown:unresolved",
        "label": "Неуточнённая точка",
        "entity_type": "Неуточнённая локация",
        "entity_code": "unknown",
        "granularity_rank": 0,
    }


def _new_point_bucket(identity: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **identity,
        "incident_count": 0,
        "response_total": 0.0,
        "response_count": 0,
        "distance_total": 0.0,
        "distance_count": 0,
        "long_arrival_count": 0,
        "water_yes_count": 0,
        "water_no_count": 0,
        "water_unknown_count": 0,
        "severe_count": 0,
        "victims_count": 0,
        "major_damage_count": 0,
        "night_count": 0,
        "heating_count": 0,
        "rural_count": 0,
        "years": Counter(),
        "districts": Counter(),
        "territories": Counter(),
        "settlements": Counter(),
        "settlement_types": Counter(),
        "object_categories": Counter(),
        "source_tables": Counter(),
        "latitude_values": [],
        "longitude_values": [],
    }


def _update_point_bucket_from_record(bucket: Dict[str, Any], record: Dict[str, Any]) -> None:
    bucket["incident_count"] += 1
    bucket["years"][int(record.get("year") or record["date"].year)] += 1

    district = _clean_text(record.get("district"))
    territory_label = _clean_text(record.get("territory_label"))
    settlement = _clean_text(record.get("settlement"))
    settlement_type = _clean_text(record.get("settlement_type"))
    object_category = _clean_text(record.get("object_category"))
    source_table = _clean_text(record.get("source_table"))

    if district:
        bucket["districts"][district] += 1
    if territory_label:
        bucket["territories"][territory_label] += 1
    if settlement:
        bucket["settlements"][settlement] += 1
    if settlement_type:
        bucket["settlement_types"][settlement_type] += 1
    if object_category:
        bucket["object_categories"][object_category] += 1
    if source_table:
        bucket["source_tables"][source_table] += 1

    response_minutes = record.get("response_minutes")
    if response_minutes is not None:
        bucket["response_total"] += float(response_minutes)
        bucket["response_count"] += 1

    distance_km = record.get("fire_station_distance")
    if distance_km is not None:
        bucket["distance_total"] += float(distance_km)
        bucket["distance_count"] += 1

    if record.get("long_arrival"):
        bucket["long_arrival_count"] += 1

    has_water_supply = record.get("has_water_supply")
    if has_water_supply is True:
        bucket["water_yes_count"] += 1
    elif has_water_supply is False:
        bucket["water_no_count"] += 1
    else:
        bucket["water_unknown_count"] += 1

    if record.get("severe_consequence"):
        bucket["severe_count"] += 1
    if record.get("victims_present"):
        bucket["victims_count"] += 1
    if record.get("major_damage"):
        bucket["major_damage_count"] += 1
    if record.get("night_incident"):
        bucket["night_count"] += 1
    if record.get("heating_season"):
        bucket["heating_count"] += 1

    rural_hint = settlement_type or settlement or territory_label or _clean_text(record.get("address"))
    if _is_rural_label(rural_hint):
        bucket["rural_count"] += 1

    latitude = _normalize_coordinate(record.get("latitude"), -90.0, 90.0)
    longitude = _normalize_coordinate(record.get("longitude"), -180.0, 180.0)
    if latitude is not None and longitude is not None:
        bucket["latitude_values"].append(latitude)
        bucket["longitude_values"].append(longitude)


def _build_point_priors(buckets: Dict[str, Dict[str, Any]], total_incidents: int) -> Dict[str, float]:
    total_response_count = 0
    total_long_arrivals = 0
    total_known_water = 0
    total_no_water = 0
    total_severe = 0
    total_victims = 0
    total_major_damage = 0
    total_night = 0
    total_heating = 0
    total_rural = 0
    for bucket in buckets.values():
        total_response_count += int(bucket["response_count"])
        total_long_arrivals += int(bucket["long_arrival_count"])
        total_known_water += int(bucket["water_yes_count"] + bucket["water_no_count"])
        total_no_water += int(bucket["water_no_count"])
        total_severe += int(bucket["severe_count"])
        total_victims += int(bucket["victims_count"])
        total_major_damage += int(bucket["major_damage_count"])
        total_night += int(bucket["night_count"])
        total_heating += int(bucket["heating_count"])
        total_rural += int(bucket["rural_count"])
    return {
        "long_arrival": _share(total_long_arrivals, total_response_count),
        "no_water": _share(total_no_water, total_known_water),
        "severe": _share(total_severe, total_incidents),
        "victims": _share(total_victims, total_incidents),
        "major_damage": _share(total_major_damage, total_incidents),
        "night": _share(total_night, total_incidents),
        "heating": _share(total_heating, total_incidents),
        "rural": _share(total_rural, total_incidents),
    }


def _build_point_entity_frames(
    records: Sequence[Dict[str, Any]],
    *,
    minimum_support: int = MIN_ACCESS_POINT_SUPPORT,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not records:
        empty = pd.DataFrame()
        return empty, empty

    resolved_support = max(2, int(minimum_support or MIN_ACCESS_POINT_SUPPORT))
    buckets: Dict[str, Dict[str, Any]] = {}
    total_incidents = 0

    for record in records:
        identity = _resolve_point_identity(record)
        point_id = str(identity["point_id"])
        bucket = buckets.get(point_id)
        if bucket is None:
            bucket = _new_point_bucket(identity)
            buckets[point_id] = bucket

        total_incidents += 1
        _update_point_bucket_from_record(bucket, record)

    priors = _build_point_priors(buckets, total_incidents)

    rows: List[Dict[str, Any]] = []
    for bucket in buckets.values():
        incident_count = int(bucket["incident_count"])
        response_count = int(bucket["response_count"])
        distance_count = int(bucket["distance_count"])
        known_water_count = int(bucket["water_yes_count"] + bucket["water_no_count"])
        years_observed = max(1, len(bucket["years"]))

        average_response = _safe_mean(bucket["response_total"], response_count)
        average_distance = _safe_mean(bucket["distance_total"], distance_count)
        response_coverage_share = _share(response_count, incident_count)
        water_coverage_share = _share(known_water_count, incident_count)
        distance_coverage_share = _share(distance_count, incident_count)
        water_unknown_share = max(0.0, 1.0 - water_coverage_share)

        long_arrival_share = _smooth_share(
            int(bucket["long_arrival_count"]),
            response_count,
            float(priors["long_arrival"]),
            resolved_support,
        )
        no_water_share = _smooth_share(
            int(bucket["water_no_count"]),
            known_water_count,
            float(priors["no_water"]),
            resolved_support,
        )
        severe_share = _smooth_share(
            int(bucket["severe_count"]),
            incident_count,
            float(priors["severe"]),
            resolved_support,
        )
        victim_share = _smooth_share(
            int(bucket["victims_count"]),
            incident_count,
            float(priors["victims"]),
            resolved_support,
        )
        major_damage_share = _smooth_share(
            int(bucket["major_damage_count"]),
            incident_count,
            float(priors["major_damage"]),
            resolved_support,
        )
        night_share = _smooth_share(
            int(bucket["night_count"]),
            incident_count,
            float(priors["night"]),
            resolved_support,
        )
        heating_share = _smooth_share(
            int(bucket["heating_count"]),
            incident_count,
            float(priors["heating"]),
            resolved_support,
        )
        rural_share = _smooth_share(
            int(bucket["rural_count"]),
            incident_count,
            float(priors["rural"]),
            resolved_support,
        )

        district_label = bucket["districts"].most_common(1)[0][0] if bucket["districts"] else ""
        territory_label = bucket["territories"].most_common(1)[0][0] if bucket["territories"] else ""
        settlement_label = bucket["settlements"].most_common(1)[0][0] if bucket["settlements"] else ""
        settlement_type = bucket["settlement_types"].most_common(1)[0][0] if bucket["settlement_types"] else ""
        object_category = bucket["object_categories"].most_common(1)[0][0] if bucket["object_categories"] else ""
        latitude = round(sum(bucket["latitude_values"]) / len(bucket["latitude_values"]), 5) if bucket["latitude_values"] else None
        longitude = round(sum(bucket["longitude_values"]) / len(bucket["longitude_values"]), 5) if bucket["longitude_values"] else None
        low_support = incident_count < resolved_support
        support_weight = min(1.0, incident_count / float(resolved_support))

        location_parts = _unique_non_empty(
            [
                settlement_label if settlement_label and settlement_label != bucket["label"] else "",
                territory_label if territory_label and territory_label not in {bucket["label"], settlement_label} else "",
                district_label if district_label and district_label not in {bucket["label"], territory_label, settlement_label} else "",
            ]
        )
        rows.append(
            {
                "point_id": bucket["point_id"],
                "label": bucket["label"],
                "entity_type": bucket["entity_type"],
                "entity_code": bucket["entity_code"],
                "granularity_rank": bucket["granularity_rank"],
                "district": district_label,
                "territory_label": territory_label,
                "settlement": settlement_label,
                "settlement_type": settlement_type,
                "rural_flag": bool(rural_share >= 0.5),
                "rural_share": round(rural_share, 4),
                "incident_count": incident_count,
                "years_observed": years_observed,
                "incidents_per_year": round(incident_count / float(years_observed), 4),
                "average_response_minutes": None if average_response is None else round(average_response, 4),
                "response_coverage_share": round(response_coverage_share, 4),
                "long_arrival_share": round(long_arrival_share, 4),
                "average_distance_km": None if average_distance is None else round(average_distance, 4),
                "distance_coverage_share": round(distance_coverage_share, 4),
                "no_water_share": round(no_water_share, 4),
                "water_coverage_share": round(water_coverage_share, 4),
                "water_unknown_share": round(water_unknown_share, 4),
                "severe_share": round(severe_share, 4),
                "victim_share": round(victim_share, 4),
                "major_damage_share": round(major_damage_share, 4),
                "victims_count": int(bucket["victims_count"]),
                "major_damage_count": int(bucket["major_damage_count"]),
                "night_share": round(night_share, 4),
                "heating_share": round(heating_share, 4),
                "low_support": low_support,
                "minimum_support": resolved_support,
                "support_weight": round(0.4 + (0.6 * support_weight), 4),
                "response_count": response_count,
                "known_water_count": known_water_count,
                "distance_count": distance_count,
                "source_tables": list(bucket["source_tables"].keys()),
                "source_tables_display": ", ".join(bucket["source_tables"].keys()),
                "object_category": object_category,
                "location_hint": " | ".join(location_parts) if location_parts else "Локация определена по лучшей доступной сущности",
                "latitude": latitude,
                "longitude": longitude,
            }
        )

    entity_frame = pd.DataFrame(rows)
    if entity_frame.empty:
        return entity_frame, entity_frame.copy()

    entity_frame = entity_frame.sort_values(
        ["incident_count", "granularity_rank", "label"],
        ascending=[False, False, True],
    ).reset_index(drop=True)

    feature_frame = entity_frame.loc[:, [column for column in POINT_FEATURE_COLUMNS if column in entity_frame.columns]].copy()
    if "low_support" in feature_frame.columns:
        feature_frame["low_support"] = feature_frame["low_support"].astype(float)
    if "rural_flag" in feature_frame.columns:
        feature_frame["rural_flag"] = feature_frame["rural_flag"].astype(float)
    feature_frame = _finite_numeric_frame(feature_frame)
    return entity_frame, feature_frame


def _load_access_point_dataset(
    source_tables: Sequence[str],
    *,
    district: str = "all",
    selected_year: Optional[int] = None,
    metadata_items: Optional[Sequence[Dict[str, Any]]] = None,
    minimum_support: int = MIN_ACCESS_POINT_SUPPORT,
) -> Dict[str, Any]:
    records, notes = _collect_access_point_inputs(
        source_tables,
        district=district,
        selected_year=selected_year,
        metadata_items=metadata_items,
    )
    entity_frame, feature_frame = _build_point_entity_frames(records, minimum_support=minimum_support)
    return {
        "records": records,
        "entity_frame": entity_frame,
        "feature_frame": feature_frame,
        "total_incidents": len(records),
        "total_entities": len(entity_frame),
        "notes": list(notes),
        "minimum_support": max(2, int(minimum_support or MIN_ACCESS_POINT_SUPPORT)),
    }
