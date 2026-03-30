from __future__ import annotations

from collections import Counter
from statistics import mean
from typing import Any, Dict, List, Sequence

from app.services.forecast_risk.utils import (
    _clamp,
    _clean_text,
    _format_integer,
    _format_number,
    _format_percent,
    _is_rural_label,
    _normalize_match_text,
    _unique_non_empty,
)

from .constants import (
    GENERIC_OBJECT_TOKENS,
    HIGH_RISK_THRESHOLD,
    LONG_RESPONSE_THRESHOLD_MINUTES,
    MAX_INCOMPLETE_POINTS,
    REVIEW_RISK_THRESHOLD,
    TOP_POINT_CARD_COUNT,
    WATCH_RISK_THRESHOLD,
)


def _share(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _safe_mean(total: float, count: int) -> float | None:
    if count <= 0:
        return None
    return total / float(count)


def _normalize_identity_token(value: Any) -> str:
    return _normalize_match_text(_clean_text(value))


def _is_generic_object_name(value: str) -> bool:
    normalized = _normalize_identity_token(value)
    if not normalized or len(normalized) < 4:
        return True
    return any(token in normalized for token in GENERIC_OBJECT_TOKENS)


def _format_coordinate(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.4f}".replace(".", ",")


def _status_for_score(score: float) -> tuple[str, str]:
    if score >= HIGH_RISK_THRESHOLD:
        return "critical", "Критичный приоритет"
    if score >= REVIEW_RISK_THRESHOLD:
        return "warning", "Повышенный приоритет"
    if score >= WATCH_RISK_THRESHOLD:
        return "watch", "Наблюдение"
    return "normal", "Контроль"


def _component_tone(score: float) -> str:
    if score >= 65.0:
        return "critical"
    if score >= 45.0:
        return "warning"
    if score >= 25.0:
        return "watch"
    return "normal"


def _resolve_point_identity(record: Dict[str, Any]) -> Dict[str, Any]:
    address = _clean_text(record.get("address"))
    address_comment = _clean_text(record.get("address_comment"))
    object_name = _clean_text(record.get("object_name"))
    territory_label = _clean_text(record.get("territory_label"))
    district = _clean_text(record.get("district"))
    latitude = record.get("latitude")
    longitude = record.get("longitude")
    settlement_type = _clean_text(record.get("settlement_type"))

    normalized_address = _normalize_identity_token(address)
    normalized_comment = _normalize_identity_token(address_comment)
    normalized_object = _normalize_identity_token(object_name)

    if address:
        label = address
        if object_name and not _is_generic_object_name(object_name) and normalized_object not in normalized_address:
            label = f"{object_name}, {address}"
        if address_comment and normalized_comment not in normalized_address:
            label = f"{label} ({address_comment})"
        return {
            "point_id": f"address:{normalized_address}|{normalized_object}",
            "label": label,
            "entity_type": "Объект / адрес",
            "entity_code": "address",
            "granularity_rank": 5,
        }

    if object_name and not _is_generic_object_name(object_name):
        return {
            "point_id": f"object:{normalized_object}",
            "label": object_name,
            "entity_type": "Объект",
            "entity_code": "object",
            "granularity_rank": 4,
        }

    if latitude is not None and longitude is not None:
        rounded_lat = round(float(latitude), 3)
        rounded_lon = round(float(longitude), 3)
        base_label = territory_label or district or "Координатная точка"
        return {
            "point_id": f"coords:{rounded_lat:.3f}:{rounded_lon:.3f}",
            "label": f"{base_label} ({rounded_lat:.3f}, {rounded_lon:.3f})",
            "entity_type": "Координатная точка",
            "entity_code": "coordinates",
            "granularity_rank": 3,
        }

    if territory_label:
        return {
            "point_id": f"territory:{_normalize_identity_token(territory_label)}",
            "label": territory_label,
            "entity_type": "Населённый пункт" if settlement_type else "Территория",
            "entity_code": "territory",
            "granularity_rank": 2,
        }

    if district:
        return {
            "point_id": f"district:{_normalize_identity_token(district)}",
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


def _typology_for_row(row: Dict[str, Any]) -> tuple[str, str]:
    if row["missing_data_priority"]:
        return "needs_data", "Данные неполные"
    components = {
        "access": row["access_score"],
        "water": row["water_score"],
        "severity": row["severity_score"],
        "recurrence": row["recurrence_score"],
    }
    dominant = max(components, key=components.get)
    if dominant == "access" and row["access_score"] >= 45.0:
        return "access", "Дальний выезд"
    if dominant == "water" and row["water_score"] >= 40.0:
        return "water", "Дефицит воды"
    if dominant == "severity" and row["severity_score"] >= 40.0:
        return "severity", "Тяжёлые последствия"
    if dominant == "recurrence" and row["recurrence_score"] >= 35.0:
        return "recurrence", "Повторяющийся очаг"
    return "mixed", "Комбинированный риск"


def _reason_candidates(row: Dict[str, Any]) -> List[tuple[float, str]]:
    reasons: List[tuple[float, str]] = []

    average_distance = row.get("average_distance_km")
    average_response = row.get("average_response_minutes")
    long_arrival_share = float(row.get("long_arrival_share") or 0.0)
    no_water_share = float(row.get("no_water_share") or 0.0)
    water_unknown_share = float(row.get("water_unknown_share") or 0.0)
    severe_share = float(row.get("severe_share") or 0.0)
    night_share = float(row.get("night_share") or 0.0)
    heating_share = float(row.get("heating_share") or 0.0)
    rural_share = float(row.get("rural_share") or 0.0)
    completeness_share = float(row.get("completeness_share") or 0.0)

    if row["access_score"] >= 35.0:
        parts: List[str] = []
        if average_distance is not None and average_distance >= 8.0:
            parts.append(f"средняя удалённость до ПЧ {_format_number(average_distance)} км")
        if average_response is not None and average_response >= LONG_RESPONSE_THRESHOLD_MINUTES:
            parts.append(f"среднее прибытие {_format_number(average_response)} мин")
        if long_arrival_share >= 0.25:
            parts.append(f"долгое прибытие в {_format_percent(long_arrival_share * 100.0)} случаев")
        if rural_share >= 0.55:
            parts.append("преимущественно сельский контекст")
        if parts:
            reasons.append((row["access_score"], "; ".join(parts)))

    if row["water_score"] >= 30.0:
        water_parts: List[str] = []
        if no_water_share > 0:
            water_parts.append(f"водоснабжение не подтверждено в {_format_percent(no_water_share * 100.0)} подтверждённых кейсов")
        if water_unknown_share >= 0.2:
            water_parts.append(f"по воде пропуски в {_format_percent(water_unknown_share * 100.0)} инцидентов")
        if water_parts:
            reasons.append((row["water_score"], "; ".join(water_parts)))

    if row["severity_score"] >= 30.0:
        severity_parts: List[str] = []
        if severe_share > 0:
            severity_parts.append(f"тяжёлые последствия в {_format_percent(severe_share * 100.0)} случаев")
        if row.get("victims_count", 0) > 0:
            severity_parts.append(f"есть пострадавшие/погибшие: {_format_integer(row['victims_count'])}")
        if row.get("major_damage_count", 0) > 0:
            severity_parts.append(f"материальный ущерб/разрушения в {_format_integer(row['major_damage_count'])} инцидентах")
        if severity_parts:
            reasons.append((row["severity_score"], "; ".join(severity_parts)))

    if row["recurrence_score"] >= 28.0:
        recurrence_parts = [
            f"{_format_integer(row['incident_count'])} пожаров за период",
        ]
        if night_share >= 0.25:
            recurrence_parts.append(f"ночные инциденты {_format_percent(night_share * 100.0)}")
        if heating_share >= 0.4:
            recurrence_parts.append(f"отопительный сезон {_format_percent(heating_share * 100.0)}")
        reasons.append((row["recurrence_score"], "; ".join(recurrence_parts)))

    if row["data_gap_score"] >= 35.0:
        missing_parts: List[str] = []
        if float(row.get("arrival_missing_share") or 0.0) >= 0.25:
            missing_parts.append("не хватает времени прибытия")
        if float(row.get("distance_missing_share") or 0.0) >= 0.25:
            missing_parts.append("не хватает дистанции до ПЧ")
        if water_unknown_share >= 0.25:
            missing_parts.append("не хватает сведений о воде")
        if missing_parts:
            reasons.append((row["data_gap_score"], "; ".join(missing_parts)))

    if not reasons:
        reasons.append((0.0, f"сводная полнота данных {_format_percent(completeness_share * 100.0)}"))
    reasons.sort(key=lambda item: item[0], reverse=True)
    return reasons


def _build_component_scores(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        {
            "key": "access",
            "label": "Доступность ПЧ",
            "score": round(float(row["access_score"]), 1),
            "score_display": _format_number(row["access_score"]),
            "tone": _component_tone(float(row["access_score"])),
        },
        {
            "key": "water",
            "label": "Водоснабжение",
            "score": round(float(row["water_score"]), 1),
            "score_display": _format_number(row["water_score"]),
            "tone": _component_tone(float(row["water_score"])),
        },
        {
            "key": "severity",
            "label": "Последствия",
            "score": round(float(row["severity_score"]), 1),
            "score_display": _format_number(row["severity_score"]),
            "tone": _component_tone(float(row["severity_score"])),
        },
        {
            "key": "recurrence",
            "label": "Частота и контекст",
            "score": round(float(row["recurrence_score"]), 1),
            "score_display": _format_number(row["recurrence_score"]),
            "tone": _component_tone(float(row["recurrence_score"])),
        },
        {
            "key": "data_gap",
            "label": "Неполнота данных",
            "score": round(float(row["data_gap_score"]), 1),
            "score_display": _format_number(row["data_gap_score"]),
            "tone": _component_tone(float(row["data_gap_score"])),
        },
    ]


def _build_access_point_rows(records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not records:
        return []

    buckets: Dict[str, Dict[str, Any]] = {}
    for record in records:
        identity = _resolve_point_identity(record)
        point_id = identity["point_id"]
        bucket = buckets.get(point_id)
        if bucket is None:
            bucket = {
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
                "arrival_missing_count": 0,
                "distance_missing_count": 0,
                "years": Counter(),
                "districts": Counter(),
                "territories": Counter(),
                "object_categories": Counter(),
                "source_tables": Counter(),
                "latitude_values": [],
                "longitude_values": [],
            }
            buckets[point_id] = bucket

        bucket["incident_count"] += 1
        bucket["years"][int(record.get("year") or record["date"].year)] += 1

        district = _clean_text(record.get("district"))
        territory_label = _clean_text(record.get("territory_label"))
        object_category = _clean_text(record.get("object_category"))
        source_table = _clean_text(record.get("source_table"))

        if district:
            bucket["districts"][district] += 1
        if territory_label:
            bucket["territories"][territory_label] += 1
        if object_category:
            bucket["object_categories"][object_category] += 1
        if source_table:
            bucket["source_tables"][source_table] += 1

        response_minutes = record.get("response_minutes")
        if response_minutes is None:
            bucket["arrival_missing_count"] += 1
        else:
            bucket["response_total"] += float(response_minutes)
            bucket["response_count"] += 1

        distance_km = record.get("fire_station_distance")
        if distance_km is None:
            bucket["distance_missing_count"] += 1
        else:
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

        rural_hint = _clean_text(record.get("settlement_type")) or territory_label or _clean_text(record.get("address"))
        if _is_rural_label(rural_hint):
            bucket["rural_count"] += 1

        latitude = record.get("latitude")
        longitude = record.get("longitude")
        if latitude is not None:
            bucket["latitude_values"].append(float(latitude))
        if longitude is not None:
            bucket["longitude_values"].append(float(longitude))

    normalized_rows: List[Dict[str, Any]] = []
    max_incidents = max(bucket["incident_count"] for bucket in buckets.values())
    max_incidents_per_year = max(
        bucket["incident_count"] / max(1, len(bucket["years"]))
        for bucket in buckets.values()
    )
    distance_scale = max(
        12.0,
        max((_safe_mean(bucket["distance_total"], bucket["distance_count"]) or 0.0) for bucket in buckets.values()),
    )
    response_scale = max(
        LONG_RESPONSE_THRESHOLD_MINUTES,
        max((_safe_mean(bucket["response_total"], bucket["response_count"]) or 0.0) for bucket in buckets.values()),
    )

    for bucket in buckets.values():
        incident_count = int(bucket["incident_count"])
        years_observed = max(1, len(bucket["years"]))
        incidents_per_year = incident_count / float(years_observed)
        average_distance = _safe_mean(bucket["distance_total"], bucket["distance_count"])
        average_response = _safe_mean(bucket["response_total"], bucket["response_count"])

        long_arrival_share = _share(bucket["long_arrival_count"], incident_count)
        known_water_count = bucket["water_yes_count"] + bucket["water_no_count"]
        no_water_share = _share(bucket["water_no_count"], known_water_count)
        water_unknown_share = _share(bucket["water_unknown_count"], incident_count)
        severe_share = _share(bucket["severe_count"], incident_count)
        victim_share = _share(bucket["victims_count"], incident_count)
        major_damage_share = _share(bucket["major_damage_count"], incident_count)
        night_share = _share(bucket["night_count"], incident_count)
        heating_share = _share(bucket["heating_count"], incident_count)
        rural_share = _share(bucket["rural_count"], incident_count)
        arrival_missing_share = _share(bucket["arrival_missing_count"], incident_count)
        distance_missing_share = _share(bucket["distance_missing_count"], incident_count)
        completeness_share = max(
            0.0,
            1.0 - mean([arrival_missing_share, distance_missing_share, water_unknown_share]),
        )

        distance_norm = 0.0 if average_distance is None else _clamp(average_distance / distance_scale, 0.0, 1.0)
        response_norm = 0.0 if average_response is None else _clamp(average_response / response_scale, 0.0, 1.0)
        frequency_norm = _clamp(incidents_per_year / max(1.0, max_incidents_per_year), 0.0, 1.0)
        incidents_norm = _clamp(incident_count / max(1.0, max_incidents), 0.0, 1.0)

        access_score = 100.0 * _clamp(
            0.34 * distance_norm
            + 0.30 * response_norm
            + 0.24 * long_arrival_share
            + 0.12 * rural_share,
            0.0,
            1.0,
        )
        water_score = 100.0 * _clamp(0.72 * no_water_share + 0.28 * water_unknown_share, 0.0, 1.0)
        severity_score = 100.0 * _clamp(
            0.52 * severe_share + 0.24 * victim_share + 0.24 * major_damage_share,
            0.0,
            1.0,
        )
        recurrence_score = 100.0 * _clamp(
            0.38 * frequency_norm
            + 0.22 * incidents_norm
            + 0.16 * night_share
            + 0.14 * heating_share
            + 0.10 * rural_share,
            0.0,
            1.0,
        )
        data_gap_score = 100.0 * _clamp(
            0.42 * arrival_missing_share + 0.33 * water_unknown_share + 0.25 * distance_missing_share,
            0.0,
            1.0,
        )
        score = (
            0.36 * access_score
            + 0.18 * water_score
            + 0.22 * severity_score
            + 0.18 * recurrence_score
            + 0.06 * data_gap_score
        )
        investigation_score = (
            0.18 * access_score
            + 0.10 * water_score
            + 0.14 * severity_score
            + 0.12 * recurrence_score
            + 0.46 * data_gap_score
        )
        missing_data_priority = (
            data_gap_score >= 45.0
            and investigation_score >= REVIEW_RISK_THRESHOLD
            and score < REVIEW_RISK_THRESHOLD
        )

        district_label = bucket["districts"].most_common(1)[0][0] if bucket["districts"] else ""
        territory_label = bucket["territories"].most_common(1)[0][0] if bucket["territories"] else ""
        object_category = bucket["object_categories"].most_common(1)[0][0] if bucket["object_categories"] else ""
        latitude = round(mean(bucket["latitude_values"]), 5) if bucket["latitude_values"] else None
        longitude = round(mean(bucket["longitude_values"]), 5) if bucket["longitude_values"] else None
        location_parts = _unique_non_empty(
            [
                territory_label if territory_label and territory_label != bucket["label"] else "",
                district_label if district_label and district_label != territory_label else "",
            ]
        )
        location_hint = " | ".join(location_parts) if location_parts else "Локация определяется по лучшей доступной сущности"

        row: Dict[str, Any] = {
            "point_id": bucket["point_id"],
            "label": bucket["label"],
            "entity_type": bucket["entity_type"],
            "entity_code": bucket["entity_code"],
            "granularity_rank": bucket["granularity_rank"],
            "district": district_label,
            "territory_label": territory_label,
            "location_hint": location_hint,
            "object_category": object_category,
            "incident_count": incident_count,
            "incident_count_display": _format_integer(incident_count),
            "years_observed": years_observed,
            "years_observed_display": _format_integer(years_observed),
            "incidents_per_year": round(incidents_per_year, 2),
            "incidents_per_year_display": _format_number(incidents_per_year),
            "average_distance_km": None if average_distance is None else round(average_distance, 2),
            "average_distance_display": "н/д" if average_distance is None else f"{_format_number(average_distance)} км",
            "average_response_minutes": None if average_response is None else round(average_response, 1),
            "average_response_display": "н/д" if average_response is None else f"{_format_number(average_response)} мин",
            "long_arrival_share": round(long_arrival_share, 4),
            "long_arrival_share_display": _format_percent(long_arrival_share * 100.0),
            "no_water_share": round(no_water_share, 4),
            "no_water_share_display": _format_percent(no_water_share * 100.0),
            "water_unknown_share": round(water_unknown_share, 4),
            "water_unknown_share_display": _format_percent(water_unknown_share * 100.0),
            "severe_share": round(severe_share, 4),
            "severe_share_display": _format_percent(severe_share * 100.0),
            "night_share": round(night_share, 4),
            "night_share_display": _format_percent(night_share * 100.0),
            "heating_share": round(heating_share, 4),
            "heating_share_display": _format_percent(heating_share * 100.0),
            "rural_share": round(rural_share, 4),
            "rural_share_display": _format_percent(rural_share * 100.0),
            "arrival_missing_share": round(arrival_missing_share, 4),
            "distance_missing_share": round(distance_missing_share, 4),
            "completeness_share": round(completeness_share, 4),
            "completeness_display": _format_percent(completeness_share * 100.0),
            "access_score": round(access_score, 1),
            "water_score": round(water_score, 1),
            "severity_score": round(severity_score, 1),
            "recurrence_score": round(recurrence_score, 1),
            "data_gap_score": round(data_gap_score, 1),
            "score": round(score, 1),
            "score_display": _format_number(score),
            "investigation_score": round(investigation_score, 1),
            "investigation_score_display": _format_number(investigation_score),
            "missing_data_priority": missing_data_priority,
            "victims_count": int(bucket["victims_count"]),
            "major_damage_count": int(bucket["major_damage_count"]),
            "source_tables": list(bucket["source_tables"].keys()),
            "source_tables_display": ", ".join(bucket["source_tables"].keys()),
            "latitude": latitude,
            "longitude": longitude,
            "coordinates_display": (
                f"{_format_coordinate(latitude)}, {_format_coordinate(longitude)}"
                if latitude is not None and longitude is not None
                else ""
            ),
        }
        row["tone"], row["priority_label"] = _status_for_score(float(row["score"]))
        row["component_scores"] = _build_component_scores(row)
        row["typology_code"], row["typology_label"] = _typology_for_row(row)
        reason_pairs = _reason_candidates(row)
        row["reasons"] = [text for _score, text in reason_pairs[:4]]
        row["reason_chips"] = row["reasons"][:3]
        row["explanation"] = ". ".join(row["reasons"][:2]) if row["reasons"] else "Точка включена в рейтинг по сумме факторов."
        row["incomplete_note"] = (
            "Высокий приоритет проверки связан прежде всего с пропусками по доступности, воде или времени прибытия."
            if missing_data_priority
            else ""
        )
        normalized_rows.append(row)

    normalized_rows.sort(
        key=lambda item: (
            float(item["score"]),
            float(item["severity_score"]),
            float(item["access_score"]),
            int(item["incident_count"]),
            int(item["granularity_rank"]),
        ),
        reverse=True,
    )
    for index, row in enumerate(normalized_rows, start=1):
        row["rank"] = index
        row["rank_display"] = str(index)
    return normalized_rows


def _select_top_points(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(row) for row in list(rows)[:TOP_POINT_CARD_COUNT]]


def _select_incomplete_points(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    candidates = [
        dict(row)
        for row in rows
        if row.get("missing_data_priority")
        or (
            float(row.get("data_gap_score") or 0.0) >= 50.0
            and float(row.get("investigation_score") or 0.0) >= WATCH_RISK_THRESHOLD
        )
    ]
    candidates.sort(
        key=lambda item: (
            float(item.get("investigation_score") or 0.0),
            float(item.get("data_gap_score") or 0.0),
            float(item.get("score") or 0.0),
        ),
        reverse=True,
    )
    return candidates[:MAX_INCOMPLETE_POINTS]


def _build_typology_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = str(row.get("typology_code") or "mixed")
        bucket = grouped.get(code)
        if bucket is None:
            bucket = {
                "code": code,
                "label": row.get("typology_label") or "Комбинированный риск",
                "count": 0,
                "max_score": 0.0,
                "lead_label": "",
            }
            grouped[code] = bucket
        bucket["count"] += 1
        if float(row.get("score") or 0.0) >= float(bucket["max_score"]):
            bucket["max_score"] = float(row.get("score") or 0.0)
            bucket["lead_label"] = str(row.get("label") or "")

    total = max(1, len(rows))
    summary_rows = []
    for bucket in grouped.values():
        summary_rows.append(
            {
                "code": bucket["code"],
                "label": bucket["label"],
                "count": int(bucket["count"]),
                "count_display": _format_integer(bucket["count"]),
                "share_display": _format_percent(_share(bucket["count"], total) * 100.0),
                "max_score": round(float(bucket["max_score"]), 1),
                "max_score_display": _format_number(bucket["max_score"]),
                "lead_label": bucket["lead_label"] or "-",
            }
        )
    summary_rows.sort(key=lambda item: (item["count"], item["max_score"]), reverse=True)
    return summary_rows
