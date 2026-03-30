from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

from app.db_metadata import get_table_columns_cached
from app.services.forecast_risk.utils import (
    _calculate_response_minutes,
    _clean_text,
    _date_expression,
    _is_heating_season,
    _numeric_expression_for_column,
    _parse_datetime_text,
    _parse_water_supply_flag,
    _pick_territory_label,
    _quote_identifier,
    _resolve_column_name,
    _risk_category_score,
    _text_expression,
    _to_float_or_none,
    _truthy_value,
)
from app.services.table_options import get_fire_map_table_options
from config.db import engine

from .constants import (
    ACCESS_POINT_LIMIT_OPTIONS,
    ADDRESS_COLUMN_CANDIDATES,
    ADDRESS_COMMENT_COLUMN_CANDIDATES,
    ARRIVAL_TIME_COLUMN_CANDIDATES,
    CASUALTY_FLAG_COLUMN_CANDIDATES,
    CONSEQUENCE_COLUMN_CANDIDATES,
    DATE_COLUMN,
    DETECTION_TIME_COLUMN_CANDIDATES,
    DEATHS_COLUMN_CANDIDATES,
    DESTROYED_AREA_COLUMN_CANDIDATES,
    DESTROYED_BUILDINGS_COLUMN_CANDIDATES,
    DISTRICT_COLUMN_CANDIDATES,
    FIRE_STATION_DISTANCE_COLUMN_CANDIDATES,
    INJURIES_COLUMN_CANDIDATES,
    LATITUDE_COLUMN_CANDIDATES,
    LONGITUDE_COLUMN_CANDIDATES,
    LONG_RESPONSE_THRESHOLD_MINUTES,
    OBJECT_CATEGORY_COLUMN_CANDIDATES,
    OBJECT_NAME_COLUMN_CANDIDATES,
    REGISTERED_DAMAGE_COLUMN_CANDIDATES,
    REPORT_TIME_COLUMN_CANDIDATES,
    SETTLEMENT_COLUMN_CANDIDATES,
    SETTLEMENT_TYPE_COLUMN_CANDIDATES,
    TERRITORY_LABEL_COLUMN_CANDIDATES,
    WATER_SUPPLY_COUNT_COLUMN_CANDIDATES,
    WATER_SUPPLY_DETAILS_COLUMN_CANDIDATES,
)


def _build_access_points_table_options() -> List[Dict[str, str]]:
    options: List[Dict[str, str]] = []
    seen = set()
    for option in get_fire_map_table_options():
        value = str(option.get("value") or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        options.append({"value": value, "label": str(option.get("label") or value)})
    return [{"value": "all", "label": "Все таблицы"}] + options


def _resolve_selected_table(table_options: Sequence[Dict[str, str]], table_name: str) -> str:
    available = {str(option.get("value") or "") for option in table_options}
    normalized = str(table_name or "").strip()
    if normalized in available:
        return normalized
    return "all" if available else ""


def _selected_source_tables(table_options: Sequence[Dict[str, str]], selected_table: str) -> List[str]:
    concrete_tables = [str(option.get("value") or "") for option in table_options if option.get("value") and option.get("value") != "all"]
    if selected_table == "all":
        return concrete_tables
    return [selected_table] if selected_table in concrete_tables else []


def _parse_limit(value: str) -> int:
    try:
        parsed = int(str(value).strip())
    except Exception:
        return ACCESS_POINT_LIMIT_OPTIONS[1]
    if parsed in ACCESS_POINT_LIMIT_OPTIONS:
        return parsed
    return min(ACCESS_POINT_LIMIT_OPTIONS, key=lambda item: abs(item - parsed))


def _resolve_option_value(options: Sequence[Dict[str, str]], selected_value: object, default: str = "all") -> str:
    normalized = str(selected_value or "").strip() or default
    available = {str(option.get("value") or "") for option in options}
    if normalized in available:
        return normalized
    return str(options[0].get("value") or default) if options else default


def _load_table_metadata(table_name: str) -> Dict[str, Any]:
    try:
        columns = get_table_columns_cached(table_name)
    except ValueError as exc:
        raise ValueError(f"Таблица '{table_name}' не найдена в базе данных.") from exc

    resolved_columns = {
        "date": _resolve_column_name(columns, [DATE_COLUMN]),
        "district": _resolve_column_name(columns, DISTRICT_COLUMN_CANDIDATES),
        "territory_label": _resolve_column_name(columns, TERRITORY_LABEL_COLUMN_CANDIDATES),
        "settlement": _resolve_column_name(columns, SETTLEMENT_COLUMN_CANDIDATES),
        "settlement_type": _resolve_column_name(columns, SETTLEMENT_TYPE_COLUMN_CANDIDATES),
        "object_category": _resolve_column_name(columns, OBJECT_CATEGORY_COLUMN_CANDIDATES),
        "fire_station_distance": _resolve_column_name(columns, FIRE_STATION_DISTANCE_COLUMN_CANDIDATES),
        "water_supply_count": _resolve_column_name(columns, WATER_SUPPLY_COUNT_COLUMN_CANDIDATES),
        "water_supply_details": _resolve_column_name(columns, WATER_SUPPLY_DETAILS_COLUMN_CANDIDATES),
        "report_time": _resolve_column_name(columns, REPORT_TIME_COLUMN_CANDIDATES),
        "arrival_time": _resolve_column_name(columns, ARRIVAL_TIME_COLUMN_CANDIDATES),
        "detection_time": _resolve_column_name(columns, DETECTION_TIME_COLUMN_CANDIDATES),
        "consequence": _resolve_column_name(columns, CONSEQUENCE_COLUMN_CANDIDATES),
        "registered_damage": _resolve_column_name(columns, REGISTERED_DAMAGE_COLUMN_CANDIDATES),
        "destroyed_buildings": _resolve_column_name(columns, DESTROYED_BUILDINGS_COLUMN_CANDIDATES),
        "destroyed_area": _resolve_column_name(columns, DESTROYED_AREA_COLUMN_CANDIDATES),
        "casualty_flag": _resolve_column_name(columns, CASUALTY_FLAG_COLUMN_CANDIDATES),
        "injuries": _resolve_column_name(columns, INJURIES_COLUMN_CANDIDATES),
        "deaths": _resolve_column_name(columns, DEATHS_COLUMN_CANDIDATES),
        "address": _resolve_column_name(columns, ADDRESS_COLUMN_CANDIDATES),
        "address_comment": _resolve_column_name(columns, ADDRESS_COMMENT_COLUMN_CANDIDATES),
        "object_name": _resolve_column_name(columns, OBJECT_NAME_COLUMN_CANDIDATES),
        "latitude": _resolve_column_name(columns, LATITUDE_COLUMN_CANDIDATES),
        "longitude": _resolve_column_name(columns, LONGITUDE_COLUMN_CANDIDATES),
    }
    return {"table_name": table_name, "columns": columns, "resolved_columns": resolved_columns}


def _collect_access_point_metadata(source_tables: Sequence[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
    metadata_items: List[Dict[str, Any]] = []
    notes: List[str] = []
    for source_table in source_tables:
        try:
            metadata_items.append(_load_table_metadata(source_table))
        except Exception as exc:
            notes.append(f"{source_table}: {exc}")
    return metadata_items, notes


def _build_scope_conditions(
    resolved_columns: Dict[str, str],
    *,
    district: str = "all",
    selected_year: Optional[int] = None,
) -> Tuple[Optional[str], List[str], Dict[str, Any], bool]:
    date_column = resolved_columns.get("date") or ""
    if not date_column:
        return None, [], {}, True

    date_expression = _date_expression(date_column)
    conditions = [f"{date_expression} IS NOT NULL"]
    params: Dict[str, Any] = {}

    if selected_year is not None:
        conditions.append(f"EXTRACT(YEAR FROM {date_expression}) = :selected_year")
        params["selected_year"] = selected_year

    normalized_district = str(district or "").strip() or "all"
    if normalized_district != "all":
        district_column = resolved_columns.get("district") or ""
        if not district_column:
            return date_expression, conditions, params, False
        conditions.append(f"{_text_expression(district_column)} = :district")
        params["district"] = normalized_district

    return date_expression, conditions, params, True


def _collect_access_point_inputs(
    source_tables: Sequence[str],
    *,
    district: str = "all",
    selected_year: Optional[int] = None,
    metadata_items: Optional[Sequence[Dict[str, Any]]] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    records: List[Dict[str, Any]] = []
    notes: List[str] = []
    local_metadata = list(metadata_items) if metadata_items is not None else _collect_access_point_metadata(source_tables)[0]
    for metadata in local_metadata:
        try:
            records.extend(
                _load_access_point_records(
                    metadata["table_name"],
                    metadata["resolved_columns"],
                    district=district,
                    selected_year=selected_year,
                )
            )
        except Exception as exc:
            notes.append(f"{metadata['table_name']}: {exc}")
    records.sort(key=lambda item: (item["date"], item.get("district") or "", item.get("territory_label") or ""))
    return records, notes


def _load_access_point_records(
    table_name: str,
    resolved_columns: Dict[str, str],
    *,
    district: str = "all",
    selected_year: Optional[int] = None,
) -> List[Dict[str, Any]]:
    date_expression, conditions, params, scope_is_valid = _build_scope_conditions(
        resolved_columns,
        district=district,
        selected_year=selected_year,
    )
    if date_expression is None or not scope_is_valid:
        return []

    select_parts = [f"{date_expression} AS fire_date"]
    text_aliases = {
        "district": "district_value",
        "territory_label": "territory_label_value",
        "settlement": "settlement_value",
        "settlement_type": "settlement_type_value",
        "object_category": "object_category_value",
        "water_supply_details": "water_supply_details_value",
        "report_time": "report_time_value",
        "arrival_time": "arrival_time_value",
        "detection_time": "detection_time_value",
        "consequence": "consequence_value",
        "casualty_flag": "casualty_flag_value",
        "address": "address_value",
        "address_comment": "address_comment_value",
        "object_name": "object_name_value",
    }
    numeric_aliases = {
        "fire_station_distance": "fire_station_distance_value",
        "water_supply_count": "water_supply_count_value",
        "registered_damage": "registered_damage_value",
        "destroyed_buildings": "destroyed_buildings_value",
        "destroyed_area": "destroyed_area_value",
        "injuries": "injuries_value",
        "deaths": "deaths_value",
        "latitude": "latitude_value",
        "longitude": "longitude_value",
    }

    for key, alias in text_aliases.items():
        column_name = resolved_columns.get(key) or ""
        if column_name:
            select_parts.append(f"{_text_expression(column_name)} AS {alias}")
    for key, alias in numeric_aliases.items():
        column_name = resolved_columns.get(key) or ""
        if column_name:
            select_parts.append(f"{_numeric_expression_for_column(column_name)} AS {alias}")

    query = text(
        f"""
        SELECT {", ".join(select_parts)}
        FROM {_quote_identifier(table_name)}
        WHERE {" AND ".join(conditions)}
        ORDER BY fire_date
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()

    records: List[Dict[str, Any]] = []
    for row in rows:
        fire_date = row.get("fire_date")
        if fire_date is None:
            continue

        district_value = _clean_text(row.get("district_value"))
        territory_label = _pick_territory_label(row.get("territory_label_value"), district_value)
        report_time = _parse_datetime_text(row.get("report_time_value"))
        arrival_time = _parse_datetime_text(row.get("arrival_time_value"))
        detection_time = _parse_datetime_text(row.get("detection_time_value"))
        response_minutes = _calculate_response_minutes(report_time or detection_time, arrival_time)
        water_supply_count = _to_float_or_none(row.get("water_supply_count_value"))
        water_supply_details = _clean_text(row.get("water_supply_details_value"))
        registered_damage = _to_float_or_none(row.get("registered_damage_value")) or 0.0
        destroyed_buildings = _to_float_or_none(row.get("destroyed_buildings_value")) or 0.0
        destroyed_area = _to_float_or_none(row.get("destroyed_area_value")) or 0.0
        injuries = _to_float_or_none(row.get("injuries_value")) or 0.0
        deaths = _to_float_or_none(row.get("deaths_value")) or 0.0
        consequence_flag = _truthy_value(row.get("consequence_value"))
        casualty_flag = _truthy_value(row.get("casualty_flag_value"))
        incident_time = report_time or detection_time or arrival_time

        latitude = _to_float_or_none(row.get("latitude_value"))
        longitude = _to_float_or_none(row.get("longitude_value"))
        if latitude is not None and not (-90.0 <= latitude <= 90.0):
            latitude = None
        if longitude is not None and not (-180.0 <= longitude <= 180.0):
            longitude = None

        records.append(
            {
                "source_table": table_name,
                "date": fire_date,
                "year": int(fire_date.year),
                "district": district_value,
                "territory_label": territory_label,
                "settlement": _clean_text(row.get("settlement_value")),
                "settlement_type": _clean_text(row.get("settlement_type_value")),
                "object_category": _clean_text(row.get("object_category_value")),
                "address": _clean_text(row.get("address_value")),
                "address_comment": _clean_text(row.get("address_comment_value")),
                "object_name": _clean_text(row.get("object_name_value")),
                "latitude": latitude,
                "longitude": longitude,
                "fire_station_distance": _to_float_or_none(row.get("fire_station_distance_value")),
                "water_supply_count": water_supply_count,
                "water_supply_details": water_supply_details,
                "has_water_supply": _parse_water_supply_flag(water_supply_count, water_supply_details),
                "report_time": report_time,
                "arrival_time": arrival_time,
                "detection_time": detection_time,
                "response_minutes": response_minutes,
                "long_arrival": response_minutes is not None and response_minutes >= LONG_RESPONSE_THRESHOLD_MINUTES,
                "heating_season": _is_heating_season(fire_date),
                "night_incident": bool(incident_time and (incident_time.hour >= 22 or incident_time.hour < 6)),
                "registered_damage": registered_damage,
                "destroyed_buildings": destroyed_buildings,
                "destroyed_area": destroyed_area,
                "injuries": injuries,
                "deaths": deaths,
                "victims_present": bool(casualty_flag) or injuries > 0 or deaths > 0,
                "major_damage": registered_damage > 0 or destroyed_buildings > 0 or destroyed_area > 0,
                "severe_consequence": bool(consequence_flag) or injuries > 0 or deaths > 0 or registered_damage > 0 or destroyed_buildings > 0 or destroyed_area > 0,
                "risk_category_score": _risk_category_score(""),
            }
        )
    return records


def _build_option_catalog(
    source_tables: Sequence[str],
    *,
    metadata_items: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, List[Dict[str, str]]]:
    district_counter: Counter = Counter()
    year_counter: Counter = Counter()
    local_metadata = list(metadata_items) if metadata_items is not None else _collect_access_point_metadata(source_tables)[0]
    for metadata in local_metadata:
        table_name = str(metadata.get("table_name") or "")
        if not table_name:
            continue
        resolved_columns = metadata.get("resolved_columns") or {}
        district_counter.update(_load_district_counts(table_name, resolved_columns))
        year_counter.update(_load_year_counts(table_name, resolved_columns))
    return {
        "districts": _build_options_from_counter(district_counter, "Все районы"),
        "years": _build_year_options_from_counter(year_counter),
    }


def _load_district_counts(table_name: str, resolved_columns: Dict[str, str]) -> Counter:
    district_column = resolved_columns.get("district") or ""
    date_column = resolved_columns.get("date") or ""
    if not district_column or not date_column:
        return Counter()

    district_expression = _text_expression(district_column)
    date_expression = _date_expression(date_column)
    query = text(
        f"""
        SELECT {district_expression} AS option_value, COUNT(*) AS occurrences
        FROM {_quote_identifier(table_name)}
        WHERE {date_expression} IS NOT NULL AND {district_expression} IS NOT NULL
        GROUP BY option_value
        ORDER BY occurrences DESC, option_value
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query).mappings().all()

    counter: Counter = Counter()
    for row in rows:
        value = _clean_text(row.get("option_value"))
        if value:
            counter[value] += int(row.get("occurrences") or 0)
    return counter


def _load_year_counts(table_name: str, resolved_columns: Dict[str, str]) -> Counter:
    date_column = resolved_columns.get("date") or ""
    if not date_column:
        return Counter()
    date_expression = _date_expression(date_column)
    query = text(
        f"""
        SELECT CAST(EXTRACT(YEAR FROM {date_expression}) AS INTEGER) AS option_year, COUNT(*) AS occurrences
        FROM {_quote_identifier(table_name)}
        WHERE {date_expression} IS NOT NULL
        GROUP BY option_year
        ORDER BY option_year DESC
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query).mappings().all()

    counter: Counter = Counter()
    for row in rows:
        value = row.get("option_year")
        if value is not None:
            counter[str(int(value))] += int(row.get("occurrences") or 0)
    return counter


def _build_options_from_counter(counter: Counter, all_label: str) -> List[Dict[str, str]]:
    options = [{"value": "all", "label": all_label}]
    for value, _count in counter.most_common():
        if str(value).strip():
            options.append({"value": str(value), "label": str(value)})
    return options


def _build_year_options_from_counter(counter: Counter) -> List[Dict[str, str]]:
    options = [{"value": "all", "label": "Все годы"}]
    ordered_years = sorted((str(value) for value in counter if str(value).strip()), reverse=True)
    options.extend({"value": year, "label": year} for year in ordered_years)
    return options
