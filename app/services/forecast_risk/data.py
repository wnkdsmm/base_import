from __future__ import annotations

from typing import Any, Dict, List, Sequence

from sqlalchemy import inspect, text

from config.db import engine

from .constants import *
from .utils import (
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


def _collect_risk_inputs(source_tables: Sequence[str]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    metadata_items: List[Dict[str, Any]] = []
    records: List[Dict[str, Any]] = []
    notes: List[str] = []
    for source_table in source_tables:
        try:
            metadata = _load_table_metadata(source_table)
            metadata_items.append(metadata)
            records.extend(_load_risk_records(source_table, metadata["resolved_columns"]))
        except Exception as exc:
            notes.append(f"{source_table}: {exc}")
    records.sort(key=lambda item: item["date"])
    return metadata_items, records, notes



def _load_table_metadata(table_name: str) -> Dict[str, Any]:
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if table_name not in table_names:
        raise ValueError(f"Таблица '{table_name}' не найдена в базе данных.")
    columns = [column["name"] for column in inspector.get_columns(table_name)]
    resolved_columns = {
        "date": _resolve_column_name(columns, [DATE_COLUMN]),
        "district": _resolve_column_name(columns, DISTRICT_COLUMN_CANDIDATES),
        "cause": _resolve_column_name(columns, CAUSE_COLUMN_CANDIDATES),
        "object_category": _resolve_column_name(columns, [OBJECT_CATEGORY_COLUMN, "Категория объекта пожара"]),
        "temperature": _resolve_column_name(columns, TEMPERATURE_COLUMN_CANDIDATES),
        "fire_area": _resolve_column_name(columns, FIRE_AREA_COLUMN_CANDIDATES),
        "territory_label": _resolve_column_name(columns, TERRITORY_LABEL_COLUMN_CANDIDATES),
        "settlement_type": _resolve_column_name(columns, SETTLEMENT_TYPE_COLUMN_CANDIDATES),
        "building_category": _resolve_column_name(columns, BUILDING_CATEGORY_COLUMN_CANDIDATES),
        "risk_category": _resolve_column_name(columns, RISK_CATEGORY_COLUMN_CANDIDATES),
        "fire_station_distance": _resolve_column_name(columns, FIRE_STATION_DISTANCE_COLUMN_CANDIDATES),
        "water_supply_count": _resolve_column_name(columns, WATER_SUPPLY_COUNT_COLUMN_CANDIDATES),
        "water_supply_details": _resolve_column_name(columns, WATER_SUPPLY_DETAILS_COLUMN_CANDIDATES),
        "report_time": _resolve_column_name(columns, REPORT_TIME_COLUMN_CANDIDATES),
        "arrival_time": _resolve_column_name(columns, ARRIVAL_TIME_COLUMN_CANDIDATES),
        "detection_time": _resolve_column_name(columns, DETECTION_TIME_COLUMN_CANDIDATES),
        "heating_type": _resolve_column_name(columns, HEATING_TYPE_COLUMN_CANDIDATES),
        "consequence": _resolve_column_name(columns, CONSEQUENCE_COLUMN_CANDIDATES),
        "registered_damage": _resolve_column_name(columns, REGISTERED_DAMAGE_COLUMN_CANDIDATES),
        "destroyed_buildings": _resolve_column_name(columns, DESTROYED_BUILDINGS_COLUMN_CANDIDATES),
        "destroyed_area": _resolve_column_name(columns, DESTROYED_AREA_COLUMN_CANDIDATES),
        "casualty_flag": _resolve_column_name(columns, CASUALTY_FLAG_COLUMN_CANDIDATES),
        "injuries": _resolve_column_name(columns, INJURIES_COLUMN_CANDIDATES),
        "deaths": _resolve_column_name(columns, DEATHS_COLUMN_CANDIDATES),
    }
    return {"table_name": table_name, "columns": columns, "resolved_columns": resolved_columns}



def _load_risk_records(table_name: str, resolved_columns: Dict[str, str]) -> List[Dict[str, Any]]:
    date_column = resolved_columns["date"]
    if not date_column:
        return []

    select_parts = [f"{_date_expression(date_column)} AS fire_date"]
    text_aliases = {
        "district": "district_value",
        "cause": "cause_value",
        "object_category": "object_category_value",
        "territory_label": "territory_label_value",
        "settlement_type": "settlement_type_value",
        "building_category": "building_category_value",
        "risk_category": "risk_category_value",
        "water_supply_details": "water_supply_details_value",
        "report_time": "report_time_value",
        "arrival_time": "arrival_time_value",
        "detection_time": "detection_time_value",
        "heating_type": "heating_type_value",
        "consequence": "consequence_value",
        "casualty_flag": "casualty_flag_value",
    }
    numeric_aliases = {
        "temperature": "temperature_value",
        "fire_area": "fire_area_value",
        "fire_station_distance": "fire_station_distance_value",
        "water_supply_count": "water_supply_count_value",
        "registered_damage": "registered_damage_value",
        "destroyed_buildings": "destroyed_buildings_value",
        "destroyed_area": "destroyed_area_value",
        "injuries": "injuries_value",
        "deaths": "deaths_value",
    }
    for key, alias in text_aliases.items():
        if resolved_columns.get(key):
            select_parts.append(f"{_text_expression(resolved_columns[key])} AS {alias}")
    for key, alias in numeric_aliases.items():
        if resolved_columns.get(key):
            select_parts.append(f"{_numeric_expression_for_column(resolved_columns[key])} AS {alias}")

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
        district = _clean_text(row.get("district_value"))
        territory_label = _pick_territory_label(row.get("territory_label_value"), district)
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
        records.append(
            {
                "date": fire_date,
                "district": district,
                "cause": _clean_text(row.get("cause_value")),
                "object_category": _clean_text(row.get("object_category_value")),
                "territory_label": territory_label,
                "settlement_type": _clean_text(row.get("settlement_type_value")),
                "building_category": _clean_text(row.get("building_category_value")),
                "risk_category": _clean_text(row.get("risk_category_value")),
                "temperature": _to_float_or_none(row.get("temperature_value")),
                "fire_area": _to_float_or_none(row.get("fire_area_value")),
                "fire_station_distance": _to_float_or_none(row.get("fire_station_distance_value")),
                "water_supply_count": water_supply_count,
                "water_supply_details": water_supply_details,
                "has_water_supply": _parse_water_supply_flag(water_supply_count, water_supply_details),
                "report_time": report_time,
                "arrival_time": arrival_time,
                "detection_time": detection_time,
                "response_minutes": response_minutes,
                "long_arrival": response_minutes is not None and response_minutes >= LONG_RESPONSE_THRESHOLD_MINUTES,
                "heating_type": _clean_text(row.get("heating_type_value")),
                "heating_season": _is_heating_season(fire_date),
                "night_incident": incident_time.hour >= 22 or incident_time.hour < 6 if incident_time else False,
                "consequence_flag": consequence_flag,
                "casualty_flag": casualty_flag,
                "registered_damage": registered_damage,
                "destroyed_buildings": destroyed_buildings,
                "destroyed_area": destroyed_area,
                "injuries": injuries,
                "deaths": deaths,
                "victims_present": bool(casualty_flag) or injuries > 0 or deaths > 0,
                "major_damage": registered_damage > 0 or destroyed_buildings > 0 or destroyed_area > 0,
                "severe_consequence": bool(consequence_flag) or injuries > 0 or deaths > 0 or registered_damage > 0 or destroyed_buildings > 0 or destroyed_area > 0,
                "risk_category_score": _risk_category_score(_clean_text(row.get("risk_category_value"))),
            }
        )
    return records