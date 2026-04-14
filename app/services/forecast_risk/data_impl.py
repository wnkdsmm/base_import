from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text

from app.db_metadata import get_table_columns_cached
from app.services.shared.sql_helpers import build_scope_conditions, build_select_parts
from config.db import engine

from .constants import (
    ARRIVAL_TIME_COLUMN_CANDIDATES,
    BUILDING_CATEGORY_COLUMN_CANDIDATES,
    CASUALTY_FLAG_COLUMN_CANDIDATES,
    CAUSE_COLUMN_CANDIDATES,
    CONSEQUENCE_COLUMN_CANDIDATES,
    DATE_COLUMN,
    DEATHS_COLUMN_CANDIDATES,
    DESTROYED_AREA_COLUMN_CANDIDATES,
    DESTROYED_BUILDINGS_COLUMN_CANDIDATES,
    DETECTION_TIME_COLUMN_CANDIDATES,
    DISTRICT_COLUMN_CANDIDATES,
    FIRE_AREA_COLUMN_CANDIDATES,
    FIRE_STATION_DISTANCE_COLUMN_CANDIDATES,
    HEATING_TYPE_COLUMN_CANDIDATES,
    INJURIES_COLUMN_CANDIDATES,
    LONG_RESPONSE_THRESHOLD_MINUTES,
    OBJECT_CATEGORY_COLUMN,
    REGISTERED_DAMAGE_COLUMN_CANDIDATES,
    REPORT_TIME_COLUMN_CANDIDATES,
    RISK_CATEGORY_COLUMN_CANDIDATES,
    SETTLEMENT_TYPE_COLUMN_CANDIDATES,
    TEMPERATURE_COLUMN_CANDIDATES,
    TERRITORY_LABEL_COLUMN_CANDIDATES,
    WATER_SUPPLY_COUNT_COLUMN_CANDIDATES,
    WATER_SUPPLY_DETAILS_COLUMN_CANDIDATES,
)
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


def _collect_risk_metadata(source_tables: Sequence[str]) -> tuple[List[Dict[str, Any]], List[str]]:
    metadata_items: List[Dict[str, Any]] = []
    notes: List[str] = []
    for source_table in source_tables:
        try:
            metadata = _load_table_metadata(source_table)
            metadata_items.append(metadata)
        except Exception as exc:
            notes.append(f"{source_table}: {exc}")
    return metadata_items, notes


def _history_window_year_span(history_window: str) -> int:
    if history_window == "recent_3":
        return 3
    if history_window == "recent_5":
        return 5
    return 0


def _resolve_history_window_min_year(metadata_items: Sequence[Dict[str, Any]], history_window: str) -> Optional[int]:
    year_span = _history_window_year_span(history_window)
    if year_span <= 0 or not metadata_items:
        return None

    latest_years: List[int] = []
    with engine.connect() as conn:
        for metadata in metadata_items:
            resolved_columns = metadata.get("resolved_columns") or {}
            date_column = resolved_columns.get("date")
            table_name = str(metadata.get("table_name") or "")
            if not date_column or not table_name:
                continue
            date_expression = _date_expression(date_column)
            query = text(
                f"""
                SELECT MAX(EXTRACT(YEAR FROM {date_expression})) AS max_year
                FROM {_quote_identifier(table_name)}
                WHERE {date_expression} IS NOT NULL
                """
            )
            max_year = conn.execute(query).scalar()
            if max_year is not None:
                latest_years.append(int(max_year))

    if not latest_years:
        return None

    return max(latest_years) - (year_span - 1)


def _build_scope_conditions(
    resolved_columns: Dict[str, str],
    min_year: Optional[int] = None,
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    selected_year: Optional[int] = None,
) -> tuple[Optional[str], list[str], Dict[str, Any], bool]:
    return build_scope_conditions(
        resolved_columns,
        date_field="date",
        date_expression_builder=_date_expression,
        text_expression_builder=_text_expression,
        text_filters=(
            ("district", district),
            ("cause", cause),
            ("object_category", object_category),
        ),
        min_year=min_year,
        selected_year=selected_year,
        all_value="all",
    )


def _collect_risk_inputs(
    source_tables: Sequence[str],
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    history_window: str = "all",
    selected_year: Optional[int] = None,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    metadata_items, notes = _collect_risk_metadata(source_tables)
    records: List[Dict[str, Any]] = []
    min_year = _resolve_history_window_min_year(metadata_items, history_window)
    for metadata in metadata_items:
        try:
            records.extend(
                _load_risk_records(
                    metadata["table_name"],
                    metadata["resolved_columns"],
                    district=district,
                    cause=cause,
                    object_category=object_category,
                    min_year=min_year,
                    selected_year=selected_year,
                )
            )
        except Exception as exc:
            notes.append(f"{metadata['table_name']}: {exc}")
    return metadata_items, records, notes


def _load_table_metadata(table_name: str) -> Dict[str, Any]:
    try:
        columns = get_table_columns_cached(table_name)
    except ValueError as exc:
        raise ValueError(f"\u0422\u0430\u0431\u043b\u0438\u0446\u0430 '{table_name}' \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430 \u0432 \u0431\u0430\u0437\u0435 \u0434\u0430\u043d\u043d\u044b\u0445.") from exc
    resolved_columns = {
        "date": _resolve_column_name(columns, [DATE_COLUMN]),
        "district": _resolve_column_name(columns, DISTRICT_COLUMN_CANDIDATES),
        "cause": _resolve_column_name(columns, CAUSE_COLUMN_CANDIDATES),
        "object_category": _resolve_column_name(columns, [OBJECT_CATEGORY_COLUMN, "\u041a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u044f \u043e\u0431\u044a\u0435\u043a\u0442\u0430 \u043f\u043e\u0436\u0430\u0440\u0430"]),
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



def _load_risk_records(
    table_name: str,
    resolved_columns: Dict[str, str],
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    min_year: Optional[int] = None,
    selected_year: Optional[int] = None,
) -> List[Dict[str, Any]]:
    date_expression, conditions, params, scope_is_valid = _build_scope_conditions(
        resolved_columns,
        min_year=min_year,
        district=district,
        cause=cause,
        object_category=object_category,
        selected_year=selected_year,
    )
    if date_expression is None or not scope_is_valid:
        return []

    select_parts = [f"{date_expression} AS fire_date"]
    text_aliases = {
        "district": "district_value",
        "cause": "cause_value",
        "object_category": "object_category_value",
        "territory_label": "territory_label_value",
        "settlement_type": "settlement_type_value",
        "risk_category": "risk_category_value",
        "water_supply_details": "water_supply_details_value",
        "report_time": "report_time_value",
        "arrival_time": "arrival_time_value",
        "detection_time": "detection_time_value",
        "consequence": "consequence_value",
        "casualty_flag": "casualty_flag_value",
    }
    numeric_aliases = {
        "fire_station_distance": "fire_station_distance_value",
        "water_supply_count": "water_supply_count_value",
        "registered_damage": "registered_damage_value",
        "destroyed_buildings": "destroyed_buildings_value",
        "destroyed_area": "destroyed_area_value",
        "injuries": "injuries_value",
        "deaths": "deaths_value",
    }
    select_parts.extend(
        build_select_parts(
            resolved_columns,
            text_aliases=text_aliases,
            numeric_aliases=numeric_aliases,
            text_expression_builder=_text_expression,
            numeric_expression_builder=_numeric_expression_for_column,
        )
    )

    query = text(
        f"""
        SELECT {", ".join(select_parts)}
        FROM {_quote_identifier(table_name)}
        WHERE {" AND ".join(conditions)}
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()

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
        cause_value = _clean_text(row.get("cause_value"))
        object_category_value = _clean_text(row.get("object_category_value"))
        settlement_type_value = _clean_text(row.get("settlement_type_value"))
        risk_category_value = _clean_text(row.get("risk_category_value"))
        records.append(
            {
                "date": fire_date,
                "district": district,
                "cause": cause_value,
                "object_category": object_category_value,
                "territory_label": territory_label,
                "settlement_type": settlement_type_value,
                "fire_station_distance": _to_float_or_none(row.get("fire_station_distance_value")),
                "has_water_supply": _parse_water_supply_flag(water_supply_count, water_supply_details),
                "response_minutes": response_minutes,
                "long_arrival": response_minutes is not None and response_minutes >= LONG_RESPONSE_THRESHOLD_MINUTES,
                "heating_season": _is_heating_season(fire_date),
                "night_incident": incident_time.hour >= 22 or incident_time.hour < 6 if incident_time else False,
                "victims_present": bool(casualty_flag) or injuries > 0 or deaths > 0,
                "major_damage": registered_damage > 0 or destroyed_buildings > 0 or destroyed_area > 0,
                "severe_consequence": bool(consequence_flag) or injuries > 0 or deaths > 0 or registered_damage > 0 or destroyed_buildings > 0 or destroyed_area > 0,
                "risk_category_score": _risk_category_score(risk_category_value),
            }
        )
    return records
