from __future__ import annotations

from collections import Counter
from typing import Any, Sequence

from sqlalchemy import text

from app.db_metadata import get_table_columns_cached
from app.services.shared.sql_helpers import select_expression_or_fallback
from app.services.shared.data_utils import (
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
    _text_expression,
    _to_float_or_none,
    _truthy_value,
)
from app.table_catalog import get_user_table_options, resolve_selected_table_value
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
from .types import (
    RawPointRow,
    AccessPointInput,
    AccessPointMetadata,
    AccessPointsDataPayload,
    AccessPointsSummary,
    ConsequenceSummary,
    OptionItem,
    PointRecord,
    PriorityRow,
    ResolvedColumns,
    ResponseSummary,
    WaterSupplySummary,
)


def _build_access_points_table_options() -> list[OptionItem]:
    return [{"value": "all", "label": "\u0412\u0441\u0435 \u0442\u0430\u0431\u043b\u0438\u0446\u044b"}, *get_user_table_options(prefer_clean=True)]


def _resolve_selected_table(table_options: Sequence[OptionItem], table_name: str) -> str:
    return resolve_selected_table_value(table_options, table_name, fallback_value="all")


def _selected_source_tables(table_options: Sequence[OptionItem], selected_table: str) -> list[str]:
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


def _resolve_option_value(options: Sequence[OptionItem], selected_value: object, default: str = "all") -> str:
    normalized = str(selected_value or "").strip() or default
    available = {str(option.get("value") or "") for option in options}
    if normalized in available:
        return normalized
    return str(options[0].get("value") or default) if options else default


def _load_table_metadata(table_name: str) -> AccessPointMetadata:
    try:
        columns = get_table_columns_cached(table_name)
    except ValueError as exc:
        raise ValueError(f"\u0422\u0430\u0431\u043b\u0438\u0446\u0430 '{table_name}' \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430 \u0432 \u0431\u0430\u0437\u0435 \u0434\u0430\u043d\u043d\u044b\u0445.") from exc

    resolved_columns = {
        "district": _resolve_column_name(columns, DISTRICT_COLUMN_CANDIDATES),
        "settlement": _resolve_column_name(columns, SETTLEMENT_COLUMN_CANDIDATES),
        "settlement_type": _resolve_column_name(columns, SETTLEMENT_TYPE_COLUMN_CANDIDATES),
        "territory_label": _resolve_column_name(columns, TERRITORY_LABEL_COLUMN_CANDIDATES),
        "object_category": _resolve_column_name(columns, OBJECT_CATEGORY_COLUMN_CANDIDATES),
        "object_name": _resolve_column_name(columns, OBJECT_NAME_COLUMN_CANDIDATES),
        "address": _resolve_column_name(columns, ADDRESS_COLUMN_CANDIDATES),
        "address_comment": _resolve_column_name(columns, ADDRESS_COMMENT_COLUMN_CANDIDATES),
        "latitude": _resolve_column_name(columns, LATITUDE_COLUMN_CANDIDATES),
        "longitude": _resolve_column_name(columns, LONGITUDE_COLUMN_CANDIDATES),
        "report_time": _resolve_column_name(columns, REPORT_TIME_COLUMN_CANDIDATES),
        "arrival_time": _resolve_column_name(columns, ARRIVAL_TIME_COLUMN_CANDIDATES),
        "detection_time": _resolve_column_name(columns, DETECTION_TIME_COLUMN_CANDIDATES),
        "distance_to_fire_station": _resolve_column_name(columns, FIRE_STATION_DISTANCE_COLUMN_CANDIDATES),
        "water_supply_count": _resolve_column_name(columns, WATER_SUPPLY_COUNT_COLUMN_CANDIDATES),
        "water_supply_details": _resolve_column_name(columns, WATER_SUPPLY_DETAILS_COLUMN_CANDIDATES),
        "consequence": _resolve_column_name(columns, CONSEQUENCE_COLUMN_CANDIDATES),
        "deaths": _resolve_column_name(columns, DEATHS_COLUMN_CANDIDATES),
        "injuries": _resolve_column_name(columns, INJURIES_COLUMN_CANDIDATES),
        "casualty_flag": _resolve_column_name(columns, CASUALTY_FLAG_COLUMN_CANDIDATES),
        "destroyed_area": _resolve_column_name(columns, DESTROYED_AREA_COLUMN_CANDIDATES),
        "destroyed_buildings": _resolve_column_name(columns, DESTROYED_BUILDINGS_COLUMN_CANDIDATES),
        "registered_damage": _resolve_column_name(columns, REGISTERED_DAMAGE_COLUMN_CANDIDATES),
    }
    return {"table_name": table_name, "columns": columns, "resolved_columns": resolved_columns}


def _optional_text_expression(column_name: str | None, fallback: str = "''") -> str:
    return select_expression_or_fallback(
        column_name,
        _text_expression,
        fallback=fallback,
    )


def _optional_numeric_expression(column_name: str | None, fallback: str = "NULL") -> str:
    return select_expression_or_fallback(
        column_name,
        _numeric_expression_for_column,
        fallback=fallback,
    )


def _build_source_sql(table_name: str, resolved_columns: ResolvedColumns) -> str:
    district_expr = _optional_text_expression(resolved_columns["district"], fallback="''")
    settlement_expr = _optional_text_expression(resolved_columns["settlement"], fallback="''")
    settlement_type_expr = _optional_text_expression(resolved_columns["settlement_type"], fallback="''")
    territory_expr = _optional_text_expression(resolved_columns["territory_label"], fallback="''")
    object_category_expr = _optional_text_expression(resolved_columns["object_category"], fallback="''")
    object_name_expr = _optional_text_expression(resolved_columns["object_name"], fallback="''")
    address_expr = _optional_text_expression(resolved_columns["address"], fallback="''")
    address_comment_expr = _optional_text_expression(resolved_columns["address_comment"], fallback="''")
    latitude_expr = _optional_numeric_expression(resolved_columns["latitude"], fallback="NULL")
    longitude_expr = _optional_numeric_expression(resolved_columns["longitude"], fallback="NULL")
    report_expr = _optional_text_expression(resolved_columns["report_time"], fallback="''")
    arrival_expr = _optional_text_expression(resolved_columns["arrival_time"], fallback="''")
    detection_expr = _optional_text_expression(resolved_columns["detection_time"], fallback="''")
    distance_expr = _optional_numeric_expression(resolved_columns["distance_to_fire_station"], fallback="NULL")
    water_supply_count_expr = _optional_numeric_expression(resolved_columns["water_supply_count"], fallback="NULL")
    water_supply_details_expr = _optional_text_expression(resolved_columns["water_supply_details"], fallback="''")
    consequence_expr = _optional_text_expression(resolved_columns["consequence"], fallback="''")
    deaths_expr = _optional_numeric_expression(resolved_columns["deaths"], fallback="NULL")
    injuries_expr = _optional_numeric_expression(resolved_columns["injuries"], fallback="NULL")
    casualty_flag_expr = _optional_text_expression(resolved_columns["casualty_flag"], fallback="''")
    destroyed_area_expr = _optional_numeric_expression(resolved_columns["destroyed_area"], fallback="NULL")
    destroyed_buildings_expr = _optional_numeric_expression(resolved_columns["destroyed_buildings"], fallback="NULL")
    registered_damage_expr = _optional_numeric_expression(resolved_columns["registered_damage"], fallback="NULL")
    date_expr = _date_expression(DATE_COLUMN)

    return f"""
        SELECT
            {district_expr} AS district,
            {territory_expr} AS territory_label,
            {settlement_expr} AS settlement,
            {settlement_type_expr} AS settlement_type,
            {object_category_expr} AS object_category,
            {object_name_expr} AS object_name,
            {address_expr} AS address,
            {address_comment_expr} AS address_comment,
            {latitude_expr} AS latitude,
            {longitude_expr} AS longitude,
            {report_expr} AS report_time,
            {arrival_expr} AS arrival_time,
            {detection_expr} AS detection_time,
            {distance_expr} AS distance_to_fire_station,
            {water_supply_count_expr} AS water_supply_count,
            {water_supply_details_expr} AS water_supply_details,
            {consequence_expr} AS consequence,
            {deaths_expr} AS deaths,
            {injuries_expr} AS injuries,
            {casualty_flag_expr} AS casualty_flag,
            {destroyed_area_expr} AS destroyed_area,
            {destroyed_buildings_expr} AS destroyed_buildings,
            {registered_damage_expr} AS registered_damage,
            {date_expr} AS event_date
        FROM {_quote_identifier(table_name)}
    """


def _normalize_record(row: RawPointRow) -> PointRecord | None:
    district = _clean_text(row.get("district"))
    territory_label = _clean_text(row.get("territory_label"))
    settlement = _clean_text(row.get("settlement"))
    settlement_type = _clean_text(row.get("settlement_type"))
    object_category = _clean_text(row.get("object_category"))
    object_name = _clean_text(row.get("object_name"))
    address = _clean_text(row.get("address"))
    address_comment = _clean_text(row.get("address_comment"))
    latitude = _to_float_or_none(row.get("latitude"))
    longitude = _to_float_or_none(row.get("longitude"))
    report_time = _parse_datetime_text(row.get("report_time"))
    arrival_time = _parse_datetime_text(row.get("arrival_time"))
    detection_time = _parse_datetime_text(row.get("detection_time"))
    distance_to_fire_station = _to_float_or_none(row.get("distance_to_fire_station"))
    water_supply_count = _to_float_or_none(row.get("water_supply_count"))
    water_supply_details = _clean_text(row.get("water_supply_details"))
    consequence = _clean_text(row.get("consequence"))
    deaths = _to_float_or_none(row.get("deaths"))
    injuries = _to_float_or_none(row.get("injuries"))
    casualty_flag = _truthy_value(row.get("casualty_flag"))
    destroyed_area = _to_float_or_none(row.get("destroyed_area"))
    destroyed_buildings = _to_float_or_none(row.get("destroyed_buildings"))
    registered_damage = _to_float_or_none(row.get("registered_damage"))
    event_date = _parse_datetime_text(row.get("event_date"))

    if latitude is None or longitude is None:
        return None

    return {
        "district": district,
        "territory_label": _pick_territory_label(territory_label, district),
        "settlement": settlement,
        "settlement_type": settlement_type,
        "object_category": object_category,
        "object_name": object_name,
        "address": address,
        "address_comment": address_comment,
        "latitude": latitude,
        "longitude": longitude,
        "report_time": report_time,
        "arrival_time": arrival_time,
        "detection_time": detection_time,
        "distance_to_fire_station": distance_to_fire_station,
        "water_supply_count": water_supply_count,
        "water_supply_details": water_supply_details,
        "consequence": consequence,
        "deaths": deaths,
        "injuries": injuries,
        "casualty_flag": casualty_flag,
        "destroyed_area": destroyed_area,
        "destroyed_buildings": destroyed_buildings,
        "registered_damage": registered_damage,
        "event_date": event_date,
    }


def _collect_source_records(table_name: str) -> list[PointRecord]:
    metadata = _load_table_metadata(table_name)
    query = text(_build_source_sql(metadata["table_name"], metadata["resolved_columns"]))
    with engine.connect() as conn:
        rows = [dict(row._mapping) for row in conn.execute(query)]

    normalized = []
    for row in rows:
        record = _normalize_record(row)
        if record is not None:
            normalized.append(record)
    return normalized


def _filter_source_records(
    records: Sequence[PointRecord],
    *,
    district: str = "all",
    year: str = "all",
) -> list[PointRecord]:
    normalized_district = str(district or "").strip().lower()
    normalized_year = str(year or "").strip()

    filtered = list(records)
    if normalized_district and normalized_district != "all":
        filtered = [record for record in filtered if _clean_text(record.get("district")).lower() == normalized_district]

    if normalized_year and normalized_year != "all":
        filtered = [
            record
            for record in filtered
            if record.get("event_date") is not None and str(record["event_date"].year) == normalized_year
        ]

    return filtered


def _collect_available_districts(records: Sequence[PointRecord]) -> list[OptionItem]:
    districts = sorted({record["district"] for record in records if _clean_text(record.get("district"))})
    return [{"value": "all", "label": "Все районы"}, *({"value": district, "label": district} for district in districts)]


def _collect_available_years(records: Sequence[PointRecord]) -> list[OptionItem]:
    years = sorted(
        {
            str(record["event_date"].year)
            for record in records
            if record.get("event_date") is not None
        },
        reverse=True,
    )
    return [{"value": "all", "label": "Все годы"}, *({"value": year, "label": year} for year in years)]


def _summarize_consequences(records: Sequence[PointRecord]) -> ConsequenceSummary:
    fires_with_consequences = sum(
        1
        for record in records
        if _truthy_value(record.get("casualty_flag"))
        or _to_float_or_none(record.get("deaths"))
        or _to_float_or_none(record.get("injuries"))
        or _clean_text(record.get("consequence"))
    )
    return {
        "records_with_consequences": fires_with_consequences,
        "deaths": sum(int(record.get("deaths") or 0) for record in records if record.get("deaths")),
        "injuries": sum(int(record.get("injuries") or 0) for record in records if record.get("injuries")),
    }


def _summarize_water_supply(records: Sequence[PointRecord]) -> WaterSupplySummary:
    flags = [
        _parse_water_supply_flag(record.get("water_supply_count"), _clean_text(record.get("water_supply_details")))
        for record in records
    ]
    with_water_supply = sum(1 for flag in flags if flag is True)
    without_water_supply = sum(1 for flag in flags if flag is False)
    return {
        "with_water_supply": with_water_supply,
        "without_water_supply": without_water_supply,
    }


def _summarize_response(records: Sequence[PointRecord]) -> ResponseSummary:
    response_minutes = [
        minutes
        for minutes in (
            _calculate_response_minutes(
                record.get("report_time") or record.get("detection_time"),
                record.get("arrival_time"),
            )
            for record in records
        )
        if minutes is not None
    ]
    long_response_count = sum(1 for minutes in response_minutes if minutes >= LONG_RESPONSE_THRESHOLD_MINUTES)
    average_response_minutes = round(sum(response_minutes) / len(response_minutes), 1) if response_minutes else None
    return {
        "response_minutes": response_minutes,
        "long_response_count": long_response_count,
        "average_response_minutes": average_response_minutes,
    }


def _build_priority_rows(records: Sequence[PointRecord]) -> list[PriorityRow]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for record in records:
        key = (record.get("district") or "", record.get("territory_label") or "")
        grouped.setdefault(key, []).append(record)

    rows = []
    for (district, label), items in grouped.items():
        response_summary = _summarize_response(items)
        consequences = _summarize_consequences(items)
        water_supply = _summarize_water_supply(items)
        fire_count = len(items)
        long_response_count = response_summary["long_response_count"]
        risk_score = fire_count + long_response_count * 1.5 + consequences["records_with_consequences"] * 2 + water_supply["without_water_supply"]

        rows.append(
            {
                "district": district,
                "label": label or district,
                "fire_count": fire_count,
                "risk_score": round(risk_score, 1),
                "long_response_count": long_response_count,
                "average_response_minutes": response_summary["average_response_minutes"],
                "records_with_consequences": consequences["records_with_consequences"],
                "deaths": consequences["deaths"],
                "injuries": consequences["injuries"],
                "with_water_supply": water_supply["with_water_supply"],
                "without_water_supply": water_supply["without_water_supply"],
                "heating_season_fires": sum(1 for item in items if _is_heating_season(item.get("event_date"))),
                "top_object_category": Counter(
                    _clean_text(item.get("object_category")) or "�� �������"
                    for item in items
                ).most_common(1)[0][0],
            }
        )

    rows.sort(key=lambda item: (-item["risk_score"], -item["fire_count"], item["label"]))
    return rows


def get_access_points_data(
    *,
    table_name: str = "all",
    district: str = "all",
    year: str = "all",
    limit: str = "25",
    feature_columns: Sequence[str] | None = None,
) -> AccessPointsDataPayload:
    del feature_columns

    table_options = _build_access_points_table_options()
    selected_table = _resolve_selected_table(table_options, table_name)
    source_tables = _selected_source_tables(table_options, selected_table)
    parsed_limit = _parse_limit(limit)

    if not source_tables:
        return {
            "table_options": table_options,
            "selected_table": selected_table,
            "selected_table_label": next((item["label"] for item in table_options if item["value"] == selected_table), "��� �������"),
            "district_options": [{"value": "all", "label": "��� ������"}],
            "year_options": [{"value": "all", "label": "��� ����"}],
            "selected_district": "all",
            "selected_year": "all",
            "summary": {"total_points": 0, "total_points_display": "0"},
            "points": [],
            "notes": ["\u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b\u0445 \u0442\u0430\u0431\u043b\u0438\u0446 \u0434\u043b\u044f \u0430\u043d\u0430\u043b\u0438\u0437\u0430 \u043f\u0440\u043e\u0431\u043b\u0435\u043c\u043d\u044b\u0445 \u0442\u043e\u0447\u0435\u043a."],
        }

    all_records: list[dict[str, Any]] = []
    for source_table in source_tables:
        all_records.extend(_collect_source_records(source_table))

    district_options = _collect_available_districts(all_records)
    year_options = _collect_available_years(all_records)
    selected_district = _resolve_option_value(district_options, district, default="all")
    selected_year = _resolve_option_value(year_options, year, default="all")
    filtered_records = _filter_source_records(all_records, district=selected_district, year=selected_year)
    priority_rows = _build_priority_rows(filtered_records)[:parsed_limit]

    return {
        "table_options": table_options,
        "selected_table": selected_table,
        "selected_table_label": next((item["label"] for item in table_options if item["value"] == selected_table), "��� �������"),
        "district_options": district_options,
        "year_options": year_options,
        "selected_district": selected_district,
        "selected_year": selected_year,
        "summary": {
            "total_points": len(priority_rows),
            "total_points_display": str(len(priority_rows)),
        },
        "points": priority_rows,
        "notes": [],
    }


def _collect_access_point_metadata(source_tables: Sequence[str]) -> tuple[list[AccessPointMetadata], list[str]]:
    metadata_items: list[dict[str, Any]] = []
    notes: list[str] = []
    for table_name in source_tables:
        try:
            metadata = _load_table_metadata(table_name)
            metadata_items.append({
                "table_name": table_name,
                "columns": list(metadata["columns"]),
                "resolved_columns": dict(metadata["resolved_columns"]),
            })
        except Exception as exc:
            notes.append(str(exc))
    return metadata_items, notes


def _record_to_access_point_input(record: PointRecord, *, source_table: str) -> AccessPointInput:
    event_date = record.get("event_date")
    response_minutes = _calculate_response_minutes(
        record.get("report_time") or record.get("detection_time"),
        record.get("arrival_time"),
    )
    has_water_supply = _parse_water_supply_flag(
        record.get("water_supply_count"),
        _clean_text(record.get("water_supply_details")),
    )
    deaths = int(record.get("deaths") or 0) if record.get("deaths") is not None else 0
    injuries = int(record.get("injuries") or 0) if record.get("injuries") is not None else 0
    return {
        **record,
        "source_table": source_table,
        "date": event_date,
        "year": event_date.year if event_date is not None else None,
        "response_minutes": response_minutes,
        "fire_station_distance": record.get("distance_to_fire_station"),
        "long_arrival": bool(response_minutes is not None and response_minutes >= LONG_RESPONSE_THRESHOLD_MINUTES),
        "has_water_supply": has_water_supply,
        "severe_consequence": bool(_clean_text(record.get("consequence")) or deaths or injuries),
        "victims_present": bool(deaths or injuries or record.get("casualty_flag")),
        "major_damage": bool(
            (record.get("destroyed_area") or 0) > 0
            or (record.get("destroyed_buildings") or 0) > 0
            or (record.get("registered_damage") or 0) > 0
        ),
        "night_incident": bool(record.get("report_time") is not None and int(record["report_time"].hour) < 6),
        "heating_season": _is_heating_season(event_date),
    }


def _collect_access_point_inputs(
    source_tables: Sequence[str],
    *,
    district: str = "all",
    selected_year: int | None = None,
    metadata_items: Sequence[AccessPointMetadata | None] = None,
) -> tuple[list[AccessPointInput], list[str]]:
    del metadata_items
    records: list[dict[str, Any]] = []
    notes: list[str] = []
    for table_name in source_tables:
        try:
            raw_records = _collect_source_records(table_name)
        except Exception as exc:
            notes.append(str(exc))
            continue
        for record in raw_records:
            records.append(_record_to_access_point_input(record, source_table=table_name))
    selected_district = str(district or "").strip().lower()
    if selected_district and selected_district != "all":
        records = [record for record in records if _clean_text(record.get("district")).lower() == selected_district]
    if selected_year is not None:
        records = [record for record in records if record.get("year") == int(selected_year)]
    return records, notes


def _build_option_catalog(
    source_tables: Sequence[str],
    *,
    metadata_items: Sequence[AccessPointMetadata | None] = None,
) -> dict[str, list[OptionItem]]:
    del metadata_items
    records, _notes = _collect_access_point_inputs(source_tables)
    return {
        "districts": _collect_available_districts(records),
        "years": _collect_available_years(records),
    }

