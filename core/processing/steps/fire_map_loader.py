from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd
from sqlalchemy import text

from app.db_metadata import get_table_columns_cached
from config.db import engine
from core.mapping.config import Config
from core.types import FireMapSource

_FIRE_MAP_COLUMN_GROUPS: Tuple[str, ...] = (
    "lat_names",
    "lon_names",
    "date_names",
    "address_names",
    "deaths_names",
    "injured_names",
    "evacuated_names",
    "children_saved_names",
    "children_evacuated_names",
    "fire_cause_general_names",
    "fire_cause_open_names",
    "fire_cause_building_names",
    "building_category_names",
    "object_category_names",
    "object_area_names",
    "district_names",
    "territory_names",
    "settlement_type_names",
    "fire_station_distance_names",
    "report_time_names",
    "arrival_time_names",
)


def _normalize_column_name(value: str) -> str:
    return str(value or "").strip().lower()


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _resolve_fire_map_columns(available_columns: List[str], config: Config) -> Tuple[List[str], Dict[str, str]]:
    normalized_lookup: Dict[str, str] = {}
    for column in available_columns:
        normalized_lookup.setdefault(_normalize_column_name(column), column)

    selected_columns: List[str] = []
    matched_columns: Dict[str, str] = {}
    for group_name in _FIRE_MAP_COLUMN_GROUPS:
        for candidate in getattr(config, group_name, ()):
            matched = normalized_lookup.get(_normalize_column_name(candidate))
            if not matched:
                continue
            matched_columns[group_name] = matched
            if matched not in selected_columns:
                selected_columns.append(matched)
            break
    return selected_columns, matched_columns


def load_fire_map_source(table_name: str, config: Config) -> FireMapSource:
    normalized_table_name = str(table_name or "").strip()
    if not normalized_table_name:
        raise ValueError("Не выбрана таблица для построения карты.")
    try:
        available_columns = get_table_columns_cached(normalized_table_name)
    except ValueError as exc:
        raise ValueError(f"Таблица '{normalized_table_name}' не найдена в базе данных.") from exc
    selected_columns, matched_columns = _resolve_fire_map_columns(available_columns, config)
    if not matched_columns.get("lat_names") or not matched_columns.get("lon_names"):
        raise ValueError(
            f"Для таблицы '{normalized_table_name}' не найдены колонки координат, поэтому карта не может быть построена."
        )

    limit = max(1, int(getattr(config, "max_records_per_table", 10000) or 10000))
    selected_sql = ", ".join(_quote_identifier(column) for column in selected_columns)
    table_sql = _quote_identifier(normalized_table_name)
    query = text(f"SELECT {selected_sql} FROM {table_sql} LIMIT :limit")

    with engine.connect() as connection:
        dataframe = pd.read_sql(query, connection, params={"limit": limit})

    return {
        "dataframe": dataframe,
        "selected_columns": selected_columns,
        "matched_columns": matched_columns,
        "limit": limit,
        "table_name": normalized_table_name,
    }
