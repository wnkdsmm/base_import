from datetime import date

import pandas as pd

from app.services.access_points.analysis import _build_access_point_rows, _select_incomplete_points
from app.services.access_points.features import _build_access_point_candidate_features


def build_access_point_record(
    *,
    date_value: date,
    year: int = 2025,
    source_table: str = "fires",
    district: str = "District",
    territory_label: str = "Territory",
    settlement: str | None = None,
    settlement_type: str = "village",
    object_category: str = "Residential",
    address: str = "Address 1",
    address_comment: str = "",
    object_name: str = "house",
    latitude: float | None = 56.1,
    longitude: float | None = 92.1,
    fire_station_distance: float | None = 12.0,
    has_water_supply: bool | None = False,
    response_minutes: float | None = 22.0,
    long_arrival: bool = True,
    heating_season: bool = True,
    night_incident: bool = False,
    victims_present: bool = False,
    major_damage: bool = False,
    severe_consequence: bool = False,
) -> dict[str, object]:
    return {
        "date": date_value,
        "year": year,
        "source_table": source_table,
        "district": district,
        "territory_label": territory_label,
        "settlement": settlement or territory_label,
        "settlement_type": settlement_type,
        "object_category": object_category,
        "address": address,
        "address_comment": address_comment,
        "object_name": object_name,
        "latitude": latitude,
        "longitude": longitude,
        "fire_station_distance": fire_station_distance,
        "has_water_supply": has_water_supply,
        "response_minutes": response_minutes,
        "long_arrival": long_arrival,
        "heating_season": heating_season,
        "night_incident": night_incident,
        "victims_present": victims_present,
        "major_damage": major_damage,
        "severe_consequence": severe_consequence,
    }
