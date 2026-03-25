from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

@dataclass
class Config:
    output_dir: Path
    max_records_per_table: int = 10000

    # Имена колонок (в порядке приоритета)
    lat_names: Tuple[str, ...] = ('широта', 'latitude', 'lat')
    lon_names: Tuple[str, ...] = ('долгота', 'longitude', 'lon')
    
    date_names: Tuple[str, ...] = ('дата возникновения пожара', 'date', 'data')
    address_names: Tuple[str, ...] = ('адрес', 'address')
    
    deaths_names: Tuple[str, ...] = ('количество погибших в куп', 'deaths', 'pogibshie')
    injured_names: Tuple[str, ...] = ('количество травмированных в куп', 'injured', 'travmirovano')
    evacuated_names: Tuple[str, ...] = ('эвакуировано на пожаре', 'evacuated', 'evakuirovano')
    children_saved_names: Tuple[str, ...] = ('спасено детей', 'children_saved', 'spaseno_detey')
    children_evacuated_names: Tuple[str, ...] = ('эвакуировано детей', 'children_evacuated', 'evakuirovano_detey')
    
    fire_cause_general_names: Tuple[str, ...] = ('причина пожара (общая)', 'fire_cause_general')
    fire_cause_open_names: Tuple[str, ...] = ('причина пожара для открытой территории', 'fire_cause_open')
    fire_cause_building_names: Tuple[str, ...] = ('причина пожара для зданий (сооружений)', 'fire_cause_building')
    building_category_names: Tuple[str, ...] = ('категория здания', 'building_category')
    object_category_names: Tuple[str, ...] = ('категория объекта', 'object_category')
    object_area_names: Tuple[str, ...] = ('общая площадь объекта', 'object_area')

    district_names: Tuple[str, ...] = ('октмо. текст', 'территориальная принадлежность', 'district')
    territory_names: Tuple[str, ...] = ('территориальная принадлежность', 'territory_label')
    settlement_type_names: Tuple[str, ...] = ('вид населенного пункта', 'settlement_type')
    fire_station_distance_names: Tuple[str, ...] = ('удаленность от ближайшей пч', 'удаленность до ближайшей пч', 'fire_station_distance')
    report_time_names: Tuple[str, ...] = ('время сообщения', 'report_time')
    arrival_time_names: Tuple[str, ...] = ('время прибытия 1-го пп', 'arrival_time')

    def __post_init__(self):
        self.output_dir.mkdir(exist_ok=True)


# =====================================================
# COLUMN FINDER
# =====================================================

@dataclass
class MarkerStyle:
    color: str
    stroke: str
    icon: str
    label: str
    radius: int = 8


# =====================================================
# MAP CREATOR
# =====================================================

__all__ = ["Config", "MarkerStyle"]