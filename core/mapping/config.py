from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
@dataclass
class Config:
    output_dir: Path
    max_records_per_table: int = 10000

    # Имена колонок (в порядке приоритета)
    lat_names: tuple[str, ...] = ('широта', 'latitude', 'lat')
    lon_names: tuple[str, ...] = ('долгота', 'longitude', 'lon')
    
    date_names: tuple[str, ...] = ('дата возникновения пожара', 'date', 'data')
    address_names: tuple[str, ...] = ('адрес', 'address')
    
    deaths_names: tuple[str, ...] = ('количество погибших в куп', 'deaths', 'pogibshie')
    injured_names: tuple[str, ...] = ('количество травмированных в куп', 'injured', 'travmirovano')
    evacuated_names: tuple[str, ...] = ('эвакуировано на пожаре', 'evacuated', 'evakuirovano')
    children_saved_names: tuple[str, ...] = ('спасено детей', 'children_saved', 'spaseno_detey')
    children_evacuated_names: tuple[str, ...] = ('эвакуировано детей', 'children_evacuated', 'evakuirovano_detey')
    
    fire_cause_general_names: tuple[str, ...] = ('причина пожара (общая)', 'fire_cause_general')
    fire_cause_open_names: tuple[str, ...] = ('причина пожара для открытой территории', 'fire_cause_open')
    fire_cause_building_names: tuple[str, ...] = ('причина пожара для зданий (сооружений)', 'fire_cause_building')
    building_category_names: tuple[str, ...] = ('категория здания', 'building_category')
    object_category_names: tuple[str, ...] = ('категория объекта', 'object_category')
    object_area_names: tuple[str, ...] = ('общая площадь объекта', 'object_area')

    district_names: tuple[str, ...] = ('октмо. текст', 'территориальная принадлежность', 'district')
    territory_names: tuple[str, ...] = ('территориальная принадлежность', 'territory_label')
    settlement_type_names: tuple[str, ...] = ('вид населенного пункта', 'settlement_type')
    fire_station_distance_names: tuple[str, ...] = ('удаленность от ближайшей пч', 'удаленность до ближайшей пч', 'fire_station_distance')
    report_time_names: tuple[str, ...] = ('время сообщения', 'report_time')
    arrival_time_names: tuple[str, ...] = ('время прибытия 1-го пп', 'arrival_time')

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
