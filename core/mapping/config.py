from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

@dataclass
class Config:
    output_dir: Path
    max_records_per_table: int = 10000

    # РРјРµРЅР° РєРѕР»РѕРЅРѕРє (РІ РїРѕСЂСЏРґРєРµ РїСЂРёРѕСЂРёС‚РµС‚Р°)
    lat_names: Tuple[str, ...] = ('С€РёСЂРѕС‚Р°', 'latitude', 'lat')
    lon_names: Tuple[str, ...] = ('РґРѕР»РіРѕС‚Р°', 'longitude', 'lon')
    
    date_names: Tuple[str, ...] = ('РґР°С‚Р° РІРѕР·РЅРёРєРЅРѕРІРµРЅРёСЏ РїРѕР¶Р°СЂР°', 'date', 'data')
    address_names: Tuple[str, ...] = ('Р°РґСЂРµСЃ', 'address')
    
    deaths_names: Tuple[str, ...] = ('РєРѕР»РёС‡РµСЃС‚РІРѕ РїРѕРіРёР±С€РёС… РІ РєСѓРї', 'deaths', 'pogibshie')
    injured_names: Tuple[str, ...] = ('РєРѕР»РёС‡РµСЃС‚РІРѕ С‚СЂР°РІРјРёСЂРѕРІР°РЅРЅС‹С… РІ РєСѓРї', 'injured', 'travmirovano')
    evacuated_names: Tuple[str, ...] = ('СЌРІР°РєСѓРёСЂРѕРІР°РЅРѕ РЅР° РїРѕР¶Р°СЂРµ', 'evacuated', 'evakuirovano')
    children_saved_names: Tuple[str, ...] = ('СЃРїР°СЃРµРЅРѕ РґРµС‚РµР№', 'children_saved', 'spaseno_detey')
    children_evacuated_names: Tuple[str, ...] = ('СЌРІР°РєСѓРёСЂРѕРІР°РЅРѕ РґРµС‚РµР№', 'children_evacuated', 'evakuirovano_detey')
    
    fire_cause_general_names: Tuple[str, ...] = ('РїСЂРёС‡РёРЅР° РїРѕР¶Р°СЂР° (РѕР±С‰Р°СЏ)', 'fire_cause_general')
    fire_cause_open_names: Tuple[str, ...] = ('РїСЂРёС‡РёРЅР° РїРѕР¶Р°СЂР° РґР»СЏ РѕС‚РєСЂС‹С‚РѕР№ С‚РµСЂСЂРёС‚РѕСЂРёРё', 'fire_cause_open')
    fire_cause_building_names: Tuple[str, ...] = ('РїСЂРёС‡РёРЅР° РїРѕР¶Р°СЂР° РґР»СЏ Р·РґР°РЅРёР№ (СЃРѕРѕСЂСѓР¶РµРЅРёР№)', 'fire_cause_building')
    building_category_names: Tuple[str, ...] = ('РєР°С‚РµРіРѕСЂРёСЏ Р·РґР°РЅРёСЏ', 'building_category')
    object_category_names: Tuple[str, ...] = ('РєР°С‚РµРіРѕСЂРёСЏ РѕР±СЉРµРєС‚Р°', 'object_category')
    object_area_names: Tuple[str, ...] = ('РѕР±С‰Р°СЏ РїР»РѕС‰Р°РґСЊ РѕР±СЉРµРєС‚Р°', 'object_area')

    district_names: Tuple[str, ...] = ('РѕРєС‚РјРѕ. С‚РµРєСЃС‚', 'С‚РµСЂСЂРёС‚РѕСЂРёР°Р»СЊРЅР°СЏ РїСЂРёРЅР°РґР»РµР¶РЅРѕСЃС‚СЊ', 'district')
    territory_names: Tuple[str, ...] = ('С‚РµСЂСЂРёС‚РѕСЂРёР°Р»СЊРЅР°СЏ РїСЂРёРЅР°РґР»РµР¶РЅРѕСЃС‚СЊ', 'territory_label')
    settlement_type_names: Tuple[str, ...] = ('РІРёРґ РЅР°СЃРµР»РµРЅРЅРѕРіРѕ РїСѓРЅРєС‚Р°', 'settlement_type')
    fire_station_distance_names: Tuple[str, ...] = ('СѓРґР°Р»РµРЅРЅРѕСЃС‚СЊ РѕС‚ Р±Р»РёР¶Р°Р№С€РµР№ РїС‡', 'СѓРґР°Р»РµРЅРЅРѕСЃС‚СЊ РґРѕ Р±Р»РёР¶Р°Р№С€РµР№ РїС‡', 'fire_station_distance')
    report_time_names: Tuple[str, ...] = ('РІСЂРµРјСЏ СЃРѕРѕР±С‰РµРЅРёСЏ', 'report_time')
    arrival_time_names: Tuple[str, ...] = ('РІСЂРµРјСЏ РїСЂРёР±С‹С‚РёСЏ 1-РіРѕ РїРї', 'arrival_time')

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
