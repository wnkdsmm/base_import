from __future__ import annotations

from app.domain.fire_columns import (
    APARTMENTS_DAMAGED_COLUMN,
    APARTMENTS_DESTROYED_COLUMN,
    APART_HOTEL_DAMAGED_COLUMN,
    APART_HOTEL_DESTROYED_COLUMN,
    AREA_COLUMN,
    AREA_DAMAGED_COLUMN,
    AREA_DESTROYED_COLUMN,
    BIRDS_DESTROYED_COLUMN,
    BUILDINGS_DAMAGED_COLUMN,
    BUILDINGS_DESTROYED_COLUMN,
    BUILDING_CATEGORY_COLUMN,
    CAUSE_COLUMNS,
    FEED_DAMAGED_COLUMN,
    FEED_DESTROYED_COLUMN,
    FIRE_STATION_DISTANCE_COLUMN,
    GENERAL_CAUSE_COLUMN,
    GRAIN_DAMAGED_COLUMN,
    GRAIN_DESTROYED_COLUMN,
    LARGE_CATTLE_DESTROYED_COLUMN,
    OBJECT_CATEGORY_COLUMN,
    OBJECT_NAME_COLUMN,
    OPEN_AREA_CAUSE_COLUMN,
    REGISTERED_DAMAGE_COLUMN,
    RISK_CATEGORY_COLUMN,
    SMALL_CATTLE_DESTROYED_COLUMN,
    TECH_CROPS_DAMAGED_COLUMN,
    TECH_CROPS_DESTROYED_COLUMN,
    VEHICLES_DAMAGED_COLUMN,
    VEHICLES_DESTROYED_COLUMN,
    BUILDING_CAUSE_COLUMN,
    DATE_COLUMN,
)

DISTRIBUTION_GROUPS = [
    (
        "РџСЂРёС‡РёРЅС‹",
        [
            GENERAL_CAUSE_COLUMN,
            OPEN_AREA_CAUSE_COLUMN,
            BUILDING_CAUSE_COLUMN,
        ],
    ),
    (
        "РћР±СЉРµРєС‚ Рё Р»РѕРєР°С†РёСЏ",
        [
            AREA_COLUMN,
            FIRE_STATION_DISTANCE_COLUMN,
            OBJECT_NAME_COLUMN,
            RISK_CATEGORY_COLUMN,
            BUILDING_CATEGORY_COLUMN,
            OBJECT_CATEGORY_COLUMN,
        ],
    ),
    (
        "РЈС‰РµСЂР±",
        [
            REGISTERED_DAMAGE_COLUMN,
            BUILDINGS_DESTROYED_COLUMN,
            BUILDINGS_DAMAGED_COLUMN,
            APARTMENTS_DESTROYED_COLUMN,
            APARTMENTS_DAMAGED_COLUMN,
            APART_HOTEL_DESTROYED_COLUMN,
            APART_HOTEL_DAMAGED_COLUMN,
            AREA_DESTROYED_COLUMN,
            AREA_DAMAGED_COLUMN,
            VEHICLES_DESTROYED_COLUMN,
            VEHICLES_DAMAGED_COLUMN,
            GRAIN_DESTROYED_COLUMN,
            GRAIN_DAMAGED_COLUMN,
            FEED_DESTROYED_COLUMN,
            FEED_DAMAGED_COLUMN,
            TECH_CROPS_DESTROYED_COLUMN,
            TECH_CROPS_DAMAGED_COLUMN,
            LARGE_CATTLE_DESTROYED_COLUMN,
            SMALL_CATTLE_DESTROYED_COLUMN,
            BIRDS_DESTROYED_COLUMN,
        ],
    ),
]

DISTRIBUTION_COLUMNS = [
    column_name
    for _, columns in DISTRIBUTION_GROUPS
    for column_name in columns
]

IMPACT_METRIC_CONFIG = {
    "deaths": {
        "label": "РџРѕРіРёР±С€РёРµ",
        "preferred": ["РљРѕР»РёС‡РµСЃС‚РІРѕ РїРѕРіРёР±С€РёС… РІ РљРЈРџ"],
        "include_any": [["РїРѕРіРёР±С€"], ["СЃРјРµСЂС‚"], ["РіРёР±РµР»"]],
        "exclude": ["РїСЂРёС‡РёРЅ", "РјРµСЃС‚Рѕ", "РјРѕРјРµРЅС‚", "С„РёРѕ", "РґР°С‚Р°", "РІРѕР·СЂР°СЃС‚", "РїРѕР»", "СѓРі#", "СѓРі_", "СЃРѕС‚СЂСѓРґРЅРёРє"],
        "tone": "fire",
    },
    "injuries": {
        "label": "РўСЂР°РІРјРёСЂРѕРІР°РЅРЅС‹Рµ",
        "preferred": ["РљРѕР»РёС‡РµСЃС‚РІРѕ С‚СЂР°РІРјРёСЂРѕРІР°РЅРЅС‹С… РІ РљРЈРџ"],
        "include_any": [["С‚СЂР°РІРј"]],
        "exclude": ["РІРёРґ", "РјРµСЃС‚Рѕ", "С„РёРѕ", "РґР°С‚Р°", "РІРѕР·СЂР°СЃС‚", "РїРѕР»", "СѓС‚#", "СѓС‚_", "СЃРѕС‚СЂСѓРґРЅРёРє"],
        "tone": "sand",
    },
    "evacuated": {
        "label": "Р­РІР°РєСѓРёСЂРѕРІР°РЅРѕ",
        "preferred": ["Р­РІР°РєСѓРёСЂРѕРІР°РЅРѕ РЅР° РїРѕР¶Р°СЂРµ"],
        "include_any": [["СЌРІР°РєСѓ"]],
        "exclude": ["РґРµС‚"],
        "tone": "sky",
    },
    "evacuated_children": {
        "label": "Р­РІР°РєСѓРёСЂРѕРІР°РЅРѕ РґРµС‚РµР№",
        "preferred": ["Р­РІР°РєСѓРёСЂРѕРІР°РЅРѕ РґРµС‚РµР№"],
        "include_all": [["СЌРІР°РєСѓ", "РґРµС‚"]],
        "exclude": [],
        "tone": "sky",
    },
    "rescued_total": {
        "label": "РЎРїР°СЃРµРЅРѕ",
        "preferred": ["РЎРїР°СЃРµРЅРѕ РЅР° РїРѕР¶Р°СЂРµ"],
        "include_any": [["СЃРїР°СЃ"]],
        "exclude": ["РґРµС‚", "Р·РґР°РЅ", "СЃРѕРѕСЂСѓР¶", "СЃРєРѕС‚", "С‚РµС…РЅРёРє", "РјР°С‚РµСЂРёР°Р»", "С†РµРЅРЅРѕСЃС‚"],
        "tone": "forest",
    },
    "rescued_children": {
        "label": "РЎРїР°СЃРµРЅРѕ РґРµС‚РµР№",
        "preferred": ["РЎРїР°СЃРµРЅРѕ РґРµС‚РµР№"],
        "include_all": [["СЃРїР°СЃ", "РґРµС‚"]],
        "exclude": [],
        "tone": "forest",
    },
}

COLUMN_LABELS = {
    GENERAL_CAUSE_COLUMN: GENERAL_CAUSE_COLUMN,
    OPEN_AREA_CAUSE_COLUMN: OPEN_AREA_CAUSE_COLUMN,
    BUILDING_CAUSE_COLUMN: BUILDING_CAUSE_COLUMN,
    BUILDING_CATEGORY_COLUMN: BUILDING_CATEGORY_COLUMN,
    RISK_CATEGORY_COLUMN: RISK_CATEGORY_COLUMN,
    AREA_COLUMN: AREA_COLUMN,
    FIRE_STATION_DISTANCE_COLUMN: FIRE_STATION_DISTANCE_COLUMN,
    OBJECT_NAME_COLUMN: OBJECT_NAME_COLUMN,
    OBJECT_CATEGORY_COLUMN: OBJECT_CATEGORY_COLUMN,
    REGISTERED_DAMAGE_COLUMN: REGISTERED_DAMAGE_COLUMN,
    BUILDINGS_DESTROYED_COLUMN: BUILDINGS_DESTROYED_COLUMN,
    BUILDINGS_DAMAGED_COLUMN: BUILDINGS_DAMAGED_COLUMN,
    APARTMENTS_DESTROYED_COLUMN: APARTMENTS_DESTROYED_COLUMN,
    APARTMENTS_DAMAGED_COLUMN: APARTMENTS_DAMAGED_COLUMN,
    APART_HOTEL_DESTROYED_COLUMN: APART_HOTEL_DESTROYED_COLUMN,
    APART_HOTEL_DAMAGED_COLUMN: APART_HOTEL_DAMAGED_COLUMN,
    AREA_DESTROYED_COLUMN: AREA_DESTROYED_COLUMN,
    AREA_DAMAGED_COLUMN: AREA_DAMAGED_COLUMN,
    VEHICLES_DESTROYED_COLUMN: VEHICLES_DESTROYED_COLUMN,
    VEHICLES_DAMAGED_COLUMN: VEHICLES_DAMAGED_COLUMN,
    GRAIN_DESTROYED_COLUMN: GRAIN_DESTROYED_COLUMN,
    GRAIN_DAMAGED_COLUMN: GRAIN_DAMAGED_COLUMN,
    FEED_DESTROYED_COLUMN: FEED_DESTROYED_COLUMN,
    FEED_DAMAGED_COLUMN: FEED_DAMAGED_COLUMN,
    TECH_CROPS_DESTROYED_COLUMN: TECH_CROPS_DESTROYED_COLUMN,
    TECH_CROPS_DAMAGED_COLUMN: TECH_CROPS_DAMAGED_COLUMN,
    LARGE_CATTLE_DESTROYED_COLUMN: LARGE_CATTLE_DESTROYED_COLUMN,
    SMALL_CATTLE_DESTROYED_COLUMN: SMALL_CATTLE_DESTROYED_COLUMN,
    BIRDS_DESTROYED_COLUMN: BIRDS_DESTROYED_COLUMN,
    DATE_COLUMN: DATE_COLUMN,
}

DAMAGE_GROUP_LABEL = "РЈС‰РµСЂР±"
DAMAGE_GROUP_OPTION_VALUE = "__group__:damage_overview"
DAMAGE_GROUP_OPTION_LABEL = "Р’СЃРµ РїРѕРєР°Р·Р°С‚РµР»Рё СѓС‰РµСЂР±Р°"

DAMAGE_PAIR_COLUMNS = [
    ("Р—РґР°РЅРёСЏ", BUILDINGS_DESTROYED_COLUMN, BUILDINGS_DAMAGED_COLUMN),
    ("РљРІР°СЂС‚РёСЂС‹", APARTMENTS_DESTROYED_COLUMN, APARTMENTS_DAMAGED_COLUMN),
    ("РђРїР°СЂС‚Р°РјРµРЅС‚С‹", APART_HOTEL_DESTROYED_COLUMN, APART_HOTEL_DAMAGED_COLUMN),
    ("РџР»РѕС‰Р°РґСЊ, Рј2", AREA_DESTROYED_COLUMN, AREA_DAMAGED_COLUMN),
    ("РўРµС…РЅРёРєР°", VEHICLES_DESTROYED_COLUMN, VEHICLES_DAMAGED_COLUMN),
    ("Р—РµСЂРЅРѕРІС‹Рµ", GRAIN_DESTROYED_COLUMN, GRAIN_DAMAGED_COLUMN),
    ("РљРѕСЂРјР°", FEED_DESTROYED_COLUMN, FEED_DAMAGED_COLUMN),
    ("РўРµС…РєСѓР»СЊС‚СѓСЂС‹", TECH_CROPS_DESTROYED_COLUMN, TECH_CROPS_DAMAGED_COLUMN),
]

DAMAGE_STANDALONE_COLUMNS = [
    REGISTERED_DAMAGE_COLUMN,
    LARGE_CATTLE_DESTROYED_COLUMN,
    SMALL_CATTLE_DESTROYED_COLUMN,
    BIRDS_DESTROYED_COLUMN,
]

DAMAGE_OVERVIEW_LABELS = {
    REGISTERED_DAMAGE_COLUMN: "Р—Р°СЂРµРіРёСЃС‚СЂРёСЂРѕРІР°РЅРЅС‹Р№ СѓС‰РµСЂР±",
    BUILDINGS_DESTROYED_COLUMN: "Р—РґР°РЅРёСЏ: СѓРЅРёС‡С‚РѕР¶РµРЅРѕ",
    BUILDINGS_DAMAGED_COLUMN: "Р—РґР°РЅРёСЏ: РїРѕРІСЂРµР¶РґРµРЅРѕ",
    APARTMENTS_DESTROYED_COLUMN: "РљРІР°СЂС‚РёСЂС‹: СѓРЅРёС‡С‚РѕР¶РµРЅРѕ",
    APARTMENTS_DAMAGED_COLUMN: "РљРІР°СЂС‚РёСЂС‹: РїРѕРІСЂРµР¶РґРµРЅРѕ",
    APART_HOTEL_DESTROYED_COLUMN: "РђРїР°СЂС‚Р°РјРµРЅС‚С‹: СѓРЅРёС‡С‚РѕР¶РµРЅРѕ",
    APART_HOTEL_DAMAGED_COLUMN: "РђРїР°СЂС‚Р°РјРµРЅС‚С‹: РїРѕРІСЂРµР¶РґРµРЅРѕ",
    AREA_DESTROYED_COLUMN: "РџР»РѕС‰Р°РґСЊ Рј2: СѓРЅРёС‡С‚РѕР¶РµРЅРѕ",
    AREA_DAMAGED_COLUMN: "РџР»РѕС‰Р°РґСЊ Рј2: РїРѕРІСЂРµР¶РґРµРЅРѕ",
    VEHICLES_DESTROYED_COLUMN: "РўРµС…РЅРёРєР°: СѓРЅРёС‡С‚РѕР¶РµРЅРѕ",
    VEHICLES_DAMAGED_COLUMN: "РўРµС…РЅРёРєР°: РїРѕРІСЂРµР¶РґРµРЅРѕ",
    GRAIN_DESTROYED_COLUMN: "Р—РµСЂРЅРѕРІС‹Рµ: СѓРЅРёС‡С‚РѕР¶РµРЅРѕ",
    GRAIN_DAMAGED_COLUMN: "Р—РµСЂРЅРѕРІС‹Рµ: РїРѕРІСЂРµР¶РґРµРЅРѕ",
    FEED_DESTROYED_COLUMN: "РљРѕСЂРјР°: СѓРЅРёС‡С‚РѕР¶РµРЅРѕ",
    FEED_DAMAGED_COLUMN: "РљРѕСЂРјР°: РїРѕРІСЂРµР¶РґРµРЅРѕ",
    TECH_CROPS_DESTROYED_COLUMN: "РўРµС…РєСѓР»СЊС‚СѓСЂС‹: СѓРЅРёС‡С‚РѕР¶РµРЅРѕ",
    TECH_CROPS_DAMAGED_COLUMN: "РўРµС…РєСѓР»СЊС‚СѓСЂС‹: РїРѕРІСЂРµР¶РґРµРЅРѕ",
    LARGE_CATTLE_DESTROYED_COLUMN: "РљСЂСѓРїРЅС‹Р№ СЃРєРѕС‚",
    SMALL_CATTLE_DESTROYED_COLUMN: "РњРµР»РєРёР№ СЃРєРѕС‚",
    BIRDS_DESTROYED_COLUMN: "РџС‚РёС†С‹",
}

EXCLUDED_TABLE_PREFIXES = ("final_", "tmp_", "pg_", "sql_", "benchmark_")

PLOTLY_PALETTE = {
    "fire": "#d95d39",
    "fire_soft": "#f3a66d",
    "forest": "#2f7a5f",
    "forest_soft": "#73b799",
    "sky": "#2d6c8f",
    "sky_soft": "#7db6d5",
    "sand": "#d1a15f",
    "sand_soft": "#e4c593",
    "ink": "#332920",
    "grid": "rgba(94, 73, 49, 0.12)",
    "paper": "rgba(255,255,255,0)",
}

METADATA_CACHE_TTL_SECONDS = 300
DASHBOARD_CACHE_TTL_SECONDS = 120

