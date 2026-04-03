DATE_COLUMN = "Дата возникновения пожара"
AREA_COLUMN = "Площадь пожара"
GENERAL_CAUSE_COLUMN = "Причина пожара (общая)"
OPEN_AREA_CAUSE_COLUMN = "Причина пожара для открытой территории"
BUILDING_CAUSE_COLUMN = "Причина пожара для зданий (сооружений)"
BUILDING_CATEGORY_COLUMN = "Категория здания"
RISK_CATEGORY_COLUMN = "Категория риска"
FIRE_STATION_DISTANCE_COLUMN = "Удаленность от ближайшей ПЧ"
OBJECT_NAME_COLUMN = "Наименование объекта"
OBJECT_CATEGORY_COLUMN = "Категория объекта"
REGISTERED_DAMAGE_COLUMN = "Зарегистрированный ущерб от пожара"
BUILDINGS_DESTROYED_COLUMN = "Здания (сооружения), уничтожено"
BUILDINGS_DAMAGED_COLUMN = "Здания (сооружения), повреждено"
APARTMENTS_DESTROYED_COLUMN = "Жилые квартиры, уничтожено"
APARTMENTS_DAMAGED_COLUMN = "Жилые квартиры, повреждено"
APART_HOTEL_DESTROYED_COLUMN = "Апартаменты, уничтожено"
APART_HOTEL_DAMAGED_COLUMN = "Апартаменты, повреждено"
AREA_DESTROYED_COLUMN = "Площадь м2, уничтожено"
AREA_DAMAGED_COLUMN = "Площадь м2, повреждено"
VEHICLES_DESTROYED_COLUMN = "Автотракторная и др.техника, уничтожено"
VEHICLES_DAMAGED_COLUMN = "Автотракторная и др.техника, повреждено"
GRAIN_DESTROYED_COLUMN = "Зерновые и зернобобовые, уничтожено"
GRAIN_DAMAGED_COLUMN = "Зерновые и зернобобовые, повреждено"
FEED_DESTROYED_COLUMN = "Корма, уничтожено"
FEED_DAMAGED_COLUMN = "Корма, повреждено"
TECH_CROPS_DESTROYED_COLUMN = "Технические культуры, уничтожено"
TECH_CROPS_DAMAGED_COLUMN = "Технические культуры, повреждено"
LARGE_CATTLE_DESTROYED_COLUMN = "Крупный скот, уничтожено"
SMALL_CATTLE_DESTROYED_COLUMN = "Мелкий скот, уничтожено"
BIRDS_DESTROYED_COLUMN = "Птиц, уничтожено"

DISTRIBUTION_GROUPS = [
    (
        "Причины",
        [
            GENERAL_CAUSE_COLUMN,
            OPEN_AREA_CAUSE_COLUMN,
            BUILDING_CAUSE_COLUMN,
        ],
    ),
    (
        "Объект и локация",
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
        "Ущерб",
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

DISTRIBUTION_COLUMNS = [column_name for _, columns in DISTRIBUTION_GROUPS for column_name in columns]

CAUSE_COLUMNS = [
    GENERAL_CAUSE_COLUMN,
    OPEN_AREA_CAUSE_COLUMN,
    BUILDING_CAUSE_COLUMN,
]

IMPACT_METRIC_CONFIG = {
    "deaths": {
        "label": "Погибшие",
        "preferred": ["Количество погибших в КУП"],
        "include_any": [["погибш"], ["смерт"], ["гибел"]],
        "exclude": ["причин", "место", "момент", "фио", "дата", "возраст", "пол", "уг#", "уг_", "сотрудник"],
        "tone": "fire",
    },
    "injuries": {
        "label": "Травмированные",
        "preferred": ["Количество травмированных в КУП"],
        "include_any": [["травм"]],
        "exclude": ["вид", "место", "фио", "дата", "возраст", "пол", "ут#", "ут_", "сотрудник"],
        "tone": "sand",
    },
    "evacuated": {
        "label": "Эвакуировано",
        "preferred": ["Эвакуировано на пожаре"],
        "include_any": [["эваку"]],
        "exclude": ["дет"],
        "tone": "sky",
    },
    "evacuated_children": {
        "label": "Эвакуировано детей",
        "preferred": ["Эвакуировано детей"],
        "include_all": [["эваку", "дет"]],
        "exclude": [],
        "tone": "sky",
    },
    "rescued_total": {
        "label": "Спасено",
        "preferred": ["Спасено на пожаре"],
        "include_any": [["спас"]],
        "exclude": ["дет", "здан", "сооруж", "скот", "техник", "материал", "ценност"],
        "tone": "forest",
    },
    "rescued_children": {
        "label": "Спасено детей",
        "preferred": ["Спасено детей"],
        "include_all": [["спас", "дет"]],
        "exclude": [],
        "tone": "forest",
    },
}

COLUMN_LABELS = {
    GENERAL_CAUSE_COLUMN: "Причина пожара (общая)",
    OPEN_AREA_CAUSE_COLUMN: "Причина пожара для открытой территории",
    BUILDING_CAUSE_COLUMN: "Причина пожара для зданий (сооружений)",
    BUILDING_CATEGORY_COLUMN: "Категория здания",
    RISK_CATEGORY_COLUMN: "Категория риска",
    AREA_COLUMN: "Площадь пожара",
    FIRE_STATION_DISTANCE_COLUMN: "Удаленность от ближайшей ПЧ",
    OBJECT_NAME_COLUMN: "Наименование объекта",
    OBJECT_CATEGORY_COLUMN: "Категория объекта",
    REGISTERED_DAMAGE_COLUMN: "Зарегистрированный ущерб от пожара",
    BUILDINGS_DESTROYED_COLUMN: "Здания (сооружения), уничтожено",
    BUILDINGS_DAMAGED_COLUMN: "Здания (сооружения), повреждено",
    APARTMENTS_DESTROYED_COLUMN: "Жилые квартиры, уничтожено",
    APARTMENTS_DAMAGED_COLUMN: "Жилые квартиры, повреждено",
    APART_HOTEL_DESTROYED_COLUMN: "Апартаменты, уничтожено",
    APART_HOTEL_DAMAGED_COLUMN: "Апартаменты, повреждено",
    AREA_DESTROYED_COLUMN: "Площадь м2, уничтожено",
    AREA_DAMAGED_COLUMN: "Площадь м2, повреждено",
    VEHICLES_DESTROYED_COLUMN: "Автотракторная и др.техника, уничтожено",
    VEHICLES_DAMAGED_COLUMN: "Автотракторная и др.техника, повреждено",
    GRAIN_DESTROYED_COLUMN: "Зерновые и зернобобовые, уничтожено",
    GRAIN_DAMAGED_COLUMN: "Зерновые и зернобобовые, повреждено",
    FEED_DESTROYED_COLUMN: "Корма, уничтожено",
    FEED_DAMAGED_COLUMN: "Корма, повреждено",
    TECH_CROPS_DESTROYED_COLUMN: "Технические культуры, уничтожено",
    TECH_CROPS_DAMAGED_COLUMN: "Технические культуры, повреждено",
    LARGE_CATTLE_DESTROYED_COLUMN: "Крупный скот, уничтожено",
    SMALL_CATTLE_DESTROYED_COLUMN: "Мелкий скот, уничтожено",
    BIRDS_DESTROYED_COLUMN: "Птиц, уничтожено",
    DATE_COLUMN: "Дата возникновения пожара",
}

DAMAGE_GROUP_LABEL = "Ущерб"
DAMAGE_GROUP_OPTION_VALUE = "__group__:damage_overview"
DAMAGE_GROUP_OPTION_LABEL = "Все показатели ущерба"
DAMAGE_PAIR_COLUMNS = [
    ("Здания", BUILDINGS_DESTROYED_COLUMN, BUILDINGS_DAMAGED_COLUMN),
    ("Квартиры", APARTMENTS_DESTROYED_COLUMN, APARTMENTS_DAMAGED_COLUMN),
    ("Апартаменты", APART_HOTEL_DESTROYED_COLUMN, APART_HOTEL_DAMAGED_COLUMN),
    ("Площадь, м2", AREA_DESTROYED_COLUMN, AREA_DAMAGED_COLUMN),
    ("Техника", VEHICLES_DESTROYED_COLUMN, VEHICLES_DAMAGED_COLUMN),
    ("Зерновые", GRAIN_DESTROYED_COLUMN, GRAIN_DAMAGED_COLUMN),
    ("Корма", FEED_DESTROYED_COLUMN, FEED_DAMAGED_COLUMN),
    ("Техкультуры", TECH_CROPS_DESTROYED_COLUMN, TECH_CROPS_DAMAGED_COLUMN),
]
DAMAGE_STANDALONE_COLUMNS = [
    REGISTERED_DAMAGE_COLUMN,
    LARGE_CATTLE_DESTROYED_COLUMN,
    SMALL_CATTLE_DESTROYED_COLUMN,
    BIRDS_DESTROYED_COLUMN,
]
DAMAGE_OVERVIEW_LABELS = {
    REGISTERED_DAMAGE_COLUMN: "Зарегистрированный ущерб",
    BUILDINGS_DESTROYED_COLUMN: "Здания: уничтожено",
    BUILDINGS_DAMAGED_COLUMN: "Здания: повреждено",
    APARTMENTS_DESTROYED_COLUMN: "Квартиры: уничтожено",
    APARTMENTS_DAMAGED_COLUMN: "Квартиры: повреждено",
    APART_HOTEL_DESTROYED_COLUMN: "Апартаменты: уничтожено",
    APART_HOTEL_DAMAGED_COLUMN: "Апартаменты: повреждено",
    AREA_DESTROYED_COLUMN: "Площадь м2: уничтожено",
    AREA_DAMAGED_COLUMN: "Площадь м2: повреждено",
    VEHICLES_DESTROYED_COLUMN: "Техника: уничтожено",
    VEHICLES_DAMAGED_COLUMN: "Техника: повреждено",
    GRAIN_DESTROYED_COLUMN: "Зерновые: уничтожено",
    GRAIN_DAMAGED_COLUMN: "Зерновые: повреждено",
    FEED_DESTROYED_COLUMN: "Корма: уничтожено",
    FEED_DAMAGED_COLUMN: "Корма: повреждено",
    TECH_CROPS_DESTROYED_COLUMN: "Техкультуры: уничтожено",
    TECH_CROPS_DAMAGED_COLUMN: "Техкультуры: повреждено",
    LARGE_CATTLE_DESTROYED_COLUMN: "Крупный скот",
    SMALL_CATTLE_DESTROYED_COLUMN: "Мелкий скот",
    BIRDS_DESTROYED_COLUMN: "Птицы",
}
EXCLUDED_TABLE_PREFIXES = ("final_", "tmp_", "pg_", "sql_", "benchmark_")
MONTH_LABELS = {
    1: "Янв",
    2: "Фев",
    3: "Мар",
    4: "Апр",
    5: "Май",
    6: "Июн",
    7: "Июл",
    8: "Авг",
    9: "Сен",
    10: "Окт",
    11: "Ноя",
    12: "Дек",
}

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
