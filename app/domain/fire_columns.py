from __future__ import annotations

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

CAUSE_COLUMNS = [
    GENERAL_CAUSE_COLUMN,
    OPEN_AREA_CAUSE_COLUMN,
    BUILDING_CAUSE_COLUMN,
]

FIRE_DATE_COLUMN_CANDIDATES = [
    DATE_COLUMN,
    "Дата пожара",
    "Дата возгорания",
    "Дата загорания",
]

DISTRICT_COLUMN_CANDIDATES = [
    "ОКТМО. Текст",
    "Район",
    "Муниципальный район",
    "Административный район",
    "Район выезда подразделения",
    "Район пожара",
    "Территория",
    "Территориальная принадлежность",
]

DASHBOARD_DISTRICT_COLUMN_CANDIDATES = [
    "Район",
    "Муниципальный район",
    "Муниципальное образование",
    "Административный район",
    "Район выезда подразделения",
    "Район пожара",
    "Территория",
]

CAUSE_COLUMN_CANDIDATES = [
    *CAUSE_COLUMNS,
    "Причина пожара",
    "Причина",
]

TEMPERATURE_COLUMN_CANDIDATES = [
    "Температура",
    "Температура воздуха",
    "Температура воздуха, С",
    "Температура воздуха, C",
    "Температура воздуха, °C",
    "Температура окружающей среды",
    "Температура окружающей среды на момент возникновения пожара",
]

LATITUDE_COLUMN_CANDIDATES = ["Широта", "Latitude", "Lat"]
LONGITUDE_COLUMN_CANDIDATES = ["Долгота", "Longitude", "Lon"]

COORDINATE_COLUMN_CANDIDATES = [
    "Координаты",
    *LATITUDE_COLUMN_CANDIDATES,
    *LONGITUDE_COLUMN_CANDIDATES,
    "Lng",
    "GPS координаты",
]

FIRE_AREA_COLUMN_CANDIDATES = [
    AREA_COLUMN,
    "Общая площадь объекта",
]

LOCALITY_COLUMN_CANDIDATES = [
    "Населенный пункт",
    "Населённый пункт",
    "ОКТМО. Текст",
    "Территориальная принадлежность",
]

TERRITORY_LABEL_COLUMN_CANDIDATES = [
    "ОКТМО. Текст",
    "Территориальная принадлежность",
    "Населенный пункт",
    "Населённый пункт",
]

SETTLEMENT_TYPE_COLUMN_CANDIDATES = [
    "Вид населенного пункта",
    "Вид населённого пункта",
]

BUILDING_CATEGORY_COLUMN_CANDIDATES = [BUILDING_CATEGORY_COLUMN]
RISK_CATEGORY_COLUMN_CANDIDATES = [RISK_CATEGORY_COLUMN]

FIRE_STATION_DISTANCE_COLUMN_CANDIDATES = [
    FIRE_STATION_DISTANCE_COLUMN,
    "Удаленность до ближайшей ПЧ",
]

WATER_SUPPLY_COUNT_COLUMN_CANDIDATES = [
    "Количество записей о водоснабжении на пожаре",
]

WATER_SUPPLY_DETAILS_COLUMN_CANDIDATES = [
    "Сведения о водоснабжении на пожаре",
]

WATER_SUPPLY_COLUMN_CANDIDATES = [
    *WATER_SUPPLY_DETAILS_COLUMN_CANDIDATES,
    "Сведения о водоснабжении на пожар",
    *WATER_SUPPLY_COUNT_COLUMN_CANDIDATES,
    "Наличие водоснабжения",
    "Водоснабжение",
]

REPORT_TIME_COLUMN_CANDIDATES = ["Время сообщения"]
ARRIVAL_TIME_COLUMN_CANDIDATES = ["Время прибытия 1-го ПП"]
DETECTION_TIME_COLUMN_CANDIDATES = ["Время обнаружения"]
HEATING_TYPE_COLUMN_CANDIDATES = ["Вид отопления"]
CONSEQUENCE_COLUMN_CANDIDATES = ["Наличие последствий пожара"]
REGISTERED_DAMAGE_COLUMN_CANDIDATES = [REGISTERED_DAMAGE_COLUMN]
DESTROYED_BUILDINGS_COLUMN_CANDIDATES = [BUILDINGS_DESTROYED_COLUMN]
DESTROYED_AREA_COLUMN_CANDIDATES = [AREA_DESTROYED_COLUMN]
CASUALTY_FLAG_COLUMN_CANDIDATES = ["Есть травмированные или погибшие"]
INJURIES_COLUMN_CANDIDATES = ["Количество травмированных в КУП"]
DEATHS_COLUMN_CANDIDATES = ["Количество погибших в КУП"]

OBJECT_CATEGORY_COLUMN_CANDIDATES = [
    OBJECT_CATEGORY_COLUMN,
    "Категория объекта пожара",
]

ADDRESS_COLUMN_CANDIDATES = [
    "Адрес",
    "Адрес пожара",
    "Место пожара",
    "Адрес объекта",
]

ADDRESS_COMMENT_COLUMN_CANDIDATES = [
    "Комментарий к адресу",
]

OBJECT_NAME_COLUMN_CANDIDATES = [
    OBJECT_NAME_COLUMN,
    "Вид объекта",
    "Объект",
]

SETTLEMENT_COLUMN_CANDIDATES = [
    "Населенный пункт",
    "Населённый пункт",
    "Наименование населенного пункта",
    "Наименование населённого пункта",
    "Нас. пункт",
    "Поселение",
    "Местность",
    "locality",
]

OBJECT_CATEGORY_LOOKUP = list(OBJECT_CATEGORY_COLUMN_CANDIDATES)
LONG_RESPONSE_THRESHOLD_MINUTES = 20.0

