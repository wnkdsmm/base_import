from __future__ import annotations

from app.statistics_constants import (
    BUILDING_CAUSE_COLUMN,
    DATE_COLUMN,
    GENERAL_CAUSE_COLUMN,
    OBJECT_CATEGORY_COLUMN,
    OPEN_AREA_CAUSE_COLUMN,
    PLOTLY_PALETTE,
)


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
TEMPERATURE_COLUMN_CANDIDATES = [
    "Температура",
    "Температура воздуха",
    "Температура воздуха, С",
    "Температура воздуха, C",
    "Температура воздуха, °C",
    "Температура окружающей среды",
]
CAUSE_COLUMN_CANDIDATES = [
    GENERAL_CAUSE_COLUMN,
    OPEN_AREA_CAUSE_COLUMN,
    BUILDING_CAUSE_COLUMN,
    "Причина пожара",
    "Причина",
]
LATITUDE_COLUMN_CANDIDATES = ["Широта", "Latitude", "Lat"]
LONGITUDE_COLUMN_CANDIDATES = ["Долгота", "Longitude", "Lon"]
FORECAST_DAY_OPTIONS = [7, 14, 30, 60]
HISTORY_WINDOW_OPTIONS = [
    {"value": "all", "label": "\u0412\u0441\u0435 \u0433\u043e\u0434\u044b"},
    {"value": "recent_3", "label": "\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0435 3 \u0433\u043e\u0434\u0430"},
    {"value": "recent_5", "label": "\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0435 5 \u043b\u0435\u0442"},
]
GEO_LOOKBACK_DAYS = 180
MAX_GEO_CHART_POINTS = 40
MAX_GEO_HOTSPOTS = 8
WEEKDAY_LABELS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
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



