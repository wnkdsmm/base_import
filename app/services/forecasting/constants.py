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



SCENARIO_FORECAST_DESCRIPTION = (
    "\u0421\u0446\u0435\u043d\u0430\u0440\u043d\u044b\u0439 \u043f\u0440\u043e\u0433\u043d\u043e\u0437 \u043e\u0442\u043a\u0440\u044b\u0432\u0430\u044e\u0442, \u043a\u043e\u0433\u0434\u0430 \u043d\u0443\u0436\u043d\u043e \u043f\u043e\u043d\u044f\u0442\u044c, \u0432 \u043a\u0430\u043a\u0438\u0435 \u0431\u043b\u0438\u0436\u0430\u0439\u0448\u0438\u0435 \u0434\u043d\u0438 \u0432\u0435\u0440\u043e\u044f\u0442\u043d\u043e\u0441\u0442\u044c \u043f\u043e\u0436\u0430\u0440\u0430 \u0432\u044b\u0448\u0435 \u0438 \u043a\u043e\u0433\u0434\u0430 \u0441\u0442\u043e\u0438\u0442 \u0433\u043e\u0442\u043e\u0432\u0438\u0442\u044c\u0441\u044f \u0437\u0430\u0440\u0430\u043d\u0435\u0435. "
    "\u041e\u043d \u043f\u043e\u043a\u0430\u0437\u044b\u0432\u0430\u0435\u0442 \u043a\u0430\u043b\u0435\u043d\u0434\u0430\u0440\u044c \u043f\u043e \u0434\u043d\u044f\u043c \u0438 \u0437\u0430\u0442\u0435\u043c \u043f\u043e\u043c\u043e\u0433\u0430\u0435\u0442 \u043f\u0435\u0440\u0435\u0439\u0442\u0438 \u043a \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0430\u043b\u044c\u043d\u043e\u043c\u0443 \u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442\u0443. "
    "\u042d\u0442\u043e \u043d\u0435 ML-\u043f\u0440\u043e\u0433\u043d\u043e\u0437 \u043e\u0436\u0438\u0434\u0430\u0435\u043c\u043e\u0433\u043e \u0447\u0438\u0441\u043b\u0430 \u043f\u043e\u0436\u0430\u0440\u043e\u0432: \u044d\u043a\u0440\u0430\u043d \u043e\u0442\u0432\u0435\u0447\u0430\u0435\u0442 \u043d\u0430 \u0432\u043e\u043f\u0440\u043e\u0441 \xab\u043a\u043e\u0433\u0434\u0430 \u0433\u043e\u0442\u043e\u0432\u0438\u0442\u044c\u0441\u044f\xbb, \u0430 \u043d\u0435 \xab\u0441\u043a\u043e\u043b\u044c\u043a\u043e \u043f\u043e\u0436\u0430\u0440\u043e\u0432 \u043c\u043e\u0436\u0435\u0442 \u0431\u044b\u0442\u044c \u043f\u043e \u0434\u0430\u0442\u0430\u043c\xbb."
)
