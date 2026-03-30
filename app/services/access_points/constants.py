from __future__ import annotations

from app.services.forecast_risk.constants import (
    ARRIVAL_TIME_COLUMN_CANDIDATES,
    CASUALTY_FLAG_COLUMN_CANDIDATES,
    CONSEQUENCE_COLUMN_CANDIDATES,
    DETECTION_TIME_COLUMN_CANDIDATES,
    DEATHS_COLUMN_CANDIDATES,
    DESTROYED_AREA_COLUMN_CANDIDATES,
    DESTROYED_BUILDINGS_COLUMN_CANDIDATES,
    DISTRICT_COLUMN_CANDIDATES,
    FIRE_STATION_DISTANCE_COLUMN_CANDIDATES,
    INJURIES_COLUMN_CANDIDATES,
    LONG_RESPONSE_THRESHOLD_MINUTES,
    REGISTERED_DAMAGE_COLUMN_CANDIDATES,
    REPORT_TIME_COLUMN_CANDIDATES,
    SETTLEMENT_TYPE_COLUMN_CANDIDATES,
    TERRITORY_LABEL_COLUMN_CANDIDATES,
    WATER_SUPPLY_COUNT_COLUMN_CANDIDATES,
    WATER_SUPPLY_DETAILS_COLUMN_CANDIDATES,
)
from app.services.forecasting.constants import LATITUDE_COLUMN_CANDIDATES, LONGITUDE_COLUMN_CANDIDATES
from app.statistics_constants import DATE_COLUMN, OBJECT_CATEGORY_COLUMN

ACCESS_POINTS_TITLE = "Проблемные точки"
ACCESS_POINTS_DESCRIPTION = (
    "Новый блок выделяет не типы территорий, а отдельные точки с плохой доступностью пожарных подразделений. "
    "Рейтинг строится по наиболее granular сущности, которая реально есть в данных: адрес или объект, затем координатная точка, "
    "далее населенный пункт или territory label и только потом район. "
    "Итоговый балл раскладывается на доступность ПЧ, водоснабжение, тяжесть последствий, повторяемость пожаров и неполноту данных."
)

OBJECT_CATEGORY_COLUMN_CANDIDATES = [OBJECT_CATEGORY_COLUMN, "Категория объекта пожара"]
ADDRESS_COLUMN_CANDIDATES = ["Адрес", "Адрес пожара", "Место пожара", "Адрес объекта"]
ADDRESS_COMMENT_COLUMN_CANDIDATES = ["Комментарий к адресу"]
OBJECT_NAME_COLUMN_CANDIDATES = ["Наименование объекта", "Вид объекта", "Объект"]

ACCESS_POINT_LIMIT_OPTIONS = [10, 25, 50, 100]
DEFAULT_ACCESS_POINT_LIMIT = 25
TOP_POINT_CARD_COUNT = 5
MAX_INCOMPLETE_POINTS = 6
MAX_NOTES = 7

GENERIC_OBJECT_TOKENS = (
    "жилой дом",
    "домовладение",
    "дом",
    "квартира",
    "гараж",
    "баня",
    "сарай",
    "постройка",
    "участок",
    "склад",
)

HIGH_RISK_THRESHOLD = 70.0
REVIEW_RISK_THRESHOLD = 55.0
WATCH_RISK_THRESHOLD = 40.0
