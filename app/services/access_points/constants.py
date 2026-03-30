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

ACCESS_POINT_LIMIT_OPTIONS = [10, 25, 50, 100]
DEFAULT_ACCESS_POINT_LIMIT = 25
MIN_ACCESS_POINT_SUPPORT = 3
TOP_POINT_CARD_COUNT = 5
MAX_INCOMPLETE_POINTS = 6
MAX_NOTES = 7
MAX_ACCESS_POINT_FEATURE_OPTIONS = 10

ACCESS_POINT_FEATURE_METADATA = {
    "DISTANCE_TO_STATION": {
        "label": "Удалённость до ПЧ",
        "description": "Средняя дистанция до пожарной части по точке.",
    },
    "RESPONSE_TIME": {
        "label": "Среднее время прибытия",
        "description": "Насколько долго в среднем подразделения добираются до точки.",
    },
    "LONG_ARRIVAL_SHARE": {
        "label": "Доля долгих прибытий",
        "description": "Как часто прибытие превышает порог долгого выезда.",
    },
    "NO_WATER": {
        "label": "Отсутствие воды",
        "description": "Доля случаев без подтверждённого водоснабжения.",
    },
    "SEVERE_CONSEQUENCES": {
        "label": "Тяжёлые последствия",
        "description": "Комбинация тяжёлых последствий, пострадавших и крупного ущерба.",
    },
    "REPEAT_FIRES": {
        "label": "Повторяемость пожаров",
        "description": "Частота пожаров по точке с поправкой на период наблюдения.",
    },
    "NIGHT_PROFILE": {
        "label": "Ночной профиль",
        "description": "Доля инцидентов, произошедших ночью.",
    },
    "HEATING_SEASON": {
        "label": "Отопительный сезон",
        "description": "Доля пожаров в отопительный сезон.",
    },
}
DEFAULT_ACCESS_POINT_FEATURES = tuple(ACCESS_POINT_FEATURE_METADATA.keys())

POINT_FEATURE_COLUMNS = (
    "incident_count",
    "incidents_per_year",
    "average_response_minutes",
    "response_coverage_share",
    "long_arrival_share",
    "average_distance_km",
    "distance_coverage_share",
    "no_water_share",
    "water_coverage_share",
    "water_unknown_share",
    "severe_share",
    "victim_share",
    "major_damage_share",
    "night_share",
    "heating_share",
    "rural_share",
    "rural_flag",
    "low_support",
    "support_weight",
)

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
