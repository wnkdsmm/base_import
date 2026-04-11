from __future__ import annotations

from app.domain.analytics_metadata import PLOTLY_PALETTE
from app.domain.fire_columns import (
    BUILDING_CAUSE_COLUMN,
    CAUSE_COLUMN_CANDIDATES,
    DATE_COLUMN,
    DISTRICT_COLUMN_CANDIDATES,
    GENERAL_CAUSE_COLUMN,
    LATITUDE_COLUMN_CANDIDATES,
    LONGITUDE_COLUMN_CANDIDATES,
    OBJECT_CATEGORY_COLUMN,
    OPEN_AREA_CAUSE_COLUMN,
    TEMPERATURE_COLUMN_CANDIDATES,
)
from app.domain.time_labels import FORECAST_MONTH_LABELS as MONTH_LABELS
from app.domain.time_labels import FORECAST_WEEKDAY_LABELS as WEEKDAY_LABELS
from app.domain.predictive_settings import GEO_LOOKBACK_DAYS, MAX_GEO_CHART_POINTS, MAX_GEO_HOTSPOTS


FORECAST_DAY_OPTIONS = [7, 14, 30, 60]
HISTORY_WINDOW_OPTIONS = [
    {"value": "all", "label": "Все годы"},
    {"value": "recent_3", "label": "Последние 3 года"},
    {"value": "recent_5", "label": "Последние 5 лет"},
]


SCENARIO_FORECAST_DESCRIPTION = (
    "Сценарный прогноз открывают, когда нужно понять, в какие ближайшие дни вероятность пожара выше и когда стоит готовиться заранее. "
    "Он показывает календарь по дням и затем помогает перейти к территориальному приоритету. "
    "Это не ML-прогноз ожидаемого числа пожаров: экран отвечает на вопрос «когда готовиться», а не «сколько пожаров может быть по датам»."
)
