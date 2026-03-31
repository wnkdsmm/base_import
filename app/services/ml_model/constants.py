from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict, Tuple

MODEL_NAME = 'ML-прогноз по числу пожаров'

FORECAST_DAY_OPTIONS = [7, 14, 30]
HISTORY_WINDOW_OPTIONS = [
    {'value': 'all', 'label': 'Все годы'},
    {'value': 'recent_3', 'label': 'Последние 3 года'},
    {'value': 'recent_5', 'label': 'Последние 5 лет'},
]

FEATURE_LABELS = {
    'temp_value': 'Температура',
    'weekday': 'День недели',
    'month': 'Месяц',
    'lag_1': 'Пожары вчера',
    'lag_7': 'Пожары 7 дней назад',
    'lag_14': 'Пожары 14 дней назад',
    'rolling_7': 'Среднее за 7 дней',
    'rolling_28': 'Среднее за 28 дней',
    'trend_gap': 'Разница 7/28 дней',
}

COUNT_MODEL_LABELS = {
    'poisson': 'Регрессия Пуассона',
    'negative_binomial': 'Negative Binomial GLM',
    'tweedie': 'Tweedie GLM (compound Poisson-Gamma)',
    'heuristic_forecast': 'Сценарный эвристический прогноз',
    'seasonal_baseline': 'Сезонная базовая модель',
}
EVENT_MODEL_LABEL = 'Логистическая регрессия'

MIN_DAILY_HISTORY = 60
MIN_FEATURE_ROWS = 24
MIN_BACKTEST_POINTS = 8
MIN_EVENT_CLASS_COUNT = 8
EVENT_RATE_SATURATION_MARGIN = 0.05
MIN_TEMPERATURE_NON_NULL_DAYS = 30
MIN_TEMPERATURE_COVERAGE = 0.20
MAX_HISTORY_POINTS = 900
MAX_BACKTEST_POINTS = 45
PERMUTATION_REPEATS = 8
ROLLING_MIN_TRAIN_ROWS = 28
COUNT_MODEL_SELECTION_TOLERANCE = 0.05

_CACHE_LIMIT = 12
_ML_CACHE: 'OrderedDict[Tuple[str, str, str, str, int, str], Dict[str, Any]]' = OrderedDict()

_POISSON_PARAMS = {
    'alpha': 0.40,
    'max_iter': 2000,
}

_TWEEDIE_PARAMS = {
    'power': 1.5,
    'alpha': 0.20,
    'max_iter': 2000,
}

_LOGISTIC_PARAMS = {
    'solver': 'liblinear',
    'max_iter': 500,
    'class_weight': 'balanced',
    'random_state': 42,
}
