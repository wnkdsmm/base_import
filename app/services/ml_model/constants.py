from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict, Tuple

MODEL_NAME = 'Count-first ML pipeline'

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
    'poisson': 'Poisson Regressor',
    'random_forest': 'Random Forest Regressor',
    'seasonal_baseline': 'Seasonal baseline',
}
EVENT_MODEL_LABEL = 'Logistic Regression'

MIN_DAILY_HISTORY = 60
MIN_FEATURE_ROWS = 24
MIN_BACKTEST_POINTS = 8
MIN_EVENT_CLASS_COUNT = 8
MAX_HISTORY_POINTS = 900
MAX_BACKTEST_POINTS = 45
PERMUTATION_REPEATS = 8

_CACHE_LIMIT = 12
_ML_CACHE: 'OrderedDict[Tuple[str, str, str, str, int, str], Dict[str, Any]]' = OrderedDict()

_RF_PARAMS = {
    'n_estimators': 180,
    'max_depth': 8,
    'min_samples_leaf': 2,
    'random_state': 42,
    'n_jobs': -1,
}

_POISSON_PARAMS = {
    'alpha': 0.15,
    'max_iter': 500,
}

_LOGISTIC_PARAMS = {
    'solver': 'liblinear',
    'max_iter': 500,
    'class_weight': 'balanced',
    'random_state': 42,
}
