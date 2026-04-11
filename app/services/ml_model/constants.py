from __future__ import annotations

from app.domain.predictive_settings import MIN_TEMPERATURE_COVERAGE, MIN_TEMPERATURE_NON_NULL_DAYS

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
    'heuristic_forecast': 'Сценарный эвристический прогноз',
    'seasonal_baseline': 'Сезонная базовая модель',
}
EVENT_MODEL_LABEL = 'Логистическая регрессия'

MIN_DAILY_HISTORY = 60
MIN_FEATURE_ROWS = 24
MIN_BACKTEST_POINTS = 8
MIN_EVENT_CLASS_COUNT = 8
EVENT_RATE_SATURATION_MARGIN = 0.05
MAX_HISTORY_POINTS = 900
MAX_BACKTEST_POINTS = 45
PERMUTATION_REPEATS = 8
ROLLING_MIN_TRAIN_ROWS = 28
COUNT_MODEL_SELECTION_TOLERANCE = 0.05

FEATURE_COLUMNS = ['temp_value', 'weekday', 'month', 'lag_1', 'lag_7', 'lag_14', 'rolling_7', 'rolling_28', 'trend_gap']
NON_TEMPERATURE_FEATURE_COLUMNS = [column for column in FEATURE_COLUMNS if column != 'temp_value']
COUNT_MODEL_CONTINUOUS_COLUMNS = ['temp_value', 'lag_1', 'lag_7', 'lag_14', 'rolling_7', 'rolling_28', 'trend_gap']
COUNT_MODEL_KEYS = ['poisson', 'negative_binomial']
EXPLAINABLE_COUNT_MODEL_KEY = 'poisson'
NEGATIVE_BINOMIAL_OVERDISPERSION_THRESHOLD = 1.35
NEGATIVE_BINOMIAL_MIN_TRAIN_ROWS = 56
MIN_POSITIVE_PREDICTION = 1e-6
CLASSIFICATION_THRESHOLD = 0.5
PREDICTION_INTERVAL_LEVEL = 0.8
PREDICTION_INTERVAL_CALIBRATION_FRACTION = 0.6
MIN_INTERVAL_CALIBRATION_WINDOWS = 6
MIN_INTERVAL_EVALUATION_WINDOWS = 4
PREDICTION_INTERVAL_TARGET_BINS = 3
MIN_INTERVAL_BIN_RESIDUALS = 2
PREDICTION_INTERVAL_METHOD_LABEL = 'Adaptive conformal interval with predicted-count bins'
PREDICTION_INTERVAL_FIXED_CHRONO_LABEL = 'Fixed 60/40 chrono split conformal'
PREDICTION_INTERVAL_BLOCKED_CV_LABEL = 'Blocked forward CV conformal'
PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL = 'Forward rolling split conformal'
PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL = 'Jackknife+ for time series'

EVENT_SELECTION_RULE = (
    'Минимум Brier score, затем log-loss и ROC-AUC; при близком качестве '
    'сохраняется более простой и интерпретируемый метод.'
)

COUNT_SELECTION_RULE = (
    'Минимум Poisson deviance, затем MAE и RMSE среди seasonal baseline, heuristic forecast и count-model; '
    'если heuristic forecast почти не хуже лучшей count-model, сохраняется более объяснимый рабочий метод; внутри ML-паритета предпочитается Poisson.'
)

EVENT_BASELINE_METHOD_LABEL = 'Сезонная событийная базовая модель'
EVENT_BASELINE_ROLE_LABEL = 'Базовая модель'
EVENT_HEURISTIC_METHOD_LABEL = 'Сценарная эвристическая вероятность'
EVENT_HEURISTIC_ROLE_LABEL = 'Сценарный прогноз'
EVENT_CLASSIFIER_ROLE_LABEL = 'Классификатор'
EVENT_PROBABILITY_REASON_TOO_FEW_COMPARABLE_WINDOWS = 'too_few_comparable_windows'
EVENT_PROBABILITY_REASON_SINGLE_CLASS_EVALUATION = 'single_class_evaluation'
EVENT_PROBABILITY_REASON_SATURATED_EVENT_RATE = 'saturated_event_rate'
WARNING_INSTABILITY_MESSAGE_TOKENS = (
    'perfect separation',
    'perfect prediction',
    'parameter may not be identified',
    'parameters are not identified',
    'failed to converge',
    'did not converge',
    'singular',
    'hessian',
)

ML_PREDICTIVE_BLOCK_DESCRIPTION = (
    'ML-прогноз открывают, когда нужно оценить ожидаемое число пожаров по датам для выбранного среза. '
    'Ниже показаны прогноз количества, качество модели и факторы, которые сильнее всего влияют на результат. '
    'Этот экран не ранжирует территории и не заменяет сценарный прогноз по вероятности пожара.'
)

ML_CACHE_SCHEMA_VERSION = 2

_CACHE_LIMIT = 12

_POISSON_PARAMS = {
    'alpha': 0.40,
    'max_iter': 2000,
}

_LOGISTIC_PARAMS = {
    'solver': 'liblinear',
    'max_iter': 500,
    'class_weight': 'balanced',
    'random_state': 42,
}
