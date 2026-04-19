from __future__ import annotations

"""Centralized UI labels and user-facing copy."""

# === Forecasting UI ===

FORECASTING_HISTORY_WINDOW_LABELS = {
    "all": "Все годы",
    "recent_3": "Последние 3 года",
    "recent_5": "Последние 5 лет",
}

SCENARIO_FORECAST_DESCRIPTION = (
    "Сценарный прогноз открывают, когда нужно понять, в какие ближайшие дни вероятность пожара выше и когда стоит готовиться заранее. "
    "Он показывает календарь по дням и затем помогает перейти к территориальному приоритету. "
    "Это не ML-прогноз ожидаемого числа пожаров: экран отвечает на вопрос «когда готовиться», а не «сколько пожаров может быть по датам»."
)

# === Clustering UI ===

CLUSTERING_SAMPLING_STRATEGY_LABELS = {
    "stratified": "Стратифицированная",
    "random": "Случайная",
}

CLUSTERING_DEFAULT_CLUSTER_FEATURES = [
    "Число пожаров",
    "Средняя площадь пожара",
    "Доля ночных пожаров",
    "Среднее время прибытия, мин",
    "Доля без подтвержденного водоснабжения",
]

CLUSTERING_AUTO_DEFAULT_EXCLUDED_FEATURES = {
    "Покрытие данных по водоснабжению",
    "Покрытие данных по времени прибытия",
}

CLUSTERING_MODE_PROFILE_LABEL = "Профиль территории"
CLUSTERING_MODE_LOAD_LABEL = "Профиль территории + нагрузка"

CLUSTERING_WEIGHTING_STRATEGY_LABELS = {
    "uniform": "Равный вес территорий",
    "incident_log": "Умеренный вес по числу пожаров",
    "not_applicable": "Весы не применяются",
}

CLUSTERING_FEATURE_METADATA = {
    "Число пожаров": {
        "description": "Сколько пожаров накопила территория за всю выбранную историю.",
        "high_phrase": "частые пожары",
        "low_phrase": "редкие пожары",
        "format": "number",
    },
    "Средняя площадь пожара": {
        "description": "Средний масштаб пожара на территории.",
        "high_phrase": "крупные площади пожара",
        "low_phrase": "небольшие площади пожара",
        "format": "number",
    },
    "Доля ночных пожаров": {
        "description": "Как часто пожары происходят ночью, когда обнаружение и реагирование обычно сложнее.",
        "high_phrase": "ночной профиль пожаров",
        "low_phrase": "в основном дневные пожары",
        "format": "percent",
    },
    "Среднее время прибытия, мин": {
        "description": "Среднее время от сообщения или обнаружения до прибытия первого подразделения.",
        "high_phrase": "долгое прибытие",
        "low_phrase": "быстрое прибытие",
        "format": "number",
    },
    "Доля тяжелых последствий": {
        "description": "Доля пожаров с ущербом, пострадавшими, погибшими или иными тяжелыми последствиями.",
        "high_phrase": "тяжелые последствия",
        "low_phrase": "умеренные последствия",
        "format": "percent",
    },
    "Доля без подтвержденного водоснабжения": {
        "description": "Как часто в истории территории не подтверждалось доступное водоснабжение на пожаре.",
        "high_phrase": "дефицит подтвержденного водоснабжения",
        "low_phrase": "вода обычно подтверждена",
        "format": "percent",
    },
    "Доля долгих прибытий": {
        "description": "Доля выездов, где прибытие занимало 20 минут и более.",
        "high_phrase": "много долгих прибытий",
        "low_phrase": "долгие прибытия редки",
        "format": "percent",
    },
    "Средняя удаленность до ПЧ, км": {
        "description": "Средняя удаленность территории от ближайшей пожарной части.",
        "high_phrase": "территории удалены от ПЧ",
        "low_phrase": "территории близко к ПЧ",
        "format": "number",
    },
    "Доля пожаров в отопительный сезон": {
        "description": "Насколько пожарная история территории сосредоточена в отопительный период.",
        "high_phrase": "отопительный профиль риска",
        "low_phrase": "слабая сезонность отопления",
        "format": "percent",
    },
    "Покрытие данных по водоснабжению": {
        "description": "В какой доле пожаров вообще есть подтвержденные записи о водоснабжении.",
        "high_phrase": "данные по воде заполнены лучше среднего",
        "low_phrase": "данные по воде часто отсутствуют",
        "format": "percent",
    },
    "Покрытие данных по времени прибытия": {
        "description": "В какой доле пожаров найдено время прибытия подразделения.",
        "high_phrase": "данные по прибытиям заполнены лучше среднего",
        "low_phrase": "данные по прибытиям часто отсутствуют",
        "format": "percent",
    },
}

CLUSTERING_LOG_SCALE_FEATURES = {
    "Число пожаров",
    "Средняя площадь пожара",
    "Среднее время прибытия, мин",
    "Средняя удаленность до ПЧ, км",
}

CLUSTERING_PROFILE_MODE_EXCLUDED_FEATURES = {"Число пожаров"}

# === ML UI ===

ML_MODEL_NAME = "ML-прогноз по числу пожаров"
ML_HISTORY_WINDOW_LABELS = FORECASTING_HISTORY_WINDOW_LABELS

ML_FEATURE_LABELS = {
    "temp_value": "Температура",
    "weekday": "День недели",
    "month": "Месяц",
    "lag_1": "Пожары вчера",
    "lag_7": "Пожары 7 дней назад",
    "lag_14": "Пожары 14 дней назад",
    "rolling_7": "Среднее за 7 дней",
    "rolling_28": "Среднее за 28 дней",
    "trend_gap": "Разница 7/28 дней",
}

ML_COUNT_MODEL_LABELS = {
    "poisson": "Регрессия Пуассона",
    "negative_binomial": "Negative Binomial GLM",
    "heuristic_forecast": "Сценарный эвристический прогноз",
    "seasonal_baseline": "Сезонная базовая модель",
}
ML_EVENT_MODEL_LABEL = "Логистическая регрессия"

ML_PREDICTION_INTERVAL_METHOD_LABEL = "Adaptive conformal interval with predicted-count bins"
ML_PREDICTION_INTERVAL_FIXED_CHRONO_LABEL = "Fixed 60/40 chrono split conformal"
ML_PREDICTION_INTERVAL_BLOCKED_CV_LABEL = "Blocked forward CV conformal"
ML_PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL = "Forward rolling split conformal"
ML_PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL = "Jackknife+ for time series"

ML_EVENT_SELECTION_RULE = (
    "Минимум Brier score, затем log-loss и ROC-AUC; при близком качестве сохраняется более простой и интерпретируемый метод."
)

ML_COUNT_SELECTION_RULE = (
    "Минимум Poisson deviance, затем MAE и RMSE среди seasonal baseline, heuristic forecast и count-model; "
    "если heuristic forecast почти не хуже лучшей count-model, сохраняется более объяснимый рабочий метод; "
    "внутри ML-паритета предпочитается Poisson."
)

ML_EVENT_BASELINE_METHOD_LABEL = "Сезонная событийная базовая модель"
ML_EVENT_BASELINE_ROLE_LABEL = "Базовая модель"
ML_EVENT_HEURISTIC_METHOD_LABEL = "Сценарная эвристическая вероятность"
ML_EVENT_HEURISTIC_ROLE_LABEL = "Сценарный прогноз"
ML_EVENT_CLASSIFIER_ROLE_LABEL = "Классификатор"

ML_PREDICTIVE_BLOCK_DESCRIPTION = (
    "ML-прогноз открывают, когда нужно оценить ожидаемое число пожаров по датам для выбранного среза. "
    "Ниже показаны прогноз количества, качество модели и факторы, которые сильнее всего влияют на результат. "
    "Этот экран не ранжирует территории и не заменяет сценарный прогноз по вероятности пожара."
)

