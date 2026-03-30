from __future__ import annotations

from app.statistics_constants import EXCLUDED_TABLE_PREFIXES as TABLE_EXCLUDED_PREFIXES

CLUSTER_COUNT_OPTIONS = [2, 3, 4, 5, 6]
SAMPLE_LIMIT_OPTIONS = [50, 100, 200, 500, 1000]
SAMPLING_STRATEGY_OPTIONS = [
    {"value": "stratified", "label": "Стратифицированная"},
    {"value": "random", "label": "Случайная"},
]
CARD_TONES = ["group", "area", "table", "fire", "muted"]
MAX_FEATURE_OPTIONS = 12
MIN_ROWS_PER_CLUSTER = 5
MAX_K_DIAGNOSTICS = max(CLUSTER_COUNT_OPTIONS)

DEFAULT_CLUSTER_FEATURES = [
    "Число пожаров",
    "Средняя площадь пожара",
    "Доля ночных пожаров",
    "Среднее время прибытия, мин",
    "Доля без подтвержденного водоснабжения",
]
AUTO_DEFAULT_EXCLUDED_FEATURES = {
    "Покрытие данных по водоснабжению",
    "Покрытие данных по времени прибытия",
}
DEFAULT_FEATURE_TARGET_COUNT = len(DEFAULT_CLUSTER_FEATURES)
MIN_DEFAULT_FEATURE_COUNT = 4
FEATURE_SELECTION_MIN_IMPROVEMENT = 0.0025
DEFAULT_CLUSTER_MODE_PROFILE = "territory_profile"
DEFAULT_CLUSTER_MODE_LOAD = "load_profile"
DEFAULT_CLUSTER_MODE_PROFILE_LABEL = "Профиль территории"
DEFAULT_CLUSTER_MODE_LOAD_LABEL = "Профиль территории + нагрузка"
WEIGHTING_STRATEGY_UNIFORM = "uniform"
WEIGHTING_STRATEGY_INCIDENT_LOG = "incident_log"
WEIGHTING_STRATEGY_NOT_APPLICABLE = "not_applicable"
WEIGHTING_STRATEGY_UNIFORM_LABEL = "Равный вес территорий"
WEIGHTING_STRATEGY_INCIDENT_LOG_LABEL = "Умеренный вес по числу пожаров"
WEIGHTING_STRATEGY_NOT_APPLICABLE_LABEL = "Весы не применяются"
PROFILE_MODE_EXCLUDED_FEATURES = {"Число пожаров"}
PROFILE_MODE_SCORE_TOLERANCE = 0.01
PROFILE_MODE_SILHOUETTE_TOLERANCE = 0.015
VOLUME_DOMINANCE_RATIO = 1.35
VOLUME_DOMINANCE_MIN_SCORE_DELTA = 0.01
RATE_SMOOTHING_PRIOR_STRENGTH = 3.0
MEAN_SMOOTHING_PRIOR_STRENGTH = 2.0
LOW_SUPPORT_TERRITORY_THRESHOLD = 2
STABILITY_RESAMPLE_RATIO = 0.8
STABILITY_RANDOM_SEEDS = [7, 21, 42, 84, 126]

FEATURE_METADATA = {
    "Число пожаров": {
        "description": "Сколько пожаров накопила территория за всю выбранную историю.",
        "high_phrase": "частые пожары",
        "low_phrase": "редкие пожары",
    },
    "Средняя площадь пожара": {
        "description": "Средний масштаб пожара на территории.",
        "high_phrase": "крупные площади пожара",
        "low_phrase": "небольшие площади пожара",
    },
    "Доля ночных пожаров": {
        "description": "Как часто пожары происходят ночью, когда обнаружение и реагирование обычно сложнее.",
        "high_phrase": "ночной профиль пожаров",
        "low_phrase": "в основном дневные пожары",
    },
    "Среднее время прибытия, мин": {
        "description": "Среднее время от сообщения или обнаружения до прибытия первого подразделения.",
        "high_phrase": "долгое прибытие",
        "low_phrase": "быстрое прибытие",
    },
    "Доля тяжелых последствий": {
        "description": "Доля пожаров с ущербом, пострадавшими, погибшими или иными тяжелыми последствиями.",
        "high_phrase": "тяжелые последствия",
        "low_phrase": "умеренные последствия",
    },
    "Доля без подтвержденного водоснабжения": {
        "description": "Как часто в истории территории не подтверждалось доступное водоснабжение на пожаре.",
        "high_phrase": "дефицит подтвержденного водоснабжения",
        "low_phrase": "вода обычно подтверждена",
    },
    "Доля долгих прибытий": {
        "description": "Доля выездов, где прибытие занимало 20 минут и более.",
        "high_phrase": "много долгих прибытий",
        "low_phrase": "долгие прибытия редки",
    },
    "Средняя удаленность до ПЧ, км": {
        "description": "Средняя удаленность территории от ближайшей пожарной части.",
        "high_phrase": "территории удалены от ПЧ",
        "low_phrase": "территории близко к ПЧ",
    },
    "Доля пожаров в отопительный сезон": {
        "description": "Насколько пожарная история территории сосредоточена в отопительный период.",
        "high_phrase": "отопительный профиль риска",
        "low_phrase": "слабая сезонность отопления",
    },
    "Покрытие данных по водоснабжению": {
        "description": "В какой доле пожаров вообще есть подтвержденные записи о водоснабжении.",
        "high_phrase": "данные по воде заполнены лучше среднего",
        "low_phrase": "данные по воде часто отсутствуют",
    },
    "Покрытие данных по времени прибытия": {
        "description": "В какой доле пожаров найдено время прибытия подразделения.",
        "high_phrase": "данные по прибытиям заполнены лучше среднего",
        "low_phrase": "данные по прибытиям часто отсутствуют",
    },
}

LOG_SCALE_FEATURES = {
    "Число пожаров",
    "Средняя площадь пожара",
    "Среднее время прибытия, мин",
    "Средняя удаленность до ПЧ, км",
}
