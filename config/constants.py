from __future__ import annotations

"""Centralized project constants grouped by domain.

This module is the canonical source for non-UI constants.
UI labels (especially Russian copy) live in ``app.labels``.
"""

import os

# === DB / Config ===

NULL_THRESHOLD = 0.9
UNIQUE_ID_THRESHOLD = 0.99
LOW_VARIANCE_THRESHOLD = 0.0001
DOMINANT_VALUE_THRESHOLD = 0.85

MISSING_LIKE_VALUES = [
    "нет данных",
    "н/д",
    "nan",
    "none",
    "null",
    "-",
    "",
    " ",
]

PROFILING_CSV_SUFFIX = "_fires_profiling_report.csv"
PROFILING_XLSX_SUFFIX = "_fires_profiling_report.xlsx"


# === Business Rules ===

CORR_THRESHOLD = 0.9
VIF_THRESHOLD = 10
IMPORTANT_KEYWORDS = [
    "травмировать",
    "погибнуть",
    "эвакуировать",
    "ребёнок",
]

FORECASTING_FORECAST_DAY_OPTIONS = [7, 14, 30, 60]
FORECASTING_HISTORY_WINDOWS = ("all", "recent_3", "recent_5")
PRIORITY_HORIZON_DAYS = 14

ACCESS_POINT_LIMIT_OPTIONS = [10, 25, 50, 100]
DEFAULT_ACCESS_POINT_LIMIT = 25
MIN_ACCESS_POINT_SUPPORT = 3
LONG_RESPONSE_THRESHOLD_MINUTES = 20.0
TOP_POINT_CARD_COUNT = 5
MAX_INCOMPLETE_POINTS = 6
MAX_NOTES = 7
MAX_ACCESS_POINT_FEATURE_OPTIONS = 10
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

CLUSTER_COUNT_OPTIONS = [2, 3, 4, 5, 6, 7, 8, 9, 10]
SAMPLE_LIMIT_OPTIONS = [50, 100, 200, 500, 1000]
SAMPLING_STRATEGY_VALUES = ["stratified", "random"]
CARD_TONES = ["group", "area", "table", "fire", "muted"]
MAX_FEATURE_OPTIONS = 12
MIN_ROWS_PER_CLUSTER = 5
MIN_DEFAULT_FEATURE_COUNT = 4
FEATURE_SELECTION_MIN_IMPROVEMENT = 0.0025
DEFAULT_CLUSTER_MODE_PROFILE = "territory_profile"
DEFAULT_CLUSTER_MODE_LOAD = "load_profile"
WEIGHTING_STRATEGY_UNIFORM = "uniform"
WEIGHTING_STRATEGY_INCIDENT_LOG = "incident_log"
WEIGHTING_STRATEGY_NOT_APPLICABLE = "not_applicable"
PROFILE_MODE_SCORE_TOLERANCE = 0.01
PROFILE_MODE_SILHOUETTE_TOLERANCE = 0.015
VOLUME_DOMINANCE_RATIO = 1.35
VOLUME_DOMINANCE_MIN_SCORE_DELTA = 0.01
RATE_SMOOTHING_PRIOR_STRENGTH = 3.0
MEAN_SMOOTHING_PRIOR_STRENGTH = 2.0
LOW_SUPPORT_TERRITORY_THRESHOLD = 2
STABILITY_RESAMPLE_RATIO = 0.8
STABILITY_RANDOM_SEEDS = [7, 21, 42, 84, 126]
HOPKINS_MIN_CLUSTERABLE = 0.6
FEATURE_SELECTION_N_INIT = 20
MODEL_N_INIT = 40
GAP_STAT_MAX_WORKERS = min(4, (os.cpu_count() or 1))
GAP_STAT_N_REFERENCES = 10

MAX_TERRITORIES = 12
MIN_TEMPERATURE_NON_NULL_DAYS = 30
MIN_TEMPERATURE_COVERAGE = 0.20
GEO_LOOKBACK_DAYS = 180
MAX_GEO_CHART_POINTS = 40
MAX_GEO_HOTSPOTS = 8

# Explainable logistics thresholds.
SERVICE_TIME_TARGET_MINUTES = 20.0
SERVICE_DISTANCE_TARGET_KM = 12.0
CORE_SERVICE_TIME_MINUTES = 14.0


# === ML ===

ML_FORECAST_DAY_OPTIONS = [7, 14, 30]
ML_HISTORY_WINDOWS = ("all", "recent_3", "recent_5")

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

FEATURE_COLUMNS = [
    "temp_value",
    "weekday",
    "month",
    "lag_1",
    "lag_7",
    "lag_14",
    "rolling_7",
    "rolling_28",
    "trend_gap",
]
COUNT_MODEL_CONTINUOUS_COLUMNS = [
    "temp_value",
    "lag_1",
    "lag_7",
    "lag_14",
    "rolling_7",
    "rolling_28",
    "trend_gap",
]
COUNT_MODEL_KEYS = ["poisson", "negative_binomial"]
EXPLAINABLE_COUNT_MODEL_KEY = "poisson"
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

EVENT_PROBABILITY_REASON_TOO_FEW_COMPARABLE_WINDOWS = "too_few_comparable_windows"
EVENT_PROBABILITY_REASON_SINGLE_CLASS_EVALUATION = "single_class_evaluation"
EVENT_PROBABILITY_REASON_SATURATED_EVENT_RATE = "saturated_event_rate"

WARNING_INSTABILITY_MESSAGE_TOKENS = (
    "perfect separation",
    "perfect prediction",
    "parameter may not be identified",
    "parameters are not identified",
    "failed to converge",
    "did not converge",
    "singular",
    "hessian",
)

ML_CACHE_SCHEMA_VERSION = 2
ML_CACHE_LIMIT = 128

POISSON_PARAMS = {
    "alpha": 0.40,
    "max_iter": 2000,
}

LOGISTIC_PARAMS = {
    "solver": "liblinear",
    "max_iter": 500,
    "class_weight": "balanced",
    "random_state": 42,
}

