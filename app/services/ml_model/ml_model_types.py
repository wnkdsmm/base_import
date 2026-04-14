from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from typing import Any, Callable, Dict, Iterable, Optional, Self, TypedDict

from app.domain.predictive_settings import MIN_TEMPERATURE_COVERAGE, MIN_TEMPERATURE_NON_NULL_DAYS
from app.labels import (
    ML_COUNT_MODEL_LABELS as L_ML_COUNT_MODEL_LABELS,
    ML_COUNT_SELECTION_RULE as L_ML_COUNT_SELECTION_RULE,
    ML_EVENT_BASELINE_METHOD_LABEL as L_ML_EVENT_BASELINE_METHOD_LABEL,
    ML_EVENT_BASELINE_ROLE_LABEL as L_ML_EVENT_BASELINE_ROLE_LABEL,
    ML_EVENT_CLASSIFIER_ROLE_LABEL as L_ML_EVENT_CLASSIFIER_ROLE_LABEL,
    ML_EVENT_HEURISTIC_METHOD_LABEL as L_ML_EVENT_HEURISTIC_METHOD_LABEL,
    ML_EVENT_HEURISTIC_ROLE_LABEL as L_ML_EVENT_HEURISTIC_ROLE_LABEL,
    ML_EVENT_MODEL_LABEL as L_ML_EVENT_MODEL_LABEL,
    ML_EVENT_SELECTION_RULE as L_ML_EVENT_SELECTION_RULE,
    ML_FEATURE_LABELS as L_ML_FEATURE_LABELS,
    ML_HISTORY_WINDOW_LABELS as L_ML_HISTORY_WINDOW_LABELS,
    ML_MODEL_NAME as L_ML_MODEL_NAME,
    ML_PREDICTION_INTERVAL_BLOCKED_CV_LABEL as L_ML_PREDICTION_INTERVAL_BLOCKED_CV_LABEL,
    ML_PREDICTION_INTERVAL_FIXED_CHRONO_LABEL as L_ML_PREDICTION_INTERVAL_FIXED_CHRONO_LABEL,
    ML_PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL as L_ML_PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL,
    ML_PREDICTION_INTERVAL_METHOD_LABEL as L_ML_PREDICTION_INTERVAL_METHOD_LABEL,
    ML_PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL as L_ML_PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL,
    ML_PREDICTIVE_BLOCK_DESCRIPTION as L_ML_PREDICTIVE_BLOCK_DESCRIPTION,
)
from config.constants import (
    CLASSIFICATION_THRESHOLD as C_CLASSIFICATION_THRESHOLD,
    COUNT_MODEL_CONTINUOUS_COLUMNS as C_COUNT_MODEL_CONTINUOUS_COLUMNS,
    COUNT_MODEL_KEYS as C_COUNT_MODEL_KEYS,
    COUNT_MODEL_SELECTION_TOLERANCE as C_COUNT_MODEL_SELECTION_TOLERANCE,
    EVENT_PROBABILITY_REASON_SATURATED_EVENT_RATE,
    EVENT_PROBABILITY_REASON_SINGLE_CLASS_EVALUATION,
    EVENT_PROBABILITY_REASON_TOO_FEW_COMPARABLE_WINDOWS,
    EVENT_RATE_SATURATION_MARGIN as C_EVENT_RATE_SATURATION_MARGIN,
    EXPLAINABLE_COUNT_MODEL_KEY as C_EXPLAINABLE_COUNT_MODEL_KEY,
    FEATURE_COLUMNS as C_FEATURE_COLUMNS,
    LOGISTIC_PARAMS as C_LOGISTIC_PARAMS,
    MAX_BACKTEST_POINTS as C_MAX_BACKTEST_POINTS,
    MAX_HISTORY_POINTS as C_MAX_HISTORY_POINTS,
    MIN_BACKTEST_POINTS as C_MIN_BACKTEST_POINTS,
    MIN_DAILY_HISTORY as C_MIN_DAILY_HISTORY,
    MIN_EVENT_CLASS_COUNT as C_MIN_EVENT_CLASS_COUNT,
    MIN_FEATURE_ROWS as C_MIN_FEATURE_ROWS,
    MIN_INTERVAL_BIN_RESIDUALS as C_MIN_INTERVAL_BIN_RESIDUALS,
    MIN_INTERVAL_CALIBRATION_WINDOWS as C_MIN_INTERVAL_CALIBRATION_WINDOWS,
    MIN_INTERVAL_EVALUATION_WINDOWS as C_MIN_INTERVAL_EVALUATION_WINDOWS,
    MIN_POSITIVE_PREDICTION as C_MIN_POSITIVE_PREDICTION,
    ML_CACHE_LIMIT as C_ML_CACHE_LIMIT,
    ML_CACHE_SCHEMA_VERSION,
    ML_FORECAST_DAY_OPTIONS as C_ML_FORECAST_DAY_OPTIONS,
    ML_HISTORY_WINDOWS as C_ML_HISTORY_WINDOWS,
    NEGATIVE_BINOMIAL_MIN_TRAIN_ROWS as C_NEGATIVE_BINOMIAL_MIN_TRAIN_ROWS,
    NEGATIVE_BINOMIAL_OVERDISPERSION_THRESHOLD as C_NEGATIVE_BINOMIAL_OVERDISPERSION_THRESHOLD,
    PERMUTATION_REPEATS as C_PERMUTATION_REPEATS,
    POISSON_PARAMS as C_POISSON_PARAMS,
    PREDICTION_INTERVAL_CALIBRATION_FRACTION as C_PREDICTION_INTERVAL_CALIBRATION_FRACTION,
    PREDICTION_INTERVAL_LEVEL as C_PREDICTION_INTERVAL_LEVEL,
    PREDICTION_INTERVAL_TARGET_BINS as C_PREDICTION_INTERVAL_TARGET_BINS,
    ROLLING_MIN_TRAIN_ROWS as C_ROLLING_MIN_TRAIN_ROWS,
    WARNING_INSTABILITY_MESSAGE_TOKENS,
)

MODEL_NAME = L_ML_MODEL_NAME

FORECAST_DAY_OPTIONS = list(C_ML_FORECAST_DAY_OPTIONS)
HISTORY_WINDOW_OPTIONS = [
    {"value": value, "label": L_ML_HISTORY_WINDOW_LABELS.get(value, value)}
    for value in C_ML_HISTORY_WINDOWS
]

FEATURE_LABELS = L_ML_FEATURE_LABELS

COUNT_MODEL_LABELS = L_ML_COUNT_MODEL_LABELS
EVENT_MODEL_LABEL = L_ML_EVENT_MODEL_LABEL

MIN_DAILY_HISTORY = C_MIN_DAILY_HISTORY
MIN_FEATURE_ROWS = C_MIN_FEATURE_ROWS
MIN_BACKTEST_POINTS = C_MIN_BACKTEST_POINTS
MIN_EVENT_CLASS_COUNT = C_MIN_EVENT_CLASS_COUNT
EVENT_RATE_SATURATION_MARGIN = C_EVENT_RATE_SATURATION_MARGIN
MAX_HISTORY_POINTS = C_MAX_HISTORY_POINTS
MAX_BACKTEST_POINTS = C_MAX_BACKTEST_POINTS
PERMUTATION_REPEATS = C_PERMUTATION_REPEATS
ROLLING_MIN_TRAIN_ROWS = C_ROLLING_MIN_TRAIN_ROWS
COUNT_MODEL_SELECTION_TOLERANCE = C_COUNT_MODEL_SELECTION_TOLERANCE

FEATURE_COLUMNS = list(C_FEATURE_COLUMNS)
NON_TEMPERATURE_FEATURE_COLUMNS = [column for column in FEATURE_COLUMNS if column != 'temp_value']
COUNT_MODEL_CONTINUOUS_COLUMNS = list(C_COUNT_MODEL_CONTINUOUS_COLUMNS)
COUNT_MODEL_KEYS = list(C_COUNT_MODEL_KEYS)
EXPLAINABLE_COUNT_MODEL_KEY = C_EXPLAINABLE_COUNT_MODEL_KEY
NEGATIVE_BINOMIAL_OVERDISPERSION_THRESHOLD = C_NEGATIVE_BINOMIAL_OVERDISPERSION_THRESHOLD
NEGATIVE_BINOMIAL_MIN_TRAIN_ROWS = C_NEGATIVE_BINOMIAL_MIN_TRAIN_ROWS
MIN_POSITIVE_PREDICTION = C_MIN_POSITIVE_PREDICTION
CLASSIFICATION_THRESHOLD = C_CLASSIFICATION_THRESHOLD
PREDICTION_INTERVAL_LEVEL = C_PREDICTION_INTERVAL_LEVEL
PREDICTION_INTERVAL_CALIBRATION_FRACTION = C_PREDICTION_INTERVAL_CALIBRATION_FRACTION
MIN_INTERVAL_CALIBRATION_WINDOWS = C_MIN_INTERVAL_CALIBRATION_WINDOWS
MIN_INTERVAL_EVALUATION_WINDOWS = C_MIN_INTERVAL_EVALUATION_WINDOWS
PREDICTION_INTERVAL_TARGET_BINS = C_PREDICTION_INTERVAL_TARGET_BINS
MIN_INTERVAL_BIN_RESIDUALS = C_MIN_INTERVAL_BIN_RESIDUALS
PREDICTION_INTERVAL_METHOD_LABEL = L_ML_PREDICTION_INTERVAL_METHOD_LABEL
PREDICTION_INTERVAL_FIXED_CHRONO_LABEL = L_ML_PREDICTION_INTERVAL_FIXED_CHRONO_LABEL
PREDICTION_INTERVAL_BLOCKED_CV_LABEL = L_ML_PREDICTION_INTERVAL_BLOCKED_CV_LABEL
PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL = L_ML_PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL
PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL = L_ML_PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL

EVENT_SELECTION_RULE = L_ML_EVENT_SELECTION_RULE

COUNT_SELECTION_RULE = L_ML_COUNT_SELECTION_RULE

EVENT_BASELINE_METHOD_LABEL = L_ML_EVENT_BASELINE_METHOD_LABEL
EVENT_BASELINE_ROLE_LABEL = L_ML_EVENT_BASELINE_ROLE_LABEL
EVENT_HEURISTIC_METHOD_LABEL = L_ML_EVENT_HEURISTIC_METHOD_LABEL
EVENT_HEURISTIC_ROLE_LABEL = L_ML_EVENT_HEURISTIC_ROLE_LABEL
EVENT_CLASSIFIER_ROLE_LABEL = L_ML_EVENT_CLASSIFIER_ROLE_LABEL

ML_PREDICTIVE_BLOCK_DESCRIPTION = L_ML_PREDICTIVE_BLOCK_DESCRIPTION

_CACHE_LIMIT = C_ML_CACHE_LIMIT
_POISSON_PARAMS = C_POISSON_PARAMS
_LOGISTIC_PARAMS = C_LOGISTIC_PARAMS


MlProgressCallback = Optional[Callable[[str, str], None]]


class PredictionIntervalAdaptiveBin(TypedDict, total=False):
    bin_index: int
    prediction_min: Optional[float]
    prediction_max: Optional[float]
    lower_edge: Optional[float]
    upper_edge: Optional[float]
    residual_count: int
    absolute_error_quantile: float
    fallback_to_global: bool


class PredictionIntervalCalibration(TypedDict, total=False):
    level: float
    level_display: str
    absolute_error_quantile: float
    residual_count: int
    adaptive_binning_strategy: str
    adaptive_bin_count: int
    adaptive_bin_edges: list[float]
    adaptive_bins: list[PredictionIntervalAdaptiveBin]
    method_label: str
    coverage_validated: bool
    validated_coverage: Optional[float]
    coverage_note: str
    calibration_window_count: int
    evaluation_window_count: int
    calibration_window_range_label: str
    evaluation_window_range_label: str
    validation_scheme_key: str
    validation_scheme_label: str
    validation_scheme_explanation: str
    reference_scheme_key: str
    reference_scheme_label: str


def _emit_progress(progress_callback: MlProgressCallback, phase: str, message: str) -> None:
    if progress_callback is None:
        return
    try:
        progress_callback(phase, message)
    except Exception:
        return


def _optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    return float(value)


def _optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    return int(value)


class MappingAccessMixin:
    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def keys(self) -> list[str]:
        return [field_info.name for field_info in fields(self)]

    def items(self) -> Iterable[tuple[str, Any]]:
        for key in self.keys():
            yield key, getattr(self, key)

    def values(self) -> Iterable[Any]:
        for key in self.keys():
            yield getattr(self, key)

    def __iter__(self) -> Iterable[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self.keys())

    def clone(self, **changes: Any) -> Self:
        return replace(self, **changes)

    def copy(self) -> dict[str, object]:
        return {key: getattr(self, key) for key in self.keys()}


@dataclass
class CountMetrics(MappingAccessMixin):
    mae: Optional[float] = None
    rmse: Optional[float] = None
    smape: Optional[float] = None
    poisson_deviance: Optional[float] = None
    mae_delta_vs_baseline: Optional[float] = None
    rmse_delta_vs_baseline: Optional[float] = None
    smape_delta_vs_baseline: Optional[float] = None

    @classmethod
    def coerce(cls, value: Any) -> "CountMetrics":
        if isinstance(value, cls):
            return value
        value = value or {}
        return cls(
            mae=_optional_float(value.get("mae")),
            rmse=_optional_float(value.get("rmse")),
            smape=_optional_float(value.get("smape")),
            poisson_deviance=_optional_float(value.get("poisson_deviance")),
            mae_delta_vs_baseline=_optional_float(value.get("mae_delta_vs_baseline")),
            rmse_delta_vs_baseline=_optional_float(value.get("rmse_delta_vs_baseline")),
            smape_delta_vs_baseline=_optional_float(value.get("smape_delta_vs_baseline")),
        )


@dataclass
class CountComparisonRow(MappingAccessMixin):
    method_key: str
    method_label: str
    role_label: str
    is_selected: bool
    metrics: CountMetrics = field(default_factory=CountMetrics)

    @property
    def mae(self) -> Optional[float]:
        return self.metrics.mae

    @property
    def rmse(self) -> Optional[float]:
        return self.metrics.rmse

    @property
    def smape(self) -> Optional[float]:
        return self.metrics.smape

    @property
    def poisson_deviance(self) -> Optional[float]:
        return self.metrics.poisson_deviance

    @property
    def mae_delta_vs_baseline(self) -> Optional[float]:
        return self.metrics.mae_delta_vs_baseline

    @property
    def rmse_delta_vs_baseline(self) -> Optional[float]:
        return self.metrics.rmse_delta_vs_baseline

    @property
    def smape_delta_vs_baseline(self) -> Optional[float]:
        return self.metrics.smape_delta_vs_baseline

    @classmethod
    def coerce(cls, value: Any) -> "CountComparisonRow":
        if isinstance(value, cls):
            return value
        value = value or {}
        return cls(
            method_key=str(value.get("method_key") or ""),
            method_label=str(value.get("method_label") or ""),
            role_label=str(value.get("role_label") or ""),
            is_selected=bool(value.get("is_selected")),
            metrics=CountMetrics.coerce(value),
        )


@dataclass
class EventComparisonRow(MappingAccessMixin):
    method_key: str
    method_label: str
    role_label: str
    brier_score: Optional[float] = None
    roc_auc: Optional[float] = None
    f1: Optional[float] = None
    log_loss: Optional[float] = None
    is_selected: bool = False

    @classmethod
    def coerce(cls, value: Any) -> "EventComparisonRow":
        if isinstance(value, cls):
            return value
        value = value or {}
        return cls(
            method_key=str(value.get("method_key") or ""),
            method_label=str(value.get("method_label") or ""),
            role_label=str(value.get("role_label") or ""),
            brier_score=_optional_float(value.get("brier_score")),
            roc_auc=_optional_float(value.get("roc_auc")),
            f1=_optional_float(value.get("f1")),
            log_loss=_optional_float(value.get("log_loss")),
            is_selected=bool(value.get("is_selected")),
        )


@dataclass
class EventMetrics(MappingAccessMixin):
    available: bool = False
    logistic_available: bool = False
    selected_model_key: Optional[str] = None
    selected_model_label: Optional[str] = None
    brier_score: Optional[float] = None
    baseline_brier_score: Optional[float] = None
    heuristic_brier_score: Optional[float] = None
    roc_auc: Optional[float] = None
    baseline_roc_auc: Optional[float] = None
    heuristic_roc_auc: Optional[float] = None
    f1: Optional[float] = None
    baseline_f1: Optional[float] = None
    heuristic_f1: Optional[float] = None
    log_loss: Optional[float] = None
    baseline_log_loss: Optional[float] = None
    heuristic_log_loss: Optional[float] = None
    comparison_rows: list[EventComparisonRow] = field(default_factory=list)
    rows_used: int = 0
    selection_rule: str = ""
    event_rate: Optional[float] = None
    evaluation_has_both_classes: bool = False
    event_probability_informative: bool = False
    event_probability_note: Optional[str] = None
    event_probability_reason_code: Optional[str] = None

    @classmethod
    def coerce(cls, value: Any) -> "EventMetrics":
        if isinstance(value, cls):
            return value
        value = value or {}
        return cls(
            available=bool(value.get("available")),
            logistic_available=bool(value.get("logistic_available")),
            selected_model_key=value.get("selected_model_key"),
            selected_model_label=value.get("selected_model_label"),
            brier_score=_optional_float(value.get("brier_score")),
            baseline_brier_score=_optional_float(value.get("baseline_brier_score")),
            heuristic_brier_score=_optional_float(value.get("heuristic_brier_score")),
            roc_auc=_optional_float(value.get("roc_auc")),
            baseline_roc_auc=_optional_float(value.get("baseline_roc_auc")),
            heuristic_roc_auc=_optional_float(value.get("heuristic_roc_auc")),
            f1=_optional_float(value.get("f1")),
            baseline_f1=_optional_float(value.get("baseline_f1")),
            heuristic_f1=_optional_float(value.get("heuristic_f1")),
            log_loss=_optional_float(value.get("log_loss")),
            baseline_log_loss=_optional_float(value.get("baseline_log_loss")),
            heuristic_log_loss=_optional_float(value.get("heuristic_log_loss")),
            comparison_rows=[EventComparisonRow.coerce(row) for row in value.get("comparison_rows", [])],
            rows_used=int(value.get("rows_used") or 0),
            selection_rule=str(value.get("selection_rule") or ""),
            event_rate=_optional_float(value.get("event_rate")),
            evaluation_has_both_classes=bool(value.get("evaluation_has_both_classes")),
            event_probability_informative=bool(value.get("event_probability_informative")),
            event_probability_note=value.get("event_probability_note"),
            event_probability_reason_code=value.get("event_probability_reason_code"),
        )


@dataclass
class BacktestWindowRow(MappingAccessMixin):
    origin_date: Optional[str]
    date: str
    horizon_days: int
    actual_count: float
    baseline_count: float
    heuristic_count: float
    actual_event: int
    baseline_event_probability: Optional[float]
    heuristic_event_probability: Optional[float]
    predictions: Dict[str, Optional[float]] = field(default_factory=dict)
    predicted_event_probabilities: Dict[str, Optional[float]] = field(default_factory=dict)
    predicted_event_probability: Optional[float] = None

    @classmethod
    def coerce(cls, value: Any) -> "BacktestWindowRow":
        if isinstance(value, cls):
            return value
        value = value or {}
        return cls(
            origin_date=value.get("origin_date"),
            date=str(value.get("date") or ""),
            horizon_days=int(value.get("horizon_days") or 0),
            actual_count=float(value.get("actual_count") or 0.0),
            baseline_count=float(value.get("baseline_count") or 0.0),
            heuristic_count=float(value.get("heuristic_count") or 0.0),
            actual_event=int(value.get("actual_event") or 0),
            baseline_event_probability=_optional_float(value.get("baseline_event_probability")),
            heuristic_event_probability=_optional_float(value.get("heuristic_event_probability")),
            predictions=dict(value.get("predictions") or {}),
            predicted_event_probabilities=dict(value.get("predicted_event_probabilities") or {}),
            predicted_event_probability=_optional_float(value.get("predicted_event_probability")),
        )


@dataclass
class BacktestEvaluationRow(MappingAccessMixin):
    origin_date: Optional[str]
    horizon_days: int
    date: str
    actual_count: float
    predicted_count: float
    lower_bound: float
    upper_bound: float
    baseline_count: float
    heuristic_count: float
    actual_event: int
    predicted_event_probability: Optional[float]
    baseline_event_probability: Optional[float]
    heuristic_event_probability: Optional[float]

    @classmethod
    def coerce(cls, value: Any) -> "BacktestEvaluationRow":
        if isinstance(value, cls):
            return value
        value = value or {}
        return cls(
            origin_date=value.get("origin_date"),
            horizon_days=int(value.get("horizon_days") or 0),
            date=str(value.get("date") or ""),
            actual_count=float(value.get("actual_count") or 0.0),
            predicted_count=float(value.get("predicted_count") or 0.0),
            lower_bound=float(value.get("lower_bound") or 0.0),
            upper_bound=float(value.get("upper_bound") or 0.0),
            baseline_count=float(value.get("baseline_count") or 0.0),
            heuristic_count=float(value.get("heuristic_count") or 0.0),
            actual_event=int(value.get("actual_event") or 0),
            predicted_event_probability=_optional_float(value.get("predicted_event_probability")),
            baseline_event_probability=_optional_float(value.get("baseline_event_probability")),
            heuristic_event_probability=_optional_float(value.get("heuristic_event_probability")),
        )


@dataclass
class HorizonSummary(MappingAccessMixin):
    horizon_days: int
    horizon_label: str
    folds: int
    count_metrics: CountMetrics = field(default_factory=CountMetrics)
    baseline_count_mae: Optional[float] = None
    heuristic_count_mae: Optional[float] = None
    prediction_interval_coverage: Optional[float] = None
    prediction_interval_coverage_display: str = ""
    prediction_interval_coverage_validated: bool = False
    prediction_interval_coverage_note: Optional[str] = None
    prediction_interval_validation_scheme_key: Optional[str] = None
    prediction_interval_validation_scheme_label: Optional[str] = None
    prediction_interval_method_label: Optional[str] = None

    @property
    def count_mae(self) -> Optional[float]:
        return self.count_metrics.mae

    @property
    def count_rmse(self) -> Optional[float]:
        return self.count_metrics.rmse

    @property
    def count_smape(self) -> Optional[float]:
        return self.count_metrics.smape

    @property
    def count_poisson_deviance(self) -> Optional[float]:
        return self.count_metrics.poisson_deviance

    @classmethod
    def coerce(cls, value: Any) -> "HorizonSummary":
        if isinstance(value, cls):
            return value
        value = value or {}
        return cls(
            horizon_days=int(value.get("horizon_days") or 0),
            horizon_label=str(value.get("horizon_label") or ""),
            folds=int(value.get("folds") or 0),
            count_metrics=CountMetrics(
                mae=_optional_float(value.get("count_mae")),
                rmse=_optional_float(value.get("count_rmse")),
                smape=_optional_float(value.get("count_smape")),
                poisson_deviance=_optional_float(value.get("count_poisson_deviance")),
            ),
            baseline_count_mae=_optional_float(value.get("baseline_count_mae")),
            heuristic_count_mae=_optional_float(value.get("heuristic_count_mae")),
            prediction_interval_coverage=_optional_float(value.get("prediction_interval_coverage")),
            prediction_interval_coverage_display=str(value.get("prediction_interval_coverage_display") or ""),
            prediction_interval_coverage_validated=bool(value.get("prediction_interval_coverage_validated")),
            prediction_interval_coverage_note=value.get("prediction_interval_coverage_note"),
            prediction_interval_validation_scheme_key=value.get("prediction_interval_validation_scheme_key"),
            prediction_interval_validation_scheme_label=value.get("prediction_interval_validation_scheme_label"),
            prediction_interval_method_label=value.get("prediction_interval_method_label"),
        )


@dataclass
class PredictionIntervalCalibrationByHorizon(MappingAccessMixin):
    by_horizon: dict[int, PredictionIntervalCalibration] = field(default_factory=dict)

    @classmethod
    def coerce(cls, value: Any) -> "PredictionIntervalCalibrationByHorizon":
        if isinstance(value, cls):
            return value
        if value is None:
            return cls()
        if isinstance(value, dict) and "by_horizon" in value:
            return cls(by_horizon=dict(value.get("by_horizon") or {}))
        return cls(by_horizon=dict(value or {}))


@dataclass
class BacktestOverview(MappingAccessMixin):
    folds: int = 0
    min_train_rows: int = 0
    validation_horizon_days: int = 1
    validation_horizon_label: str = "1 day"
    forecast_horizon_days: int = 1
    forecast_horizon_label: str = "1 day"
    validated_horizon_days: list[int] = field(default_factory=list)
    selection_rule: str = ""
    event_selection_rule: str = ""
    classification_threshold: float = 0.5
    event_backtest_event_rate: Optional[float] = None
    event_probability_informative: bool = False
    event_probability_note: Optional[str] = None
    event_probability_reason_code: Optional[str] = None
    candidate_model_labels: list[str] = field(default_factory=list)
    candidate_window_count: int = 0
    candidate_covered_window_count_by_model: Dict[str, int] = field(default_factory=dict)
    candidate_window_coverage_by_model: Dict[str, float] = field(default_factory=dict)
    dispersion_ratio: Optional[float] = None
    prediction_interval_level: Optional[float] = None
    prediction_interval_level_display: str = ""
    prediction_interval_coverage: Optional[float] = None
    prediction_interval_coverage_display: str = ""
    prediction_interval_method_label: str = ""
    prediction_interval_coverage_validated: bool = False
    prediction_interval_coverage_note: str = ""
    prediction_interval_calibration_windows: int = 0
    prediction_interval_evaluation_windows: int = 0
    prediction_interval_validation_scheme_key: Optional[str] = None
    prediction_interval_validation_scheme_label: Optional[str] = None
    prediction_interval_validation_explanation: Optional[str] = None
    prediction_interval_calibration_range_label: str = ""
    prediction_interval_evaluation_range_label: str = ""
    prediction_interval_validated_horizon_days: list[int] = field(default_factory=list)
    prediction_interval_coverage_by_horizon: Dict[str, Optional[float]] = field(default_factory=dict)
    prediction_interval_coverage_display_by_horizon: Dict[str, str] = field(default_factory=dict)
    rolling_scheme_label: str = ""

    @classmethod
    def coerce(cls, value: Any) -> "BacktestOverview":
        if isinstance(value, cls):
            return value
        value = value or {}
        return cls(
            folds=int(value.get("folds") or 0),
            min_train_rows=int(value.get("min_train_rows") or 0),
            validation_horizon_days=int(value.get("validation_horizon_days") or 1),
            validation_horizon_label=str(value.get("validation_horizon_label") or "1 day"),
            forecast_horizon_days=int(value.get("forecast_horizon_days") or 1),
            forecast_horizon_label=str(value.get("forecast_horizon_label") or "1 day"),
            validated_horizon_days=[int(item) for item in value.get("validated_horizon_days", [])],
            selection_rule=str(value.get("selection_rule") or ""),
            event_selection_rule=str(value.get("event_selection_rule") or ""),
            classification_threshold=float(value.get("classification_threshold") or 0.5),
            event_backtest_event_rate=_optional_float(value.get("event_backtest_event_rate")),
            event_probability_informative=bool(value.get("event_probability_informative")),
            event_probability_note=value.get("event_probability_note"),
            event_probability_reason_code=value.get("event_probability_reason_code"),
            candidate_model_labels=[str(item) for item in value.get("candidate_model_labels", [])],
            candidate_window_count=int(value.get("candidate_window_count") or 0),
            candidate_covered_window_count_by_model=dict(value.get("candidate_covered_window_count_by_model") or {}),
            candidate_window_coverage_by_model=dict(value.get("candidate_window_coverage_by_model") or {}),
            dispersion_ratio=_optional_float(value.get("dispersion_ratio")),
            prediction_interval_level=_optional_float(value.get("prediction_interval_level")),
            prediction_interval_level_display=str(value.get("prediction_interval_level_display") or ""),
            prediction_interval_coverage=_optional_float(value.get("prediction_interval_coverage")),
            prediction_interval_coverage_display=str(value.get("prediction_interval_coverage_display") or ""),
            prediction_interval_method_label=str(value.get("prediction_interval_method_label") or ""),
            prediction_interval_coverage_validated=bool(value.get("prediction_interval_coverage_validated")),
            prediction_interval_coverage_note=str(value.get("prediction_interval_coverage_note") or ""),
            prediction_interval_calibration_windows=int(value.get("prediction_interval_calibration_windows") or 0),
            prediction_interval_evaluation_windows=int(value.get("prediction_interval_evaluation_windows") or 0),
            prediction_interval_validation_scheme_key=value.get("prediction_interval_validation_scheme_key"),
            prediction_interval_validation_scheme_label=value.get("prediction_interval_validation_scheme_label"),
            prediction_interval_validation_explanation=value.get("prediction_interval_validation_explanation"),
            prediction_interval_calibration_range_label=str(value.get("prediction_interval_calibration_range_label") or ""),
            prediction_interval_evaluation_range_label=str(value.get("prediction_interval_evaluation_range_label") or ""),
            prediction_interval_validated_horizon_days=[
                int(item) for item in value.get("prediction_interval_validated_horizon_days", [])
            ],
            prediction_interval_coverage_by_horizon=dict(value.get("prediction_interval_coverage_by_horizon") or {}),
            prediction_interval_coverage_display_by_horizon=dict(
                value.get("prediction_interval_coverage_display_by_horizon") or {}
            ),
            rolling_scheme_label=str(value.get("rolling_scheme_label") or ""),
        )


@dataclass
class BacktestFailure(MappingAccessMixin):
    is_ready: bool = False
    message: str = ""


@dataclass
class BacktestSuccess(MappingAccessMixin):
    is_ready: bool = True
    message: str = ""
    rows: list[BacktestEvaluationRow] = field(default_factory=list)
    window_rows: list[BacktestWindowRow] = field(default_factory=list)
    baseline_metrics: CountMetrics = field(default_factory=CountMetrics)
    heuristic_metrics: CountMetrics = field(default_factory=CountMetrics)
    count_metrics: Dict[str, CountMetrics] = field(default_factory=dict)
    count_comparison_rows: list[CountComparisonRow] = field(default_factory=list)
    selected_count_model_key: str = ""
    selected_count_model_reason: str = ""
    selected_count_model_reason_short: str = ""
    selected_metrics: CountMetrics = field(default_factory=CountMetrics)
    prediction_interval_calibration: PredictionIntervalCalibration = field(default_factory=dict)
    prediction_interval_calibration_by_horizon: PredictionIntervalCalibrationByHorizon = field(
        default_factory=PredictionIntervalCalibrationByHorizon
    )
    event_metrics: EventMetrics = field(default_factory=EventMetrics)
    horizon_summaries: Dict[str, HorizonSummary] = field(default_factory=dict)
    backtest_overview: BacktestOverview = field(default_factory=BacktestOverview)


BacktestRunResult = BacktestSuccess | BacktestFailure


def coerce_backtest_result(value: Any) -> BacktestRunResult:
    if isinstance(value, (BacktestSuccess, BacktestFailure)):
        return value
    value = value or {}
    if not bool(value.get("is_ready")):
        return BacktestFailure(message=str(value.get("message") or ""))
    return BacktestSuccess(
        message=str(value.get("message") or ""),
        rows=[BacktestEvaluationRow.coerce(row) for row in value.get("rows", [])],
        window_rows=[BacktestWindowRow.coerce(row) for row in value.get("window_rows", [])],
        baseline_metrics=CountMetrics.coerce(value.get("baseline_metrics")),
        heuristic_metrics=CountMetrics.coerce(value.get("heuristic_metrics")),
        count_metrics={
            str(model_key): CountMetrics.coerce(metrics)
            for model_key, metrics in (value.get("count_metrics") or {}).items()
        },
        count_comparison_rows=[CountComparisonRow.coerce(row) for row in value.get("count_comparison_rows", [])],
        selected_count_model_key=str(value.get("selected_count_model_key") or ""),
        selected_count_model_reason=str(value.get("selected_count_model_reason") or ""),
        selected_count_model_reason_short=str(value.get("selected_count_model_reason_short") or ""),
        selected_metrics=CountMetrics.coerce(value.get("selected_metrics")),
        prediction_interval_calibration=dict(value.get("prediction_interval_calibration") or {}),
        prediction_interval_calibration_by_horizon=PredictionIntervalCalibrationByHorizon.coerce(
            value.get("prediction_interval_calibration_by_horizon")
        ),
        event_metrics=EventMetrics.coerce(value.get("event_metrics")),
        horizon_summaries={
            str(horizon_key): HorizonSummary.coerce(summary)
            for horizon_key, summary in (value.get("horizon_summaries") or {}).items()
        },
        backtest_overview=BacktestOverview.coerce(value.get("backtest_overview")),
    )


# --- Training internals ---
# `training.constants` and `training.domain_types` were thin re-export shims over
# this module. `training.runtime` only exposed `MlProgressCallback` and
# `_emit_progress`, which are defined above.
