from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from typing import Any, Callable, Dict, Iterable, Optional, Self

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

_CACHE_LIMIT = 128

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


MlProgressCallback = Optional[Callable[[str, str], None]]


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

    def copy(self) -> Dict[str, Any]:
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
    by_horizon: Dict[int, Dict[str, Any]] = field(default_factory=dict)

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
    prediction_interval_calibration: Dict[str, Any] = field(default_factory=dict)
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
