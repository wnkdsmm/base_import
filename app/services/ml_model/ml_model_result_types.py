from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from typing import Any, Dict, Iterable, Optional, Self, TypedDict

from .ml_model_interval_types import PredictionIntervalCalibration


def _optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    return float(value)


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
class EventScoreMetrics:
    brier_score: Optional[float] = None
    roc_auc: Optional[float] = None
    f1: Optional[float] = None
    log_loss: Optional[float] = None

    @classmethod
    def coerce(cls, value: Any) -> "EventScoreMetrics":
        if isinstance(value, cls):
            return value
        value = value or {}
        return cls(
            brier_score=_optional_float(value.get("brier_score")),
            roc_auc=_optional_float(value.get("roc_auc")),
            f1=_optional_float(value.get("f1")),
            log_loss=_optional_float(value.get("log_loss")),
        )


@dataclass
class EventMetrics(MappingAccessMixin):
    available: bool = False
    logistic_available: bool = False
    selected_model_key: Optional[str] = None
    selected_model_label: Optional[str] = None
    selected_metrics: EventScoreMetrics = field(default_factory=EventScoreMetrics)
    baseline_metrics: EventScoreMetrics = field(default_factory=EventScoreMetrics)
    heuristic_metrics: EventScoreMetrics = field(default_factory=EventScoreMetrics)
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
        selected_metrics_raw_value = value.get("selected_metrics")
        selected_metrics_raw = selected_metrics_raw_value if selected_metrics_raw_value is not None else {
            "brier_score": value.get("brier_score"),
            "roc_auc": value.get("roc_auc"),
            "f1": value.get("f1"),
            "log_loss": value.get("log_loss"),
        }
        baseline_metrics_raw_value = value.get("baseline_metrics")
        baseline_metrics_raw = baseline_metrics_raw_value if baseline_metrics_raw_value is not None else {
            "brier_score": value.get("baseline_brier_score"),
            "roc_auc": value.get("baseline_roc_auc"),
            "f1": value.get("baseline_f1"),
            "log_loss": value.get("baseline_log_loss"),
        }
        heuristic_metrics_raw_value = value.get("heuristic_metrics")
        heuristic_metrics_raw = heuristic_metrics_raw_value if heuristic_metrics_raw_value is not None else {
            "brier_score": value.get("heuristic_brier_score"),
            "roc_auc": value.get("heuristic_roc_auc"),
            "f1": value.get("heuristic_f1"),
            "log_loss": value.get("heuristic_log_loss"),
        }
        return cls(
            available=bool(value.get("available")),
            logistic_available=bool(value.get("logistic_available")),
            selected_model_key=value.get("selected_model_key"),
            selected_model_label=value.get("selected_model_label"),
            selected_metrics=EventScoreMetrics.coerce(selected_metrics_raw),
            baseline_metrics=EventScoreMetrics.coerce(baseline_metrics_raw),
            heuristic_metrics=EventScoreMetrics.coerce(heuristic_metrics_raw),
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


class _ScalarFieldsMap(TypedDict, total=False):
    str: list[str]
    int: list[str]
    bool: list[str]
    optional_float: list[str]


def _coerce_scalar_fields(value: Dict[str, Any], cls_fields_map: _ScalarFieldsMap) -> Dict[str, Any]:
    # Shared scalar coercion helper for dataclass `.coerce()` methods.
    kwargs: Dict[str, Any] = {}
    for field_name in cls_fields_map.get("str", []):
        kwargs[field_name] = str(value.get(field_name) or "")
    for field_name in cls_fields_map.get("int", []):
        kwargs[field_name] = int(value.get(field_name) or 0)
    for field_name in cls_fields_map.get("bool", []):
        kwargs[field_name] = bool(value.get(field_name))
    for field_name in cls_fields_map.get("optional_float", []):
        kwargs[field_name] = _optional_float(value.get(field_name))
    return kwargs


@dataclass
class BacktestOverview(MappingAccessMixin):
    _STR_FIELDS = [
        "selection_rule",
        "event_selection_rule",
        "prediction_interval_level_display",
        "prediction_interval_coverage_display",
        "prediction_interval_method_label",
        "prediction_interval_coverage_note",
        "prediction_interval_calibration_range_label",
        "prediction_interval_evaluation_range_label",
        "rolling_scheme_label",
    ]
    _INT_FIELDS = [
        "folds",
        "min_train_rows",
        "candidate_window_count",
        "prediction_interval_calibration_windows",
        "prediction_interval_evaluation_windows",
    ]
    _BOOL_FIELDS = [
        "event_probability_informative",
        "prediction_interval_coverage_validated",
    ]
    _OPTIONAL_FLOAT_FIELDS = [
        "event_backtest_event_rate",
        "dispersion_ratio",
        "prediction_interval_level",
        "prediction_interval_coverage",
    ]

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

    @staticmethod
    def _coerce_complex_fields(value: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "validation_horizon_days": int(value.get("validation_horizon_days") or 1),
            "validation_horizon_label": str(value.get("validation_horizon_label") or "1 day"),
            "forecast_horizon_days": int(value.get("forecast_horizon_days") or 1),
            "forecast_horizon_label": str(value.get("forecast_horizon_label") or "1 day"),
            "classification_threshold": float(value.get("classification_threshold") or 0.5),
            "event_probability_note": value.get("event_probability_note"),
            "event_probability_reason_code": value.get("event_probability_reason_code"),
            "prediction_interval_validation_scheme_key": value.get("prediction_interval_validation_scheme_key"),
            "prediction_interval_validation_scheme_label": value.get("prediction_interval_validation_scheme_label"),
            "prediction_interval_validation_explanation": value.get("prediction_interval_validation_explanation"),
            "validated_horizon_days": [int(item) for item in value.get("validated_horizon_days", [])],
            "candidate_model_labels": [str(item) for item in value.get("candidate_model_labels", [])],
            "candidate_covered_window_count_by_model": dict(value.get("candidate_covered_window_count_by_model") or {}),
            "candidate_window_coverage_by_model": dict(value.get("candidate_window_coverage_by_model") or {}),
            "prediction_interval_validated_horizon_days": [int(item) for item in value.get("prediction_interval_validated_horizon_days", [])],
            "prediction_interval_coverage_by_horizon": dict(value.get("prediction_interval_coverage_by_horizon") or {}),
            "prediction_interval_coverage_display_by_horizon": dict(value.get("prediction_interval_coverage_display_by_horizon") or {}),
        }

    @classmethod
    def coerce(cls, value: Any) -> "BacktestOverview":
        if isinstance(value, cls):
            return value
        value = value or {}
        scalar_fields_map: _ScalarFieldsMap = {
            "str": cls._STR_FIELDS,
            "int": cls._INT_FIELDS,
            "bool": cls._BOOL_FIELDS,
            "optional_float": cls._OPTIONAL_FLOAT_FIELDS,
        }
        kwargs = _coerce_scalar_fields(value, scalar_fields_map)
        kwargs.update(cls._coerce_complex_fields(value))
        return cls(**kwargs)


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
    horizon_7_mae: Optional[float] = None


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
        horizon_7_mae=_optional_float(value.get("horizon_7_mae")),
    )
