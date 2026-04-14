from __future__ import annotations

from datetime import date
from typing import Any, TypedDict, TypeAlias

from ..ml_model_types import PredictionIntervalAdaptiveBin, PredictionIntervalCalibration


class TrainingTemperatureStats(TypedDict, total=False):
    """Temperature quality summary used by training and recursive forecast steps."""

    usable: bool
    non_null_days: int
    total_days: int
    coverage: float
    note: str


class TrainingHistoryRecord(TypedDict, total=False):
    """Single daily history record used to build model features."""

    date: Any
    count: float
    avg_temperature: float | None


class TrainingModelArtifact(TypedDict, total=False):
    """Serialized model artifact with resolved training columns."""

    columns: list[str]


class TrainingFeatureImportanceRow(TypedDict, total=False):
    """Feature-importance row shown in ML explanation block."""

    label: str
    value: float
    value_display: str
    tone: str
    explanation: str


class TrainingForecastPathPoint(TypedDict, total=False):
    """Recursive future simulation point before UI formatting."""

    step: int
    target_date: date
    temp_value: float | None
    forecast_value: float
    event_probability: float | None


class TrainingForecastRow(TypedDict, total=False):
    """Final forecast row for ML output with risk and interval metadata."""

    horizon_days: int
    date: str
    date_display: str
    forecast_value: float
    forecast_value_display: str
    lower_bound: float
    lower_bound_display: str
    upper_bound: float
    upper_bound_display: str
    range_label: str
    range_display: str
    temperature_display: str
    risk_index: float
    risk_index_display: str
    risk_level_label: str
    risk_level_tone: str
    event_probability: float | None
    event_probability_display: str


class TrainingResultPayload(TypedDict, total=False):
    """Top-level ML training payload returned by training orchestration."""

    is_ready: bool
    message: str
    forecast_rows: list[TrainingForecastRow]
    feature_importance: list[TrainingFeatureImportanceRow]
    feature_importance_source_key: str | None
    feature_importance_source_label: str | None
    feature_importance_note: str | None
    selected_count_model_key: str


class PredictionIntervalBinsResult(TypedDict, total=False):
    """Adaptive binning metadata for prediction interval calibration."""

    strategy: str
    edges: list[float]
    bins: list[PredictionIntervalAdaptiveBin]
    bin_count: int


class PredictionIntervalStabilitySummary(TypedDict, total=False):
    """Coverage stability metrics for candidate validation schemes."""

    coverage: float | None
    coverage_gap: float
    segment_coverages: list[float]
    segment_coverage_std: float
    stability_score: float


class PredictionIntervalCandidate(TypedDict, total=False):
    """Single validated candidate for interval calibration selection."""

    scheme_key: str
    scheme_label: str
    coverage: float | None
    coverage_display: str
    coverage_gap: float
    segment_coverages: list[float]
    segment_coverage_std: float
    stability_score: float
    calibration_window_count: int
    evaluation_window_count: int
    calibration_window_range_label: str
    evaluation_window_range_label: str
    calibration_refresh_count: int
    validation_block_count: int
    covered_flags: list[bool]
    calibration: PredictionIntervalCalibration


class PredictionIntervalBacktestEvaluation(TypedDict, total=False):
    """Backtest evaluation summary and selected calibration payload."""

    calibration: PredictionIntervalCalibration
    coverage: float | None
    coverage_validated: bool
    coverage_note: str
    calibration_window_count: int
    evaluation_window_count: int
    calibration_window_range_label: str
    evaluation_window_range_label: str
    validation_scheme_key: str
    validation_scheme_label: str
    validation_scheme_explanation: str
    reference_candidate: PredictionIntervalCandidate | None
    runner_up_candidate: PredictionIntervalCandidate | None


IntervalCalibrationByHorizon: TypeAlias = dict[int, PredictionIntervalCalibration] | dict[str, PredictionIntervalCalibration]
IntervalCalibrationInput: TypeAlias = PredictionIntervalCalibration | IntervalCalibrationByHorizon
