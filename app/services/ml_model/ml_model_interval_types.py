from __future__ import annotations

from typing import TypedDict

from .ml_model_config_types import (
    MIN_INTERVAL_BIN_RESIDUALS,
    MIN_INTERVAL_CALIBRATION_WINDOWS,
    MIN_INTERVAL_EVALUATION_WINDOWS,
    PREDICTION_INTERVAL_BLOCKED_CV_LABEL,
    PREDICTION_INTERVAL_CALIBRATION_FRACTION,
    PREDICTION_INTERVAL_FIXED_CHRONO_LABEL,
    PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL,
    PREDICTION_INTERVAL_LEVEL,
    PREDICTION_INTERVAL_METHOD_LABEL,
    PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL,
    PREDICTION_INTERVAL_TARGET_BINS,
)


class PredictionIntervalAdaptiveBin(TypedDict, total=False):
    bin_index: int
    prediction_min: float | None
    prediction_max: float | None
    lower_edge: float | None
    upper_edge: float | None
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
    validated_coverage: float | None
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

