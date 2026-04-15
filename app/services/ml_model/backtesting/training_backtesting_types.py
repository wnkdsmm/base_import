from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from ..ml_model_result_types import BacktestEvaluationRow, BacktestWindowRow, CountMetrics, EventComparisonRow, EventMetrics, HorizonSummary


@dataclass
class _HorizonEvaluationData:
    rows: List[BacktestWindowRow]
    actuals: np.ndarray
    baseline_predictions: np.ndarray
    heuristic_predictions: np.ndarray
    selected_predictions: np.ndarray
    count_model_predictions: Dict[str, np.ndarray]
    actual_events: np.ndarray
    baseline_event_probabilities: np.ndarray
    heuristic_event_probabilities: np.ndarray
    selected_event_probabilities: np.ndarray
    count_model_event_probabilities: Dict[str, np.ndarray]
    coverage_by_model: Dict[str, _CandidateCoverage]
    dates: List[str]


@dataclass
class _BacktestSelection:
    scored_candidates: _ScoredCandidates
    baseline_metrics: CountMetrics
    heuristic_metrics: CountMetrics
    count_metrics: Dict[str, CountMetrics]
    selected_count_model_key: str
    selected_metrics: CountMetrics
    selection_details: dict[str, Any]  # one-off
    overdispersion_ratio: float
    validation_evaluation_data: _HorizonEvaluationData


@dataclass
class _EventMetricInputs:
    common_rows: int
    rows_used: int
    actuals: np.ndarray
    baseline_probabilities: np.ndarray
    heuristic_probabilities: np.ndarray
    classifier_probabilities: np.ndarray
    logistic_available: bool


@dataclass
class _EventMetricMaskContext:
    common_rows: int
    evaluation_mask: np.ndarray
    rows_used: int
    logistic_available: bool


@dataclass
class _EventMetricSelection:
    selected_model_key: str
    selected_model_label: str
    selected_metrics: dict[str, Any]  # one-off
    selected_roc_auc: Optional[float]
    selected_log_loss: Optional[float]
    comparison_rows: List[EventComparisonRow]


@dataclass
class _EventProbabilityScores:
    heuristic_metrics: dict[str, Any]  # one-off
    baseline_roc_auc: Optional[float]
    heuristic_roc_auc: Optional[float]
    baseline_log_loss: Optional[float]
    heuristic_log_loss: Optional[float]


@dataclass
class _EventMetricContext:
    event_rate: Optional[float]
    evaluation_has_both_classes: bool
    event_probability_informative: bool
    event_probability_note: Optional[str]
    event_probability_reason_code: Optional[str]


@dataclass
class _BacktestEvaluationArtifacts:
    selection: _BacktestSelection
    selected_count_model_key: str
    interval_calibration_by_horizon: dict[int, dict[str, Any]]  # one-off
    horizon_summaries: Dict[str, HorizonSummary]
    prediction_interval_calibration: dict[str, Any]  # one-off
    validation_summary: HorizonSummary
    backtest_rows: List[BacktestEvaluationRow]
    event_metrics: EventMetrics
    window_rows: List[BacktestWindowRow]


@dataclass
class _BacktestOriginSelection:
    available_backtest_points: int
    selected_origin_dates: List[Any]
    total_windows: int


@dataclass
class _BacktestRunContext:
    history_frame: pd.DataFrame
    dataset: pd.DataFrame
    history_dates: np.ndarray
    validation_horizon_days: int
    max_horizon_days: int
    horizon_days: List[int]
    min_train_rows: int


@dataclass
class _BacktestWindow:
    origin_date: pd.Timestamp
    prepared_train: pd.DataFrame
    future_rows: pd.DataFrame
    feature_columns: List[str]
    model_train_design: pd.DataFrame
    count_targets: np.ndarray
    event_targets: np.ndarray
    temperature_stats: dict[str, Any]  # one-off


@dataclass
class _WindowCandidateFits:
    event_bundle: Optional[dict[str, Any]]  # one-off
    forecast_paths: dict[str, Optional[List[BacktestWindowRow]]]  # one-off


@dataclass
class _CandidateCoverage:
    covered_window_count: int
    window_count: int
    window_coverage: float


@dataclass
class _ScoredCandidates:
    baseline_metrics: CountMetrics
    heuristic_metrics: CountMetrics
    count_metrics: Dict[str, CountMetrics]
    coverage_by_model: Dict[str, _CandidateCoverage]
