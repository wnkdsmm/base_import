from __future__ import annotations

from datetime import date
from typing import Any, TypedDict, TypeAlias

from app.services.forecasting.types import (
    ForecastingDailyHistoryRow,
    ForecastingOptionCatalog,
    ForecastingTableMetadata,
)

from ..ml_model_interval_types import PredictionIntervalAdaptiveBin, PredictionIntervalCalibration
from ..ml_model_result_types import CountComparisonRow, EventComparisonRow


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


class TrainingBacktestRowPayload(TypedDict, total=False):
    origin_date: str | None
    horizon_days: int
    date: str
    actual_count: float
    predicted_count: float
    lower_bound: float
    upper_bound: float
    baseline_count: float
    heuristic_count: float
    actual_event: int
    predicted_event_probability: float | None
    baseline_event_probability: float | None
    heuristic_event_probability: float | None


class TrainingCountComparisonPayload(TypedDict, total=False):
    method_key: str
    method_label: str
    role_label: str
    is_selected: bool
    mae: float | None
    rmse: float | None
    smape: float | None
    poisson_deviance: float | None
    mae_delta_vs_baseline: float | None
    rmse_delta_vs_baseline: float | None
    smape_delta_vs_baseline: float | None


class TrainingEventComparisonPayload(TypedDict, total=False):
    method_key: str
    method_label: str
    role_label: str
    brier_score: float | None
    roc_auc: float | None
    f1: float | None
    log_loss: float | None
    is_selected: bool


class TrainingHorizonSummaryPayload(TypedDict, total=False):
    horizon_days: int
    horizon_label: str
    folds: int
    count_mae: float | None
    count_rmse: float | None
    count_smape: float | None
    count_poisson_deviance: float | None
    baseline_count_mae: float | None
    heuristic_count_mae: float | None
    prediction_interval_coverage: float | None
    prediction_interval_coverage_display: str
    prediction_interval_coverage_validated: bool
    prediction_interval_coverage_note: str | None
    prediction_interval_validation_scheme_key: str | None
    prediction_interval_validation_scheme_label: str | None
    prediction_interval_method_label: str | None


class TrainingBacktestOverviewPayload(TypedDict, total=False):
    folds: int
    min_train_rows: int
    validation_horizon_days: int
    validation_horizon_label: str
    forecast_horizon_days: int
    forecast_horizon_label: str
    validated_horizon_days: list[int]
    selection_rule: str
    event_selection_rule: str
    classification_threshold: float
    event_backtest_event_rate: float | None
    event_probability_informative: bool
    event_probability_note: str | None
    event_probability_reason_code: str | None
    candidate_model_labels: list[str]
    candidate_window_count: int
    candidate_covered_window_count_by_model: dict[str, int]
    candidate_window_coverage_by_model: dict[str, float]
    dispersion_ratio: float | None
    prediction_interval_level: float | None
    prediction_interval_level_display: str
    prediction_interval_coverage: float | None
    prediction_interval_coverage_display: str
    prediction_interval_method_label: str
    prediction_interval_coverage_validated: bool
    prediction_interval_coverage_note: str
    prediction_interval_calibration_windows: int
    prediction_interval_evaluation_windows: int
    prediction_interval_validation_scheme_key: str | None
    prediction_interval_validation_scheme_label: str | None
    prediction_interval_validation_explanation: str | None
    prediction_interval_calibration_range_label: str
    prediction_interval_evaluation_range_label: str
    prediction_interval_validated_horizon_days: list[int]
    prediction_interval_coverage_by_horizon: dict[str, float | None]
    prediction_interval_coverage_display_by_horizon: dict[str, str]
    rolling_scheme_label: str


class TrainingEventModelPayload(TypedDict, total=False):
    # one-off: carrier for optional trained classifier artifact
    pass


class TrainingMlResultPayload(TypedDict, total=False):
    is_ready: bool
    message: str
    forecast_rows: list[TrainingForecastRow]
    feature_importance: list[TrainingFeatureImportanceRow]
    feature_importance_source_key: str | None
    feature_importance_source_label: str | None
    feature_importance_note: str | None
    backtest_rows: list[TrainingBacktestRowPayload]
    horizon_summaries: dict[str, TrainingHorizonSummaryPayload]
    count_mae: float | None
    count_rmse: float | None
    count_smape: float | None
    count_poisson_deviance: float | None
    baseline_count_mae: float | None
    baseline_count_rmse: float | None
    baseline_count_smape: float | None
    baseline_count_poisson_deviance: float | None
    heuristic_count_mae: float | None
    heuristic_count_rmse: float | None
    heuristic_count_smape: float | None
    heuristic_count_poisson_deviance: float | None
    count_vs_baseline_delta: float | None
    brier_score: float | None
    baseline_brier_score: float | None
    heuristic_brier_score: float | None
    roc_auc: float | None
    baseline_roc_auc: float | None
    heuristic_roc_auc: float | None
    f1_score: float | None
    baseline_f1_score: float | None
    heuristic_f1_score: float | None
    log_loss: float | None
    baseline_log_loss: float | None
    heuristic_log_loss: float | None
    count_comparison_rows: list[TrainingCountComparisonPayload]
    event_comparison_rows: list[TrainingEventComparisonPayload]
    backtest_overview: TrainingBacktestOverviewPayload
    selected_count_model_key: str
    selected_count_model_reason: str
    selected_count_model_reason_short: str
    candidate_count_model_labels: list[str]
    selected_event_model_key: str | None
    selected_event_model_label: str | None
    top_feature_label: str
    count_model_label: str
    prediction_interval_level: float | None
    prediction_interval_level_display: str
    prediction_interval_coverage: float | None
    prediction_interval_coverage_display: str
    prediction_interval_method_label: str
    event_model_label: str | None
    event_backtest_available: bool
    event_probability_enabled: bool
    event_probability_note: str | None
    event_probability_reason_code: str | None
    temperature_feature_enabled: bool
    temperature_non_null_days: int
    temperature_total_days: int
    temperature_coverage: float
    temperature_note: str | None
    backtest_method_label: str
    classifier_ready: bool


class MlRequestState(TypedDict, total=False):
    """Normalized request state used by ml_model/core cache and shell flow."""

    table_options: list[dict[str, str]]
    selected_table: str
    source_tables: list[str]
    source_table_notes: list[str]
    days_ahead: int
    selected_history_window: str
    scenario_temperature: float | None
    cache_key: tuple[Any, ...]


class MlFilterBundle(TypedDict, total=False):
    """Preloaded metadata and filter options for ML payload assembly."""

    metadata_items: list[ForecastingTableMetadata]
    preload_notes: list[str]
    option_catalog: ForecastingOptionCatalog
    selected_cause: str
    selected_object_category: str


class MlAggregationInputs(MlFilterBundle, total=False):
    """Aggregated daily history and counts after applying ML filters."""

    daily_history: list[ForecastingDailyHistoryRow]
    filtered_records_count: int


class MlPayload(TypedDict, total=False):
    """Top-level ML payload cached and returned by core orchestration."""

    has_data: bool
    notes: list[str]
    feature_importance: list[dict[str, Any]]
    forecast_rows: list[dict[str, Any]]
    filters: dict[str, Any]


class MlContext(TypedDict, total=False):
    generated_at: str
    initial_data: MlPayload
    plotly_js: str
    has_data: bool


class MlBacktestPresentationResult(TypedDict, total=False):
    """ML backtesting payload consumed by presentation_backtesting helpers."""

    is_ready: bool
    backtest_overview: dict[str, Any]
    prediction_interval_method_label: str
    prediction_interval_level_display: str
    prediction_interval_coverage_display: str
    count_comparison_rows: list[CountComparisonRow]
    event_comparison_rows: list[EventComparisonRow]
    candidate_count_model_labels: list[str]
    count_model_label: str
    selected_count_model_key: str
    selected_count_model_reason_short: str
    selected_count_model_reason: str
    top_feature_label: str
    event_backtest_available: bool
    count_mae: float
    count_rmse: float
    count_smape: float
    count_poisson_deviance: float
    baseline_count_mae: float
    baseline_count_rmse: float
    baseline_count_smape: float
    baseline_count_poisson_deviance: float
    heuristic_count_mae: float
    heuristic_count_rmse: float
    heuristic_count_smape: float
    heuristic_count_poisson_deviance: float
    brier_score: float
    baseline_brier_score: float
    heuristic_brier_score: float
    roc_auc: float
    baseline_roc_auc: float
    heuristic_roc_auc: float
    f1_score: float
    baseline_f1_score: float
    heuristic_f1_score: float
    log_loss: float
    baseline_log_loss: float
    heuristic_log_loss: float


class PredictionIntervalDisplayContext(TypedDict):
    level_display: str
    coverage_display: str
    method_label_display: str
    method_label: str
    quality_note: str


class BacktestEventTable(TypedDict, total=False):
    title: str
    rows: list[dict[str, str]]
    empty_message: str
    reason_code: str | None


class ModelChoiceSection(TypedDict, total=False):
    title: str
    lead: str
    body: str
    facts: list[dict[str, str]]


class BacktestQualityAssessment(TypedDict, total=False):
    ready: bool
    title: str
    subtitle: str
    methodology_items: list[dict[str, str]]
    interval_card: dict[str, str]
    metric_cards: list[dict[str, str]]
    event_metric_cards: list[dict[str, str]]
    model_choice: ModelChoiceSection
    count_table: dict[str, Any]
    event_table: BacktestEventTable
    event_probability_reason_code: str | None
    dissertation_points: list[str]


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
