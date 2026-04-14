from __future__ import annotations

from typing import Any, Callable, TypedDict


class TableOption(TypedDict, total=False):
    value: str
    label: str


class ForecastingFilters(TypedDict, total=False):
    table_name: str
    district: str
    cause: str
    object_category: str
    temperature: str
    forecast_days: str
    history_window: str
    available_tables: list[TableOption]
    available_districts: list[dict[str, Any]]
    available_causes: list[dict[str, Any]]
    available_object_categories: list[dict[str, Any]]
    available_forecast_days: list[dict[str, Any]]
    available_history_windows: list[dict[str, Any]]


class ForecastingSummary(TypedDict, total=False):
    selected_table_label: str
    history_window_label: str
    slice_label: str
    forecast_days_display: str
    fires_count: int
    fires_count_display: str


class ForecastingRiskPrediction(TypedDict, total=False):
    geo_prediction: dict[str, Any]
    feature_cards: list[dict[str, Any]]
    notes: list[str]


class ForecastingExecutiveBrief(TypedDict, total=False):
    notes: list[str]
    export_text: str
    export_excerpt: str


class ForecastingPayload(TypedDict, total=False):
    generated_at: str
    has_data: bool
    bootstrap_mode: str
    loading: bool
    deferred: bool
    metadata_pending: bool
    metadata_ready: bool
    metadata_error: bool
    metadata_status_message: str
    base_forecast_pending: bool
    base_forecast_ready: bool
    loading_status_message: str
    decision_support_pending: bool
    decision_support_ready: bool
    decision_support_error: bool
    decision_support_status_message: str
    model_description: str
    summary: ForecastingSummary
    quality_assessment: dict[str, Any]
    features: list[dict[str, Any]]
    risk_prediction: ForecastingRiskPrediction
    executive_brief: ForecastingExecutiveBrief
    insights: list[dict[str, Any]]
    charts: dict[str, Any]
    forecast_rows: list[dict[str, Any]]
    notes: list[str]
    filters: ForecastingFilters


class ForecastingRequestState(TypedDict, total=False):
    table_options: list[TableOption]
    selected_table: str
    source_tables: list[str]
    source_table_notes: list[str]
    days_ahead: int
    history_window: str
    cache_key: tuple[Any, ...]


class ForecastingContext(TypedDict):
    generated_at: str
    initial_data: ForecastingPayload
    plotly_js: str
    has_data: bool


class ForecastingDailyHistoryRow(TypedDict, total=False):
    date: Any
    count: float
    temperature: float | None


class ForecastingForecastRow(TypedDict, total=False):
    date: str
    date_display: str
    forecast_value: float
    lower_bound: float
    upper_bound: float
    fire_probability: float
    fire_probability_display: str
    usual_fire_probability: float
    weekday_label: str
    scenario_hint: str


class ForecastingWeekdayProfileRow(TypedDict, total=False):
    label: str
    avg_value: float
    avg_display: str


class ForecastingGeoPoint(TypedDict, total=False):
    latitude: float
    longitude: float
    short_label: str
    risk_score: float
    risk_display: str
    incidents_display: str
    last_fire_display: str
    dominant_cause: str
    dominant_object_category: str
    marker_size: float


class ForecastingGeoPrediction(TypedDict, total=False):
    has_coordinates: bool
    points: list[ForecastingGeoPoint]


class ForecastingTableMetadata(TypedDict, total=False):
    table_name: str
    columns: list[str]
    resolved_columns: dict[str, str]


class ForecastingInputRecord(TypedDict, total=False):
    date: Any
    district: str
    cause: str
    object_category: str
    count: float
    temperature: float | None


class ForecastingOptionCatalog(TypedDict, total=False):
    districts: list[dict[str, str]]
    causes: list[dict[str, str]]
    object_categories: list[dict[str, str]]


class ForecastingFeatureCard(TypedDict, total=False):
    label: str
    status: str
    status_label: str
    source: str
    description: str
    quality_status: str | None
    quality_label: str | None
    coverage_display: str | None
    usable: bool | None


class ForecastingInsightCard(TypedDict, total=False):
    label: str
    value: str
    meta: str
    tone: str


class ForecastingQualityAssessment(TypedDict, total=False):
    title: str
    subtitle: str
    metric_cards: list[dict[str, Any]]
    methodology_items: list[dict[str, Any]]
    comparison_rows: list[dict[str, Any]]
    dissertation_points: list[str]


class ForecastingCharts(TypedDict, total=False):
    daily: dict[str, Any]
    breakdown: dict[str, Any]
    weekday: dict[str, Any]
    geo: dict[str, Any]


class ForecastingBaseArtifacts(TypedDict, total=False):
    quality_assessment: ForecastingQualityAssessment
    forecast_rows: list[ForecastingForecastRow]
    weekday_profile: list[ForecastingWeekdayProfileRow]
    charts: ForecastingCharts


class ForecastingBasePresentation(TypedDict, total=False):
    generated_at: str
    notes: list[str]
    features: list[ForecastingFeatureCard]
    insights: list[ForecastingInsightCard]
    summary: ForecastingSummary
    executive_brief: ForecastingExecutiveBrief
    filters: ForecastingFilters


class ForecastingDeps(TypedDict, total=False):
    """Dependency container with forecasting service callbacks."""

    format_datetime: Callable[..., str]
    build_slice_label: Callable[..., str]
    history_window_label: Callable[..., str]
    build_shell_risk_prediction: Callable[..., ForecastingRiskPrediction]
    build_pending_executive_brief: Callable[..., ForecastingExecutiveBrief]
    build_decision_support_followup_message: Callable[..., str]
    format_float_for_input: Callable[..., str]
    build_forecast_chart: Callable[..., dict[str, Any]]
    build_forecast_breakdown_chart: Callable[..., dict[str, Any]]
    build_weekday_chart: Callable[..., dict[str, Any]]
    build_notes: Callable[..., list[str]]
    build_executive_brief_from_risk_payload: Callable[..., ForecastingExecutiveBrief]
    compose_executive_brief_text: Callable[..., str]
    build_summary: Callable[..., ForecastingSummary]
    build_insights: Callable[..., list[ForecastingInsightCard]]
    build_geo_chart: Callable[..., dict[str, Any]]
    run_scenario_backtesting: Callable[..., dict[str, Any]]
    build_scenario_quality_assessment: Callable[..., ForecastingQualityAssessment]
    build_forecast_rows: Callable[..., list[ForecastingForecastRow]]
    build_weekday_profile: Callable[..., list[ForecastingWeekdayProfileRow]]
    build_decision_support_payload: Callable[..., ForecastingRiskPrediction]
    build_pending_decision_support_payload: Callable[..., ForecastingRiskPrediction]
    selected_source_tables: Callable[..., list[str]]
    emit_forecasting_progress: Callable[..., None]
    scenario_forecast_description: str
    forecast_day_options: list[int]
    history_window_options: list[dict[str, str]]


class ForecastInput(TypedDict, total=False):
    """Input bundle for forecasting payload assembly."""

    table_options: list[TableOption]
    selected_table: str
    source_tables: list[str]
    source_table_notes: list[str]
    district: str
    cause: str
    object_category: str
    temperature: str
    temperature_value: float | None
    days_ahead: int
    selected_history_window: str
    include_decision_support: bool


class ForecastPayload(ForecastingPayload, total=False):
    """Final forecasting payload served to the UI."""

    pass


class SqlFilters(TypedDict, total=False):
    """SQL scope/filter parameters passed into query builders."""

    district: str
    cause: str
    object_category: str
    min_year: int | None
    history_window: str


class SqlRow(TypedDict, total=False):
    """One normalized daily history row returned by SQL layer."""

    date: Any
    count: float
    temperature: float | None


class SqlMaterializedRow(SqlRow, total=False):
    avg_temperature: float | None
    temperature_samples: int


class SqlMergedBucket(TypedDict, total=False):
    count: int
    temperature_sum: float
    temperature_samples: int


class ForecastingMetadataInputs(TypedDict, total=False):
    metadata_items: list[ForecastingTableMetadata]
    preload_notes: list[str]
    feature_cards: list[dict[str, Any]]
    option_catalog: ForecastingOptionCatalog
    selected_district: str
    selected_cause: str
    selected_object_category: str


class ForecastingBaseInputs(ForecastingMetadataInputs, total=False):
    daily_history: list[ForecastingDailyHistoryRow]
    filtered_records_count: int


class ForecastingJobStatus(TypedDict, total=False):
    job_id: str
    status: str
    message: str
    payload: ForecastingPayload


class JobStageMeta(TypedDict, total=False):
    stage_index: int
    stage_label: str


class JobMetaPayload(TypedDict, total=False):
    stage_index: int
    stage_label: str
    stage_message: str
    cache_key: str
    cache_hit: bool
    params: dict[str, Any]


class JobSnapshot(TypedDict, total=False):
    job_id: str
    kind: str
    status: str
    logs: list[str]
    result: Any
    error_message: str
    meta: JobMetaPayload


class JobStatusPayload(TypedDict, total=False):
    job_id: str
    kind: str
    status: str
    logs: list[str]
    result: Any
    error_message: str
    is_final: bool
    meta: JobMetaPayload
    reused: bool


class ForecastingTemperatureQuality(TypedDict, total=False):
    usable: bool
    non_null_days: int
    total_days: int
    coverage: float
    quality_key: str
    quality_label: str
    note: str
