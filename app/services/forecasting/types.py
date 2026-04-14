from __future__ import annotations

from typing import Any, TypedDict


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
