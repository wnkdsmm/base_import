from __future__ import annotations

from typing import Any, TypedDict


class TerritoryBucket(TypedDict, total=False):
    """Aggregated incident statistics for one territory used by risk scoring."""

    label: str
    incidents: int
    weighted_history: float
    seasonal_month_sum: float
    seasonal_weekday_sum: float
    severe: int
    victims: int
    major_damage: int
    heating_incidents: int
    night_incidents: int
    risk_score_sum: float
    risk_score_count: int
    response_sum: float
    response_count: int
    distance_sum: float
    distance_count: int
    long_arrivals: int
    water_available: int
    water_known: int
    last_fire: Any
    object_categories: Any
    settlement_types: Any


class RiskEventRecord(TypedDict, total=False):
    """Raw incident record used for historical validation windows."""

    date: Any
    territory_label: str
    district: str


class HorizonContext(TypedDict, total=False):
    """Scoring horizon and seasonal context values."""

    history_days: int
    horizon_days: int
    recent_window_days: int
    future_heating_share: float


class TerritoryIdentity(TypedDict, total=False):
    """Identity attributes inferred from territory labels and categories."""

    dominant_object_category: str
    dominant_settlement_type: str
    is_rural: bool
    settlement_context_label: str


class LogisticsProfile(TypedDict, total=False):
    """Explainable logistics profile from the shared logistics service."""

    travel_time_pressure: float
    service_coverage_gap: float
    service_zone_pressure: float
    travel_time_minutes: float
    travel_time_display: str
    travel_time_source: str
    service_coverage_ratio: float
    service_coverage_display: str
    fire_station_coverage_label: str
    service_zone_label: str
    service_zone_tone: str
    service_zone_reason: str
    logistics_priority_score: float
    logistics_priority_display: str
    logistics_priority_label: str


class RiskFactors(TypedDict, total=False):
    """Normalized risk factors derived from historical territory stats."""

    incidents: int
    history_pressure: float
    recency_pressure: float
    seasonal_alignment: float
    fire_probability: float
    severe_rate: float
    victims_rate: float
    damage_rate: float
    heating_share: float
    heating_pressure: float
    night_share: float
    risk_factor: float
    casualty_pressure: float
    damage_pressure: float
    severe_probability: float


class LogisticsFactors(TypedDict, total=False):
    """Logistics-derived factors used by scoring and presentation."""

    avg_response: float | None
    avg_distance: float | None
    distance_score: float
    long_arrival_rate: float
    response_pressure: float
    logistics_profile: LogisticsProfile
    arrival_probability: float


class WaterFactors(TypedDict, total=False):
    """Water-availability factors used in the final risk score."""

    water_gap_rate: float
    tanker_dependency: float
    water_deficit_probability: float


class ScoreContext(TypedDict, total=False):
    """Component-explanation context shared across scoring helpers."""

    incidents: int
    history_pressure: float
    recency_pressure: float
    seasonal_alignment: float
    fire_probability: float
    severe_rate: float
    victims_rate: float
    damage_rate: float
    risk_factor: float
    avg_response: float | None
    avg_distance: float | None
    long_arrival_rate: float
    travel_time_minutes: float
    travel_time_source: str
    service_coverage_ratio: float
    service_coverage_display: str
    service_zone_label: str
    service_zone_reason: str
    logistics_priority_score: float
    logistics_priority_label: str
    night_share: float
    water_gap_rate: float
    water_known: int
    water_available: int
    tanker_dependency: float
    is_rural: bool
    settlement_context_label: str
    heating_share: float


class ComponentSignal(TypedDict, total=False):
    """One weighted signal inside a component score."""

    key: str
    label: str
    value: float
    value_display: str
    weight: float
    weight_display: str
    weighted_value: float


class ComponentScore(TypedDict, total=False):
    """Scored risk component with rationale and weighted contribution."""

    key: str
    label: str
    description: str
    score: float
    score_display: str
    weight: float
    weight_display: str
    contribution: float
    contribution_display: str
    tone: str
    signals: list[ComponentSignal]
    bar_width: str
    summary: str
    rationale: str
    driver_text: str


class ComponentScoreMapEntry(TypedDict, total=False):
    """Compact numeric projection of one component score."""

    score: float
    contribution: float
    weight: float


class CalibrationMetrics(TypedDict, total=False):
    """Ranking metrics used to compare profile variants."""

    top1_hit_rate: float
    topk_capture_rate: float
    precision_at_k: float
    ndcg_at_k: float
    k_value: int
    windows_count: int


class CalibrationComparison(TypedDict, total=False):
    """Delta between selected and expert calibration metrics."""

    top1_hit_delta: float
    topk_capture_delta: float
    precision_at_k_delta: float
    ndcg_at_k_delta: float
    summary: str


class WeightCandidate(TypedDict, total=False):
    """Alternative component-weight configuration tested in calibration."""

    key: str
    label: str
    weights: dict[str, float]


class CalibrationEntry(TypedDict, total=False):
    """Evaluated weight candidate with objective and aggregate metrics."""

    key: str
    label: str
    weights: dict[str, float]
    evaluation: dict[str, Any]
    aggregate: CalibrationMetrics
    objective: float
    regularized_objective: float


class RiskProfile(TypedDict, total=False):
    """Resolved risk-weight profile shared by scoring and presentation layers."""

    mode: str
    mode_label: str
    status_label: str
    status_tone: str
    description: str
    notes: list[str]
    component_weights: dict[str, float]
    expert_component_weights: dict[str, float]
    rural_weight_shift: dict[str, Any]
    components: dict[str, Any]
    component_order: list[str]
    thresholds: dict[str, Any]
    defaults: dict[str, Any]
    calibration: dict[str, Any]


class RiskScore(TypedDict, total=False):
    """Territory-level scoring result produced by scoring_compute."""

    label: str
    risk_score: float
    risk_display: str
    risk_formula_display: str
    risk_class_label: str
    risk_tone: str
    priority_label: str
    priority_tone: str
    weight_mode: str
    weight_mode_label: str
    component_scores: list[ComponentScore]
    component_score_map: dict[str, ComponentScoreMapEntry]
    fire_probability: float
    severe_probability: float
    arrival_probability: float
    water_deficit_probability: float
    fire_probability_display: str
    severe_probability_display: str
    arrival_probability_display: str
    water_deficit_display: str
    history_count: int
    history_count_display: str
    last_fire_display: str
    avg_response_minutes: float | None
    response_time_display: str
    avg_distance_km: float | None
    distance_display: str
    travel_time_minutes: float
    travel_time_display: str
    travel_time_source: str
    fire_station_coverage_display: str
    fire_station_coverage_label: str
    service_zone_label: str
    service_zone_tone: str
    service_zone_reason: str
    logistics_priority_score: float
    logistics_priority_display: str
    logistics_priority_label: str
    water_availability_share: float | None
    water_supply_display: str
    dominant_object_category: str
    dominant_settlement_type: str
    settlement_context_label: str
    is_rural: bool
    drivers_display: str
    action_label: str
    action_hint: str
    recommendations: list[str]
    explanation: str
    bar_width: str
    history_pressure: float


class FeatureSource(TypedDict, total=False):
    """Table/column source metadata used for feature cards."""

    table_name: str
    columns: list[str]


class FeatureCard(TypedDict, total=False):
    """Feature availability card shown in decision support UI."""

    label: str
    description: str
    status: str
    status_label: str
    source: str


class QualityPassport(TypedDict, total=False):
    """Quality summary for data readiness and critical gaps."""

    title: str
    confidence_score: int
    confidence_score_display: str
    confidence_label: str
    confidence_tone: str
    validation_label: str
    validation_summary: str
    table_count_display: str
    used_count_display: str
    partial_count_display: str
    missing_count_display: str
    critical_gaps: list[str]
    used_labels: list[str]
    partial_labels: list[str]
    missing_labels: list[str]
    reliability_notes: list[str]


class GeoItem(TypedDict, total=False):
    """Compact geo-row for hotspots/districts in UI summary blocks."""

    label: str
    risk_display: str
    meta: str


class GeoSummary(TypedDict, total=False):
    """Geo prediction summary and compact map-readiness signals."""

    has_coordinates: bool
    has_map_points: bool
    compact_message: str
    model_description: str
    coverage_display: str
    top_zone_label: str
    top_risk_display: str
    hotspots_count_display: str
    top_explanation: str
    hotspots: list[GeoItem]
    districts: list[GeoItem]


class TopConfidence(TypedDict, total=False):
    """Confidence payload for top-territory explanations."""

    label: str
    score_display: str
    tone: str
    note: str


class ValidationMetricCard(TypedDict, total=False):
    """One metric card in historical ranking validation output."""

    label: str
    value: str
    meta: str


class ValidationWindowSummaryCard(TypedDict, total=False):
    """Compact per-window validation summary for UI preview."""

    label: str
    risk_display: str
    meta: str


class ValidationMetricsRaw(TypedDict, total=False):
    """Aggregated ranking metrics over historical windows."""

    windows_count: int
    horizon_days: int
    k_value: int
    top1_hit_rate: float
    topk_capture_rate: float
    precision_at_k: float
    ndcg_at_k: float
    objective_score: float


class ValidationRankedRow(TypedDict, total=False):
    """Minimal reranked row used inside validation windows."""

    label: str
    risk_score: float
    history_pressure: float


class ValidationWindowMetrics(TypedDict, total=False):
    """Evaluation metrics for one historical validation window."""

    cutoff: Any
    top_label: str
    future_incidents: int
    top1_hit: float
    topk_capture: float
    precision_at_k: float
    ndcg_at_k: float
    summary_card: ValidationWindowSummaryCard


class ValidationEvaluation(TypedDict, total=False):
    """Result of profile evaluation across historical windows."""

    has_metrics: bool
    window_metrics: list[ValidationWindowMetrics]
    aggregate: ValidationMetricsRaw
    skipped_no_rows: int


class HistoricalWindow(TypedDict, total=False):
    """Train/future split describing one rolling validation window."""

    cutoff: Any
    future_end: Any
    horizon_days: int
    train_records: list[dict[str, Any]]
    future_records: list[dict[str, Any]]
    predicted_rows: list[ValidationRankedRow]


class HistoricalWindowsBundle(TypedDict, total=False):
    """Historical windows bundle with readiness metadata."""

    is_ready: bool
    reason: str
    history_days: int
    horizon_days: int
    min_training_days: int
    windows: list[HistoricalWindow]
    skipped_no_future: int


class HistoricalValidationPayload(TypedDict, total=False):
    """Historical validation block attached to decision-support payload."""

    title: str
    mode_label: str
    has_metrics: bool
    status_label: str
    status_tone: str
    summary: str
    metric_cards: list[ValidationMetricCard]
    notes: list[str]
    recent_windows: list[ValidationWindowSummaryCard]
    metrics_raw: ValidationMetricsRaw


class GeoPredictionData(TypedDict, total=False):
    """Raw geo prediction payload before compact geo-summary transform."""

    has_coordinates: bool
    points: list[dict[str, Any]]
    hotspots: list[dict[str, Any]]
    districts: list[dict[str, Any]]
    model_description: str
    coverage_display: str
    top_zone_label: str
    top_risk_display: str
    hotspots_count_display: str
    top_explanation: str


class RiskPresentation(TypedDict, total=False):
    """Final decision-support payload rendered by presentation layer."""

    has_data: bool
    title: str
    model_description: str
    coverage_display: str
    quality_passport: QualityPassport
    summary_cards: list[dict[str, Any]]
    top_territory_label: str
    top_territory_explanation: str
    top_territory_confidence_label: str
    top_territory_confidence_score_display: str
    top_territory_confidence_tone: str
    top_territory_confidence_note: str
    territories: list[RiskScore]
    feature_cards: list[FeatureCard]
    weight_profile: RiskProfile
    historical_validation: HistoricalValidationPayload
    notes: list[str]
    geo_summary: GeoSummary
    geo_prediction: GeoPredictionData
