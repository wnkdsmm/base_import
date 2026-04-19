from __future__ import annotations

from typing import Any, NotRequired, TypedDict


class ClusterMetrics(TypedDict):
    """Core clustering quality metrics and cluster shape counters."""

    silhouette: NotRequired[float | None]
    davies_bouldin: NotRequired[float | None]
    calinski_harabasz: NotRequired[float | None]
    cluster_balance_ratio: NotRequired[float | None]
    smallest_cluster_size: NotRequired[int | None]
    largest_cluster_size: NotRequired[int | None]
    stability_ari: NotRequired[float | None]
    initialization_ari: NotRequired[float | None]
    explained_variance: NotRequired[float | None]
    has_microclusters: NotRequired[bool | None]
    microcluster_threshold: NotRequired[int | None]


class QualityScore(TypedDict):
    """Single UI card/row item with value and optional explanation."""

    label: str
    value: str
    meta: NotRequired[str]


class ClusterLabel(TypedDict):
    """Human-readable clustering quality label and explanatory note."""

    label: str
    note: str


class ClusterMethod(TypedDict):
    """Method-level row used in quality comparison and selection logic."""

    method_key: str
    method_label: str
    algorithm_key: str
    silhouette: NotRequired[float | None]
    davies_bouldin: NotRequired[float | None]
    calinski_harabasz: NotRequired[float | None]
    cluster_balance_ratio: NotRequired[float | None]
    is_selected: NotRequired[bool]
    is_recommended: NotRequired[bool]
    cluster_count: NotRequired[int]


class MethodComparisonRow(TypedDict):
    """Prepared method comparison row for rendering in quality table."""

    method_label: str
    selection_label: str
    silhouette_display: str
    davies_display: str
    calinski_display: str
    balance_display: str


class FeatureAblationRow(TypedDict):
    """Ablation delta row for feature-inclusion quality diagnostics."""

    feature: str
    direction: str
    delta_score: NotRequired[float | None]


class FeatureSelectionReport(TypedDict):
    """Feature-selection metadata used in clustering quality narrative."""

    volume_role_label: NotRequired[str]
    volume_note: NotRequired[str]
    weighting_label: NotRequired[str]
    weighting_note: NotRequired[str]
    weighting_meta: NotRequired[str]
    ablation_rows: NotRequired[list[FeatureAblationRow]]


class QualityDiagnostics(TypedDict):
    """Diagnostics payload with best method/cluster recommendations."""

    best_configuration: NotRequired[ClusterMethod]
    best_quality_k: NotRequired[int | None]
    best_silhouette_k: NotRequired[int | None]
    best_gap_k: NotRequired[int | None]


class QualityConfigurationContext(TypedDict):
    """Resolved selected/recommended clustering configuration context."""

    recommended_k: int | None
    best_silhouette_k: int | None
    selected_method: ClusterMethod | None
    working_configuration: ClusterMethod
    effective_recommended_configuration: ClusterMethod
    recommended_method: ClusterMethod
    working_config_label: str
    recommended_config_label: str


class QualityLabelContext(TypedDict):
    """Resolved textual labels for mode/weighting/ablation messaging."""

    mode_label: str
    mode_note: str
    weighting_label: str
    weighting_note: str
    weighting_meta: str
    ablation_rows: list[FeatureAblationRow]


class QualityNoteContext(TypedDict):
    """Narrative pieces that feed clustering quality dissertation points."""

    segmentation_summary: ClusterLabel
    stability_note: str
    method_note: str
    comparison_scope_note: str
    cluster_shape_note: str
    label_context: QualityLabelContext
    ablation_note: str


class ClusterCountGuidanceContext(TypedDict):
    """Normalized context for cluster-count recommendation decisions."""

    recommended_k: int | None
    best_silhouette_k: int | None
    best_gap_k: int | None
    requested_cluster_count: int
    adjusted_requested_cluster_count: int
    current_cluster_count: int
    request_adjusted: bool
    recommendation_gap: bool
    has_recommended_k: bool
    auto_switched_to_recommended: bool


class ClusterCountRecommendationMessages(TypedDict):
    """User-facing recommendation and explanation texts for cluster count."""

    suggested_label: str
    suggested_note: str
    current_note: str
    quality_note: str
    notes_message: str
    model_note: str


class ClusterCountGuidance(TypedDict):
    """Final cluster-count guidance payload used by quality assessment."""

    recommended_cluster_count: int | None
    best_silhouette_k: int | None
    best_gap_k: int | None
    has_recommendation_gap: bool
    request_adjusted: bool
    suggested_label: str
    suggested_note: str
    current_note: str
    quality_note: str
    notes_message: str
    model_note: str


class ClusteringQualityAssessment(TypedDict):
    """Final clustering quality assessment payload for API/template layer."""

    ready: bool
    title: str
    subtitle: str
    metric_cards: list[QualityScore]
    methodology_items: list[QualityScore]
    comparison_rows: list[MethodComparisonRow]
    dissertation_points: list[str]


class SupportSummary(TypedDict):
    """Support coverage summary with share of low-support territories."""

    low_support_share: NotRequired[float | None]


class ClusteringFilters(TypedDict):
    """Request filters and selectable options for clustering page payloads."""

    table_name: NotRequired[str]
    cluster_count: NotRequired[str]
    sample_limit: NotRequired[str]
    sampling_strategy: NotRequired[str]
    feature_columns: NotRequired[list[str]]
    available_tables: NotRequired[list[dict[str, str]]]
    available_cluster_counts: NotRequired[list[dict[str, str]]]
    available_sample_limits: NotRequired[list[dict[str, str]]]
    available_sampling_strategies: NotRequired[list[dict[str, str]]]
    available_features: NotRequired[list[dict[str, Any]]]


class ClusteringSummary(TypedDict):
    """Top-level clustering summary counters and labels shown in hero/cards."""

    selected_table_label: NotRequired[str]
    total_incidents_display: NotRequired[str]
    total_entities_display: NotRequired[str]
    sampled_entities_display: NotRequired[str]
    clustered_entities_display: NotRequired[str]
    excluded_entities_display: NotRequired[str]
    candidate_features_display: NotRequired[str]
    selected_features_display: NotRequired[str]
    cluster_count_display: NotRequired[str]
    cluster_count_requested_display: NotRequired[str]
    cluster_count_note: NotRequired[str]
    suggested_cluster_count_label: NotRequired[str]
    suggested_cluster_count_display: NotRequired[str]
    suggested_cluster_count_note: NotRequired[str]
    elbow_cluster_count_display: NotRequired[str]
    silhouette_display: NotRequired[str]
    pca_variance_display: NotRequired[str]
    inertia_display: NotRequired[str]
    sampling_strategy_label: NotRequired[str]


class DiagnosticsRow(TypedDict):
    """Diagnostics row for cluster-count quality chart and table views."""

    cluster_count: int
    silhouette: NotRequired[float | None]
    davies_bouldin: NotRequired[float | None]
    calinski_harabasz: NotRequired[float | None]
    cluster_balance_ratio: NotRequired[float | None]


class ClusteringDiagnostics(QualityDiagnostics):
    """Diagnostics payload enriched with chart rows and elbow recommendation."""

    rows: NotRequired[list[DiagnosticsRow]]
    elbow_k: NotRequired[int | None]


class ClusteringModelOutput(TypedDict):
    """Model output used by result assembly and quality narrative builders."""

    silhouette: NotRequired[float | None]
    explained_variance: NotRequired[float | None]
    inertia: NotRequired[float | None]
    pca_points: NotRequired[Any]
    pca_projection: NotRequired[dict[str, Any]]
    stability_ari: NotRequired[float | None]


class ClusteringDataset(TypedDict):
    """Dataset-level counters and support summary after preprocessing stage."""

    total_incidents: int
    total_entities: int
    sampled_entities: int
    support_summary: NotRequired[SupportSummary | None]


class ClusteringModelInputs(TypedDict):
    """Prepared inputs that feed clustering model stage."""

    requested_working_cluster_count: int


class ClusteringModelBundle(TypedDict):
    """Combined model stage outputs used by core_results and rendering."""

    actual_cluster_count: int
    diagnostics: ClusteringDiagnostics
    clustering: ClusteringModelOutput
    runtime_feature_context: FeatureSelectionReport


class ClusteringCharts(TypedDict):
    """Chart bundle rendered in clustering payload."""

    scatter: dict[str, Any]
    radar_chart: dict[str, Any]
    feature_importance_chart: dict[str, Any]
    distribution: dict[str, Any]
    diagnostics: dict[str, Any]


class ClusteringPayload(TypedDict):
    """Top-level clustering payload returned to API/template layer."""

    has_data: NotRequired[bool]
    model_description: NotRequired[str]
    summary: NotRequired[ClusteringSummary]
    quality_assessment: NotRequired[ClusteringQualityAssessment]
    cluster_profiles: NotRequired[list[dict[str, Any]]]
    centroid_columns: NotRequired[list[dict[str, Any]]]
    centroid_rows: NotRequired[list[dict[str, Any]]]
    representative_columns: NotRequired[list[dict[str, Any]]]
    representative_rows: NotRequired[list[dict[str, Any]]]
    cluster_risk: NotRequired[list[dict[str, Any]]]
    pca_projection: NotRequired[dict[str, Any]]
    charts: NotRequired[ClusteringCharts]
    notes: NotRequired[list[str]]
    filters: NotRequired[ClusteringFilters]


class ClusteringTableOption(TypedDict):
    """Selectable table option for clustering page filters."""

    value: str
    label: str


class CandidateFeatureOption(TypedDict):
    """Candidate feature row with quality metadata for selection UI."""

    name: str
    description: str
    coverage: float
    coverage_display: str
    variance: float
    variance_display: str
    is_default: bool
    is_selected: bool
    score: NotRequired[float]


class ClusteringBaseState(TypedDict):
    """Mutable base payload scaffold used during clustering pipeline stages."""

    has_data: NotRequired[bool]
    model_description: NotRequired[str]
    summary: NotRequired[ClusteringSummary]
    quality_assessment: NotRequired[ClusteringQualityAssessment]
    cluster_profiles: NotRequired[list[dict[str, Any]]]
    centroid_columns: NotRequired[list[dict[str, Any]]]
    centroid_rows: NotRequired[list[dict[str, Any]]]
    representative_columns: NotRequired[list[dict[str, Any]]]
    representative_rows: NotRequired[list[dict[str, Any]]]
    cluster_risk: NotRequired[list[dict[str, Any]]]
    charts: NotRequired[ClusteringCharts]
    notes: NotRequired[list[str]]
    filters: NotRequired[ClusteringFilters]


class TerritoryRecord(TypedDict):
    """Incident-level record used to aggregate territory-level features."""

    territory_label: str
    district: NotRequired[str]
    settlement_type: NotRequired[str]
    fire_area: NotRequired[float | None]
    response_minutes: NotRequired[float | None]
    long_arrival: NotRequired[bool]
    severe_consequence: NotRequired[bool]
    night_incident: NotRequired[bool]
    heating_season: NotRequired[bool]
    has_water_supply: NotRequired[bool | None]
    fire_station_distance: NotRequired[float | None]


class TerritoryBucket(TypedDict):
    """Mutable territory aggregate bucket with counters and sums."""

    label: str
    incidents: int
    districts: Any
    settlement_types: Any
    area_sum: float
    area_count: int
    night_incidents: int
    response_sum: float
    response_count: int
    long_arrivals: int
    severe: int
    water_known: int
    water_available: int
    distance_sum: float
    distance_count: int
    heating_incidents: int


class TerritorySupportSummary(TypedDict):
    """Support-level summary metrics across aggregated territories."""

    territory_count: float
    low_support_share: float
    single_incident_share: float
    median_incidents: float


class TerritoryGlobalStats(TypedDict):
    """Global prior means/rates used for territory feature shrinkage."""

    area_mean: NotRequired[float | None]
    response_mean: NotRequired[float | None]
    distance_mean: NotRequired[float | None]
    night_rate: NotRequired[float | None]
    severe_rate: NotRequired[float | None]
    heating_rate: NotRequired[float | None]
    response_coverage_rate: NotRequired[float | None]
    water_coverage_rate: NotRequired[float | None]
    long_arrival_rate: NotRequired[float | None]
    no_water_rate: NotRequired[float | None]


class ClusteringDatasetBundle(TypedDict):
    """Loaded/aggregated dataset bundle for clustering pipeline stages."""

    entity_frame: Any
    feature_frame: Any
    candidate_features: list[CandidateFeatureOption]
    total_incidents: int
    total_entities: int
    sampled_entities: int
    support_summary: TerritorySupportSummary
    sampling_note: str
    notes: list[str]


class ClusteringFeatureSelectionContext(TypedDict):
    """Prepared feature-selection context used before model training stage."""

    summary: ClusteringSummary
    candidate_features: list[CandidateFeatureOption]
    selected_features: list[str]
    feature_selection_report: FeatureSelectionReport
    selection_note: str


class ClusteringModelStageInputs(TypedDict):
    """Prepared model-stage inputs with filtered frames and cluster counts."""

    cluster_frame: Any
    entity_frame: Any
    excluded_entities: int
    requested_working_cluster_count: int
    actual_cluster_count: int


class ClusteringMethodRow(TypedDict):
    """Single method-comparison row with metrics and selection flags."""

    method_key: str
    algorithm_key: str
    method_label: str
    is_selected: bool
    is_recommended: bool
    weighting_strategy: str
    cluster_count: NotRequired[int]
    inertia: float | None
    silhouette: float | None
    davies_bouldin: float | None
    calinski_harabasz: float | None
    cluster_balance_ratio: float | None
    smallest_cluster_size: int | None
    largest_cluster_size: int | None
    quality_score: float
    shape_penalty: float
    has_microclusters: bool
    has_balance_warning: bool
    best_quality_k: NotRequired[int | None]
    best_silhouette_k: NotRequired[int | None]


class ClusteringMethodCandidate(TypedDict):
    """Candidate method definition used before metric evaluation."""

    method_key: str
    algorithm_key: str
    method_label: str
    weighting_strategy: str


class ClusteringDiagnosticsResult(TypedDict):
    """Diagnostics summary across cluster-count sweep and method comparisons."""

    rows: list[ClusteringMethodRow]
    method_rows_by_cluster_count: NotRequired[dict[int, list[ClusteringMethodRow]]]
    best_silhouette_k: int | None
    best_quality_k: int | None
    best_gap_k: int | None
    best_configuration: NotRequired[ClusteringMethodRow | None]
    elbow_k: int | None


class ClusteringRunResult(TypedDict):
    """Single-run clustering result with labels, centers, and metrics."""

    labels: Any
    scaled_points: Any
    scaled_centers: Any
    raw_centers: Any
    silhouette: float | None
    davies_bouldin: float | None
    calinski_harabasz: float | None
    cluster_balance_ratio: float | None
    smallest_cluster_size: int | None
    largest_cluster_size: int | None
    quality_score: float
    shape_penalty: float
    has_microclusters: bool
    has_balance_warning: bool
    microcluster_threshold: int | None
    stability_ari: float | None
    initialization_ari: float | None
    inertia: float | None
    pca_points: Any
    pca_projection: dict[str, Any]
    explained_variance: float | None
    algorithm_key: str
    method_key: str
    weighting_strategy: str


class ClusteringRuntimeBundle(TypedDict):
    """Runtime bundle produced after one clustering run for selected config."""

    runtime_feature_context: NotRequired[FeatureSelectionReport]
    clustering: NotRequired[ClusteringRunResult]
    pca_projection: NotRequired[dict[str, Any]]
    method_comparison: NotRequired[list[ClusteringMethodRow]]
    labels: NotRequired[Any]
    cluster_labels: NotRequired[list[str]]
    profiles: NotRequired[list[dict[str, Any]]]
    centroid_columns: NotRequired[list[dict[str, Any]]]
    centroid_rows: NotRequired[list[dict[str, Any]]]
    representative_columns: NotRequired[list[dict[str, Any]]]
    representative_rows: NotRequired[list[dict[str, Any]]]


class ClusteringDiagnosticsRuntimeBundle(ClusteringRuntimeBundle):
    """Runtime bundle enriched with diagnostics and resolved render config."""

    diagnostics: NotRequired[ClusteringDiagnosticsResult]
    render_configuration: NotRequired[ClusteringMethodRow]
    actual_cluster_count: NotRequired[int]
    actual_method_key: NotRequired[str]
    actual_algorithm_key: NotRequired[str]
    actual_weighting_strategy: NotRequired[str]
    method_comparison_reused: NotRequired[bool]


class ClusteringLoadStageResult(TypedDict):
    """Result of dataset/feature loading stage or early error payload."""

    dataset: NotRequired[ClusteringDatasetBundle]
    feature_selection: NotRequired[ClusteringFeatureSelectionContext]
    error_payload: NotRequired[ClusteringBaseState]


class ClusteringModelStageResult(TypedDict):
    """Result of model-stage setup or early error payload."""

    model_inputs: NotRequired[ClusteringModelStageInputs]
    model_bundle: NotRequired[ClusteringModelBundle]
    error_payload: NotRequired[ClusteringBaseState]
