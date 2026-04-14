from __future__ import annotations

from typing import Any, TypedDict


class ClusterMetrics(TypedDict, total=False):
    """Core clustering quality metrics and cluster shape counters."""

    silhouette: float | None
    davies_bouldin: float | None
    calinski_harabasz: float | None
    cluster_balance_ratio: float | None
    smallest_cluster_size: int | None
    largest_cluster_size: int | None
    stability_ari: float | None
    initialization_ari: float | None
    explained_variance: float | None
    has_microclusters: bool | None
    microcluster_threshold: int | None


class QualityScore(TypedDict, total=False):
    """Single UI card/row item with value and optional explanation."""

    label: str
    value: str
    meta: str


class ClusterLabel(TypedDict):
    """Human-readable clustering quality label and explanatory note."""

    label: str
    note: str


class ClusterMethod(TypedDict, total=False):
    """Method-level row used in quality comparison and selection logic."""

    method_key: str
    method_label: str
    algorithm_key: str
    silhouette: float | None
    davies_bouldin: float | None
    calinski_harabasz: float | None
    cluster_balance_ratio: float | None
    is_selected: bool
    is_recommended: bool
    cluster_count: int


class MethodComparisonRow(TypedDict):
    """Prepared method comparison row for rendering in quality table."""

    method_label: str
    selection_label: str
    silhouette_display: str
    davies_display: str
    calinski_display: str
    balance_display: str


class FeatureAblationRow(TypedDict, total=False):
    """Ablation delta row for feature-inclusion quality diagnostics."""

    feature: str
    direction: str
    delta_score: float | None


class FeatureSelectionReport(TypedDict, total=False):
    """Feature-selection metadata used in clustering quality narrative."""

    volume_role_label: str
    volume_note: str
    weighting_label: str
    weighting_note: str
    weighting_meta: str
    ablation_rows: list[FeatureAblationRow]


class QualityDiagnostics(TypedDict, total=False):
    """Diagnostics payload with best method/cluster recommendations."""

    best_configuration: ClusterMethod
    best_quality_k: int | None
    best_silhouette_k: int | None


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


class SupportSummary(TypedDict, total=False):
    """Support coverage summary with share of low-support territories."""

    low_support_share: float | None


class ClusteringFilters(TypedDict, total=False):
    """Request filters and selectable options for clustering page payloads."""

    table_name: str
    cluster_count: str
    sample_limit: str
    sampling_strategy: str
    feature_columns: list[str]
    available_tables: list[dict[str, str]]
    available_cluster_counts: list[dict[str, str]]
    available_sample_limits: list[dict[str, str]]
    available_sampling_strategies: list[dict[str, str]]
    available_features: list[dict[str, Any]]


class ClusteringSummary(TypedDict, total=False):
    """Top-level clustering summary counters and labels shown in hero/cards."""

    selected_table_label: str
    total_incidents_display: str
    total_entities_display: str
    sampled_entities_display: str
    clustered_entities_display: str
    excluded_entities_display: str
    candidate_features_display: str
    selected_features_display: str
    cluster_count_display: str
    cluster_count_requested_display: str
    cluster_count_note: str
    suggested_cluster_count_label: str
    suggested_cluster_count_display: str
    suggested_cluster_count_note: str
    elbow_cluster_count_display: str
    silhouette_display: str
    pca_variance_display: str
    inertia_display: str
    sampling_strategy_label: str


class DiagnosticsRow(TypedDict, total=False):
    """Diagnostics row for cluster-count quality chart and table views."""

    cluster_count: int
    silhouette: float | None
    davies_bouldin: float | None
    calinski_harabasz: float | None
    cluster_balance_ratio: float | None


class ClusteringDiagnostics(QualityDiagnostics, total=False):
    """Diagnostics payload enriched with chart rows and elbow recommendation."""

    rows: list[DiagnosticsRow]
    elbow_k: int | None


class ClusteringModelOutput(TypedDict, total=False):
    """Model output used by result assembly and quality narrative builders."""

    silhouette: float | None
    explained_variance: float | None
    inertia: float | None
    pca_points: Any
    stability_ari: float | None


class ClusteringDataset(TypedDict, total=False):
    """Dataset-level counters and support summary after preprocessing stage."""

    total_incidents: int
    total_entities: int
    sampled_entities: int
    support_summary: SupportSummary | None


class ClusteringModelInputs(TypedDict, total=False):
    """Prepared inputs that feed clustering model stage."""

    requested_working_cluster_count: int


class ClusteringModelBundle(TypedDict, total=False):
    """Combined model stage outputs used by core_results and rendering."""

    actual_cluster_count: int
    diagnostics: ClusteringDiagnostics
    clustering: ClusteringModelOutput
    runtime_feature_context: FeatureSelectionReport


class ClusteringCharts(TypedDict):
    """Chart bundle rendered in clustering payload."""

    scatter: dict[str, Any]
    distribution: dict[str, Any]
    diagnostics: dict[str, Any]


class ClusteringPayload(TypedDict, total=False):
    """Top-level clustering payload returned to API/template layer."""

    has_data: bool
    model_description: str
    summary: ClusteringSummary
    quality_assessment: ClusteringQualityAssessment
    cluster_profiles: list[dict[str, Any]]
    centroid_columns: list[dict[str, Any]]
    centroid_rows: list[dict[str, Any]]
    representative_columns: list[dict[str, Any]]
    representative_rows: list[dict[str, Any]]
    charts: ClusteringCharts
    notes: list[str]
    filters: ClusteringFilters
