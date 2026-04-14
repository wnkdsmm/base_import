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
