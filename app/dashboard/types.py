from __future__ import annotations

from typing import Any, TypedDict


class DashboardOption(TypedDict, total=False):
    """Selectable option item used by dashboard filters."""

    value: str
    label: str


class DashboardTableRef(TypedDict, total=False):
    """Resolved source table metadata used across dashboard services."""

    name: str
    column_set: set[str]
    table_year: int | None


class ImpactTotals(TypedDict, total=False):
    """Aggregated impact counters keyed by metric name."""

    deaths: float
    injuries: float
    evacuated: float
    evacuated_children: float
    rescued_total: float
    rescued_children: float


class SummaryRow(TypedDict, total=False):
    """Per-table summary row produced by summary aggregation queries."""

    table_name: str
    years_in_scope: list[int]
    fire_count: int
    total_area: float
    area_count: int
    impact_totals: dict[str, float]


class SummaryBundle(TypedDict, total=False):
    """Bundle with rows and yearly grouping used by summary builders."""

    summary_rows: list[SummaryRow]
    yearly_grouped: dict[int, dict[str, float]]


class SummaryResult(TypedDict, total=False):
    """Top-level summary metrics shown in dashboard cards and scope."""

    fires_count: int
    fires_count_display: str
    total_area: float
    total_area_display: str
    average_area: float
    average_area_display: str
    tables_used: int
    tables_used_display: str
    area_records: int
    area_records_display: str
    area_fill_rate: float
    area_fill_rate_display: str
    years_covered: int
    years_covered_display: str
    period_label: str
    year_label: str
    deaths: float
    deaths_display: str
    lethality_rate: float
    lethality_rate_display: str
    injuries: float
    injuries_display: str
    evacuated: float
    evacuated_display: str
    evacuated_adults: float
    evacuated_adults_display: str
    evacuated_children: float
    evacuated_children_display: str
    rescued_total: float
    rescued_total_display: str
    rescued_adults: float
    rescued_adults_display: str
    rescued_children: float
    rescued_children_display: str
    children_total: float
    children_total_display: str


class DistributionItem(TypedDict, total=False):
    """Single chart row with label, numeric value, and optional split fields."""

    label: str
    value: float | int
    value_display: str
    destroyed: int
    damaged: int
    date_value: str


class DistributionResult(TypedDict, total=False):
    """Chart payload returned by distribution and overview builders."""

    title: str
    items: list[DistributionItem]
    empty_message: str
    description: str
    plotly: Any


class SummaryCard(TypedDict, total=False):
    """Compact dashboard highlight card."""

    label: str
    value: str
    meta: str
    tone: str


class ImpactMetric(TypedDict, total=False):
    """Combined impact metric point for timeline/impact charts."""

    label: str
    date_value: str
    value: float
    value_display: str
    deaths: float
    injuries: float
    evacuated_adults: float
    evacuated_children: float
    rescued_children: float


class ImpactTimelineSqlRow(TypedDict, total=False):
    """Raw timeline row returned by SQL union before chart shaping."""

    date_value: Any
    deaths: float
    injuries: float
    evacuated: float
    evacuated_children: float
    rescued_children: float


class ImpactTimelineBucket(TypedDict, total=False):
    """Aggregated timeline bucket keyed by date string."""

    date_value: str
    label: str
    deaths: float
    injuries: float
    evacuated: float
    evacuated_children: float
    rescued_children: float


class DashboardWidgets(TypedDict, total=False):
    """Three SQL widgets rendered in the dashboard side panel."""

    causes: DistributionResult
    districts: DistributionResult
    seasons: DistributionResult


class DashboardSummarySeries(TypedDict, total=False):
    """Precomputed summary series reused by trend/highlight builders."""

    summary: SummaryResult
    yearly_fires_series: DistributionResult
    table_breakdown_series: DistributionResult


class DashboardSummaryMetrics(TypedDict, total=False):
    """Derived trend/ranking/highlight block based on summary series."""

    trend: DashboardSection
    rankings: dict[str, list[DistributionItem]]
    highlights: list[SummaryCard]


class DashboardGroupedQueryContext(TypedDict, total=False):
    """Resolved query context for grouped SQL aggregation."""

    where_clause: str
    cause_column: str
    distribution_column: str
    district_column: str
    has_date_column: bool
    has_timeline: bool
    include_area_buckets: bool
    dimensions: list[tuple[str, str]]


class DashboardGroupedDimensionSql(TypedDict, total=False):
    """SQL fragments for GROUPING SETS and label/metric projections."""

    metric_kind_case: str
    label_case: str
    grouping_sets: str
    having_clause: str
    positive_group_condition: str


class DashboardGroupedResultSelects(TypedDict, total=False):
    """SQL SELECT fragments for grouped rows and positive column bundles."""

    fire_count_select: str
    date_value_select: str
    metric_selects: list[str]
    positive_metric_selects: list[str]


class DashboardSection(TypedDict, total=False):
    """Reusable dashboard section object for scope, trend, and widgets."""

    title: str
    description: str
    direction: str
    table_label: str
    year_label: str
    group_label: str
    items: list[DistributionItem]


class DashboardMetadata(TypedDict, total=False):
    """Dashboard metadata and filter catalog loaded from cache/introspection."""

    tables: list[DashboardTableRef]
    table_options: list[DashboardOption]
    default_group_column: str
    errors: list[str]


class DashboardRequestState(TypedDict, total=False):
    """Resolved request state produced from query params and metadata."""

    selected_tables: list[DashboardTableRef]
    selected_table_name: str
    selected_year: int | None
    selected_group_column: str
    available_years: list[DashboardOption]
    available_group_columns: list[DashboardOption]
    cache_key: tuple[Any, ...]
    resolved_cache_key: tuple[Any, ...]


class DashboardGroupedCounts(TypedDict, total=False):
    """Grouped counters used to build dashboard charts and SQL widgets."""

    cause_counts: dict[str, int]
    district_counts: dict[str, int]
    distribution_counts: dict[str, int]
    month_counts: dict[int, int]
    area_bucket_counts: dict[str, int]
    positive_column_counts: dict[str, int]
    impact_timeline_rows: list[ImpactTimelineSqlRow]


class DashboardAggregation(TypedDict, total=False):
    """Intermediate aggregation bundle assembled before final payload render."""

    summary: SummaryResult
    yearly_fires_series: DistributionResult
    cause_overview: DistributionResult
    distribution: DistributionResult
    yearly_area_chart: DistributionResult
    monthly_profile: DistributionResult
    monthly_heatmap: DistributionResult
    area_buckets: DistributionResult
    cumulative_area: DistributionResult
    trend: DashboardSection
    rankings: dict[str, list[DistributionItem]]
    highlights: list[SummaryCard]
    widgets: DashboardWidgets
    management: dict[str, Any]
    scope: DashboardSection


class DashboardPayload(TypedDict, total=False):
    """Final dashboard payload returned to API/template rendering layer."""

    generated_at: str
    has_data: bool
    summary: SummaryResult
    scope: DashboardSection
    trend: DashboardSection
    management: dict[str, Any]
    highlights: list[SummaryCard]
    rankings: dict[str, list[DistributionItem]]
    widgets: dict[str, DistributionResult]
    charts: dict[str, DistributionResult]
    filters: dict[str, Any]
    notes: list[str]
    bootstrap_mode: str


class DashboardContext(TypedDict, total=False):
    """Page-level context with filters, initial data, and UI metadata."""

    generated_at: str
    filters: dict[str, Any]
    initial_data: DashboardPayload
    errors: list[str]
    has_data: bool
    plotly_js: str
