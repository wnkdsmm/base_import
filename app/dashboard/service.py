# Compatibility re-export layer. Import directly from submodules in new code.

from .dashboard_service_data import *
from .dashboard_service_build import *
from .cache import _collect_dashboard_metadata_cached, _get_dashboard_cache, _set_dashboard_cache
from .distribution import _collect_damage_counts
from .distribution import _build_distribution_chart, _build_rankings, _build_table_breakdown_chart
from .distribution_logic import (
    _can_reuse_distribution_counts,
    _build_dashboard_widgets,
    _build_damage_dashboard_charts,
    _build_standard_dashboard_charts,
)
from .impact_fire_metrics import (
    _build_area_buckets_chart,
    _build_area_buckets_chart_from_counts,
    _build_cause_chart,
    _collect_cause_counts,
    _collect_month_counts,
    _build_monthly_profile_chart,
)
from .impact_forecast_metrics import _build_combined_impact_timeline_chart, _build_sql_widgets
from .impact import _collect_dashboard_grouped_counts
from .summary_logic import _build_dashboard_summary_series
from .distribution import _damage_count_columns
from .management import _build_management_snapshot
from .metadata import _resolve_dashboard_filters
from .summary import (
    _build_highlights,
    _build_scope,
    _build_summary,
    _build_trend,
    _build_yearly_chart,
    _collect_dashboard_summary_bundle,
)
from app.services.executive_brief import compose_executive_brief_text

__all__ = [
    '_build_dashboard_cache_key',
    '_build_resolved_dashboard_cache_key',
    '_build_dashboard_context_payload',
    '_resolve_shell_group_columns',
    '_build_dashboard_shell_initial_data',
    '_resolve_requested_dashboard_cache',
    '_build_dashboard_request_state',
    '_update_dashboard_filter_metrics',
    'build_dashboard_context',
    'get_dashboard_page_context',
    'get_dashboard_shell_context',
    'get_dashboard_data',
    '_build_dashboard_error_context',
    '_build_dashboard_aggregation',
    '_build_dashboard_payload',
    '_empty_dashboard_data',
    '_can_reuse_distribution_counts',
    '_collect_damage_counts',
    '_build_dashboard_widgets',
    '_build_damage_dashboard_charts',
    '_build_standard_dashboard_charts',
    '_build_cause_chart',
    '_collect_dashboard_grouped_counts',
    '_build_dashboard_summary_series',
    '_damage_count_columns',
    '_build_trend',
    '_build_rankings',
    '_build_highlights',
    '_build_area_buckets_chart',
    '_build_area_buckets_chart_from_counts',
    '_build_combined_impact_timeline_chart',
    '_build_distribution_chart',
    '_build_management_snapshot',
    '_build_monthly_profile_chart',
    '_build_scope',
    '_build_sql_widgets',
    '_build_summary',
    '_build_table_breakdown_chart',
    '_build_yearly_chart',
    '_collect_cause_counts',
    '_collect_dashboard_metadata_cached',
    '_collect_dashboard_summary_bundle',
    '_collect_month_counts',
    '_get_dashboard_cache',
    '_resolve_dashboard_filters',
    '_set_dashboard_cache',
    'compose_executive_brief_text',
]
