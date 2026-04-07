from __future__ import annotations

"""Compatibility facade for dashboard aggregate builders.

Prefer direct imports from ``app.dashboard.summary``, ``app.dashboard.distribution``,
and ``app.dashboard.impact`` in new code.
"""

from app.compat import install_lazy_exports

_EXPORTS = {
    "_build_area_buckets_chart": ("app.dashboard.impact", "_build_area_buckets_chart"),
    "_build_cause_chart": ("app.dashboard.impact", "_build_cause_chart"),
    "_build_combined_impact_timeline_chart": ("app.dashboard.impact", "_build_combined_impact_timeline_chart"),
    "_build_damage_category_items": ("app.dashboard.distribution", "_build_damage_category_items"),
    "_build_damage_overview_chart": ("app.dashboard.distribution", "_build_damage_overview_chart"),
    "_build_damage_pairs_chart": ("app.dashboard.distribution", "_build_damage_pairs_chart"),
    "_build_damage_share_chart": ("app.dashboard.distribution", "_build_damage_share_chart"),
    "_build_damage_standalone_chart": ("app.dashboard.distribution", "_build_damage_standalone_chart"),
    "_build_damage_theme_items": ("app.dashboard.distribution", "_build_damage_theme_items"),
    "_build_distribution_chart": ("app.dashboard.distribution", "_build_distribution_chart"),
    "_build_highlights": ("app.dashboard.summary", "_build_highlights"),
    "_build_monthly_profile_chart": ("app.dashboard.impact", "_build_monthly_profile_chart"),
    "_build_rankings": ("app.dashboard.distribution", "_build_rankings"),
    "_build_scope": ("app.dashboard.summary", "_build_scope"),
    "_build_sql_widgets": ("app.dashboard.impact", "_build_sql_widgets"),
    "_build_summary": ("app.dashboard.summary", "_build_summary"),
    "_build_table_breakdown_chart": ("app.dashboard.distribution", "_build_table_breakdown_chart"),
    "_build_trend": ("app.dashboard.summary", "_build_trend"),
    "_build_yearly_chart": ("app.dashboard.summary", "_build_yearly_chart"),
    "_collect_positive_column_counts": ("app.dashboard.distribution", "_collect_positive_column_counts"),
}

__all__, __getattr__, __dir__ = install_lazy_exports(__name__, globals(), _EXPORTS)
