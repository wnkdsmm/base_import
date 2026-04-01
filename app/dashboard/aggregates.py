from __future__ import annotations

"""Compatibility facade for dashboard aggregate builders.

Prefer direct imports from ``app.dashboard.summary``, ``app.dashboard.distribution``,
and ``app.dashboard.impact`` in new code.
"""

from .distribution import (
    _build_damage_category_items,
    _build_damage_overview_chart,
    _build_damage_pairs_chart,
    _build_damage_share_chart,
    _build_damage_standalone_chart,
    _build_damage_theme_items,
    _build_distribution_chart,
    _build_rankings,
    _build_table_breakdown_chart,
    _collect_positive_column_counts,
)
from .impact import (
    _build_area_buckets_chart,
    _build_cause_chart,
    _build_combined_impact_timeline_chart,
    _build_monthly_profile_chart,
    _build_sql_widgets,
)
from .summary import _build_highlights, _build_scope, _build_summary, _build_trend, _build_yearly_chart

__all__ = [
    "_build_area_buckets_chart",
    "_build_cause_chart",
    "_build_combined_impact_timeline_chart",
    "_build_damage_category_items",
    "_build_damage_overview_chart",
    "_build_damage_pairs_chart",
    "_build_damage_share_chart",
    "_build_damage_standalone_chart",
    "_build_damage_theme_items",
    "_build_distribution_chart",
    "_build_highlights",
    "_build_monthly_profile_chart",
    "_build_rankings",
    "_build_scope",
    "_build_sql_widgets",
    "_build_summary",
    "_build_table_breakdown_chart",
    "_build_trend",
    "_build_yearly_chart",
    "_collect_positive_column_counts",
]
