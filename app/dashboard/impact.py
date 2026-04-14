from __future__ import annotations

from .impact_fire_metrics import (
    _build_area_buckets_chart,
    _build_area_buckets_chart_from_counts,
    _build_cause_chart,
    _build_monthly_profile_chart,
    _collect_cause_counts,
    _collect_dashboard_grouped_counts,
    _collect_month_counts,
)
from .impact_forecast_metrics import (
    _build_combined_impact_timeline_chart,
    _build_sql_district_widget_from_counts,
    _build_sql_widgets,
    _collect_impact_timeline_rows,
)


class DashboardImpactMetrics:
    collect_dashboard_grouped_counts = staticmethod(_collect_dashboard_grouped_counts)
    collect_impact_timeline_rows = staticmethod(_collect_impact_timeline_rows)
    collect_cause_counts = staticmethod(_collect_cause_counts)
    collect_month_counts = staticmethod(_collect_month_counts)
    build_area_buckets_chart = staticmethod(_build_area_buckets_chart)
    build_area_buckets_chart_from_counts = staticmethod(_build_area_buckets_chart_from_counts)
    build_cause_chart = staticmethod(_build_cause_chart)
    build_combined_impact_timeline_chart = staticmethod(_build_combined_impact_timeline_chart)
    build_monthly_profile_chart = staticmethod(_build_monthly_profile_chart)
    build_sql_district_widget_from_counts = staticmethod(_build_sql_district_widget_from_counts)
    build_sql_widgets = staticmethod(_build_sql_widgets)


__all__ = [
    "DashboardImpactMetrics",
    "_collect_dashboard_grouped_counts",
    "_collect_impact_timeline_rows",
    "_collect_cause_counts",
    "_collect_month_counts",
    "_build_area_buckets_chart",
    "_build_area_buckets_chart_from_counts",
    "_build_cause_chart",
    "_build_combined_impact_timeline_chart",
    "_build_monthly_profile_chart",
    "_build_sql_district_widget_from_counts",
    "_build_sql_widgets",
]
