from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from . import impact_fire_metrics as _fire_metrics
from . import impact_forecast_metrics as _forecast_metrics


class DashboardImpactMetrics:
    @staticmethod
    def collect_dashboard_grouped_counts(
        selected_tables: List[Dict[str, Any]],
        selected_year: Optional[int],
        selected_group_column: Optional[str] = None,
        *,
        include_area_buckets: bool = True,
        include_impact_timeline: bool = True,
        positive_count_columns: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        return _fire_metrics._collect_dashboard_grouped_counts(
            selected_tables=selected_tables,
            selected_year=selected_year,
            selected_group_column=selected_group_column,
            include_area_buckets=include_area_buckets,
            include_impact_timeline=include_impact_timeline,
            positive_count_columns=positive_count_columns,
        )

    @staticmethod
    def collect_impact_timeline_rows(
        selected_tables: List[Dict[str, Any]],
        selected_year: Optional[int],
    ) -> List[Dict[str, Any]]:
        return _forecast_metrics._collect_impact_timeline_rows(
            selected_tables=selected_tables,
            selected_year=selected_year,
        )

    @staticmethod
    def collect_cause_counts(
        selected_tables: List[Dict[str, Any]],
        selected_year: Optional[int],
    ) -> Dict[str, int]:
        return _fire_metrics._collect_cause_counts(
            selected_tables=selected_tables,
            selected_year=selected_year,
        )

    @staticmethod
    def collect_month_counts(
        selected_tables: List[Dict[str, Any]],
        selected_year: Optional[int],
    ) -> Dict[int, int]:
        return _fire_metrics._collect_month_counts(
            selected_tables=selected_tables,
            selected_year=selected_year,
        )

    @staticmethod
    def build_area_buckets_chart(
        selected_tables: List[Dict[str, Any]],
        selected_year: Optional[int],
    ) -> Dict[str, Any]:
        return _fire_metrics._build_area_buckets_chart(
            selected_tables=selected_tables,
            selected_year=selected_year,
        )

    @staticmethod
    def build_area_buckets_chart_from_counts(bucket_counts: Dict[str, int]) -> Dict[str, Any]:
        return _fire_metrics._build_area_buckets_chart_from_counts(bucket_counts)

    @staticmethod
    def build_cause_chart(
        selected_tables: List[Dict[str, Any]],
        selected_year: Optional[int],
        *,
        cause_counts: Optional[Dict[str, int]] = None,
    ) -> Dict[str, Any]:
        return _fire_metrics._build_cause_chart(
            selected_tables=selected_tables,
            selected_year=selected_year,
            cause_counts=cause_counts,
        )

    @staticmethod
    def build_combined_impact_timeline_chart(
        selected_tables: List[Dict[str, Any]],
        selected_year: Optional[int],
        *,
        impact_timeline_rows: Optional[Sequence[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        return _forecast_metrics._build_combined_impact_timeline_chart(
            selected_tables=selected_tables,
            selected_year=selected_year,
            impact_timeline_rows=impact_timeline_rows,
        )

    @staticmethod
    def build_monthly_profile_chart(
        selected_tables: List[Dict[str, Any]],
        selected_year: Optional[int],
        *,
        month_counts: Optional[Dict[int, int]] = None,
    ) -> Dict[str, Any]:
        return _fire_metrics._build_monthly_profile_chart(
            selected_tables=selected_tables,
            selected_year=selected_year,
            month_counts=month_counts,
        )

    @staticmethod
    def build_sql_district_widget_from_counts(district_counts: Dict[str, int]) -> Dict[str, Any]:
        return _forecast_metrics._build_sql_district_widget_from_counts(district_counts)

    @staticmethod
    def build_sql_widgets(
        selected_tables: List[Dict[str, Any]],
        selected_year: Optional[int],
        *,
        cause_counts: Optional[Dict[str, int]] = None,
        month_counts: Optional[Dict[int, int]] = None,
        district_counts: Optional[Dict[str, int]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        return _forecast_metrics._build_sql_widgets(
            selected_tables=selected_tables,
            selected_year=selected_year,
            cause_counts=cause_counts,
            month_counts=month_counts,
            district_counts=district_counts,
        )


def _collect_dashboard_grouped_counts(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    selected_group_column: Optional[str] = None,
    *,
    include_area_buckets: bool = True,
    include_impact_timeline: bool = True,
    positive_count_columns: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    return DashboardImpactMetrics.collect_dashboard_grouped_counts(
        selected_tables=selected_tables,
        selected_year=selected_year,
        selected_group_column=selected_group_column,
        include_area_buckets=include_area_buckets,
        include_impact_timeline=include_impact_timeline,
        positive_count_columns=positive_count_columns,
    )


def _collect_impact_timeline_rows(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
) -> List[Dict[str, Any]]:
    return DashboardImpactMetrics.collect_impact_timeline_rows(
        selected_tables=selected_tables,
        selected_year=selected_year,
    )


def _collect_cause_counts(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
) -> Dict[str, int]:
    return DashboardImpactMetrics.collect_cause_counts(
        selected_tables=selected_tables,
        selected_year=selected_year,
    )


def _collect_month_counts(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
) -> Dict[int, int]:
    return DashboardImpactMetrics.collect_month_counts(
        selected_tables=selected_tables,
        selected_year=selected_year,
    )


def _build_area_buckets_chart(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
) -> Dict[str, Any]:
    return DashboardImpactMetrics.build_area_buckets_chart(
        selected_tables=selected_tables,
        selected_year=selected_year,
    )


def _build_area_buckets_chart_from_counts(bucket_counts: Dict[str, int]) -> Dict[str, Any]:
    return DashboardImpactMetrics.build_area_buckets_chart_from_counts(bucket_counts)


def _build_cause_chart(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    *,
    cause_counts: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    return DashboardImpactMetrics.build_cause_chart(
        selected_tables=selected_tables,
        selected_year=selected_year,
        cause_counts=cause_counts,
    )


def _build_combined_impact_timeline_chart(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    *,
    impact_timeline_rows: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return DashboardImpactMetrics.build_combined_impact_timeline_chart(
        selected_tables=selected_tables,
        selected_year=selected_year,
        impact_timeline_rows=impact_timeline_rows,
    )


def _build_monthly_profile_chart(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    *,
    month_counts: Optional[Dict[int, int]] = None,
) -> Dict[str, Any]:
    return DashboardImpactMetrics.build_monthly_profile_chart(
        selected_tables=selected_tables,
        selected_year=selected_year,
        month_counts=month_counts,
    )


def _build_sql_district_widget_from_counts(district_counts: Dict[str, int]) -> Dict[str, Any]:
    return DashboardImpactMetrics.build_sql_district_widget_from_counts(district_counts)


def _build_sql_widgets(
    selected_tables: List[Dict[str, Any]],
    selected_year: Optional[int],
    *,
    cause_counts: Optional[Dict[str, int]] = None,
    month_counts: Optional[Dict[int, int]] = None,
    district_counts: Optional[Dict[str, int]] = None,
) -> Dict[str, Dict[str, Any]]:
    return DashboardImpactMetrics.build_sql_widgets(
        selected_tables=selected_tables,
        selected_year=selected_year,
        cause_counts=cause_counts,
        month_counts=month_counts,
        district_counts=district_counts,
    )


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
