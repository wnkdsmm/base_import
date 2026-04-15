from __future__ import annotations

from config.db import engine as _default_engine

from . import impact_fire_metrics as _impact_fire_metrics_mod
from . import impact_forecast_metrics as _impact_forecast_metrics_mod


engine = _default_engine


def _sync_engine() -> None:
    _impact_fire_metrics_mod.engine = engine
    _impact_forecast_metrics_mod.engine = engine


def _collect_dashboard_grouped_counts(*args, **kwargs):
    _sync_engine()
    return _impact_fire_metrics_mod._collect_dashboard_grouped_counts(*args, **kwargs)


def _collect_impact_timeline_rows(*args, **kwargs):
    _sync_engine()
    return _impact_forecast_metrics_mod._collect_impact_timeline_rows(*args, **kwargs)


def _collect_cause_counts(*args, **kwargs):
    _sync_engine()
    return _impact_fire_metrics_mod._collect_cause_counts(*args, **kwargs)


def _collect_month_counts(*args, **kwargs):
    _sync_engine()
    return _impact_fire_metrics_mod._collect_month_counts(*args, **kwargs)


def _build_area_buckets_chart(*args, **kwargs):
    _sync_engine()
    return _impact_fire_metrics_mod._build_area_buckets_chart(*args, **kwargs)


def _build_area_buckets_chart_from_counts(*args, **kwargs):
    _sync_engine()
    return _impact_fire_metrics_mod._build_area_buckets_chart_from_counts(*args, **kwargs)


def _build_cause_chart(*args, **kwargs):
    _sync_engine()
    return _impact_fire_metrics_mod._build_cause_chart(*args, **kwargs)


def _build_combined_impact_timeline_chart(*args, **kwargs):
    _sync_engine()
    return _impact_forecast_metrics_mod._build_combined_impact_timeline_chart(*args, **kwargs)


def _build_monthly_profile_chart(*args, **kwargs):
    _sync_engine()
    return _impact_fire_metrics_mod._build_monthly_profile_chart(*args, **kwargs)


def _build_sql_district_widget_from_counts(*args, **kwargs):
    _sync_engine()
    return _impact_forecast_metrics_mod._build_sql_district_widget_from_counts(*args, **kwargs)


def _build_sql_district_widget(*args, **kwargs):
    _sync_engine()
    return _impact_forecast_metrics_mod._build_sql_district_widget(*args, **kwargs)


def _build_sql_widgets(*args, **kwargs):
    _sync_engine()
    selected_tables = args[0] if args else kwargs.get("selected_tables")
    selected_year = args[1] if len(args) > 1 else kwargs.get("selected_year")
    cause_counts = kwargs.get("cause_counts")
    month_counts = kwargs.get("month_counts")
    district_counts = kwargs.get("district_counts")
    return {
        "causes": (
            _impact_forecast_metrics_mod._build_sql_cause_widget(
                selected_tables,
                selected_year,
                cause_counts=cause_counts,
            )
            if cause_counts is not None
            else _impact_forecast_metrics_mod._build_sql_cause_widget(selected_tables, selected_year)
        ),
        "districts": (
            _build_sql_district_widget_from_counts(district_counts)
            if district_counts is not None
            else _build_sql_district_widget(selected_tables, selected_year)
        ),
        "seasons": _impact_forecast_metrics_mod._build_sql_season_widget(
            selected_tables,
            selected_year,
            month_counts=month_counts,
        ),
    }


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
    "engine",
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
    "_build_sql_district_widget",
    "_build_sql_widgets",
]
