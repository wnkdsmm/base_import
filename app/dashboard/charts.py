from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.services.charting import (
    build_dashboard_area_bucket_plotly,
    build_dashboard_cause_plotly,
    build_dashboard_combined_impact_timeline_plotly,
    build_dashboard_damage_overview_plotly,
    build_dashboard_damage_pairs_plotly,
    build_dashboard_damage_share_plotly,
    build_dashboard_damage_standalone_plotly,
    build_dashboard_distribution_pie_plotly,
    build_dashboard_distribution_plotly,
    build_dashboard_empty_plotly_chart,
    build_dashboard_finalize_chart,
    build_dashboard_monthly_profile_plotly,
    build_dashboard_plotly_layout,
    build_dashboard_sql_widget_bar_plotly,
    build_dashboard_sql_widget_season_plotly,
    build_dashboard_table_breakdown_plotly,
    build_dashboard_wrap_plotly_label,
    build_dashboard_yearly_plotly,
)


def _finalize_chart(
    title: str,
    items: List[Dict[str, Any]],
    empty_message: str,
    plotly: Optional[Dict[str, Any]] = None,
    description: str = "",
) -> Dict[str, Any]:
    return build_dashboard_finalize_chart(title, items, empty_message, plotly=plotly, description=description)


def _build_empty_plotly_chart(title: str, empty_message: str) -> Dict[str, Any]:
    return build_dashboard_empty_plotly_chart(title, empty_message)


def _build_yearly_plotly(title: str, items: List[Dict[str, Any]], metric: str, empty_message: str) -> Dict[str, Any]:
    return build_dashboard_yearly_plotly(title, items, metric, empty_message)


def _wrap_plotly_label(value: Any, max_width: int = 34, max_lines: int = 3) -> str:
    return build_dashboard_wrap_plotly_label(value, max_width=max_width, max_lines=max_lines)


def _build_cause_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    return build_dashboard_cause_plotly(title, items, empty_message)


def _build_distribution_pie_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    return build_dashboard_distribution_pie_plotly(title, items, empty_message)


def _build_distribution_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    return build_dashboard_distribution_plotly(title, items, empty_message)


def _build_combined_impact_timeline_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    return build_dashboard_combined_impact_timeline_plotly(title, items, empty_message)


def _build_damage_overview_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    return build_dashboard_damage_overview_plotly(title, items, empty_message)


def _build_damage_pairs_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    return build_dashboard_damage_pairs_plotly(title, items, empty_message)


def _build_damage_standalone_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    return build_dashboard_damage_standalone_plotly(title, items, empty_message)


def _build_damage_share_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    return build_dashboard_damage_share_plotly(title, items, empty_message)


def _build_table_breakdown_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    return build_dashboard_table_breakdown_plotly(title, items, empty_message)


def _build_monthly_profile_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    return build_dashboard_monthly_profile_plotly(title, items, empty_message)


def _build_area_bucket_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    return build_dashboard_area_bucket_plotly(title, items, empty_message)


def _build_sql_widget_bar_plotly(
    title: str,
    items: List[Dict[str, Any]],
    empty_message: str,
    color_key: str,
    value_label: str,
) -> Dict[str, Any]:
    return build_dashboard_sql_widget_bar_plotly(title, items, empty_message, color_key, value_label)


def _build_sql_widget_season_plotly(title: str, items: List[Dict[str, Any]], empty_message: str) -> Dict[str, Any]:
    return build_dashboard_sql_widget_season_plotly(title, items, empty_message)


def _plotly_layout(yaxis_title: str, showlegend: bool) -> Dict[str, Any]:
    return build_dashboard_plotly_layout(yaxis_title, showlegend=showlegend)


__all__ = [
    "_finalize_chart",
    "_build_yearly_plotly",
    "_wrap_plotly_label",
    "_build_cause_plotly",
    "_build_distribution_pie_plotly",
    "_build_distribution_plotly",
    "_build_combined_impact_timeline_plotly",
    "_build_damage_overview_plotly",
    "_build_damage_pairs_plotly",
    "_build_damage_standalone_plotly",
    "_build_damage_share_plotly",
    "_build_table_breakdown_plotly",
    "_build_monthly_profile_plotly",
    "_build_area_bucket_plotly",
    "_build_sql_widget_bar_plotly",
    "_build_sql_widget_season_plotly",
    "_plotly_layout",
]
