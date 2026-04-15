from __future__ import annotations

from typing import Any, Dict, Optional

from app.statistics_constants import CAUSE_COLUMNS

from .data_access import _resolve_cause_column, _resolve_table_column_name
from .distribution import (
    _build_damage_category_items,
    _build_damage_overview_chart,
    _build_damage_pairs_chart,
    _build_damage_share_chart,
    _build_damage_standalone_chart,
    _build_distribution_chart,
    _build_damage_theme_items,
    _collect_damage_counts,
)
from .impact import (
    _build_area_buckets_chart,
    _build_area_buckets_chart_from_counts,
    _build_combined_impact_timeline_chart,
    _build_monthly_profile_chart,
    _build_sql_district_widget_from_counts,
    _build_sql_widgets,
)


def _can_reuse_distribution_counts(
    selected_tables: list[dict[str, Any]],
    group_column: str,
) -> bool:
    if not group_column:
        return False
    if group_column not in CAUSE_COLUMNS:
        return any(_resolve_table_column_name(table, group_column) for table in selected_tables)
    for table in selected_tables:
        if _resolve_table_column_name(table, group_column) != _resolve_cause_column(table):
            return False
    return True


def _build_damage_dashboard_item_bundle(
    selected_tables: list[dict[str, Any]],
    selected_year: Optional[int],
    damage_counts: Dict[str, int],
) -> Dict[str, list[dict[str, Any]]]:
    return {
        "category_items": _build_damage_category_items(
            selected_tables,
            selected_year,
            counts=damage_counts,
        ),
        "theme_items": _build_damage_theme_items(
            selected_tables,
            selected_year,
            counts=damage_counts,
        ),
    }


def _build_damage_dashboard_charts(
    selected_tables: list[dict[str, Any]],
    selected_year: Optional[int],
    *,
    damage_counts: Optional[Dict[str, int]] = None,
) -> dict[str, Any]:
    damage_counts = damage_counts if damage_counts is not None else _collect_damage_counts(selected_tables, selected_year)
    damage_item_bundle = _build_damage_dashboard_item_bundle(selected_tables, selected_year, damage_counts)
    damage_category_items = damage_item_bundle["category_items"]
    damage_theme_items = damage_item_bundle["theme_items"]
    return {
        "distribution": _build_damage_overview_chart(
            selected_tables,
            selected_year,
            items=damage_category_items,
        ),
        "yearly_area_chart": _build_damage_pairs_chart(
            selected_tables,
            selected_year,
            items=damage_category_items,
        ),
        "monthly_profile": _build_damage_standalone_chart(
            selected_tables,
            selected_year,
            items=damage_theme_items,
        ),
        "area_buckets": _build_damage_share_chart(
            selected_tables,
            selected_year,
            items=damage_theme_items,
        ),
    }


def _build_standard_dashboard_charts(
    selected_tables: list[dict[str, Any]],
    selected_year: Optional[int],
    selected_group_column: str,
    grouped_counts_bundle: dict[str, Any],
) -> dict[str, Any]:
    distribution_counts = grouped_counts_bundle["distribution_counts"]
    reusable_distribution_counts = (
        distribution_counts
        if _can_reuse_distribution_counts(
            selected_tables,
            selected_group_column,
        )
        else None
    )
    area_bucket_counts = grouped_counts_bundle["area_bucket_counts"]
    return {
        "distribution": _build_distribution_chart(
            selected_tables,
            selected_year,
            selected_group_column,
            grouped_counts=reusable_distribution_counts,
        ),
        "yearly_area_chart": _build_combined_impact_timeline_chart(
            selected_tables,
            selected_year,
            impact_timeline_rows=grouped_counts_bundle["impact_timeline_rows"],
        ),
        "monthly_profile": _build_monthly_profile_chart(
            selected_tables,
            selected_year,
            month_counts=grouped_counts_bundle["month_counts"],
        ),
        "area_buckets": (
            _build_area_buckets_chart_from_counts(area_bucket_counts)
            if area_bucket_counts is not None
            else _build_area_buckets_chart(selected_tables, selected_year)
        ),
    }


def _build_dashboard_widgets(
    selected_tables: list[dict[str, Any]],
    selected_year: Optional[int],
    grouped_counts_bundle: dict[str, Any],
) -> dict[str, Any]:
    widgets = _build_sql_widgets(
        selected_tables,
        selected_year,
        cause_counts=grouped_counts_bundle["cause_counts"],
        month_counts=grouped_counts_bundle["month_counts"],
        district_counts=grouped_counts_bundle["district_counts"],
    )
    district_counts = grouped_counts_bundle["district_counts"]
    if district_counts and not widgets.get("districts", {}).get("items"):
        widgets["districts"] = _build_sql_district_widget_from_counts(district_counts)
    return widgets


__all__ = [
    "_build_dashboard_widgets",
    "_build_damage_dashboard_charts",
    "_build_standard_dashboard_charts",
    "_can_reuse_distribution_counts",
]
