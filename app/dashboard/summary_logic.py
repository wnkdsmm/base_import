from __future__ import annotations

from typing import Any, Dict, Optional

from .distribution import _build_rankings, _build_table_breakdown_chart
from .summary import (
    _build_highlights,
    _build_scope,
    _build_summary,
    _build_trend,
    _build_yearly_chart,
    _collect_dashboard_summary_bundle,
)
from .utils import _find_option_label


def _build_dashboard_summary_series(
    selected_tables: list[dict[str, Any]],
    selected_year: Optional[int],
) -> Dict[str, Any]:
    summary_bundle = _collect_dashboard_summary_bundle(selected_tables, selected_year)
    summary_rows = summary_bundle["summary_rows"]
    return {
        "summary": _build_summary(selected_tables, selected_year, summary_rows=summary_rows),
        "yearly_fires_series": _build_yearly_chart(
            selected_tables,
            metric="count",
            yearly_grouped=summary_bundle["yearly_grouped"],
            include_plotly=False,
        ),
        "table_breakdown_series": _build_table_breakdown_chart(
            selected_tables,
            selected_year,
            summary_rows=summary_rows,
            include_plotly=False,
        ),
    }


def _build_dashboard_summary_metrics(
    *,
    summary: Dict[str, Any],
    yearly_fires_series: Dict[str, Any],
    table_breakdown_series: Dict[str, Any],
    distribution: Dict[str, Any],
    cause_overview: Dict[str, Any],
) -> Dict[str, Any]:
    trend = _build_trend(yearly_fires_series)
    rankings = _build_rankings(distribution, table_breakdown_series, yearly_fires_series)
    highlights = _build_highlights(summary, yearly_fires_series, cause_overview)
    return {
        "trend": trend,
        "rankings": rankings,
        "highlights": highlights,
    }


def _build_dashboard_scope(
    *,
    summary: Dict[str, Any],
    metadata: Dict[str, Any],
    selected_table_name: str,
    selected_group_column: str,
    available_group_columns: list[Dict[str, Any]],
    available_years: list[Dict[str, Any]],
) -> Dict[str, Any]:
    return _build_scope(
        summary=summary,
        metadata=metadata,
        selected_table_label=_find_option_label(metadata["table_options"], selected_table_name, "Все таблицы"),
        selected_group_label=_find_option_label(available_group_columns, selected_group_column, "Нет доступных колонок"),
        available_years=available_years,
    )


__all__ = [
    "_build_dashboard_scope",
    "_build_dashboard_summary_metrics",
    "_build_dashboard_summary_series",
]
