from __future__ import annotations

from typing import Optional

from .distribution import _build_rankings, _build_table_breakdown_chart
from .summary import (
    _build_highlights,
    _build_scope,
    _build_summary,
    _build_trend,
    _build_yearly_chart,
    _collect_dashboard_summary_bundle,
)
from .types import (
    DashboardMetadata,
    DashboardOption,
    DashboardSection,
    DashboardSummaryMetrics,
    DashboardSummarySeries,
    DashboardTableRef,
    DistributionResult,
    SummaryResult,
)
from .utils import _find_option_label


def _build_dashboard_summary_series(
    selected_tables: list[DashboardTableRef],
    selected_year: Optional[int],
) -> DashboardSummarySeries:
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
    summary: SummaryResult,
    yearly_fires_series: DistributionResult,
    table_breakdown_series: DistributionResult,
    distribution: DistributionResult,
    cause_overview: DistributionResult,
) -> DashboardSummaryMetrics:
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
    summary: SummaryResult,
    metadata: DashboardMetadata,
    selected_table_name: str,
    selected_group_column: str,
    available_group_columns: list[DashboardOption],
    available_years: list[DashboardOption],
) -> DashboardSection:
    return _build_scope(
        summary=summary,
        metadata=metadata,
        selected_table_label=_find_option_label(metadata["table_options"], selected_table_name, "Р’СЃРµ С‚Р°Р±Р»РёС†С‹"),
        selected_group_label=_find_option_label(available_group_columns, selected_group_column, "РќРµС‚ РґРѕСЃС‚СѓРїРЅС‹С… РєРѕР»РѕРЅРѕРє"),
        available_years=available_years,
    )


__all__ = [
    "_build_dashboard_scope",
    "_build_dashboard_summary_metrics",
    "_build_dashboard_summary_series",
]
