from contextlib import ExitStack
from datetime import date
import unittest
from unittest.mock import patch

from app.dashboard import distribution, impact, management, service, summary, utils as dashboard_utils
from app.dashboard.data_access import DISTRICT_COLUMN_CANDIDATES
from app.statistics_constants import (
    AREA_COLUMN,
    BUILDING_CATEGORY_COLUMN,
    BUILDING_CAUSE_COLUMN,
    DAMAGE_GROUP_OPTION_VALUE,
    DATE_COLUMN,
    GENERAL_CAUSE_COLUMN,
    REGISTERED_DAMAGE_COLUMN,
)
from tests.dashboard_analytics_test_helpers import (
    MOJIBAKE_TOKEN_RE,
    _DashboardAggregationEngine,
    _DashboardConnection,
    _FailingDashboardEngine,
    _ImpactTimelineConnection,
    _PositiveColumnCountsConnection,
    _SummaryBundleConnection,
    _iter_payload_strings,
)



class DashboardDamageOptimizationTests(unittest.TestCase):
    def test_service_reuses_grouped_positive_counts_for_damage_dashboard(self) -> None:
        metadata = {
            "tables": [
                {
                    "name": "fires",
                    "column_set": {GENERAL_CAUSE_COLUMN, REGISTERED_DAMAGE_COLUMN},
                    "years": [2024],
                    "table_year": None,
                }
            ],
            "table_options": [{"value": "all", "label": "all"}, {"value": "fires", "label": "fires"}],
            "default_group_column": GENERAL_CAUSE_COLUMN,
            "errors": [],
        }
        empty_chart = {"items": []}
        grouped_counts_bundle = {
            "cause_counts": {"cause_a": 5},
            "distribution_counts": {},
            "month_counts": {1: 2},
            "district_counts": {"district_a": 4},
            "area_bucket_counts": {},
            "positive_column_counts": {REGISTERED_DAMAGE_COLUMN: 3},
            "impact_timeline_rows": [],
        }

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "app.dashboard.service._build_dashboard_summary_series",
                    return_value={
                        "summary": {"fires_count": 3, "year_label": "2024", "tables_used": 1, "tables_used_display": "1"},
                        "yearly_fires_series": {"items": []},
                        "table_breakdown_series": {"items": []},
                    },
                )
            )
            grouped_counts_mock = stack.enter_context(
                patch("app.dashboard.service._collect_dashboard_grouped_counts", return_value=grouped_counts_bundle)
            )
            stack.enter_context(patch("app.dashboard.service._damage_count_columns", return_value=[REGISTERED_DAMAGE_COLUMN]))
            stack.enter_context(
                patch(
                    "app.dashboard.service._collect_damage_counts",
                    side_effect=AssertionError("damage counts should come from grouped bundle"),
                )
            )
            stack.enter_context(
                patch(
                    "app.dashboard.distribution._collect_damage_counts",
                    side_effect=AssertionError("damage chart helpers should reuse provided items"),
                )
            )
            stack.enter_context(patch("app.dashboard.service._build_cause_chart", return_value=empty_chart))
            stack.enter_context(patch("app.dashboard.service._build_trend", return_value={}))
            stack.enter_context(patch("app.dashboard.service._build_rankings", return_value={}))
            stack.enter_context(patch("app.dashboard.service._build_highlights", return_value=[]))
            stack.enter_context(patch("app.dashboard.service._build_dashboard_widgets", return_value={"districts": {"items": []}}))
            stack.enter_context(patch("app.dashboard.service._build_management_snapshot", return_value={}))
            stack.enter_context(
                patch(
                    "app.dashboard.service._build_scope",
                    return_value={"table_label": "fires", "year_label": "2024", "group_label": "damage"},
                )
            )

            aggregation = service._build_dashboard_aggregation(
                metadata=metadata,
                selected_tables=metadata["tables"],
                selected_year=2024,
                selected_group_column=DAMAGE_GROUP_OPTION_VALUE,
                selected_table_name="fires",
                available_years=[{"value": "2024", "label": "2024"}],
                available_group_columns=[{"value": DAMAGE_GROUP_OPTION_VALUE, "label": "damage"}],
            )

        grouped_counts_mock.assert_called_once_with(
            metadata["tables"],
            2024,
            DAMAGE_GROUP_OPTION_VALUE,
            include_area_buckets=False,
            include_impact_timeline=False,
            positive_count_columns=[REGISTERED_DAMAGE_COLUMN],
        )
        self.assertEqual(aggregation["distribution"]["items"][0]["value"], 3)

    def test_damage_dashboard_empty_grouped_counts_still_skip_fallback_sql(self) -> None:
        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "app.dashboard.service._collect_damage_counts",
                    side_effect=AssertionError("empty grouped damage counts should not trigger fallback SQL"),
                )
            )
            stack.enter_context(
                patch(
                    "app.dashboard.distribution._collect_damage_counts",
                    side_effect=AssertionError("damage chart helpers should reuse explicit grouped counts"),
                )
            )

            charts = service._build_damage_dashboard_charts(
                [],
                None,
                damage_counts={},
            )

        self.assertEqual(charts["distribution"]["items"], [])
        self.assertEqual(charts["yearly_area_chart"]["items"], [])
        self.assertEqual(charts["monthly_profile"]["items"], [])
        self.assertEqual(charts["area_buckets"]["items"], [])

    def test_damage_dashboard_aggregation_uses_two_sql_queries(self) -> None:
        table = {
            "name": "fires",
            "column_set": {DATE_COLUMN, GENERAL_CAUSE_COLUMN, REGISTERED_DAMAGE_COLUMN},
            "years": [2024],
            "table_year": None,
        }
        metadata = {
            "tables": [table],
            "table_options": [{"value": "all", "label": "all"}, {"value": "fires", "label": "fires"}],
            "default_group_column": GENERAL_CAUSE_COLUMN,
            "errors": [],
        }
        engine = _DashboardAggregationEngine()

        with ExitStack() as stack:
            stack.enter_context(patch("app.dashboard.summary.engine", engine))
            stack.enter_context(patch("app.dashboard.impact.engine", engine))
            stack.enter_context(patch("app.dashboard.distribution.engine", _FailingDashboardEngine()))
            stack.enter_context(patch("app.dashboard.service._damage_count_columns", return_value=[REGISTERED_DAMAGE_COLUMN]))
            stack.enter_context(
                patch(
                    "app.dashboard.service._collect_damage_counts",
                    side_effect=AssertionError("damage counts should come from grouped bundle"),
                )
            )
            stack.enter_context(
                patch(
                    "app.dashboard.distribution._collect_damage_counts",
                    side_effect=AssertionError("damage chart helpers should reuse provided items"),
                )
            )
            stack.enter_context(patch("app.dashboard.service._build_management_snapshot", return_value={}))

            aggregation = service._build_dashboard_aggregation(
                metadata=metadata,
                selected_tables=[table],
                selected_year=2024,
                selected_group_column=DAMAGE_GROUP_OPTION_VALUE,
                selected_table_name="fires",
                available_years=[{"value": "2024", "label": "2024"}],
                available_group_columns=[{"value": DAMAGE_GROUP_OPTION_VALUE, "label": "damage"}],
            )

        self.assertEqual(len(engine.connection.queries), 2)
        self.assertIn("AS table_name", engine.connection.queries[0])
        self.assertIn("GROUPING SETS", engine.connection.queries[1])
        self.assertIn("positive_column_bundle", engine.connection.queries[1])
        self.assertEqual(engine.connection.params, [{}, {"selected_year": 2024}])
        self.assertEqual(aggregation["distribution"]["items"][0]["value"], 3)
        self.assertEqual(aggregation["widgets"]["causes"]["items"][0]["value"], 3)
