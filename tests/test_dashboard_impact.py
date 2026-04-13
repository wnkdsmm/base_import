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



class DashboardImpactOptimizationTests(unittest.TestCase):
    def test_sql_widgets_reuse_precomputed_cause_and_month_counts(self) -> None:
        with (
            patch("app.dashboard.impact._build_sql_district_widget", return_value={"items": []}),
            patch("app.dashboard.impact._collect_cause_counts", side_effect=AssertionError("cause counts should be reused")),
            patch("app.dashboard.impact._collect_month_counts", side_effect=AssertionError("month counts should be reused")),
        ):
            widgets = impact._build_sql_widgets(
                [],
                None,
                cause_counts={"cause_a": 5, "cause_b": 2},
                month_counts={1: 2, 7: 3},
            )

        self.assertEqual(widgets["causes"]["items"][0]["label"], "cause_a")
        self.assertEqual([item["value"] for item in widgets["seasons"]["items"]], [2, 3])

    def test_sql_widgets_reuse_precomputed_district_counts_without_sql(self) -> None:
        with patch(
            "app.dashboard.impact._build_sql_district_widget",
            side_effect=AssertionError("district counts should be reused"),
        ):
            widgets = impact._build_sql_widgets(
                [],
                None,
                district_counts={"district_a": 4, "district_b": 2},
            )

        self.assertEqual([item["label"] for item in widgets["districts"]["items"]], ["district_a", "district_b"])
        self.assertEqual([item["value"] for item in widgets["districts"]["items"]], [4, 2])

    def test_impact_timeline_collects_selected_tables_in_one_union_query(self) -> None:
        tables = [
            {"name": "fires_dashboard", "column_set": {DATE_COLUMN}, "years": [2024], "table_year": None},
            {"name": "fires_dashboard_next", "column_set": {DATE_COLUMN}, "years": [2024], "table_year": None},
        ]
        conn = _ImpactTimelineConnection()

        with patch("app.dashboard.impact.engine.connect", return_value=conn):
            chart = impact._build_combined_impact_timeline_chart(tables, 2024)

        self.assertEqual(len(conn.queries), 1)
        self.assertIn("UNION ALL", conn.queries[0])
        self.assertIn("impact_timeline_0", conn.queries[0])
        self.assertIn("impact_timeline_1", conn.queries[0])
        self.assertEqual(conn.params, [{"selected_year": 2024}])
        self.assertEqual(chart["items"][0]["value"], 15.0)

    def test_impact_timeline_reuses_grouped_bundle_rows_without_sql(self) -> None:
        with patch("app.dashboard.impact.engine.connect", side_effect=AssertionError("timeline rows should be reused")):
            chart = impact._build_combined_impact_timeline_chart(
                [],
                None,
                impact_timeline_rows=[
                    {
                        "date_value": date(2024, 1, 2),
                        "deaths": 1,
                        "injuries": 2,
                        "evacuated": 3,
                        "evacuated_children": 4,
                        "rescued_children": 5,
                    }
                ],
            )

        self.assertEqual(chart["items"][0]["date_value"], "2024-01-02")
        self.assertEqual(chart["items"][0]["value"], 15.0)

    def test_positive_column_counts_collects_selected_tables_in_one_union_query(self) -> None:
        tables = [
            {"name": "fires_dashboard", "column_set": {DATE_COLUMN, AREA_COLUMN}, "years": [2024], "table_year": None},
            {"name": "fires_dashboard_next", "column_set": {DATE_COLUMN, AREA_COLUMN}, "years": [2024], "table_year": None},
        ]
        conn = _PositiveColumnCountsConnection()

        with patch("app.dashboard.distribution.engine.connect", return_value=conn):
            counts = distribution._collect_positive_column_counts(tables, 2024, [AREA_COLUMN, "missing_metric"])

        self.assertEqual(len(conn.queries), 1)
        self.assertIn("UNION ALL", conn.queries[0])
        self.assertIn("positive_column_counts", conn.queries[0])
        self.assertEqual(conn.params, [{"selected_year": 2024}])
        self.assertEqual(counts, {AREA_COLUMN: 3, "missing_metric": 0})

    def test_grouped_bundle_collects_positive_counts_in_same_query(self) -> None:
        table = {
            "name": "fires_dashboard",
            "column_set": {DATE_COLUMN, GENERAL_CAUSE_COLUMN, REGISTERED_DAMAGE_COLUMN},
            "years": [2024],
            "table_year": None,
        }
        conn = _DashboardConnection()

        with patch("app.dashboard.impact.engine.connect", return_value=conn):
            bundle = impact._collect_dashboard_grouped_counts(
                [table],
                2024,
                GENERAL_CAUSE_COLUMN,
                positive_count_columns=[REGISTERED_DAMAGE_COLUMN, "missing_metric"],
            )

        self.assertEqual(len(conn.queries), 1)
        self.assertIn("GROUPING SETS", conn.queries[0])
        self.assertIn("positive_column_bundle", conn.queries[0])
        self.assertIn("GROUP BY GROUPING SETS", conn.queries[0])
        self.assertEqual(conn.params, [{"selected_year": 2024}])
        self.assertEqual(bundle["positive_column_counts"][REGISTERED_DAMAGE_COLUMN], 3)
        self.assertEqual(bundle["positive_column_counts"]["missing_metric"], 0)

    def test_dashboard_payload_and_widgets_do_not_emit_mojibake_tokens(self) -> None:
        payload = service._empty_dashboard_data()
        payload["widgets"] = impact._build_sql_widgets(
            [],
            None,
            cause_counts={"Не указано": 3},
            month_counts={1: 2, 7: 1},
            district_counts={"Не указано": 4},
        )
        payload["charts"]["area_buckets"] = impact._build_area_buckets_chart_from_counts(
            {"До 1 га": 2, "Не указано": 1}
        )

        offenders = [text for text in _iter_payload_strings(payload) if MOJIBAKE_TOKEN_RE.search(text)]

        self.assertEqual(offenders, [])
