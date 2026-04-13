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



class DashboardDistributionOptimizationTests(unittest.TestCase):
    def test_table_breakdown_reuses_summary_rows_without_extra_sql(self) -> None:
        with patch("app.dashboard.distribution.engine.connect", side_effect=AssertionError("summary rows should avoid extra SQL")):
            chart = distribution._build_table_breakdown_chart(
                [],
                None,
                summary_rows=[
                    {"table_name": "fires_a", "fire_count": 7},
                    {"table_name": "fires_b", "fire_count": 3},
                ],
            )

        self.assertEqual([item["label"] for item in chart["items"]], ["fires_a", "fires_b"])
        self.assertEqual([item["value"] for item in chart["items"]], [7, 3])

    def test_distribution_chart_reuses_grouped_counts_without_sql(self) -> None:
        with patch("app.dashboard.distribution.engine.connect", side_effect=AssertionError("grouped counts should avoid extra SQL")):
            chart = distribution._build_distribution_chart(
                [],
                None,
                GENERAL_CAUSE_COLUMN,
                grouped_counts={"cause_a": 7, "cause_b": 3},
            )

        self.assertEqual([item["label"] for item in chart["items"]], ["cause_a", "cause_b"])
        self.assertEqual([item["value"] for item in chart["items"]], [7, 3])

    def test_collect_dashboard_grouped_counts_builds_real_bundle_for_cause_and_non_cause(self) -> None:
        table = {
            "name": "fires_dashboard",
            "column_set": {
                DATE_COLUMN,
                GENERAL_CAUSE_COLUMN,
                BUILDING_CATEGORY_COLUMN,
                DISTRICT_COLUMN_CANDIDATES[0],
                AREA_COLUMN,
            },
            "years": [2024],
            "table_year": None,
        }

        cause_conn = _DashboardConnection()
        with patch("app.dashboard.impact.engine.connect", return_value=cause_conn):
            cause_bundle = impact._collect_dashboard_grouped_counts(
                [table, {**table, "name": "fires_dashboard_next"}],
                2024,
                GENERAL_CAUSE_COLUMN,
            )

        self.assertEqual(cause_bundle["cause_counts"], {"Электрика": 2, "Неосторожность": 1})
        self.assertEqual(cause_bundle["distribution_counts"], cause_bundle["cause_counts"])
        self.assertEqual(cause_bundle["district_counts"], {"Центральный": 2, "Северный": 1})
        self.assertEqual(cause_bundle["month_counts"], {1: 2, 2: 1})
        self.assertEqual(cause_bundle["area_bucket_counts"], {"До 1 га": 1, "5-20 га": 1, "Не указано": 1})
        self.assertEqual(cause_bundle["impact_timeline_rows"][0]["date_value"], date(2024, 1, 2))
        self.assertEqual(cause_conn.params, [{"selected_year": 2024}])
        self.assertEqual(len(cause_conn.queries), 1)
        self.assertIn("fires_dashboard_next", cause_conn.queries[0])
        self.assertIn("GROUPING SETS", cause_conn.queries[0])
        for metric_kind in ("'cause'", "'district'", "'month'", "'area_bucket'", "'impact_timeline'"):
            self.assertIn(metric_kind, cause_conn.queries[0])
        self.assertNotIn("'distribution'", cause_conn.queries[0])
        self.assertTrue(service._can_reuse_distribution_counts([table], GENERAL_CAUSE_COLUMN))

        non_cause_conn = _DashboardConnection()
        with patch("app.dashboard.impact.engine.connect", return_value=non_cause_conn):
            non_cause_bundle = impact._collect_dashboard_grouped_counts([table], 2024, BUILDING_CATEGORY_COLUMN)

        self.assertEqual(non_cause_bundle["cause_counts"], {"Электрика": 2, "Неосторожность": 1})
        self.assertEqual(non_cause_bundle["distribution_counts"], {"Жилое": 2, "Склад": 1})
        self.assertEqual(non_cause_bundle["district_counts"], {"Центральный": 2, "Северный": 1})
        self.assertEqual(non_cause_bundle["month_counts"], {1: 2, 2: 1})
        self.assertEqual(non_cause_bundle["area_bucket_counts"], {"До 1 га": 1, "5-20 га": 1, "Не указано": 1})
        self.assertEqual(non_cause_bundle["impact_timeline_rows"][0]["deaths"], 1)
        self.assertEqual(non_cause_conn.params, [{"selected_year": 2024}])
        self.assertIn("GROUPING SETS", non_cause_conn.queries[0])
        for metric_kind in ("'cause'", "'distribution'", "'district'", "'month'", "'area_bucket'", "'impact_timeline'"):
            self.assertIn(metric_kind, non_cause_conn.queries[0])
        self.assertIn("'distribution'", non_cause_conn.queries[0])
        self.assertTrue(
            service._can_reuse_distribution_counts(
                [table],
                BUILDING_CATEGORY_COLUMN,
            )
        )

        trimmed_conn = _DashboardConnection()
        with patch("app.dashboard.impact.engine.connect", return_value=trimmed_conn):
            trimmed_bundle = impact._collect_dashboard_grouped_counts(
                [table],
                2024,
                BUILDING_CATEGORY_COLUMN,
                include_area_buckets=False,
                include_impact_timeline=False,
            )

        self.assertEqual(trimmed_bundle["area_bucket_counts"], {})
        self.assertEqual(trimmed_bundle["impact_timeline_rows"], [])
        self.assertNotIn("'area_bucket'", trimmed_conn.queries[0])
        self.assertNotIn("'impact_timeline'", trimmed_conn.queries[0])

    def test_distribution_fallback_keeps_sql_path_and_rejects_unsupported_grouped_counts(self) -> None:
        supported_table = {
            "name": "fires_dashboard",
            "column_set": {DATE_COLUMN, BUILDING_CATEGORY_COLUMN},
            "years": [2024],
            "table_year": None,
        }
        unsupported_table = {
            "name": "fires_without_category",
            "column_set": {DATE_COLUMN, GENERAL_CAUSE_COLUMN},
            "years": [2024],
            "table_year": None,
        }

        self.assertFalse(
            service._can_reuse_distribution_counts(
                [unsupported_table],
                BUILDING_CATEGORY_COLUMN,
            )
        )
        self.assertFalse(
            service._can_reuse_distribution_counts(
                [
                    {
                        "name": "fires_with_other_cause",
                        "column_set": {DATE_COLUMN, BUILDING_CAUSE_COLUMN},
                        "years": [2024],
                        "table_year": None,
                    }
                ],
                GENERAL_CAUSE_COLUMN,
            )
        )

        supported_conn = _DashboardConnection()
        with patch("app.dashboard.distribution.engine.connect", return_value=supported_conn):
            chart = distribution._build_distribution_chart([supported_table], 2024, BUILDING_CATEGORY_COLUMN)

        self.assertEqual([item["label"] for item in chart["items"]], ["Жилое", "Склад"])
        self.assertEqual([item["value"] for item in chart["items"]], [2, 1])
        self.assertEqual(supported_conn.params, [{"selected_year": 2024}])
        self.assertTrue(any("GROUP BY label" in query for query in supported_conn.queries))

        unsupported_conn = _DashboardConnection(distribution_rows=[{"label": "wrong", "fire_count": 99}])
        with patch("app.dashboard.distribution.engine.connect", return_value=unsupported_conn):
            chart = distribution._build_distribution_chart([unsupported_table], 2024, BUILDING_CATEGORY_COLUMN)

        self.assertEqual(chart["items"], [])
        self.assertEqual(unsupported_conn.queries, [])

    def test_select_tables_excludes_benchmark_prefix(self) -> None:
        selected = dashboard_utils._select_tables(
            ["fires_2024", "benchmark_fire_perf_20000_20260403_083825", "tmp_stage_table"]
        )

        self.assertEqual(selected, ["fires_2024"])

    def test_standard_dashboard_charts_reuse_empty_grouped_distribution_counts_without_sql(self) -> None:
        selected_tables = [
            {
                "name": "fires",
                "column_set": {BUILDING_CATEGORY_COLUMN, DATE_COLUMN, AREA_COLUMN},
                "years": [2024],
                "table_year": None,
            }
        ]

        with patch(
            "app.dashboard.distribution.engine.connect",
            side_effect=AssertionError("empty grouped distribution counts should not trigger fallback SQL"),
        ):
            charts = service._build_standard_dashboard_charts(
                selected_tables,
                2024,
                BUILDING_CATEGORY_COLUMN,
                {
                    "distribution_counts": {},
                    "impact_timeline_rows": [],
                    "month_counts": {},
                    "area_bucket_counts": {},
                },
            )

        self.assertEqual(charts["distribution"]["items"], [])
        self.assertEqual(charts["monthly_profile"]["items"], [])
        self.assertEqual(charts["area_buckets"]["items"], [])

    def test_service_reuses_empty_distribution_counts_but_rejects_unsupported_grouped_counts(self) -> None:
        self.assertTrue(
            service._can_reuse_distribution_counts(
                [{"name": "fires", "column_set": {BUILDING_CATEGORY_COLUMN}}],
                BUILDING_CATEGORY_COLUMN,
            )
        )
        self.assertFalse(
            service._can_reuse_distribution_counts(
                [{"name": "fires", "column_set": {GENERAL_CAUSE_COLUMN}}],
                BUILDING_CATEGORY_COLUMN,
            )
        )
        self.assertFalse(
            service._can_reuse_distribution_counts(
                [{"name": "fires", "column_set": {GENERAL_CAUSE_COLUMN}}],
                BUILDING_CATEGORY_COLUMN,
            )
        )
