from contextlib import ExitStack
import unittest
from unittest.mock import patch

from app.dashboard import distribution, impact, management, service, utils as dashboard_utils
from app.statistics_constants import GENERAL_CAUSE_COLUMN


class DashboardAnalyticsOptimizationTests(unittest.TestCase):
    def test_management_snapshot_uses_lightweight_decision_support_payload(self) -> None:
        risk_payload = {
            "quality_passport": {
                "validation_summary": "Паспорт качества собран.",
                "confidence_label": "Рабочая",
                "confidence_score_display": "72 / 100",
                "confidence_tone": "sky",
            },
            "territories": [],
            "notes": ["Базовая заметка."],
            "top_territory_label": "-",
            "top_territory_explanation": "Недостаточно данных.",
            "top_territory_confidence_label": "Рабочая",
            "top_territory_confidence_score_display": "72 / 100",
            "top_territory_confidence_tone": "sky",
            "top_territory_confidence_note": "Паспорт качества собран.",
        }
        with patch("app.dashboard.management.build_decision_support_payload", return_value=risk_payload) as payload_mock:
            snapshot = management._build_management_snapshot(
                selected_tables=[{"name": "fires"}],
                selected_year=2024,
                summary={},
                trend={"direction": "flat", "description": "", "delta_display": "0"},
                cause_overview={"items": []},
                district_widget={"items": []},
            )

        payload_mock.assert_called_once()
        self.assertFalse(payload_mock.call_args.kwargs["include_geo_prediction"])
        self.assertFalse(payload_mock.call_args.kwargs["include_historical_validation"])
        self.assertIn("brief", snapshot)
        self.assertIn("summary_line", snapshot)

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

    def test_select_tables_excludes_benchmark_prefix(self) -> None:
        selected = dashboard_utils._select_tables(
            ["fires_2024", "benchmark_fire_perf_20000_20260403_083825", "tmp_stage_table"]
        )

        self.assertEqual(selected, ["fires_2024"])

    def test_service_reuses_grouped_counts_bundle_for_distribution_widgets_and_area_buckets(self) -> None:
        metadata = {
            "tables": [{"name": "fires", "column_set": {GENERAL_CAUSE_COLUMN}, "years": [2024], "table_year": None}],
            "table_options": [{"value": "all", "label": "all"}, {"value": "fires", "label": "fires"}],
            "default_group_column": GENERAL_CAUSE_COLUMN,
            "errors": [],
        }
        filter_state = {
            "selected_tables": metadata["tables"],
            "available_years": [{"value": "2024", "label": "2024"}],
            "selected_year": 2024,
            "available_group_columns": [{"value": GENERAL_CAUSE_COLUMN, "label": GENERAL_CAUSE_COLUMN}],
            "selected_group_column": GENERAL_CAUSE_COLUMN,
            "selected_table_name": "fires",
        }
        empty_chart = {"items": []}
        widgets = {"districts": {"items": []}}
        grouped_counts_bundle = {
            "cause_counts": {"cause_a": 5},
            "month_counts": {1: 2, 7: 3},
            "district_counts": {"district_a": 4},
            "area_bucket_counts": {"До 1 га": 6},
        }

        with ExitStack() as stack:
            stack.enter_context(patch("app.dashboard.service._collect_dashboard_metadata_cached", return_value=metadata))
            stack.enter_context(patch("app.dashboard.service._resolve_dashboard_filters", return_value=filter_state))
            stack.enter_context(patch("app.dashboard.service._get_dashboard_cache", return_value=None))
            stack.enter_context(patch("app.dashboard.service._set_dashboard_cache"))
            stack.enter_context(patch("app.dashboard.service._collect_summary_table_rows", return_value=[]))
            stack.enter_context(patch("app.dashboard.service._build_summary", return_value={"fires_count": 0}))
            stack.enter_context(patch("app.dashboard.service._build_yearly_chart", return_value=empty_chart))
            stack.enter_context(patch("app.dashboard.service._build_table_breakdown_chart", return_value=empty_chart))
            stack.enter_context(
                patch("app.dashboard.service._collect_dashboard_grouped_counts", return_value=grouped_counts_bundle)
            )
            stack.enter_context(
                patch(
                    "app.dashboard.service._collect_cause_counts",
                    side_effect=AssertionError("cause counts should come from grouped bundle"),
                )
            )
            stack.enter_context(patch("app.dashboard.service._build_cause_chart", return_value=empty_chart))
            stack.enter_context(
                patch(
                    "app.dashboard.service._collect_month_counts",
                    side_effect=AssertionError("month counts should come from grouped bundle"),
                )
            )
            distribution_mock = stack.enter_context(
                patch("app.dashboard.service._build_distribution_chart", return_value=empty_chart)
            )
            stack.enter_context(patch("app.dashboard.service._build_combined_impact_timeline_chart", return_value=empty_chart))
            stack.enter_context(patch("app.dashboard.service._build_monthly_profile_chart", return_value=empty_chart))
            stack.enter_context(
                patch(
                    "app.dashboard.service._build_area_buckets_chart",
                    side_effect=AssertionError("area buckets should come from grouped bundle"),
                )
            )
            area_buckets_mock = stack.enter_context(
                patch("app.dashboard.service._build_area_buckets_chart_from_counts", return_value=empty_chart)
            )
            stack.enter_context(patch("app.dashboard.service._build_trend", return_value={}))
            stack.enter_context(patch("app.dashboard.service._build_rankings", return_value={}))
            stack.enter_context(patch("app.dashboard.service._build_highlights", return_value=[]))
            sql_widgets_mock = stack.enter_context(patch("app.dashboard.service._build_sql_widgets", return_value=widgets))
            stack.enter_context(patch("app.dashboard.service._build_management_snapshot", return_value={}))
            stack.enter_context(
                patch(
                    "app.dashboard.service._build_scope",
                    return_value={"table_label": "fires", "year_label": "2024", "group_label": "cause"},
                )
            )
            stack.enter_context(patch("app.dashboard.service.compose_executive_brief_text", return_value="brief"))
            service.get_dashboard_data(table_name="fires", year="2024", group_column=GENERAL_CAUSE_COLUMN, allow_fallback=False)

        self.assertEqual(distribution_mock.call_args.kwargs["grouped_counts"], {"cause_a": 5})
        self.assertEqual(area_buckets_mock.call_args.args[0], {"До 1 га": 6})
        self.assertEqual(sql_widgets_mock.call_args.kwargs["cause_counts"], {"cause_a": 5})
        self.assertEqual(sql_widgets_mock.call_args.kwargs["month_counts"], {1: 2, 7: 3})
        self.assertEqual(sql_widgets_mock.call_args.kwargs["district_counts"], {"district_a": 4})


if __name__ == "__main__":
    unittest.main()
