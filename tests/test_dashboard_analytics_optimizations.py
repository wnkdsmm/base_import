import unittest
from unittest.mock import patch

from app.dashboard import distribution, impact, management


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


if __name__ == "__main__":
    unittest.main()
