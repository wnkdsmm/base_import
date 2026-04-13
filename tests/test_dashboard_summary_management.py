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



class DashboardSummaryManagementOptimizationTests(unittest.TestCase):
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

    def test_summary_bundle_collects_summary_and_yearly_rows_in_one_sql_pass(self) -> None:
        table = {
            "name": "fires_dashboard",
            "column_set": {DATE_COLUMN, AREA_COLUMN},
            "years": [2023, 2024],
            "table_year": None,
        }
        second_table = {
            "name": "fires_dashboard_next",
            "column_set": {DATE_COLUMN, AREA_COLUMN},
            "years": [2024],
            "table_year": None,
        }
        conn = _SummaryBundleConnection()

        with patch("app.dashboard.summary.engine.connect", return_value=conn):
            bundle = summary._collect_dashboard_summary_bundle([table, second_table], 2024)

        self.assertEqual(len(conn.queries), 1)
        self.assertIn("AS year_value", conn.queries[0])
        self.assertIn("GROUP BY year_value", conn.queries[0])
        self.assertIn("UNION ALL", conn.queries[0])
        self.assertIn("fires_dashboard_next", conn.queries[0])
        self.assertEqual(conn.params, [{}])
        self.assertEqual(bundle["summary_rows"][0]["fire_count"], 3)
        self.assertEqual(bundle["summary_rows"][1]["table_name"], "fires_dashboard_next")
        self.assertEqual(bundle["summary_rows"][1]["fire_count"], 0)
        self.assertEqual(bundle["yearly_grouped"][2024]["count"], 3.0)
