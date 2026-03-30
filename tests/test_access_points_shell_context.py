import unittest
from unittest.mock import patch

from app.services.access_points import core


class AccessPointsShellContextTests(unittest.TestCase):
    def test_shell_context_prefers_cached_payload_when_available(self) -> None:
        table_options = [{"value": "fires", "label": "Fires"}]
        cached_payload = {
            "bootstrap_mode": "resolved",
            "has_data": True,
            "filters": {
                "table_name": "fires",
                "available_tables": table_options,
            },
            "summary": {
                "selected_table_label": "Fires",
                "total_points_display": "12",
            },
        }

        with (
            patch.object(core, "_build_access_points_table_options", return_value=table_options),
            patch.object(core, "_resolve_selected_table", return_value="fires"),
            patch.object(core, "_selected_source_tables", return_value=["fires"]),
            patch.object(core, "_parse_limit", return_value=25),
            patch.object(core._ACCESS_POINTS_CACHE, "get", return_value=cached_payload),
        ):
            context = core.get_access_points_shell_context(table_name="fires")

        self.assertEqual(context["initial_data"], cached_payload)
        self.assertTrue(context["has_data"])
        self.assertEqual(context["plotly_js"], "")

    def test_shell_context_returns_deferred_payload(self) -> None:
        table_options = [{"value": "fires", "label": "Fires"}]

        with (
            patch.object(core, "_build_access_points_table_options", return_value=table_options),
            patch.object(core, "_resolve_selected_table", return_value="fires"),
            patch.object(core, "_selected_source_tables", return_value=["fires"]),
            patch.object(core, "_parse_limit", return_value=50),
            patch.object(core._ACCESS_POINTS_CACHE, "get", return_value=None),
        ):
            context = core.get_access_points_shell_context(
                table_name="fires",
                district="North",
                year="2025",
                limit="50",
            )

        data = context["initial_data"]
        self.assertEqual(context["plotly_js"], "")
        self.assertEqual(data["bootstrap_mode"], "deferred")
        self.assertTrue(data["loading"])
        self.assertFalse(data["has_data"])
        self.assertEqual(data["filters"]["table_name"], "fires")
        self.assertEqual(data["filters"]["district"], "North")
        self.assertEqual(data["filters"]["year"], "2025")
        self.assertEqual(data["filters"]["limit"], "50")
        self.assertEqual(data["filters"]["feature_columns"], [])
        self.assertTrue(data["filters"]["available_features"])
        self.assertTrue(all(item["is_selected"] for item in data["filters"]["available_features"]))
        self.assertEqual(data["filters"]["available_tables"], table_options)
        self.assertEqual(data["filters"]["available_districts"][1]["value"], "North")
        self.assertEqual(data["filters"]["available_years"][1]["value"], "2025")
        self.assertIn("shell", data["notes"][0].lower())

    def test_shell_context_preserves_selected_features_on_reload(self) -> None:
        table_options = [{"value": "fires", "label": "Fires"}]

        with (
            patch.object(core, "_build_access_points_table_options", return_value=table_options),
            patch.object(core, "_resolve_selected_table", return_value="fires"),
            patch.object(core, "_selected_source_tables", return_value=["fires"]),
            patch.object(core, "_parse_limit", return_value=25),
            patch.object(core._ACCESS_POINTS_CACHE, "get", return_value=None),
        ):
            context = core.get_access_points_shell_context(
                table_name="fires",
                feature_columns=["NO_WATER", "RESPONSE_TIME"],
            )

        data = context["initial_data"]
        self.assertEqual(data["filters"]["feature_columns"], ["NO_WATER", "RESPONSE_TIME"])
        selected = [item["name"] for item in data["filters"]["available_features"] if item.get("is_selected")]
        self.assertEqual(set(selected), {"NO_WATER", "RESPONSE_TIME"})

    def test_resolved_payload_contains_explainable_sections(self) -> None:
        table_options = [{"value": "fires", "label": "Fires"}]
        rows = [
            {
                "rank": 1,
                "label": "Point A",
                "entity_type": "address",
                "district": "District A",
                "location_hint": "Central 1",
                "coordinates_display": "",
                "latitude": float("nan"),
                "longitude": float("nan"),
                "score": 62.4,
                "score_display": "62.4",
                "total_score": 62.4,
                "total_score_display": "62.4",
                "severity_band": "высокий",
                "severity_band_code": "high",
                "priority_label": "Повышенный приоритет",
                "tone": "warning",
                "typology_code": "access",
                "typology_label": "Дальний выезд",
                "incident_count": 4,
                "incident_count_display": "4",
                "average_distance_display": "18 км",
                "average_response_display": "27 мин",
                "no_water_share_display": "25%",
                "water_unknown_share_display": "15%",
                "completeness_display": "62%",
                "completeness_share": 0.62,
                "access_score": 65.0,
                "water_score": 30.0,
                "severity_score": 50.0,
                "recurrence_score": 40.0,
                "data_gap_score": 48.0,
                "uncertainty_penalty": 3.2,
                "uncertainty_flag": True,
                "missing_data_priority": True,
                "investigation_score": 60.0,
                "investigation_score_display": "60.0",
                "low_support": False,
                "top_reason_codes": ["DISTANCE_TO_STATION", "RESPONSE_TIME"],
                "reason_details": [
                    {
                        "code": "DISTANCE_TO_STATION",
                        "label": "Удалённость до ПЧ",
                        "contribution_points": 12.4,
                        "contribution_display": "+12.4",
                        "value_display": "18 км",
                    }
                ],
                "reason_chips": ["Удалённость до ПЧ: +12.4"],
                "human_readable_explanation": "Point A has high risk because of poor access.",
                "explanation": "Point A has high risk because of poor access.",
                "granularity_rank": 1,
            },
            {
                "rank": 2,
                "label": "Point B",
                "entity_type": "settlement",
                "district": "District B",
                "location_hint": "Base",
                "coordinates_display": "",
                "score": 18.0,
                "score_display": "18.0",
                "total_score": 18.0,
                "total_score_display": "18.0",
                "severity_band": "низкий",
                "severity_band_code": "low",
                "priority_label": "Контроль",
                "tone": "normal",
                "typology_code": "mixed",
                "typology_label": "Комбинированный риск",
                "incident_count": 2,
                "incident_count_display": "2",
                "average_distance_display": "3 км",
                "average_response_display": "9 мин",
                "no_water_share_display": "0%",
                "water_unknown_share_display": "0%",
                "completeness_display": "100%",
                "completeness_share": 1.0,
                "access_score": 12.0,
                "water_score": 0.0,
                "severity_score": 10.0,
                "recurrence_score": 15.0,
                "data_gap_score": 0.0,
                "uncertainty_penalty": 0.0,
                "uncertainty_flag": False,
                "missing_data_priority": False,
                "investigation_score": 12.0,
                "investigation_score_display": "12.0",
                "low_support": False,
                "top_reason_codes": ["REPEAT_FIRES"],
                "reason_details": [
                    {
                        "code": "REPEAT_FIRES",
                        "label": "Повторяемость пожаров",
                        "contribution_points": 4.1,
                        "contribution_display": "+4.1",
                        "value_display": "2 в год",
                    }
                ],
                "reason_chips": ["Повторяемость пожаров: +4.1"],
                "human_readable_explanation": "Point B is low risk.",
                "explanation": "Point B is low risk.",
                "granularity_rank": 2,
            },
        ]
        dataset = {
            "entity_frame": None,
            "feature_frame": None,
            "total_incidents": 6,
            "notes": ["dataset ready"],
        }

        with (
            patch.object(core, "_build_access_points_table_options", return_value=table_options),
            patch.object(core, "_resolve_selected_table", return_value="fires"),
            patch.object(core, "_selected_source_tables", return_value=["fires"]),
            patch.object(core, "_parse_limit", return_value=25),
            patch.object(core._ACCESS_POINTS_CACHE, "get", return_value=None),
            patch.object(core._ACCESS_POINTS_CACHE, "set", side_effect=lambda key, value: value),
            patch.object(core, "_collect_access_point_metadata", return_value=([], [])),
            patch.object(
                core,
                "_build_option_catalog",
                return_value={
                    "districts": [{"value": "all", "label": "All districts"}],
                    "years": [{"value": "all", "label": "All years"}],
                },
            ),
            patch.object(core, "_resolve_option_value", return_value="all"),
            patch.object(core, "_load_access_point_dataset", return_value=dataset),
            patch.object(
                core,
                "_build_access_point_candidate_features",
                return_value=[
                    {
                        "name": "NO_WATER",
                        "label": "No water",
                        "description": "Water gap",
                        "coverage_display": "100%",
                        "variance_display": "0.4",
                    }
                ],
            ),
            patch.object(core, "_resolve_selected_access_point_features", return_value=(["NO_WATER"], "")),
            patch.object(
                core,
                "_build_access_point_feature_options",
                return_value=[
                    {
                        "name": "NO_WATER",
                        "label": "No water",
                        "description": "Water gap",
                        "coverage_display": "100%",
                        "variance_display": "0.4",
                        "is_selected": True,
                    }
                ],
            ),
            patch.object(core, "_build_access_point_rows_from_entity_frame", return_value=rows) as rows_builder,
            patch.object(core, "_build_points_scatter_chart", return_value={"title": "Chart", "plotly": None, "empty_message": "No chart"}),
        ):
            payload = core.get_access_points_data(table_name="fires", feature_columns=["NO_WATER"])

        self.assertTrue(payload["has_data"])
        self.assertIn("top_points", payload)
        self.assertIn("score_distribution", payload)
        self.assertIn("summary_cards", payload)
        self.assertIn("reason_breakdown", payload)
        self.assertIn("uncertainty_notes", payload)
        self.assertEqual(payload["filters"]["feature_columns"], ["NO_WATER"])
        self.assertTrue(payload["filters"]["available_features"])
        self.assertEqual(rows_builder.call_args.kwargs["selected_features"], ["NO_WATER"])
        self.assertIsNone(payload["points"][0]["latitude"])
        self.assertIsNone(payload["points"][0]["longitude"])
        self.assertEqual(payload["points"][0]["severity_band"], "высокий")
        self.assertTrue(payload["points"][0]["top_reason_codes"])
        self.assertTrue(payload["reason_breakdown"])
        self.assertTrue(payload["uncertainty_notes"])


if __name__ == "__main__":
    unittest.main()
