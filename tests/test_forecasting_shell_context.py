import unittest
from unittest.mock import patch

import app.services.forecasting.core as core


class ForecastingShellContextTests(unittest.TestCase):
    def test_shell_context_prefers_cached_payload_when_available(self) -> None:
        table_options = [{"value": "fires", "label": "Пожары"}]
        cached_payload = {
            "bootstrap_mode": "full",
            "has_data": True,
            "notes": ["cached"],
            "filters": {
                "table_name": "fires",
                "available_tables": table_options,
            },
        }

        with (
            patch.object(core, "_build_forecasting_table_options", return_value=table_options),
            patch.object(core, "_resolve_forecasting_selection", return_value="fires"),
            patch.object(core, "_selected_source_tables", return_value=["fires"]),
            patch.object(core, "_parse_forecast_days", return_value=14),
            patch.object(core, "_parse_history_window", return_value="all"),
            patch.object(core, "_build_forecasting_shell_data", side_effect=AssertionError("shell should use cache first")),
            patch.object(core._FORECASTING_CACHE, "get", side_effect=[cached_payload]),
        ):
            context = core.get_forecasting_shell_context(table_name="fires")

        self.assertEqual(context["initial_data"], cached_payload)
        self.assertTrue(context["has_data"])
        self.assertEqual(context["plotly_js"], "")
    def test_shell_context_returns_lightweight_deferred_bootstrap(self) -> None:
        table_options = [{"value": "fires", "label": "Пожары"}]

        with (
            patch.object(core, "get_forecasting_data") as full_forecast_mock,
            patch.object(core, "_build_forecasting_table_options", return_value=table_options),
            patch.object(core, "_resolve_forecasting_selection", return_value="fires"),
            patch.object(core, "_selected_source_tables", return_value=["fires"]),
            patch.object(core, "_parse_forecast_days", return_value=14),
            patch.object(core, "_parse_history_window", return_value="all"),
            patch.object(core, "_collect_forecasting_metadata", side_effect=AssertionError("shell should not load metadata")),
            patch.object(core, "_build_option_catalog_sql", side_effect=AssertionError("shell should not build option catalog")),
            patch.object(core._FORECASTING_CACHE, "get", return_value=None),
        ):
            full_forecast_mock.side_effect = AssertionError("shell context should not call full forecast")
            context = core.get_forecasting_shell_context(
                table_name="fires",
                district="North",
                cause="Grass",
                object_category="Warehouse",
                temperature="",
                forecast_days="14",
                history_window="all",
            )

        full_forecast_mock.assert_not_called()
        data = context["initial_data"]

        self.assertEqual(context["plotly_js"], "")
        self.assertEqual(data["bootstrap_mode"], "deferred")
        self.assertTrue(data["loading"])
        self.assertTrue(data["deferred"])
        self.assertTrue(data["metadata_pending"])
        self.assertFalse(data["metadata_ready"])
        self.assertTrue(data["base_forecast_pending"])
        self.assertFalse(data["base_forecast_ready"])
        self.assertFalse(data["decision_support_pending"])
        self.assertFalse(data["decision_support_ready"])
        self.assertEqual(data["filters"]["table_name"], "fires")
        self.assertEqual(data["filters"]["district"], "North")
        self.assertEqual(data["filters"]["cause"], "Grass")
        self.assertEqual(data["filters"]["object_category"], "Warehouse")
        self.assertEqual(data["filters"]["available_tables"], table_options)
        self.assertEqual(data["filters"]["available_districts"][0]["value"], "North")
        self.assertEqual(data["filters"]["available_causes"][0]["value"], "Grass")
        self.assertEqual(data["filters"]["available_object_categories"][0]["value"], "Warehouse")
        self.assertEqual(data["summary"]["slice_label"], "район: North | причина: Grass | категория: Warehouse")
        self.assertIn("фильтры и доступные признаки", data["metadata_status_message"])
        self.assertIn("Базовый прогноз", data["loading_status_message"])
        self.assertEqual(data["features"], [])
        self.assertEqual(data["risk_prediction"]["feature_cards"], [])
        self.assertTrue(data["charts"]["daily"]["empty_message"])

    def test_metadata_payload_loads_catalog_without_running_base_forecast(self) -> None:
        table_options = [{"value": "fires", "label": "Пожары"}]
        option_catalog = {
            "districts": [
                {"value": "all", "label": "Все районы"},
                {"value": "North", "label": "North"},
            ],
            "causes": [
                {"value": "all", "label": "Все причины"},
                {"value": "Grass", "label": "Grass"},
            ],
            "object_categories": [
                {"value": "all", "label": "Все категории"},
                {"value": "Warehouse", "label": "Warehouse"},
            ],
        }
        metadata_items = [
            {
                "table_name": "fires",
                "resolved_columns": {
                    "date": "fire_date",
                    "cause": "fire_cause",
                    "object_category": "object_category",
                },
            }
        ]

        def resolve_option(options, value):
            for item in options:
                if item["value"] == value:
                    return value
            return options[0]["value"] if options else "all"

        with (
            patch.object(core, "_build_forecasting_table_options", return_value=table_options),
            patch.object(core, "_resolve_forecasting_selection", return_value="fires"),
            patch.object(core, "_selected_source_tables", return_value=["fires"]),
            patch.object(core, "_parse_forecast_days", return_value=14),
            patch.object(core, "_parse_history_window", return_value="all"),
            patch.object(core, "_collect_forecasting_metadata", return_value=(metadata_items, ["metadata ready"])),
            patch.object(core, "_build_option_catalog_sql", return_value=option_catalog),
            patch.object(core, "_resolve_option_value", side_effect=resolve_option),
            patch.object(core, "_build_daily_history_sql", side_effect=AssertionError("metadata stage should not build daily history")),
            patch.object(core, "_count_forecasting_records_sql", side_effect=AssertionError("metadata stage should not count records for forecast")),
            patch.object(core, "_build_forecast_rows", side_effect=AssertionError("metadata stage should not build forecast rows")),
            patch.object(core, "build_decision_support_payload", side_effect=AssertionError("metadata stage should not build decision support")),
        ):
            data = core.get_forecasting_metadata(
                table_name="fires",
                district="North",
                cause="Grass",
                object_category="Warehouse",
                temperature="",
                forecast_days="14",
                history_window="all",
            )

        self.assertEqual(data["bootstrap_mode"], "deferred")
        self.assertTrue(data["loading"])
        self.assertTrue(data["metadata_ready"])
        self.assertFalse(data["metadata_pending"])
        self.assertTrue(data["base_forecast_pending"])
        self.assertFalse(data["base_forecast_ready"])
        self.assertFalse(data["decision_support_pending"])
        self.assertEqual(data["metadata_status_message"], "Фильтры и признаки готовы.")
        self.assertIn("Запускаем базовый прогноз", data["loading_status_message"])
        self.assertEqual(data["filters"]["district"], "North")
        self.assertEqual(data["filters"]["cause"], "Grass")
        self.assertEqual(data["filters"]["object_category"], "Warehouse")
        self.assertEqual(data["filters"]["available_districts"], option_catalog["districts"])
        self.assertEqual(data["filters"]["available_causes"], option_catalog["causes"])
        self.assertEqual(data["filters"]["available_object_categories"], option_catalog["object_categories"])
        self.assertEqual(data["notes"][0], "metadata ready")
        self.assertTrue(data["features"])
        self.assertEqual(data["risk_prediction"]["feature_cards"], data["features"])
        self.assertEqual(data["summary"]["slice_label"], "район: North | причина: Grass | категория: Warehouse")


if __name__ == "__main__":
    unittest.main()
