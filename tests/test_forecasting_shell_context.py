import unittest
from unittest.mock import patch

from app.services.forecasting import core


class ForecastingShellContextTests(unittest.TestCase):
    def test_shell_context_avoids_full_forecast_and_returns_deferred_bootstrap(self) -> None:
        table_options = [
            {"value": "fires", "label": "Пожары"},
        ]
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

        with patch.object(core, "get_forecasting_data") as full_forecast_mock, patch.object(
            core,
            "_build_forecasting_table_options",
            return_value=table_options,
        ), patch.object(
            core,
            "_resolve_forecasting_selection",
            return_value="fires",
        ), patch.object(
            core,
            "_selected_source_tables",
            return_value=["fires"],
        ), patch.object(
            core,
            "_parse_forecast_days",
            return_value=14,
        ), patch.object(
            core,
            "_parse_history_window",
            return_value="all",
        ), patch.object(
            core,
            "_collect_forecasting_metadata",
            return_value=(metadata_items, ["metadata ready"]),
        ), patch.object(
            core,
            "_build_option_catalog_sql",
            return_value=option_catalog,
        ), patch.object(
            core,
            "_resolve_option_value",
            side_effect=resolve_option,
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
        self.assertTrue(data["base_forecast_pending"])
        self.assertFalse(data["base_forecast_ready"])
        self.assertFalse(data["decision_support_pending"])
        self.assertFalse(data["decision_support_ready"])
        self.assertEqual(data["filters"]["table_name"], "fires")
        self.assertEqual(data["filters"]["district"], "North")
        self.assertEqual(data["filters"]["cause"], "Grass")
        self.assertEqual(data["filters"]["object_category"], "Warehouse")
        self.assertEqual(data["filters"]["available_districts"], option_catalog["districts"])
        self.assertEqual(data["filters"]["available_causes"], option_catalog["causes"])
        self.assertEqual(data["filters"]["available_object_categories"], option_catalog["object_categories"])
        self.assertEqual(data["summary"]["slice_label"], "район: North | причина: Grass | категория: Warehouse")
        self.assertIn("быстром режиме", data["loading_status_message"])
        self.assertEqual(data["risk_prediction"]["top_territory_label"], "Подготавливаем прогноз")
        self.assertEqual(data["executive_brief"]["lead"], data["loading_status_message"])
        self.assertEqual(data["notes"][0], "metadata ready")
        self.assertTrue(data["charts"]["daily"]["empty_message"])


if __name__ == "__main__":
    unittest.main()
