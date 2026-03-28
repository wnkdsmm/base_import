import unittest
from datetime import date
from unittest.mock import patch

from app.services.forecasting import core as forecasting_core
from app.services.ml_model import core as ml_core


def _resolve_option(options, value):
    for item in options:
        if item["value"] == value:
            return value
    return options[0]["value"] if options else "all"


class ForecastingSqlAggregationTests(unittest.TestCase):
    def tearDown(self) -> None:
        forecasting_core.clear_forecasting_cache()
        ml_core.clear_ml_model_cache()

    def test_partial_forecast_uses_sql_count_without_loading_decision_support(self) -> None:
        table_options = [{"value": "fires", "label": "Пожары"}]
        option_catalog = {
            "districts": [{"value": "all", "label": "Все районы"}],
            "causes": [{"value": "all", "label": "Все причины"}],
            "object_categories": [{"value": "all", "label": "Все категории"}],
        }
        metadata_items = [{"table_name": "fires", "resolved_columns": {"date": "fire_date"}}]
        daily_history = [
            {"date": date(2024, 1, 1), "count": 3, "avg_temperature": 1.5},
            {"date": date(2024, 1, 2), "count": 0, "avg_temperature": None},
        ]
        forecast_rows = [
            {
                "date": date(2024, 1, 3),
                "date_display": "03.01.2024",
                "weekday_label": "ср",
                "forecast_value": 2.0,
                "forecast_value_display": "2",
                "fire_probability": 0.5,
                "fire_probability_display": "50%",
                "scenario_label": "Выше обычного",
                "scenario_hint": "Тестовый сценарий",
                "scenario_tone": "fire",
            }
        ]
        backtest = {
            "is_ready": False,
            "message": "",
            "rows": [],
            "model_metrics": {},
            "baseline_metrics": {},
            "overview": {"folds": 0, "min_train_days": 0, "validation_horizon_days": 1},
        }

        with (
            patch.object(forecasting_core, "_build_forecasting_table_options", return_value=table_options),
            patch.object(forecasting_core, "_resolve_forecasting_selection", return_value="fires"),
            patch.object(forecasting_core, "_selected_source_tables", return_value=["fires"]),
            patch.object(forecasting_core, "_parse_forecast_days", return_value=14),
            patch.object(forecasting_core, "_parse_history_window", return_value="all"),
            patch.object(forecasting_core, "_collect_forecasting_metadata", return_value=(metadata_items, [])),
            patch.object(forecasting_core, "_build_option_catalog_sql", return_value=option_catalog),
            patch.object(forecasting_core, "_resolve_option_value", side_effect=_resolve_option),
            patch.object(forecasting_core, "_count_forecasting_records_sql", return_value=17) as count_mock,
            patch.object(forecasting_core, "_build_daily_history_sql", return_value=daily_history),
            patch.object(forecasting_core, "_run_scenario_backtesting", return_value=backtest),
            patch.object(forecasting_core, "_build_forecast_rows", return_value=forecast_rows),
            patch.object(forecasting_core, "_build_weekday_profile", return_value=[]),
            patch.object(
                forecasting_core,
                "_build_forecast_chart",
                return_value={"title": "daily", "plotly": {}, "empty_message": ""},
            ),
            patch.object(
                forecasting_core,
                "_build_forecast_breakdown_chart",
                return_value={"title": "breakdown", "plotly": {}, "empty_message": ""},
            ),
            patch.object(
                forecasting_core,
                "_build_weekday_chart",
                return_value={"title": "weekday", "plotly": {}, "empty_message": ""},
            ),
            patch.object(
                forecasting_core,
                "_build_geo_chart",
                return_value={"title": "geo", "plotly": {}, "empty_message": "pending"},
            ),
            patch.object(forecasting_core, "_build_insights", return_value=[]),
            patch.object(
                forecasting_core,
                "build_decision_support_payload",
                side_effect=AssertionError("decision support must stay deferred"),
            ),
        ):
            payload = forecasting_core.get_forecasting_data(
                table_name="fires",
                district="all",
                cause="all",
                object_category="all",
                temperature="",
                forecast_days="14",
                history_window="all",
                include_decision_support=False,
            )

        count_mock.assert_called_once()
        self.assertTrue(payload["has_data"])
        self.assertTrue(payload["decision_support_pending"])
        self.assertFalse(payload["decision_support_ready"])
        self.assertEqual(payload["summary"]["fires_count_display"], "17")
        self.assertEqual(payload["charts"]["geo"]["empty_message"], "pending")

    def test_ml_summary_uses_sql_count_instead_of_summing_daily_history(self) -> None:
        table_options = [{"value": "fires", "label": "Пожары"}]
        option_catalog = {
            "causes": [{"value": "all", "label": "Все причины"}],
            "object_categories": [{"value": "all", "label": "Все категории"}],
        }
        metadata_items = [{"table_name": "fires", "resolved_columns": {"date": "fire_date"}}]
        daily_history = [
            {"date": date(2024, 1, 1), "count": 1, "avg_temperature": 0.0},
            {"date": date(2024, 1, 2), "count": 2, "avg_temperature": 1.0},
        ]
        empty_result = ml_core._empty_ml_result("Недостаточно данных для обучения.")

        with (
            patch.object(ml_core, "_build_forecasting_table_options", return_value=table_options),
            patch.object(ml_core, "_resolve_forecasting_selection", return_value="fires"),
            patch.object(ml_core, "_selected_source_tables", return_value=["fires"]),
            patch.object(ml_core, "_parse_forecast_days", return_value=14),
            patch.object(ml_core, "_parse_history_window", return_value="all"),
            patch.object(ml_core, "_collect_forecasting_metadata", return_value=(metadata_items, [])),
            patch.object(ml_core, "_build_option_catalog_sql", return_value=option_catalog),
            patch.object(ml_core, "_resolve_option_value", side_effect=_resolve_option),
            patch.object(ml_core, "_build_daily_history_sql", return_value=daily_history),
            patch.object(ml_core, "_count_forecasting_records_sql", return_value=11) as count_mock,
            patch.object(ml_core, "_train_ml_model", return_value=empty_result),
        ):
            payload = ml_core.get_ml_model_data(
                table_name="fires",
                cause="all",
                object_category="all",
                temperature="",
                forecast_days="14",
                history_window="all",
            )

        count_mock.assert_called_once()
        self.assertEqual(payload["summary"]["fires_count_display"], "11")


if __name__ == "__main__":
    unittest.main()
