import unittest
from datetime import date
from unittest.mock import patch

import app.services.forecasting.core as forecasting_core
import app.services.forecasting.data as forecasting_data
from app.services.ml_model import core as ml_core
from tests.mojibake_check import MOJIBAKE_PATTERN as SHARED_MOJIBAKE_PATTERN


def resolve_option(options, value):
    for item in options:
        if item["value"] == value:
            return value
    return options[0]["value"] if options else "all"


class ForecastingSqlSupport(unittest.TestCase):
    MOJIBAKE_PATTERN = SHARED_MOJIBAKE_PATTERN

    def tearDown(self) -> None:
        forecasting_core.clear_forecasting_cache()
        ml_core.clear_ml_model_cache()

    def _assert_no_mojibake(self, value: object, *, context: str) -> None:
        values_to_visit = [value]
        while values_to_visit:
            current = values_to_visit.pop()
            if isinstance(current, str):
                self.assertIsNone(
                    self.MOJIBAKE_PATTERN.search(current),
                    msg=f"{context}: unexpected mojibake in {current!r}",
                )
            elif isinstance(current, dict):
                values_to_visit.extend(current.values())
            elif isinstance(current, (list, tuple, set)):
                values_to_visit.extend(current)

    def _build_forecasting_service_payload_smoke(self) -> dict[str, object]:
        table_options = [{"value": "fires", "label": "fires"}]
        option_catalog = {
            "districts": [{"value": "all", "label": "all"}],
            "causes": [{"value": "all", "label": "all"}],
            "object_categories": [{"value": "all", "label": "all"}],
        }
        metadata_items = [
            {
                "table_name": "fires",
                "resolved_columns": {
                    "date": "fire_date",
                    "temperature": "avg_temperature",
                    "cause": "fire_cause",
                    "object_category": "object_category",
                },
            }
        ]
        daily_history = [
            {"date": date(2024, 1, 1), "count": 3, "avg_temperature": 1.5},
            {"date": date(2024, 1, 2), "count": 0, "avg_temperature": None},
            {"date": date(2024, 1, 3), "count": 1, "avg_temperature": -2.0},
        ]
        forecast_rows = [
            {
                "date": date(2024, 1, 4),
                "date_display": "04.01.2024",
                "weekday_label": "чт",
                "forecast_value": 2.0,
                "forecast_value_display": "2",
                "fire_probability": 0.5,
                "fire_probability_display": "50%",
                "scenario_label": "Выше обычного",
                "scenario_hint": "Тестовый сценарий",
                "scenario_tone": "fire",
            },
            {
                "date": date(2024, 1, 5),
                "date_display": "05.01.2024",
                "weekday_label": "пт",
                "forecast_value": 1.0,
                "forecast_value_display": "1",
                "fire_probability": 0.25,
                "fire_probability_display": "25%",
                "scenario_label": "Около обычного",
                "scenario_hint": "Тестовый сценарий",
                "scenario_tone": "forest",
            },
        ]
        backtest = {
            "is_ready": True,
            "message": "",
            "rows": [
                {
                    "date": "2024-01-03",
                    "actual_count": 1.0,
                    "predicted_count": 1.5,
                    "baseline_count": 1.2,
                }
            ],
            "model_metrics": {
                "mae": 0.4,
                "rmse": 0.6,
                "smape": 18.0,
                "mae_delta_vs_baseline": -0.2,
            },
            "baseline_metrics": {
                "mae": 0.5,
                "rmse": 0.7,
                "smape": 20.0,
            },
            "overview": {
                "folds": 12,
                "min_train_days": 14,
                "validation_horizon_days": 1,
            },
        }

        with (
            patch.object(forecasting_core, "_build_forecasting_table_options", return_value=table_options),
            patch.object(forecasting_core, "_resolve_forecasting_selection", return_value="fires"),
            patch.object(forecasting_core, "_selected_source_tables", return_value=["fires"]),
            patch.object(forecasting_core, "_parse_forecast_days", return_value=2),
            patch.object(forecasting_core, "_parse_history_window", return_value="all"),
            patch.object(forecasting_core, "_collect_forecasting_metadata", return_value=(metadata_items, [])),
            patch.object(forecasting_core, "_build_option_catalog_sql", return_value=option_catalog),
            patch.object(forecasting_core, "_resolve_option_value", side_effect=resolve_option),
            patch.object(forecasting_core, "_count_forecasting_records_sql", return_value=17),
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
            return forecasting_core.get_forecasting_data(
                table_name="fires",
                district="all",
                cause="all",
                object_category="all",
                temperature="",
                forecast_days="2",
                history_window="all",
                include_decision_support=False,
            )
