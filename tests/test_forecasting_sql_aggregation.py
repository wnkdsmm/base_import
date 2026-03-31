import re
import unittest
from datetime import date
from unittest.mock import patch

from app.services.forecasting import core as forecasting_core
from app.services.forecasting import data as forecasting_data
from app.services.ml_model import core as ml_core


def _resolve_option(options, value):
    for item in options:
        if item["value"] == value:
            return value
    return options[0]["value"] if options else "all"


class ForecastingSqlAggregationTests(unittest.TestCase):
    MOJIBAKE_PATTERN = re.compile(
        r"(Р [Р‚РѓР‰РЉР‹РЊРЌРЋРЏ]|РЎ[Р‚РѓР‰РЉР‹РЊРЌРЋРЏ]|РІР‚|\?{3,}|Р Т‘Р Р…Р ВµР в„–|Р С—Р С•Р С”РЎР‚|Р С™Р С•Р В»Р С•Р Р…Р С”Р В°)"
    )

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
            patch.object(forecasting_core, "_resolve_option_value", side_effect=_resolve_option),
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

    def test_selected_source_tables_prefers_clean_pair_in_all_mode(self) -> None:
        raw_table = "ekup_Yemelyanovo_2025"
        clean_table = f"clean_{raw_table}"
        table_options = [
            {"value": raw_table, "label": raw_table},
            {"value": clean_table, "label": clean_table},
            {"value": "clean_other_dataset_2025", "label": "clean_other_dataset_2025"},
        ]

        selected_tables = forecasting_data._selected_source_tables(table_options, "all")
        notes = forecasting_data._selected_source_table_notes(table_options, "all")

        self.assertEqual(selected_tables, [clean_table, "clean_other_dataset_2025"])
        self.assertEqual(len(notes), 1)
        self.assertIn(raw_table, notes[0])
        self.assertIn(clean_table, notes[0])

    def test_selected_source_tables_keeps_explicit_raw_selection(self) -> None:
        raw_table = "ekup_Yemelyanovo_2025"
        clean_table = f"clean_{raw_table}"
        table_options = [
            {"value": raw_table, "label": raw_table},
            {"value": clean_table, "label": clean_table},
        ]

        self.assertEqual(forecasting_data._selected_source_tables(table_options, raw_table), [raw_table])
        self.assertEqual(forecasting_data._selected_source_table_notes(table_options, raw_table), [])

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

    def test_ml_all_mode_uses_clean_table_once_and_reports_raw_exclusion(self) -> None:
        raw_table = "ekup_Yemelyanovo_2025"
        clean_table = f"clean_{raw_table}"
        table_options = [
            {"value": raw_table, "label": raw_table},
            {"value": clean_table, "label": clean_table},
        ]
        option_catalog = {
            "causes": [{"value": "all", "label": "All causes"}],
            "object_categories": [{"value": "all", "label": "All categories"}],
        }
        metadata_items = [{"table_name": clean_table, "resolved_columns": {"date": "fire_date"}}]
        daily_history = [
            {"date": date(2024, 1, 1), "count": 1, "avg_temperature": 0.0},
            {"date": date(2024, 1, 2), "count": 2, "avg_temperature": 1.0},
        ]
        empty_result = ml_core._empty_ml_result("Not enough data for training.")

        with (
            patch.object(ml_core, "_build_forecasting_table_options", return_value=table_options),
            patch.object(ml_core, "_resolve_forecasting_selection", return_value="all"),
            patch.object(ml_core, "_parse_forecast_days", return_value=14),
            patch.object(ml_core, "_parse_history_window", return_value="all"),
            patch.object(ml_core, "_collect_forecasting_metadata", return_value=(metadata_items, [])) as metadata_mock,
            patch.object(ml_core, "_build_option_catalog_sql", return_value=option_catalog) as catalog_mock,
            patch.object(ml_core, "_resolve_option_value", side_effect=_resolve_option),
            patch.object(ml_core, "_build_daily_history_sql", return_value=daily_history) as history_mock,
            patch.object(ml_core, "_count_forecasting_records_sql", return_value=11) as count_mock,
            patch.object(ml_core, "_train_ml_model", return_value=empty_result),
        ):
            payload = ml_core.get_ml_model_data(
                table_name="all",
                cause="all",
                object_category="all",
                temperature="",
                forecast_days="14",
                history_window="all",
            )

        metadata_mock.assert_called_once_with([clean_table])
        self.assertEqual(catalog_mock.call_args.args[0], [clean_table])
        self.assertEqual(history_mock.call_args.args[0], [clean_table])
        self.assertEqual(count_mock.call_args.args[0], [clean_table])
        self.assertTrue(any(raw_table in note and clean_table in note for note in payload["notes"]))

    def test_forecasting_service_payload_smoke_keeps_sections_consistent(self) -> None:
        payload = self._build_forecasting_service_payload_smoke()
        summary = payload["summary"]
        comparison_rows = payload["quality_assessment"]["comparison_rows"]
        temperature_card = next(item for item in payload["features"] if item["label"] == "Температура")

        self.assertTrue(payload["has_data"])
        self.assertTrue(payload["decision_support_pending"])
        self.assertFalse(payload["decision_support_ready"])
        self.assertEqual(summary["selected_table_label"], "fires")
        self.assertEqual(summary["fires_count_display"], "17")
        self.assertEqual(summary["history_days_display"], "3")
        self.assertEqual(summary["forecast_days_display"], "2")
        self.assertEqual(summary["predicted_total_display"], "3")
        self.assertEqual(summary["peak_forecast_day_display"], "04.01.2024")
        self.assertEqual(
            payload["notes"],
            [
                "История короткая, поэтому сценарный прогноз может быть менее устойчивым.",
                "Сценарный прогноз лучше читать как ориентир по уровню нагрузки и приоритетам, а не как точное обещание числа пожаров в каждый день.",
            ],
        )
        self.assertEqual(temperature_card["status_label"], "Низкое покрытие (2/3 дней (66,7%))")
        self.assertEqual(
            temperature_card["source"],
            "fires: avg_temperature | покрытие по дневной истории: 2/3 дней (66,7%)",
        )
        self.assertEqual(
            temperature_card["description"],
            "Колонка температуры найдена, но покрытие низкое: температурный признак нельзя считать надёжным для ML и температурной поправки.",
        )
        self.assertEqual(temperature_card["coverage_display"], "2/3 дней (66,7%)")
        self.assertEqual(
            comparison_rows,
            [
                {
                    "method_label": "Сезонная базовая модель",
                    "role_label": "Базовая модель",
                    "mae_display": "0,5",
                    "rmse_display": "0,7",
                    "smape_display": "20%",
                    "selection_label": "Опорная линия",
                    "mae_delta_display": "0%",
                },
                {
                    "method_label": "Сценарный прогноз",
                    "role_label": "Эвристическая модель",
                    "mae_display": "0,4",
                    "rmse_display": "0,6",
                    "smape_display": "18%",
                    "selection_label": "Рабочая модель",
                    "mae_delta_display": "-20%",
                },
            ],
        )
        self.assertEqual(payload["risk_prediction"]["feature_cards"], payload["features"])
        self._assert_no_mojibake(
            {
                "summary": summary,
                "notes": payload["notes"],
                "features": payload["features"],
                "comparison_rows": comparison_rows,
            },
            context="forecasting service payload smoke",
        )


if __name__ == "__main__":
    unittest.main()
