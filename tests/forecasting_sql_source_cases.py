from datetime import date

from app.services.forecasting import sql as forecasting_sql
from app.services.ml_model.training_result import _empty_ml_result
from tests.forecasting_sql_support import (
    ForecastingSqlSupport,
    forecasting_data,
    ml_core,
    patch,
    resolve_option,
)


class ForecastingSqlSourceSelectionTests(ForecastingSqlSupport):
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
        empty_result = _empty_ml_result("Недостаточно данных для обучения.")

        with (
            patch.object(ml_core, "_build_forecasting_table_options", return_value=table_options),
            patch.object(ml_core, "_resolve_forecasting_selection", return_value="fires"),
            patch.object(ml_core, "_selected_source_tables", return_value=["fires"]),
            patch.object(ml_core, "_parse_forecast_days", return_value=14),
            patch.object(ml_core, "_parse_history_window", return_value="all"),
            patch.object(ml_core, "_collect_forecasting_metadata", return_value=(metadata_items, [])),
            patch.object(ml_core, "_build_option_catalog_sql", return_value=option_catalog),
            patch.object(ml_core, "_resolve_option_value", side_effect=resolve_option),
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
        empty_result = _empty_ml_result("Not enough data for training.")

        with (
            patch.object(ml_core, "_build_forecasting_table_options", return_value=table_options),
            patch.object(ml_core, "_resolve_forecasting_selection", return_value="all"),
            patch.object(ml_core, "_parse_forecast_days", return_value=14),
            patch.object(ml_core, "_parse_history_window", return_value="all"),
            patch.object(ml_core, "_collect_forecasting_metadata", return_value=(metadata_items, [])) as metadata_mock,
            patch.object(ml_core, "_build_option_catalog_sql", return_value=option_catalog) as catalog_mock,
            patch.object(ml_core, "_resolve_option_value", side_effect=resolve_option),
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

    def test_daily_history_populates_filtered_count_cache(self) -> None:
        metadata_items = [{"table_name": "fires", "resolved_columns": {"date": "fire_date"}}]
        table_rows = [
            {
                "date": date(2024, 1, 1),
                "count": 3,
                "avg_temperature": None,
                "temperature_samples": 0,
            },
            {
                "date": date(2024, 1, 3),
                "count": 2,
                "avg_temperature": 1.0,
                "temperature_samples": 1,
            },
        ]

        forecasting_sql.clear_forecasting_sql_cache()
        with patch.object(forecasting_sql, "_load_daily_history_rows", return_value=table_rows):
            history = forecasting_sql._build_daily_history_sql(["fires"], metadata_items=metadata_items)

        with patch.object(
            forecasting_sql,
            "_load_scope_total_count",
            side_effect=AssertionError("count should reuse daily history cache"),
        ):
            count = forecasting_sql._count_forecasting_records_sql(["fires"], metadata_items=metadata_items)

        self.assertEqual(sum(int(item["count"]) for item in history), 5)
        self.assertEqual(count, 5)
