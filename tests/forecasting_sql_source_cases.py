from datetime import date

import app.services.forecasting.sql as forecasting_sql
from app.services.ml_model.training.training_result import _empty_ml_result
from tests.forecasting_sql_support import (
    ForecastingSqlSupport,
    forecasting_data,
    ml_core,
    patch,
    resolve_option,
)


class _PerfRecorder:
    def __init__(self) -> None:
        self.values = {}

    def update(self, **values) -> None:
        self.values.update(values)


class _ForecastingQueryResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _ForecastingConnection:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []
        self.params = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=None):
        self.queries.append(str(query))
        self.params.append(dict(params or {}))
        return _ForecastingQueryResult(self.rows)


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

        count_perf = _PerfRecorder()
        with (
            patch.object(forecasting_sql, "current_perf_trace", return_value=count_perf),
            patch.object(
                forecasting_sql,
                "_load_scope_total_count",
                side_effect=AssertionError("count should reuse daily history cache"),
            ),
        ):
            count = forecasting_sql._count_forecasting_records_sql(["fires"], metadata_items=metadata_items)

        self.assertEqual(sum(int(item["count"]) for item in history), 5)
        self.assertEqual(count, 5)
        self.assertTrue(count_perf.values["forecasting_filtered_record_count_cache_hit"])
        self.assertEqual(count_perf.values["forecasting_filtered_record_count"], 5)

    def test_single_table_daily_history_skips_union_fast_path_and_reports_perf(self) -> None:
        metadata_items = [{"table_name": "fires", "resolved_columns": {"date": "fire_date"}}]
        table_rows = [
            {
                "date": date(2024, 1, 1),
                "count": 3,
                "avg_temperature": 5.0,
                "temperature_samples": 1,
            }
        ]
        perf = _PerfRecorder()

        forecasting_sql.clear_forecasting_sql_cache()
        with (
            patch.object(forecasting_sql, "current_perf_trace", return_value=perf),
            patch.object(
                forecasting_sql,
                "_load_daily_history_rows_union",
                side_effect=AssertionError("single-table history should not use union fast path"),
            ),
            patch.object(forecasting_sql, "_load_daily_history_rows", return_value=table_rows) as table_mock,
        ):
            history = forecasting_sql._build_daily_history_sql(["fires"], metadata_items=metadata_items)

        table_mock.assert_called_once()
        self.assertEqual(history, [{"date": date(2024, 1, 1), "count": 3, "avg_temperature": 5.0}])
        self.assertFalse(perf.values["forecasting_daily_history_union_attempted"])
        self.assertFalse(perf.values["forecasting_daily_history_union_fast_path"])
        self.assertTrue(perf.values["forecasting_daily_history_count_cache_populated"])
        self.assertEqual(perf.values["forecasting_daily_history_total_count"], 3)

    def test_multi_table_union_without_materialized_views_keeps_filters_in_one_query(self) -> None:
        metadata_items = [
            {
                "table_name": "fires_a",
                "resolved_columns": {
                    "date": "fire_date",
                    "district": "district_name",
                    "cause": "fire_cause",
                    "object_category": "object_category",
                    "temperature": "temperature",
                },
            },
            {
                "table_name": "fires_b",
                "resolved_columns": {
                    "date": "fire_date",
                    "district": "district_name",
                    "cause": "fire_cause",
                    "object_category": "object_category",
                    "temperature": "temperature",
                },
            },
            {
                "table_name": "fires_c",
                "resolved_columns": {
                    "date": "fire_date",
                    "district": "district_name",
                    "cause": "fire_cause",
                    "object_category": "object_category",
                    "temperature": "temperature",
                },
            },
        ]
        conn = _ForecastingConnection(
            [
                {
                    "fire_date": date(2024, 1, 2),
                    "incident_count": 6,
                    "avg_temperature": 4.0,
                    "temperature_samples": 3,
                }
            ]
        )

        with (
            patch.object(
                forecasting_sql,
                "_daily_aggregate_view_status_map",
                return_value={"fires_a": False, "fires_b": False, "fires_c": False},
            ) as view_status_mock,
            patch.object(forecasting_sql.engine, "connect", return_value=conn),
        ):
            rows = forecasting_sql._load_daily_history_rows_union(
                metadata_items,
                district="Central",
                cause="Electrical",
                object_category="Residential",
                min_year=2024,
            )

        view_status_mock.assert_called_once_with(["fires_a", "fires_b", "fires_c"])
        self.assertEqual(len(conn.queries), 1)
        self.assertEqual(conn.queries[0].count("UNION ALL"), 2)
        self.assertNotIn("mv_forecasting_daily_", conn.queries[0])
        for table_name in ("fires_a", "fires_b", "fires_c"):
            self.assertIn(table_name, conn.queries[0])
        self.assertEqual(
            conn.params[0],
            {
                "district": "Central",
                "cause": "Electrical",
                "object_category": "Residential",
                "min_year": 2024,
            },
        )
        self.assertEqual(
            rows,
            [
                {
                    "date": date(2024, 1, 2),
                    "count": 6,
                    "avg_temperature": 4.0,
                    "temperature_samples": 3,
                }
            ],
        )

    def test_multi_table_union_mixes_materialized_view_and_base_table_safely(self) -> None:
        metadata_items = [
            {
                "table_name": "fires_a",
                "resolved_columns": {
                    "date": "fire_date",
                    "district": "district_name",
                    "cause": "fire_cause",
                    "object_category": "object_category",
                    "temperature": "temperature",
                },
            },
            {
                "table_name": "fires_b",
                "resolved_columns": {
                    "date": "fire_date",
                    "district": "district_name",
                    "cause": "fire_cause",
                    "object_category": "object_category",
                    "temperature": "temperature",
                },
            },
        ]
        conn = _ForecastingConnection(
            [
                {
                    "fire_date": date(2024, 1, 2),
                    "incident_count": 4,
                    "avg_temperature": 6.0,
                    "temperature_samples": 2,
                }
            ]
        )

        with (
            patch.object(
                forecasting_sql,
                "_daily_aggregate_view_status_map",
                return_value={"fires_a": True, "fires_b": False},
            ) as view_status_mock,
            patch.object(forecasting_sql.engine, "connect", return_value=conn),
        ):
            rows = forecasting_sql._load_daily_history_rows_union(
                metadata_items,
                district="Central",
                cause="Electrical",
                object_category="Residential",
                min_year=2024,
            )

        query = conn.queries[0]
        view_status_mock.assert_called_once_with(["fires_a", "fires_b"])
        self.assertEqual(len(conn.queries), 1)
        self.assertEqual(query.count("UNION ALL"), 1)
        self.assertIn(forecasting_sql._daily_aggregate_view_name("fires_a"), query)
        self.assertIn("fires_b", query)
        self.assertNotIn(forecasting_sql._daily_aggregate_view_name("fires_b"), query)
        self.assertEqual(
            conn.params[0],
            {
                "district": "Central",
                "cause": "Electrical",
                "object_category": "Residential",
                "min_year": 2024,
            },
        )
        self.assertEqual(
            rows,
            [
                {
                    "date": date(2024, 1, 2),
                    "count": 4,
                    "avg_temperature": 6.0,
                    "temperature_samples": 2,
                }
            ],
        )

    def test_multi_table_daily_history_uses_union_fast_path_and_count_cache(self) -> None:
        metadata_items = [
            {"table_name": "fires_a", "resolved_columns": {"date": "fire_date"}},
            {"table_name": "fires_b", "resolved_columns": {"date": "fire_date"}},
        ]
        union_rows = [
            {
                "date": date(2024, 1, 1),
                "count": 3,
                "avg_temperature": 10.0,
                "temperature_samples": 2,
            },
            {
                "date": date(2024, 1, 3),
                "count": 2,
                "avg_temperature": None,
                "temperature_samples": 0,
            },
        ]

        forecasting_sql.clear_forecasting_sql_cache()
        perf = _PerfRecorder()
        with (
            patch.object(forecasting_sql, "current_perf_trace", return_value=perf),
            patch.object(forecasting_sql, "_load_daily_history_rows_union", return_value=union_rows) as union_mock,
            patch.object(
                forecasting_sql,
                "_load_daily_history_rows",
                side_effect=AssertionError("per-table daily history should not run after union fast path"),
            ),
        ):
            history = forecasting_sql._build_daily_history_sql(
                ["fires_a", "fires_b"],
                metadata_items=metadata_items,
            )

        with patch.object(
            forecasting_sql,
            "_load_scope_total_count",
            side_effect=AssertionError("count should reuse daily history cache"),
        ):
            count = forecasting_sql._count_forecasting_records_sql(
                ["fires_a", "fires_b"],
                metadata_items=metadata_items,
            )

        union_mock.assert_called_once()
        self.assertEqual(
            history,
            [
                {"date": date(2024, 1, 1), "count": 3, "avg_temperature": 10.0},
                {"date": date(2024, 1, 2), "count": 0, "avg_temperature": None},
                {"date": date(2024, 1, 3), "count": 2, "avg_temperature": None},
            ],
        )
        self.assertEqual(count, 5)
        self.assertTrue(perf.values["forecasting_daily_history_union_attempted"])
        self.assertTrue(perf.values["forecasting_daily_history_union_fast_path"])
        self.assertFalse(perf.values["forecasting_daily_history_union_fallback"])
        self.assertEqual(perf.values["forecasting_daily_history_union_rows"], 2)
        self.assertEqual(perf.values["forecasting_daily_history_total_count"], 5)

    def test_multi_table_daily_history_falls_back_when_union_fast_path_fails(self) -> None:
        metadata_items = [
            {"table_name": "fires_a", "resolved_columns": {"date": "fire_date"}},
            {"table_name": "fires_b", "resolved_columns": {"date": "fire_date"}},
        ]
        rows_by_table = {
            "fires_a": [
                {
                    "date": date(2024, 1, 1),
                    "count": 1,
                    "avg_temperature": 2.0,
                    "temperature_samples": 1,
                }
            ],
            "fires_b": [
                {
                    "date": date(2024, 1, 1),
                    "count": 2,
                    "avg_temperature": 4.0,
                    "temperature_samples": 1,
                }
            ],
        }

        def _load_table_rows(table_name, *_args, **_kwargs):
            return rows_by_table[table_name]

        forecasting_sql.clear_forecasting_sql_cache()
        perf = _PerfRecorder()
        with (
            patch.object(forecasting_sql, "current_perf_trace", return_value=perf),
            patch.object(forecasting_sql, "_load_daily_history_rows_union", side_effect=RuntimeError("boom")),
            patch.object(forecasting_sql, "_load_daily_history_rows", side_effect=_load_table_rows) as table_mock,
        ):
            history = forecasting_sql._build_daily_history_sql(
                ["fires_a", "fires_b"],
                metadata_items=metadata_items,
            )

        self.assertEqual(table_mock.call_count, 2)
        self.assertEqual(history, [{"date": date(2024, 1, 1), "count": 3, "avg_temperature": 3.0}])
        self.assertTrue(perf.values["forecasting_daily_history_union_attempted"])
        self.assertFalse(perf.values["forecasting_daily_history_union_fast_path"])
        self.assertTrue(perf.values["forecasting_daily_history_union_fallback"])
        self.assertEqual(perf.values["forecasting_daily_history_union_error_type"], "RuntimeError")
