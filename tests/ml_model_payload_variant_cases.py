import re
import unittest
import warnings
from datetime import date, timedelta
from unittest.mock import patch

from app.services.ml_model import core as ml_core


def _resolve_option(options, value):
    for item in options:
        if item["value"] == value:
            return value
    return options[0]["value"] if options else "all"


ALL_MODE_EVENT_REASON_CODE = "single_class_evaluation"
ALL_MODE_EVENT_BACKTEST_MODEL_LABEL = "\u041d\u0435 \u043f\u043e\u043a\u0430\u0437\u0430\u043d"
ALL_MODE_EVENT_TABLE_TITLE = (
    "\u0421\u0440\u0430\u0432\u043d\u0435\u043d\u0438\u0435 \u043f\u043e "
    "\u0432\u0435\u0440\u043e\u044f\u0442\u043d\u043e\u0441\u0442\u0438 "
    "\u0441\u043e\u0431\u044b\u0442\u0438\u044f \u043f\u043e\u0436\u0430\u0440\u0430"
)
ALL_MODE_EVENT_EMPTY_MESSAGE = (
    "\u0412\u0435\u0440\u043e\u044f\u0442\u043d\u043e\u0441\u0442\u043d\u044b\u0439 "
    "\u0431\u043b\u043e\u043a \u0441\u043e\u0431\u044b\u0442\u0438\u044f "
    "\u043f\u043e\u0436\u0430\u0440\u0430 \u0441\u043a\u0440\u044b\u0442: \u0432\u0441\u0435 45 "
    "evaluation-\u043e\u043a\u043e\u043d rolling-origin backtesting "
    "\u043e\u0442\u043d\u043e\u0441\u044f\u0442\u0441\u044f \u043a \u043e\u0434\u043d\u043e\u043c\u0443 "
    "\u043a\u043b\u0430\u0441\u0441\u0443 (\u0442\u043e\u043b\u044c\u043a\u043e "
    "\u0434\u043d\u0438 \u0441 \u043f\u043e\u0436\u0430\u0440\u043e\u043c), "
    "\u043f\u043e\u044d\u0442\u043e\u043c\u0443 "
    "\u0432\u0435\u0440\u043e\u044f\u0442\u043d\u043e\u0441\u0442\u043d\u0430\u044f "
    "\u0432\u0430\u043b\u0438\u0434\u0430\u0446\u0438\u044f "
    "\u043d\u0435\u043a\u043e\u0440\u0440\u0435\u043a\u0442\u043d\u0430."
)
ALL_MODE_RAW_CLEAN_NOTE = (
    "\u0422\u0430\u0431\u043b\u0438\u0446\u0430 'ekup_Yemelyanovo_2025' "
    "\u0438\u0441\u043a\u043b\u044e\u0447\u0435\u043d\u0430 \u043a\u0430\u043a "
    "\u0434\u0443\u0431\u043b\u0438\u043a\u0430\u0442 clean-\u0432\u0435\u0440\u0441\u0438\u0438 "
    "'clean_ekup_Yemelyanovo_2025', \u0447\u0442\u043e\u0431\u044b "
    "\u0438\u0441\u0442\u043e\u0440\u0438\u044f \u043d\u0435 "
    "\u0443\u0447\u0438\u0442\u044b\u0432\u0430\u043b\u0430\u0441\u044c "
    "\u0434\u0432\u0430\u0436\u0434\u044b."
)
TEMPERATURE_LABEL = "\u0422\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u0430"
SPARSE_TEMPERATURE_QUALITY_LABEL = "\u041d\u0438\u0437\u043a\u043e\u0435 \u043f\u043e\u043a\u0440\u044b\u0442\u0438\u0435"
SPARSE_TEMPERATURE_COVERAGE_DISPLAY = "3/365 \u0434\u043d\u0435\u0439 (0,8%)"
SPARSE_TEMPERATURE_STATUS_LABEL = (
    "\u041d\u0438\u0437\u043a\u043e\u0435 \u043f\u043e\u043a\u0440\u044b\u0442\u0438\u0435 "
    "(3/365 \u0434\u043d\u0435\u0439 (0,8%))"
)
SPARSE_TEMPERATURE_SOURCE = (
    "ekup_Yemelyanovo_2023: avg_temperature | "
    "\u043f\u043e\u043a\u0440\u044b\u0442\u0438\u0435 \u043f\u043e "
    "\u0434\u043d\u0435\u0432\u043d\u043e\u0439 \u0438\u0441\u0442\u043e\u0440\u0438\u0438: "
    "3/365 \u0434\u043d\u0435\u0439 (0,8%)"
)
SPARSE_TEMPERATURE_DESCRIPTION = (
    "\u041a\u043e\u043b\u043e\u043d\u043a\u0430 \u0442\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u044b "
    "\u043d\u0430\u0439\u0434\u0435\u043d\u0430, \u043d\u043e "
    "\u043f\u043e\u043a\u0440\u044b\u0442\u0438\u0435 \u043d\u0438\u0437\u043a\u043e\u0435: "
    "\u0442\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u043d\u044b\u0439 "
    "\u043f\u0440\u0438\u0437\u043d\u0430\u043a \u043d\u0435\u043b\u044c\u0437\u044f "
    "\u0441\u0447\u0438\u0442\u0430\u0442\u044c \u043d\u0430\u0434\u0451\u0436\u043d\u044b\u043c "
    "\u0434\u043b\u044f ML \u0438 \u0442\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u043d\u043e\u0439 "
    "\u043f\u043e\u043f\u0440\u0430\u0432\u043a\u0438."
)
SPARSE_TEMPERATURE_NOTE = (
    "\u0422\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u043d\u044b\u0445 "
    "\u0434\u043d\u0435\u0439 \u0441 \u043d\u0435\u043f\u0443\u0441\u0442\u044b\u043c "
    "\u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435\u043c: 3 \u0438\u0437 365 (0,8%); "
    "\u044d\u0442\u043e \u043d\u0438\u0436\u0435 \u043f\u043e\u0440\u043e\u0433\u0430 30 "
    "\u0434\u043d\u0435\u0439 \u0438 20% \u043f\u043e\u043a\u0440\u044b\u0442\u0438\u044f, "
    "\u043f\u043e\u044d\u0442\u043e\u043c\u0443 "
    "\u0442\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u043d\u044b\u0439 "
    "\u043f\u0440\u0438\u0437\u043d\u0430\u043a \u0438\u0441\u043a\u043b\u044e\u0447\u0451\u043d "
    "\u0438\u0437 ML \u0438 \u0438\u0441\u0442\u043e\u0440\u0438\u0447\u0435\u0441\u043a\u0438\u0445 "
    "\u0442\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u043d\u044b\u0445 fallback-"
    "\u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a."
)
COUNT_TABLE_WORKING_METHOD_LABEL = "\u0420\u0430\u0431\u043e\u0447\u0438\u0439 \u043c\u0435\u0442\u043e\u0434"
COUNT_TABLE_BASELINE_ROLE_LABEL = "\u0411\u0430\u0437\u043e\u0432\u0430\u044f \u043c\u043e\u0434\u0435\u043b\u044c"


class MlModelPayloadTests(unittest.TestCase):
    MOJIBAKE_PATTERN = re.compile(r"(Р[ЂЃЉЊЋЌЍЎЏ]|С[ЂЃЉЊЋЌЍЎЏ]|вЂ|\?{3,}|РґРЅРµР№|РїРѕРєСЂ|РљРѕР»РѕРЅРєР°)")
    BACKTEST_OVERVIEW_REQUIRED_KEYS = {
        "folds",
        "min_train_rows",
        "validation_horizon_days",
        "selection_rule",
        "event_selection_rule",
        "classification_threshold",
        "candidate_model_labels",
        "dispersion_ratio",
        "prediction_interval_level",
        "prediction_interval_level_display",
        "prediction_interval_coverage",
        "prediction_interval_coverage_display",
        "prediction_interval_method_label",
        "prediction_interval_coverage_validated",
        "prediction_interval_coverage_note",
        "prediction_interval_calibration_windows",
        "prediction_interval_evaluation_windows",
        "prediction_interval_validation_scheme_key",
        "prediction_interval_validation_scheme_label",
        "prediction_interval_validation_explanation",
        "prediction_interval_calibration_range_label",
        "prediction_interval_evaluation_range_label",
        "rolling_scheme_label",
    }
    SUMMARY_REQUIRED_KEYS = {
        "selected_table_label",
        "slice_label",
        "history_period_label",
        "history_window_label",
        "model_label",
        "count_model_label",
        "event_model_label",
        "event_backtest_model_label",
        "backtest_method_label",
        "prediction_interval_level_display",
        "prediction_interval_coverage_display",
        "prediction_interval_method_label",
    }
    QUALITY_ASSESSMENT_REQUIRED_KEYS = {
        "ready",
        "title",
        "subtitle",
        "methodology_items",
        "metric_cards",
        "event_metric_cards",
        "interval_card",
        "model_choice",
        "count_table",
        "event_table",
        "dissertation_points",
    }

    def tearDown(self) -> None:
        ml_core.clear_ml_model_cache()

    @staticmethod
    def _build_daily_history(days: int) -> list[dict[str, object]]:
        history = []
        current_date = date(2024, 1, 1)
        for index in range(days):
            history.append(
                {
                    "date": current_date,
                    "count": float((1 if index % 4 == 0 else 0) + (1 if index % 7 == 0 else 0)),
                    "avg_temperature": float((index % 15) - 5),
                }
            )
            current_date += timedelta(days=1)
        return history

    @staticmethod
    def _build_all_positive_history(days: int = 120) -> list[dict[str, object]]:
        history = []
        current_date = date(2024, 1, 1)
        for index in range(days):
            history.append(
                {
                    "date": current_date,
                    "count": float(1 if index % 2 == 0 else 2),
                    "avg_temperature": 10.0,
                }
            )
            current_date += timedelta(days=1)
        return history

    @staticmethod
    def _build_sparse_temperature_history_2023() -> list[dict[str, object]]:
        history = []
        current_date = date(2023, 1, 1)
        for index in range(365):
            history.append(
                {
                    "date": current_date,
                    "count": float(1 if index % 19 == 0 else 0),
                    "avg_temperature": float(index) if index < 3 else None,
                }
            )
            current_date += timedelta(days=1)
        return history

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

    def _build_service_payload(self, days: int) -> dict[str, object]:
        table_options = [{"value": "fires", "label": "fires"}]
        option_catalog = {
            "causes": [{"value": "all", "label": "all"}],
            "object_categories": [{"value": "all", "label": "all"}],
        }
        metadata_items = [
            {
                "table_name": "fires",
                "resolved_columns": {
                    "date": "fire_date",
                    "temperature": "avg_temperature",
                },
            }
        ]

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with (
                patch.object(ml_core, "_build_forecasting_table_options", return_value=table_options),
                patch.object(ml_core, "_resolve_forecasting_selection", return_value="fires"),
                patch.object(ml_core, "_selected_source_tables", return_value=["fires"]),
                patch.object(ml_core, "_parse_forecast_days", return_value=7),
                patch.object(ml_core, "_parse_history_window", return_value="all"),
                patch.object(ml_core, "_collect_forecasting_metadata", return_value=(metadata_items, [])),
                patch.object(ml_core, "_build_option_catalog_sql", return_value=option_catalog),
                patch.object(ml_core, "_resolve_option_value", side_effect=_resolve_option),
                patch.object(ml_core, "_build_daily_history_sql", return_value=self._build_daily_history(days)),
                patch.object(ml_core, "_count_forecasting_records_sql", return_value=days),
            ):
                return ml_core.get_ml_model_data(
                    table_name="fires",
                    cause="all",
                    object_category="all",
                    temperature="",
                    forecast_days="7",
                    history_window="all",
                )

    def _interval_card(self, quality: dict[str, object]) -> dict[str, object]:
        return quality["interval_card"]

    def _interval_methodology_item(self, quality: dict[str, object]) -> dict[str, object]:
        return next(
            item
            for item in quality["methodology_items"]
            if item.get("label") == "Интервал прогноза"
        )

    def _assert_backtest_overview_shape(self, overview: dict[str, object]) -> None:
        self.assertTrue(self.BACKTEST_OVERVIEW_REQUIRED_KEYS.issubset(set(overview)))

    def _assert_summary_shape(self, summary: dict[str, object]) -> None:
        self.assertTrue(self.SUMMARY_REQUIRED_KEYS.issubset(set(summary)))

    def _assert_quality_assessment_shape(self, quality: dict[str, object]) -> None:
        self.assertTrue(self.QUALITY_ASSESSMENT_REQUIRED_KEYS.issubset(set(quality)))
        self.assertTrue({"title", "lead", "body", "facts"}.issubset(set(quality["model_choice"])))
        self.assertTrue({"title", "rows", "empty_message"}.issubset(set(quality["count_table"])))
        self.assertTrue({"title", "rows", "empty_message"}.issubset(set(quality["event_table"])))

    def _assert_validated_contract_consistency(
        self,
        overview: dict[str, object],
        summary: dict[str, object],
        quality: dict[str, object],
    ) -> None:
        coverage_card = self._interval_card(quality)
        interval_item = self._interval_methodology_item(quality)

        self.assertTrue(overview["prediction_interval_coverage_validated"])
        self.assertNotEqual(overview["prediction_interval_validation_scheme_key"], "not_validated")
        self.assertNotEqual(summary["prediction_interval_coverage_display"], "—")
        self.assertEqual(summary["prediction_interval_coverage_display"], coverage_card["value"])
        self.assertEqual(interval_item["value"], summary["prediction_interval_level_display"])
        self.assertIn("Адаптивный конформный интервал", summary["prediction_interval_method_label"])
        self.assertIn(summary["prediction_interval_method_label"], coverage_card["meta"])
        self.assertIn(summary["prediction_interval_method_label"], interval_item["meta"])
        self.assertIn("проверка схемой", summary["prediction_interval_method_label"])
        self.assertIn("проверка по истории", coverage_card["meta"])
        self.assertIn("Покрытие оценивается только на", coverage_card["meta"])
        self.assertIn("jackknife+ для временного ряда", coverage_card["meta"])
        self.assertEqual(coverage_card["meta"], interval_item["meta"])

    def _assert_unavailable_contract_consistency(
        self,
        overview: dict[str, object],
        summary: dict[str, object],
        quality: dict[str, object],
    ) -> None:
        coverage_card = self._interval_card(quality)
        interval_item = self._interval_methodology_item(quality)

        self.assertFalse(overview["prediction_interval_coverage_validated"])
        self.assertEqual(overview["prediction_interval_validation_scheme_key"], "not_validated")
        self.assertEqual(overview["prediction_interval_validation_scheme_label"], "validated out-of-sample coverage unavailable")
        self.assertEqual(summary["prediction_interval_coverage_display"], "—")
        self.assertEqual(coverage_card["value"], "—")
        self.assertEqual(interval_item["value"], summary["prediction_interval_level_display"])
        self.assertIn("Адаптивный конформный интервал", summary["prediction_interval_method_label"])
        self.assertIn(summary["prediction_interval_method_label"], coverage_card["meta"])
        self.assertIn(summary["prediction_interval_method_label"], interval_item["meta"])
        self.assertIn("проверка покрытия на отложенных окнах пока недоступна", summary["prediction_interval_method_label"])
        self.assertIn("Покрытие на отложенных окнах пока недоступно", coverage_card["meta"])
        self.assertIn("последовательной проверки интервала", coverage_card["meta"])
        self.assertEqual(coverage_card["meta"], interval_item["meta"])

    def _build_all_mode_payload_with_raw_clean_pair(self) -> dict[str, object]:
        raw_table = "ekup_Yemelyanovo_2025"
        clean_table = f"clean_{raw_table}"
        table_options = [
            {"value": raw_table, "label": raw_table},
            {"value": clean_table, "label": clean_table},
        ]
        option_catalog = {
            "causes": [{"value": "all", "label": "all"}],
            "object_categories": [{"value": "all", "label": "all"}],
        }
        metadata_items = [
            {
                "table_name": clean_table,
                "resolved_columns": {
                    "date": "fire_date",
                    "temperature": "avg_temperature",
                    "cause": None,
                    "object_category": None,
                },
            }
        ]
        daily_history = self._build_all_positive_history()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with (
                patch.object(ml_core, "_build_forecasting_table_options", return_value=table_options),
                patch.object(ml_core, "_parse_forecast_days", return_value=7),
                patch.object(ml_core, "_parse_history_window", return_value="all"),
                patch.object(ml_core, "_collect_forecasting_metadata", return_value=(metadata_items, [])) as metadata_mock,
                patch.object(ml_core, "_build_option_catalog_sql", return_value=option_catalog) as catalog_mock,
                patch.object(ml_core, "_resolve_option_value", side_effect=_resolve_option),
                patch.object(ml_core, "_build_daily_history_sql", return_value=daily_history) as history_mock,
                patch.object(ml_core, "_count_forecasting_records_sql", return_value=len(daily_history)) as count_mock,
            ):
                payload = ml_core.get_ml_model_data(
                    table_name="all",
                    cause="all",
                    object_category="all",
                    temperature="",
                    forecast_days="7",
                    history_window="all",
                )

        metadata_mock.assert_called_once_with([clean_table])
        return {
            "payload": payload,
            "raw_table": raw_table,
            "clean_table": clean_table,
            "metadata_tables": metadata_mock.call_args.args[0],
            "catalog_tables": catalog_mock.call_args.args[0],
            "history_tables": history_mock.call_args.args[0],
            "count_tables": count_mock.call_args.args[0],
        }

    def _build_sparse_temperature_payload_2023(self) -> dict[str, object]:
        table_name = "ekup_Yemelyanovo_2023"
        table_options = [{"value": table_name, "label": table_name}]
        option_catalog = {
            "causes": [{"value": "all", "label": "all"}],
            "object_categories": [{"value": "all", "label": "all"}],
        }
        metadata_items = [
            {
                "table_name": table_name,
                "resolved_columns": {
                    "date": "fire_date",
                    "temperature": "avg_temperature",
                    "cause": None,
                    "object_category": None,
                },
                "column_quality": {
                    "temperature": {
                        "non_null_days": 3,
                        "total_days": 429,
                        "coverage": 3 / 429,
                        "usable": False,
                        "quality_key": "sparse",
                        "quality_label": SPARSE_TEMPERATURE_QUALITY_LABEL,
                    }
                },
            }
        ]
        daily_history = self._build_sparse_temperature_history_2023()
        filtered_records_count = sum(int(item["count"]) for item in daily_history)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with (
                patch.object(ml_core, "_build_forecasting_table_options", return_value=table_options),
                patch.object(ml_core, "_parse_forecast_days", return_value=7),
                patch.object(ml_core, "_parse_history_window", return_value="all"),
                patch.object(ml_core, "_collect_forecasting_metadata", return_value=(metadata_items, [])),
                patch.object(ml_core, "_build_option_catalog_sql", return_value=option_catalog),
                patch.object(ml_core, "_resolve_option_value", side_effect=_resolve_option),
                patch.object(ml_core, "_build_daily_history_sql", return_value=daily_history),
                patch.object(ml_core, "_count_forecasting_records_sql", return_value=filtered_records_count),
            ):
                payload = ml_core.get_ml_model_data(
                    table_name=table_name,
                    cause="all",
                    object_category="all",
                    temperature="",
                    forecast_days="7",
                    history_window="all",
                )

        temperature_card = next(item for item in payload["features"] if item["label"] == TEMPERATURE_LABEL)
        return {
            "payload": payload,
            "temperature_card": temperature_card,
        }

    def _assert_all_mode_user_facing_payload_contract(self, payload: dict[str, object]) -> None:
        summary = payload["summary"]
        event_table = payload["quality_assessment"]["event_table"]

        self.assertFalse(summary["event_probability_enabled"])
        self.assertFalse(summary["event_backtest_available"])
        self.assertEqual(summary["event_backtest_model_label"], ALL_MODE_EVENT_BACKTEST_MODEL_LABEL)
        self.assertEqual(event_table["reason_code"], ALL_MODE_EVENT_REASON_CODE)
        self.assertEqual(event_table["title"], ALL_MODE_EVENT_TABLE_TITLE)
        self.assertEqual(event_table["empty_message"], ALL_MODE_EVENT_EMPTY_MESSAGE)
        self.assertEqual(event_table["rows"], [])
        self.assertNotIn(
            "\u043d\u0435\u0434\u043e\u0441\u0442\u0430\u0442\u043e\u0447\u043d\u043e \u043e\u043a\u043e\u043d",
            str(event_table["empty_message"]).lower(),
        )
        self._assert_no_mojibake(
            {
                "summary": summary,
                "event_table": event_table,
                "notes": payload["notes"],
            },
            context="all-mode user-facing payload",
        )

    def _assert_sparse_temperature_user_facing_payload_contract(
        self,
        payload: dict[str, object],
        temperature_card: dict[str, object],
    ) -> None:
        self.assertFalse(temperature_card["usable"])
        self.assertEqual(temperature_card["coverage_display"], SPARSE_TEMPERATURE_COVERAGE_DISPLAY)
        self.assertEqual(temperature_card["status_label"], SPARSE_TEMPERATURE_STATUS_LABEL)
        self.assertEqual(temperature_card["source"], SPARSE_TEMPERATURE_SOURCE)
        self.assertEqual(temperature_card["description"], SPARSE_TEMPERATURE_DESCRIPTION)
        self.assertIn(SPARSE_TEMPERATURE_NOTE, payload["notes"])
        self._assert_no_mojibake(
            {
                "summary": payload["summary"],
                "temperature_card": temperature_card,
                "notes": payload["notes"],
            },
            context="sparse-temperature user-facing payload",
        )

    def _assert_count_table_stays_user_facing(
        self,
        count_table: dict[str, object],
        *,
        context: str,
    ) -> None:
        rows = count_table["rows"]
        self.assertGreaterEqual(len(rows), 2)
        self.assertTrue(any(row["selection_label"] == COUNT_TABLE_WORKING_METHOD_LABEL for row in rows))
        self.assertTrue(any(row["role_label"] == COUNT_TABLE_BASELINE_ROLE_LABEL for row in rows))
        for row in rows:
            self.assertTrue(str(row["method_label"]))
            self.assertTrue(str(row["role_label"]))
            self.assertTrue(str(row["selection_label"]))
        self._assert_no_mojibake(count_table, context=context)

    def test_all_mode_deduplicates_source_tables_before_building_ml_payload(self) -> None:
        artifact = self._build_all_mode_payload_with_raw_clean_pair()

        self.assertEqual(artifact["metadata_tables"], [artifact["clean_table"]])
        self.assertEqual(artifact["catalog_tables"], [artifact["clean_table"]])
        self.assertEqual(artifact["history_tables"], [artifact["clean_table"]])
        self.assertEqual(artifact["count_tables"], [artifact["clean_table"]])

    def test_all_mode_user_facing_payload_contract_is_stable(self) -> None:
        payload = self._build_all_mode_payload_with_raw_clean_pair()["payload"]
        self._assert_all_mode_user_facing_payload_contract(payload)

    def test_all_mode_hides_event_probability_for_one_class_backtest(self) -> None:
        payload = self._build_all_mode_payload_with_raw_clean_pair()["payload"]

        self.assertFalse(payload["summary"]["event_probability_enabled"])
        self.assertFalse(payload["summary"]["event_backtest_available"])
        self.assertEqual(payload["quality_assessment"]["event_table"]["reason_code"], ALL_MODE_EVENT_REASON_CODE)
        self.assertEqual(
            payload["quality_assessment"]["event_table"]["empty_message"],
            ALL_MODE_EVENT_EMPTY_MESSAGE,
        )
        self.assertNotIn(
            "\u043d\u0435\u0434\u043e\u0441\u0442\u0430\u0442\u043e\u0447\u043d\u043e \u043e\u043a\u043e\u043d",
            str(payload["quality_assessment"]["event_table"]["empty_message"]).lower(),
        )

    def test_all_mode_uses_stable_summary_and_event_table_labels(self) -> None:
        payload = self._build_all_mode_payload_with_raw_clean_pair()["payload"]

        self.assertEqual(payload["summary"]["event_backtest_model_label"], ALL_MODE_EVENT_BACKTEST_MODEL_LABEL)
        self.assertEqual(
            payload["quality_assessment"]["event_table"]["title"],
            ALL_MODE_EVENT_TABLE_TITLE,
        )
        self._assert_no_mojibake(
            {
                "summary": payload["summary"],
                "event_table": payload["quality_assessment"]["event_table"],
            },
            context="all-mode event labels",
        )

    def test_all_mode_probability_notes_remain_honest_and_stable(self) -> None:
        artifact = self._build_all_mode_payload_with_raw_clean_pair()
        payload = artifact["payload"]

        self.assertIn(
            ALL_MODE_RAW_CLEAN_NOTE,
            payload["notes"],
        )
        self.assertIn(
            ALL_MODE_EVENT_EMPTY_MESSAGE,
            payload["notes"],
        )
        self._assert_no_mojibake(payload["notes"], context="all-mode probability notes")

    def test_all_mode_service_sections_stay_consistent(self) -> None:
        payload = self._build_all_mode_payload_with_raw_clean_pair()["payload"]
        quality = payload["quality_assessment"]

        self.assertEqual(payload["summary"]["selected_table_label"], "Все таблицы")
        self.assertEqual(payload["summary"]["event_backtest_model_label"], ALL_MODE_EVENT_BACKTEST_MODEL_LABEL)
        self.assertEqual(quality["event_table"]["empty_message"], ALL_MODE_EVENT_EMPTY_MESSAGE)
        self.assertIn(ALL_MODE_EVENT_EMPTY_MESSAGE, payload["notes"])
        self._assert_count_table_stays_user_facing(
            quality["count_table"],
            context="all-mode count comparison table",
        )

    def test_sparse_temperature_card_fields_remain_stable(self) -> None:
        artifact = self._build_sparse_temperature_payload_2023()
        temperature_card = artifact["temperature_card"]

        self.assertFalse(temperature_card["usable"])
        self.assertEqual(temperature_card["coverage_display"], SPARSE_TEMPERATURE_COVERAGE_DISPLAY)
        self.assertEqual(temperature_card["status_label"], SPARSE_TEMPERATURE_STATUS_LABEL)
        self.assertEqual(
            temperature_card["source"],
            SPARSE_TEMPERATURE_SOURCE,
        )
        self.assertEqual(
            temperature_card["description"],
            SPARSE_TEMPERATURE_DESCRIPTION,
        )
        self._assert_no_mojibake(
            temperature_card,
            context="sparse-temperature card",
        )

    def test_sparse_temperature_user_facing_payload_contract_is_stable(self) -> None:
        artifact = self._build_sparse_temperature_payload_2023()
        self._assert_sparse_temperature_user_facing_payload_contract(
            artifact["payload"],
            artifact["temperature_card"],
        )

    def test_sparse_temperature_notes_remain_stable(self) -> None:
        artifact = self._build_sparse_temperature_payload_2023()
        payload = artifact["payload"]

        self.assertIn(
            SPARSE_TEMPERATURE_NOTE,
            payload["notes"],
        )
        self._assert_no_mojibake(
            {
                "summary": payload["summary"],
                "notes": payload["notes"],
            },
            context="sparse-temperature notes",
        )

    def test_sparse_temperature_service_sections_stay_consistent(self) -> None:
        artifact = self._build_sparse_temperature_payload_2023()
        payload = artifact["payload"]
        quality = payload["quality_assessment"]

        self.assertEqual(payload["summary"]["selected_table_label"], "ekup_Yemelyanovo_2023")
        self.assertIn(SPARSE_TEMPERATURE_NOTE, payload["notes"])
        self._assert_sparse_temperature_user_facing_payload_contract(
            payload,
            artifact["temperature_card"],
        )
        self._assert_count_table_stays_user_facing(
            quality["count_table"],
            context="sparse-temperature count comparison table",
        )


if __name__ == "__main__":
    unittest.main()
