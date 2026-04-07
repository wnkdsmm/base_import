import re
import unittest
import warnings
from datetime import date, timedelta
from unittest.mock import patch

from app.services.ml_model import core as ml_core
from app.services.ml_model import training as ml_training


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

    def _build_service_payload(
        self,
        days: int,
        *,
        forecast_days: int = 7,
        daily_history: list[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        table_options = [{"value": "fires", "label": "fires"}]
        option_catalog = {
            "causes": [{"value": "all", "label": "all"}],
            "object_categories": [{"value": "all", "label": "all"}],
        }
        history = daily_history if daily_history is not None else self._build_daily_history(days)
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
                patch.object(ml_core, "_parse_forecast_days", return_value=forecast_days),
                patch.object(ml_core, "_parse_history_window", return_value="all"),
                patch.object(ml_core, "_collect_forecasting_metadata", return_value=(metadata_items, [])),
                patch.object(ml_core, "_build_option_catalog_sql", return_value=option_catalog),
                patch.object(ml_core, "_resolve_option_value", side_effect=_resolve_option),
                patch.object(ml_core, "_build_daily_history_sql", return_value=history),
                patch.object(ml_core, "_count_forecasting_records_sql", return_value=len(history)),
            ):
                return ml_core.get_ml_model_data(
                    table_name="fires",
                    cause="all",
                    object_category="all",
                    temperature="",
                    forecast_days=str(forecast_days),
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

    def test_service_payload_forecast_rows_stay_in_sync_with_training_horizon_metadata_for_30_day_forecast(self) -> None:
        daily_history = self._build_daily_history(180)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            training_result = ml_training._train_ml_model(daily_history, 30, None)

        self.assertTrue(training_result["is_ready"], msg=training_result.get("message"))
        payload = self._build_service_payload(180, forecast_days=30, daily_history=daily_history)
        summary = payload["summary"]
        self._assert_summary_shape(summary)

        self.assertEqual(summary["prediction_interval_coverage_display"], training_result["prediction_interval_coverage_display"])

        overview = training_result["backtest_overview"]
        for horizon_day in (1, 7, 14, 30):
            training_summary = training_result["horizon_summaries"][str(horizon_day)]
            training_row = training_result["forecast_rows"][horizon_day - 1]
            payload_row = payload["forecast_rows"][horizon_day - 1]

            self.assertEqual(int(payload_row["horizon_days"]), horizon_day)
            self.assertAlmostEqual(
                float(overview["prediction_interval_coverage_by_horizon"][str(horizon_day)]),
                float(training_summary["prediction_interval_coverage"]),
            )
            self.assertEqual(
                overview["prediction_interval_coverage_display_by_horizon"][str(horizon_day)],
                training_summary["prediction_interval_coverage_display"],
            )
            self.assertAlmostEqual(
                float(training_row["prediction_interval_coverage"]),
                float(training_summary["prediction_interval_coverage"]),
            )
            self.assertAlmostEqual(
                float(payload_row["prediction_interval_coverage"]),
                float(training_summary["prediction_interval_coverage"]),
            )
            self.assertEqual(
                payload_row["prediction_interval_coverage_display"],
                training_summary["prediction_interval_coverage_display"],
            )
            self.assertEqual(
                bool(payload_row["prediction_interval_coverage_validated"]),
                bool(training_summary["prediction_interval_coverage_validated"]),
            )

        self.assertEqual(
            summary["prediction_interval_coverage_display"],
            training_result["horizon_summaries"]["30"]["prediction_interval_coverage_display"],
        )

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

    def test_training_payload_exposes_interval_validation_metadata_in_backtest_overview(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = ml_training._train_ml_model(self._build_daily_history(75), 7, None)

        self.assertTrue(result["is_ready"], msg=result.get("message"))
        overview = result["backtest_overview"]
        self._assert_backtest_overview_shape(overview)
        self.assertTrue(overview["prediction_interval_coverage_validated"])
        self.assertGreater(int(overview["prediction_interval_calibration_windows"]), 0)
        self.assertGreater(int(overview["prediction_interval_evaluation_windows"]), 0)
        self.assertNotEqual(overview["prediction_interval_calibration_range_label"], "—")
        self.assertNotEqual(overview["prediction_interval_evaluation_range_label"], "—")
        self.assertEqual(int(overview["validation_horizon_days"]), 7)
        self.assertIn(
            overview["prediction_interval_validation_scheme_key"],
            {"blocked_forward_cv", "rolling_split_conformal"},
        )
        self.assertTrue(overview["prediction_interval_validation_scheme_label"])
        self.assertIn("validated out-of-sample coverage", overview["prediction_interval_validation_explanation"])
        self.assertIn("Coverage is evaluated only on", overview["prediction_interval_coverage_note"])

    def test_service_payload_hides_unvalidated_coverage_when_honest_split_is_unavailable(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            training_result = ml_training._train_ml_model(self._build_daily_history(65), 7, None)
        self.assertTrue(training_result["is_ready"], msg=training_result.get("message"))
        overview = training_result["backtest_overview"]

        self._assert_backtest_overview_shape(overview)
        self.assertEqual(int(overview["prediction_interval_evaluation_windows"]), 0)

        payload = self._build_service_payload(65)
        summary = payload["summary"]
        quality = payload["quality_assessment"]
        self._assert_summary_shape(summary)
        self._assert_quality_assessment_shape(quality)
        self._assert_unavailable_contract_consistency(overview, summary, quality)

    def test_service_payload_shows_validated_out_of_sample_coverage_when_windows_are_sufficient(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            training_result = ml_training._train_ml_model(self._build_daily_history(75), 7, None)
        self.assertTrue(training_result["is_ready"], msg=training_result.get("message"))
        overview = training_result["backtest_overview"]

        self._assert_backtest_overview_shape(overview)

        payload = self._build_service_payload(75)
        summary = payload["summary"]
        quality = payload["quality_assessment"]
        self._assert_summary_shape(summary)
        self._assert_quality_assessment_shape(quality)
        self._assert_validated_contract_consistency(overview, summary, quality)

    def test_service_payload_contract_shape_and_notes_for_validated_interval_status(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            training_result = ml_training._train_ml_model(self._build_daily_history(75), 7, None)
        self.assertTrue(training_result["is_ready"], msg=training_result.get("message"))
        overview = training_result["backtest_overview"]

        payload = self._build_service_payload(75)
        summary = payload["summary"]
        quality = payload["quality_assessment"]
        coverage_card = self._interval_card(quality)
        interval_item = self._interval_methodology_item(quality)

        self._assert_summary_shape(summary)
        self._assert_quality_assessment_shape(quality)
        self.assertEqual(summary["prediction_interval_level_display"], "80%")
        self.assertTrue(str(coverage_card["label"]).startswith("Покрытие 80% интервала"))
        self.assertTrue(str(interval_item["label"]))
        self.assertIn("Адаптивный конформный интервал", summary["prediction_interval_method_label"])
        self.assertIn("проверка по истории", summary["prediction_interval_method_label"])
        self.assertIn("Покрытие оценивается только на", coverage_card["meta"])
        self.assertIn("начиная с 20", coverage_card["meta"])
        self.assertIn("перекалибруются", coverage_card["meta"])
        self.assertEqual(coverage_card["meta"], interval_item["meta"])

    def test_service_payload_contract_shape_and_notes_for_unavailable_interval_status(self) -> None:
        payload = self._build_service_payload(65)
        summary = payload["summary"]
        quality = payload["quality_assessment"]
        coverage_card = self._interval_card(quality)
        interval_item = self._interval_methodology_item(quality)

        self._assert_summary_shape(summary)
        self._assert_quality_assessment_shape(quality)
        self.assertEqual(summary["prediction_interval_level_display"], "80%")
        self.assertTrue(str(coverage_card["label"]).startswith("Покрытие 80% интервала"))
        self.assertTrue(str(interval_item["label"]))
        self.assertEqual(
            summary["prediction_interval_method_label"],
            "Адаптивный конформный интервал по группам ожидаемого числа пожаров; проверка покрытия на отложенных окнах пока недоступна",
        )
        self.assertEqual(coverage_card["value"], "—")
        self.assertIn("Покрытие на отложенных окнах пока недоступно", coverage_card["meta"])
        self.assertEqual(coverage_card["meta"], interval_item["meta"])
