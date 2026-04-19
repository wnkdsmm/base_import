import unittest
from datetime import date, timedelta

from app.services.forecasting.presentation import _build_feature_cards_with_quality
from app.services.forecasting.data import _temperature_quality_from_daily_history
from app.services.ml_model.training.presentation import _build_notes
from app.services.ml_model.training.training import _train_ml_model
from tests.mojibake_check import encode_as_mojibake


TEMPERATURE_LABEL = "\u0422\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u0430"
SPARSE_QUALITY_LABEL = "\u041d\u0438\u0437\u043a\u043e\u0435 \u043f\u043e\u043a\u0440\u044b\u0442\u0438\u0435"
SPARSE_OVERRIDE_COVERAGE_DISPLAY = "3/365 \u0434\u043d\u0435\u0439 (0,8%)"
SPARSE_OVERRIDE_STATUS_LABEL = "\u041d\u0438\u0437\u043a\u043e\u0435 \u043f\u043e\u043a\u0440\u044b\u0442\u0438\u0435 (3/365 \u0434\u043d\u0435\u0439 (0,8%))"
SPARSE_OVERRIDE_SOURCE = (
    "ekup_Yemelyanovo_2023: avg_temperature | "
    "\u043f\u043e\u043a\u0440\u044b\u0442\u0438\u0435 \u043f\u043e \u0434\u043d\u0435\u0432\u043d\u043e\u0439 "
    "\u0438\u0441\u0442\u043e\u0440\u0438\u0438: 3/365 \u0434\u043d\u0435\u0439 (0,8%)"
)
SPARSE_OVERRIDE_DESCRIPTION = (
    "\u041a\u043e\u043b\u043e\u043d\u043a\u0430 \u0442\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u044b "
    "\u043d\u0430\u0439\u0434\u0435\u043d\u0430, \u043d\u043e \u043f\u043e\u043a\u0440\u044b\u0442\u0438\u0435 "
    "\u043d\u0438\u0437\u043a\u043e\u0435: \u0442\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u043d\u044b\u0439 "
    "\u043f\u0440\u0438\u0437\u043d\u0430\u043a \u043d\u0435\u043b\u044c\u0437\u044f \u0441\u0447\u0438\u0442\u0430\u0442\u044c "
    "\u043d\u0430\u0434\u0451\u0436\u043d\u044b\u043c \u0434\u043b\u044f ML \u0438 \u0442\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u043d\u043e\u0439 "
    "\u043f\u043e\u043f\u0440\u0430\u0432\u043a\u0438."
)
MOJIBAKE_TOKENS = tuple(
    encode_as_mojibake(token)
    for token in ("\u0434\u043d\u0435\u0439", "\u043f\u043e\u043a\u0440", "\u041a\u043e\u043b\u043e\u043d\u043a\u0430")
)


class SparseTemperatureCoverageTests(unittest.TestCase):
    @staticmethod
    def _build_sparse_daily_history() -> list[dict[str, object]]:
        history: list[dict[str, object]] = []
        current_date = date(2024, 1, 1)
        for index in range(120):
            history.append(
                {
                    "date": current_date,
                    "count": float(1 if index % 5 == 0 else 0),
                    "avg_temperature": float(-5 + index) if index < 5 else None,
                }
            )
            current_date += timedelta(days=1)
        return history

    def test_sparse_temperature_coverage_disables_temperature_feature_and_adds_note(self) -> None:
        daily_history = self._build_sparse_daily_history()

        result = _train_ml_model(
            daily_history=daily_history,
            forecast_days=7,
            scenario_temperature=None,
        )

        self.assertTrue(result["is_ready"], msg=result.get("message"))
        self.assertFalse(result["temperature_feature_enabled"])
        self.assertEqual(result["temperature_non_null_days"], 5)
        self.assertEqual(result["temperature_total_days"], 120)
        self.assertLess(float(result["temperature_coverage"]), 0.2)
        self.assertIn("5 \u0438\u0437 120", str(result["temperature_note"]))
        self.assertIn("20% \u043f\u043e\u043a\u0440\u044b\u0442\u0438\u044f", str(result["temperature_note"]))

        notes = _build_notes(
            preload_notes=[],
            metadata_items=[{"resolved_columns": {"temperature": "avg_temperature"}}],
            filtered_records_count=len(daily_history),
            daily_history=daily_history,
            ml_result=result,
            scenario_temperature=None,
            source_tables=["ekup_Yemelyanovo_sparse"],
        )
        self.assertIn(str(result["temperature_note"]), notes)

    def test_feature_card_reports_temperature_quality_and_coverage(self) -> None:
        feature_cards = _build_feature_cards_with_quality(
            [
                {
                    "table_name": "ekup_Yemelyanovo_2023",
                    "resolved_columns": {
                        "date": "fire_date",
                        "temperature": "avg_temperature",
                        "cause": None,
                        "object_category": None,
                    },
                    "column_quality": {
                        "temperature": {
                            "non_null_days": 3,
                            "total_days": 365,
                            "coverage": 3 / 365,
                            "usable": False,
                            "quality_key": "sparse",
                            "quality_label": SPARSE_QUALITY_LABEL,
                        }
                    },
                }
            ]
        )

        temperature_card = next(item for item in feature_cards if item["label"] == TEMPERATURE_LABEL)
        self.assertEqual(temperature_card["status"], "partial")
        self.assertFalse(temperature_card["usable"])
        self.assertIn(SPARSE_QUALITY_LABEL, str(temperature_card["status_label"]))
        self.assertIn("3/365", str(temperature_card["status_label"]))
        self.assertIn("3/365", str(temperature_card["source"]))

    def test_feature_card_temperature_coverage_matches_ml_note(self) -> None:
        daily_history = self._build_sparse_daily_history()
        result = _train_ml_model(
            daily_history=daily_history,
            forecast_days=7,
            scenario_temperature=None,
        )

        feature_cards = _build_feature_cards_with_quality(
            [
                {
                    "table_name": "ekup_Yemelyanovo_2023",
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
                            "quality_label": SPARSE_QUALITY_LABEL,
                        }
                    },
                }
            ],
            temperature_quality=_temperature_quality_from_daily_history(daily_history),
        )

        temperature_card = next(item for item in feature_cards if item["label"] == TEMPERATURE_LABEL)
        ratio = f"{result['temperature_non_null_days']}/{result['temperature_total_days']}"
        self.assertIn(f"{result['temperature_non_null_days']} \u0438\u0437 {result['temperature_total_days']}", str(result["temperature_note"]))
        self.assertIn(ratio, str(temperature_card["coverage_display"]))
        self.assertIn("4,2%", str(temperature_card["coverage_display"]))
        self.assertIn(ratio, str(temperature_card["status_label"]))
        self.assertIn(ratio, str(temperature_card["source"]))
        self.assertNotIn("3/429", str(temperature_card["status_label"]))
        self.assertNotIn("3/429", str(temperature_card["source"]))

    def test_temperature_card_uses_readable_russian_strings_without_mojibake(self) -> None:
        daily_history: list[dict[str, object]] = []
        current_date = date(2023, 1, 1)
        for index in range(365):
            daily_history.append(
                {
                    "date": current_date,
                    "count": float(1 if index % 21 == 0 else 0),
                    "avg_temperature": float(index) if index < 3 else None,
                }
            )
            current_date += timedelta(days=1)

        feature_cards = _build_feature_cards_with_quality(
            [
                {
                    "table_name": "ekup_Yemelyanovo_2023",
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
                            "quality_label": SPARSE_QUALITY_LABEL,
                        }
                    },
                }
            ],
            temperature_quality=_temperature_quality_from_daily_history(daily_history),
        )

        temperature_card = next(item for item in feature_cards if item["label"] == TEMPERATURE_LABEL)
        self.assertEqual(temperature_card["coverage_display"], SPARSE_OVERRIDE_COVERAGE_DISPLAY)
        self.assertEqual(temperature_card["status_label"], SPARSE_OVERRIDE_STATUS_LABEL)
        self.assertEqual(temperature_card["source"], SPARSE_OVERRIDE_SOURCE)
        self.assertEqual(temperature_card["description"], SPARSE_OVERRIDE_DESCRIPTION)

        checked_fields = (
            str(temperature_card["coverage_display"]),
            str(temperature_card["status_label"]),
            str(temperature_card["source"]),
            str(temperature_card["description"]),
        )
        for token in MOJIBAKE_TOKENS:
            for value in checked_fields:
                self.assertNotIn(token, value)


if __name__ == "__main__":
    unittest.main()
