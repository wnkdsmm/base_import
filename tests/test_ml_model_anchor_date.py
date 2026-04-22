import unittest
from datetime import date

import pandas as pd

from app.services.ml_model.training.forecast_intervals import _build_future_forecast_rows
from app.services.shared.request_state import build_ml_cache_key


def _build_history_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "count": [1.0, 0.0, 2.0],
            "avg_temperature": [-5.0, -4.0, -3.0],
            "temp_value": [-5.0, -4.0, -3.0],
        }
    )


class MlForecastAnchorDateTests(unittest.TestCase):
    def test_future_forecast_rows_start_from_current_user_date_when_history_is_in_the_past(self) -> None:
        rows = _build_future_forecast_rows(
            frame=_build_history_frame(),
            selected_count_model_key="seasonal_baseline",
            count_model=None,
            event_model=None,
            forecast_days=3,
            scenario_temperature=None,
            interval_calibration={"absolute_error_quantile": 1.0, "level_display": "90%"},
            baseline_expected_count=lambda _train, _day: 1.0,
            current_user_date=date(2024, 2, 10),
        )

        self.assertEqual(
            [row["date"] for row in rows],
            ["2024-02-10", "2024-02-11", "2024-02-12"],
        )

    def test_future_forecast_rows_do_not_overlap_with_history_when_user_day_is_behind(self) -> None:
        rows = _build_future_forecast_rows(
            frame=_build_history_frame(),
            selected_count_model_key="seasonal_baseline",
            count_model=None,
            event_model=None,
            forecast_days=2,
            scenario_temperature=None,
            interval_calibration={"absolute_error_quantile": 1.0, "level_display": "90%"},
            baseline_expected_count=lambda _train, _day: 1.0,
            current_user_date=date(2024, 1, 2),
        )

        self.assertEqual(rows[0]["date"], "2024-01-04")


class MlCacheKeyDateTests(unittest.TestCase):
    def test_ml_cache_key_changes_with_current_user_date(self) -> None:
        first = build_ml_cache_key(
            cache_schema_version=99,
            selected_table="fires",
            cause="all",
            object_category="all",
            temperature="",
            days_ahead=14,
            history_window="all",
            current_user_date="2024-02-10",
        )
        second = build_ml_cache_key(
            cache_schema_version=99,
            selected_table="fires",
            cause="all",
            object_category="all",
            temperature="",
            days_ahead=14,
            history_window="all",
            current_user_date="2024-02-11",
        )

        self.assertNotEqual(first, second)


if __name__ == "__main__":
    unittest.main()
