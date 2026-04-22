import unittest
from datetime import date

from app.services.forecasting.shaping import _build_forecast_rows
from app.services.shared.request_state import build_forecasting_cache_key


class ForecastingAnchorDateTests(unittest.TestCase):
    def test_forecast_rows_start_from_current_user_date_when_history_is_in_the_past(self) -> None:
        daily_history = [
            {"date": date(2024, 1, 1), "count": 1, "avg_temperature": -5.0},
            {"date": date(2024, 1, 2), "count": 0, "avg_temperature": -4.0},
            {"date": date(2024, 1, 3), "count": 2, "avg_temperature": -3.0},
        ]

        rows = _build_forecast_rows(
            daily_history,
            forecast_days=3,
            temperature_value=None,
            current_user_date=date(2024, 2, 10),
        )

        self.assertEqual(
            [row["date"] for row in rows],
            ["2024-02-10", "2024-02-11", "2024-02-12"],
        )

    def test_forecast_rows_do_not_overlap_with_history_when_user_day_is_behind(self) -> None:
        daily_history = [
            {"date": date(2024, 1, 1), "count": 1, "avg_temperature": -5.0},
            {"date": date(2024, 1, 2), "count": 0, "avg_temperature": -4.0},
            {"date": date(2024, 1, 3), "count": 2, "avg_temperature": -3.0},
        ]

        rows = _build_forecast_rows(
            daily_history,
            forecast_days=2,
            temperature_value=None,
            current_user_date=date(2024, 1, 2),
        )

        self.assertEqual(rows[0]["date"], "2024-01-04")


class ForecastingCacheKeyDateTests(unittest.TestCase):
    def test_cache_key_changes_with_current_user_date(self) -> None:
        first = build_forecasting_cache_key(
            "fires",
            ["fires"],
            "all",
            "all",
            "all",
            "",
            14,
            "all",
            "2024-02-10",
            False,
        )
        second = build_forecasting_cache_key(
            "fires",
            ["fires"],
            "all",
            "all",
            "all",
            "",
            14,
            "all",
            "2024-02-11",
            False,
        )

        self.assertNotEqual(first, second)


if __name__ == "__main__":
    unittest.main()
