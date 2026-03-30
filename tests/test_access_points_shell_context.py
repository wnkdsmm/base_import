import unittest
from unittest.mock import patch

from app.services.access_points import core


class AccessPointsShellContextTests(unittest.TestCase):
    def test_shell_context_prefers_cached_payload_when_available(self) -> None:
        table_options = [{"value": "fires", "label": "Пожары"}]
        cached_payload = {
            "bootstrap_mode": "resolved",
            "has_data": True,
            "filters": {
                "table_name": "fires",
                "available_tables": table_options,
            },
            "summary": {
                "selected_table_label": "Пожары",
                "total_points_display": "12",
            },
        }

        with (
            patch.object(core, "_build_access_points_table_options", return_value=table_options),
            patch.object(core, "_resolve_selected_table", return_value="fires"),
            patch.object(core, "_selected_source_tables", return_value=["fires"]),
            patch.object(core, "_parse_limit", return_value=25),
            patch.object(core._ACCESS_POINTS_CACHE, "get", return_value=cached_payload),
        ):
            context = core.get_access_points_shell_context(table_name="fires")

        self.assertEqual(context["initial_data"], cached_payload)
        self.assertTrue(context["has_data"])
        self.assertEqual(context["plotly_js"], "")

    def test_shell_context_returns_deferred_payload(self) -> None:
        table_options = [{"value": "fires", "label": "Пожары"}]

        with (
            patch.object(core, "_build_access_points_table_options", return_value=table_options),
            patch.object(core, "_resolve_selected_table", return_value="fires"),
            patch.object(core, "_selected_source_tables", return_value=["fires"]),
            patch.object(core, "_parse_limit", return_value=50),
            patch.object(core._ACCESS_POINTS_CACHE, "get", return_value=None),
        ):
            context = core.get_access_points_shell_context(
                table_name="fires",
                district="North",
                year="2025",
                limit="50",
            )

        data = context["initial_data"]
        self.assertEqual(context["plotly_js"], "")
        self.assertEqual(data["bootstrap_mode"], "deferred")
        self.assertTrue(data["loading"])
        self.assertFalse(data["has_data"])
        self.assertEqual(data["filters"]["table_name"], "fires")
        self.assertEqual(data["filters"]["district"], "North")
        self.assertEqual(data["filters"]["year"], "2025")
        self.assertEqual(data["filters"]["limit"], "50")
        self.assertEqual(data["filters"]["available_tables"], table_options)
        self.assertEqual(data["filters"]["available_districts"][1]["value"], "North")
        self.assertEqual(data["filters"]["available_years"][1]["value"], "2025")
        self.assertIn("shell", data["notes"][0].lower())


if __name__ == "__main__":
    unittest.main()
