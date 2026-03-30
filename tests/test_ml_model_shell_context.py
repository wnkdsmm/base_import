import unittest
from unittest.mock import patch

from app.services.ml_model import core


class MlModelShellContextTests(unittest.TestCase):
    def test_shell_context_prefers_cached_payload_when_available(self) -> None:
        cached_payload = {
            "has_data": True,
            "filters": {
                "available_tables": [{"value": "fires", "label": "Пожары"}],
            },
            "quality_assessment": {
                "count_table": {"rows": [{"method_label": "cached"}]},
            },
        }
        request_state = {
            "cache_key": ("fires", "all", "all", "", 14, "all"),
            "table_options": [{"value": "fires", "label": "Пожары"}],
            "selected_table": "fires",
            "days_ahead": 14,
            "selected_history_window": "all",
        }

        with (
            patch.object(core, "_build_ml_request_state", return_value=request_state),
            patch.object(core, "_cache_get", return_value=cached_payload),
            patch.object(core, "_empty_ml_model_data", side_effect=AssertionError("shell should use cache first")),
        ):
            context = core.get_ml_model_shell_context(table_name="fires")

        self.assertEqual(context["initial_data"], cached_payload)
        self.assertTrue(context["has_data"])
        self.assertEqual(context["plotly_js"], "")


if __name__ == "__main__":
    unittest.main()
