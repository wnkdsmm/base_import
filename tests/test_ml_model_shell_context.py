import unittest
from unittest.mock import patch

from fastapi.responses import HTMLResponse
from starlette.requests import Request

import app.routes.pages as pages_routes
from app.services.ml_model import core


class MlModelShellContextTests(unittest.TestCase):
    def test_shell_context_returns_deferred_shell_by_default_even_when_cache_exists(self) -> None:
        cached_payload = {
            "has_data": True,
            "filters": {
                "available_tables": [{"value": "fires", "label": "fires"}],
            },
            "quality_assessment": {
                "count_table": {"rows": [{"method_label": "cached"}]},
            },
        }
        request_state = {
            "cache_key": ("fires", "all", "all", "", 14, "all"),
            "table_options": [{"value": "fires", "label": "fires"}],
            "selected_table": "fires",
            "days_ahead": 14,
            "selected_history_window": "all",
            "source_table_notes": [],
        }
        empty_shell = {
            "filters": {
                "available_tables": [{"value": "fires", "label": "fires"}],
                "cause": "all",
                "object_category": "all",
            },
            "charts": {
                "importance": {"empty_message": "placeholder"},
            },
            "notes": [],
        }

        with (
            patch.object(core, "_build_ml_request_state", return_value=request_state),
            patch.object(core, "_cache_get", return_value=cached_payload),
            patch.object(core, "_empty_ml_model_data", return_value=empty_shell),
        ):
            context = core.get_ml_model_shell_context(table_name="fires")

        self.assertEqual(context["initial_data"]["bootstrap_mode"], "deferred")
        self.assertIn("Собираем драйверы прогноза", context["initial_data"]["charts"]["importance"]["empty_message"])
        self.assertTrue(context["has_data"])
        self.assertEqual(context["plotly_js"], "")

    def test_shell_context_can_explicitly_return_cached_payload(self) -> None:
        cached_payload = {
            "has_data": True,
            "filters": {
                "available_tables": [{"value": "fires", "label": "fires"}],
            },
            "quality_assessment": {
                "count_table": {"rows": [{"method_label": "cached"}]},
            },
        }
        request_state = {
            "cache_key": ("fires", "all", "all", "", 14, "all"),
            "table_options": [{"value": "fires", "label": "fires"}],
            "selected_table": "fires",
            "days_ahead": 14,
            "selected_history_window": "all",
            "source_table_notes": [],
        }

        with (
            patch.object(core, "_build_ml_request_state", return_value=request_state),
            patch.object(core, "_cache_get", return_value=cached_payload),
            patch.object(core, "_empty_ml_model_data", side_effect=AssertionError("explicit cache path should not build shell")),
        ):
            context = core.get_ml_model_shell_context(table_name="fires", prefer_cached=True)

        self.assertEqual(context["initial_data"], cached_payload)
        self.assertTrue(context["has_data"])
        self.assertEqual(context["plotly_js"], "")


if __name__ == "__main__":
    unittest.main()


class MlModelPageRouteTests(unittest.TestCase):
    @staticmethod
    def _build_request(path: str = "/ml-model") -> Request:
        return Request(
            {
                "type": "http",
                "method": "GET",
                "path": path,
                "headers": [],
                "query_string": b"",
            }
        )

    def test_ml_model_page_uses_deferred_shell_with_cache_preference(self) -> None:
        shell_context = {
            "generated_at": "02.04.2026 10:00",
            "initial_data": {
                "bootstrap_mode": "deferred",
                "filters": {"available_tables": []},
                "summary": {},
                "charts": {},
                "quality_assessment": {},
                "notes": [],
                "has_data": False,
            },
            "plotly_js": "",
            "has_data": False,
        }

        with (
            patch.object(pages_routes, "get_ml_model_shell_context", return_value=shell_context) as shell_context_mock,
            patch.object(pages_routes.templates, "TemplateResponse", return_value=HTMLResponse("ok")) as template_response_mock,
        ):
            response = pages_routes.ml_model_page(request=self._build_request())

        shell_context_mock.assert_called_once_with(
            table_name="all",
            cause="all",
            object_category="all",
            temperature="",
            forecast_days="14",
            history_window="all",
            current_user_date="",
            prefer_cached=True,
        )
        template_response_mock.assert_called_once()
        self.assertEqual(response.status_code, 200)
