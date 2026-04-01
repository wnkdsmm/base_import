import json
import os
import unittest
from unittest.mock import patch

from fastapi.responses import Response
from starlette.requests import Request

from app.routes.api import (
    access_points_data_endpoint,
    clustering_data_endpoint,
    dashboard_data_endpoint,
    forecasting_data_endpoint,
    forecasting_metadata_endpoint,
    ml_model_data_endpoint,
)


class AnalyticsApiContractTests(unittest.TestCase):
    @staticmethod
    def _decode_response(response: Response) -> dict:
        return json.loads(response.body.decode("utf-8"))

    @staticmethod
    def _build_request(*, query_string: bytes = b"") -> Request:
        return Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/api/clustering-data",
                "headers": [],
                "query_string": query_string,
            }
        )

    def test_forecasting_api_returns_resolved_payload_on_success(self) -> None:
        resolved_payload = {
            "bootstrap_mode": "resolved",
            "loading": False,
            "decision_support_pending": False,
            "filters": {"table_name": "fires"},
            "summary": {"selected_table_label": "fires"},
        }

        with patch("app.routes.api_forecasting.get_forecasting_data", return_value=resolved_payload):
            response = forecasting_data_endpoint(include_decision_support=False)

        self.assertEqual(response, resolved_payload)

    def test_dashboard_api_returns_resolved_payload_on_success(self) -> None:
        resolved_payload = {
            "generated_at": "29.03.2026 09:00",
            "summary": {"fires_count_display": "12"},
            "filters": {"table_name": "fires"},
            "scope": {"table_label": "fires"},
        }

        with patch("app.routes.api_dashboard.get_dashboard_data", return_value=resolved_payload):
            response = dashboard_data_endpoint()

        self.assertEqual(response, resolved_payload)

    def test_dashboard_api_returns_400_for_invalid_request(self) -> None:
        with self.assertLogs("app.routes.api", level="WARNING") as captured_logs:
            with patch("app.routes.api_dashboard.get_dashboard_data", side_effect=ValueError("bad dashboard params")):
                response = dashboard_data_endpoint()

        payload = self._decode_response(response)
        self.assertEqual(response.status_code, 400)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "dashboard_invalid_request")
        self.assertEqual(payload["error"]["message"], "bad dashboard params")
        self.assertEqual(payload["error"]["status_code"], 400)
        self.assertTrue(payload["error"]["error_id"])
        self.assertNotIn("detail", payload["error"])
        self.assertTrue(any("bad dashboard params" in message for message in captured_logs.output))

    def test_dashboard_api_returns_structured_http_error(self) -> None:
        with self.assertLogs("app.routes.api", level="ERROR") as captured_logs:
            with patch("app.routes.api_dashboard.get_dashboard_data", side_effect=RuntimeError("dashboard exploded")):
                response = dashboard_data_endpoint()

        payload = self._decode_response(response)
        self.assertEqual(response.status_code, 500)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "dashboard_failed")
        self.assertEqual(payload["error"]["status_code"], 500)
        self.assertTrue(payload["error"]["error_id"])
        self.assertNotIn("detail", payload["error"])
        self.assertTrue(any("dashboard exploded" in message for message in captured_logs.output))

    def test_forecasting_api_returns_structured_http_error(self) -> None:
        with self.assertLogs("app.routes.api", level="ERROR") as captured_logs:
            with patch("app.routes.api_forecasting.get_forecasting_data", side_effect=RuntimeError("forecast exploded")):
                response = forecasting_data_endpoint(include_decision_support=False)

        payload = self._decode_response(response)
        self.assertEqual(response.status_code, 500)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "forecasting_failed")
        self.assertEqual(payload["error"]["status_code"], 500)
        self.assertTrue(payload["error"]["error_id"])
        self.assertNotIn("detail", payload["error"])
        self.assertTrue(any("forecast exploded" in message for message in captured_logs.output))

    def test_forecasting_metadata_api_returns_resolved_payload_on_success(self) -> None:
        resolved_payload = {
            "bootstrap_mode": "deferred",
            "metadata_ready": True,
            "filters": {"table_name": "fires"},
            "summary": {"selected_table_label": "fires"},
        }

        with patch("app.routes.api_forecasting.get_forecasting_metadata", return_value=resolved_payload):
            response = forecasting_metadata_endpoint()

        self.assertEqual(response, resolved_payload)

    def test_forecasting_metadata_api_returns_structured_http_error(self) -> None:
        with self.assertLogs("app.routes.api", level="ERROR") as captured_logs:
            with patch("app.routes.api_forecasting.get_forecasting_metadata", side_effect=RuntimeError("metadata exploded")):
                response = forecasting_metadata_endpoint()

        payload = self._decode_response(response)
        self.assertEqual(response.status_code, 500)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "forecasting_metadata_failed")
        self.assertEqual(payload["error"]["status_code"], 500)
        self.assertTrue(payload["error"]["error_id"])
        self.assertNotIn("detail", payload["error"])
        self.assertTrue(any("metadata exploded" in message for message in captured_logs.output))

    def test_ml_model_api_returns_resolved_payload_on_success(self) -> None:
        resolved_payload = {
            "generated_at": "31.03.2026 09:00",
            "summary": {
                "selected_table_label": "Все таблицы",
                "event_backtest_model_label": "Не показан",
            },
            "notes": [
                "Вероятностный блок события пожара скрыт: все 45 evaluation-окон rolling-origin backtesting относятся к одному классу (только дни с пожаром), поэтому вероятностная валидация некорректна."
            ],
            "features": [
                {
                    "label": "Температура",
                    "status_label": "Низкое покрытие (3/365 дней (0,8%))",
                }
            ],
            "quality_assessment": {
                "event_table": {
                    "title": "Сравнение по вероятности события пожара",
                    "rows": [],
                    "empty_message": "Вероятностный блок события пожара скрыт: все 45 evaluation-окон rolling-origin backtesting относятся к одному классу (только дни с пожаром), поэтому вероятностная валидация некорректна.",
                }
            },
        }

        with patch("app.routes.api_ml_model.get_ml_model_data", return_value=resolved_payload):
            response = ml_model_data_endpoint()

        self.assertEqual(response, resolved_payload)

    def test_clustering_api_returns_400_for_invalid_request(self) -> None:
        with self.assertLogs("app.routes.api", level="WARNING") as captured_logs:
            with patch("app.routes.api_clustering.get_clustering_data", side_effect=ValueError("bad cluster params")):
                response = clustering_data_endpoint(self._build_request())

        payload = self._decode_response(response)
        self.assertEqual(response.status_code, 400)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "clustering_invalid_request")
        self.assertEqual(payload["error"]["message"], "bad cluster params")
        self.assertEqual(payload["error"]["status_code"], 400)
        self.assertTrue(payload["error"]["error_id"])
        self.assertNotIn("detail", payload["error"])
        self.assertTrue(any("bad cluster params" in message for message in captured_logs.output))

    def test_clustering_api_returns_structured_http_error(self) -> None:
        with self.assertLogs("app.routes.api", level="ERROR") as captured_logs:
            with patch("app.routes.api_clustering.get_clustering_data", side_effect=RuntimeError("clustering exploded")):
                response = clustering_data_endpoint(self._build_request())

        payload = self._decode_response(response)
        self.assertEqual(response.status_code, 500)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "clustering_failed")
        self.assertEqual(payload["error"]["status_code"], 500)
        self.assertTrue(payload["error"]["error_id"])
        self.assertNotIn("detail", payload["error"])
        self.assertTrue(any("clustering exploded" in message for message in captured_logs.output))

    def test_access_points_api_returns_resolved_payload_on_success(self) -> None:
        resolved_payload = {
            "bootstrap_mode": "resolved",
            "has_data": True,
            "summary": {"top_point_label": "Точка А"},
            "points": [{"label": "Точка А", "score_display": "78"}],
        }

        with patch("app.routes.api_access_points.get_access_points_data", return_value=resolved_payload):
            response = access_points_data_endpoint()

        self.assertEqual(response, resolved_payload)

    def test_access_points_api_returns_400_for_invalid_request(self) -> None:
        with self.assertLogs("app.routes.api", level="WARNING") as captured_logs:
            with patch("app.routes.api_access_points.get_access_points_data", side_effect=ValueError("bad access params")):
                response = access_points_data_endpoint()

        payload = self._decode_response(response)
        self.assertEqual(response.status_code, 400)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "access_points_invalid_request")
        self.assertEqual(payload["error"]["message"], "bad access params")
        self.assertTrue(any("bad access params" in message for message in captured_logs.output))

    def test_access_points_api_returns_structured_http_error(self) -> None:
        with self.assertLogs("app.routes.api", level="ERROR") as captured_logs:
            with patch("app.routes.api_access_points.get_access_points_data", side_effect=RuntimeError("access exploded")):
                response = access_points_data_endpoint()

        payload = self._decode_response(response)
        self.assertEqual(response.status_code, 500)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "access_points_failed")
        self.assertEqual(payload["error"]["status_code"], 500)
        self.assertTrue(payload["error"]["error_id"])
        self.assertNotIn("detail", payload["error"])
        self.assertTrue(any("access exploded" in message for message in captured_logs.output))

    def test_ml_model_api_returns_structured_http_error(self) -> None:
        with self.assertLogs("app.routes.api", level="ERROR") as captured_logs:
            with patch("app.routes.api_ml_model.get_ml_model_data", side_effect=RuntimeError("ml exploded")):
                response = ml_model_data_endpoint()

        payload = self._decode_response(response)
        self.assertEqual(response.status_code, 500)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "ml_model_failed")
        self.assertEqual(payload["error"]["status_code"], 500)
        self.assertTrue(payload["error"]["error_id"])
        self.assertNotIn("detail", payload["error"])
        self.assertTrue(any("ml exploded" in message for message in captured_logs.output))

    def test_analytics_detail_is_returned_only_with_explicit_local_opt_in(self) -> None:
        with (
            patch.dict(
                os.environ,
                {
                    "APP_ENV": "local",
                    "FIRE_MONITOR_EXPOSE_API_ERROR_DETAIL": "1",
                },
                clear=False,
            ),
            patch("app.routes.api_forecasting.get_forecasting_data", side_effect=RuntimeError("forecast exploded")),
        ):
            response = forecasting_data_endpoint(include_decision_support=False)

        payload = self._decode_response(response)
        self.assertEqual(response.status_code, 500)
        self.assertEqual(payload["error"]["detail"], "forecast exploded")


if __name__ == "__main__":
    unittest.main()
