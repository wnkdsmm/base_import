from tests.analytics_api_contract_support import (
    AnalyticsApiContractSupport,
    dashboard_data_endpoint,
    forecasting_data_endpoint,
    forecasting_metadata_endpoint,
    os,
    patch,
)


class AnalyticsDashboardAndForecastingApiContractTests(AnalyticsApiContractSupport):
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
