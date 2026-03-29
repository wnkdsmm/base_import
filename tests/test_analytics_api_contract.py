import json
import unittest
from unittest.mock import patch

from fastapi.responses import Response

from app.routes.api import clustering_data_endpoint, forecasting_data_endpoint


class AnalyticsApiContractTests(unittest.TestCase):
    @staticmethod
    def _decode_response(response: Response) -> dict:
        return json.loads(response.body.decode("utf-8"))

    def test_forecasting_api_returns_resolved_payload_on_success(self) -> None:
        resolved_payload = {
            "bootstrap_mode": "resolved",
            "loading": False,
            "decision_support_pending": False,
            "filters": {"table_name": "fires"},
            "summary": {"selected_table_label": "fires"},
        }

        with patch("app.routes.api.get_forecasting_data", return_value=resolved_payload):
            response = forecasting_data_endpoint()

        self.assertEqual(response, resolved_payload)

    def test_forecasting_api_returns_structured_http_error(self) -> None:
        with patch("app.routes.api.get_forecasting_data", side_effect=RuntimeError("forecast exploded")):
            response = forecasting_data_endpoint()

        payload = self._decode_response(response)
        self.assertEqual(response.status_code, 500)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "forecasting_failed")
        self.assertEqual(payload["error"]["status_code"], 500)
        self.assertEqual(payload["error"]["detail"], "forecast exploded")

    def test_clustering_api_returns_400_for_invalid_request(self) -> None:
        with patch("app.routes.api.get_clustering_data", side_effect=ValueError("bad cluster params")):
            response = clustering_data_endpoint()

        payload = self._decode_response(response)
        self.assertEqual(response.status_code, 400)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "clustering_invalid_request")
        self.assertEqual(payload["error"]["message"], "bad cluster params")
        self.assertEqual(payload["error"]["status_code"], 400)

    def test_clustering_api_returns_structured_http_error(self) -> None:
        with patch("app.routes.api.get_clustering_data", side_effect=RuntimeError("clustering exploded")):
            response = clustering_data_endpoint()

        payload = self._decode_response(response)
        self.assertEqual(response.status_code, 500)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "clustering_failed")
        self.assertEqual(payload["error"]["status_code"], 500)
        self.assertEqual(payload["error"]["detail"], "clustering exploded")


if __name__ == "__main__":
    unittest.main()
