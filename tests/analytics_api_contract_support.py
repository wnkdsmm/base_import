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


class AnalyticsApiContractSupport(unittest.TestCase):
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
