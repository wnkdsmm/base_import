from datetime import date
import math
import unittest
from unittest.mock import patch

from core.mapping.mixins.analytics import MapCreatorAnalyticsMixin


class _AnalyticsSmokeCreator(MapCreatorAnalyticsMixin):
    def _risk_level(self, score):
        if score >= 70:
            return ("High", "high")
        if score >= 40:
            return ("Medium", "medium")
        return ("Low", "low")

    def _km_distance(self, origin, target):
        lat_delta = (float(origin["latitude"]) - float(target["latitude"])) * 110.574
        lon_delta = (float(origin["longitude"]) - float(target["longitude"])) * 60.0
        return math.hypot(lat_delta, lon_delta)

    def _build_circle_polygon(self, longitude, latitude, radius_km):
        delta = radius_km / 111.0
        return [
            [round(longitude - delta, 6), round(latitude - delta, 6)],
            [round(longitude + delta, 6), round(latitude - delta, 6)],
            [round(longitude + delta, 6), round(latitude + delta, 6)],
            [round(longitude - delta, 6), round(latitude + delta, 6)],
            [round(longitude - delta, 6), round(latitude - delta, 6)],
        ]


def _record(index, territory, latitude, longitude):
    return {
        "latitude": latitude,
        "longitude": longitude,
        "date": date(2024, 1, index),
        "district": "District",
        "territory_label": territory,
        "settlement_type": "village",
        "address": f"Address {index}",
        "cause": "test",
        "object_category": "house",
        "response_minutes": 12.0 + index,
        "fire_station_distance": 2.0 + index,
        "severity_raw": 1.5 + index,
        "has_victims": index == 1,
        "weight": 1.5 + index,
        "rural_flag": True,
    }


class MappingAnalyticsTests(unittest.TestCase):
    @patch("core.mapping.mixins.analytics._build_geo_prediction")
    def test_spatial_analytics_payload_survives_helper_decomposition(self, geo_prediction):
        geo_prediction.return_value = {
            "hotspots": [
                {
                    "short_label": "Zone A",
                    "latitude": 56.0,
                    "longitude": 92.8,
                    "incidents": 3,
                    "marker_size": 12,
                    "risk_score": 82.0,
                    "risk_display": "82.0 / 100",
                    "explanation": "Hotspot A",
                }
            ]
        }
        records = [
            _record(1, "Territory A", 56.0, 92.8),
            _record(2, "Territory A", 56.01, 92.81),
            _record(3, "Territory B", 56.02, 92.82),
        ]

        payload = _AnalyticsSmokeCreator()._build_spatial_analytics("demo_table", records, source_record_count=5)

        geo_prediction.assert_called_once()
        self.assertEqual(payload["quality"]["valid_coordinate_count"], 3)
        self.assertEqual(payload["quality"]["dated_record_count"], 3)
        self.assertEqual(len(payload["heatmap"]["points"]), 3)
        self.assertTrue(payload["heatmap"]["enabled"])
        self.assertEqual(payload["hotspots"][0]["label"], "Zone A")
        self.assertEqual(payload["risk_zones"][0]["source"], "Hotspot")
        self.assertTrue(payload["risk_zones"][0]["polygon"])
        self.assertTrue(payload["priority_territories"])
        self.assertIn("Hotspot detection", payload["summary"]["methods"])
        self.assertTrue(payload["layer_defaults"]["hotspots"])
        self.assertTrue(payload["layer_defaults"]["risk_zones"])

    @patch("core.mapping.mixins.analytics._build_geo_prediction")
    @patch.object(
        _AnalyticsSmokeCreator,
        "_build_dbscan_clusters",
        return_value={
            "clusters": [],
            "eps_km": 1.234,
            "min_samples": 4,
            "noise_count": 0,
            "availability_note": "",
        },
    )
    def test_spatial_analytics_formats_dbscan_eps_display_with_km(self, dbscan_clusters, geo_prediction):
        geo_prediction.return_value = {"hotspots": []}
        records = [
            _record(1, "Territory A", 56.0, 92.8),
            _record(2, "Territory A", 56.01, 92.81),
            _record(3, "Territory B", 56.02, 92.82),
        ]

        payload = _AnalyticsSmokeCreator()._build_spatial_analytics("demo_table", records, source_record_count=3)

        dbscan_clusters.assert_called_once_with(records)
        self.assertEqual(payload["dbscan"]["eps_display"], "1.23 км")
        self.assertNotIn("??", payload["dbscan"]["eps_display"])


if __name__ == "__main__":
    unittest.main()
