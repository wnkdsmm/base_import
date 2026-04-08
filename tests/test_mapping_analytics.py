from datetime import date
import math
import unittest
from unittest.mock import patch

from core.mapping.mixins import (
    analytics as analytics_module,
    analytics_dbscan,
    analytics_geometry,
    analytics_hotspots,
    analytics_logistics,
    analytics_payload,
    analytics_priority,
)
from core.mapping.mixins.analytics import MapCreatorAnalyticsMixin
from core.mapping.mixins.analytics_dbscan import build_dbscan_cluster_result
from core.mapping.mixins.analytics_geometry import group_records_by_field, project_records_to_local_xy
from core.mapping.mixins.analytics_hotspots import build_hotspot_payloads
from core.mapping.mixins.analytics_logistics import build_logistics_summary_payload
from core.mapping.mixins.analytics_payload import build_spatial_dbscan_payload
from core.mapping.mixins.analytics_priority import build_fallback_risk_zones, build_priority_territories


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

    def _dominant_label(self, records, key, fallback):
        for item in records:
            if item.get(key):
                return item[key]
        return fallback


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
    def test_analytics_helper_modules_export_explicit_public_helpers(self):
        expected_exports = {
            analytics_module: {"MapCreatorAnalyticsMixin", "SKLEARN_AVAILABLE"},
            analytics_dbscan: {
                "build_dbscan_cluster_result",
                "build_dbscan_clusters",
                "estimate_dbscan_eps_km",
            },
            analytics_geometry: {
                "group_records_by_cluster_label",
                "group_records_by_field",
                "mean_record_value",
                "nanmean_record_value",
                "project_records_to_local_xy",
            },
            analytics_hotspots: {"build_hotspot_payloads", "build_hotspots_from_dated_records"},
            analytics_logistics: {"build_logistics_summary_payload"},
            analytics_payload: {
                "build_empty_spatial_analytics",
                "build_heatmap_points",
                "build_spatial_analytics_payload",
                "build_spatial_dbscan_payload",
                "build_spatial_heatmap_payload",
                "build_spatial_insights",
                "build_spatial_layer_defaults",
                "build_spatial_methods",
                "build_spatial_quality_context",
                "build_spatial_quality_payload",
                "build_spatial_summary_payload",
                "build_spatial_thesis_paragraphs",
            },
            analytics_priority: {
                "build_fallback_risk_zones",
                "build_priority_territories",
                "build_spatial_risk_zones",
            },
        }

        for module, names in expected_exports.items():
            self.assertEqual(set(module.__all__), names)
            for name in names:
                self.assertTrue(callable(getattr(module, name)) or name == "SKLEARN_AVAILABLE")

    def test_map_creator_analytics_mixin_keeps_orchestrator_boundary(self):
        method_names = {
            name
            for name, value in MapCreatorAnalyticsMixin.__dict__.items()
            if callable(value) and name.startswith("_") and not name.startswith("__")
        }

        self.assertEqual(method_names, {"_collect_spatial_records", "_build_spatial_analytics"})

    @patch("core.mapping.mixins.analytics_hotspots._build_geo_prediction")
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

    @patch("core.mapping.mixins.analytics_hotspots._build_geo_prediction")
    @patch(
        "core.mapping.mixins.analytics.build_dbscan_clusters",
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

        dbscan_clusters.assert_called_once()
        self.assertIs(dbscan_clusters.call_args.args[0], records)
        self.assertEqual(dbscan_clusters.call_args.kwargs["sklearn_available"], analytics_module.SKLEARN_AVAILABLE)
        self.assertEqual(payload["dbscan"]["eps_display"], "1.23 км")
        self.assertNotIn("??", payload["dbscan"]["eps_display"])
        self.assertEqual(
            payload["dbscan"],
            {
                "enabled": False,
                "clusters": [],
                "eps_km": 1.234,
                "eps_display": "1.23 км",
                "min_samples": 4,
                "cluster_count": 0,
                "noise_count": 0,
                "availability_note": "",
            },
        )
        self.assertFalse(payload["layer_defaults"]["clusters"])

    def test_dbscan_payload_helper_preserves_eps_display_unit(self):
        payload = build_spatial_dbscan_payload(
            {
                "clusters": [{"label": "Cluster A"}],
                "eps_km": 2.5,
                "min_samples": 5,
                "noise_count": 1,
                "availability_note": "",
            }
        )

        self.assertEqual(payload["eps_display"], "2.50 км")
        self.assertEqual(payload["cluster_count"], 1)
        self.assertTrue(payload["enabled"])

    def test_dbscan_cluster_helper_preserves_payload_shape(self):
        creator = _AnalyticsSmokeCreator()
        records = [
            _record(1, "Territory A", 56.0, 92.8),
            _record(2, "Territory A", 56.001, 92.801),
            _record(3, "Territory A", 56.002, 92.802),
            _record(4, "Territory A", 56.003, 92.803),
            _record(5, "Territory B", 56.2, 93.0),
            _record(6, "Territory B", 56.201, 93.001),
            _record(7, "Territory B", 56.202, 93.002),
            _record(8, "Territory C", 56.6, 93.4),
        ]

        payload = build_dbscan_cluster_result(
            records,
            [0, 0, 0, 0, 1, 1, 1, -1],
            1.25,
            4,
            risk_level=creator._risk_level,
            km_distance=creator._km_distance,
            dominant_label=lambda items, field, fallback: items[0].get(field) or fallback,
        )

        self.assertEqual(payload["eps_km"], 1.25)
        self.assertEqual(payload["min_samples"], 4)
        self.assertEqual(payload["noise_count"], 1)
        self.assertEqual(len(payload["clusters"]), 2)
        self.assertEqual(payload["clusters"][0]["cluster_display"], "DBSCAN #1")
        self.assertIn("avg_station_distance", payload["clusters"][0])

    def test_dbscan_cluster_orchestration_helpers_keep_fallback_shape(self):
        creator = _AnalyticsSmokeCreator()
        records = [
            _record(1, "Territory A", 56.0, 92.8),
            _record(2, "Territory A", 56.01, 92.81),
            _record(3, "Territory B", 56.02, 92.82),
        ]

        eps = analytics_dbscan.estimate_dbscan_eps_km(
            records,
            sklearn_available=False,
            nearest_neighbors_cls=None,
        )
        payload = analytics_dbscan.build_dbscan_clusters(
            records,
            sklearn_available=False,
            dbscan_cls=None,
            nearest_neighbors_cls=None,
            risk_level=creator._risk_level,
            km_distance=creator._km_distance,
            dominant_label=lambda items, field, fallback: items[0].get(field) or fallback,
        )

        self.assertEqual(eps, 1.0)
        self.assertEqual(payload["clusters"], [])
        self.assertEqual(payload["eps_km"], 0.0)
        self.assertTrue(payload["availability_note"])

    @patch("core.mapping.mixins.analytics_hotspots._build_geo_prediction")
    def test_hotspot_orchestration_helper_uses_geo_prediction_payload(self, geo_prediction):
        geo_prediction.return_value = {
            "hotspots": [
                {
                    "short_label": "Hotspot A",
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
        notes = []

        hotspots = analytics_hotspots.build_hotspots_from_dated_records(
            records,
            notes,
            _AnalyticsSmokeCreator()._risk_level,
        )

        geo_prediction.assert_called_once_with(records, planning_horizon_days=30)
        self.assertEqual(notes, [])
        self.assertEqual(hotspots[0]["label"], "Hotspot A")
        self.assertEqual(hotspots[0]["risk_tone"], "high")

    def test_hotspot_priority_and_logistics_helpers_keep_payload_shape(self):
        creator = _AnalyticsSmokeCreator()
        hotspot_payload = build_hotspot_payloads(
            {
                "hotspots": [
                    {
                        "short_label": "Hotspot A",
                        "latitude": 56.0,
                        "longitude": 92.8,
                        "incidents": 3,
                        "marker_size": 12,
                        "risk_score": 82.0,
                        "risk_display": "82.0 / 100",
                        "explanation": "Hotspot A",
                    }
                ]
            },
            creator._risk_level,
        )
        records = [
            _record(1, "Territory A", 56.0, 92.8),
            _record(2, "Territory A", 56.01, 92.81),
            _record(3, "Territory A", 56.02, 92.82),
        ]

        priority = build_priority_territories(
            records,
            [],
            risk_level=creator._risk_level,
            km_distance=creator._km_distance,
        )
        fallback_zones = build_fallback_risk_zones(
            records,
            priority,
            risk_level=creator._risk_level,
            km_distance=creator._km_distance,
            build_circle_polygon=creator._build_circle_polygon,
        )
        logistics = build_logistics_summary_payload(records, priority)

        self.assertEqual(hotspot_payload[0]["label"], "Hotspot A")
        self.assertEqual(hotspot_payload[0]["risk_tone"], "high")
        self.assertEqual(priority[0]["priority_label"], "Территория #1")
        self.assertIn("travel_time_display", priority[0])
        self.assertEqual(fallback_zones[0]["priority_label"], "Приоритет 1")
        self.assertTrue(fallback_zones[0]["polygon"])
        self.assertEqual(logistics["top_delayed_territories"][0]["label"], priority[0]["label"])
        self.assertTrue(logistics["average_station_distance_display"].endswith("км"))

    def test_spatial_risk_zone_helper_merges_cluster_and_hotspot_candidates(self):
        creator = _AnalyticsSmokeCreator()
        cluster = {
            "label": "Cluster A",
            "latitude": 56.0,
            "longitude": 92.8,
            "radius_km": 1.4,
            "risk_score": 65.0,
            "risk_score_display": "65.0 / 100",
            "risk_label": "Medium",
            "risk_tone": "medium",
            "incident_count": 4,
            "explanation": "Cluster",
        }
        hotspot = {
            "label": "Hotspot B",
            "latitude": 56.4,
            "longitude": 93.2,
            "radius_km": 1.2,
            "risk_score": 85.0,
            "risk_score_display": "85.0 / 100",
            "risk_label": "High",
            "risk_tone": "high",
            "support_count": 3,
            "explanation": "Hotspot",
        }

        zones = analytics_priority.build_spatial_risk_zones(
            {"clusters": [cluster]},
            [hotspot],
            km_distance=creator._km_distance,
            build_circle_polygon=creator._build_circle_polygon,
        )

        self.assertEqual([zone["source"] for zone in zones], ["Hotspot", "DBSCAN"])
        self.assertEqual([zone["rank"] for zone in zones], [1, 2])
        self.assertTrue(all(zone["polygon"] for zone in zones))

    def test_spatial_payload_helpers_keep_summary_and_quality_shape(self):
        records = [
            _record(1, "Territory A", 56.0, 92.8),
            _record(2, "Territory A", 56.01, 92.81),
            _record(3, "Territory B", 56.02, 92.82),
        ]
        quality_context = analytics_payload.build_spatial_quality_context(records, source_record_count=4)
        heatmap_points = analytics_payload.build_heatmap_points(records)
        dbscan = {
            "clusters": [],
            "eps_km": 0.0,
            "min_samples": 0,
            "noise_count": 0,
            "availability_note": "DBSCAN unavailable",
        }
        hotspots = [{"label": "Hotspot A", "risk_score_display": "80.0 / 100"}]
        priority_territories = [{"label": "Territory A", "risk_score_display": "90.0 / 100"}]
        logistics = {"basis_ready": False, "summary": "", "coverage_note": "Coverage note"}

        methods = analytics_payload.build_spatial_methods(
            records,
            hotspots,
            dbscan,
            [],
            priority_territories,
            logistics,
        )
        insights = analytics_payload.build_spatial_insights(
            hotspots,
            priority_territories,
            logistics,
            dbscan,
            quality_context["notes"],
        )
        thesis = analytics_payload.build_spatial_thesis_paragraphs(
            "demo_table",
            records,
            4,
            methods,
            [],
            priority_territories,
            logistics,
        )
        quality = analytics_payload.build_spatial_quality_payload(records, 4, quality_context)
        summary = analytics_payload.build_spatial_summary_payload(
            quality_context["mode"],
            methods,
            insights,
            thesis,
        )
        empty_payload = analytics_payload.build_empty_spatial_analytics(source_record_count=4)

        self.assertEqual(quality["valid_coordinate_count"], 3)
        self.assertEqual(quality["dated_record_count"], 3)
        self.assertEqual(len(heatmap_points), 3)
        self.assertIn("Hotspot detection", methods)
        self.assertIn("Coverage note", insights)
        self.assertEqual(summary["methods"], methods)
        self.assertEqual(summary["thesis_paragraphs"], thesis)
        self.assertEqual(empty_payload["dbscan"]["eps_display"], "-")

    def test_analytics_geometry_helpers_preserve_projection_and_grouping(self):
        records = [
            _record(1, "Territory A", 56.0, 92.8),
            _record(2, "Territory A", 56.01, 92.81),
            _record(3, "Territory B", 56.02, 92.82),
        ]

        xy = project_records_to_local_xy(records)
        grouped = group_records_by_field(records, "territory_label")

        self.assertEqual(tuple(xy.shape), (3, 2))
        self.assertAlmostEqual(float(xy[:, 0].mean()), 0.0, places=9)
        self.assertEqual([item["territory_label"] for item in grouped["Territory A"]], ["Territory A", "Territory A"])


if __name__ == "__main__":
    unittest.main()
