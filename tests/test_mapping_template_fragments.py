import json
import re
import unittest

from core.mapping.config import MarkerStyle
import core.mapping.mixins.template_scripts as template_scripts
from core.mapping.mixins.templates import MapCreatorTemplateMixin
from core.mapping.mixins.template_scripts import build_popup_script_lines
from core.mapping.mixins.utilities import MapCreatorUtilityMixin


class _SmokeMapCreator(MapCreatorUtilityMixin, MapCreatorTemplateMixin):
    CATEGORY_STYLES = {
        "deaths": MarkerStyle("rgba(255,0,0,0.8)", "darkred", "D", "Deaths", 8),
        "injured": MarkerStyle("rgba(255,165,0,0.8)", "orange", "I", "Injured", 8),
        "children": MarkerStyle("rgba(173,216,230,0.8)", "blue", "C", "Children", 8),
        "evacuated": MarkerStyle("rgba(0,255,0,0.6)", "green", "E", "Evacuated", 8),
        "other": MarkerStyle("rgba(128,128,128,0.5)", "gray", "O", "Other", 6),
    }


def _extract_js_constant(html: str, name: str):
    match = re.search(rf"const {name} = (.*?);", html)
    if not match:
        raise AssertionError(f"Missing JS constant: {name}")
    return json.loads(match.group(1))


def _extract_read_geojson_payload(html: str, name: str):
    match = re.search(rf"const {name} = readGeoJson\((.*?)\);", html)
    if not match:
        raise AssertionError(f"Missing readGeoJson payload: {name}")
    return json.loads(match.group(1))


def _minimal_table():
    return {
        "name": "demo_table",
        "counts": {"deaths": 1, "injured": 0, "children": 0, "evacuated": 0, "other": 0},
        "center": (92.8, 56.0),
        "initial_zoom": 7,
        "geojson": {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [92.8, 56.0]},
                    "properties": {
                        "category": "deaths",
                        "popup_rows": [{"label": "Address", "value": "Demo"}],
                    },
                }
            ],
        },
        "spatial_analytics": {
            "quality": {
                "coordinate_coverage_display": "100%",
                "date_coverage_display": "100%",
            },
            "heatmap": {
                "enabled": True,
                "radius": 18,
                "blur": 22,
                "points": [{"longitude": 92.8, "latitude": 56.0, "weight": 0.7}],
            },
            "hotspots": [
                {
                    "longitude": 92.8,
                    "latitude": 56.0,
                    "label": "Hotspot A",
                    "risk_score_display": "80",
                    "support_count": 1,
                    "explanation": "Dense",
                    "rank": 1,
                    "risk_tone": "high",
                    "risk_score": 80,
                }
            ],
            "dbscan": {
                "cluster_count": 1,
                "clusters": [
                    {
                        "longitude": 92.81,
                        "latitude": 56.01,
                        "cluster_display": "Cluster 1",
                        "label": "Cluster A",
                        "risk_score_display": "75",
                        "incident_count": 2,
                        "explanation": "Cluster",
                        "rank": 1,
                        "risk_tone": "medium",
                        "risk_score": 75,
                    }
                ],
            },
            "risk_zones": [
                {
                    "polygon": [[92.7, 55.9], [92.9, 55.9], [92.9, 56.1], [92.7, 56.1], [92.7, 55.9]],
                    "label": "Zone A",
                    "source": "test",
                    "risk_score_display": "70",
                    "explanation": "Zone",
                    "priority_label": "Priority",
                    "risk_tone": "watch",
                    "risk_score": 70,
                }
            ],
            "priority_territories": [
                {
                    "longitude": 92.82,
                    "latitude": 56.02,
                    "label": "Territory A",
                    "risk_score_display": "90",
                    "incident_count_display": "3",
                    "travel_time_display": "12 min",
                    "avg_response_display": "9 min",
                    "avg_station_distance_display": "2 km",
                    "fire_station_coverage_display": "80%",
                    "service_zone_label": "Covered",
                    "logistics_priority_display": "High",
                    "explanation": "Priority",
                    "priority_label": "P1",
                    "rank": 1,
                    "risk_tone": "critical",
                    "risk_score": 90,
                }
            ],
            "logistics": {
                "summary": "OK",
                "average_travel_time_display": "12 min",
                "fire_station_coverage_display": "80%",
                "service_zone_label": "Covered",
            },
            "summary": {
                "title": "Analytics",
                "subtitle": "Smoke",
                "methods": ["KDE", "DBSCAN"],
                "insights": ["Insight"],
                "thesis_paragraphs": ["Thesis"],
            },
            "layer_defaults": {
                "incidents": True,
                "heatmap": True,
                "hotspots": True,
                "clusters": True,
                "risk_zones": True,
                "priorities": True,
            },
        },
    }


class MappingTemplateFragmentsTest(unittest.TestCase):
    def test_template_scripts_export_only_entry_points(self):
        self.assertEqual(
            set(template_scripts.__all__),
            {
                "build_filter_script_lines",
                "build_map_setup_script_lines",
                "build_map_layer_script_lines",
                "build_popup_script_lines",
                "build_tab_script_lines",
            },
        )

    def test_minimal_analytics_html_contains_expected_controls_and_payload(self):
        creator = _SmokeMapCreator()
        html = creator._generate_html([_minimal_table()], {"deaths": 1})

        self.assertIn('id="filter-panel-0"', html)
        self.assertIn('id="analytics-panel-0"', html)
        self.assertIn('id="select-all-0"', html)
        self.assertIn('id="deselect-all-0"', html)
        self.assertIn('data-category="deaths"', html)
        for layer_id in ("incidents", "heatmap", "hotspots", "clusters", "risk_zones", "priorities"):
            self.assertIn(f'data-layer="{layer_id}"', html)
        self.assertIn('window["map0"] = map;', html)
        self.assertIn('new ol.layer.Heatmap', html)
        self.assertIn('analyticsLayers.heatmap', html)
        self.assertIn('analyticsLayers.hotspots', html)
        self.assertIn('analyticsLayers.clusters', html)
        self.assertIn('analyticsLayers.risk_zones', html)
        self.assertIn('analyticsLayers.priorities', html)
        for layer_id in ("heatmap", "hotspots", "clusters", "risk_zones", "priorities"):
            self.assertIn(f"visible: !!analyticsLayerDefaults.{layer_id}", html)
        self.assertLess(html.index("analyticsLayers.hotspots ="), html.index("analyticsLayers.risk_zones ="))
        self.assertLess(html.index("analyticsLayers.risk_zones ="), html.index("analyticsLayers.priorities ="))
        self.assertIn('updateCategoryLayers', html)
        self.assertIn('updateAnalyticsLayers', html)
        self.assertIn('map.addControl(new ol.control.FullScreen());', html)
        self.assertIn('map.addControl(new ol.control.ScaleLine());', html)

        incident_payload = _extract_read_geojson_payload(html, "features")
        incident_properties = incident_payload["features"][0]["properties"]
        self.assertEqual(
            incident_properties["popup_rows"],
            [{"label": "Address", "value": "Demo"}],
        )
        self.assertIn('feature.get("popup_rows")', html)
        self.assertIn('replaceChildren(popupElement)', html)

        features = _extract_js_constant(html, "analyticsLayersPayload")
        self.assertEqual(len(features["heatmap"]["features"]), 1)
        self.assertEqual(len(features["hotspots"]["features"]), 1)
        self.assertEqual(len(features["clusters"]["features"]), 1)
        self.assertEqual(len(features["risk_zones"]["features"]), 1)
        self.assertEqual(len(features["priorities"]["features"]), 1)
        self.assertEqual(
            features["clusters"]["features"][0]["properties"]["popup_rows"][0],
            {"label": "DBSCAN", "value": "Cluster 1"},
        )
        self.assertEqual(
            features["risk_zones"]["features"][0]["properties"]["popup_rows"][0],
            {"title": "Priority"},
        )
        self.assertEqual(
            features["priorities"]["features"][0]["properties"]["popup_rows"][0],
            {"title": "P1"},
        )

        layer_defaults = _extract_js_constant(html, "analyticsLayerDefaults")
        self.assertEqual(
            layer_defaults,
            {
                "incidents": True,
                "heatmap": True,
                "hotspots": True,
                "clusters": True,
                "risk_zones": True,
                "priorities": True,
            },
        )

        heatmap_config = _extract_js_constant(html, "heatmapConfig")
        self.assertEqual(heatmap_config, {"enabled": True, "radius": 18, "blur": 22})
        self.assertNotIn("points", heatmap_config)

    def test_popup_script_builder_keeps_popup_rows_contract(self):
        script = "\n".join(build_popup_script_lines())

        self.assertIn('const rows = feature.get("popup_rows");', script)
        self.assertIn('Object.prototype.hasOwnProperty.call(row, "title")', script)
        self.assertIn('replaceChildren(popupElement)', script)
        self.assertIn('overlay.setPosition(undefined);', script)

    def test_popup_script_uses_only_js_nullish_coalescing_tokens(self):
        lines_with_nullish = [line for line in build_popup_script_lines() if "??" in line]

        self.assertEqual(len(lines_with_nullish), 3)
        for line in lines_with_nullish:
            self.assertIn("row", line)
            self.assertNotIn('"??"', line)
            self.assertNotIn("'??'", line)


if __name__ == "__main__":
    unittest.main()
