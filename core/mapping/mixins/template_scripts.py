from __future__ import annotations

from typing import Any, Callable, Dict, List

from ...types import AnalyticsLayersPayload, CategoryStyleLike, MapTablePayload, SpatialLayerDefaults


_POINT_ANALYTICS_LAYER_SPECS = (
    ("hotspots", 10),
    ("clusters", 12),
    ("priorities", 11),
)


def build_filter_script_lines(idx: int, container_id: str) -> List[str]:
    return [
        '    const categoryCheckboxes = document.querySelectorAll("#%s .category-filter");' % container_id,
        '    const layerCheckboxes = document.querySelectorAll("#%s .layer-filter");' % container_id,
        "",
        "    const updateCategoryLayers = () => {",
        '        const incidentsToggle = Array.from(layerCheckboxes).find(box => box.dataset.layer === "incidents");',
        "        const incidentsVisible = incidentsToggle ? incidentsToggle.checked : true;",
        "        categoryCheckboxes.forEach(box => {",
        "            const layer = categoryLayers[box.dataset.category];",
        "            if (layer) {",
        "                layer.setVisible(incidentsVisible && box.checked);",
        "            }",
        "        });",
        "    };",
        "",
        "    const updateAnalyticsLayers = () => {",
        "        layerCheckboxes.forEach(box => {",
        '            if (box.dataset.layer === "incidents") {',
        "                return;",
        "            }",
        "            const layer = analyticsLayers[box.dataset.layer];",
        "            if (layer) {",
        "                layer.setVisible(box.checked);",
        "            }",
        "        });",
        "        updateCategoryLayers();",
        "    };",
        "",
        '    categoryCheckboxes.forEach(box => box.addEventListener("change", updateCategoryLayers));',
        '    layerCheckboxes.forEach(box => box.addEventListener("change", updateAnalyticsLayers));',
        "",
        '    document.getElementById("select-all-%s").addEventListener("click", () => {' % idx,
        "        categoryCheckboxes.forEach(box => box.checked = true);",
        "        layerCheckboxes.forEach(box => box.checked = true);",
        "        updateAnalyticsLayers();",
        "    });",
        "",
        '    document.getElementById("deselect-all-%s").addEventListener("click", () => {' % idx,
        "        categoryCheckboxes.forEach(box => box.checked = false);",
        "        layerCheckboxes.forEach(box => box.checked = false);",
        "        updateAnalyticsLayers();",
        "    });",
        "",
        "    updateAnalyticsLayers();",
        "    map.addControl(new ol.control.FullScreen());",
        "    map.addControl(new ol.control.ScaleLine());",
        "",
        '    window["map%s"] = map;' % idx,
        "    setTimeout(() => {",
        "        map.updateSize();",
        "        restoreMapView();",
        "    }, 200);",
        "})();",
        "</script>",
    ]


def _map_constructor_script_lines(idx: int, center_lon: Any, center_lat: Any, initial_zoom: Any) -> List[str]:
    return [
        "<script>",
        "(function() {",
        "    const map = new ol.Map({",
        '        target: "map%s",' % idx,
        "        layers: [new ol.layer.Tile({source: new ol.source.OSM()})],",
        "        view: new ol.View({",
        "            center: ol.proj.fromLonLat([%s, %s])," % (center_lon, center_lat),
        "            zoom: %s" % initial_zoom,
        "        })",
        "    });",
        "",
    ]


def _payload_constant_script_lines(
    styles_json: str,
    analytics_layers_json: str,
    analytics_defaults_json: str,
    heatmap_json: str,
) -> List[str]:
    return [
        "    const styles = %s;" % styles_json,
        "    const analyticsLayersPayload = %s;" % analytics_layers_json,
        "    const analyticsLayerDefaults = %s;" % analytics_defaults_json,
        "    const heatmapConfig = %s;" % heatmap_json,
        "",
    ]


def _style_helper_script_lines() -> List[str]:
    return [
        "    function tonePalette(tone) {",
        "        const palette = {",
        '            critical: { fill: "rgba(179, 59, 46, 0.18)", stroke: "#b33b2e", point: "rgba(179, 59, 46, 0.86)" },',
        '            high: { fill: "rgba(226, 107, 66, 0.18)", stroke: "#d25830", point: "rgba(226, 107, 66, 0.82)" },',
        '            medium: { fill: "rgba(240, 176, 62, 0.16)", stroke: "#cc9628", point: "rgba(240, 176, 62, 0.82)" },',
        '            watch: { fill: "rgba(70, 132, 196, 0.15)", stroke: "#3a75aa", point: "rgba(70, 132, 196, 0.8)" },',
        "        };",
        "        return palette[tone] || palette.watch;",
        "    }",
        "",
        "    function createStyle(category) {",
        "        const s = styles[category] || styles.other;",
        "        return new ol.style.Style({",
        "            image: new ol.style.Circle({",
        "                radius: s.radius,",
        "                fill: new ol.style.Fill({color: s.color}),",
        "                stroke: new ol.style.Stroke({color: s.stroke, width: 2})",
        "            })",
        "        });",
        "    }",
        "",
        "    function readGeoJson(collection) {",
        "        return new ol.format.GeoJSON().readFeatures(collection, {",
        '            dataProjection: "EPSG:4326",',
        '            featureProjection: "EPSG:3857"',
        "        });",
        "    }",
        "",
        "    function buildPointStyle(feature, baseRadius) {",
        '        const tone = tonePalette(feature.get("risk_tone"));',
        '        const rank = feature.get("rank") ? String(feature.get("rank")) : "";',
        "        return new ol.style.Style({",
        "            image: new ol.style.Circle({",
        "                radius: baseRadius,",
        "                fill: new ol.style.Fill({color: tone.point}),",
        "                stroke: new ol.style.Stroke({color: tone.stroke, width: 2})",
        "            }),",
        "            text: rank ? new ol.style.Text({",
        "                text: rank,",
        '                fill: new ol.style.Fill({color: "#ffffff"}),',
        '                font: "bold 11px sans-serif"',
        "            }) : undefined",
        "        });",
        "    }",
        "",
    ]


def _feature_setup_script_lines(geojson_json: str, center_lon: Any, center_lat: Any, initial_zoom: Any) -> List[str]:
    return [
        "    const features = readGeoJson(%s);" % geojson_json,
        "    const restoreMapView = () => {",
        "        const targetCenter = ol.proj.fromLonLat([%s, %s]);" % (center_lon, center_lat),
        "        const view = map.getView();",
        "        view.setCenter(targetCenter);",
        "        view.setZoom(%s);" % initial_zoom,
        "    };",
        "",
    ]


def build_map_setup_script_lines(
    idx: int,
    center_lon: Any,
    center_lat: Any,
    initial_zoom: Any,
    styles_json: str,
    analytics_layers_json: str,
    analytics_defaults_json: str,
    heatmap_json: str,
    geojson_json: str,
) -> List[str]:
    return (
        _map_constructor_script_lines(idx, center_lon, center_lat, initial_zoom)
        + _payload_constant_script_lines(styles_json, analytics_layers_json, analytics_defaults_json, heatmap_json)
        + _style_helper_script_lines()
        + _feature_setup_script_lines(geojson_json, center_lon, center_lat, initial_zoom)
    )


def _incident_category_layer_script_lines() -> List[str]:
    return [
        "    const categoryLayers = {};",
        '    ["deaths", "injured", "children", "evacuated", "other"].forEach(cat => {',
        '        const catFeatures = features.filter(feature => feature.get("category") === cat);',
        "        if (catFeatures.length) {",
        "            const layer = new ol.layer.Vector({",
        "                source: new ol.source.Vector({features: catFeatures}),",
        "                style: createStyle(cat),",
        "                visible: true",
        "            });",
        "            categoryLayers[cat] = layer;",
        "            map.addLayer(layer);",
        "        }",
        "    });",
        "",
    ]


def _heatmap_layer_script_lines() -> List[str]:
    return [
        "    const analyticsLayers = {};",
        "    if ((analyticsLayersPayload.heatmap?.features || []).length) {",
        "        const heatmapFeatures = readGeoJson(analyticsLayersPayload.heatmap);",
        '        heatmapFeatures.forEach(feature => feature.set("weight", Number(feature.get("weight") || 0.15)));',
        "        analyticsLayers.heatmap = new ol.layer.Heatmap({",
        "            source: new ol.source.Vector({features: heatmapFeatures}),",
        "            radius: heatmapConfig.radius || 20,",
        "            blur: heatmapConfig.blur || 26,",
        "            visible: !!analyticsLayerDefaults.heatmap,",
        "            opacity: 0.8",
        "        });",
        "        map.addLayer(analyticsLayers.heatmap);",
        "    }",
        "",
    ]


def _point_analytics_layer_script_lines(layer_id: str, base_radius: int) -> List[str]:
    return [
        "    if ((analyticsLayersPayload.%s?.features || []).length) {" % layer_id,
        "        analyticsLayers.%s = new ol.layer.Vector({" % layer_id,
        "            source: new ol.source.Vector({features: readGeoJson(analyticsLayersPayload.%s)})," % layer_id,
        "            visible: !!analyticsLayerDefaults.%s," % layer_id,
        "            style: feature => buildPointStyle(feature, %s)" % base_radius,
        "        });",
        "        map.addLayer(analyticsLayers.%s);" % layer_id,
        "    }",
        "",
    ]


def _point_analytics_layers_script_lines(layer_specs: tuple[tuple[str, int], ...]) -> List[str]:
    lines: List[str] = []
    for layer_id, base_radius in layer_specs:
        lines.extend(_point_analytics_layer_script_lines(layer_id, base_radius))
    return lines


def _risk_zone_layer_script_lines() -> List[str]:
    return [
        "    if ((analyticsLayersPayload.risk_zones?.features || []).length) {",
        "        analyticsLayers.risk_zones = new ol.layer.Vector({",
        "            source: new ol.source.Vector({features: readGeoJson(analyticsLayersPayload.risk_zones)}),",
        "            visible: !!analyticsLayerDefaults.risk_zones,",
        "            style: feature => {",
        '                const tone = tonePalette(feature.get("risk_tone"));',
        "                return new ol.style.Style({",
        "                    stroke: new ol.style.Stroke({color: tone.stroke, width: 2}),",
        "                    fill: new ol.style.Fill({color: tone.fill})",
        "                });",
        "            }",
        "        });",
        "        map.addLayer(analyticsLayers.risk_zones);",
        "    }",
        "",
    ]


def _analytics_layer_script_lines() -> List[str]:
    return (
        _heatmap_layer_script_lines()
        + _point_analytics_layers_script_lines(_POINT_ANALYTICS_LAYER_SPECS[:2])
        + _risk_zone_layer_script_lines()
        + _point_analytics_layers_script_lines(_POINT_ANALYTICS_LAYER_SPECS[2:])
    )


def build_map_layer_script_lines() -> List[str]:
    return _incident_category_layer_script_lines() + _analytics_layer_script_lines()


def _popup_overlay_script_lines() -> List[str]:
    return [
        "    const overlay = new ol.Overlay({",
        '        element: document.createElement("div"),',
        '        positioning: "bottom-center",',
        "        autoPan: true",
        "    });",
        "    map.addOverlay(overlay);",
        "",
    ]


def _popup_builder_script_lines() -> List[str]:
    return [
        "    function buildPopupElement(feature) {",
        '        const rows = feature.get("popup_rows");',
        "        if (!Array.isArray(rows) || !rows.length) {",
        "            return null;",
        "        }",
        '        const wrapper = document.createElement("div");',
        '        wrapper.className = "popup";',
        '        const content = document.createElement("div");',
        '        content.style.fontFamily = "Arial, sans-serif";',
        '        content.style.minWidth = "250px";',
        '        content.style.padding = "10px";',
        "        rows.forEach((row, rowIndex) => {",
        '            if (row && Object.prototype.hasOwnProperty.call(row, "title")) {',
        '                const titleNode = document.createElement("b");',
        '                titleNode.textContent = String(row.title ?? "");',
        "                content.appendChild(titleNode);",
        "            } else {",
        '                const labelNode = document.createElement("b");',
        '                labelNode.textContent = String(row?.label ?? "") + ":";',
        "                content.appendChild(labelNode);",
        '                content.appendChild(document.createTextNode(" " + String(row?.value ?? "")));',
        "            }",
        "            if (rowIndex < rows.length - 1) {",
        '                content.appendChild(document.createElement("br"));',
        "            }",
        "        });",
        "        wrapper.appendChild(content);",
        "        return wrapper;",
        "    }",
        "",
    ]


def _popup_click_handler_script_lines() -> List[str]:
    return [
        "    function featureHasPopupRows(feature) {",
        "        const rows = feature ? feature.get(\"popup_rows\") : null;",
        "        return Array.isArray(rows) && rows.length > 0;",
        "    }",
        "",
        '    map.on("click", event => {',
        "        const feature = map.forEachFeatureAtPixel(",
        "            event.pixel,",
        "            item => featureHasPopupRows(item) ? item : undefined,",
        "            { hitTolerance: 6 }",
        "        );",
        "        const popupElement = feature ? buildPopupElement(feature) : null;",
        "        if (feature && popupElement) {",
        "            const geometry = feature.getGeometry();",
        '            const coordinate = geometry.getType() === "Polygon"',
        "                ? geometry.getInteriorPoint().getCoordinates()",
        "                : geometry.getCoordinates();",
        "            overlay.setPosition(coordinate);",
        "            overlay.getElement().replaceChildren(popupElement);",
        "        } else {",
        "            overlay.getElement().replaceChildren();",
        "            overlay.setPosition(undefined);",
        "        }",
        "    });",
        "",
    ]


def build_popup_script_lines() -> List[str]:
    return _popup_overlay_script_lines() + _popup_builder_script_lines() + _popup_click_handler_script_lines()


def build_tab_script_lines(
    idx: int,
    table: MapTablePayload,
    container_id: str,
    analytics_layers: AnalyticsLayersPayload,
    analytics_defaults: SpatialLayerDefaults,
    heatmap_config: Dict[str, object],
    *,
    category_styles: Dict[str, CategoryStyleLike],
    json_for_script: Callable[[Any], str],
) -> List[str]:
    center_lon, center_lat = table["center"]
    initial_zoom = min(table.get("initial_zoom", 6) + 4, 13)
    styles_json = json_for_script({key: vars(value) for key, value in category_styles.items()})
    geojson_json = json_for_script(table["geojson"])
    analytics_layers_json = json_for_script(analytics_layers)
    analytics_defaults_json = json_for_script(analytics_defaults)
    heatmap_json = json_for_script(heatmap_config)
    return (
        build_map_setup_script_lines(
            idx,
            center_lon,
            center_lat,
            initial_zoom,
            styles_json,
            analytics_layers_json,
            analytics_defaults_json,
            heatmap_json,
            geojson_json,
        )
        + build_map_layer_script_lines()
        + build_popup_script_lines()
        + build_filter_script_lines(idx, container_id)
    )


__all__ = [
    "build_filter_script_lines",
    "build_map_layer_script_lines",
    "build_map_setup_script_lines",
    "build_popup_script_lines",
    "build_tab_script_lines",
]
