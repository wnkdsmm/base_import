from __future__ import annotations

from typing import Any, Callable, Dict, List

from ...types import (
    AnalyticsLayersPayload,
    PopupRow,
    SpatialAnalyticsPayload,
    SpatialLayerDefaults,
)


def build_analytics_layer_geojsons(
    analytics: SpatialAnalyticsPayload,
    build_popup_rows: Callable[..., List[PopupRow]],
) -> AnalyticsLayersPayload:
    layers = {
        "heatmap": {"type": "FeatureCollection", "features": []},
        "hotspots": {"type": "FeatureCollection", "features": []},
        "clusters": {"type": "FeatureCollection", "features": []},
        "risk_zones": {"type": "FeatureCollection", "features": []},
        "priorities": {"type": "FeatureCollection", "features": []},
    }

    for point in analytics.get("heatmap", {}).get("points", []):
        layers["heatmap"]["features"].append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [point["longitude"], point["latitude"]]},
                "properties": {"weight": point["weight"]},
            }
        )

    for item in analytics.get("hotspots", []):
        layers["hotspots"]["features"].append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [item["longitude"], item["latitude"]]},
                "properties": {
                    "popup_rows": build_popup_rows(
                        [
                            ("Hotspot", item.get("label", "")),
                            ("\u0420\u0438\u0441\u043a", item.get("risk_score_display", "")),
                            ("\u041f\u043e\u0436\u0430\u0440\u043e\u0432", item.get("support_count", "")),
                            ("\u041f\u043e\u044f\u0441\u043d\u0435\u043d\u0438\u0435", item.get("explanation", "")),
                        ]
                    ),
                    "label": item.get("label", ""),
                    "rank": item.get("rank"),
                    "risk_tone": item.get("risk_tone"),
                    "risk_score": item.get("risk_score"),
                },
            }
        )

    for item in analytics.get("dbscan", {}).get("clusters", []):
        layers["clusters"]["features"].append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [item["longitude"], item["latitude"]]},
                "properties": {
                    "popup_rows": build_popup_rows(
                        [
                            ("DBSCAN", item.get("cluster_display", "")),
                            ("\u041b\u043e\u043a\u0430\u0446\u0438\u044f", item.get("label", "")),
                            ("\u0420\u0438\u0441\u043a", item.get("risk_score_display", "")),
                            ("\u041f\u043e\u0436\u0430\u0440\u043e\u0432", item.get("incident_count", "")),
                            ("\u041f\u043e\u044f\u0441\u043d\u0435\u043d\u0438\u0435", item.get("explanation", "")),
                        ]
                    ),
                    "label": item.get("label", ""),
                    "rank": item.get("rank"),
                    "risk_tone": item.get("risk_tone"),
                    "risk_score": item.get("risk_score"),
                    "incident_count": item.get("incident_count"),
                },
            }
        )

    for item in analytics.get("risk_zones", []):
        layers["risk_zones"]["features"].append(
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [item["polygon"]]},
                "properties": {
                    "popup_rows": build_popup_rows(
                        [
                            ("\u0417\u043e\u043d\u0430", item.get("label", "")),
                            ("\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a", item.get("source", "")),
                            ("\u0420\u0438\u0441\u043a", item.get("risk_score_display", "")),
                            ("\u041f\u043e\u044f\u0441\u043d\u0435\u043d\u0438\u0435", item.get("explanation", "")),
                        ],
                        title=item.get("priority_label", ""),
                    ),
                    "label": item.get("label", ""),
                    "priority_label": item.get("priority_label", ""),
                    "risk_tone": item.get("risk_tone"),
                    "risk_score": item.get("risk_score"),
                },
            }
        )

    for item in analytics.get("priority_territories", []):
        layers["priorities"]["features"].append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [item["longitude"], item["latitude"]]},
                "properties": {
                    "popup_rows": build_popup_rows(
                        [
                            ("\u0422\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u044f", item.get("label", "")),
                            ("\u0420\u0438\u0441\u043a", item.get("risk_score_display", "")),
                            ("\u041f\u043e\u0436\u0430\u0440\u043e\u0432", item.get("incident_count_display", "")),
                            ("Travel-time", item.get("travel_time_display", "")),
                            ("\u0424\u0430\u043a\u0442 \u043f\u0440\u0438\u0431\u044b\u0442\u0438\u044f", item.get("avg_response_display", "")),
                            ("\u0423\u0434\u0430\u043b\u0451\u043d\u043d\u043e\u0441\u0442\u044c \u0434\u043e \u041f\u0427", item.get("avg_station_distance_display", "")),
                            ("\u041f\u043e\u043a\u0440\u044b\u0442\u0438\u0435 \u041f\u0427", item.get("fire_station_coverage_display", "")),
                            ("\u0421\u0435\u0440\u0432\u0438\u0441\u043d\u0430\u044f \u0437\u043e\u043d\u0430", item.get("service_zone_label", "")),
                            (
                                "\u041b\u043e\u0433\u0438\u0441\u0442\u0438\u0447\u0435\u0441\u043a\u0438\u0439 \u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442",
                                item.get("logistics_priority_display", ""),
                            ),
                            ("\u041f\u043e\u044f\u0441\u043d\u0435\u043d\u0438\u0435", item.get("explanation", "")),
                        ],
                        title=item.get("priority_label", ""),
                    ),
                    "label": item.get("label", ""),
                    "rank": item.get("rank"),
                    "risk_tone": item.get("risk_tone"),
                    "risk_score": item.get("risk_score"),
                },
            }
        )
    return layers


def default_analytics_layer_flags() -> Dict[str, bool]:
    return {
        "incidents": True,
        "heatmap": False,
        "hotspots": False,
        "clusters": False,
        "risk_zones": False,
        "priorities": False,
    }


def analytics_layer_defaults(analytics: SpatialAnalyticsPayload) -> SpatialLayerDefaults:
    return analytics.get("layer_defaults", default_analytics_layer_flags())


def analytics_heatmap_config(analytics: SpatialAnalyticsPayload) -> Dict[str, object]:
    heatmap = analytics.get("heatmap") or {}
    return {
        "enabled": bool(heatmap.get("enabled", False)),
        "radius": heatmap.get("radius", 20),
        "blur": heatmap.get("blur", 26),
    }


def analytics_layer_definitions(analytics_layers: AnalyticsLayersPayload) -> List[tuple[str, str, str, bool]]:
    return [
        ("incidents", "&#128506;", "\u0422\u043e\u0447\u043a\u0438 \u043f\u043e\u0436\u0430\u0440\u043e\u0432", True),
        ("heatmap", "&#128293;", "KDE / heatmap", bool(analytics_layers.get("heatmap", {}).get("features"))),
        ("hotspots", "&#128205;", "Hotspot detection", bool(analytics_layers.get("hotspots", {}).get("features"))),
        ("clusters", "&#129517;", "DBSCAN \u043a\u043b\u0430\u0441\u0442\u0435\u0440\u044b", bool(analytics_layers.get("clusters", {}).get("features"))),
        ("risk_zones", "&#9888;", "\u0417\u043e\u043d\u044b \u0440\u0438\u0441\u043a\u0430", bool(analytics_layers.get("risk_zones", {}).get("features"))),
        ("priorities", "&#127919;", "\u041f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442\u043d\u044b\u0435 \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0438", bool(analytics_layers.get("priorities", {}).get("features"))),
    ]


def build_analytics_panel_html(analytics: SpatialAnalyticsPayload, idx: int, escape: Callable[[Any], str]) -> str:
    quality = analytics.get("quality", {})
    dbscan = analytics.get("dbscan", {})
    logistics = analytics.get("logistics", {})
    summary = analytics.get("summary", {})
    hotspot_items = "".join(
        f"<div class='analytics-item'><strong>{escape(item.get('label', ''))}</strong><span>{escape(item.get('risk_score_display', ''))}</span><small>{escape(item.get('explanation', ''))}</small></div>"
        for item in analytics.get("hotspots", [])[:4]
    ) or "<div class='analytics-item analytics-item-empty'>Hotspot-\u0434\u0430\u043d\u043d\u044b\u0435 \u043f\u043e\u043a\u0430 \u043d\u0435 \u0432\u044b\u0434\u0435\u043b\u0435\u043d\u044b \u0430\u043d\u0430\u043b\u0438\u0442\u0438\u043a\u043e\u0439.</div>"
    territory_items = "".join(
        f"<div class='analytics-item'><strong>{escape(item.get('label', ''))}</strong><span>{escape(item.get('risk_score_display', ''))}</span><small>{escape(item.get('travel_time_display', '\u043d/\u0434'))} | {escape(item.get('fire_station_coverage_display', '\u043d/\u0434'))} | {escape(item.get('service_zone_label', '\u0437\u043e\u043d\u0430 \u043d\u0435 \u043e\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0430'))}</small></div>"
        for item in analytics.get("priority_territories", [])[:5]
    ) or "<div class='analytics-item analytics-item-empty'>\u041f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442\u043d\u044b\u0435 \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0438 \u043f\u043e\u043a\u0430 \u043d\u0435 \u043e\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u044b.</div>"
    method_items = "".join(f"<span class='analytics-chip'>{escape(item)}</span>" for item in summary.get("methods", []))
    note_items = "".join(f"<li>{escape(item)}</li>" for item in summary.get("insights", [])) or "<li>\u0411\u0435\u0437 \u0434\u043e\u043f\u043e\u043b\u043d\u0438\u0442\u0435\u043b\u044c\u043d\u044b\u0445 \u0430\u043d\u0430\u043b\u0438\u0442\u0438\u0447\u0435\u0441\u043a\u0438\u0445 \u0432\u044b\u0432\u043e\u0434\u043e\u0432.</li>"
    thesis_items = "".join(f"<p>{escape(item)}</p>" for item in summary.get("thesis_paragraphs", []))
    fallback_message = quality.get("fallback_message")
    fallback_html = f"<div class='analytics-warning'>{escape(fallback_message)}</div>" if fallback_message else ""
    logistics_text = escape(logistics.get("summary") or logistics.get("coverage_note") or "\u041b\u043e\u0433\u0438\u0441\u0442\u0438\u0447\u0435\u0441\u043a\u0438\u0439 \u0441\u043b\u043e\u0439 \u043f\u043e\u043a\u0430 \u043d\u0435 \u0440\u0430\u0441\u0441\u0447\u0438\u0442\u0430\u043d.")
    return f"""
        <div id="analytics-panel-{idx}" class="analytics-panel">
            <div class="analytics-head">
                <h5>{escape(summary.get('title', '\u041f\u0440\u043e\u0441\u0442\u0440\u0430\u043d\u0441\u0442\u0432\u0435\u043d\u043d\u0430\u044f \u0430\u043d\u0430\u043b\u0438\u0442\u0438\u043a\u0430 \u043f\u043e\u0436\u0430\u0440\u043e\u0432'))}</h5>
                <span>{escape(summary.get('subtitle', ''))}</span>
            </div>
            <div class="analytics-grid">
                <div class="analytics-card"><small>\u041f\u043e\u043a\u0440\u044b\u0442\u0438\u0435 \u043a\u043e\u043e\u0440\u0434\u0438\u043d\u0430\u0442</small><strong>{escape(quality.get('coordinate_coverage_display', '0'))}</strong></div>
                <div class="analytics-card"><small>\u0414\u0430\u0442\u044b \u0434\u043b\u044f hotspot</small><strong>{escape(quality.get('date_coverage_display', '\u043d/\u0434'))}</strong></div>
                <div class="analytics-card"><small>Hotspot-\u043e\u0432</small><strong>{escape(len(analytics.get('hotspots', [])))}</strong></div>
                <div class="analytics-card"><small>DBSCAN \u043a\u043b\u0430\u0441\u0442\u0435\u0440\u043e\u0432</small><strong>{escape(dbscan.get('cluster_count', 0))}</strong></div>
                <div class="analytics-card"><small>Travel-time</small><strong>{escape(logistics.get('average_travel_time_display', '\u043d/\u0434'))}</strong></div>
                <div class="analytics-card"><small>\u041f\u043e\u043a\u0440\u044b\u0442\u0438\u0435 \u041f\u0427</small><strong>{escape(logistics.get('fire_station_coverage_display', '\u043d/\u0434'))}</strong></div>
            </div>
            {fallback_html}
            <div class="analytics-section">
                <div class="analytics-section-title">\u041f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442\u043d\u044b\u0435 \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0438</div>
                {territory_items}
            </div>
            <div class="analytics-section">
                <div class="analytics-section-title">Hotspot detection</div>
                {hotspot_items}
            </div>
            <div class="analytics-section">
                <div class="analytics-section-title">\u041b\u043e\u0433\u0438\u0441\u0442\u0438\u043a\u0430 \u043f\u0440\u0438\u0431\u044b\u0442\u0438\u044f \u0438 \u043f\u0440\u0438\u043a\u0440\u044b\u0442\u0438\u044f</div>
                <div class="analytics-item">
                    <strong>{escape(logistics.get('service_zone_label', '\u0421\u0435\u0440\u0432\u0438\u0441\u043d\u0430\u044f \u0437\u043e\u043d\u0430 \u043d\u0435 \u043e\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0430'))}</strong>
                    <span>{escape(logistics.get('average_travel_time_display', '\u043d/\u0434'))} | {escape(logistics.get('fire_station_coverage_display', '\u043d/\u0434'))}</span>
                    <small>{logistics_text}</small>
                </div>
            </div>
            <div class="analytics-section">
                <div class="analytics-section-title">\u041c\u0435\u0442\u043e\u0434\u044b</div>
                <div class="analytics-chip-group">{method_items}</div>
            </div>
            <div class="analytics-section">
                <div class="analytics-section-title">\u041a\u043b\u044e\u0447\u0435\u0432\u044b\u0435 \u0432\u044b\u0432\u043e\u0434\u044b</div>
                <ul class="analytics-list">{note_items}</ul>
            </div>
            <details class="analytics-details">
                <summary>\u0422\u0435\u0437\u0438\u0441\u044b \u0434\u043b\u044f \u043c\u0430\u0433\u0438\u0441\u0442\u0435\u0440\u0441\u043a\u043e\u0439</summary>
                <div class="analytics-thesis">{thesis_items}</div>
            </details>
        </div>
        """


__all__ = [
    "analytics_heatmap_config",
    "analytics_layer_defaults",
    "analytics_layer_definitions",
    "build_analytics_layer_geojsons",
    "build_analytics_panel_html",
    "default_analytics_layer_flags",
]
