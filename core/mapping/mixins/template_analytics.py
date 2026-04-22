from __future__ import annotations

from typing import Any, Callable

from ...types import (
    AnalyticsLayersPayload,
    PopupRow,
    SpatialAnalyticsPayload,
    SpatialLayerDefaults,
)

_NO_DATA_DISPLAY = "н/д"
_UNDEFINED_ZONE_LABEL = "зона не определена"
_DEFAULT_ANALYTICS_TITLE = "Пространственная аналитика пожаров"
_DEFAULT_LOGISTICS_TITLE = "Сервисная зона не определена"
_DEFAULT_LOGISTICS_TEXT = "Логистический слой пока не рассчитан."
_DEFAULT_NO_INSIGHTS_HTML = "<li>Без дополнительных аналитических выводов.</li>"
_DEFAULT_NO_HOTSPOTS_HTML = (
    "<div class='analytics-item analytics-item-empty'>Hotspot-данные пока не выделены аналитикой.</div>"
)
_DEFAULT_NO_TERRITORIES_HTML = (
    "<div class='analytics-item analytics-item-empty'>Приоритетные территории пока не определены.</div>"
)


def build_analytics_layer_geojsons(
    analytics: SpatialAnalyticsPayload,
    build_popup_rows: Callable[..., list[PopupRow]],
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
                            ("Риск", item.get("risk_score_display", "")),
                            ("Пожаров", item.get("support_count", "")),
                            ("Пояснение", item.get("explanation", "")),
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
                            ("Локация", item.get("label", "")),
                            ("Риск", item.get("risk_score_display", "")),
                            ("Пожаров", item.get("incident_count", "")),
                            ("Пояснение", item.get("explanation", "")),
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
                            ("Зона", item.get("label", "")),
                            ("Источник", item.get("source", "")),
                            ("Риск", item.get("risk_score_display", "")),
                            ("Пояснение", item.get("explanation", "")),
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
                            ("Территория", item.get("label", "")),
                            ("Риск", item.get("risk_score_display", "")),
                            ("Пожаров", item.get("incident_count_display", "")),
                            ("Travel-time", item.get("travel_time_display", "")),
                            ("Факт прибытия", item.get("avg_response_display", "")),
                            ("Удалённость до ПЧ", item.get("avg_station_distance_display", "")),
                            ("Покрытие ПЧ", item.get("fire_station_coverage_display", "")),
                            ("Сервисная зона", item.get("service_zone_label", "")),
                            ("Логистический приоритет", item.get("logistics_priority_display", "")),
                            ("Пояснение", item.get("explanation", "")),
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


def default_analytics_layer_flags() -> dict[str, bool]:
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


def analytics_heatmap_config(analytics: SpatialAnalyticsPayload) -> dict[str, object]:
    heatmap = analytics.get("heatmap") or {}
    return {
        "enabled": bool(heatmap.get("enabled", False)),
        "radius": heatmap.get("radius", 20),
        "blur": heatmap.get("blur", 26),
    }


def analytics_layer_definitions(analytics_layers: AnalyticsLayersPayload) -> list[tuple[str, str, str, bool]]:
    return [
        ("incidents", "🗺", "Точки пожаров", True),
        ("heatmap", "🔥", "KDE / heatmap", bool(analytics_layers.get("heatmap", {}).get("features"))),
        ("hotspots", "📍", "Hotspot detection", bool(analytics_layers.get("hotspots", {}).get("features"))),
        ("clusters", "🧭", "DBSCAN кластеры", bool(analytics_layers.get("clusters", {}).get("features"))),
        ("risk_zones", "⚠", "Зоны риска", bool(analytics_layers.get("risk_zones", {}).get("features"))),
        ("priorities", "🎯", "Приоритетные территории", bool(analytics_layers.get("priorities", {}).get("features"))),
    ]


def build_analytics_panel_html(analytics: SpatialAnalyticsPayload, idx: int, escape: Callable[[Any], str]) -> str:
    quality = analytics.get("quality", {})
    dbscan = analytics.get("dbscan", {})
    logistics = analytics.get("logistics", {})
    summary = analytics.get("summary", {})
    summary_title = summary.get("title") or _DEFAULT_ANALYTICS_TITLE
    date_coverage_display = quality.get("date_coverage_display") or _NO_DATA_DISPLAY
    average_travel_time_display = logistics.get("average_travel_time_display") or _NO_DATA_DISPLAY
    fire_station_coverage_display = logistics.get("fire_station_coverage_display") or _NO_DATA_DISPLAY
    service_zone_label = logistics.get("service_zone_label") or _DEFAULT_LOGISTICS_TITLE
    logistics_text = escape(logistics.get("summary") or logistics.get("coverage_note") or _DEFAULT_LOGISTICS_TEXT)

    hotspot_items = "".join(
        (
            f"<div class='analytics-item'><strong>{escape(item.get('label', ''))}</strong>"
            f"<span>{escape(item.get('risk_score_display', ''))}</span>"
            f"<small>{escape(item.get('explanation', ''))}</small></div>"
        )
        for item in analytics.get("hotspots", [])[:4]
    ) or _DEFAULT_NO_HOTSPOTS_HTML

    territory_items = "".join(
        (
            f"<div class='analytics-item'><strong>{escape(item.get('label', ''))}</strong>"
            f"<span>{escape(item.get('risk_score_display', ''))}</span>"
            f"<small>{escape(item.get('travel_time_display') or _NO_DATA_DISPLAY)} | "
            f"{escape(item.get('fire_station_coverage_display') or _NO_DATA_DISPLAY)} | "
            f"{escape(item.get('service_zone_label') or _UNDEFINED_ZONE_LABEL)}</small></div>"
        )
        for item in analytics.get("priority_territories", [])[:5]
    ) or _DEFAULT_NO_TERRITORIES_HTML

    method_items = "".join(f"<span class='analytics-chip'>{escape(item)}</span>" for item in summary.get("methods", []))
    note_items = "".join(f"<li>{escape(item)}</li>" for item in summary.get("insights", [])) or _DEFAULT_NO_INSIGHTS_HTML
    thesis_items = "".join(f"<p>{escape(item)}</p>" for item in summary.get("thesis_paragraphs", []))
    fallback_message = quality.get("fallback_message")
    fallback_html = f"<div class='analytics-warning'>{escape(fallback_message)}</div>" if fallback_message else ""

    return f"""
        <div id="analytics-panel-{idx}" class="analytics-panel">
            <div class="analytics-head">
                <h5>{escape(summary_title)}</h5>
                <span>{escape(summary.get('subtitle', ''))}</span>
            </div>
            <div class="analytics-grid">
                <div class="analytics-card"><small>Покрытие координат</small><strong>{escape(quality.get('coordinate_coverage_display', '0'))}</strong></div>
                <div class="analytics-card"><small>Даты для hotspot</small><strong>{escape(date_coverage_display)}</strong></div>
                <div class="analytics-card"><small>Hotspot-ов</small><strong>{escape(len(analytics.get('hotspots', [])))}</strong></div>
                <div class="analytics-card"><small>DBSCAN кластеров</small><strong>{escape(dbscan.get('cluster_count', 0))}</strong></div>
                <div class="analytics-card"><small>Travel-time</small><strong>{escape(average_travel_time_display)}</strong></div>
                <div class="analytics-card"><small>Покрытие ПЧ</small><strong>{escape(fire_station_coverage_display)}</strong></div>
            </div>
            {fallback_html}
            <div class="analytics-section">
                <div class="analytics-section-title">Приоритетные территории</div>
                {territory_items}
            </div>
            <div class="analytics-section">
                <div class="analytics-section-title">Hotspot detection</div>
                {hotspot_items}
            </div>
            <div class="analytics-section">
                <div class="analytics-section-title">Логистика прибытия и прикрытия</div>
                <div class="analytics-item">
                    <strong>{escape(service_zone_label)}</strong>
                    <span>{escape(average_travel_time_display)} | {escape(fire_station_coverage_display)}</span>
                    <small>{logistics_text}</small>
                </div>
            </div>
            <div class="analytics-section">
                <div class="analytics-section-title">Методы</div>
                <div class="analytics-chip-group">{method_items}</div>
            </div>
            <div class="analytics-section">
                <div class="analytics-section-title">Ключевые выводы</div>
                <ul class="analytics-list">{note_items}</ul>
            </div>
            <details class="analytics-details">
                <summary>Тезисы для магистерской</summary>
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
