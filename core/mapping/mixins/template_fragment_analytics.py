from __future__ import annotations

from typing import Any, Callable, Dict, List


def build_analytics_layer_geojsons(
    analytics: Dict[str, Any],
    build_popup_rows: Callable[..., List[Dict[str, str]]],
) -> Dict[str, Dict[str, Any]]:
    layers = {
        'heatmap': {'type': 'FeatureCollection', 'features': []},
        'hotspots': {'type': 'FeatureCollection', 'features': []},
        'clusters': {'type': 'FeatureCollection', 'features': []},
        'risk_zones': {'type': 'FeatureCollection', 'features': []},
        'priorities': {'type': 'FeatureCollection', 'features': []},
    }

    for point in analytics.get('heatmap', {}).get('points', []):
        layers['heatmap']['features'].append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [point['longitude'], point['latitude']]},
            'properties': {'weight': point['weight']},
        })

    for item in analytics.get('hotspots', []):
        layers['hotspots']['features'].append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [item['longitude'], item['latitude']]},
            'properties': {
                'popup_rows': build_popup_rows([
                    ('Hotspot', item.get('label', '')),
                    ('Риск', item.get('risk_score_display', '')),
                    ('Пожаров', item.get('support_count', '')),
                    ('Пояснение', item.get('explanation', '')),
                ]),
                'label': item.get('label', ''),
                'rank': item.get('rank'),
                'risk_tone': item.get('risk_tone'),
                'risk_score': item.get('risk_score'),
            },
        })

    for item in analytics.get('dbscan', {}).get('clusters', []):
        layers['clusters']['features'].append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [item['longitude'], item['latitude']]},
            'properties': {
                'popup_rows': build_popup_rows([
                    ('DBSCAN', item.get('cluster_display', '')),
                    ('Локация', item.get('label', '')),
                    ('Риск', item.get('risk_score_display', '')),
                    ('Пожаров', item.get('incident_count', '')),
                    ('Пояснение', item.get('explanation', '')),
                ]),
                'label': item.get('label', ''),
                'rank': item.get('rank'),
                'risk_tone': item.get('risk_tone'),
                'risk_score': item.get('risk_score'),
                'incident_count': item.get('incident_count'),
            },
        })

    for item in analytics.get('risk_zones', []):
        layers['risk_zones']['features'].append({
            'type': 'Feature',
            'geometry': {'type': 'Polygon', 'coordinates': [item['polygon']]},
            'properties': {
                'popup_rows': build_popup_rows(
                    [
                        ('Зона', item.get('label', '')),
                        ('Источник', item.get('source', '')),
                        ('Риск', item.get('risk_score_display', '')),
                        ('Пояснение', item.get('explanation', '')),
                    ],
                    title=item.get('priority_label', ''),
                ),
                'label': item.get('label', ''),
                'priority_label': item.get('priority_label', ''),
                'risk_tone': item.get('risk_tone'),
                'risk_score': item.get('risk_score'),
            },
        })

    for item in analytics.get('priority_territories', []):
        layers['priorities']['features'].append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [item['longitude'], item['latitude']]},
            'properties': {
                'popup_rows': build_popup_rows(
                    [
                        ('Территория', item.get('label', '')),
                        ('Риск', item.get('risk_score_display', '')),
                        ('Пожаров', item.get('incident_count_display', '')),
                        ('Travel-time', item.get('travel_time_display', '')),
                        ('Факт прибытия', item.get('avg_response_display', '')),
                        ('Удалённость до ПЧ', item.get('avg_station_distance_display', '')),
                        ('Покрытие ПЧ', item.get('fire_station_coverage_display', '')),
                        ('Сервисная зона', item.get('service_zone_label', '')),
                        ('Логистический приоритет', item.get('logistics_priority_display', '')),
                        ('Пояснение', item.get('explanation', '')),
                    ],
                    title=item.get('priority_label', ''),
                ),
                'label': item.get('label', ''),
                'rank': item.get('rank'),
                'risk_tone': item.get('risk_tone'),
                'risk_score': item.get('risk_score'),
            },
        })
    return layers

def default_analytics_layer_flags() -> Dict[str, bool]:
    return {
        'incidents': True,
        'heatmap': False,
        'hotspots': False,
        'clusters': False,
        'risk_zones': False,
        'priorities': False,
    }

def analytics_layer_defaults(analytics: Dict[str, Any]) -> Dict[str, bool]:
    return analytics.get('layer_defaults', default_analytics_layer_flags())

def analytics_heatmap_config(analytics: Dict[str, Any]) -> Dict[str, Any]:
    heatmap = analytics.get('heatmap') or {}
    return {
        'enabled': bool(heatmap.get('enabled', False)),
        'radius': heatmap.get('radius', 20),
        'blur': heatmap.get('blur', 26),
    }

def analytics_layer_definitions(analytics_layers: Dict[str, Dict[str, Any]]) -> List[tuple[str, str, str, bool]]:
    return [
        ('incidents', '&#128506;', '\u0422\u043e\u0447\u043a\u0438 \u043f\u043e\u0436\u0430\u0440\u043e\u0432', True),
        ('heatmap', '&#128293;', 'KDE / heatmap', bool(analytics_layers.get('heatmap', {}).get('features'))),
        ('hotspots', '&#128205;', 'Hotspot detection', bool(analytics_layers.get('hotspots', {}).get('features'))),
        ('clusters', '&#129517;', 'DBSCAN \u043a\u043b\u0430\u0441\u0442\u0435\u0440\u044b', bool(analytics_layers.get('clusters', {}).get('features'))),
        ('risk_zones', '&#9888;', '\u0417\u043e\u043d\u044b \u0440\u0438\u0441\u043a\u0430', bool(analytics_layers.get('risk_zones', {}).get('features'))),
        ('priorities', '&#127919;', '\u041f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442\u043d\u044b\u0435 \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0438', bool(analytics_layers.get('priorities', {}).get('features'))),
    ]


__all__ = [
    "build_analytics_layer_geojsons",
    "default_analytics_layer_flags",
    "analytics_layer_defaults",
    "analytics_heatmap_config",
    "analytics_layer_definitions",
]
