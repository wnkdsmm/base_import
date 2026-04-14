from __future__ import annotations

import math
from typing import Any, Callable, Dict, List

import numpy as np

from app.services.explainable_logistics import build_explainable_logistics_profile

from ...types import DbscanResult, HotspotPayload, PriorityTerritory, ProcessedRecord, RiskZone, SpatialPoint
from .analytics_geometry import group_records_by_field, mean_record_value


def build_priority_territories(
    records: List[ProcessedRecord],
    risk_zones: List[RiskZone],
    *,
    risk_level: Callable[[float], tuple[str, str]],
    km_distance: Callable[[SpatialPoint, SpatialPoint], float],
) -> List[PriorityTerritory]:
    grouped = group_records_by_field(records, 'territory_label')
    if not grouped:
        return []

    max_incidents = max(len(items) for items in grouped.values()) or 1
    max_weight = max(sum(point['weight'] for point in items) for items in grouped.values()) or 1.0
    territories: List[Dict[str, Any]] = []
    for label, items in grouped.items():
        incident_count = len(items)
        severe_count = sum(1 for item in items if item['has_victims'])
        avg_distance = mean_record_value(items, 'fire_station_distance')
        avg_response = mean_record_value(items, 'response_minutes')
        response_observations = sum(1 for item in items if item['response_minutes'] is not None)
        distance_observations = sum(1 for item in items if item['fire_station_distance'] is not None)
        if response_observations:
            long_arrival_rate = sum(1 for item in items if item['response_minutes'] is not None and float(item['response_minutes']) >= 20.0) / response_observations
        elif avg_response is not None:
            long_arrival_rate = min(1.0, max(0.0, (avg_response - 12.0) / 18.0))
        else:
            long_arrival_rate = 0.0
        zone_hits = sum(1 for item in items if any(km_distance(item, zone) <= zone['radius_km'] for zone in risk_zones[:4]))
        total_weight = sum(item['weight'] for item in items)
        is_rural = sum(1 for item in items if item.get('rural_flag')) >= max(1, math.ceil(incident_count / 2.0))
        logistics_profile = build_explainable_logistics_profile(
            avg_distance_km=avg_distance,
            avg_response_minutes=avg_response,
            long_arrival_rate=long_arrival_rate,
            is_rural=is_rural,
            response_observations=response_observations,
            distance_observations=distance_observations,
        )
        recurrence_component = incident_count / max_incidents
        severity_component = min(1.0, severe_count / max(incident_count, 1) + total_weight / max_weight * 0.45)
        hotspot_component = min(1.0, zone_hits / max(incident_count, 1))
        logistics_component = float(logistics_profile['logistics_priority_score']) / 100.0
        risk_score = round(100.0 * (
            recurrence_component * 0.32 +
            severity_component * 0.24 +
            logistics_component * 0.26 +
            hotspot_component * 0.18
        ), 1)
        risk_label, risk_tone = risk_level(risk_score)
        explanation_parts = [
            f"Территория {label} выделена по сочетанию повторяемости пожаров и логистической нагрузки.",
            f"Travel-time {logistics_profile['travel_time_display']}, покрытие ПЧ {logistics_profile['service_coverage_display']} ({logistics_profile['fire_station_coverage_label']}).",
            f"Сервисная зона: {logistics_profile['service_zone_label']}; логистический приоритет {logistics_profile['logistics_priority_display']}",
        ]
        if zone_hits:
            explanation_parts.append(f"Внутри верхних зон риска отмечено {zone_hits} исторических очагов.")
        elif severe_count:
            explanation_parts.append(f"Тяжёлые последствия фиксировались в {severe_count} случаях.")
        territories.append({
            'label': label,
            'latitude': round(float(np.mean([item['latitude'] for item in items])), 6),
            'longitude': round(float(np.mean([item['longitude'] for item in items])), 6),
            'incident_count': incident_count,
            'incident_count_display': str(incident_count),
            'severe_count': severe_count,
            'risk_score': risk_score,
            'risk_score_display': f'{risk_score:.1f} / 100',
            'risk_label': risk_label,
            'risk_tone': risk_tone,
            'avg_station_distance': round(avg_distance, 1) if avg_distance is not None else None,
            'avg_station_distance_display': f'{avg_distance:.1f} км' if avg_distance is not None else 'н/д',
            'avg_response_minutes': round(avg_response, 1) if avg_response is not None else None,
            'avg_response_display': f'{avg_response:.1f} мин' if avg_response is not None else 'н/д',
            'travel_time_minutes': logistics_profile['travel_time_minutes'],
            'travel_time_display': logistics_profile['travel_time_display'],
            'travel_time_source': logistics_profile['travel_time_source'],
            'fire_station_coverage_display': logistics_profile['service_coverage_display'],
            'fire_station_coverage_label': logistics_profile['fire_station_coverage_label'],
            'service_zone_label': logistics_profile['service_zone_label'],
            'service_zone_tone': logistics_profile['service_zone_tone'],
            'service_zone_reason': logistics_profile['service_zone_reason'],
            'logistics_priority_score': logistics_profile['logistics_priority_score'],
            'logistics_priority_display': logistics_profile['logistics_priority_display'],
            'logistics_priority_label': logistics_profile['logistics_priority_label'],
            'long_arrival_share': round(long_arrival_rate, 4),
            'long_arrival_share_display': f'{long_arrival_rate * 100.0:.1f}%',
            'zone_hits': zone_hits,
            'explanation': ' '.join(part for part in explanation_parts if part),
        })
    territories.sort(key=lambda item: (item['risk_score'], item['incident_count']), reverse=True)
    for rank, item in enumerate(territories, start=1):
        item['rank'] = rank
        item['priority_label'] = f'Территория #{rank}'
    return territories[:8]


def build_fallback_risk_zones(
    records: List[ProcessedRecord],
    priority_territories: List[PriorityTerritory],
    *,
    risk_level: Callable[[float], tuple[str, str]],
    km_distance: Callable[[SpatialPoint, SpatialPoint], float],
    build_circle_polygon: Callable[[float, float, float], List[List[float]]],
) -> List[RiskZone]:
    grouped = group_records_by_field(records, 'territory_label')
    fallback_zones: List[Dict[str, Any]] = []
    for rank, territory in enumerate(priority_territories[:3], start=1):
        items = grouped.get(territory['label'], [])
        if not items:
            continue

        center = {'latitude': territory['latitude'], 'longitude': territory['longitude']}
        distances = [km_distance(item, center) for item in items]
        radius_km = max(1.2, float(np.percentile(distances, 75)) if distances else 0.0)
        risk_score = max(float(territory['risk_score']), 35.0)
        risk_label, risk_tone = risk_level(risk_score)
        fallback_zones.append({
            'label': territory['label'],
            'latitude': territory['latitude'],
            'longitude': territory['longitude'],
            'radius_km': round(radius_km, 2),
            'risk_score': round(risk_score, 1),
            'risk_score_display': f'{risk_score:.1f} / 100',
            'risk_label': risk_label,
            'risk_tone': risk_tone,
            'support_count': territory['incident_count'],
            'source': 'Резервная территориальная зона',
            'explanation': 'Зона сформирована по центроиду приоритетной территории, так как сигнал hotspot/DBSCAN пока недостаточно устойчив.',
            'rank': rank,
            'priority_label': f'Приоритет {rank}',
            'polygon': build_circle_polygon(territory['longitude'], territory['latitude'], radius_km),
        })
    return fallback_zones


def build_spatial_risk_zones(
    dbscan: DbscanResult,
    hotspots: List[HotspotPayload],
    *,
    km_distance: Callable[[SpatialPoint, SpatialPoint], float],
    build_circle_polygon: Callable[[float, float, float], List[List[float]]],
) -> List[RiskZone]:
    risk_zone_candidates: List[Dict[str, Any]] = []
    for cluster in dbscan.get('clusters', []):
        risk_zone_candidates.append({
            'label': cluster['label'], 'latitude': cluster['latitude'], 'longitude': cluster['longitude'], 'radius_km': max(cluster['radius_km'], 1.0), 'risk_score': cluster['risk_score'], 'risk_score_display': cluster['risk_score_display'], 'risk_label': cluster['risk_label'], 'risk_tone': cluster['risk_tone'], 'support_count': cluster['incident_count'], 'source': 'DBSCAN', 'explanation': cluster['explanation'],
        })
    for hotspot in hotspots:
        if any(km_distance(hotspot, existing) < max(hotspot['radius_km'], existing['radius_km']) * 0.75 for existing in risk_zone_candidates):
            continue
        risk_zone_candidates.append({
            'label': hotspot['label'], 'latitude': hotspot['latitude'], 'longitude': hotspot['longitude'], 'radius_km': hotspot['radius_km'], 'risk_score': hotspot['risk_score'], 'risk_score_display': hotspot['risk_score_display'], 'risk_label': hotspot['risk_label'], 'risk_tone': hotspot['risk_tone'], 'support_count': hotspot['support_count'], 'source': 'Hotspot', 'explanation': hotspot['explanation'],
        })
    risk_zone_candidates.sort(key=lambda item: (item['risk_score'], item['support_count']), reverse=True)
    risk_zones: List[Dict[str, Any]] = []
    for rank, item in enumerate(risk_zone_candidates[:6], start=1):
        risk_zones.append({
            **item,
            'rank': rank,
            'priority_label': f'Приоритет {rank}',
            'polygon': build_circle_polygon(item['longitude'], item['latitude'], item['radius_km']),
        })
    return risk_zones


__all__ = [
    "build_fallback_risk_zones",
    "build_priority_territories",
    "build_spatial_risk_zones",
]
