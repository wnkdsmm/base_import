from __future__ import annotations

from typing import List

import numpy as np

from app.services.explainable_logistics import build_explainable_logistics_profile

from ...types import LogisticsSummaryPayload, PriorityTerritory, ProcessedRecord


def build_logistics_summary_payload(
    records: List[ProcessedRecord],
    priority_territories: List[PriorityTerritory],
) -> LogisticsSummaryPayload:
    distance_values = [item['fire_station_distance'] for item in records if item['fire_station_distance'] is not None]
    response_values = [item['response_minutes'] for item in records if item['response_minutes'] is not None]
    basis_ready = len(distance_values) >= 8 or len(response_values) >= 8
    avg_distance = float(np.mean(distance_values)) if distance_values else None
    avg_response = float(np.mean(response_values)) if response_values else None
    long_arrival_share = (sum(1 for value in response_values if value >= 20.0) / len(response_values) * 100.0) if response_values else None
    rural_share = (sum(1 for item in records if item.get('rural_flag')) / len(records)) if records else 0.0
    long_arrival_rate = (long_arrival_share / 100.0) if long_arrival_share is not None else (min(1.0, max(0.0, ((avg_response or 12.0) - 12.0) / 18.0)) if avg_response is not None else 0.0)
    logistics_profile = build_explainable_logistics_profile(
        avg_distance_km=avg_distance,
        avg_response_minutes=avg_response,
        long_arrival_rate=long_arrival_rate,
        is_rural=rural_share >= 0.5,
        response_observations=len(response_values),
        distance_observations=len(distance_values),
    ) if (distance_values or response_values) else None

    summary = ''
    coverage_note = ''
    lead_territory = priority_territories[0] if priority_territories else None
    if basis_ready and logistics_profile:
        if long_arrival_share is not None:
            summary = (
                f"Р›РѕРіРёСЃС‚РёС‡РµСЃРєРёР№ СЃР»РѕР№ РіРѕС‚РѕРІ: explainable travel-time {logistics_profile['travel_time_display']}, "
                f"РїРѕРєСЂС‹С‚РёРµ РџР§ {logistics_profile['service_coverage_display']} ({logistics_profile['fire_station_coverage_label']}), "
                f"СЃРµСЂРІРёСЃРЅР°СЏ Р·РѕРЅР° {logistics_profile['service_zone_label']}, РґРѕР»СЏ РґРѕР»РіРёС… РїСЂРёР±С‹С‚РёР№ {long_arrival_share:.1f}%."
            )
        else:
            summary = (
                f"Р›РѕРіРёСЃС‚РёС‡РµСЃРєРёР№ СЃР»РѕР№ РіРѕС‚РѕРІ: explainable travel-time {logistics_profile['travel_time_display']}, "
                f"РїРѕРєСЂС‹С‚РёРµ РџР§ {logistics_profile['service_coverage_display']} ({logistics_profile['fire_station_coverage_label']}), "
                f"СЃРµСЂРІРёСЃРЅР°СЏ Р·РѕРЅР° {logistics_profile['service_zone_label']}."
            )
        if lead_territory:
            summary += (
                f" РќР°РёР±РѕР»РµРµ РЅР°РїСЂСЏР¶С‘РЅРЅР°СЏ С‚РµСЂСЂРёС‚РѕСЂРёСЏ: {lead_territory['label']} "
                f"({lead_territory.get('logistics_priority_display', 'РЅ/Рґ')}, {lead_territory.get('service_zone_label', 'Р·РѕРЅР° РЅРµ РѕРїСЂРµРґРµР»РµРЅР°')})."
            )
    elif logistics_profile:
        coverage_note = (
            f"Р›РѕРіРёСЃС‚РёС‡РµСЃРєРёРµ РєРѕР»РѕРЅРєРё РЅР°Р№РґРµРЅС‹, РЅРѕ РЅР°Р±Р»СЋРґРµРЅРёР№ РїРѕРєР° РјР°Р»Рѕ РґР»СЏ СѓСЃС‚РѕР№С‡РёРІРѕР№ РѕС†РµРЅРєРё; С‚РµРєСѓС‰РёР№ РѕСЂРёРµРЅС‚РёСЂ вЂ” travel-time "
            f"{logistics_profile['travel_time_display']}, РїРѕРєСЂС‹С‚РёРµ РџР§ {logistics_profile['service_coverage_display']} Рё Р·РѕРЅР° {logistics_profile['service_zone_label']}."
        )
    else:
        coverage_note = 'РљРѕР»РѕРЅРєРё Р»РѕРіРёСЃС‚РёРєРё РЅРµ РЅР°Р№РґРµРЅС‹ РёР»Рё РїРѕС‡С‚Рё РїСѓСЃС‚С‹Рµ, РїРѕСЌС‚РѕРјСѓ РїСЂРёРєР»Р°РґРЅРѕР№ Р°РЅР°Р»РёР· РґРѕРµР·РґР° Рё РїСЂРёРєСЂС‹С‚РёСЏ РїРѕРєР° РЅРµРґРѕСЃС‚СѓРїРµРЅ.'

    return {
        'basis_ready': basis_ready,
        'average_station_distance': round(avg_distance, 1) if avg_distance is not None else None,
        'average_station_distance_display': f'{avg_distance:.1f} РєРј' if avg_distance is not None else 'РЅ/Рґ',
        'average_response_minutes': round(avg_response, 1) if avg_response is not None else None,
        'average_response_display': f'{avg_response:.1f} РјРёРЅ' if avg_response is not None else 'РЅ/Рґ',
        'average_travel_time_minutes': logistics_profile['travel_time_minutes'] if logistics_profile else None,
        'average_travel_time_display': logistics_profile['travel_time_display'] if logistics_profile else 'РЅ/Рґ',
        'long_arrival_share': round(long_arrival_share, 1) if long_arrival_share is not None else None,
        'long_arrival_share_display': f'{long_arrival_share:.1f}%' if long_arrival_share is not None else 'РЅ/Рґ',
        'fire_station_coverage_display': logistics_profile['service_coverage_display'] if logistics_profile else 'РЅ/Рґ',
        'fire_station_coverage_label': logistics_profile['fire_station_coverage_label'] if logistics_profile else 'РЅРµС‚ РґР°РЅРЅС‹С…',
        'service_zone_label': logistics_profile['service_zone_label'] if logistics_profile else 'Р·РѕРЅР° РЅРµ РѕРїСЂРµРґРµР»РµРЅР°',
        'service_zone_reason': logistics_profile['service_zone_reason'] if logistics_profile else 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РѕР±СЉСЏСЃРЅРµРЅРёСЏ СЃРµСЂРІРёСЃРЅРѕР№ Р·РѕРЅС‹.',
        'logistics_priority_score': logistics_profile['logistics_priority_score'] if logistics_profile else 0.0,
        'logistics_priority_display': logistics_profile['logistics_priority_display'] if logistics_profile else '0 / 100',
        'logistics_priority_label': logistics_profile['logistics_priority_label'] if logistics_profile else 'РќРµС‚ РѕС†РµРЅРєРё',
        'summary': summary,
        'coverage_note': coverage_note,
        'top_delayed_territories': [
            {
                'label': item['label'],
                'travel_time_display': item.get('travel_time_display', 'РЅ/Рґ'),
                'avg_response_display': item['avg_response_display'],
                'avg_station_distance_display': item['avg_station_distance_display'],
                'fire_station_coverage_display': item.get('fire_station_coverage_display', 'РЅ/Рґ'),
                'service_zone_label': item.get('service_zone_label', 'Р·РѕРЅР° РЅРµ РѕРїСЂРµРґРµР»РµРЅР°'),
                'logistics_priority_display': item.get('logistics_priority_display', '0 / 100'),
                'risk_score_display': item['risk_score_display'],
            }
            for item in priority_territories[:5]
        ],
    }


__all__ = ["build_logistics_summary_payload"]
