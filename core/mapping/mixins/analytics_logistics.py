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
                f"Логистический слой готов: explainable travel-time {logistics_profile['travel_time_display']}, "
                f"покрытие ПЧ {logistics_profile['service_coverage_display']} ({logistics_profile['fire_station_coverage_label']}), "
                f"сервисная зона {logistics_profile['service_zone_label']}, доля долгих прибытий {long_arrival_share:.1f}%."
            )
        else:
            summary = (
                f"Логистический слой готов: explainable travel-time {logistics_profile['travel_time_display']}, "
                f"покрытие ПЧ {logistics_profile['service_coverage_display']} ({logistics_profile['fire_station_coverage_label']}), "
                f"сервисная зона {logistics_profile['service_zone_label']}."
            )
        if lead_territory:
            summary += (
                f" Наиболее напряжённая территория: {lead_territory['label']} "
                f"({lead_territory.get('logistics_priority_display', '\u043d/\u0434')}, {lead_territory.get('service_zone_label', '\u0437\u043e\u043d\u0430 \u043d\u0435 \u043e\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0430')})."
            )
    elif logistics_profile:
        coverage_note = (
            f"Логистические колонки найдены, но наблюдений пока мало для устойчивой оценки; текущий ориентир — travel-time "
            f"{logistics_profile['travel_time_display']}, покрытие ПЧ {logistics_profile['service_coverage_display']} и зона {logistics_profile['service_zone_label']}."
        )
    else:
        coverage_note = '\u041a\u043e\u043b\u043e\u043d\u043a\u0438 \u043b\u043e\u0433\u0438\u0441\u0442\u0438\u043a\u0438 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u044b \u0438\u043b\u0438 \u043f\u043e\u0447\u0442\u0438 \u043f\u0443\u0441\u0442\u044b\u0435, \u043f\u043e\u044d\u0442\u043e\u043c\u0443 \u043f\u0440\u0438\u043a\u043b\u0430\u0434\u043d\u043e\u0439 \u0430\u043d\u0430\u043b\u0438\u0437 \u0434\u043e\u0435\u0437\u0434\u0430 \u0438 \u043f\u0440\u0438\u043a\u0440\u044b\u0442\u0438\u044f \u043f\u043e\u043a\u0430 \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d.'

    return {
        'basis_ready': basis_ready,
        'average_station_distance': round(avg_distance, 1) if avg_distance is not None else None,
        'average_station_distance_display': f'{avg_distance:.1f} \u043a\u043c' if avg_distance is not None else '\u043d/\u0434',
        'average_response_minutes': round(avg_response, 1) if avg_response is not None else None,
        'average_response_display': f'{avg_response:.1f} \u043c\u0438\u043d' if avg_response is not None else '\u043d/\u0434',
        'average_travel_time_minutes': logistics_profile['travel_time_minutes'] if logistics_profile else None,
        'average_travel_time_display': logistics_profile['travel_time_display'] if logistics_profile else 'н/д',
        'long_arrival_share': round(long_arrival_share, 1) if long_arrival_share is not None else None,
        'long_arrival_share_display': f'{long_arrival_share:.1f}%' if long_arrival_share is not None else 'н/д',
        'fire_station_coverage_display': logistics_profile['service_coverage_display'] if logistics_profile else 'н/д',
        'fire_station_coverage_label': logistics_profile['fire_station_coverage_label'] if logistics_profile else 'нет данных',
        'service_zone_label': logistics_profile['service_zone_label'] if logistics_profile else 'зона не определена',
        'service_zone_reason': logistics_profile['service_zone_reason'] if logistics_profile else 'Недостаточно данных для объяснения сервисной зоны.',
        'logistics_priority_score': logistics_profile['logistics_priority_score'] if logistics_profile else 0.0,
        'logistics_priority_display': logistics_profile['logistics_priority_display'] if logistics_profile else '0 / 100',
        'logistics_priority_label': logistics_profile['logistics_priority_label'] if logistics_profile else 'Нет оценки',
        'summary': summary,
        'coverage_note': coverage_note,
        'top_delayed_territories': [
            {
                'label': item['label'],
                'travel_time_display': item.get('travel_time_display', 'н/д'),
                'avg_response_display': item['avg_response_display'],
                'avg_station_distance_display': item['avg_station_distance_display'],
                'fire_station_coverage_display': item.get('fire_station_coverage_display', 'н/д'),
                'service_zone_label': item.get('service_zone_label', 'зона не определена'),
                'logistics_priority_display': item.get('logistics_priority_display', '0 / 100'),
                'risk_score_display': item['risk_score_display'],
            }
            for item in priority_territories[:5]
        ],
    }


__all__ = ["build_logistics_summary_payload"]

