from __future__ import annotations

from typing import Callable

from app.services.forecast_risk.geo import _build_geo_prediction

from ...types import GeoPredictionPayload, HotspotPayload, ProcessedRecord


def build_hotspot_payloads(
    geo_prediction: GeoPredictionPayload,
    risk_level: Callable[[float], tuple[str, str]],
) -> list[HotspotPayload]:
    hotspots: list[HotspotPayload] = []
    for rank, item in enumerate((geo_prediction.get('hotspots') or [])[:8], start=1):
        risk_score = float(item.get('risk_score') or 0.0)
        risk_label, risk_tone = risk_level(risk_score)
        hotspots.append({
            'rank': rank,
            'label': item.get('short_label') or item.get('location_label') or f'Hotspot {rank}',
            'latitude': float(item.get('latitude') or 0.0),
            'longitude': float(item.get('longitude') or 0.0),
            'support_count': int(item.get('incidents') or 0),
            'radius_km': max(0.9, 0.8 + (float(item.get('marker_size') or 12.0) / 8.0)),
            'risk_score': risk_score,
            'risk_score_display': item.get('risk_display') or f'{risk_score:.1f} / 100',
            'risk_label': risk_label,
            'risk_tone': risk_tone,
            'explanation': item.get('explanation') or 'Локальная концентрация пожаров выше среднего.',
        })
    return hotspots


def build_hotspots_from_dated_records(
    dated_records: list[ProcessedRecord],
    notes: list[str],
    risk_level: Callable[[float], tuple[str, str]],
) -> list[HotspotPayload]:
    geo_prediction: GeoPredictionPayload = {'hotspots': []}
    if len(dated_records) >= 3:
        geo_prediction = _build_geo_prediction(dated_records, planning_horizon_days=30)
    elif dated_records:
        notes.append('Для hotspot-анализа дат пока мало, поэтому акцент смещён на тепловую карту и приоритетные территории.')
    else:
        notes.append('Даты пожаров отсутствуют, поэтому hotspot-анализ отключён и заменён резервным пространственным режимом.')
    return build_hotspot_payloads(geo_prediction, risk_level)


__all__ = ["build_hotspot_payloads", "build_hotspots_from_dated_records"]
