from __future__ import annotations

from typing import Callable, List

from app.services.forecast_risk.geo import _build_geo_prediction

from ...types import GeoPredictionPayload, HotspotPayload, ProcessedRecord


def build_hotspot_payloads(
    geo_prediction: GeoPredictionPayload,
    risk_level: Callable[[float], tuple[str, str]],
) -> List[HotspotPayload]:
    hotspots: List[HotspotPayload] = []
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
            'explanation': item.get('explanation') or 'Р›РѕРєР°Р»СЊРЅР°СЏ РєРѕРЅС†РµРЅС‚СЂР°С†РёСЏ РїРѕР¶Р°СЂРѕРІ РІС‹С€Рµ СЃСЂРµРґРЅРµРіРѕ.',
        })
    return hotspots


def build_hotspots_from_dated_records(
    dated_records: List[ProcessedRecord],
    notes: List[str],
    risk_level: Callable[[float], tuple[str, str]],
) -> List[HotspotPayload]:
    geo_prediction: GeoPredictionPayload = {'hotspots': []}
    if len(dated_records) >= 3:
        geo_prediction = _build_geo_prediction(dated_records, planning_horizon_days=30)
    elif dated_records:
        notes.append('Р”Р»СЏ hotspot-Р°РЅР°Р»РёР·Р° РґР°С‚ РїРѕРєР° РјР°Р»Рѕ, РїРѕСЌС‚РѕРјСѓ Р°РєС†РµРЅС‚ СЃРјРµС‰С‘РЅ РЅР° С‚РµРїР»РѕРІСѓСЋ РєР°СЂС‚Сѓ Рё РїСЂРёРѕСЂРёС‚РµС‚РЅС‹Рµ С‚РµСЂСЂРёС‚РѕСЂРёРё.')
    else:
        notes.append('Р”Р°С‚С‹ РїРѕР¶Р°СЂРѕРІ РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‚, РїРѕСЌС‚РѕРјСѓ hotspot-Р°РЅР°Р»РёР· РѕС‚РєР»СЋС‡С‘РЅ Рё Р·Р°РјРµРЅС‘РЅ СЂРµР·РµСЂРІРЅС‹Рј РїСЂРѕСЃС‚СЂР°РЅСЃС‚РІРµРЅРЅС‹Рј СЂРµР¶РёРјРѕРј.')
    return build_hotspot_payloads(geo_prediction, risk_level)


__all__ = ["build_hotspot_payloads", "build_hotspots_from_dated_records"]
