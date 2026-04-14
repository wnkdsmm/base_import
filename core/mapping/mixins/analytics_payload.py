from __future__ import annotations

from typing import Any, List

from ...types import (
    DbscanResult,
    HeatmapPoint,
    HotspotPayload,
    LogisticsSummaryPayload,
    PriorityTerritory,
    ProcessedRecord,
    RiskZone,
    SpatialAnalyticsPayload,
    SpatialDbscanPayload,
    SpatialHeatmapPayload,
    SpatialLayerDefaults,
    SpatialQualityContext,
    SpatialQualityPayload,
    SpatialSummaryPayload,
)


def build_spatial_heatmap_payload(
    record_count: int,
    heatmap_points: List[HeatmapPoint],
) -> SpatialHeatmapPayload:
    return {
        'enabled': record_count >= 3,
        'points': heatmap_points,
        'radius': 20,
        'blur': 26,
    }


def build_spatial_dbscan_payload(dbscan: DbscanResult) -> SpatialDbscanPayload:
    clusters = dbscan.get('clusters', [])
    return {
        'enabled': bool(clusters),
        'clusters': clusters,
        'eps_km': dbscan.get('eps_km', 0.0),
        'eps_display': f"{dbscan.get('eps_km', 0.0):.2f} РєРј" if dbscan.get('eps_km') else '-',
        'min_samples': dbscan.get('min_samples', 0),
        'cluster_count': len(clusters),
        'noise_count': dbscan.get('noise_count', 0),
        'availability_note': dbscan.get('availability_note', ''),
    }


def build_spatial_layer_defaults(
    *,
    record_count: int,
    mode: str,
    hotspots: List[HotspotPayload],
    dbscan: DbscanResult,
    risk_zones: List[RiskZone],
    priority_territories: List[PriorityTerritory],
) -> SpatialLayerDefaults:
    return {
        'incidents': True,
        'heatmap': record_count >= 3 and mode != 'minimal',
        'hotspots': bool(hotspots),
        'clusters': bool(dbscan.get('clusters')),
        'risk_zones': bool(risk_zones),
        'priorities': bool(priority_territories),
    }


def build_spatial_analytics_payload(
    *,
    quality: SpatialQualityPayload,
    record_count: int,
    heatmap_points: List[HeatmapPoint],
    hotspots: List[HotspotPayload],
    dbscan: DbscanResult,
    risk_zones: List[RiskZone],
    priority_territories: List[PriorityTerritory],
    logistics: LogisticsSummaryPayload,
    summary: SpatialSummaryPayload,
    mode: str,
) -> SpatialAnalyticsPayload:
    return {
        'quality': quality,
        'heatmap': build_spatial_heatmap_payload(record_count, heatmap_points),
        'hotspots': hotspots,
        'dbscan': build_spatial_dbscan_payload(dbscan),
        'risk_zones': risk_zones,
        'priority_territories': priority_territories,
        'logistics': logistics,
        'summary': summary,
        'layer_defaults': build_spatial_layer_defaults(
            record_count=record_count,
            mode=mode,
            hotspots=hotspots,
            dbscan=dbscan,
            risk_zones=risk_zones,
            priority_territories=priority_territories,
        ),
    }


def build_empty_spatial_analytics(source_record_count: int) -> SpatialAnalyticsPayload:
    return {
        'quality': {
            'mode': 'minimal',
            'mode_label': 'РњРёРЅРёРјР°Р»СЊРЅС‹Р№ СЂРµР¶РёРј',
            'source_record_count': source_record_count,
            'valid_coordinate_count': 0,
            'coordinate_coverage_display': f'0 РёР· {source_record_count}',
            'unique_coordinate_count': 0,
            'duplicate_ratio_percent': 0.0,
            'notes': ['РќРµС‚ РІР°Р»РёРґРЅС‹С… РєРѕРѕСЂРґРёРЅР°С‚ РґР»СЏ РїСЂРѕСЃС‚СЂР°РЅСЃС‚РІРµРЅРЅРѕР№ Р°РЅР°Р»РёС‚РёРєРё.'],
            'fallback_message': 'РљР°СЂС‚Р° РѕСЃС‚Р°С‘С‚СЃСЏ РґРѕСЃС‚СѓРїРЅРѕР№ РєР°Рє С‚РѕС‡РµС‡РЅР°СЏ РєР°СЂС‚Р°; Р°РЅР°Р»РёС‚РёС‡РµСЃРєРёРµ СЃР»РѕРё РЅРµ РїРѕСЃС‚СЂРѕРµРЅС‹.',
        },
        'heatmap': {'enabled': False, 'points': [], 'radius': 20, 'blur': 26},
        'hotspots': [],
        'dbscan': {'enabled': False, 'clusters': [], 'eps_km': 0.0, 'eps_display': '-', 'min_samples': 0, 'cluster_count': 0, 'noise_count': 0, 'availability_note': 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С….'},
        'risk_zones': [],
        'priority_territories': [],
        'logistics': {'basis_ready': False, 'summary': '', 'coverage_note': 'Р›РѕРіРёСЃС‚РёС‡РµСЃРєРёР№ СЃР»РѕР№ РЅРµ СЂР°СЃСЃС‡РёС‚Р°РЅ.'},
        'summary': {'title': 'РџСЂРѕСЃС‚СЂР°РЅСЃС‚РІРµРЅРЅР°СЏ Р°РЅР°Р»РёС‚РёРєР° РїРѕР¶Р°СЂРѕРІ', 'subtitle': 'РќРµС‚ РґР°РЅРЅС‹С… РґР»СЏ Р°РЅР°Р»РёС‚РёС‡РµСЃРєРѕРіРѕ СЃР»РѕСЏ.', 'methods': ['РўРѕС‡РµС‡РЅС‹Р№ СЃР»РѕР№ РїРѕР¶Р°СЂРѕРІ'], 'insights': ['РљРѕРѕСЂРґРёРЅР°С‚С‹ РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‚ РёР»Рё РЅРµРєРѕСЂСЂРµРєС‚РЅС‹.'], 'thesis_paragraphs': ['РљРѕРѕСЂРґРёРЅР°С‚С‹ РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‚ РёР»Рё РЅРµРєРѕСЂСЂРµРєС‚РЅС‹, РїРѕСЌС‚РѕРјСѓ РєР°СЂС‚Р° РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ С‚РѕР»СЊРєРѕ РєР°Рє С‚РѕС‡РµС‡РЅР°СЏ РєР°СЂС‚Р°.'], 'fallback_message': 'РљРѕРѕСЂРґРёРЅР°С‚С‹ РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‚ РёР»Рё РЅРµРєРѕСЂСЂРµРєС‚РЅС‹.'},
        'layer_defaults': {'incidents': True, 'heatmap': False, 'hotspots': False, 'clusters': False, 'risk_zones': False, 'priorities': False},
    }


def build_spatial_quality_context(
    records: List[ProcessedRecord],
    source_record_count: int,
) -> SpatialQualityContext:
    unique_coordinates = len({(item['latitude'], item['longitude']) for item in records})
    duplicate_ratio = (1.0 - unique_coordinates / max(len(records), 1)) * 100.0
    mode = 'full' if len(records) >= 18 and unique_coordinates >= 10 else 'limited' if len(records) >= 6 else 'minimal'
    mode_label = 'РџРѕР»РЅС‹Р№ СЂРµР¶РёРј' if mode == 'full' else 'РћРіСЂР°РЅРёС‡РµРЅРЅС‹Р№ СЂРµР¶РёРј' if mode == 'limited' else 'РњРёРЅРёРјР°Р»СЊРЅС‹Р№ СЂРµР¶РёРј'
    notes: List[str] = []
    if duplicate_ratio > 45.0:
        notes.append(f'РљРѕРѕСЂРґРёРЅР°С‚С‹ Р·Р°РјРµС‚РЅРѕ РїРѕРІС‚РѕСЂСЏСЋС‚СЃСЏ: {duplicate_ratio:.1f}% РїРѕРІС‚РѕСЂРѕРІ.')
    if len(records) < source_record_count:
        notes.append(f'РЎ РєРѕРѕСЂРґРёРЅР°С‚Р°РјРё РѕСЃС‚Р°Р»РѕСЃСЊ {len(records)} РёР· {source_record_count} Р·Р°РїРёСЃРµР№.')
    dated_records = [item for item in records if item.get('date') is not None]
    if len(dated_records) < len(records):
        notes.append(f'Р”Р»СЏ hotspot-Р°РЅР°Р»РёР·Р° РґР°С‚С‹ РґРѕСЃС‚СѓРїРЅС‹ Сѓ {len(dated_records)} РёР· {len(records)} РїРѕР¶Р°СЂРѕРІ.')
    return {
        'unique_coordinates': unique_coordinates,
        'duplicate_ratio': duplicate_ratio,
        'mode': mode,
        'mode_label': mode_label,
        'notes': notes,
        'dated_records': dated_records,
    }


def build_heatmap_points(records: List[ProcessedRecord]) -> List[HeatmapPoint]:
    max_weight = max(item['weight'] for item in records) or 1.0
    return [
        {'latitude': item['latitude'], 'longitude': item['longitude'], 'weight': round(max(0.08, min(1.0, item['weight'] / max_weight)), 4)}
        for item in records
    ]


def build_spatial_methods(
    records: List[ProcessedRecord],
    hotspots: List[HotspotPayload],
    dbscan: DbscanResult,
    risk_zones: List[RiskZone],
    priority_territories: List[PriorityTerritory],
    logistics: LogisticsSummaryPayload,
) -> List[str]:
    methods = ['РўРѕС‡РµС‡РЅС‹Р№ СЃР»РѕР№ РїРѕР¶Р°СЂРѕРІ']
    if len(records) >= 3:
        methods.append('KDE / heatmap РїР»РѕС‚РЅРѕСЃС‚Рё')
    if hotspots:
        methods.append('Hotspot detection')
    if dbscan.get('clusters'):
        methods.append('DBSCAN РїРѕ РєРѕРѕСЂРґРёРЅР°С‚Р°Рј')
    if risk_zones:
        methods.append('Р—РѕРЅС‹ РїРѕРІС‹С€РµРЅРЅРѕРіРѕ СЂРёСЃРєР°')
    if priority_territories:
        methods.append('РџСЂРёРѕСЂРёС‚РµС‚РЅС‹Рµ С‚РµСЂСЂРёС‚РѕСЂРёРё')
    if logistics.get('basis_ready'):
        methods.append('Explainable travel-time, РїРѕРєСЂС‹С‚РёРµ РџР§ Рё СЃРµСЂРІРёСЃРЅС‹Рµ Р·РѕРЅС‹')
    return methods


def build_spatial_insights(
    hotspots: List[HotspotPayload],
    priority_territories: List[PriorityTerritory],
    logistics: LogisticsSummaryPayload,
    dbscan: DbscanResult,
    notes: List[str],
) -> List[str]:
    insights = []
    if hotspots:
        insights.append(f"Р“Р»Р°РІРЅС‹Р№ hotspot: {hotspots[0]['label']} ({hotspots[0]['risk_score_display']}).")
    if priority_territories:
        insights.append(f"РџСЂРёРѕСЂРёС‚РµС‚РЅР°СЏ С‚РµСЂСЂРёС‚РѕСЂРёСЏ: {priority_territories[0]['label']} ({priority_territories[0]['risk_score_display']}).")
    if logistics.get('basis_ready'):
        insights.append(logistics['summary'])
    elif logistics.get('coverage_note'):
        insights.append(logistics['coverage_note'])
    if dbscan.get('availability_note'):
        insights.append(dbscan['availability_note'])
    insights.extend(notes[:2])
    return insights


def build_spatial_thesis_paragraphs(
    table_name: str,
    records: List[ProcessedRecord],
    source_record_count: int,
    methods: List[str],
    risk_zones: List[RiskZone],
    priority_territories: List[PriorityTerritory],
    logistics: LogisticsSummaryPayload,
) -> List[str]:
    enhanced_methods = methods[1:] if len(methods) > 1 else ['СЂР°РЅР¶РёСЂРѕРІР°РЅРёСЏ С‚РµСЂСЂРёС‚РѕСЂРёР№ Рё СЂРµР·РµСЂРІРЅРѕРіРѕ РїСЂРѕСЃС‚СЂР°РЅСЃС‚РІРµРЅРЅРѕРіРѕ СЂРµР¶РёРјР°']
    return [
        f"Р”Р»СЏ С‚Р°Р±Р»РёС†С‹ {table_name} РїСЂРѕСЃС‚СЂР°РЅСЃС‚РІРµРЅРЅС‹Р№ Р°РЅР°Р»РёР· РІС‹РїРѕР»РЅРµРЅ РїРѕ {len(records)} РїРѕР¶Р°СЂР°Рј СЃ РєРѕРѕСЂРґРёРЅР°С‚Р°РјРё РёР· {source_record_count} РёСЃС…РѕРґРЅС‹С… Р·Р°РїРёСЃРµР№. Р‘Р°Р·РѕРІР°СЏ С‚РѕС‡РµС‡РЅР°СЏ РєР°СЂС‚Р° СЃРѕС…СЂР°РЅРµРЅР°, РЅРѕ СѓСЃРёР»РµРЅР° РјРµС‚РѕРґР°РјРё {', '.join(enhanced_methods)}.",
        f"РќР° РєР°СЂС‚Рµ РІС‹РґРµР»РµРЅРѕ {len(risk_zones)} Р·РѕРЅ РїРѕРІС‹С€РµРЅРЅРѕРіРѕ СЂРёСЃРєР° Рё {len(priority_territories)} РїСЂРёРѕСЂРёС‚РµС‚РЅС‹С… С‚РµСЂСЂРёС‚РѕСЂРёР№, С‡С‚Рѕ РїРµСЂРµРІРѕРґРёС‚ РєР°СЂС‚Сѓ РёР· СЂРµР¶РёРјР° РІРёР·СѓР°Р»РёР·Р°С†РёРё РІ СЂРµР¶РёРј РїРѕРґРґРµСЂР¶РєРё РїСЂРёРЅСЏС‚РёСЏ СЂРµС€РµРЅРёР№ РґР»СЏ СЃРµР»СЊСЃРєРёС… С‚РµСЂСЂРёС‚РѕСЂРёР№.",
        logistics['summary'] if logistics.get('summary') else logistics.get('coverage_note') or 'Р›РѕРіРёСЃС‚РёС‡РµСЃРєРёР№ СЃР»РѕР№ РїРѕРєР° РЅРѕСЃРёС‚ СЂР°Р·РІРµРґРѕС‡РЅС‹Р№ С…Р°СЂР°РєС‚РµСЂ.',
    ]


def build_spatial_quality_payload(
    records: List[ProcessedRecord],
    source_record_count: int,
    quality_context: SpatialQualityContext,
) -> SpatialQualityPayload:
    return {
        'mode': quality_context['mode'],
        'mode_label': quality_context['mode_label'],
        'source_record_count': source_record_count,
        'valid_coordinate_count': len(records),
        'coordinate_coverage_display': f"{len(records)} РёР· {source_record_count} ({len(records) / max(source_record_count, 1) * 100.0:.1f}%)",
        'dated_record_count': len(quality_context['dated_records']),
        'date_coverage_display': f"{len(quality_context['dated_records'])} РёР· {len(records)}",
        'unique_coordinate_count': quality_context['unique_coordinates'],
        'duplicate_ratio_percent': round(quality_context['duplicate_ratio'], 1),
        'notes': quality_context['notes'],
        'fallback_message': '' if quality_context['mode'] == 'full' else 'РђРЅР°Р»РёС‚РёРєР° СЂР°Р±РѕС‚Р°РµС‚ РІ РѕР±Р»РµРіС‡С‘РЅРЅРѕРј СЂРµР¶РёРјРµ РёР·-Р·Р° РјР°Р»РѕРіРѕ С‡РёСЃР»Р° СѓРЅРёРєР°Р»СЊРЅС‹С… РєРѕРѕСЂРґРёРЅР°С‚.',
    }


def build_spatial_summary_payload(
    mode: str,
    methods: List[str],
    insights: List[str],
    thesis_paragraphs: List[str],
) -> SpatialSummaryPayload:
    return {
        'title': 'РџСЂРѕСЃС‚СЂР°РЅСЃС‚РІРµРЅРЅР°СЏ Р°РЅР°Р»РёС‚РёРєР° РїРѕР¶Р°СЂРѕРІ',
        'subtitle': 'РљР°СЂС‚Р° РґРѕРїРѕР»РЅРµРЅР° СЃР»РѕСЏРјРё РїР»РѕС‚РЅРѕСЃС‚Рё, hotspot/DBSCAN, РїСЂРёРѕСЂРёС‚РµС‚Р°РјРё С‚РµСЂСЂРёС‚РѕСЂРёР№ Рё explainable logistics-РјРµС‚СЂРёРєР°РјРё РґРѕРµР·РґР°.',
        'methods': methods,
        'insights': insights[:5],
        'thesis_paragraphs': thesis_paragraphs,
        'fallback_message': '' if mode == 'full' else 'РСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ СЂРµР·РµСЂРІРЅС‹Р№ СЂРµР¶РёРј СЃ СѓРїРѕСЂРѕРј РЅР° С‚РµРїР»РѕРІСѓСЋ РєР°СЂС‚Сѓ РїР»РѕС‚РЅРѕСЃС‚Рё Рё РїСЂРёРѕСЂРёС‚РµС‚РЅС‹Рµ С‚РµСЂСЂРёС‚РѕСЂРёРё.',
    }


__all__ = [
    "build_empty_spatial_analytics",
    "build_heatmap_points",
    "build_spatial_analytics_payload",
    "build_spatial_dbscan_payload",
    "build_spatial_heatmap_payload",
    "build_spatial_insights",
    "build_spatial_layer_defaults",
    "build_spatial_methods",
    "build_spatial_quality_context",
    "build_spatial_quality_payload",
    "build_spatial_summary_payload",
    "build_spatial_thesis_paragraphs",
]
