from __future__ import annotations

from typing import Any

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
    heatmap_points: list[HeatmapPoint],
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
        'eps_display': f"{dbscan.get('eps_km', 0.0):.2f} \u043a\u043c" if dbscan.get('eps_km') else '-',
        'min_samples': dbscan.get('min_samples', 0),
        'cluster_count': len(clusters),
        'noise_count': dbscan.get('noise_count', 0),
        'availability_note': dbscan.get('availability_note', ''),
    }


def build_spatial_layer_defaults(
    *,
    record_count: int,
    mode: str,
    hotspots: list[HotspotPayload],
    dbscan: DbscanResult,
    risk_zones: list[RiskZone],
    priority_territories: list[PriorityTerritory],
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
    heatmap_points: list[HeatmapPoint],
    hotspots: list[HotspotPayload],
    dbscan: DbscanResult,
    risk_zones: list[RiskZone],
    priority_territories: list[PriorityTerritory],
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
            'mode_label': 'Минимальный режим',
            'source_record_count': source_record_count,
            'valid_coordinate_count': 0,
            'coordinate_coverage_display': f'0 из {source_record_count}',
            'unique_coordinate_count': 0,
            'duplicate_ratio_percent': 0.0,
            'notes': ['Нет валидных координат для пространственной аналитики.'],
            'fallback_message': 'Карта остаётся доступной как точечная карта; аналитические слои не построены.',
        },
        'heatmap': {'enabled': False, 'points': [], 'radius': 20, 'blur': 26},
        'hotspots': [],
        'dbscan': {'enabled': False, 'clusters': [], 'eps_km': 0.0, 'eps_display': '-', 'min_samples': 0, 'cluster_count': 0, 'noise_count': 0, 'availability_note': 'Недостаточно данных.'},
        'risk_zones': [],
        'priority_territories': [],
        'logistics': {'basis_ready': False, 'summary': '', 'coverage_note': 'Логистический слой не рассчитан.'},
        'summary': {'title': 'Пространственная аналитика пожаров', 'subtitle': 'Нет данных для аналитического слоя.', 'methods': ['Точечный слой пожаров'], 'insights': ['Координаты отсутствуют или некорректны.'], 'thesis_paragraphs': ['Координаты отсутствуют или некорректны, поэтому карта используется только как точечная карта.'], 'fallback_message': 'Координаты отсутствуют или некорректны.'},
        'layer_defaults': {'incidents': True, 'heatmap': False, 'hotspots': False, 'clusters': False, 'risk_zones': False, 'priorities': False},
    }


def build_spatial_quality_context(
    records: list[ProcessedRecord],
    source_record_count: int,
) -> SpatialQualityContext:
    unique_coordinates = len({(item['latitude'], item['longitude']) for item in records})
    duplicate_ratio = (1.0 - unique_coordinates / max(len(records), 1)) * 100.0
    mode = 'full' if len(records) >= 18 and unique_coordinates >= 10 else 'limited' if len(records) >= 6 else 'minimal'
    mode_label = 'Полный режим' if mode == 'full' else 'Ограниченный режим' if mode == 'limited' else 'Минимальный режим'
    notes: list[str] = []
    if duplicate_ratio > 45.0:
        notes.append(f'Координаты заметно повторяются: {duplicate_ratio:.1f}% повторов.')
    if len(records) < source_record_count:
        notes.append(f'\u0421 \u043a\u043e\u043e\u0440\u0434\u0438\u043d\u0430\u0442\u0430\u043c\u0438 \u043e\u0441\u0442\u0430\u043b\u043e\u0441\u044c {len(records)} \u0438\u0437 {source_record_count} \u0437\u0430\u043f\u0438\u0441\u0435\u0439.')
    dated_records = [item for item in records if item.get('date') is not None]
    if len(dated_records) < len(records):
        notes.append(f'\u0414\u043b\u044f hotspot-\u0430\u043d\u0430\u043b\u0438\u0437\u0430 \u0434\u0430\u0442\u044b \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b \u0443 {len(dated_records)} \u0438\u0437 {len(records)} \u043f\u043e\u0436\u0430\u0440\u043e\u0432.')
    return {
        'unique_coordinates': unique_coordinates,
        'duplicate_ratio': duplicate_ratio,
        'mode': mode,
        'mode_label': mode_label,
        'notes': notes,
        'dated_records': dated_records,
    }


def build_heatmap_points(records: list[ProcessedRecord]) -> list[HeatmapPoint]:
    max_weight = max(item['weight'] for item in records) or 1.0
    return [
        {'latitude': item['latitude'], 'longitude': item['longitude'], 'weight': round(max(0.08, min(1.0, item['weight'] / max_weight)), 4)}
        for item in records
    ]


def build_spatial_methods(
    records: list[ProcessedRecord],
    hotspots: list[HotspotPayload],
    dbscan: DbscanResult,
    risk_zones: list[RiskZone],
    priority_territories: list[PriorityTerritory],
    logistics: LogisticsSummaryPayload,
) -> list[str]:
    methods = ['Точечный слой пожаров']
    if len(records) >= 3:
        methods.append('KDE / heatmap плотности')
    if hotspots:
        methods.append('Hotspot detection')
    if dbscan.get('clusters'):
        methods.append('DBSCAN по координатам')
    if risk_zones:
        methods.append('Зоны повышенного риска')
    if priority_territories:
        methods.append('Приоритетные территории')
    if logistics.get('basis_ready'):
        methods.append('Explainable travel-time, покрытие ПЧ и сервисные зоны')
    return methods


def build_spatial_insights(
    hotspots: list[HotspotPayload],
    priority_territories: list[PriorityTerritory],
    logistics: LogisticsSummaryPayload,
    dbscan: DbscanResult,
    notes: list[str],
) -> list[str]:
    insights = []
    if hotspots:
        insights.append(f"Главный hotspot: {hotspots[0]['label']} ({hotspots[0]['risk_score_display']}).")
    if priority_territories:
        insights.append(f"Приоритетная территория: {priority_territories[0]['label']} ({priority_territories[0]['risk_score_display']}).")
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
    records: list[ProcessedRecord],
    source_record_count: int,
    methods: list[str],
    risk_zones: list[RiskZone],
    priority_territories: list[PriorityTerritory],
    logistics: LogisticsSummaryPayload,
) -> list[str]:
    enhanced_methods = methods[1:] if len(methods) > 1 else ['ранжирования территорий и резервного пространственного режима']
    return [
        f"\u0414\u043b\u044f \u0442\u0430\u0431\u043b\u0438\u0446\u044b {table_name} \u043f\u0440\u043e\u0441\u0442\u0440\u0430\u043d\u0441\u0442\u0432\u0435\u043d\u043d\u044b\u0439 \u0430\u043d\u0430\u043b\u0438\u0437 \u0432\u044b\u043f\u043e\u043b\u043d\u0435\u043d \u043f\u043e {len(records)} \u043f\u043e\u0436\u0430\u0440\u0430\u043c \u0441 \u043a\u043e\u043e\u0440\u0434\u0438\u043d\u0430\u0442\u0430\u043c\u0438 \u0438\u0437 {source_record_count} \u0438\u0441\u0445\u043e\u0434\u043d\u044b\u0445 \u0437\u0430\u043f\u0438\u0441\u0435\u0439. \u0411\u0430\u0437\u043e\u0432\u0430\u044f \u0442\u043e\u0447\u0435\u0447\u043d\u0430\u044f \u043a\u0430\u0440\u0442\u0430 \u0441\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u0430, \u043d\u043e \u0443\u0441\u0438\u043b\u0435\u043d\u0430 \u043c\u0435\u0442\u043e\u0434\u0430\u043c\u0438 {', '.join(enhanced_methods)}.",
        f"\u041d\u0430 \u043a\u0430\u0440\u0442\u0435 \u0432\u044b\u0434\u0435\u043b\u0435\u043d\u043e {len(risk_zones)} \u0437\u043e\u043d \u043f\u043e\u0432\u044b\u0448\u0435\u043d\u043d\u043e\u0433\u043e \u0440\u0438\u0441\u043a\u0430 \u0438 {len(priority_territories)} \u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442\u043d\u044b\u0445 \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0439, \u0447\u0442\u043e \u043f\u0435\u0440\u0435\u0432\u043e\u0434\u0438\u0442 \u043a\u0430\u0440\u0442\u0443 \u0438\u0437 \u0440\u0435\u0436\u0438\u043c\u0430 \u0432\u0438\u0437\u0443\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u0438 \u0432 \u0440\u0435\u0436\u0438\u043c \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 \u043f\u0440\u0438\u043d\u044f\u0442\u0438\u044f \u0440\u0435\u0448\u0435\u043d\u0438\u0439 \u0434\u043b\u044f \u0441\u0435\u043b\u044c\u0441\u043a\u0438\u0445 \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0439.",
        logistics['summary'] if logistics.get('summary') else logistics.get('coverage_note') or 'Логистический слой пока носит разведочный характер.',
    ]


def build_spatial_quality_payload(
    records: list[ProcessedRecord],
    source_record_count: int,
    quality_context: SpatialQualityContext,
) -> SpatialQualityPayload:
    return {
        'mode': quality_context['mode'],
        'mode_label': quality_context['mode_label'],
        'source_record_count': source_record_count,
        'valid_coordinate_count': len(records),
        'coordinate_coverage_display': f"{len(records)} из {source_record_count} ({len(records) / max(source_record_count, 1) * 100.0:.1f}%)",
        'dated_record_count': len(quality_context['dated_records']),
        'date_coverage_display': f"{len(quality_context['dated_records'])} из {len(records)}",
        'unique_coordinate_count': quality_context['unique_coordinates'],
        'duplicate_ratio_percent': round(quality_context['duplicate_ratio'], 1),
        'notes': quality_context['notes'],
        'fallback_message': '' if quality_context['mode'] == 'full' else '\u0410\u043d\u0430\u043b\u0438\u0442\u0438\u043a\u0430 \u0440\u0430\u0431\u043e\u0442\u0430\u0435\u0442 \u0432 \u043e\u0431\u043b\u0435\u0433\u0447\u0451\u043d\u043d\u043e\u043c \u0440\u0435\u0436\u0438\u043c\u0435 \u0438\u0437-\u0437\u0430 \u043c\u0430\u043b\u043e\u0433\u043e \u0447\u0438\u0441\u043b\u0430 \u0443\u043d\u0438\u043a\u0430\u043b\u044c\u043d\u044b\u0445 \u043a\u043e\u043e\u0440\u0434\u0438\u043d\u0430\u0442.',
    }


def build_spatial_summary_payload(
    mode: str,
    methods: list[str],
    insights: list[str],
    thesis_paragraphs: list[str],
) -> SpatialSummaryPayload:
    return {
        'title': 'Пространственная аналитика пожаров',
        'subtitle': 'Карта дополнена слоями плотности, hotspot/DBSCAN, приоритетами территорий и explainable logistics-метриками доезда.',
        'methods': methods,
        'insights': insights[:5],
        'thesis_paragraphs': thesis_paragraphs,
        'fallback_message': '' if mode == 'full' else '\u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0435\u0442\u0441\u044f \u0440\u0435\u0437\u0435\u0440\u0432\u043d\u044b\u0439 \u0440\u0435\u0436\u0438\u043c \u0441 \u0443\u043f\u043e\u0440\u043e\u043c \u043d\u0430 \u0442\u0435\u043f\u043b\u043e\u0432\u0443\u044e \u043a\u0430\u0440\u0442\u0443 \u043f\u043b\u043e\u0442\u043d\u043e\u0441\u0442\u0438 \u0438 \u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442\u043d\u044b\u0435 \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0438.',
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

