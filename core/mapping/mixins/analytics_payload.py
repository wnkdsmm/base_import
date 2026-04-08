from __future__ import annotations

from typing import Any, Dict, List


def build_spatial_heatmap_payload(
    record_count: int,
    heatmap_points: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        'enabled': record_count >= 3,
        'points': heatmap_points,
        'radius': 20,
        'blur': 26,
    }


def build_spatial_dbscan_payload(dbscan: Dict[str, Any]) -> Dict[str, Any]:
    clusters = dbscan.get('clusters', [])
    return {
        'enabled': bool(clusters),
        'clusters': clusters,
        'eps_km': dbscan.get('eps_km', 0.0),
        'eps_display': f"{dbscan.get('eps_km', 0.0):.2f} км" if dbscan.get('eps_km') else '-',
        'min_samples': dbscan.get('min_samples', 0),
        'cluster_count': len(clusters),
        'noise_count': dbscan.get('noise_count', 0),
        'availability_note': dbscan.get('availability_note', ''),
    }


def build_spatial_layer_defaults(
    *,
    record_count: int,
    mode: str,
    hotspots: List[Dict[str, Any]],
    dbscan: Dict[str, Any],
    risk_zones: List[Dict[str, Any]],
    priority_territories: List[Dict[str, Any]],
) -> Dict[str, bool]:
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
    quality: Dict[str, Any],
    record_count: int,
    heatmap_points: List[Dict[str, Any]],
    hotspots: List[Dict[str, Any]],
    dbscan: Dict[str, Any],
    risk_zones: List[Dict[str, Any]],
    priority_territories: List[Dict[str, Any]],
    logistics: Dict[str, Any],
    summary: Dict[str, Any],
    mode: str,
) -> Dict[str, Any]:
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


def build_empty_spatial_analytics(source_record_count: int) -> Dict[str, Any]:
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
    records: List[Dict[str, Any]],
    source_record_count: int,
) -> Dict[str, Any]:
    unique_coordinates = len({(item['latitude'], item['longitude']) for item in records})
    duplicate_ratio = (1.0 - unique_coordinates / max(len(records), 1)) * 100.0
    mode = 'full' if len(records) >= 18 and unique_coordinates >= 10 else 'limited' if len(records) >= 6 else 'minimal'
    mode_label = 'Полный режим' if mode == 'full' else 'Ограниченный режим' if mode == 'limited' else 'Минимальный режим'
    notes: List[str] = []
    if duplicate_ratio > 45.0:
        notes.append(f'Координаты заметно повторяются: {duplicate_ratio:.1f}% повторов.')
    if len(records) < source_record_count:
        notes.append(f'С координатами осталось {len(records)} из {source_record_count} записей.')
    dated_records = [item for item in records if item.get('date') is not None]
    if len(dated_records) < len(records):
        notes.append(f'Для hotspot-анализа даты доступны у {len(dated_records)} из {len(records)} пожаров.')
    return {
        'unique_coordinates': unique_coordinates,
        'duplicate_ratio': duplicate_ratio,
        'mode': mode,
        'mode_label': mode_label,
        'notes': notes,
        'dated_records': dated_records,
    }


def build_heatmap_points(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    max_weight = max(item['weight'] for item in records) or 1.0
    return [
        {'latitude': item['latitude'], 'longitude': item['longitude'], 'weight': round(max(0.08, min(1.0, item['weight'] / max_weight)), 4)}
        for item in records
    ]


def build_spatial_methods(
    records: List[Dict[str, Any]],
    hotspots: List[Dict[str, Any]],
    dbscan: Dict[str, Any],
    risk_zones: List[Dict[str, Any]],
    priority_territories: List[Dict[str, Any]],
    logistics: Dict[str, Any],
) -> List[str]:
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
    hotspots: List[Dict[str, Any]],
    priority_territories: List[Dict[str, Any]],
    logistics: Dict[str, Any],
    dbscan: Dict[str, Any],
    notes: List[str],
) -> List[str]:
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
    records: List[Dict[str, Any]],
    source_record_count: int,
    methods: List[str],
    risk_zones: List[Dict[str, Any]],
    priority_territories: List[Dict[str, Any]],
    logistics: Dict[str, Any],
) -> List[str]:
    enhanced_methods = methods[1:] if len(methods) > 1 else ['ранжирования территорий и резервного пространственного режима']
    return [
        f"Для таблицы {table_name} пространственный анализ выполнен по {len(records)} пожарам с координатами из {source_record_count} исходных записей. Базовая точечная карта сохранена, но усилена методами {', '.join(enhanced_methods)}.",
        f"На карте выделено {len(risk_zones)} зон повышенного риска и {len(priority_territories)} приоритетных территорий, что переводит карту из режима визуализации в режим поддержки принятия решений для сельских территорий.",
        logistics['summary'] if logistics.get('summary') else logistics.get('coverage_note') or 'Логистический слой пока носит разведочный характер.',
    ]


def build_spatial_quality_payload(
    records: List[Dict[str, Any]],
    source_record_count: int,
    quality_context: Dict[str, Any],
) -> Dict[str, Any]:
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
        'fallback_message': '' if quality_context['mode'] == 'full' else 'Аналитика работает в облегчённом режиме из-за малого числа уникальных координат.',
    }


def build_spatial_summary_payload(
    mode: str,
    methods: List[str],
    insights: List[str],
    thesis_paragraphs: List[str],
) -> Dict[str, Any]:
    return {
        'title': 'Пространственная аналитика пожаров',
        'subtitle': 'Карта дополнена слоями плотности, hotspot/DBSCAN, приоритетами территорий и explainable logistics-метриками доезда.',
        'methods': methods,
        'insights': insights[:5],
        'thesis_paragraphs': thesis_paragraphs,
        'fallback_message': '' if mode == 'full' else 'Используется резервный режим с упором на тепловую карту плотности и приоритетные территории.',
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
