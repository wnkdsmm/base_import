from __future__ import annotations

import math
from typing import Any, Callable, Dict, Iterable, List

import numpy as np

from ...types import DbscanResult, ProcessedRecord, SpatialPoint
from .analytics_geometry import group_records_by_cluster_label, nanmean_record_value, project_records_to_local_xy


def build_dbscan_cluster_result(
    records: List[ProcessedRecord],
    labels: Iterable[Any],
    eps_km: float,
    min_samples: int,
    *,
    risk_level: Callable[[float], tuple[str, str]],
    km_distance: Callable[[SpatialPoint, SpatialPoint], float],
    dominant_label: Callable[[List[ProcessedRecord], str, str], str],
) -> DbscanResult:
    labels_array = np.asarray(list(labels))
    noise_count = int(np.count_nonzero(labels_array == -1))
    cluster_labels = [label for label in labels_array if label >= 0]
    if not cluster_labels:
        return {
            'clusters': [],
            'eps_km': round(eps_km, 2),
            'min_samples': min_samples,
            'noise_count': noise_count,
            'availability_note': 'DBSCAN выполнен, но устойчивых кластеров не найдено.',
        }

    grouped = group_records_by_cluster_label(records, labels_array)
    max_weight = max(sum(item['weight'] for item in items) for items in grouped.values()) or 1.0
    clusters: List[Dict[str, Any]] = []
    for items in grouped.values():
        total_weight = sum(item['weight'] for item in items)
        center_lat = sum(item['latitude'] * item['weight'] for item in items) / max(total_weight, 0.1)
        center_lon = sum(item['longitude'] * item['weight'] for item in items) / max(total_weight, 0.1)
        radius_km = max(
            max(km_distance(item, {'latitude': center_lat, 'longitude': center_lon}) for item in items),
            eps_km,
        )
        risk_score = round((total_weight / max_weight) * 100.0, 1)
        risk_label, risk_tone = risk_level(risk_score)
        avg_response = nanmean_record_value(items, 'response_minutes')
        avg_station_distance = nanmean_record_value(items, 'fire_station_distance')
        clusters.append({
            'label': dominant_label(items, 'territory_label', 'DBSCAN-кластер'),
            'district': dominant_label(items, 'district', 'Район не указан'),
            'latitude': round(center_lat, 6),
            'longitude': round(center_lon, 6),
            'incident_count': len(items),
            'radius_km': round(radius_km, 2),
            'risk_score': risk_score,
            'risk_score_display': f'{risk_score:.1f} / 100',
            'risk_label': risk_label,
            'risk_tone': risk_tone,
            'avg_response_minutes': round(avg_response, 1) if avg_response is not None else None,
            'avg_station_distance': round(avg_station_distance, 1) if avg_station_distance is not None else None,
            'explanation': f"Кластер объединяет {len(items)} пожаров и показывает устойчивое пространственное скопление событий.",
        })

    clusters.sort(key=lambda item: (item['risk_score'], item['incident_count']), reverse=True)
    for rank, item in enumerate(clusters, start=1):
        item['rank'] = rank
        item['cluster_display'] = f'DBSCAN #{rank}'
    return {
        'clusters': clusters[:8],
        'eps_km': round(eps_km, 2),
        'min_samples': min_samples,
        'noise_count': noise_count,
        'availability_note': '',
    }


def estimate_dbscan_eps_km(
    records: List[ProcessedRecord],
    *,
    sklearn_available: bool,
    nearest_neighbors_cls: Any,
) -> float:
    if len(records) < 4 or not sklearn_available or nearest_neighbors_cls is None:
        return 1.0
    xy = project_records_to_local_xy(records)
    neighbours = min(5, len(records))
    model = nearest_neighbors_cls(n_neighbors=neighbours)
    model.fit(xy)
    distances, _ = model.kneighbors(xy)
    reference = distances[:, -1]
    return max(0.9, float(np.percentile(reference, 70)) * 1.15)


def build_dbscan_clusters(
    records: List[ProcessedRecord],
    *,
    sklearn_available: bool,
    dbscan_cls: Any,
    nearest_neighbors_cls: Any,
    risk_level: Callable[[float], tuple[str, str]],
    km_distance: Callable[[SpatialPoint, SpatialPoint], float],
    dominant_label: Callable[[List[ProcessedRecord], str, str], str],
) -> DbscanResult:
    if len(records) < 8 or not sklearn_available or dbscan_cls is None:
        return {'clusters': [], 'eps_km': 0.0, 'min_samples': 0, 'noise_count': 0, 'availability_note': 'DBSCAN отключен: наблюдений пока недостаточно.'}

    xy = project_records_to_local_xy(records)
    eps_km = estimate_dbscan_eps_km(
        records,
        sklearn_available=sklearn_available,
        nearest_neighbors_cls=nearest_neighbors_cls,
    )
    min_samples = max(4, min(8, int(round(math.log(len(records) + 1, 2) + 2))))
    model = dbscan_cls(eps=eps_km, min_samples=min_samples)
    labels = model.fit_predict(xy)
    return build_dbscan_cluster_result(
        records,
        labels,
        eps_km,
        min_samples,
        risk_level=risk_level,
        km_distance=km_distance,
        dominant_label=dominant_label,
    )


__all__ = [
    "build_dbscan_cluster_result",
    "build_dbscan_clusters",
    "estimate_dbscan_eps_km",
]
