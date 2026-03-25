from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from app.services.explainable_logistics import build_explainable_logistics_profile
from app.services.forecasting.geo import _build_geo_prediction

try:
    from sklearn.cluster import DBSCAN
    from sklearn.neighbors import NearestNeighbors
    SKLEARN_AVAILABLE = True
except Exception:  # pragma: no cover - graceful fallback when sklearn is unavailable
    DBSCAN = None
    NearestNeighbors = None
    SKLEARN_AVAILABLE = False

class MapCreatorAnalyticsMixin:
    def _collect_spatial_records(self, df: pd.DataFrame, lat_col: str, lon_col: str, columns: Dict[str, Optional[str]]) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            latitude = self._to_float(row.get(lat_col))
            longitude = self._to_float(row.get(lon_col))
            if latitude is None or longitude is None:
                continue

            event_date = self._parse_date(self.cleaner.safe_get(row, columns.get('date'), ''))
            district = self._clean_text(self.cleaner.safe_get(row, columns.get('district'), ''))
            territory_label = self._clean_text(self.cleaner.safe_get(row, columns.get('territory_label'), district)) or district or 'Территория не указана'
            settlement_type = self._clean_text(self.cleaner.safe_get(row, columns.get('settlement_type'), ''))
            address = self._clean_text(self.cleaner.safe_get(row, columns.get('address'), ''))
            deaths = self._to_float(self.cleaner.safe_get(row, columns.get('deaths'), 0)) or 0.0
            injured = self._to_float(self.cleaner.safe_get(row, columns.get('injured'), 0)) or 0.0
            evacuated = self._to_float(self.cleaner.safe_get(row, columns.get('evacuated'), 0)) or 0.0
            children = (self._to_float(self.cleaner.safe_get(row, columns.get('children_saved'), 0)) or 0.0) + (self._to_float(self.cleaner.safe_get(row, columns.get('children_evacuated'), 0)) or 0.0)
            response_minutes = self._calculate_response_minutes(
                self.cleaner.safe_get(row, columns.get('report_time'), ''),
                self.cleaner.safe_get(row, columns.get('arrival_time'), ''),
                event_date,
            )
            station_distance = self._to_float(self.cleaner.safe_get(row, columns.get('fire_station_distance'), ''))
            severity_raw = 1.0 + deaths * 2.4 + injured * 1.6 + evacuated * 0.08 + children * 0.2
            records.append({
                'latitude': round(latitude, 6),
                'longitude': round(longitude, 6),
                'date': event_date,
                'district': district,
                'territory_label': territory_label,
                'settlement_type': settlement_type,
                'address': address,
                'cause': self._clean_text(self.cleaner.safe_get(row, columns.get('fire_cause_general'), '')),
                'object_category': self._clean_text(self.cleaner.safe_get(row, columns.get('object_category'), '')),
                'response_minutes': response_minutes,
                'fire_station_distance': station_distance,
                'severity_raw': severity_raw,
                'has_victims': (deaths + injured) > 0,
                'weight': severity_raw,
                'rural_flag': self._is_rural_label(territory_label, settlement_type, address),
            })
        return records

    def _estimate_dbscan_eps_km(self, records: List[Dict[str, Any]]) -> float:
        if len(records) < 4 or not SKLEARN_AVAILABLE or NearestNeighbors is None:
            return 1.0
        coords = np.array([[item['latitude'], item['longitude']] for item in records], dtype=float)
        lat0 = float(np.mean(coords[:, 0]))
        lon0 = float(np.mean(coords[:, 1]))
        cos_lat = max(math.cos(math.radians(lat0)), 0.1)
        xy = np.column_stack([
            (coords[:, 1] - lon0) * 111.320 * cos_lat,
            (coords[:, 0] - lat0) * 110.574,
        ])
        neighbours = min(5, len(records))
        model = NearestNeighbors(n_neighbors=neighbours)
        model.fit(xy)
        distances, _ = model.kneighbors(xy)
        reference = distances[:, -1]
        return max(0.9, float(np.percentile(reference, 70)) * 1.15)

    def _build_dbscan_clusters(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        if len(records) < 8 or not SKLEARN_AVAILABLE or DBSCAN is None:
            return {'clusters': [], 'eps_km': 0.0, 'min_samples': 0, 'noise_count': 0, 'availability_note': 'DBSCAN отключен: наблюдений пока недостаточно.'}

        coords = np.array([[item['latitude'], item['longitude']] for item in records], dtype=float)
        lat0 = float(np.mean(coords[:, 0]))
        lon0 = float(np.mean(coords[:, 1]))
        cos_lat = max(math.cos(math.radians(lat0)), 0.1)
        xy = np.column_stack([
            (coords[:, 1] - lon0) * 111.320 * cos_lat,
            (coords[:, 0] - lat0) * 110.574,
        ])
        eps_km = self._estimate_dbscan_eps_km(records)
        min_samples = max(4, min(8, int(round(math.log(len(records) + 1, 2) + 2))))
        model = DBSCAN(eps=eps_km, min_samples=min_samples)
        labels = model.fit_predict(xy)
        cluster_labels = [label for label in labels if label >= 0]
        if not cluster_labels:
            return {'clusters': [], 'eps_km': round(eps_km, 2), 'min_samples': min_samples, 'noise_count': int(np.count_nonzero(labels == -1)), 'availability_note': 'DBSCAN выполнен, но устойчивых кластеров не найдено.'}

        grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for idx, label in enumerate(labels):
            if label >= 0:
                grouped[int(label)].append(records[idx])

        max_weight = max(sum(item['weight'] for item in items) for items in grouped.values()) or 1.0
        clusters: List[Dict[str, Any]] = []
        for items in grouped.values():
            total_weight = sum(item['weight'] for item in items)
            center_lat = sum(item['latitude'] * item['weight'] for item in items) / max(total_weight, 0.1)
            center_lon = sum(item['longitude'] * item['weight'] for item in items) / max(total_weight, 0.1)
            radius_km = max(
                max(self._km_distance(item, {'latitude': center_lat, 'longitude': center_lon}) for item in items),
                eps_km,
            )
            risk_score = round((total_weight / max_weight) * 100.0, 1)
            risk_label, risk_tone = self._risk_level(risk_score)
            clusters.append({
                'label': self._dominant_label(items, 'territory_label', 'DBSCAN-кластер'),
                'district': self._dominant_label(items, 'district', 'Район не указан'),
                'latitude': round(center_lat, 6),
                'longitude': round(center_lon, 6),
                'incident_count': len(items),
                'radius_km': round(radius_km, 2),
                'risk_score': risk_score,
                'risk_score_display': f'{risk_score:.1f} / 100',
                'risk_label': risk_label,
                'risk_tone': risk_tone,
                'avg_response_minutes': round(float(np.nanmean([item['response_minutes'] for item in items if item['response_minutes'] is not None])), 1) if any(item['response_minutes'] is not None for item in items) else None,
                'avg_station_distance': round(float(np.nanmean([item['fire_station_distance'] for item in items if item['fire_station_distance'] is not None])), 1) if any(item['fire_station_distance'] is not None for item in items) else None,
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
            'noise_count': int(np.count_nonzero(labels == -1)),
            'availability_note': '',
        }

    def _build_priority_territories(self, records: List[Dict[str, Any]], risk_zones: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in records:
            grouped[item['territory_label']].append(item)
        if not grouped:
            return []

        max_incidents = max(len(items) for items in grouped.values()) or 1
        max_weight = max(sum(point['weight'] for point in items) for items in grouped.values()) or 1.0
        territories: List[Dict[str, Any]] = []
        for label, items in grouped.items():
            incident_count = len(items)
            severe_count = sum(1 for item in items if item['has_victims'])
            avg_distance = float(np.mean([item['fire_station_distance'] for item in items if item['fire_station_distance'] is not None])) if any(item['fire_station_distance'] is not None for item in items) else None
            avg_response = float(np.mean([item['response_minutes'] for item in items if item['response_minutes'] is not None])) if any(item['response_minutes'] is not None for item in items) else None
            response_observations = sum(1 for item in items if item['response_minutes'] is not None)
            distance_observations = sum(1 for item in items if item['fire_station_distance'] is not None)
            if response_observations:
                long_arrival_rate = sum(1 for item in items if item['response_minutes'] is not None and float(item['response_minutes']) >= 20.0) / response_observations
            elif avg_response is not None:
                long_arrival_rate = min(1.0, max(0.0, (avg_response - 12.0) / 18.0))
            else:
                long_arrival_rate = 0.0
            zone_hits = sum(1 for item in items if any(self._km_distance(item, zone) <= zone['radius_km'] for zone in risk_zones[:4]))
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
            risk_label, risk_tone = self._risk_level(risk_score)
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

    def _build_fallback_risk_zones(self, records: List[Dict[str, Any]], priority_territories: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in records:
            grouped[item['territory_label']].append(item)

        fallback_zones: List[Dict[str, Any]] = []
        for rank, territory in enumerate(priority_territories[:3], start=1):
            items = grouped.get(territory['label'], [])
            if not items:
                continue

            center = {'latitude': territory['latitude'], 'longitude': territory['longitude']}
            distances = [self._km_distance(item, center) for item in items]
            radius_km = max(1.2, float(np.percentile(distances, 75)) if distances else 0.0)
            risk_score = max(float(territory['risk_score']), 35.0)
            risk_label, risk_tone = self._risk_level(risk_score)
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
                'polygon': self._build_circle_polygon(territory['longitude'], territory['latitude'], radius_km),
            })
        return fallback_zones

    def _build_logistics_summary(self, records: List[Dict[str, Any]], priority_territories: List[Dict[str, Any]], risk_zones: List[Dict[str, Any]]) -> Dict[str, Any]:
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
                    f"({lead_territory.get('logistics_priority_display', 'н/д')}, {lead_territory.get('service_zone_label', 'зона не определена')})."
                )
        elif logistics_profile:
            coverage_note = (
                f"Логистические колонки найдены, но наблюдений пока мало для устойчивой оценки; текущий ориентир — travel-time "
                f"{logistics_profile['travel_time_display']}, покрытие ПЧ {logistics_profile['service_coverage_display']} и зона {logistics_profile['service_zone_label']}."
            )
        else:
            coverage_note = 'Колонки логистики не найдены или почти пустые, поэтому прикладной анализ доезда и прикрытия пока недоступен.'

        return {
            'basis_ready': basis_ready,
            'average_station_distance': round(avg_distance, 1) if avg_distance is not None else None,
            'average_station_distance_display': f'{avg_distance:.1f} км' if avg_distance is not None else 'н/д',
            'average_response_minutes': round(avg_response, 1) if avg_response is not None else None,
            'average_response_display': f'{avg_response:.1f} мин' if avg_response is not None else 'н/д',
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

    def _build_spatial_analytics(self, table_name: str, records: List[Dict[str, Any]], source_record_count: int) -> Dict[str, Any]:
        if not records:
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

        geo_prediction: Dict[str, Any] = {'hotspots': []}
        if len(dated_records) >= 3:
            geo_prediction = _build_geo_prediction(dated_records, planning_horizon_days=30)
        elif dated_records:
            notes.append('Для hotspot-анализа дат пока мало, поэтому акцент смещён на тепловую карту и приоритетные территории.')
        else:
            notes.append('Даты пожаров отсутствуют, поэтому hotspot-анализ отключён и заменён резервным пространственным режимом.')
        hotspots = []
        for rank, item in enumerate((geo_prediction.get('hotspots') or [])[:8], start=1):
            risk_score = float(item.get('risk_score') or 0.0)
            risk_label, risk_tone = self._risk_level(risk_score)
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

        dbscan = self._build_dbscan_clusters(records)
        risk_zone_candidates: List[Dict[str, Any]] = []
        for cluster in dbscan.get('clusters', []):
            risk_zone_candidates.append({
                'label': cluster['label'], 'latitude': cluster['latitude'], 'longitude': cluster['longitude'], 'radius_km': max(cluster['radius_km'], 1.0), 'risk_score': cluster['risk_score'], 'risk_score_display': cluster['risk_score_display'], 'risk_label': cluster['risk_label'], 'risk_tone': cluster['risk_tone'], 'support_count': cluster['incident_count'], 'source': 'DBSCAN', 'explanation': cluster['explanation'],
            })
        for hotspot in hotspots:
            if any(self._km_distance(hotspot, existing) < max(hotspot['radius_km'], existing['radius_km']) * 0.75 for existing in risk_zone_candidates):
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
                'polygon': self._build_circle_polygon(item['longitude'], item['latitude'], item['radius_km']),
            })

        priority_territories = self._build_priority_territories(records, risk_zones)
        if not risk_zones and priority_territories:
            risk_zones = self._build_fallback_risk_zones(records, priority_territories)
            if risk_zones:
                notes.append('Основные зоны риска построены по центроидам приоритетных территорий, потому что hotspot/DBSCAN дали слабый сигнал.')
                priority_territories = self._build_priority_territories(records, risk_zones)
        logistics = self._build_logistics_summary(records, priority_territories, risk_zones)
        max_weight = max(item['weight'] for item in records) or 1.0
        heatmap_points = [{'latitude': item['latitude'], 'longitude': item['longitude'], 'weight': round(max(0.08, min(1.0, item['weight'] / max_weight)), 4)} for item in records]

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
        enhanced_methods = methods[1:] if len(methods) > 1 else ['ранжирования территорий и резервного пространственного режима']

        thesis_paragraphs = [
            f"Для таблицы {table_name} пространственный анализ выполнен по {len(records)} пожарам с координатами из {source_record_count} исходных записей. Базовая точечная карта сохранена, но усилена методами {', '.join(enhanced_methods)}.",
            f"На карте выделено {len(risk_zones)} зон повышенного риска и {len(priority_territories)} приоритетных территорий, что переводит карту из режима визуализации в режим поддержки принятия решений для сельских территорий.",
            logistics['summary'] if logistics.get('summary') else logistics.get('coverage_note') or 'Логистический слой пока носит разведочный характер.',
        ]

        return {
            'quality': {
                'mode': mode,
                'mode_label': mode_label,
                'source_record_count': source_record_count,
                'valid_coordinate_count': len(records),
                'coordinate_coverage_display': f"{len(records)} из {source_record_count} ({len(records) / max(source_record_count, 1) * 100.0:.1f}%)",
                'dated_record_count': len(dated_records),
                'date_coverage_display': f"{len(dated_records)} из {len(records)}",
                'unique_coordinate_count': unique_coordinates,
                'duplicate_ratio_percent': round(duplicate_ratio, 1),
                'notes': notes,
                'fallback_message': '' if mode == 'full' else 'Аналитика работает в облегчённом режиме из-за малого числа уникальных координат.',
            },
            'heatmap': {'enabled': len(records) >= 3, 'points': heatmap_points, 'radius': 20, 'blur': 26},
            'hotspots': hotspots,
            'dbscan': {
                'enabled': bool(dbscan.get('clusters')),
                'clusters': dbscan.get('clusters', []),
                'eps_km': dbscan.get('eps_km', 0.0),
                'eps_display': f"{dbscan.get('eps_km', 0.0):.2f} км" if dbscan.get('eps_km') else '-',
                'min_samples': dbscan.get('min_samples', 0),
                'cluster_count': len(dbscan.get('clusters', [])),
                'noise_count': dbscan.get('noise_count', 0),
                'availability_note': dbscan.get('availability_note', ''),
            },
            'risk_zones': risk_zones,
            'priority_territories': priority_territories,
            'logistics': logistics,
            'summary': {
                'title': 'Пространственная аналитика пожаров',
                'subtitle': 'Карта дополнена слоями плотности, hotspot/DBSCAN, приоритетами территорий и explainable logistics-метриками доезда.',
                'methods': methods,
                'insights': insights[:5],
                'thesis_paragraphs': thesis_paragraphs,
                'fallback_message': '' if mode == 'full' else 'Используется резервный режим с упором на тепловую карту плотности и приоритетные территории.',
            },
            'layer_defaults': {'incidents': True, 'heatmap': len(records) >= 3 and mode != 'minimal', 'hotspots': bool(hotspots), 'clusters': bool(dbscan.get('clusters')), 'risk_zones': bool(risk_zones), 'priorities': bool(priority_territories)},
        }
    # =====================================================
    # PREPARE TABLE
    # =====================================================

__all__ = ["MapCreatorAnalyticsMixin", "SKLEARN_AVAILABLE"]
