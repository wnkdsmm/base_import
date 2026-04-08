from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from .analytics_dbscan import build_dbscan_clusters
from .analytics_hotspots import build_hotspots_from_dated_records
from .analytics_logistics import build_logistics_summary_payload
from .analytics_payload import (
    build_empty_spatial_analytics,
    build_heatmap_points,
    build_spatial_analytics_payload,
    build_spatial_insights,
    build_spatial_methods,
    build_spatial_quality_context,
    build_spatial_quality_payload,
    build_spatial_summary_payload,
    build_spatial_thesis_paragraphs,
)
from .analytics_priority import (
    build_fallback_risk_zones,
    build_priority_territories,
    build_spatial_risk_zones,
)

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

    def _build_spatial_analytics(self, table_name: str, records: List[Dict[str, Any]], source_record_count: int) -> Dict[str, Any]:
        if not records:
            return build_empty_spatial_analytics(source_record_count)

        quality_context = build_spatial_quality_context(records, source_record_count)
        notes = quality_context['notes']
        dated_records = quality_context['dated_records']

        hotspots = build_hotspots_from_dated_records(dated_records, notes, self._risk_level)
        dbscan = build_dbscan_clusters(
            records,
            sklearn_available=SKLEARN_AVAILABLE,
            dbscan_cls=DBSCAN,
            nearest_neighbors_cls=NearestNeighbors,
            risk_level=self._risk_level,
            km_distance=self._km_distance,
            dominant_label=self._dominant_label,
        )
        risk_zones = build_spatial_risk_zones(
            dbscan,
            hotspots,
            km_distance=self._km_distance,
            build_circle_polygon=self._build_circle_polygon,
        )

        priority_territories = build_priority_territories(
            records,
            risk_zones,
            risk_level=self._risk_level,
            km_distance=self._km_distance,
        )
        if not risk_zones and priority_territories:
            risk_zones = build_fallback_risk_zones(
                records,
                priority_territories,
                risk_level=self._risk_level,
                km_distance=self._km_distance,
                build_circle_polygon=self._build_circle_polygon,
            )
            if risk_zones:
                notes.append('Основные зоны риска построены по центроидам приоритетных территорий, потому что hotspot/DBSCAN дали слабый сигнал.')
                priority_territories = build_priority_territories(
                    records,
                    risk_zones,
                    risk_level=self._risk_level,
                    km_distance=self._km_distance,
                )

        logistics = build_logistics_summary_payload(records, priority_territories)
        heatmap_points = build_heatmap_points(records)
        methods = build_spatial_methods(records, hotspots, dbscan, risk_zones, priority_territories, logistics)
        insights = build_spatial_insights(hotspots, priority_territories, logistics, dbscan, notes)
        thesis_paragraphs = build_spatial_thesis_paragraphs(
            table_name,
            records,
            source_record_count,
            methods,
            risk_zones,
            priority_territories,
            logistics,
        )
        mode = quality_context['mode']

        return build_spatial_analytics_payload(
            quality=build_spatial_quality_payload(records, source_record_count, quality_context),
            record_count=len(records),
            heatmap_points=heatmap_points,
            hotspots=hotspots,
            dbscan=dbscan,
            risk_zones=risk_zones,
            priority_territories=priority_territories,
            logistics=logistics,
            summary=build_spatial_summary_payload(mode, methods, insights, thesis_paragraphs),
            mode=mode,
        )
    # =====================================================
    # PREPARE TABLE
    # =====================================================

__all__ = ["MapCreatorAnalyticsMixin", "SKLEARN_AVAILABLE"]
