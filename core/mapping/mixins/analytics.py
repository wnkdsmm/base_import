from __future__ import annotations

from typing import Any

import pandas as pd

from ...types import ColumnMapping, ProcessedRecord, SpatialAnalyticsPayload
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
    def _collect_spatial_records(self, df: pd.DataFrame, lat_col: str, lon_col: str, columns: ColumnMapping) -> list[ProcessedRecord]:
        latitudes = pd.to_numeric(df[lat_col], errors='coerce')
        longitudes = pd.to_numeric(df[lon_col], errors='coerce')
        valid_mask = latitudes.notna() & longitudes.notna()
        if not valid_mask.any():
            return []

        frame = df.loc[valid_mask].copy()
        latitudes = latitudes.loc[valid_mask].round(6)
        longitudes = longitudes.loc[valid_mask].round(6)

        def _series_for(field_name: str, default: Any = "") -> pd.Series:
            column_name = columns.get(field_name)
            if column_name and column_name in frame.columns:
                return frame[column_name]
            return pd.Series([default] * len(frame), index=frame.index)

        date_values = _series_for('date', '').map(self._parse_date)
        district_values = _series_for('district', '').map(self._clean_text).fillna("")
        territory_values = _series_for('territory_label', '').map(self._clean_text).fillna("")
        territory_values = territory_values.where(territory_values.ne(""), district_values)
        territory_values = territory_values.where(territory_values.ne(""), 'Территория не указана')
        settlement_values = _series_for('settlement_type', '').map(self._clean_text).fillna("")
        address_values = _series_for('address', '').map(self._clean_text).fillna("")
        cause_values = _series_for('fire_cause_general', '').map(self._clean_text).fillna("")
        object_category_values = _series_for('object_category', '').map(self._clean_text).fillna("")

        deaths_values = pd.to_numeric(_series_for('deaths', 0), errors='coerce').fillna(0.0)
        injured_values = pd.to_numeric(_series_for('injured', 0), errors='coerce').fillna(0.0)
        evacuated_values = pd.to_numeric(_series_for('evacuated', 0), errors='coerce').fillna(0.0)
        children_values = (
            pd.to_numeric(_series_for('children_saved', 0), errors='coerce').fillna(0.0)
            + pd.to_numeric(_series_for('children_evacuated', 0), errors='coerce').fillna(0.0)
        )

        response_minutes = [
            self._calculate_response_minutes(report_time, arrival_time, event_date)
            for report_time, arrival_time, event_date in zip(
                _series_for('report_time', '').tolist(),
                _series_for('arrival_time', '').tolist(),
                date_values.tolist(),
            )
        ]

        station_distance_raw = pd.to_numeric(_series_for('fire_station_distance', None), errors='coerce')
        station_distance_values = station_distance_raw.astype(object).where(station_distance_raw.notna(), None)
        severity_values = 1.0 + deaths_values * 2.4 + injured_values * 1.6 + evacuated_values * 0.08 + children_values * 0.2
        rural_flags = [
            self._is_rural_label(territory_label, settlement_type, address)
            for territory_label, settlement_type, address in zip(
                territory_values.tolist(),
                settlement_values.tolist(),
                address_values.tolist(),
            )
        ]

        records_frame = pd.DataFrame(
            {
                'latitude': latitudes,
                'longitude': longitudes,
                'date': date_values,
                'district': district_values,
                'territory_label': territory_values,
                'settlement_type': settlement_values,
                'address': address_values,
                'cause': cause_values,
                'object_category': object_category_values,
                'response_minutes': response_minutes,
                'fire_station_distance': station_distance_values,
                'severity_raw': severity_values,
                'has_victims': (deaths_values + injured_values) > 0,
                'weight': severity_values,
                'rural_flag': rural_flags,
            },
            index=frame.index,
        )
        return records_frame.to_dict(orient='records')

    def _build_spatial_analytics(self, table_name: str, records: list[ProcessedRecord], source_record_count: int) -> SpatialAnalyticsPayload:
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
