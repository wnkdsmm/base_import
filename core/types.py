from __future__ import annotations

from typing import Protocol, TypedDict

import pandas as pd


class RawDataRow(TypedDict, total=False):
    column_values: dict[str, object]


class ProcessedRecord(TypedDict, total=False):
    latitude: float
    longitude: float
    date: object
    district: str
    territory_label: str
    settlement_type: str
    address: str
    cause: str
    object_category: str
    response_minutes: float | None
    fire_station_distance: float | None
    severity_raw: float
    has_victims: bool
    weight: float
    rural_flag: bool


class ColumnMapping(TypedDict, total=False):
    date: str | None
    district: str | None
    territory_label: str | None
    settlement_type: str | None
    address: str | None
    fire_cause_general: str | None
    object_category: str | None
    deaths: str | None
    injured: str | None
    evacuated: str | None
    children_saved: str | None
    children_evacuated: str | None
    report_time: str | None
    arrival_time: str | None
    fire_station_distance: str | None


class PipelineContext(TypedDict, total=False):
    table_name: str
    selected_columns: list[str]
    matched_columns: dict[str, str]
    limit: int


class FireMapSource(PipelineContext, total=False):
    dataframe: pd.DataFrame


class ColumnTermPayload(TypedDict, total=False):
    original_name: str
    normalized_name: str
    words: set[str]
    lemmas: set[str]


class ColumnMatchMetadata(TypedDict, total=False):
    scope: str
    feature_id: str
    feature_label: str
    rule_id: str
    matched_value: str
    reason: str
    mandatory: bool


class MandatoryFeatureSpec(TypedDict, total=False):
    id: str
    label: str
    description: str
    synonyms: list[str]
    token_sets: list[list[str]]
    exclude_tokens: list[str]
    include_all: list[list[str]]
    include_any: list[list[str]]
    exclude: list[str]
    prepared_synonyms: list[dict[str, object]]
    prepared_token_sets: list[list[str]]
    prepared_exclude_tokens: list[str]


class CategoryRule(TypedDict, total=False):
    id: str
    label: str
    description: str
    parts: list[str]
    keywords: list[str]


class GroupCatalogEntry(TypedDict, total=False):
    id: str
    label: str
    description: str
    count: int
    columns: list[str]


class SpatialPoint(TypedDict, total=False):
    latitude: float
    longitude: float


class HeatmapPoint(SpatialPoint, total=False):
    weight: float


class HotspotPayload(SpatialPoint, total=False):
    rank: int
    label: str
    support_count: int
    radius_km: float
    risk_score: float
    risk_score_display: str
    risk_label: str
    risk_tone: str
    explanation: str


class DbscanCluster(SpatialPoint, total=False):
    label: str
    district: str
    incident_count: int
    radius_km: float
    risk_score: float
    risk_score_display: str
    risk_label: str
    risk_tone: str
    avg_response_minutes: float | None
    avg_station_distance: float | None
    explanation: str
    rank: int
    cluster_display: str


class DbscanResult(TypedDict, total=False):
    clusters: list[DbscanCluster]
    eps_km: float
    min_samples: int
    noise_count: int
    availability_note: str


class RiskZone(SpatialPoint, total=False):
    label: str
    radius_km: float
    risk_score: float
    risk_score_display: str
    risk_label: str
    risk_tone: str
    support_count: int
    source: str
    explanation: str
    rank: int
    priority_label: str
    polygon: list[list[float]]


class PriorityTerritory(SpatialPoint, total=False):
    label: str
    incident_count: int
    incident_count_display: str
    severe_count: int
    risk_score: float
    risk_score_display: str
    risk_label: str
    risk_tone: str
    avg_station_distance: float | None
    avg_station_distance_display: str
    avg_response_minutes: float | None
    avg_response_display: str
    travel_time_minutes: float | None
    travel_time_display: str
    travel_time_source: str
    fire_station_coverage_display: str
    fire_station_coverage_label: str
    service_zone_label: str
    service_zone_tone: str
    service_zone_reason: str
    logistics_priority_score: float
    logistics_priority_display: str
    logistics_priority_label: str
    long_arrival_share: float
    long_arrival_share_display: str
    zone_hits: int
    explanation: str
    rank: int
    priority_label: str


class LogisticsSummaryPayload(TypedDict, total=False):
    basis_ready: bool
    average_station_distance: float | None
    average_station_distance_display: str
    average_response_minutes: float | None
    average_response_display: str
    average_travel_time_minutes: float | None
    average_travel_time_display: str
    long_arrival_share: float | None
    long_arrival_share_display: str
    fire_station_coverage_display: str
    fire_station_coverage_label: str
    service_zone_label: str
    service_zone_reason: str
    logistics_priority_score: float
    logistics_priority_display: str
    logistics_priority_label: str
    summary: str
    coverage_note: str
    top_delayed_territories: list[dict[str, object]]


class SpatialQualityContext(TypedDict, total=False):
    unique_coordinates: int
    duplicate_ratio: float
    mode: str
    mode_label: str
    notes: list[str]
    dated_records: list[ProcessedRecord]


class SpatialQualityPayload(TypedDict, total=False):
    mode: str
    mode_label: str
    source_record_count: int
    valid_coordinate_count: int
    coordinate_coverage_display: str
    dated_record_count: int
    date_coverage_display: str
    unique_coordinate_count: int
    duplicate_ratio_percent: float
    notes: list[str]
    fallback_message: str


class SpatialSummaryPayload(TypedDict, total=False):
    title: str
    subtitle: str
    methods: list[str]
    insights: list[str]
    thesis_paragraphs: list[str]
    fallback_message: str


class SpatialHeatmapPayload(TypedDict, total=False):
    enabled: bool
    points: list[HeatmapPoint]
    radius: int
    blur: int


class SpatialDbscanPayload(DbscanResult, total=False):
    enabled: bool
    cluster_count: int
    eps_display: str


class SpatialLayerDefaults(TypedDict, total=False):
    incidents: bool
    heatmap: bool
    hotspots: bool
    clusters: bool
    risk_zones: bool
    priorities: bool


class SpatialAnalyticsPayload(TypedDict, total=False):
    quality: SpatialQualityPayload
    heatmap: SpatialHeatmapPayload
    hotspots: list[HotspotPayload]
    dbscan: SpatialDbscanPayload
    risk_zones: list[RiskZone]
    priority_territories: list[PriorityTerritory]
    logistics: LogisticsSummaryPayload
    summary: SpatialSummaryPayload
    layer_defaults: SpatialLayerDefaults


class GeoPredictionPayload(TypedDict, total=False):
    hotspots: list[dict[str, object]]


class PopupRow(TypedDict, total=False):
    title: str
    label: str
    value: str


class GeoJsonFeatureCollection(TypedDict, total=False):
    type: str
    features: list[dict[str, object]]


class AnalyticsLayersPayload(TypedDict, total=False):
    heatmap: GeoJsonFeatureCollection
    hotspots: GeoJsonFeatureCollection
    clusters: GeoJsonFeatureCollection
    risk_zones: GeoJsonFeatureCollection
    priorities: GeoJsonFeatureCollection


class MapTablePayload(TypedDict, total=False):
    name: str
    feature_count: int
    counts: dict[str, int]
    center: tuple[float, float]
    initial_zoom: int
    geojson: dict[str, object]
    spatial_analytics: SpatialAnalyticsPayload


class AnalysisTableExport(TypedDict, total=False):
    table_name: str
    feature_count: int
    spatial_analytics: SpatialAnalyticsPayload


class AnalysisExportPayload(TypedDict, total=False):
    tables: list[AnalysisTableExport]


class ProtectedColumnInfo(TypedDict, total=False):
    column: str
    protected_feature_id: str
    protected_feature_label: str
    mandatory_feature_detected: bool
    protection_scope: str
    protection_rule: str
    protection_match: str
    protection_reason: str
    drop_reasons: list[object]


class KeepImportantColumnsResult(TypedDict, total=False):
    updated_csv: str
    updated_xlsx: str
    protected_report_csv: str
    protected_report_xlsx: str
    profile_df: pd.DataFrame
    protected_df: pd.DataFrame
    protected_columns: list[ProtectedColumnInfo]
    protected_count: int
    mandatory_feature_catalog: list[dict[str, object]]


class CategoryStyleLike(Protocol):
    icon: str
    label: str
