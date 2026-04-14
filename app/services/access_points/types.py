from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from typing import Any, Optional, TypedDict

import pandas as pd


class OptionItem(TypedDict, total=False):
    """Generic select-option item for filters."""

    value: str
    label: str


class ResolvedColumns(TypedDict, total=False):
    """Resolved source-column mapping for access-points SQL extraction."""

    district: Optional[str]
    settlement: Optional[str]
    settlement_type: Optional[str]
    territory_label: Optional[str]
    object_category: Optional[str]
    object_name: Optional[str]
    address: Optional[str]
    address_comment: Optional[str]
    latitude: Optional[str]
    longitude: Optional[str]
    report_time: Optional[str]
    arrival_time: Optional[str]
    detection_time: Optional[str]
    distance_to_fire_station: Optional[str]
    water_supply_count: Optional[str]
    water_supply_details: Optional[str]
    consequence: Optional[str]
    deaths: Optional[str]
    injuries: Optional[str]
    casualty_flag: Optional[str]
    destroyed_area: Optional[str]
    destroyed_buildings: Optional[str]
    registered_damage: Optional[str]


class AccessPointMetadata(TypedDict, total=False):
    """Metadata payload describing source table structure and resolved columns."""

    table_name: str
    columns: list[str]
    resolved_columns: ResolvedColumns


class PointRecord(TypedDict, total=False):
    """Normalized raw incident record with geospatial fields from data_impl."""

    district: str
    territory_label: str
    settlement: str
    settlement_type: str
    object_category: str
    object_name: str
    address: str
    address_comment: str
    latitude: float
    longitude: float
    report_time: datetime | None
    arrival_time: datetime | None
    detection_time: datetime | None
    distance_to_fire_station: float | None
    water_supply_count: float | None
    water_supply_details: str
    consequence: str
    deaths: float | None
    injuries: float | None
    casualty_flag: bool
    destroyed_area: float | None
    destroyed_buildings: float | None
    registered_damage: float | None
    event_date: datetime | None


class RawPointRow(TypedDict, total=False):
    """Raw SQL-mapped row before access-point normalization."""

    district: Any
    territory_label: Any
    settlement: Any
    settlement_type: Any
    object_category: Any
    object_name: Any
    address: Any
    address_comment: Any
    latitude: Any
    longitude: Any
    report_time: Any
    arrival_time: Any
    detection_time: Any
    distance_to_fire_station: Any
    water_supply_count: Any
    water_supply_details: Any
    consequence: Any
    deaths: Any
    injuries: Any
    casualty_flag: Any
    destroyed_area: Any
    destroyed_buildings: Any
    registered_damage: Any
    event_date: Any


class AccessPointInput(PointRecord, total=False):
    """Derived incident-level input used by point aggregation and scoring."""

    source_table: str
    date: datetime | None
    year: int | None
    response_minutes: float | None
    fire_station_distance: float | None
    long_arrival: bool
    has_water_supply: bool | None
    severe_consequence: bool
    victims_present: bool
    major_damage: bool
    night_incident: bool
    heating_season: bool


class ConsequenceSummary(TypedDict, total=False):
    """Consequence counters aggregated from incident records."""

    records_with_consequences: int
    deaths: int
    injuries: int


class WaterSupplySummary(TypedDict, total=False):
    """Water-supply availability counters aggregated from incidents."""

    with_water_supply: int
    without_water_supply: int


class ResponseSummary(TypedDict, total=False):
    """Response-time summary with long-arrival count and mean value."""

    response_minutes: list[float]
    long_response_count: int
    average_response_minutes: float | None


class PriorityRow(TypedDict, total=False):
    """Priority row returned by data_impl for access-point ranking."""

    district: str
    label: str
    fire_count: int
    risk_score: float
    long_response_count: int
    average_response_minutes: float | None
    records_with_consequences: int
    deaths: int
    injuries: int
    with_water_supply: int
    without_water_supply: int
    heating_season_fires: int
    top_object_category: str


class AccessPointsSummary(TypedDict, total=False):
    """Compact summary block used by baseline access-points payload."""

    total_points: int
    total_points_display: str


class AccessPointsDataPayload(TypedDict, total=False):
    """Data payload built by data_impl and consumed by higher layers."""

    table_options: list[OptionItem]
    selected_table: str
    selected_table_label: str
    district_options: list[OptionItem]
    year_options: list[OptionItem]
    selected_district: str
    selected_year: str
    summary: AccessPointsSummary
    points: list[PriorityRow]
    notes: list[str]


class PointIdentity(TypedDict, total=False):
    """Resolved entity identity for grouping incidents into point buckets."""

    point_id: str
    label: str
    entity_type: str
    entity_code: str
    granularity_rank: int


class PointBucket(PointIdentity, total=False):
    """Mutable aggregation bucket collecting incident counters per point."""

    incident_count: int
    response_total: float
    response_count: int
    distance_total: float
    distance_count: int
    long_arrival_count: int
    water_yes_count: int
    water_no_count: int
    water_unknown_count: int
    severe_count: int
    victims_count: int
    major_damage_count: int
    night_count: int
    heating_count: int
    rural_count: int
    years: Counter
    districts: Counter
    territories: Counter
    settlements: Counter
    settlement_types: Counter
    object_categories: Counter
    source_tables: Counter
    latitude_values: list[float]
    longitude_values: list[float]


class PointPriors(TypedDict, total=False):
    """Global empirical priors used for smoothing point-level shares."""

    long_arrival: float
    no_water: float
    severe: float
    victims: float
    major_damage: float
    night: float
    heating: float
    rural: float


class PointData(TypedDict, total=False):
    """Processed access-point row produced by point_data aggregation."""

    point_id: str
    label: str
    entity_type: str
    entity_code: str
    granularity_rank: int
    district: str
    territory_label: str
    settlement: str
    settlement_type: str
    rural_flag: bool
    rural_share: float
    incident_count: int
    years_observed: int
    incidents_per_year: float
    average_response_minutes: float | None
    response_coverage_share: float
    long_arrival_share: float
    average_distance_km: float | None
    distance_coverage_share: float
    no_water_share: float
    water_coverage_share: float
    water_unknown_share: float
    severe_share: float
    victim_share: float
    major_damage_share: float
    victims_count: int
    major_damage_count: int
    night_share: float
    heating_share: float
    low_support: bool
    minimum_support: int
    support_weight: float
    response_count: int
    known_water_count: int
    distance_count: int
    source_tables: list[str]
    source_tables_display: str
    object_category: str
    location_hint: str
    latitude: float | None
    longitude: float | None


class PointDataset(TypedDict, total=False):
    """Point-level dataset bundle used by access-points modeling/presentation."""

    records: list[AccessPointInput]
    entity_frame: pd.DataFrame
    feature_frame: pd.DataFrame
    total_incidents: int
    total_entities: int
    notes: list[str]
    minimum_support: int


class AccessPointCard(TypedDict, total=False):
    """Presentation summary card for access-points dashboard."""

    label: str
    value: str
    meta: str
    tone: str


class PresentationSummary(TypedDict, total=False):
    """Presentation summary block rendered above ranked access points."""

    selected_table_label: str
    selected_district_label: str
    selected_year_label: str
    limit_display: str
    total_points_display: str
    total_incidents_display: str
    critical_points_display: str
    high_points_display: str
    medium_points_display: str
    review_points_display: str
    incomplete_points_display: str
    uncertainty_points_display: str
    top_point_label: str
    top_point_score_display: str
    top_point_severity_band: str
    top_point_priority_label: str
    filter_description: str


class AccessPointPresentation(TypedDict, total=False):
    """Final presentation payload for the access-points page."""

    bootstrap_mode: str
    loading: bool
    has_data: bool
    title: str
    model_description: str
    filters: dict[str, Any]
    summary: PresentationSummary
    summary_cards: list[AccessPointCard]
    top_point_label: str
    top_point_explanation: str
    points: list[PointData]
    top_points: list[PointData]
    score_distribution: dict[str, Any]
    reason_breakdown: list[dict[str, Any]]
    incomplete_points: list[PointData]
    typology: list[dict[str, Any]]
    uncertainty_notes: list[str]
    notes: list[str]


class AccessPointFilters(TypedDict, total=False):
    """Filter payload passed to access-points presentation layer."""

    table_name: str
    district: str
    year: str
    limit: str
    available_tables: list[OptionItem]
    available_districts: list[OptionItem]
    available_years: list[OptionItem]
