from __future__ import annotations

from typing import Any, NamedTuple, Sequence

import pandas as pd

from .constants import DEFAULT_ACCESS_POINT_FEATURES, LONG_RESPONSE_THRESHOLD_MINUTES
from .numeric import (
    _clip_share_series,
    _finite_numeric_columns,
    _finite_series_max,
    _nullable_series_values,
)

DISTANCE_CODE = "DISTANCE_TO_STATION"
RESPONSE_CODE = "RESPONSE_TIME"
LONG_ARRIVAL_CODE = "LONG_ARRIVAL_SHARE"
WATER_CODE = "NO_WATER"
SEVERITY_CODE = "SEVERE_CONSEQUENCES"
RECURRENCE_CODE = "REPEAT_FIRES"
NIGHT_CODE = "NIGHT_PROFILE"
HEATING_CODE = "HEATING_SEASON"
UNCERTAINTY_CODE = "DATA_UNCERTAINTY"

FACTOR_WEIGHTS = {
    DISTANCE_CODE: 16.0,
    RESPONSE_CODE: 14.0,
    LONG_ARRIVAL_CODE: 10.0,
    WATER_CODE: 12.0,
    SEVERITY_CODE: 18.0,
    RECURRENCE_CODE: 14.0,
    NIGHT_CODE: 6.0,
    HEATING_CODE: 4.0,
}

ACCESS_POINT_NUMERIC_COLUMNS = (
    "incident_count",
    "years_observed",
    "incidents_per_year",
    "average_distance_km",
    "average_response_minutes",
    "long_arrival_share",
    "no_water_share",
    "water_coverage_share",
    "water_unknown_share",
    "severe_share",
    "victim_share",
    "victims_count",
    "major_damage_share",
    "major_damage_count",
    "night_share",
    "heating_share",
    "rural_share",
    "response_coverage_share",
    "distance_coverage_share",
    "support_weight",
    "latitude",
    "longitude",
)

UNCERTAINTY_PENALTY_MAX = 6.0
CRITICAL_THRESHOLD = 75.0
HIGH_THRESHOLD = 55.0
MEDIUM_THRESHOLD = 30.0
WATCH_RISK_THRESHOLD = MEDIUM_THRESHOLD


class _AccessPointBaseSeries(NamedTuple):
    incident_count: pd.Series
    years_observed: pd.Series
    incident_count_float: pd.Series
    incidents_per_year: pd.Series
    average_distance: pd.Series
    average_response: pd.Series
    long_arrival_share: pd.Series
    no_water_share: pd.Series
    water_coverage_share: pd.Series
    water_unknown_share: pd.Series
    severe_share: pd.Series
    victim_share: pd.Series
    major_damage_share: pd.Series
    night_share: pd.Series
    heating_share: pd.Series
    rural_share: pd.Series
    response_coverage_share: pd.Series
    distance_coverage_share: pd.Series
    arrival_missing_share: pd.Series
    distance_missing_share: pd.Series
    completeness_share: pd.Series
    support_weight: pd.Series


class _AccessPointFactorSeries(NamedTuple):
    distance_norm: pd.Series
    response_norm: pd.Series
    severity_factor: pd.Series
    recurrence_factor: pd.Series
    uncertainty_factor: pd.Series


def _normalize_selected_access_features(selected_features: Sequence[str] | None) -> list[str]:
    allowed = set(DEFAULT_ACCESS_POINT_FEATURES)
    normalized = [str(item).strip() for item in (selected_features or []) if str(item).strip() in allowed]
    return normalized or list(DEFAULT_ACCESS_POINT_FEATURES)


def _zero_score_series(index: pd.Index) -> pd.Series:
    return pd.Series(0.0, index=index, dtype=float)


def _weighted_score_series(
    index: pd.Index,
    active_reason_codes: set[str],
    components: Sequence[tuple[float, str, pd.Series]],
) -> pd.Series:
    weight_sum = 0.0
    factor_sum = _zero_score_series(index)
    for component_weight, code, factor_value in components:
        if code not in active_reason_codes:
            continue
        weight_sum += float(component_weight)
        factor_sum = factor_sum.add(factor_value * float(component_weight), fill_value=0.0)
    if weight_sum <= 0.0:
        return _zero_score_series(index)
    return (factor_sum / weight_sum).clip(lower=0.0, upper=1.0) * 100.0


def _resolve_access_point_weight_context(
    selected_features: Sequence[str] | None,
) -> tuple[list[str], set[str], dict[str, float]]:
    normalized_selected_features = _normalize_selected_access_features(selected_features)
    active_reason_codes = set(normalized_selected_features)
    selected_weight_sum = sum(float(FACTOR_WEIGHTS[code]) for code in normalized_selected_features if code in FACTOR_WEIGHTS)
    normalized_factor_weights = {
        code: (94.0 * float(weight) / selected_weight_sum) if code in active_reason_codes and selected_weight_sum > 0 else 0.0
        for code, weight in FACTOR_WEIGHTS.items()
    }
    return normalized_selected_features, active_reason_codes, normalized_factor_weights


def _merge_access_point_feature_frame(
    entity_frame: pd.DataFrame,
    feature_frame: pd.DataFrame | None,
) -> pd.DataFrame:
    working_frame = entity_frame.reset_index(drop=True)
    if feature_frame is None or feature_frame.empty:
        return working_frame
    aligned_features = feature_frame.reset_index(drop=True)
    extra_columns = [column for column in aligned_features.columns if column not in working_frame.columns]
    if not extra_columns:
        return working_frame
    working_frame = working_frame.copy()
    working_frame[extra_columns] = aligned_features[extra_columns]
    return working_frame


def _build_incident_frequency_series(
    numeric_inputs: pd.DataFrame,
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]:
    incident_count_series = numeric_inputs["incident_count"].fillna(0.0).clip(lower=0.0).astype(int)
    years_observed_series = numeric_inputs["years_observed"].fillna(1.0).clip(lower=1.0).astype(int)
    incident_count_float_series = incident_count_series.astype(float)
    years_observed_float_series = years_observed_series.astype(float)
    incidents_per_year_fallback = incident_count_float_series / years_observed_float_series
    incidents_per_year_source = numeric_inputs["incidents_per_year"]
    incidents_per_year_series = incidents_per_year_source.where(
        incidents_per_year_source.notna(),
        incidents_per_year_fallback,
    )
    incident_denominator_series = incident_count_float_series.clip(lower=1.0)
    return (
        incident_count_series,
        years_observed_series,
        incident_count_float_series,
        incidents_per_year_series,
        incident_denominator_series,
    )


def _build_water_share_series(
    numeric_inputs: pd.DataFrame,
    water_coverage_share_series: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    no_water_share_series = _clip_share_series(numeric_inputs["no_water_share"])
    water_unknown_source_series = numeric_inputs["water_unknown_share"]
    water_unknown_share_series = water_unknown_source_series.where(
        water_unknown_source_series.notna(),
        (1.0 - water_coverage_share_series).clip(lower=0.0),
    ).fillna(0.0).clip(lower=0.0, upper=1.0)
    return no_water_share_series, water_unknown_share_series


def _build_outcome_share_series(
    numeric_inputs: pd.DataFrame,
    incident_denominator_series: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    victim_source_series = numeric_inputs["victim_share"]
    victim_share_series = victim_source_series.where(
        victim_source_series.notna(),
        numeric_inputs["victims_count"].fillna(0.0) / incident_denominator_series,
    ).fillna(0.0).clip(lower=0.0, upper=1.0)
    major_damage_source_series = numeric_inputs["major_damage_share"]
    major_damage_share_series = major_damage_source_series.where(
        major_damage_source_series.notna(),
        numeric_inputs["major_damage_count"].fillna(0.0) / incident_denominator_series,
    ).fillna(0.0).clip(lower=0.0, upper=1.0)
    return victim_share_series, major_damage_share_series


def _build_completeness_share_series(
    *,
    response_coverage_share_series: pd.Series,
    distance_coverage_share_series: pd.Series,
    water_unknown_share_series: pd.Series,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    arrival_missing_share_series = (1.0 - response_coverage_share_series).clip(lower=0.0)
    distance_missing_share_series = (1.0 - distance_coverage_share_series).clip(lower=0.0)
    completeness_share_series = (
        1.0
        - ((arrival_missing_share_series + distance_missing_share_series + water_unknown_share_series) / 3.0)
    ).clip(lower=0.0)
    return arrival_missing_share_series, distance_missing_share_series, completeness_share_series


def _build_access_point_base_series(
    numeric_inputs: pd.DataFrame,
) -> _AccessPointBaseSeries:
    (
        incident_count_series,
        years_observed_series,
        incident_count_float_series,
        incidents_per_year_series,
        incident_denominator_series,
    ) = _build_incident_frequency_series(numeric_inputs)
    water_coverage_share_series = _clip_share_series(numeric_inputs["water_coverage_share"])
    no_water_share_series, water_unknown_share_series = _build_water_share_series(
        numeric_inputs,
        water_coverage_share_series,
    )
    victim_share_series, major_damage_share_series = _build_outcome_share_series(
        numeric_inputs,
        incident_denominator_series,
    )
    response_coverage_share_series = _clip_share_series(numeric_inputs["response_coverage_share"])
    distance_coverage_share_series = _clip_share_series(numeric_inputs["distance_coverage_share"])
    arrival_missing_share_series, distance_missing_share_series, completeness_share_series = _build_completeness_share_series(
        response_coverage_share_series=response_coverage_share_series,
        distance_coverage_share_series=distance_coverage_share_series,
        water_unknown_share_series=water_unknown_share_series,
    )
    return _AccessPointBaseSeries(
        incident_count=incident_count_series,
        years_observed=years_observed_series,
        incident_count_float=incident_count_float_series,
        incidents_per_year=incidents_per_year_series,
        average_distance=numeric_inputs["average_distance_km"],
        average_response=numeric_inputs["average_response_minutes"],
        long_arrival_share=_clip_share_series(numeric_inputs["long_arrival_share"]),
        no_water_share=no_water_share_series,
        water_coverage_share=water_coverage_share_series,
        water_unknown_share=water_unknown_share_series,
        severe_share=_clip_share_series(numeric_inputs["severe_share"]),
        victim_share=victim_share_series,
        major_damage_share=major_damage_share_series,
        night_share=_clip_share_series(numeric_inputs["night_share"]),
        heating_share=_clip_share_series(numeric_inputs["heating_share"]),
        rural_share=_clip_share_series(numeric_inputs["rural_share"]),
        response_coverage_share=response_coverage_share_series,
        distance_coverage_share=distance_coverage_share_series,
        arrival_missing_share=arrival_missing_share_series,
        distance_missing_share=distance_missing_share_series,
        completeness_share=completeness_share_series,
        support_weight=_clip_share_series(numeric_inputs["support_weight"], default=1.0),
    )


def _build_access_point_factor_series(base: _AccessPointBaseSeries) -> _AccessPointFactorSeries:
    max_incidents = max(1.0, _finite_series_max(base.incident_count_float, 1.0))
    max_incidents_per_year = max(1.0, _finite_series_max(base.incidents_per_year, 1.0))
    distance_scale = max(12.0, _finite_series_max(base.average_distance, 0.0))
    response_scale = max(
        LONG_RESPONSE_THRESHOLD_MINUTES,
        _finite_series_max(base.average_response, 0.0),
    )
    distance_norm_series = (base.average_distance / distance_scale).clip(lower=0.0, upper=1.0).fillna(0.0)
    response_norm_series = (base.average_response / response_scale).clip(lower=0.0, upper=1.0).fillna(0.0)
    frequency_norm_series = (base.incidents_per_year / max_incidents_per_year).clip(lower=0.0, upper=1.0)
    incidents_norm_series = (base.incident_count_float / max_incidents).clip(lower=0.0, upper=1.0)
    return _AccessPointFactorSeries(
        distance_norm=distance_norm_series,
        response_norm=response_norm_series,
        severity_factor=(
            (0.58 * base.severe_share)
            + (0.24 * base.victim_share)
            + (0.18 * base.major_damage_share)
        ).clip(lower=0.0, upper=1.0),
        recurrence_factor=(
            (0.70 * frequency_norm_series)
            + (0.30 * incidents_norm_series)
        ).clip(lower=0.0, upper=1.0),
        uncertainty_factor=(
            (0.35 * base.arrival_missing_share)
            + (0.30 * base.water_unknown_share)
            + (0.20 * base.distance_missing_share)
            + (0.15 * (1.0 - base.support_weight))
        ).clip(lower=0.0, upper=1.0),
    )


def _build_access_point_score_series(
    numeric_inputs: pd.DataFrame,
    active_reason_codes: set[str],
) -> dict[str, pd.Series]:
    frame_index = numeric_inputs.index
    base = _build_access_point_base_series(numeric_inputs)
    factors = _build_access_point_factor_series(base)
    score_series = {
        key: value
        for key, value in base._asdict().items()
        if key not in {"incident_count_float", "victim_share", "major_damage_share"}
    }
    score_series.update(
        {
            "distance_norm": factors.distance_norm,
            "response_norm": factors.response_norm,
            "severity_factor": factors.severity_factor,
            "recurrence_factor": factors.recurrence_factor,
            "uncertainty_factor": factors.uncertainty_factor,
            "access_score": _weighted_score_series(
                frame_index,
                active_reason_codes,
                (
                    (
                        FACTOR_WEIGHTS[DISTANCE_CODE]
                        / (FACTOR_WEIGHTS[DISTANCE_CODE] + FACTOR_WEIGHTS[RESPONSE_CODE] + FACTOR_WEIGHTS[LONG_ARRIVAL_CODE]),
                        DISTANCE_CODE,
                        factors.distance_norm,
                    ),
                    (
                        FACTOR_WEIGHTS[RESPONSE_CODE]
                        / (FACTOR_WEIGHTS[DISTANCE_CODE] + FACTOR_WEIGHTS[RESPONSE_CODE] + FACTOR_WEIGHTS[LONG_ARRIVAL_CODE]),
                        RESPONSE_CODE,
                        factors.response_norm,
                    ),
                    (
                        FACTOR_WEIGHTS[LONG_ARRIVAL_CODE]
                        / (FACTOR_WEIGHTS[DISTANCE_CODE] + FACTOR_WEIGHTS[RESPONSE_CODE] + FACTOR_WEIGHTS[LONG_ARRIVAL_CODE]),
                        LONG_ARRIVAL_CODE,
                        base.long_arrival_share,
                    ),
                ),
            ),
            "water_score": base.no_water_share * (100.0 if WATER_CODE in active_reason_codes else 0.0),
            "severity_score": factors.severity_factor * (100.0 if SEVERITY_CODE in active_reason_codes else 0.0),
            "recurrence_score": _weighted_score_series(
                frame_index,
                active_reason_codes,
                (
                    (
                        FACTOR_WEIGHTS[RECURRENCE_CODE]
                        / (FACTOR_WEIGHTS[RECURRENCE_CODE] + FACTOR_WEIGHTS[NIGHT_CODE] + FACTOR_WEIGHTS[HEATING_CODE]),
                        RECURRENCE_CODE,
                        factors.recurrence_factor,
                    ),
                    (
                        FACTOR_WEIGHTS[NIGHT_CODE]
                        / (FACTOR_WEIGHTS[RECURRENCE_CODE] + FACTOR_WEIGHTS[NIGHT_CODE] + FACTOR_WEIGHTS[HEATING_CODE]),
                        NIGHT_CODE,
                        base.night_share,
                    ),
                    (
                        FACTOR_WEIGHTS[HEATING_CODE]
                        / (FACTOR_WEIGHTS[RECURRENCE_CODE] + FACTOR_WEIGHTS[NIGHT_CODE] + FACTOR_WEIGHTS[HEATING_CODE]),
                        HEATING_CODE,
                        base.heating_share,
                    ),
                ),
            ),
            "data_gap_score": factors.uncertainty_factor * 100.0,
            "latitude": numeric_inputs["latitude"],
            "longitude": numeric_inputs["longitude"],
        }
    )
    return score_series


def _access_point_precomputed_arrays(series_map: dict[str, pd.Series]) -> dict[str, Sequence[Any]]:
    nullable_keys = {"average_distance", "average_response", "latitude", "longitude"}
    return {
        key: _nullable_series_values(series) if key in nullable_keys else series.to_numpy(copy=False)
        for key, series in series_map.items()
    }


def _prepare_access_point_row_context(
    entity_frame: pd.DataFrame,
    feature_frame: pd.DataFrame | None,
    selected_features: Sequence[str] | None,
) -> dict[str, Any]:
    normalized_selected_features, active_reason_codes, normalized_factor_weights = _resolve_access_point_weight_context(
        selected_features
    )
    working_frame = _merge_access_point_feature_frame(entity_frame, feature_frame)
    numeric_inputs = _finite_numeric_columns(working_frame, ACCESS_POINT_NUMERIC_COLUMNS)
    scoring_series = _build_access_point_score_series(numeric_inputs, active_reason_codes)

    return {
        "working_frame": working_frame,
        "normalized_selected_features": normalized_selected_features,
        "active_reason_codes": active_reason_codes,
        "normalized_factor_weights": normalized_factor_weights,
        "precomputed": _access_point_precomputed_arrays(scoring_series),
    }


def _frame_column_values(
    frame: pd.DataFrame,
    excluded_columns: set[str] | frozenset[str] = frozenset(),
) -> dict[str, Sequence[Any]]:
    return {column: frame[column].to_numpy(copy=False) for column in frame.columns if column not in excluded_columns}


def _record_from_column_values(column_values: dict[str, Sequence[Any]], row_index: int) -> dict[str, Any]:
    return {column: values[row_index] for column, values in column_values.items()}
