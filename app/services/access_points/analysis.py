from __future__ import annotations

from statistics import mean, median
from typing import Any, Dict, List, NamedTuple, Sequence

import pandas as pd

from app.services.forecast_risk.utils import (
    _clean_text,
    _format_integer,
    _format_number,
    _format_percent,
)

from .constants import (
    DEFAULT_ACCESS_POINT_FEATURES,
    LONG_RESPONSE_THRESHOLD_MINUTES,
    MAX_INCOMPLETE_POINTS,
    TOP_POINT_CARD_COUNT,
)
from .point_data import _build_point_entity_frames
from .numeric import (
    _clip_share_series,
    _finite_numeric_columns,
    _finite_series_max,
    _nullable_series_values,
    _share,
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
ACCESS_POINT_PAYLOAD_OVERWRITE_COLUMNS = frozenset(
    {
        "access_score",
        "arrival_missing_share",
        "average_distance_display",
        "average_distance_km",
        "average_response_display",
        "average_response_minutes",
        "component_scores",
        "completeness_display",
        "completeness_share",
        "coordinates_display",
        "data_gap_score",
        "distance_missing_share",
        "explanation",
        "heating_share",
        "heating_share_display",
        "human_readable_explanation",
        "incident_count",
        "incident_count_display",
        "incidents_per_year",
        "incidents_per_year_display",
        "incomplete_note",
        "investigation_score",
        "investigation_score_display",
        "latitude",
        "longitude",
        "long_arrival_share",
        "long_arrival_share_display",
        "low_support_note",
        "missing_data_priority",
        "night_share",
        "night_share_display",
        "no_water_share",
        "no_water_share_display",
        "reason_chips",
        "reason_details",
        "reasons",
        "recurrence_score",
        "response_coverage_display",
        "response_coverage_share",
        "rural_share",
        "rural_share_display",
        "score",
        "score_decomposition",
        "score_display",
        "selected_feature_columns",
        "selected_feature_count",
        "severe_share",
        "severe_share_display",
        "severity_band",
        "severity_band_code",
        "severity_score",
        "tone",
        "top_reason_codes",
        "total_score",
        "total_score_display",
        "typology_code",
        "typology_label",
        "uncertainty_flag",
        "uncertainty_penalty",
        "uncertainty_penalty_display",
        "water_coverage_display",
        "water_coverage_share",
        "water_score",
        "water_unknown_share",
        "water_unknown_share_display",
        "years_observed",
        "years_observed_display",
    }
)
UNCERTAINTY_PENALTY_MAX = 6.0
CRITICAL_THRESHOLD = 75.0
HIGH_THRESHOLD = 55.0
MEDIUM_THRESHOLD = 30.0
WATCH_RISK_THRESHOLD = MEDIUM_THRESHOLD


def _format_coordinate(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.4f}".replace(".", ",")


def _normalize_selected_access_features(selected_features: Sequence[str] | None) -> List[str]:
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


def _status_for_score(score: float) -> tuple[str, str]:
    if score >= CRITICAL_THRESHOLD:
        return "critical", "Критический приоритет"
    if score >= HIGH_THRESHOLD:
        return "warning", "Повышенный приоритет"
    if score >= MEDIUM_THRESHOLD:
        return "watch", "Наблюдение"
    return "normal", "Контроль"


def _component_tone(score: float) -> str:
    if score >= 65.0:
        return "critical"
    if score >= 45.0:
        return "warning"
    if score >= 25.0:
        return "watch"
    return "normal"


def _severity_band_descriptor(score: float) -> Dict[str, str]:
    tone, label = _status_for_score(score)
    if tone == "critical":
        return {"severity_band_code": "critical", "severity_band": "критический", "priority_label": label, "tone": tone}
    if tone == "warning":
        return {"severity_band_code": "high", "severity_band": "высокий", "priority_label": label, "tone": tone}
    if tone == "watch":
        return {"severity_band_code": "medium", "severity_band": "средний", "priority_label": label, "tone": tone}
    return {"severity_band_code": "low", "severity_band": "низкий", "priority_label": label, "tone": tone}


def _factor_label(reason_code: str) -> str:
    return {
        DISTANCE_CODE: "Удалённость до ПЧ",
        RESPONSE_CODE: "Среднее время прибытия",
        LONG_ARRIVAL_CODE: "Доля долгих прибытий",
        WATER_CODE: "Отсутствие воды",
        SEVERITY_CODE: "Тяжёлые последствия",
        RECURRENCE_CODE: "Повторяемость пожаров",
        NIGHT_CODE: "Ночной профиль",
        HEATING_CODE: "Отопительный сезон",
        UNCERTAINTY_CODE: "Неполнота данных",
    }.get(reason_code, reason_code)


def _make_decomposition_item(
    *,
    code: str,
    factor_score: float,
    weight_points: float,
    contribution_points: float,
    value_display: str,
    is_penalty: bool = False,
) -> Dict[str, Any]:
    return {
        "code": code,
        "label": _factor_label(code),
        "factor_score": round(factor_score, 1),
        "factor_score_display": _format_number(factor_score),
        "weight_points": round(weight_points, 2),
        "weight_points_display": _format_number(weight_points),
        "contribution_points": round(contribution_points, 2),
        "contribution_display": f"+{_format_number(contribution_points)}",
        "value_display": value_display,
        "is_penalty": is_penalty,
        "tone": _component_tone(contribution_points * 5.0),
    }


def _distance_value_display(value: Any) -> str:
    return "н/д" if value is None else f"{_format_number(value)} км"


def _response_value_display(value: Any) -> str:
    return "н/д" if value is None else f"{_format_number(value)} мин"


def _share_value_display(value: float) -> str:
    return _format_percent(value * 100.0)


def _build_component_score_item(key: str, label: str, score: float) -> Dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "score": round(score, 1),
        "score_display": _format_number(score),
        "tone": _component_tone(score),
    }


def _build_component_scores_from_values(
    *,
    access_score: float,
    water_score: float,
    severity_score: float,
    recurrence_score: float,
    data_gap_score: float,
) -> List[Dict[str, Any]]:
    return [
        _build_component_score_item("access", "Доступность ПЧ", access_score),
        _build_component_score_item("water", "Водоснабжение", water_score),
        _build_component_score_item("severity", "Последствия", severity_score),
        _build_component_score_item("recurrence", "Частота и контекст", recurrence_score),
        _build_component_score_item("data_gap", "Неполнота данных", data_gap_score),
    ]


def _build_component_scores(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    return _build_component_scores_from_values(
        access_score=float(row.get("access_score") or 0.0),
        water_score=float(row.get("water_score") or 0.0),
        severity_score=float(row.get("severity_score") or 0.0),
        recurrence_score=float(row.get("recurrence_score") or 0.0),
        data_gap_score=float(row.get("data_gap_score") or 0.0),
    )


def _build_reason_details(score_decomposition: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranked = sorted(
        ((item, float(item.get("contribution_points") or 0.0)) for item in score_decomposition),
        key=lambda pair: pair[1],
        reverse=True,
    )
    details = [
        {
            "code": str(item["code"]),
            "label": str(item["label"]),
            "contribution_points": round(contribution, 2),
            "contribution_display": str(item.get("contribution_display") or "0"),
            "value_display": str(item.get("value_display") or ""),
        }
        for item, contribution in ranked
        if contribution > 0
    ]
    return details[:4]


def _build_reason_details_from_decomposition(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    return _build_reason_details(row.get("score_decomposition") or [])


def _build_human_readable_explanation(row: Dict[str, Any]) -> str:
    details = list(row.get("reason_details") or [])
    if not details:
        return "Точка включена в рейтинг по сумме факторов риска."

    lead = f"{row.get('label') or 'Точка'} получает {row.get('severity_band') or 'средний'} риск {row.get('total_score_display') or '0'} из 100."
    drivers = [item for item in details if item["code"] != UNCERTAINTY_CODE][:2]
    if drivers:
        lead += " Основной вклад дали " + ", ".join(
            f"{item['label'].lower()} ({item['contribution_display']})" for item in drivers
        ) + "."
    if row.get("uncertainty_flag"):
        lead += f" Неопределённость добавляет {row.get('uncertainty_penalty_display') or '0'} п. и требует верификации."
    elif row.get("low_support"):
        lead += " Точка низкой опоры: долевые признаки сглажены, а итоговый score ослаблен."
    return lead


def _typology_for_row(row: Dict[str, Any]) -> tuple[str, str]:
    return _typology_for_score_values(
        uncertainty_flag=bool(row.get("uncertainty_flag")),
        severity_band_code=str(row.get("severity_band_code") or ""),
        access_score=float(row.get("access_score") or 0.0),
        water_score=float(row.get("water_score") or 0.0),
        severity_score=float(row.get("severity_score") or 0.0),
        recurrence_score=float(row.get("recurrence_score") or 0.0),
    )


def _typology_for_score_values(
    *,
    uncertainty_flag: bool,
    severity_band_code: str,
    access_score: float,
    water_score: float,
    severity_score: float,
    recurrence_score: float,
) -> tuple[str, str]:
    if uncertainty_flag and severity_band_code in {"low", "medium"}:
        return "needs_data", "Данные неполные"
    components = {
        "access": access_score,
        "water": water_score,
        "severity": severity_score,
        "recurrence": recurrence_score,
    }
    dominant = max(components, key=components.get)
    if dominant == "access" and components["access"] >= 35.0:
        return "access", "Дальний выезд"
    if dominant == "water" and components["water"] >= 30.0:
        return "water", "Дефицит воды"
    if dominant == "severity" and components["severity"] >= 30.0:
        return "severity", "Тяжёлые последствия"
    if dominant == "recurrence" and components["recurrence"] >= 28.0:
        return "recurrence", "Повторяющийся очаг"
    return "mixed", "Комбинированный риск"


def _resolve_access_point_weight_context(
    selected_features: Sequence[str] | None,
) -> tuple[List[str], set[str], Dict[str, float]]:
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


def _build_access_point_score_series(
    numeric_inputs: pd.DataFrame,
    active_reason_codes: set[str],
) -> Dict[str, pd.Series]:
    frame_index = numeric_inputs.index
    max_incidents = max(1.0, _finite_series_max(numeric_inputs["incident_count"], 1.0))
    max_incidents_per_year = max(1.0, _finite_series_max(numeric_inputs["incidents_per_year"], 1.0))
    distance_scale = max(12.0, _finite_series_max(numeric_inputs["average_distance_km"], 0.0))
    response_scale = max(
        LONG_RESPONSE_THRESHOLD_MINUTES,
        _finite_series_max(numeric_inputs["average_response_minutes"], 0.0),
    )

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
    average_distance_series = numeric_inputs["average_distance_km"]
    average_response_series = numeric_inputs["average_response_minutes"]
    long_arrival_share_series = _clip_share_series(numeric_inputs["long_arrival_share"])
    severe_share_series = _clip_share_series(numeric_inputs["severe_share"])
    night_share_series = _clip_share_series(numeric_inputs["night_share"])
    heating_share_series = _clip_share_series(numeric_inputs["heating_share"])
    rural_share_series = _clip_share_series(numeric_inputs["rural_share"])
    response_coverage_share_series = _clip_share_series(numeric_inputs["response_coverage_share"])
    distance_coverage_share_series = _clip_share_series(numeric_inputs["distance_coverage_share"])
    arrival_missing_share_series, distance_missing_share_series, completeness_share_series = _build_completeness_share_series(
        response_coverage_share_series=response_coverage_share_series,
        distance_coverage_share_series=distance_coverage_share_series,
        water_unknown_share_series=water_unknown_share_series,
    )

    distance_norm_series = (average_distance_series / distance_scale).clip(lower=0.0, upper=1.0).fillna(0.0)
    response_norm_series = (average_response_series / response_scale).clip(lower=0.0, upper=1.0).fillna(0.0)
    frequency_norm_series = (incidents_per_year_series / max_incidents_per_year).clip(lower=0.0, upper=1.0)
    incidents_norm_series = (incident_count_float_series / max_incidents).clip(lower=0.0, upper=1.0)
    support_weight_series = _clip_share_series(numeric_inputs["support_weight"], default=1.0)
    severity_factor_series = (
        (0.58 * severe_share_series)
        + (0.24 * victim_share_series)
        + (0.18 * major_damage_share_series)
    ).clip(lower=0.0, upper=1.0)
    recurrence_factor_series = (
        (0.70 * frequency_norm_series)
        + (0.30 * incidents_norm_series)
    ).clip(lower=0.0, upper=1.0)
    uncertainty_factor_series = (
        (0.35 * arrival_missing_share_series)
        + (0.30 * water_unknown_share_series)
        + (0.20 * distance_missing_share_series)
        + (0.15 * (1.0 - support_weight_series))
    ).clip(lower=0.0, upper=1.0)

    return {
        "incident_count": incident_count_series,
        "years_observed": years_observed_series,
        "incidents_per_year": incidents_per_year_series,
        "average_distance": average_distance_series,
        "average_response": average_response_series,
        "long_arrival_share": long_arrival_share_series,
        "no_water_share": no_water_share_series,
        "water_coverage_share": water_coverage_share_series,
        "water_unknown_share": water_unknown_share_series,
        "severe_share": severe_share_series,
        "night_share": night_share_series,
        "heating_share": heating_share_series,
        "rural_share": rural_share_series,
        "response_coverage_share": response_coverage_share_series,
        "distance_coverage_share": distance_coverage_share_series,
        "arrival_missing_share": arrival_missing_share_series,
        "distance_missing_share": distance_missing_share_series,
        "completeness_share": completeness_share_series,
        "distance_norm": distance_norm_series,
        "response_norm": response_norm_series,
        "support_weight": support_weight_series,
        "severity_factor": severity_factor_series,
        "recurrence_factor": recurrence_factor_series,
        "uncertainty_factor": uncertainty_factor_series,
        "access_score": _weighted_score_series(
            frame_index,
            active_reason_codes,
            (
                (0.42, DISTANCE_CODE, distance_norm_series),
                (0.34, RESPONSE_CODE, response_norm_series),
                (0.24, LONG_ARRIVAL_CODE, long_arrival_share_series),
            ),
        ),
        "water_score": no_water_share_series * (100.0 if WATER_CODE in active_reason_codes else 0.0),
        "severity_score": severity_factor_series * (100.0 if SEVERITY_CODE in active_reason_codes else 0.0),
        "recurrence_score": _weighted_score_series(
            frame_index,
            active_reason_codes,
            (
                (0.72, RECURRENCE_CODE, recurrence_factor_series),
                (0.18, NIGHT_CODE, night_share_series),
                (0.10, HEATING_CODE, heating_share_series),
            ),
        ),
        "data_gap_score": uncertainty_factor_series * 100.0,
        "latitude": numeric_inputs["latitude"],
        "longitude": numeric_inputs["longitude"],
    }


def _access_point_precomputed_arrays(series_map: Dict[str, pd.Series]) -> Dict[str, Sequence[Any]]:
    nullable_keys = {"average_distance", "average_response", "latitude", "longitude"}
    return {
        key: _nullable_series_values(series) if key in nullable_keys else series.to_numpy(copy=False)
        for key, series in series_map.items()
    }


def _prepare_access_point_row_context(
    entity_frame: pd.DataFrame,
    feature_frame: pd.DataFrame | None,
    selected_features: Sequence[str] | None,
) -> Dict[str, Any]:
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
) -> Dict[str, Sequence[Any]]:
    return {column: frame[column].to_numpy(copy=False) for column in frame.columns if column not in excluded_columns}


def _record_from_column_values(column_values: Dict[str, Sequence[Any]], row_index: int) -> Dict[str, Any]:
    return {column: values[row_index] for column, values in column_values.items()}


class _AccessPointRowMetrics(NamedTuple):
    incident_count: int
    years_observed: int
    incidents_per_year: float
    average_distance: float | None
    average_response: float | None
    long_arrival_share: float
    no_water_share: float
    water_coverage_share: float
    water_unknown_share: float
    severe_share: float
    night_share: float
    heating_share: float
    rural_share: float
    response_coverage_share: float
    distance_coverage_share: float
    arrival_missing_share: float
    distance_missing_share: float
    completeness_share: float
    distance_norm: float
    response_norm: float
    support_weight: float
    severity_factor: float
    recurrence_factor: float
    uncertainty_factor: float
    access_score: float
    water_score: float
    severity_score: float
    recurrence_score: float
    data_gap_score: float
    latitude: float | None
    longitude: float | None


class _AccessPointScoreContext(NamedTuple):
    score_decomposition: List[Dict[str, Any]]
    total_score: float
    total_score_display: str
    uncertainty_penalty: float
    uncertainty_penalty_display: str
    investigation_score: float
    investigation_score_display: str
    access_score_payload: float
    water_score_payload: float
    severity_score_payload: float
    recurrence_score_payload: float
    data_gap_score_payload: float


class _AccessPointDisplayContext(NamedTuple):
    average_distance_display: str
    average_response_display: str
    response_coverage_display: str
    long_arrival_share_display: str
    no_water_share_display: str
    water_unknown_share_display: str
    water_coverage_display: str
    severe_share_display: str
    night_share_display: str
    heating_share_display: str
    rural_share_display: str
    completeness_display: str


class _AccessPointReasonListContext(NamedTuple):
    top_reason_codes: List[str]
    reasons: List[str]
    reason_chips: List[str]


def _nullable_precomputed_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _coordinates_display(latitude: float | None, longitude: float | None) -> str:
    if latitude is None or longitude is None:
        return ""
    return f"{_format_coordinate(latitude)}, {_format_coordinate(longitude)}"


def _build_reason_list_context(reason_details: Sequence[Dict[str, Any]]) -> _AccessPointReasonListContext:
    top_four_details = list(reason_details[:4])
    top_three_details = top_four_details[:3]
    return _AccessPointReasonListContext(
        top_reason_codes=[item["code"] for item in top_three_details],
        reasons=[f"{item['label']}: {item['value_display']}" for item in top_four_details],
        reason_chips=[f"{item['label']}: {item['contribution_display']}" for item in top_three_details],
    )


def _access_point_row_metrics(precomputed: Dict[str, Sequence[Any]], row_index: int) -> _AccessPointRowMetrics:
    return _AccessPointRowMetrics(
        incident_count=int(precomputed["incident_count"][row_index]),
        years_observed=int(precomputed["years_observed"][row_index]),
        incidents_per_year=float(precomputed["incidents_per_year"][row_index]),
        average_distance=_nullable_precomputed_float(precomputed["average_distance"][row_index]),
        average_response=_nullable_precomputed_float(precomputed["average_response"][row_index]),
        long_arrival_share=float(precomputed["long_arrival_share"][row_index]),
        no_water_share=float(precomputed["no_water_share"][row_index]),
        water_coverage_share=float(precomputed["water_coverage_share"][row_index]),
        water_unknown_share=float(precomputed["water_unknown_share"][row_index]),
        severe_share=float(precomputed["severe_share"][row_index]),
        night_share=float(precomputed["night_share"][row_index]),
        heating_share=float(precomputed["heating_share"][row_index]),
        rural_share=float(precomputed["rural_share"][row_index]),
        response_coverage_share=float(precomputed["response_coverage_share"][row_index]),
        distance_coverage_share=float(precomputed["distance_coverage_share"][row_index]),
        arrival_missing_share=float(precomputed["arrival_missing_share"][row_index]),
        distance_missing_share=float(precomputed["distance_missing_share"][row_index]),
        completeness_share=float(precomputed["completeness_share"][row_index]),
        distance_norm=float(precomputed["distance_norm"][row_index]),
        response_norm=float(precomputed["response_norm"][row_index]),
        support_weight=float(precomputed["support_weight"][row_index]),
        severity_factor=float(precomputed["severity_factor"][row_index]),
        recurrence_factor=float(precomputed["recurrence_factor"][row_index]),
        uncertainty_factor=float(precomputed["uncertainty_factor"][row_index]),
        access_score=float(precomputed["access_score"][row_index]),
        water_score=float(precomputed["water_score"][row_index]),
        severity_score=float(precomputed["severity_score"][row_index]),
        recurrence_score=float(precomputed["recurrence_score"][row_index]),
        data_gap_score=float(precomputed["data_gap_score"][row_index]),
        latitude=_nullable_precomputed_float(precomputed["latitude"][row_index]),
        longitude=_nullable_precomputed_float(precomputed["longitude"][row_index]),
    )


def _build_access_point_display_context(metrics: _AccessPointRowMetrics) -> _AccessPointDisplayContext:
    return _AccessPointDisplayContext(
        average_distance_display=_distance_value_display(metrics.average_distance),
        average_response_display=_response_value_display(metrics.average_response),
        response_coverage_display=_share_value_display(metrics.response_coverage_share),
        long_arrival_share_display=_share_value_display(metrics.long_arrival_share),
        no_water_share_display=_share_value_display(metrics.no_water_share),
        water_unknown_share_display=_share_value_display(metrics.water_unknown_share),
        water_coverage_display=_share_value_display(metrics.water_coverage_share),
        severe_share_display=_share_value_display(metrics.severe_share),
        night_share_display=_share_value_display(metrics.night_share),
        heating_share_display=_share_value_display(metrics.heating_share),
        rural_share_display=_share_value_display(metrics.rural_share),
        completeness_display=_share_value_display(metrics.completeness_share),
    )


def _access_point_decomposition_components(
    *,
    active_reason_codes: set[str],
    average_distance: Any,
    average_response: Any,
    distance_norm: float,
    response_norm: float,
    long_arrival_share: float,
    no_water_share: float,
    severe_share: float,
    night_share: float,
    heating_share: float,
    incidents_per_year: float,
    severity_factor: float,
    recurrence_factor: float,
) -> List[tuple[str, float, float, str]]:
    components: List[tuple[str, float, float, str]] = []
    if DISTANCE_CODE in active_reason_codes:
        components.append((DISTANCE_CODE, distance_norm * 100.0, distance_norm, _distance_value_display(average_distance)))
    if RESPONSE_CODE in active_reason_codes:
        components.append((RESPONSE_CODE, response_norm * 100.0, response_norm, _response_value_display(average_response)))
    if LONG_ARRIVAL_CODE in active_reason_codes:
        components.append((LONG_ARRIVAL_CODE, long_arrival_share * 100.0, long_arrival_share, _share_value_display(long_arrival_share)))
    if WATER_CODE in active_reason_codes:
        components.append((WATER_CODE, no_water_share * 100.0, no_water_share, _share_value_display(no_water_share)))
    if SEVERITY_CODE in active_reason_codes:
        components.append((SEVERITY_CODE, severity_factor * 100.0, severity_factor, _share_value_display(severe_share)))
    if RECURRENCE_CODE in active_reason_codes:
        components.append((RECURRENCE_CODE, recurrence_factor * 100.0, recurrence_factor, f"{_format_number(incidents_per_year)} в год"))
    if NIGHT_CODE in active_reason_codes:
        components.append((NIGHT_CODE, night_share * 100.0, night_share, _share_value_display(night_share)))
    if HEATING_CODE in active_reason_codes:
        components.append((HEATING_CODE, heating_share * 100.0, heating_share, _share_value_display(heating_share)))
    return components


def _build_access_point_score_decomposition(
    *,
    average_distance: Any,
    average_response: Any,
    distance_norm: float,
    response_norm: float,
    long_arrival_share: float,
    no_water_share: float,
    severe_share: float,
    night_share: float,
    heating_share: float,
    incidents_per_year: float,
    severity_factor: float,
    recurrence_factor: float,
    support_weight: float,
    uncertainty_factor: float,
    completeness_share: float,
    active_reason_codes: set[str],
    normalized_factor_weights: Dict[str, float],
) -> List[Dict[str, Any]]:
    score_decomposition: List[Dict[str, Any]] = []
    for code, factor_score, factor_value, value_display in _access_point_decomposition_components(
        active_reason_codes=active_reason_codes,
        average_distance=average_distance,
        average_response=average_response,
        distance_norm=distance_norm,
        response_norm=response_norm,
        long_arrival_share=long_arrival_share,
        no_water_share=no_water_share,
        severe_share=severe_share,
        night_share=night_share,
        heating_share=heating_share,
        incidents_per_year=incidents_per_year,
        severity_factor=severity_factor,
        recurrence_factor=recurrence_factor,
    ):
        score_decomposition.append(
            _make_decomposition_item(
                code=code,
                factor_score=factor_score,
                weight_points=normalized_factor_weights[code],
                contribution_points=normalized_factor_weights[code] * factor_value * support_weight,
                value_display=value_display,
            )
        )
    score_decomposition.append(
        _make_decomposition_item(
            code=UNCERTAINTY_CODE,
            factor_score=uncertainty_factor * 100.0,
            weight_points=UNCERTAINTY_PENALTY_MAX,
            contribution_points=UNCERTAINTY_PENALTY_MAX * uncertainty_factor,
            value_display=f"полнота {_share_value_display(completeness_share)}",
            is_penalty=True,
        )
    )
    return score_decomposition


def _score_total_and_uncertainty_penalty(score_decomposition: Sequence[Dict[str, Any]]) -> tuple[float, float]:
    total_score = 0.0
    uncertainty_penalty: float | None = None
    for item in score_decomposition:
        contribution_points = float(item.get("contribution_points") or 0.0)
        total_score += contribution_points
        if uncertainty_penalty is None and item.get("code") == UNCERTAINTY_CODE:
            uncertainty_penalty = contribution_points
    return total_score, 0.0 if uncertainty_penalty is None else uncertainty_penalty


def _build_access_point_score_decomposition_from_metrics(
    metrics: _AccessPointRowMetrics,
    *,
    active_reason_codes: set[str],
    normalized_factor_weights: Dict[str, float],
) -> List[Dict[str, Any]]:
    return _build_access_point_score_decomposition(
        average_distance=metrics.average_distance,
        average_response=metrics.average_response,
        distance_norm=metrics.distance_norm,
        response_norm=metrics.response_norm,
        long_arrival_share=metrics.long_arrival_share,
        no_water_share=metrics.no_water_share,
        severe_share=metrics.severe_share,
        night_share=metrics.night_share,
        heating_share=metrics.heating_share,
        incidents_per_year=metrics.incidents_per_year,
        severity_factor=metrics.severity_factor,
        recurrence_factor=metrics.recurrence_factor,
        support_weight=metrics.support_weight,
        uncertainty_factor=metrics.uncertainty_factor,
        completeness_share=metrics.completeness_share,
        active_reason_codes=active_reason_codes,
        normalized_factor_weights=normalized_factor_weights,
    )


def _build_access_point_score_context(
    metrics: _AccessPointRowMetrics,
    *,
    active_reason_codes: set[str],
    normalized_factor_weights: Dict[str, float],
) -> _AccessPointScoreContext:
    score_decomposition = _build_access_point_score_decomposition_from_metrics(
        metrics,
        active_reason_codes=active_reason_codes,
        normalized_factor_weights=normalized_factor_weights,
    )
    total_score, uncertainty_penalty = _score_total_and_uncertainty_penalty(score_decomposition)
    investigation_score = min(
        100.0,
        (0.72 * total_score) + (0.28 * (uncertainty_penalty * 100.0 / UNCERTAINTY_PENALTY_MAX)),
    )
    return _AccessPointScoreContext(
        score_decomposition=score_decomposition,
        total_score=total_score,
        total_score_display=_format_number(total_score),
        uncertainty_penalty=uncertainty_penalty,
        uncertainty_penalty_display=_format_number(uncertainty_penalty),
        investigation_score=investigation_score,
        investigation_score_display=_format_number(investigation_score),
        access_score_payload=round(metrics.access_score, 1),
        water_score_payload=round(metrics.water_score, 1),
        severity_score_payload=round(metrics.severity_score, 1),
        recurrence_score_payload=round(metrics.recurrence_score, 1),
        data_gap_score_payload=round(metrics.data_gap_score, 1),
    )


def _build_access_point_payload_row(
    *,
    record: Dict[str, Any],
    precomputed: Dict[str, Sequence[Any]],
    row_index: int,
    normalized_selected_features: Sequence[str],
    active_reason_codes: set[str],
    normalized_factor_weights: Dict[str, float],
) -> Dict[str, Any]:
    metrics = _access_point_row_metrics(precomputed, row_index)
    score_context = _build_access_point_score_context(
        metrics,
        active_reason_codes=active_reason_codes,
        normalized_factor_weights=normalized_factor_weights,
    )
    displays = _build_access_point_display_context(metrics)
    low_support = bool(record.get("low_support"))
    uncertainty_flag = bool(
        score_context.uncertainty_penalty >= 2.5
        or low_support
        or metrics.completeness_share < 0.6
    )
    missing_data_priority = uncertainty_flag and score_context.total_score < HIGH_THRESHOLD
    total_score_payload = round(score_context.total_score, 1)
    uncertainty_penalty_payload = round(score_context.uncertainty_penalty, 2)
    investigation_score_payload = round(score_context.investigation_score, 1)
    severity_descriptor = _severity_band_descriptor(total_score_payload)
    component_scores = _build_component_scores_from_values(
        access_score=score_context.access_score_payload,
        water_score=score_context.water_score_payload,
        severity_score=score_context.severity_score_payload,
        recurrence_score=score_context.recurrence_score_payload,
        data_gap_score=score_context.data_gap_score_payload,
    )
    row: Dict[str, Any] = record
    row.update({
        "label": _clean_text(record.get("label")) or "Точка",
        "location_hint": _clean_text(record.get("location_hint")) or "Локация определена по лучшей доступной сущности",
        "incident_count": metrics.incident_count,
        "incident_count_display": _format_integer(metrics.incident_count),
        "years_observed": metrics.years_observed,
        "years_observed_display": _format_integer(metrics.years_observed),
        "incidents_per_year": round(metrics.incidents_per_year, 2),
        "incidents_per_year_display": _format_number(metrics.incidents_per_year),
        "average_distance_km": None if metrics.average_distance is None else round(metrics.average_distance, 2),
        "average_distance_display": displays.average_distance_display,
        "average_response_minutes": None if metrics.average_response is None else round(metrics.average_response, 1),
        "average_response_display": displays.average_response_display,
        "response_coverage_share": round(metrics.response_coverage_share, 4),
        "response_coverage_display": displays.response_coverage_display,
        "long_arrival_share": round(metrics.long_arrival_share, 4),
        "long_arrival_share_display": displays.long_arrival_share_display,
        "no_water_share": round(metrics.no_water_share, 4),
        "no_water_share_display": displays.no_water_share_display,
        "water_unknown_share": round(metrics.water_unknown_share, 4),
        "water_unknown_share_display": displays.water_unknown_share_display,
        "water_coverage_share": round(metrics.water_coverage_share, 4),
        "water_coverage_display": displays.water_coverage_display,
        "severe_share": round(metrics.severe_share, 4),
        "severe_share_display": displays.severe_share_display,
        "night_share": round(metrics.night_share, 4),
        "night_share_display": displays.night_share_display,
        "heating_share": round(metrics.heating_share, 4),
        "heating_share_display": displays.heating_share_display,
        "rural_share": round(metrics.rural_share, 4),
        "rural_share_display": displays.rural_share_display,
        "arrival_missing_share": round(metrics.arrival_missing_share, 4),
        "distance_missing_share": round(metrics.distance_missing_share, 4),
        "completeness_share": round(metrics.completeness_share, 4),
        "completeness_display": displays.completeness_display,
        "access_score": score_context.access_score_payload,
        "water_score": score_context.water_score_payload,
        "severity_score": score_context.severity_score_payload,
        "recurrence_score": score_context.recurrence_score_payload,
        "data_gap_score": score_context.data_gap_score_payload,
        "score": total_score_payload,
        "score_display": score_context.total_score_display,
        "total_score": total_score_payload,
        "total_score_display": score_context.total_score_display,
        "uncertainty_penalty": uncertainty_penalty_payload,
        "uncertainty_penalty_display": score_context.uncertainty_penalty_display,
        "investigation_score": investigation_score_payload,
        "investigation_score_display": score_context.investigation_score_display,
        "missing_data_priority": missing_data_priority,
        "uncertainty_flag": uncertainty_flag,
        "low_support": low_support,
        "low_support_note": (
            f"Точка собрана всего по {_format_integer(metrics.incident_count)} пожарам, долевые признаки сглажены."
            if low_support
            else ""
        ),
        "selected_feature_columns": list(normalized_selected_features),
        "selected_feature_count": len(normalized_selected_features),
        "score_decomposition": score_context.score_decomposition,
        "latitude": metrics.latitude,
        "longitude": metrics.longitude,
        "coordinates_display": _coordinates_display(metrics.latitude, metrics.longitude),
    })
    row.update(severity_descriptor)
    row["component_scores"] = component_scores
    row["typology_code"], row["typology_label"] = _typology_for_score_values(
        uncertainty_flag=uncertainty_flag,
        severity_band_code=severity_descriptor["severity_band_code"],
        access_score=score_context.access_score_payload,
        water_score=score_context.water_score_payload,
        severity_score=score_context.severity_score_payload,
        recurrence_score=score_context.recurrence_score_payload,
    )
    row["reason_details"] = _build_reason_details(score_context.score_decomposition)
    reason_lists = _build_reason_list_context(row["reason_details"])
    row["top_reason_codes"] = reason_lists.top_reason_codes
    row["reasons"] = reason_lists.reasons
    row["reason_chips"] = reason_lists.reason_chips
    row["human_readable_explanation"] = _build_human_readable_explanation(row)
    row["explanation"] = row["human_readable_explanation"]
    row["incomplete_note"] = (
        "Высокий приоритет проверки связан прежде всего с пропусками по доступности, воде или времени прибытия."
        if missing_data_priority
        else row["low_support_note"]
    )
    if uncertainty_flag:
        row["incomplete_note"] = (
            f"Неопределённость добавляет {row['uncertainty_penalty_display']} п. "
            "и требует верификации воды, времени прибытия и дистанции."
        )
    return row


def _build_access_point_rows_from_entity_frame(
    entity_frame: pd.DataFrame,
    feature_frame: pd.DataFrame | None = None,
    selected_features: Sequence[str] | None = None,
) -> List[Dict[str, Any]]:
    if entity_frame is None or entity_frame.empty:
        return []

    row_context = _prepare_access_point_row_context(entity_frame, feature_frame, selected_features)
    normalized_selected_features = row_context["normalized_selected_features"]
    active_reason_codes = row_context["active_reason_codes"]
    normalized_factor_weights = row_context["normalized_factor_weights"]
    working_frame = row_context["working_frame"]
    precomputed = row_context["precomputed"]
    record_columns = _frame_column_values(working_frame, ACCESS_POINT_PAYLOAD_OVERWRITE_COLUMNS)

    normalized_rows: List[Dict[str, Any]] = []
    for row_index in range(len(working_frame)):
        record = _record_from_column_values(record_columns, row_index)
        normalized_rows.append(
            _build_access_point_payload_row(
                record=record,
                precomputed=precomputed,
                row_index=row_index,
                normalized_selected_features=normalized_selected_features,
                active_reason_codes=active_reason_codes,
                normalized_factor_weights=normalized_factor_weights,
            )
        )

    normalized_rows.sort(
        key=lambda item: (
            float(item["total_score"]),
            float(item["severity_score"]),
            float(item["access_score"]),
            int(item["incident_count"]),
            int(item["granularity_rank"]),
        ),
        reverse=True,
    )
    for index, row in enumerate(normalized_rows, start=1):
        row["rank"] = index
        row["rank_display"] = str(index)
    return normalized_rows


def _build_access_point_rows(
    records: Sequence[Dict[str, Any]],
    selected_features: Sequence[str] | None = None,
) -> List[Dict[str, Any]]:
    entity_frame, _feature_frame = _build_point_entity_frames(records)
    return _build_access_point_rows_from_entity_frame(entity_frame, selected_features=selected_features)


def _select_top_points(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(row) for row in list(rows)[:TOP_POINT_CARD_COUNT]]


def _select_incomplete_points(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    candidates = [
        dict(row)
        for row in rows
        if row.get("missing_data_priority")
        or (
            float(row.get("data_gap_score") or 0.0) >= 50.0
            and float(row.get("investigation_score") or 0.0) >= WATCH_RISK_THRESHOLD
        )
    ]
    candidates.sort(
        key=lambda item: (
            float(item.get("investigation_score") or 0.0),
            float(item.get("data_gap_score") or 0.0),
            float(item.get("total_score") or 0.0),
        ),
        reverse=True,
    )
    return candidates[:MAX_INCOMPLETE_POINTS]


def _build_typology_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = str(row.get("typology_code") or "mixed")
        total_score = float(row.get("total_score") or 0.0)
        bucket = grouped.setdefault(
            code,
            {
                "code": code,
                "label": row.get("typology_label") or "Комбинированный риск",
                "count": 0,
                "max_score": 0.0,
                "lead_label": "",
            },
        )
        bucket["count"] += 1
        if total_score >= float(bucket["max_score"]):
            bucket["max_score"] = total_score
            bucket["lead_label"] = str(row.get("label") or "")

    total = max(1, len(rows))
    result = []
    for bucket in grouped.values():
        result.append(
            {
                "code": bucket["code"],
                "label": bucket["label"],
                "count": int(bucket["count"]),
                "count_display": _format_integer(bucket["count"]),
                "share_display": _format_percent(_share(bucket["count"], total) * 100.0),
                "max_score": round(float(bucket["max_score"]), 1),
                "max_score_display": _format_number(bucket["max_score"]),
                "lead_label": bucket["lead_label"] or "-",
            }
        )
    result.sort(key=lambda item: (item["count"], item["max_score"]), reverse=True)
    return result


def _build_score_distribution(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"average_score_display": "0", "median_score_display": "0", "bands": [], "buckets": []}

    scores: List[float] = []
    band_counts = {"low": 0, "medium": 0, "high": 0, "critical": 0}
    bucket_counts = [0, 0, 0, 0]
    for row in rows:
        score = float(row.get("total_score") or 0.0)
        scores.append(score)
        band_code = str(row.get("severity_band_code") or "")
        if band_code in band_counts:
            band_counts[band_code] += 1
        if 0 <= score < 25:
            bucket_counts[0] += 1
        elif 25 <= score < 50:
            bucket_counts[1] += 1
        elif 50 <= score < 75:
            bucket_counts[2] += 1
        elif 75 <= score < 101:
            bucket_counts[3] += 1

    bands = []
    for code, label in (("low", "Низкий"), ("medium", "Средний"), ("high", "Высокий"), ("critical", "Критический")):
        count = band_counts[code]
        bands.append(
            {
                "code": code,
                "label": label,
                "count": count,
                "count_display": _format_integer(count),
                "share_display": _format_percent(_share(count, len(rows)) * 100.0),
            }
        )

    buckets = []
    for count, (start, end) in zip(bucket_counts, ((0, 25), (25, 50), (50, 75), (75, 101))):
        buckets.append({"label": f"{start}-{min(100, end - 1)}", "count": count, "count_display": _format_integer(count)})

    return {
        "average_score_display": _format_number(mean(scores)),
        "median_score_display": _format_number(median(scores)),
        "bands": bands,
        "buckets": buckets,
    }


def _build_reason_breakdown(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        for detail in list(row.get("reason_details") or [])[:3]:
            code = str(detail.get("code") or "")
            if not code:
                continue
            bucket = buckets.setdefault(
                code,
                {
                    "code": code,
                    "label": str(detail.get("label") or code),
                    "count": 0,
                    "total_contribution": 0.0,
                    "max_contribution": 0.0,
                    "lead_label": "",
                },
            )
            contribution = float(detail.get("contribution_points") or 0.0)
            bucket["count"] += 1
            bucket["total_contribution"] += contribution
            if contribution >= bucket["max_contribution"]:
                bucket["max_contribution"] = contribution
                bucket["lead_label"] = str(row.get("label") or "")

    total = max(1, len(rows))
    result = []
    for bucket in buckets.values():
        average_contribution = bucket["total_contribution"] / max(1, bucket["count"])
        result.append(
            {
                "code": bucket["code"],
                "label": bucket["label"],
                "count": int(bucket["count"]),
                "count_display": _format_integer(bucket["count"]),
                "share_display": _format_percent(_share(bucket["count"], total) * 100.0),
                "average_contribution": round(average_contribution, 2),
                "average_contribution_display": _format_number(average_contribution),
                "max_contribution_display": _format_number(bucket["max_contribution"]),
                "lead_label": bucket["lead_label"] or "-",
            }
        )
    result.sort(key=lambda item: (item["count"], item["average_contribution"]), reverse=True)
    return result


def _build_uncertainty_notes(rows: Sequence[Dict[str, Any]]) -> List[str]:
    if not rows:
        return ["Неопределённость будет оценена после расчёта рейтинга."]

    low_support_count = 0
    uncertainty_count = 0
    high_penalty_count = 0
    for row in rows:
        if row.get("low_support"):
            low_support_count += 1
        if row.get("uncertainty_flag"):
            uncertainty_count += 1
        if float(row.get("uncertainty_penalty") or 0.0) >= 3.0:
            high_penalty_count += 1
    notes = [
        "Неполнота данных даёт отдельный penalty, но не является главным драйвером риска: её вклад ограничен 6 баллами.",
    ]
    if low_support_count:
        notes.append(
            f"Для {_format_integer(low_support_count)} точек опора ниже минимального порога, поэтому долевые признаки сглажены, а итоговый риск ослаблен коэффициентом поддержки."
        )
    if uncertainty_count:
        notes.append(
            f"У {_format_integer(uncertainty_count)} точек есть uncertainty flag из-за пропусков по воде, времени прибытия, дистанции до ПЧ или малого числа пожаров."
        )
    if high_penalty_count:
        notes.append(
            f"У {_format_integer(high_penalty_count)} точек penalty за неполноту данных уже заметен и требует управленческой верификации."
        )
    return notes[:4]
