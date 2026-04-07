from __future__ import annotations

from statistics import mean, median
from typing import Any, Dict, List, Sequence

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
    _finite_numeric_max,
    _finite_numeric_series,
    _normalize_nullable_float,
    _normalize_share_series,
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
        factor_sum = factor_sum.add(factor_value.astype(float) * float(component_weight), fill_value=0.0)
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


def _build_component_scores(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        {
            "key": "access",
            "label": "Доступность ПЧ",
            "score": round(float(row.get("access_score") or 0.0), 1),
            "score_display": _format_number(float(row.get("access_score") or 0.0)),
            "tone": _component_tone(float(row.get("access_score") or 0.0)),
        },
        {
            "key": "water",
            "label": "Водоснабжение",
            "score": round(float(row.get("water_score") or 0.0), 1),
            "score_display": _format_number(float(row.get("water_score") or 0.0)),
            "tone": _component_tone(float(row.get("water_score") or 0.0)),
        },
        {
            "key": "severity",
            "label": "Последствия",
            "score": round(float(row.get("severity_score") or 0.0), 1),
            "score_display": _format_number(float(row.get("severity_score") or 0.0)),
            "tone": _component_tone(float(row.get("severity_score") or 0.0)),
        },
        {
            "key": "recurrence",
            "label": "Частота и контекст",
            "score": round(float(row.get("recurrence_score") or 0.0), 1),
            "score_display": _format_number(float(row.get("recurrence_score") or 0.0)),
            "tone": _component_tone(float(row.get("recurrence_score") or 0.0)),
        },
        {
            "key": "data_gap",
            "label": "Неполнота данных",
            "score": round(float(row.get("data_gap_score") or 0.0), 1),
            "score_display": _format_number(float(row.get("data_gap_score") or 0.0)),
            "tone": _component_tone(float(row.get("data_gap_score") or 0.0)),
        },
    ]


def _build_reason_details_from_decomposition(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    decomposition = list(row.get("score_decomposition") or [])
    ranked = sorted(
        decomposition,
        key=lambda item: float(item.get("contribution_points") or 0.0),
        reverse=True,
    )
    details = [
        {
            "code": str(item["code"]),
            "label": str(item["label"]),
            "contribution_points": round(float(item.get("contribution_points") or 0.0), 2),
            "contribution_display": str(item.get("contribution_display") or "0"),
            "value_display": str(item.get("value_display") or ""),
        }
        for item in ranked
        if float(item.get("contribution_points") or 0.0) > 0
    ]
    return details[:4]


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
    if row.get("uncertainty_flag") and str(row.get("severity_band_code") or "") in {"low", "medium"}:
        return "needs_data", "Данные неполные"
    components = {
        "access": float(row.get("access_score") or 0.0),
        "water": float(row.get("water_score") or 0.0),
        "severity": float(row.get("severity_score") or 0.0),
        "recurrence": float(row.get("recurrence_score") or 0.0),
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


def _prepare_access_point_row_context(
    entity_frame: pd.DataFrame,
    feature_frame: pd.DataFrame | None,
    selected_features: Sequence[str] | None,
) -> Dict[str, Any]:
    normalized_selected_features = _normalize_selected_access_features(selected_features)
    active_reason_codes = set(normalized_selected_features)
    selected_weight_sum = sum(float(FACTOR_WEIGHTS[code]) for code in normalized_selected_features if code in FACTOR_WEIGHTS)
    normalized_factor_weights = {
        code: (94.0 * float(weight) / selected_weight_sum) if code in active_reason_codes and selected_weight_sum > 0 else 0.0
        for code, weight in FACTOR_WEIGHTS.items()
    }

    working_frame = entity_frame.reset_index(drop=True)
    if feature_frame is not None and not feature_frame.empty:
        aligned_features = feature_frame.reset_index(drop=True)
        extra_columns = [column for column in aligned_features.columns if column not in working_frame.columns]
        if extra_columns:
            working_frame = working_frame.copy()
        for column in extra_columns:
            working_frame[column] = aligned_features[column]

    max_incidents = max(1.0, _finite_numeric_max(working_frame["incident_count"], 1.0))
    max_incidents_per_year = max(1.0, _finite_numeric_max(working_frame["incidents_per_year"], 1.0))
    distance_scale = max(12.0, _finite_numeric_max(working_frame["average_distance_km"], 0.0))
    response_scale = max(
        LONG_RESPONSE_THRESHOLD_MINUTES,
        _finite_numeric_max(working_frame["average_response_minutes"], 0.0),
    )

    frame_index = working_frame.index
    incident_count_series = _finite_numeric_series(working_frame["incident_count"], default=0.0).clip(lower=0.0).astype(int)
    years_observed_series = _finite_numeric_series(working_frame["years_observed"], default=1.0).clip(lower=1.0).astype(int)
    incidents_per_year_fallback = incident_count_series.astype(float) / years_observed_series.astype(float)
    incidents_per_year_source = _finite_numeric_series(working_frame["incidents_per_year"])
    incidents_per_year_series = incidents_per_year_source.where(
        incidents_per_year_source.notna(),
        incidents_per_year_fallback,
    )
    average_distance_series = _finite_numeric_series(working_frame["average_distance_km"])
    average_response_series = _finite_numeric_series(working_frame["average_response_minutes"])
    long_arrival_share_series = _normalize_share_series(working_frame["long_arrival_share"])
    no_water_share_series = _normalize_share_series(working_frame["no_water_share"])
    water_coverage_share_series = _normalize_share_series(working_frame["water_coverage_share"])
    water_unknown_source_series = _finite_numeric_series(working_frame["water_unknown_share"])
    water_unknown_share_series = water_unknown_source_series.where(
        water_unknown_source_series.notna(),
        (1.0 - water_coverage_share_series).clip(lower=0.0),
    ).fillna(0.0).clip(lower=0.0, upper=1.0)
    severe_share_series = _normalize_share_series(working_frame["severe_share"])
    victim_source_series = _finite_numeric_series(working_frame["victim_share"])
    incident_denominator_series = incident_count_series.clip(lower=1).astype(float)
    victim_share_series = victim_source_series.where(
        victim_source_series.notna(),
        _finite_numeric_series(working_frame["victims_count"], default=0.0) / incident_denominator_series,
    ).fillna(0.0).clip(lower=0.0, upper=1.0)
    major_damage_source_series = _finite_numeric_series(working_frame["major_damage_share"])
    major_damage_share_series = major_damage_source_series.where(
        major_damage_source_series.notna(),
        _finite_numeric_series(working_frame["major_damage_count"], default=0.0) / incident_denominator_series,
    ).fillna(0.0).clip(lower=0.0, upper=1.0)
    night_share_series = _normalize_share_series(working_frame["night_share"])
    heating_share_series = _normalize_share_series(working_frame["heating_share"])
    rural_share_series = _normalize_share_series(working_frame["rural_share"])
    response_coverage_share_series = _normalize_share_series(working_frame["response_coverage_share"])
    distance_coverage_share_series = _normalize_share_series(working_frame["distance_coverage_share"])
    arrival_missing_share_series = (1.0 - response_coverage_share_series).clip(lower=0.0)
    distance_missing_share_series = (1.0 - distance_coverage_share_series).clip(lower=0.0)
    completeness_share_series = (
        1.0
        - ((arrival_missing_share_series + distance_missing_share_series + water_unknown_share_series) / 3.0)
    ).clip(lower=0.0)

    distance_norm_series = (average_distance_series / distance_scale).clip(lower=0.0, upper=1.0).fillna(0.0)
    response_norm_series = (average_response_series / response_scale).clip(lower=0.0, upper=1.0).fillna(0.0)
    frequency_norm_series = (incidents_per_year_series / max_incidents_per_year).clip(lower=0.0, upper=1.0)
    incidents_norm_series = (incident_count_series.astype(float) / max_incidents).clip(lower=0.0, upper=1.0)
    support_weight_series = _normalize_share_series(working_frame["support_weight"], default=1.0)
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
    access_score_series = _weighted_score_series(
        frame_index,
        active_reason_codes,
        (
            (0.42, DISTANCE_CODE, distance_norm_series),
            (0.34, RESPONSE_CODE, response_norm_series),
            (0.24, LONG_ARRIVAL_CODE, long_arrival_share_series),
        ),
    )
    water_score_series = no_water_share_series * (100.0 if WATER_CODE in active_reason_codes else 0.0)
    severity_score_series = severity_factor_series * (100.0 if SEVERITY_CODE in active_reason_codes else 0.0)
    recurrence_score_series = _weighted_score_series(
        frame_index,
        active_reason_codes,
        (
            (0.72, RECURRENCE_CODE, recurrence_factor_series),
            (0.18, NIGHT_CODE, night_share_series),
            (0.10, HEATING_CODE, heating_share_series),
        ),
    )
    data_gap_score_series = uncertainty_factor_series * 100.0

    return {
        "working_frame": working_frame,
        "normalized_selected_features": normalized_selected_features,
        "active_reason_codes": active_reason_codes,
        "normalized_factor_weights": normalized_factor_weights,
        "precomputed": {
            "incident_count": incident_count_series.tolist(),
            "years_observed": years_observed_series.tolist(),
            "incidents_per_year": incidents_per_year_series.tolist(),
            "average_distance": average_distance_series.tolist(),
            "average_response": average_response_series.tolist(),
            "long_arrival_share": long_arrival_share_series.tolist(),
            "no_water_share": no_water_share_series.tolist(),
            "water_coverage_share": water_coverage_share_series.tolist(),
            "water_unknown_share": water_unknown_share_series.tolist(),
            "severe_share": severe_share_series.tolist(),
            "night_share": night_share_series.tolist(),
            "heating_share": heating_share_series.tolist(),
            "rural_share": rural_share_series.tolist(),
            "response_coverage_share": response_coverage_share_series.tolist(),
            "distance_coverage_share": distance_coverage_share_series.tolist(),
            "arrival_missing_share": arrival_missing_share_series.tolist(),
            "distance_missing_share": distance_missing_share_series.tolist(),
            "completeness_share": completeness_share_series.tolist(),
            "distance_norm": distance_norm_series.tolist(),
            "response_norm": response_norm_series.tolist(),
            "support_weight": support_weight_series.tolist(),
            "severity_factor": severity_factor_series.tolist(),
            "recurrence_factor": recurrence_factor_series.tolist(),
            "uncertainty_factor": uncertainty_factor_series.tolist(),
            "access_score": access_score_series.tolist(),
            "water_score": water_score_series.tolist(),
            "severity_score": severity_score_series.tolist(),
            "recurrence_score": recurrence_score_series.tolist(),
            "data_gap_score": data_gap_score_series.tolist(),
        },
    }


def _frame_column_values(frame: pd.DataFrame) -> Dict[str, List[Any]]:
    return {column: frame[column].tolist() for column in frame.columns}


def _record_from_column_values(column_values: Dict[str, List[Any]], row_index: int) -> Dict[str, Any]:
    return {column: values[row_index] for column, values in column_values.items()}


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
    record_columns = _frame_column_values(working_frame)

    normalized_rows: List[Dict[str, Any]] = []
    for row_index in range(len(working_frame)):
        record = _record_from_column_values(record_columns, row_index)
        incident_count = int(precomputed["incident_count"][row_index])
        years_observed = int(precomputed["years_observed"][row_index])
        incidents_per_year = float(precomputed["incidents_per_year"][row_index])
        average_distance = _normalize_nullable_float(precomputed["average_distance"][row_index])
        average_response = _normalize_nullable_float(precomputed["average_response"][row_index])
        long_arrival_share = float(precomputed["long_arrival_share"][row_index])
        no_water_share = float(precomputed["no_water_share"][row_index])
        water_coverage_share = float(precomputed["water_coverage_share"][row_index])
        water_unknown_share = float(precomputed["water_unknown_share"][row_index])
        severe_share = float(precomputed["severe_share"][row_index])
        night_share = float(precomputed["night_share"][row_index])
        heating_share = float(precomputed["heating_share"][row_index])
        rural_share = float(precomputed["rural_share"][row_index])
        response_coverage_share = float(precomputed["response_coverage_share"][row_index])
        distance_coverage_share = float(precomputed["distance_coverage_share"][row_index])
        arrival_missing_share = float(precomputed["arrival_missing_share"][row_index])
        distance_missing_share = float(precomputed["distance_missing_share"][row_index])
        completeness_share = float(precomputed["completeness_share"][row_index])
        distance_norm = float(precomputed["distance_norm"][row_index])
        response_norm = float(precomputed["response_norm"][row_index])
        support_weight = float(precomputed["support_weight"][row_index])
        severity_factor = float(precomputed["severity_factor"][row_index])
        recurrence_factor = float(precomputed["recurrence_factor"][row_index])
        uncertainty_factor = float(precomputed["uncertainty_factor"][row_index])
        access_score = float(precomputed["access_score"][row_index])
        water_score = float(precomputed["water_score"][row_index])
        severity_score = float(precomputed["severity_score"][row_index])
        recurrence_score = float(precomputed["recurrence_score"][row_index])
        data_gap_score = float(precomputed["data_gap_score"][row_index])

        score_decomposition: List[Dict[str, Any]] = []
        for code, factor_score, factor_value, value_display in (
            (DISTANCE_CODE, distance_norm * 100.0, distance_norm, "н/д" if average_distance is None else f"{_format_number(average_distance)} км"),
            (RESPONSE_CODE, response_norm * 100.0, response_norm, "н/д" if average_response is None else f"{_format_number(average_response)} мин"),
            (LONG_ARRIVAL_CODE, long_arrival_share * 100.0, long_arrival_share, _format_percent(long_arrival_share * 100.0)),
            (WATER_CODE, no_water_share * 100.0, no_water_share, _format_percent(no_water_share * 100.0)),
            (SEVERITY_CODE, severity_factor * 100.0, severity_factor, _format_percent(severe_share * 100.0)),
            (RECURRENCE_CODE, recurrence_factor * 100.0, recurrence_factor, f"{_format_number(incidents_per_year)} в год"),
            (NIGHT_CODE, night_share * 100.0, night_share, _format_percent(night_share * 100.0)),
            (HEATING_CODE, heating_share * 100.0, heating_share, _format_percent(heating_share * 100.0)),
        ):
            if code not in active_reason_codes:
                continue
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
                value_display=f"полнота {_format_percent(completeness_share * 100.0)}",
                is_penalty=True,
            )
        )

        total_score = sum(float(item.get("contribution_points") or 0.0) for item in score_decomposition)
        uncertainty_penalty = next(
            (float(item.get("contribution_points") or 0.0) for item in score_decomposition if item.get("code") == UNCERTAINTY_CODE),
            0.0,
        )
        investigation_score = min(100.0, (0.72 * total_score) + (0.28 * (uncertainty_penalty * 100.0 / UNCERTAINTY_PENALTY_MAX)))
        uncertainty_flag = bool(uncertainty_penalty >= 2.5 or bool(record.get("low_support")) or completeness_share < 0.6)
        missing_data_priority = uncertainty_flag and total_score < HIGH_THRESHOLD

        latitude = _normalize_nullable_float(record.get("latitude"))
        longitude = _normalize_nullable_float(record.get("longitude"))
        row: Dict[str, Any] = {
            **record,
            "label": _clean_text(record.get("label")) or "Точка",
            "location_hint": _clean_text(record.get("location_hint")) or "Локация определена по лучшей доступной сущности",
            "incident_count": incident_count,
            "incident_count_display": _format_integer(incident_count),
            "years_observed": years_observed,
            "years_observed_display": _format_integer(years_observed),
            "incidents_per_year": round(incidents_per_year, 2),
            "incidents_per_year_display": _format_number(incidents_per_year),
            "average_distance_km": None if average_distance is None else round(float(average_distance), 2),
            "average_distance_display": "н/д" if average_distance is None else f"{_format_number(average_distance)} км",
            "average_response_minutes": None if average_response is None else round(float(average_response), 1),
            "average_response_display": "н/д" if average_response is None else f"{_format_number(average_response)} мин",
            "response_coverage_share": round(response_coverage_share, 4),
            "response_coverage_display": _format_percent(response_coverage_share * 100.0),
            "long_arrival_share": round(long_arrival_share, 4),
            "long_arrival_share_display": _format_percent(long_arrival_share * 100.0),
            "no_water_share": round(no_water_share, 4),
            "no_water_share_display": _format_percent(no_water_share * 100.0),
            "water_unknown_share": round(water_unknown_share, 4),
            "water_unknown_share_display": _format_percent(water_unknown_share * 100.0),
            "water_coverage_share": round(water_coverage_share, 4),
            "water_coverage_display": _format_percent(water_coverage_share * 100.0),
            "severe_share": round(severe_share, 4),
            "severe_share_display": _format_percent(severe_share * 100.0),
            "night_share": round(night_share, 4),
            "night_share_display": _format_percent(night_share * 100.0),
            "heating_share": round(heating_share, 4),
            "heating_share_display": _format_percent(heating_share * 100.0),
            "rural_share": round(rural_share, 4),
            "rural_share_display": _format_percent(rural_share * 100.0),
            "arrival_missing_share": round(arrival_missing_share, 4),
            "distance_missing_share": round(distance_missing_share, 4),
            "completeness_share": round(completeness_share, 4),
            "completeness_display": _format_percent(completeness_share * 100.0),
            "access_score": round(access_score, 1),
            "water_score": round(water_score, 1),
            "severity_score": round(severity_score, 1),
            "recurrence_score": round(recurrence_score, 1),
            "data_gap_score": round(data_gap_score, 1),
            "score": round(total_score, 1),
            "score_display": _format_number(total_score),
            "total_score": round(total_score, 1),
            "total_score_display": _format_number(total_score),
            "uncertainty_penalty": round(uncertainty_penalty, 2),
            "uncertainty_penalty_display": _format_number(uncertainty_penalty),
            "investigation_score": round(investigation_score, 1),
            "investigation_score_display": _format_number(investigation_score),
            "missing_data_priority": missing_data_priority,
            "uncertainty_flag": uncertainty_flag,
            "low_support": bool(record.get("low_support")),
            "low_support_note": (
                f"Точка собрана всего по {_format_integer(incident_count)} пожарам, долевые признаки сглажены."
                if bool(record.get("low_support"))
                else ""
            ),
            "selected_feature_columns": list(normalized_selected_features),
            "selected_feature_count": len(normalized_selected_features),
            "score_decomposition": score_decomposition,
            "latitude": latitude,
            "longitude": longitude,
            "coordinates_display": (
                f"{_format_coordinate(latitude)}, {_format_coordinate(longitude)}"
                if latitude is not None and longitude is not None
                else ""
            ),
        }
        row.update(_severity_band_descriptor(float(row["total_score"])))
        row["component_scores"] = _build_component_scores(row)
        row["typology_code"], row["typology_label"] = _typology_for_row(row)
        row["reason_details"] = _build_reason_details_from_decomposition(row)
        row["top_reason_codes"] = [item["code"] for item in row["reason_details"][:3]]
        row["reasons"] = [f"{item['label']}: {item['value_display']}" for item in row["reason_details"][:4]]
        row["reason_chips"] = [f"{item['label']}: {item['contribution_display']}" for item in row["reason_details"][:3]]
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
        normalized_rows.append(row)

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
        if float(row.get("total_score") or 0.0) >= float(bucket["max_score"]):
            bucket["max_score"] = float(row.get("total_score") or 0.0)
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

    scores = [float(row.get("total_score") or 0.0) for row in rows]
    bands = []
    for code, label in (("low", "Низкий"), ("medium", "Средний"), ("high", "Высокий"), ("critical", "Критический")):
        count = sum(1 for row in rows if str(row.get("severity_band_code") or "") == code)
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
    for start, end in ((0, 25), (25, 50), (50, 75), (75, 101)):
        count = sum(1 for score in scores if start <= score < end)
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

    low_support_count = sum(1 for row in rows if row.get("low_support"))
    uncertainty_count = sum(1 for row in rows if row.get("uncertainty_flag"))
    high_penalty_count = sum(1 for row in rows if float(row.get("uncertainty_penalty") or 0.0) >= 3.0)
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
