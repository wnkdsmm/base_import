from __future__ import annotations

from statistics import mean, median
from typing import Any, Dict, List, NamedTuple, Sequence

from app.services.shared.data_utils import _clean_text
from app.services.shared.formatting import _format_integer, _format_number, _format_percent

from .analysis_factors import (
    CRITICAL_THRESHOLD,
    DISTANCE_CODE,
    HEATING_CODE,
    HIGH_THRESHOLD,
    LONG_ARRIVAL_CODE,
    MEDIUM_THRESHOLD,
    NIGHT_CODE,
    RECURRENCE_CODE,
    RESPONSE_CODE,
    SEVERITY_CODE,
    UNCERTAINTY_CODE,
    UNCERTAINTY_PENALTY_MAX,
    WATER_CODE,
)
from .numeric import _share

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


def _format_coordinate(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.4f}".replace(".", ",")


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


def _build_human_readable_explanation(
    *,
    label: str,
    severity_band: str,
    total_score_display: str,
    reason_details: Sequence[Dict[str, Any]],
    uncertainty_flag: bool,
    low_support: bool,
    uncertainty_penalty_display: str,
) -> str:
    details = list(reason_details)
    if not details:
        return "Точка включена в рейтинг по сумме факторов риска."

    lead = f"{label or 'Точка'} получает {severity_band or 'средний'} риск {total_score_display or '0'} из 100."
    drivers = [item for item in details if item["code"] != UNCERTAINTY_CODE][:2]
    if drivers:
        lead += " Основной вклад дали " + ", ".join(
            f"{item['label'].lower()} ({item['contribution_display']})" for item in drivers
        ) + "."
    if uncertainty_flag:
        lead += f" Неопределённость добавляет {uncertainty_penalty_display or '0'} п. и требует верификации."
    elif low_support:
        lead += " Точка низкой опоры: долевые признаки сглажены, а итоговый score ослаблен."
    return lead


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
    total_score_payload: float
    total_score_display: str
    uncertainty_penalty: float
    uncertainty_penalty_payload: float
    uncertainty_penalty_display: str
    investigation_score: float
    investigation_score_payload: float
    investigation_score_display: str
    access_score_payload: float
    water_score_payload: float
    severity_score_payload: float
    recurrence_score_payload: float
    data_gap_score_payload: float
    component_scores: List[Dict[str, Any]]


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


class _AccessPointReasonContext(NamedTuple):
    reason_details: List[Dict[str, Any]]
    reason_lists: _AccessPointReasonListContext
    explanation: str


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


def _build_access_point_reason_context(
    *,
    label: str,
    severity_band: str,
    total_score_display: str,
    score_decomposition: Sequence[Dict[str, Any]],
    uncertainty_flag: bool,
    low_support: bool,
    uncertainty_penalty_display: str,
) -> _AccessPointReasonContext:
    reason_details = _build_reason_details(score_decomposition)
    return _AccessPointReasonContext(
        reason_details=reason_details,
        reason_lists=_build_reason_list_context(reason_details),
        explanation=_build_human_readable_explanation(
            label=label,
            severity_band=severity_band,
            total_score_display=total_score_display,
            reason_details=reason_details,
            uncertainty_flag=uncertainty_flag,
            low_support=low_support,
            uncertainty_penalty_display=uncertainty_penalty_display,
        ),
    )


def _build_low_support_note(*, low_support: bool, incident_count: int) -> str:
    if not low_support:
        return ""
    return (
        f"Точка собрана всего по {_format_integer(incident_count)} пожарам, "
        "долевые признаки сглажены."
    )


def _build_incomplete_note(
    *,
    low_support_note: str,
    missing_data_priority: bool,
    uncertainty_flag: bool,
    uncertainty_penalty_display: str,
) -> str:
    if uncertainty_flag:
        return (
            f"Неопределённость добавляет {uncertainty_penalty_display} п. "
            "и требует верификации воды, времени прибытия и дистанции."
        )
    if missing_data_priority:
        return (
            "Высокий приоритет проверки связан прежде всего с "
            "пропусками по доступности, воде или времени прибытия."
        )
    return low_support_note


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
    metrics: _AccessPointRowMetrics,
    displays: _AccessPointDisplayContext,
    *,
    active_reason_codes: set[str],
) -> List[tuple[str, float, float, str]]:
    components: List[tuple[str, float, float, str]] = []
    if DISTANCE_CODE in active_reason_codes:
        components.append((DISTANCE_CODE, metrics.distance_norm * 100.0, metrics.distance_norm, displays.average_distance_display))
    if RESPONSE_CODE in active_reason_codes:
        components.append((RESPONSE_CODE, metrics.response_norm * 100.0, metrics.response_norm, displays.average_response_display))
    if LONG_ARRIVAL_CODE in active_reason_codes:
        components.append((LONG_ARRIVAL_CODE, metrics.long_arrival_share * 100.0, metrics.long_arrival_share, displays.long_arrival_share_display))
    if WATER_CODE in active_reason_codes:
        components.append((WATER_CODE, metrics.no_water_share * 100.0, metrics.no_water_share, displays.no_water_share_display))
    if SEVERITY_CODE in active_reason_codes:
        components.append((SEVERITY_CODE, metrics.severity_factor * 100.0, metrics.severity_factor, displays.severe_share_display))
    if RECURRENCE_CODE in active_reason_codes:
        components.append((RECURRENCE_CODE, metrics.recurrence_factor * 100.0, metrics.recurrence_factor, f"{_format_number(metrics.incidents_per_year)} в год"))
    if NIGHT_CODE in active_reason_codes:
        components.append((NIGHT_CODE, metrics.night_share * 100.0, metrics.night_share, displays.night_share_display))
    if HEATING_CODE in active_reason_codes:
        components.append((HEATING_CODE, metrics.heating_share * 100.0, metrics.heating_share, displays.heating_share_display))
    return components


def _build_access_point_score_decomposition(
    metrics: _AccessPointRowMetrics,
    displays: _AccessPointDisplayContext,
    *,
    active_reason_codes: set[str],
    normalized_factor_weights: Dict[str, float],
) -> List[Dict[str, Any]]:
    score_decomposition: List[Dict[str, Any]] = []
    for code, factor_score, factor_value, value_display in _access_point_decomposition_components(
        metrics,
        displays,
        active_reason_codes=active_reason_codes,
    ):
        score_decomposition.append(
            _make_decomposition_item(
                code=code,
                factor_score=factor_score,
                weight_points=normalized_factor_weights[code],
                contribution_points=normalized_factor_weights[code] * factor_value * metrics.support_weight,
                value_display=value_display,
            )
        )
    score_decomposition.append(
        _make_decomposition_item(
            code=UNCERTAINTY_CODE,
            factor_score=metrics.uncertainty_factor * 100.0,
            weight_points=UNCERTAINTY_PENALTY_MAX,
            contribution_points=UNCERTAINTY_PENALTY_MAX * metrics.uncertainty_factor,
            value_display=f"полнота {displays.completeness_display}",
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


def _build_access_point_score_context(
    metrics: _AccessPointRowMetrics,
    displays: _AccessPointDisplayContext,
    *,
    active_reason_codes: set[str],
    normalized_factor_weights: Dict[str, float],
) -> _AccessPointScoreContext:
    score_decomposition = _build_access_point_score_decomposition(
        metrics,
        displays,
        active_reason_codes=active_reason_codes,
        normalized_factor_weights=normalized_factor_weights,
    )
    total_score, uncertainty_penalty = _score_total_and_uncertainty_penalty(score_decomposition)
    investigation_score = min(
        100.0,
        (0.72 * total_score) + (0.28 * (uncertainty_penalty * 100.0 / UNCERTAINTY_PENALTY_MAX)),
    )
    total_score_payload = round(total_score, 1)
    uncertainty_penalty_payload = round(uncertainty_penalty, 2)
    investigation_score_payload = round(investigation_score, 1)
    access_score_payload = round(metrics.access_score, 1)
    water_score_payload = round(metrics.water_score, 1)
    severity_score_payload = round(metrics.severity_score, 1)
    recurrence_score_payload = round(metrics.recurrence_score, 1)
    data_gap_score_payload = round(metrics.data_gap_score, 1)
    return _AccessPointScoreContext(
        score_decomposition=score_decomposition,
        total_score=total_score,
        total_score_payload=total_score_payload,
        total_score_display=_format_number(total_score),
        uncertainty_penalty=uncertainty_penalty,
        uncertainty_penalty_payload=uncertainty_penalty_payload,
        uncertainty_penalty_display=_format_number(uncertainty_penalty),
        investigation_score=investigation_score,
        investigation_score_payload=investigation_score_payload,
        investigation_score_display=_format_number(investigation_score),
        access_score_payload=access_score_payload,
        water_score_payload=water_score_payload,
        severity_score_payload=severity_score_payload,
        recurrence_score_payload=recurrence_score_payload,
        data_gap_score_payload=data_gap_score_payload,
        component_scores=_build_component_scores_from_values(
            access_score=access_score_payload,
            water_score=water_score_payload,
            severity_score=severity_score_payload,
            recurrence_score=recurrence_score_payload,
            data_gap_score=data_gap_score_payload,
        ),
    )


def _build_access_point_metric_payload(
    metrics: _AccessPointRowMetrics,
    displays: _AccessPointDisplayContext,
) -> Dict[str, Any]:
    return {
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
        "latitude": metrics.latitude,
        "longitude": metrics.longitude,
        "coordinates_display": _coordinates_display(metrics.latitude, metrics.longitude),
    }


def _build_access_point_score_payload(
    score_context: _AccessPointScoreContext,
    *,
    normalized_selected_features: Sequence[str],
    missing_data_priority: bool,
    uncertainty_flag: bool,
    low_support: bool,
    low_support_note: str,
) -> Dict[str, Any]:
    return {
        "access_score": score_context.access_score_payload,
        "water_score": score_context.water_score_payload,
        "severity_score": score_context.severity_score_payload,
        "recurrence_score": score_context.recurrence_score_payload,
        "data_gap_score": score_context.data_gap_score_payload,
        "score": score_context.total_score_payload,
        "score_display": score_context.total_score_display,
        "total_score": score_context.total_score_payload,
        "total_score_display": score_context.total_score_display,
        "uncertainty_penalty": score_context.uncertainty_penalty_payload,
        "uncertainty_penalty_display": score_context.uncertainty_penalty_display,
        "investigation_score": score_context.investigation_score_payload,
        "investigation_score_display": score_context.investigation_score_display,
        "missing_data_priority": missing_data_priority,
        "uncertainty_flag": uncertainty_flag,
        "low_support": low_support,
        "low_support_note": low_support_note,
        "selected_feature_columns": list(normalized_selected_features),
        "selected_feature_count": len(normalized_selected_features),
        "score_decomposition": score_context.score_decomposition,
    }


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
    displays = _build_access_point_display_context(metrics)
    score_context = _build_access_point_score_context(
        metrics,
        displays,
        active_reason_codes=active_reason_codes,
        normalized_factor_weights=normalized_factor_weights,
    )
    low_support = bool(record.get("low_support"))
    uncertainty_flag = bool(
        score_context.uncertainty_penalty >= 2.5
        or low_support
        or metrics.completeness_share < 0.6
    )
    missing_data_priority = uncertainty_flag and score_context.total_score < HIGH_THRESHOLD
    label = _clean_text(record.get("label")) or "Точка"
    location_hint = _clean_text(record.get("location_hint")) or "Локация определена по лучшей доступной сущности"
    low_support_note = _build_low_support_note(
        low_support=low_support,
        incident_count=metrics.incident_count,
    )
    severity_descriptor = _severity_band_descriptor(score_context.total_score_payload)
    row: Dict[str, Any] = record
    row.update({"label": label, "location_hint": location_hint})
    row.update(_build_access_point_metric_payload(metrics, displays))
    row.update(
        _build_access_point_score_payload(
            score_context,
            normalized_selected_features=normalized_selected_features,
            missing_data_priority=missing_data_priority,
            uncertainty_flag=uncertainty_flag,
            low_support=low_support,
            low_support_note=low_support_note,
        )
    )
    row.update(severity_descriptor)
    row["component_scores"] = score_context.component_scores
    row["typology_code"], row["typology_label"] = _typology_for_score_values(
        uncertainty_flag=uncertainty_flag,
        severity_band_code=severity_descriptor["severity_band_code"],
        access_score=score_context.access_score_payload,
        water_score=score_context.water_score_payload,
        severity_score=score_context.severity_score_payload,
        recurrence_score=score_context.recurrence_score_payload,
    )
    reason_context = _build_access_point_reason_context(
        label=label,
        severity_band=str(severity_descriptor["severity_band"]),
        total_score_display=score_context.total_score_display,
        score_decomposition=score_context.score_decomposition,
        uncertainty_flag=uncertainty_flag,
        low_support=low_support,
        uncertainty_penalty_display=score_context.uncertainty_penalty_display,
    )
    row["reason_details"] = reason_context.reason_details
    row["top_reason_codes"] = reason_context.reason_lists.top_reason_codes
    row["reasons"] = reason_context.reason_lists.reasons
    row["reason_chips"] = reason_context.reason_lists.reason_chips
    row["human_readable_explanation"] = reason_context.explanation
    row["explanation"] = reason_context.explanation
    row["incomplete_note"] = _build_incomplete_note(
        low_support_note=low_support_note,
        missing_data_priority=missing_data_priority,
        uncertainty_flag=uncertainty_flag,
        uncertainty_penalty_display=score_context.uncertainty_penalty_display,
    )
    return row


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
