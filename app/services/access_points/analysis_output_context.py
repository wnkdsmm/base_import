from __future__ import annotations

from typing import Any, Dict, List, Sequence

from app.services.shared.formatting import _format_integer, _format_number

from .analysis_factors import (
    DISTANCE_CODE,
    HEATING_CODE,
    LONG_ARRIVAL_CODE,
    NIGHT_CODE,
    RECURRENCE_CODE,
    RESPONSE_CODE,
    SEVERITY_CODE,
    UNCERTAINTY_CODE,
    UNCERTAINTY_PENALTY_MAX,
    WATER_CODE,
)
from .analysis_output_types import (
    _AccessPointDisplayContext,
    _AccessPointReasonContext,
    _AccessPointReasonListContext,
    _AccessPointRowMetrics,
    _build_human_readable_explanation,
    _build_reason_details,
    _distance_value_display,
    _format_coordinate,
    _make_decomposition_item,
    _response_value_display,
    _share_value_display,
)

def _nullable_precomputed_float(value: Any) -> float | None:
    return None if value is None else float(value)

def _coordinates_display(latitude: float | None, longitude: float | None) -> str:
    if latitude is None or longitude is None:
        return ""
    return f"{_format_coordinate(latitude)}, {_format_coordinate(longitude)}"

def _build_reason_list_context(reason_details: Sequence[dict[str, Any]]) -> _AccessPointReasonListContext:
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
    score_decomposition: Sequence[dict[str, Any]],
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
) -> List[dict[str, Any]]:
    score_decomposition: List[dict[str, Any]] = []
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

def _score_total_and_uncertainty_penalty(score_decomposition: Sequence[dict[str, Any]]) -> tuple[float, float]:
    total_score = 0.0
    uncertainty_penalty: float | None = None
    for item in score_decomposition:
        contribution_points = float(item.get("contribution_points") or 0.0)
        total_score += contribution_points
        if uncertainty_penalty is None and item.get("code") == UNCERTAINTY_CODE:
            uncertainty_penalty = contribution_points
    return total_score, 0.0 if uncertainty_penalty is None else uncertainty_penalty
