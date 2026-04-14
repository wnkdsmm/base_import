from __future__ import annotations

from statistics import mean, median
from typing import Any, Sequence

from app.services.shared.data_utils import _clean_text
from app.services.shared.formatting import _format_integer, _format_number, _format_percent
from .types import (
    AccessPointPayloadRow,
    AccessPointReasonBreakdownRow,
    AccessPointScoreDistribution,
    AccessPointTypologyRow,
)

from .analysis_factors import HIGH_THRESHOLD, UNCERTAINTY_PENALTY_MAX
from .analysis_output_context import (
    _access_point_row_metrics,
    _build_access_point_display_context,
    _build_access_point_reason_context,
    _build_access_point_score_decomposition,
    _build_incomplete_note,
    _build_low_support_note,
    _coordinates_display,
    _score_total_and_uncertainty_penalty,
)
from .analysis_output_types import (
    _AccessPointDisplayContext,
    _AccessPointRowMetrics,
    _AccessPointScoreContext,
    _build_component_scores_from_values,
    _severity_band_descriptor,
    _typology_for_score_values,
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

def _build_access_point_score_context(
    metrics: _AccessPointRowMetrics,
    displays: _AccessPointDisplayContext,
    *,
    active_reason_codes: set[str],
    normalized_factor_weights: dict[str, float],
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
) -> AccessPointPayloadRow:
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
) -> AccessPointPayloadRow:
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
    record: AccessPointPayloadRow,
    precomputed: dict[str, Sequence[Any]],
    row_index: int,
    normalized_selected_features: Sequence[str],
    active_reason_codes: set[str],
    normalized_factor_weights: dict[str, float],
) -> AccessPointPayloadRow:
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
    label = _clean_text(record.get("label")) or "РўРѕС‡РєР°"
    location_hint = _clean_text(record.get("location_hint")) or "Р›РѕРєР°С†РёСЏ РѕРїСЂРµРґРµР»РµРЅР° РїРѕ Р»СѓС‡С€РµР№ РґРѕСЃС‚СѓРїРЅРѕР№ СЃСѓС‰РЅРѕСЃС‚Рё"
    low_support_note = _build_low_support_note(
        low_support=low_support,
        incident_count=metrics.incident_count,
    )
    severity_descriptor = _severity_band_descriptor(score_context.total_score_payload)
    row: AccessPointPayloadRow = record
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

def _build_typology_rows(rows: Sequence[AccessPointPayloadRow]) -> list[AccessPointTypologyRow]:
    if not rows:
        return []

    grouped: dict[str, AccessPointTypologyRow] = {}
    for row in rows:
        code = str(row.get("typology_code") or "mixed")
        total_score = float(row.get("total_score") or 0.0)
        bucket = grouped.setdefault(
            code,
            {
                "code": code,
                "label": row.get("typology_label") or "РљРѕРјР±РёРЅРёСЂРѕРІР°РЅРЅС‹Р№ СЂРёСЃРє",
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

def _build_score_distribution(rows: Sequence[AccessPointPayloadRow]) -> AccessPointScoreDistribution:
    if not rows:
        return {"average_score_display": "0", "median_score_display": "0", "bands": [], "buckets": []}

    scores: list[float] = []
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
    for code, label in (("low", "РќРёР·РєРёР№"), ("medium", "РЎСЂРµРґРЅРёР№"), ("high", "Р’С‹СЃРѕРєРёР№"), ("critical", "РљСЂРёС‚РёС‡РµСЃРєРёР№")):
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

def _build_reason_breakdown(rows: Sequence[AccessPointPayloadRow]) -> list[AccessPointReasonBreakdownRow]:
    buckets: dict[str, AccessPointReasonBreakdownRow] = {}
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

def _build_uncertainty_notes(rows: Sequence[AccessPointPayloadRow]) -> list[str]:
    if not rows:
        return ["РќРµРѕРїСЂРµРґРµР»С‘РЅРЅРѕСЃС‚СЊ Р±СѓРґРµС‚ РѕС†РµРЅРµРЅР° РїРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р° СЂРµР№С‚РёРЅРіР°."]

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
        "РќРµРїРѕР»РЅРѕС‚Р° РґР°РЅРЅС‹С… РґР°С‘С‚ РѕС‚РґРµР»СЊРЅС‹Р№ penalty, РЅРѕ РЅРµ СЏРІР»СЏРµС‚СЃСЏ РіР»Р°РІРЅС‹Рј РґСЂР°Р№РІРµСЂРѕРј СЂРёСЃРєР°: РµС‘ РІРєР»Р°Рґ РѕРіСЂР°РЅРёС‡РµРЅ 6 Р±Р°Р»Р»Р°РјРё.",
    ]
    if low_support_count:
        notes.append(
            f"Р”Р»СЏ {_format_integer(low_support_count)} С‚РѕС‡РµРє РѕРїРѕСЂР° РЅРёР¶Рµ РјРёРЅРёРјР°Р»СЊРЅРѕРіРѕ РїРѕСЂРѕРіР°, РїРѕСЌС‚РѕРјСѓ РґРѕР»РµРІС‹Рµ РїСЂРёР·РЅР°РєРё СЃРіР»Р°Р¶РµРЅС‹, Р° РёС‚РѕРіРѕРІС‹Р№ СЂРёСЃРє РѕСЃР»Р°Р±Р»РµРЅ РєРѕСЌС„С„РёС†РёРµРЅС‚РѕРј РїРѕРґРґРµСЂР¶РєРё."
        )
    if uncertainty_count:
        notes.append(
            f"РЈ {_format_integer(uncertainty_count)} С‚РѕС‡РµРє РµСЃС‚СЊ uncertainty flag РёР·-Р·Р° РїСЂРѕРїСѓСЃРєРѕРІ РїРѕ РІРѕРґРµ, РІСЂРµРјРµРЅРё РїСЂРёР±С‹С‚РёСЏ, РґРёСЃС‚Р°РЅС†РёРё РґРѕ РџР§ РёР»Рё РјР°Р»РѕРіРѕ С‡РёСЃР»Р° РїРѕР¶Р°СЂРѕРІ."
        )
    if high_penalty_count:
        notes.append(
            f"РЈ {_format_integer(high_penalty_count)} С‚РѕС‡РµРє penalty Р·Р° РЅРµРїРѕР»РЅРѕС‚Сѓ РґР°РЅРЅС‹С… СѓР¶Рµ Р·Р°РјРµС‚РµРЅ Рё С‚СЂРµР±СѓРµС‚ СѓРїСЂР°РІР»РµРЅС‡РµСЃРєРѕР№ РІРµСЂРёС„РёРєР°С†РёРё."
        )
    return notes[:4]
