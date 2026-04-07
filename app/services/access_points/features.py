from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

import pandas as pd

from app.services.forecast_risk.utils import _format_percent

from .constants import (
    ACCESS_POINT_FEATURE_METADATA,
    DEFAULT_ACCESS_POINT_FEATURES,
    MAX_ACCESS_POINT_FEATURE_OPTIONS,
)


def _normalize_access_point_feature_columns(feature_columns: Sequence[str] | None) -> List[str]:
    return [str(item).strip() for item in (feature_columns or []) if str(item).strip()]


def _build_access_point_shell_feature_options(selected_features: Sequence[str] | None = None) -> List[Dict[str, Any]]:
    selected_set = set(_normalize_access_point_feature_columns(selected_features)) or set(DEFAULT_ACCESS_POINT_FEATURES)
    rows: List[Dict[str, Any]] = []
    for feature_name in DEFAULT_ACCESS_POINT_FEATURES:
        metadata = ACCESS_POINT_FEATURE_METADATA.get(feature_name, {})
        rows.append(
            {
                "name": feature_name,
                "label": str(metadata.get("label") or feature_name),
                "description": str(metadata.get("description") or ""),
                "coverage_display": "н/д",
                "variance_display": "н/д",
                "is_selected": feature_name in selected_set,
            }
        )
    return rows


def _access_point_feature_series(entity_frame: pd.DataFrame, feature_name: str) -> pd.Series:
    if entity_frame is None or entity_frame.empty:
        return pd.Series(dtype=float)

    if feature_name == "DISTANCE_TO_STATION":
        return pd.to_numeric(entity_frame.get("average_distance_km"), errors="coerce")
    if feature_name == "RESPONSE_TIME":
        return pd.to_numeric(entity_frame.get("average_response_minutes"), errors="coerce")
    if feature_name == "LONG_ARRIVAL_SHARE":
        return pd.to_numeric(entity_frame.get("long_arrival_share"), errors="coerce")
    if feature_name == "NO_WATER":
        return pd.to_numeric(entity_frame.get("no_water_share"), errors="coerce")
    if feature_name == "SEVERE_CONSEQUENCES":
        severe_share = pd.to_numeric(entity_frame.get("severe_share"), errors="coerce").fillna(0.0)
        victim_share = pd.to_numeric(entity_frame.get("victim_share"), errors="coerce").fillna(0.0)
        major_damage_share = pd.to_numeric(entity_frame.get("major_damage_share"), errors="coerce").fillna(0.0)
        return (0.58 * severe_share) + (0.24 * victim_share) + (0.18 * major_damage_share)
    if feature_name == "REPEAT_FIRES":
        return pd.to_numeric(entity_frame.get("incidents_per_year"), errors="coerce")
    if feature_name == "NIGHT_PROFILE":
        return pd.to_numeric(entity_frame.get("night_share"), errors="coerce")
    if feature_name == "HEATING_SEASON":
        return pd.to_numeric(entity_frame.get("heating_share"), errors="coerce")
    return pd.Series(dtype=float)


def _build_access_point_candidate_features(entity_frame: pd.DataFrame) -> List[Dict[str, Any]]:
    if entity_frame is None or entity_frame.empty:
        return []

    row_count = len(entity_frame)
    rows: List[Dict[str, Any]] = []
    for order, feature_name in enumerate(DEFAULT_ACCESS_POINT_FEATURES):
        series = _access_point_feature_series(entity_frame, feature_name)
        metadata = ACCESS_POINT_FEATURE_METADATA.get(feature_name, {})
        non_null_count = int(series.notna().sum())
        unique_count = int(series.nunique(dropna=True))
        coverage = (non_null_count / row_count) if row_count else 0.0
        variance = float(series.var(skipna=True) or 0.0)
        rows.append(
            {
                "name": feature_name,
                "label": str(metadata.get("label") or feature_name),
                "description": str(metadata.get("description") or ""),
                "coverage": coverage,
                "coverage_display": _format_percent(coverage * 100.0),
                "variance": variance,
                "variance_display": f"{variance:.3f}".replace(".", ","),
                "unique_count": unique_count,
                "score": (1000.0 if feature_name in DEFAULT_ACCESS_POINT_FEATURES else 0.0) + (coverage * 100.0) - order,
            }
        )
    return sorted(rows, key=lambda item: (item["score"], item["coverage"], item["variance"]), reverse=True)


def _resolve_selected_access_point_features(
    available_features: Sequence[str],
    requested_features: Sequence[str],
) -> Tuple[List[str], str]:
    allowed = set(available_features)
    normalized_requested = [item for item in requested_features if item in allowed]
    if normalized_requested:
        return normalized_requested, ""

    fallback = [item for item in DEFAULT_ACCESS_POINT_FEATURES if item in allowed]
    if not fallback:
        fallback = list(available_features[: max(1, min(len(available_features), 6))])

    if requested_features:
        return (
            fallback,
            "Часть выбранных признаков недоступна для scoring, поэтому блок вернулся к базовому набору explainable-факторов.",
        )
    return (
        fallback,
        "По умолчанию выбраны базовые признаки access-risk score: доступность ПЧ, вода, последствия, повторяемость и сезонный контекст.",
    )


def _build_access_point_feature_options(
    candidate_features: Sequence[Dict[str, Any]],
    selected_features: Sequence[str],
) -> List[Dict[str, Any]]:
    selected_set = set(selected_features)
    prioritized = list(candidate_features[:MAX_ACCESS_POINT_FEATURE_OPTIONS])
    selected_rows = [item for item in candidate_features if item["name"] in selected_set and item not in prioritized]
    rows = prioritized + selected_rows
    return [
        {
            "name": item["name"],
            "label": item["label"],
            "description": item.get("description", ""),
            "coverage_display": item["coverage_display"],
            "variance_display": item["variance_display"],
            "is_selected": item["name"] in selected_set,
        }
        for item in rows
    ]
