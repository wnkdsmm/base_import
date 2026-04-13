from __future__ import annotations

from typing import Any, Dict, List, Sequence

import pandas as pd

from .analysis_factors import (
    WATCH_RISK_THRESHOLD,
    _frame_column_values,
    _prepare_access_point_row_context,
    _record_from_column_values,
)
from .analysis_output import ACCESS_POINT_PAYLOAD_OVERWRITE_COLUMNS, _build_access_point_payload_row
from .constants import MAX_INCOMPLETE_POINTS, TOP_POINT_CARD_COUNT
from .point_data import _build_point_entity_frames


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
