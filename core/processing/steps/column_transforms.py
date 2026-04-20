from __future__ import annotations

import ast
from typing import Any

import pandas as pd

from .column_definitions import (
    PROTECTED_REPORT_COLUMNS,
    PROTECTION_REPORT_DEFAULTS,
    PROTECTION_TEXT_COLUMNS,
)
from ...types import ColumnMatchMetadata, ProtectedColumnInfo

_PROTECTED_BOOL_COLUMNS: frozenset[str] = frozenset({
    "profiling_candidate_to_drop",
    "candidate_to_drop",
    "mandatory_feature_detected",
    "protected_from_drop",
})


def coerce_bool_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    normalized = series.astype(str).str.strip().str.lower()
    return normalized.isin(["true", "1", "yes"])


def coerce_text_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").astype(object)


def _coerce_list_value(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            try:
                parsed = ast.literal_eval(text)
                if isinstance(parsed, list):
                    return parsed
            except (ValueError, SyntaxError):
                pass
    return []


def coerce_list_series(series: pd.Series) -> pd.Series:
    return series.apply(_coerce_list_value).astype(object)


def ensure_report_columns(profile_df: pd.DataFrame) -> pd.DataFrame:
    for column_name, default_value in PROTECTION_REPORT_DEFAULTS.items():
        if column_name not in profile_df.columns:
            profile_df[column_name] = default_value
    for column_name in PROTECTION_TEXT_COLUMNS:
        if column_name in profile_df.columns:
            profile_df[column_name] = coerce_text_series(profile_df[column_name])
    if "drop_reasons" in profile_df.columns:
        profile_df["drop_reasons"] = coerce_list_series(profile_df["drop_reasons"])
    return profile_df


def coerce_report_bool_columns(profile_df: pd.DataFrame) -> pd.DataFrame:
    for column_name in (
        "candidate_to_drop",
        "profiling_candidate_to_drop",
        "mandatory_feature_detected",
        "protected_from_drop",
    ):
        profile_df[column_name] = coerce_bool_series(profile_df[column_name])
    return profile_df


def _match_to_row(match: ColumnMatchMetadata | None) -> dict[str, object]:
    m = match or {}
    return {
        "has_match": bool(match),
        "mandatory": bool(m.get("mandatory")),
        "feature_id": str(m.get("feature_id") or ""),
        "feature_label": str(m.get("feature_label") or ""),
        "scope": str(m.get("scope") or ""),
        "rule_id": str(m.get("rule_id") or ""),
        "matched_value": str(m.get("matched_value") or ""),
        "reason": str(m.get("reason") or ""),
    }


def apply_match_results(
    profile_df: pd.DataFrame,
    column_names: pd.Series,
    matches: list[ColumnMatchMetadata | None],
) -> list[ProtectedColumnInfo]:
    match_df = pd.DataFrame(
        [_match_to_row(match) for match in matches],
        index=profile_df.index,
    )
    match_mask = match_df["has_match"]

    profile_df.loc[
        match_mask,
        [
            "mandatory_feature_detected",
            "protected_feature_id",
            "protected_feature_label",
            "protection_scope",
            "protection_rule",
            "protection_match",
            "protection_reason",
        ],
    ] = match_df.loc[
        match_mask,
        ["mandatory", "feature_id", "feature_label", "scope", "rule_id", "matched_value", "reason"],
    ].to_numpy()

    protected_mask = match_mask & profile_df["profiling_candidate_to_drop"]
    profile_df.loc[protected_mask, "candidate_to_drop"] = False
    profile_df.loc[protected_mask, "protected_from_drop"] = True

    protected_columns: list[ProtectedColumnInfo] = []
    if protected_mask.any():
        has_drop_reasons = "drop_reasons" in profile_df.columns
        protected_idx = profile_df.index[protected_mask]
        names_sub = column_names.loc[protected_idx]
        match_sub = match_df.loc[protected_idx]
        drop_sub = profile_df.loc[protected_idx, "drop_reasons"] if has_drop_reasons else None
        protected_columns = [
            {
                "column": str(names_sub.at[idx]),
                "protected_feature_id": str(match_sub.at[idx, "feature_id"]),
                "protected_feature_label": str(match_sub.at[idx, "feature_label"]),
                "mandatory_feature_detected": bool(match_sub.at[idx, "mandatory"]),
                "protection_scope": str(match_sub.at[idx, "scope"]),
                "protection_rule": str(match_sub.at[idx, "rule_id"]),
                "protection_match": str(match_sub.at[idx, "matched_value"]),
                "protection_reason": str(match_sub.at[idx, "reason"]),
                "drop_reasons": drop_sub.at[idx] if drop_sub is not None else [],
            }
            for idx in protected_idx
        ]
    return protected_columns


def build_protected_report(profile_df: pd.DataFrame) -> pd.DataFrame:
    protected_df = profile_df.loc[profile_df["protected_from_drop"]].copy()
    for column_name in PROTECTED_REPORT_COLUMNS:
        if column_name not in protected_df.columns:
            default = False if column_name in _PROTECTED_BOOL_COLUMNS else ""
            protected_df[column_name] = default
    return protected_df[PROTECTED_REPORT_COLUMNS].sort_values(
        by=["mandatory_feature_detected", "protected_feature_label", "column"],
        ascending=[False, True, True],
    )
