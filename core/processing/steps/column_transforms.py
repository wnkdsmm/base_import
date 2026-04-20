from __future__ import annotations

import ast
from typing import Any, Dict, List, Optional

import pandas as pd

from .column_definitions import (
    PROTECTED_REPORT_COLUMNS,
    PROTECTION_REPORT_DEFAULTS,
    PROTECTION_TEXT_COLUMNS,
)
from ...types import ColumnMatchMetadata, ProtectedColumnInfo


def coerce_bool_series(series: pd.Series) -> pd.Series:
    if str(series.dtype) == "bool":
        return series.fillna(False)
    normalized = series.astype(str).str.strip().str.lower()
    return normalized.isin(["true", "1", "yes"])


def coerce_text_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").astype(object)


def coerce_list_series(series: pd.Series) -> pd.Series:
    def _coerce_value(value: Any) -> List[Any]:
        if isinstance(value, list):
            return value
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return []
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            if text.startswith("["):
                try:
                    parsed = ast.literal_eval(text)
                except (ValueError, SyntaxError):
                    return []
                if isinstance(parsed, list):
                    return parsed
            return []
        return []

    return series.apply(_coerce_value).astype(object)


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


def apply_match_results(
    profile_df: pd.DataFrame,
    column_names: pd.Series,
    matches: List[Optional[ColumnMatchMetadata]],
) -> List[ProtectedColumnInfo]:
    match_df = pd.DataFrame(
        [
            {
                "has_match": bool(match),
                "mandatory": bool((match or {}).get("mandatory")),
                "feature_id": str((match or {}).get("feature_id") or ""),
                "feature_label": str((match or {}).get("feature_label") or ""),
                "scope": str((match or {}).get("scope") or ""),
                "rule_id": str((match or {}).get("rule_id") or ""),
                "matched_value": str((match or {}).get("matched_value") or ""),
                "reason": str((match or {}).get("reason") or ""),
            }
            for match in matches
        ],
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

    protected_export = pd.DataFrame(index=profile_df.index)
    protected_export["column"] = column_names.astype(object)
    protected_export["protected_feature_id"] = match_df["feature_id"].astype(object)
    protected_export["protected_feature_label"] = match_df["feature_label"].astype(object)
    protected_export["mandatory_feature_detected"] = match_df["mandatory"].astype(bool)
    protected_export["protection_scope"] = match_df["scope"].astype(object)
    protected_export["protection_rule"] = match_df["rule_id"].astype(object)
    protected_export["protection_match"] = match_df["matched_value"].astype(object)
    protected_export["protection_reason"] = match_df["reason"].astype(object)
    protected_export["drop_reasons"] = (
        profile_df["drop_reasons"]
        if "drop_reasons" in profile_df.columns
        else [[] for _ in range(len(profile_df))]
    )
    protected_columns: List[ProtectedColumnInfo] = protected_export.loc[
        protected_mask,
        [
            "column",
            "protected_feature_id",
            "protected_feature_label",
            "mandatory_feature_detected",
            "protection_scope",
            "protection_rule",
            "protection_match",
            "protection_reason",
            "drop_reasons",
        ],
    ].to_dict(orient="records")
    return protected_columns


def build_protected_report(profile_df: pd.DataFrame) -> pd.DataFrame:
    protected_df = profile_df.loc[profile_df["protected_from_drop"]].copy()
    for column_name in PROTECTED_REPORT_COLUMNS:
        if column_name not in protected_df.columns:
            protected_df[column_name] = (
                ""
                if column_name
                not in {
                    "profiling_candidate_to_drop",
                    "candidate_to_drop",
                    "mandatory_feature_detected",
                    "protected_from_drop",
                }
                else False
            )
    return protected_df[PROTECTED_REPORT_COLUMNS].sort_values(
        by=["mandatory_feature_detected", "protected_feature_label", "column"],
        ascending=[False, True, True],
    )
