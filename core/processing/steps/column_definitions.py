from __future__ import annotations


PROTECTION_REPORT_DEFAULTS: dict[str, str | bool] = {
    "profiling_candidate_to_drop": False,
    "mandatory_feature_detected": False,
    "protected_feature_id": "",
    "protected_feature_label": "",
    "protection_scope": "",
    "protection_rule": "",
    "protection_match": "",
    "protection_reason": "",
    "protected_from_drop": False,
}

PROTECTION_TEXT_COLUMNS: list[str] = [
    "protected_feature_id",
    "protected_feature_label",
    "protection_scope",
    "protection_rule",
    "protection_match",
    "protection_reason",
]

PROTECTED_REPORT_COLUMNS: list[str] = [
    "column",
    "dtype",
    "profiling_candidate_to_drop",
    "candidate_to_drop",
    "mandatory_feature_detected",
    "protected_feature_id",
    "protected_feature_label",
    "protection_scope",
    "protection_rule",
    "protection_match",
    "protection_reason",
    "drop_reasons",
]
