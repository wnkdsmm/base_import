from __future__ import annotations

from typing import Callable, Dict, List, Optional, Set

from ...types import ColumnMatchMetadata, ColumnTermPayload
from .column_filter_text import _column_payload_parts

def _build_column_term_payload(
    column_name: str,
    normalize_text: Callable[[str], str],
    extract_words: Callable[[str], List[str]],
    lemmatize_text: Callable[[str], List[str]],
) -> ColumnTermPayload:
    normalized_name = normalize_text(column_name)
    words = {word for word in extract_words(normalized_name) if word}
    lemmas: Set[str] = set()
    for word in words:
        try:
            lemmas.update(lemmatize_text(word))
        except Exception:
            continue
    return {
        "original_name": str(column_name),
        "normalized_name": normalized_name,
        "words": words,
        "lemmas": lemmas,
    }

def _payload_contains_fragment(column_payload: ColumnTermPayload, fragment: str) -> bool:
    normalized_name, words, lemmas = _column_payload_parts(column_payload)
    return bool(
        fragment
        and (
            fragment in normalized_name
            or any(fragment in word for word in words)
            or any(fragment in lemma for lemma in lemmas)
        )
    )

def _payload_matches_token_set(
    column_payload: ColumnTermPayload,
    token_set: List[str],
) -> bool:
    return bool(token_set) and all(_payload_contains_fragment(column_payload, token) for token in token_set)

def _payload_has_excluded_token(
    column_payload: ColumnTermPayload,
    exclude_tokens: List[str],
) -> bool:
    return any(_payload_contains_fragment(column_payload, token) for token in exclude_tokens)

def _build_match_metadata(
    *,
    scope: str,
    feature_id: str,
    feature_label: str,
    rule_id: str,
    matched_value: str,
    reason: str,
    mandatory: bool,
) -> ColumnMatchMetadata:
    return {
        "scope": scope,
        "feature_id": feature_id,
        "feature_label": feature_label,
        "rule_id": rule_id,
        "matched_value": matched_value,
        "reason": reason,
        "mandatory": mandatory,
    }

def _feature_label_from_match(match: Optional[ColumnMatchMetadata]) -> Optional[str]:
    if not match:
        return None
    feature_label = str(match.get("feature_label") or "")
    return feature_label or None
