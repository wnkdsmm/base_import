from __future__ import annotations

import re
from typing import Callable, Dict, List, Set

from ...types import CategoryRule, ColumnTermPayload, MandatoryFeatureSpec

def _normalize_column_text(value: str) -> str:
    text = re.sub(r"[?_/#\-]|[^\w\s]+", " ", str(value).lower())
    return re.sub(r"\s+", " ", text).strip()

def _extract_word_tokens(value: str) -> List[str]:
    return re.findall(r"\w+", value)

def _column_payload_parts(column_payload: ColumnTermPayload) -> tuple[str, Set[str], Set[str]]:
    normalized_name = column_payload.get("normalized_name", "")
    words = column_payload.get("words", set())
    lemmas = column_payload.get("lemmas", set())
    return normalized_name, words, lemmas

def _prepare_synonym_payloads(
    synonyms: List[str],
    normalize_text: Callable[[str], str],
    extract_words: Callable[[str], List[str]],
) -> List[Dict[str, object]]:
    return [
        {"raw": s, "normalized": (n := normalize_text(s)), "tokens": extract_words(n)}
        for s in synonyms
    ]

def _prepare_token_sets(token_sets: List[List[str]], normalize_text: Callable[[str], str]) -> List[List[str]]:
    return [[normalize_text(token) for token in token_set if token] for token_set in token_sets]

def _prepare_exclude_tokens(tokens: List[str], normalize_text: Callable[[str], str]) -> List[str]:
    return [normalize_text(token) for token in tokens if token]

def _prepare_registry_feature_payload(
    feature: MandatoryFeatureSpec,
    normalize_text: Callable[[str], str],
    extract_words: Callable[[str], List[str]],
) -> MandatoryFeatureSpec:
    return {
        **feature,
        "prepared_synonyms": _prepare_synonym_payloads(
            list(feature.get("synonyms", [])),
            normalize_text,
            extract_words,
        ),
        "prepared_token_sets": _prepare_token_sets(
            list(feature.get("token_sets", [])),
            normalize_text,
        ),
        "prepared_exclude_tokens": _prepare_exclude_tokens(
            list(feature.get("exclude_tokens", [])),
            normalize_text,
        ),
    }

def _build_category_lemma_map(
    category_rules: List[CategoryRule],
    lemmatize_text: Callable[[str], List[str]],
) -> Dict[str, Set[str]]:
    category_lemmas: Dict[str, Set[str]] = {}
    for rule in category_rules:
        lemmas: Set[str] = set()
        for keyword in rule["keywords"]:
            try:
                lemmas.update(lemmatize_text(keyword))
            except Exception:
                continue
        category_lemmas[rule["id"]] = lemmas
    return category_lemmas

