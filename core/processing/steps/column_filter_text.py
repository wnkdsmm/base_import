from __future__ import annotations

import re
from typing import Callable, Dict, List, Set

from ...types import CategoryRule, ColumnTermPayload, MandatoryFeatureSpec

def _normalize_column_text(value: str) -> str:
    text = str(value).lower().replace("?", "?")
    text = re.sub(r"[_/#-]+", " ", text)
    text = re.sub(r"[^\w\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def _extract_word_tokens(value: str) -> List[str]:
    return [word for word in re.findall(r"\w+", value) if word]

def _column_payload_parts(column_payload: ColumnTermPayload) -> tuple[str, Set[str], Set[str]]:
    normalized_name = column_payload.get("normalized_name", "")
    if not isinstance(normalized_name, str):
        normalized_name = str(normalized_name)
        column_payload["normalized_name"] = normalized_name

    words = column_payload.get("words", set())
    if not isinstance(words, set):
        words = {str(word) for word in words}
        column_payload["words"] = words

    lemmas = column_payload.get("lemmas", set())
    if not isinstance(lemmas, set):
        lemmas = {str(lemma) for lemma in lemmas}
        column_payload["lemmas"] = lemmas

    return normalized_name, words, lemmas

def _prepare_synonym_payloads(
    synonyms: List[str],
    normalize_text: Callable[[str], str],
    extract_words: Callable[[str], List[str]],
) -> List[Dict[str, object]]:
    prepared = []
    for synonym in synonyms:
        normalized = normalize_text(synonym)
        prepared.append({"raw": synonym, "normalized": normalized, "tokens": extract_words(normalized)})
    return prepared

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

