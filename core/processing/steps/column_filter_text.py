from __future__ import annotations

import logging
import re
from typing import Callable

from ...types import CategoryRule, ColumnTermPayload, MandatoryFeatureSpec


logger = logging.getLogger(__name__)


def _normalize_column_text(value: str) -> str:
    text = re.sub(r"[?_/#-]|[^\w\s]+", " ", str(value).lower())
    return re.sub(r"\s+", " ", text).strip()


def _extract_word_tokens(value: str) -> list[str]:
    return re.findall(r"\w+", value)


def _column_payload_parts(column_payload: ColumnTermPayload) -> tuple[str, set[str], set[str]]:
    normalized_name = column_payload.get("normalized_name", "")
    words = column_payload.get("words", set())
    lemmas = column_payload.get("lemmas", set())
    return normalized_name, words, lemmas


def _prepare_synonym_payloads(
    synonyms: list[str],
    normalize_text: Callable[[str], str],
    extract_words: Callable[[str], list[str]],
) -> list[dict[str, object]]:
    result = []
    for s in synonyms:
        normalized = normalize_text(s)
        result.append({"raw": s, "normalized": normalized, "tokens": extract_words(normalized)})
    return result


def _prepare_token_sets(token_sets: list[list[str]], normalize_text: Callable[[str], str]) -> list[list[str]]:
    return [[normalize_text(token) for token in token_set if token] for token_set in token_sets]


def _prepare_exclude_tokens(tokens: list[str], normalize_text: Callable[[str], str]) -> list[str]:
    return [normalize_text(token) for token in tokens if token]


def _prepare_registry_feature_payload(
    feature: MandatoryFeatureSpec,
    normalize_text: Callable[[str], str],
    extract_words: Callable[[str], list[str]],
) -> MandatoryFeatureSpec:
    return {
        **feature,
        "prepared_synonyms": _prepare_synonym_payloads(
            feature.get("synonyms", []),
            normalize_text,
            extract_words,
        ),
        "prepared_token_sets": _prepare_token_sets(
            feature.get("token_sets", []),
            normalize_text,
        ),
        "prepared_exclude_tokens": _prepare_exclude_tokens(
            feature.get("exclude_tokens", []),
            normalize_text,
        ),
    }


def _build_category_lemma_map(
    category_rules: list[CategoryRule],
    lemmatize_text: Callable[[str], list[str]],
) -> dict[str, set[str]]:
    category_lemmas: dict[str, set[str]] = {}
    for rule in category_rules:
        lemmas: set[str] = set()
        for keyword in rule["keywords"]:
            try:
                lemmas.update(lemmatize_text(keyword))
            except Exception as exc:
                logger.warning("Не удалось лемматизировать ключевое слово категории '%s': %s", keyword, exc, exc_info=False)
                continue
        category_lemmas[rule["id"]] = lemmas
    return category_lemmas
