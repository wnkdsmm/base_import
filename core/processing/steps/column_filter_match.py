from __future__ import annotations

import threading`r`nfrom collections import OrderedDict
from typing import Any, Callable

from natasha import MorphVocab, Doc, Segmenter, NewsEmbedding, NewsMorphTagger

from app.domain.column_matching import (
    COLUMN_CATEGORY_RULES,
    FALLBACK_IMPORTANT_PATTERNS,
    KEYWORD_IMPORTANCE_RULES,
    LEGACY_EXPLICIT_IMPORTANT_COLUMNS,
    MANDATORY_FEATURE_REGISTRY,
    get_mandatory_feature_catalog,
)
from .column_filter_payload import (
    _build_column_term_payload,
    _build_match_metadata,
    _feature_label_from_match,
    _payload_contains_fragment,
    _payload_has_excluded_token,
    _payload_matches_token_set,
)
from .column_filter_text import (
    _build_category_lemma_map,
    _column_payload_parts,
    _extract_word_tokens,
    _normalize_column_text,
    _prepare_exclude_tokens,
    _prepare_registry_feature_payload,
)
from ...types import (
    CategoryRule,
    ColumnMatchMetadata,
    ColumnTermPayload,
    GroupCatalogEntry,
    MandatoryFeatureSpec,
)

_COLUMN_MATCHER: NatashaColumnMatcher | None = None
_COLUMN_MATCHER_LOCK = threading.Lock()
_CATEGORY_ID_TO_LABEL: dict[str, str] = {
    rule["id"]: rule["label"] for rule in COLUMN_CATEGORY_RULES
}


def _match_mandatory_feature_payload(
    column_payload: ColumnTermPayload,
    feature: MandatoryFeatureSpec,
) -> ColumnMatchMetadata | None:
    exclude_tokens = feature.get("prepared_exclude_tokens") or []
    if exclude_tokens and _payload_has_excluded_token(column_payload, exclude_tokens):
        return None

    feature_id = str(feature["id"])
    feature_label = str(feature["label"])
    prepared_synonyms = feature.get("prepared_synonyms") or []
    normalized_name = str(column_payload["normalized_name"])
    token_match_synonym: str | None = None
    for synonym in prepared_synonyms:
        if normalized_name == synonym["normalized"]:
            return _build_match_metadata(
                scope="mandatory_registry",
                feature_id=feature_id,
                feature_label=feature_label,
                rule_id="mandatory_registry_exact",
                matched_value=str(synonym["raw"]),
                reason=f"Колонка совпала с обязательным признаком '{feature['label']}' по точному имени.",
                mandatory=True,
            )

        tokens = list(synonym.get("tokens") or [])
        if (
            token_match_synonym is None
            and len(tokens) > 1
            and all(_payload_contains_fragment(column_payload, token) for token in tokens)
        ):
            token_match_synonym = str(synonym["raw"])

    if token_match_synonym is not None:
        return _build_match_metadata(
            scope="mandatory_registry",
            feature_id=feature_id,
            feature_label=feature_label,
            rule_id="mandatory_registry_synonym",
            matched_value=token_match_synonym,
            reason=f"Колонка защищена обязательным реестром по синониму '{token_match_synonym}'.",
            mandatory=True,
        )

    for token_set in feature.get("prepared_token_sets", []):
        if _payload_matches_token_set(column_payload, token_set):
            joined_tokens = " + ".join(token_set)
            return _build_match_metadata(
                scope="mandatory_registry",
                feature_id=feature_id,
                feature_label=feature_label,
                rule_id="mandatory_registry_tokens",
                matched_value=joined_tokens,
                reason=f"Колонка защищена обязательным реестром по доменным токенам '{joined_tokens}'.",
                mandatory=True,
            )
    return None


def _group_labels_for_ids(group_ids: list[str]) -> list[str]:
    return [_CATEGORY_ID_TO_LABEL[gid] for gid in group_ids if gid in _CATEGORY_ID_TO_LABEL]


def _important_label_query_bonus(important_label_normalized: str, query_terms: list[dict[str, set[str]]]) -> int:
    if not important_label_normalized:
        return 0
    return 1 if any(
        variant in important_label_normalized
        for term in query_terms
        for variant in term.get("variants", {term["token"]})
    ) else 0


def _fallback_query_variants_for_token(
    token: str,
    fallback_patterns: list[tuple[list[str], str]],
    normalize_text: Callable[[str], str],
) -> set[str]:
    variants: set[str] = set()
    for parts, label in fallback_patterns:
        label_normalized = normalize_text(label)
        if label_normalized == token:
            variants.update(parts)
            continue

        matched_parts = {
            part for part in parts
            if token == part
            or (len(part) >= 3 and part in token)
            or (len(token) >= 4 and token in part)
        }
        if matched_parts:
            variants.update(matched_parts)
    return variants


def _build_query_term_payload(
    token: str,
    lemmatize_text: Callable[[str], list[str]],
    fallback_variants: set[str],
) -> dict[str, set[str]]:
    variants = {token}
    try:
        variants.update(lemmatize_text(token))
    except Exception:
        pass
    variants.update(fallback_variants)
    return {"token": token, "variants": {variant for variant in variants if variant}}


def _build_query_terms(
    query_text: str,
    normalize_text: Callable[[str], str],
    extract_words: Callable[[str], list[str]],
    build_query_term: Callable[[str], dict[str, set[str]]],
) -> list[dict[str, set[str]]]:
    normalized_query = normalize_text(query_text)
    terms: list[dict[str, set[str]]] = []
    for token in extract_words(normalized_query):
        if len(token) < 2:
            continue
        terms.append(build_query_term(token))
    return terms


def _payload_query_match_score(column_payload: ColumnTermPayload, variants: set[str]) -> int:
    normalized_name, words, lemmas = _column_payload_parts(column_payload)
    best = 0
    for variant in variants:
        if not variant:
            continue
        if normalized_name == variant:
            best = max(best, 6)
        elif variant in lemmas:
            best = max(best, 5)
        elif any(word.startswith(variant) for word in words):
            best = max(best, 4)
        elif variant in normalized_name:
            best = max(best, 3)
    return best


def _match_query_terms_in_payload(
    column_payload: ColumnTermPayload,
    query_terms: list[dict[str, set[str]]],
) -> tuple[list[str], int]:
    matched_terms: list[str] = []
    score = 0
    for query_term in query_terms:
        term_score = _payload_query_match_score(column_payload, query_term["variants"])
        if term_score > 0:
            matched_terms.append(str(query_term["token"]))
            score += term_score
    return matched_terms, score


def _payload_matches_category_rule(
    column_payload: ColumnTermPayload,
    category_rule: CategoryRule,
    category_lemmas: set[str],
    normalize_text: Callable[[str], str],
) -> bool:
    normalized_name, words, lemmas = _column_payload_parts(column_payload)

    if any(part in normalized_name for part in category_rule["parts"]):
        return True

    for keyword in category_rule["keywords"]:
        keyword_normalized = normalize_text(keyword)
        if keyword_normalized in normalized_name:
            return True

    if category_lemmas.intersection(lemmas):
        return True

    return bool(category_lemmas.intersection(words))


def _matching_category_rule_ids(
    column_payload: ColumnTermPayload,
    category_rules: list[CategoryRule],
    category_lemmas: dict[str, set[str]],
    normalize_text: Callable[[str], str],
) -> list[str]:
    group_ids: list[str] = []
    for rule in category_rules:
        if _payload_matches_category_rule(
            column_payload,
            rule,
            category_lemmas.get(rule["id"], set()),
            normalize_text,
        ):
            group_ids.append(rule["id"])
    return group_ids


def _keyword_rule_match_specs(rule: MandatoryFeatureSpec) -> list[tuple[list[list[str]], str, str]]:
    return [
        (
            rule.get("include_all") or [],
            "keyword_include_all",
            "Колонка сохранена по keyword-правилу с обязательным набором токенов '{joined_tokens}'.",
        ),
        (
            rule.get("include_any") or [],
            "keyword_include_any",
            "Колонка сохранена по keyword-правилу с токеном '{joined_tokens}'.",
        ),
    ]


def _match_keyword_rule_payload(
    column_payload: ColumnTermPayload,
    rule: MandatoryFeatureSpec,
    normalize_text: Callable[[str], str],
) -> ColumnMatchMetadata | None:
    exclude_tokens = _prepare_exclude_tokens(rule.get("exclude") or [], normalize_text)
    if exclude_tokens and _payload_has_excluded_token(column_payload, exclude_tokens):
        return None

    for token_sets, rule_id, reason_template in _keyword_rule_match_specs(rule):
        for token_set in token_sets:
            if _payload_matches_token_set(column_payload, token_set):
                joined_tokens = " + ".join(token_set)
                return _build_match_metadata(
                    scope="keyword_rule",
                    feature_id=str(rule.get("id") or ""),
                    feature_label=str(rule.get("label") or ""),
                    rule_id=rule_id,
                    matched_value=joined_tokens,
                    reason=reason_template.format(joined_tokens=joined_tokens),
                    mandatory=False,
                )
    return None


def _build_column_query_result(
    *,
    column_name: str,
    matched_terms: list[str],
    score: int,
    important_label: str,
    group_ids: list[str],
) -> dict[str, object]:
    return {
        "name": column_name,
        "matched_terms": matched_terms,
        "score": score,
        "important_label": important_label,
        "group_ids": group_ids,
        "group_labels": _group_labels_for_ids(group_ids),
    }


def _build_column_category_result(
    *,
    column_name: str,
    matched_groups: list[str],
    important_label: str,
) -> dict[str, object]:
    return {
        "name": column_name,
        "group_ids": matched_groups,
        "group_labels": _group_labels_for_ids(matched_groups),
        "important_label": important_label,
    }


def _query_match_mode(matched_terms: list[str], query_term_count: int) -> str:
    return "full" if len(matched_terms) == query_term_count else "partial"


def _sort_column_query_matches(matches: list[dict[str, object]]) -> list[dict[str, object]]:
    matches.sort(key=lambda item: (-len(item["matched_terms"]), -int(item["score"]), item["name"].lower()))
    return matches


def _partition_column_query_matches(
    matches: list[dict[str, object]],
    query_term_count: int,
) -> list[dict[str, object]]:
    full_matches: list[dict[str, object]] = []
    partial_matches: list[dict[str, object]] = []
    for item in matches:
        item["match_mode"] = _query_match_mode(item["matched_terms"], query_term_count)
        if item["match_mode"] == "full":
            full_matches.append(item)
        else:
            partial_matches.append(item)
    return full_matches + partial_matches


def _build_group_catalog_entry(rule: CategoryRule, group_columns: list[str]) -> GroupCatalogEntry:
    return {
        "id": rule["id"],
        "label": rule["label"],
        "description": rule["description"],
        "count": len(group_columns),
        "columns": group_columns,
    }


def _build_grouped_columns_by_category(
    columns: list[str],
    category_rules: list[CategoryRule],
    classify_column_groups: Callable[[str], list[str]],
) -> dict[str, list[str]]:
    grouped_columns: dict[str, list[str]] = {rule["id"]: [] for rule in category_rules}
    for column_name in columns:
        for group_id in classify_column_groups(column_name):
            grouped_columns[group_id].append(column_name)
    return grouped_columns


def _collect_column_matches(
    columns: list[str],
    column_terms: Callable[[str], dict[str, object]],
    match_builder: Callable[[str, dict[str, object]], dict[str, object] | None],
) -> list[dict[str, object]]:
    matches: list[dict[str, object]] = []
    for column_name in columns:
        column_payload = column_terms(column_name)
        item = match_builder(column_name, column_payload)
        if item is not None:
            matches.append(item)
    return matches


class NatashaColumnMatcher:
    """Переиспользуемый Natasha-поиск и доменный матчер по названиям колонок."""
    _terms_cache: OrderedDict[str, ColumnTermPayload]
    _group_catalog_cache: OrderedDict[frozenset[str], list[dict[str, object]]]

    def __init__(self) -> None:`r`n        self.morph_vocab: MorphVocab = MorphVocab()`r`n        self.segmenter: Segmenter = Segmenter()`r`n        self.emb: NewsEmbedding = NewsEmbedding()`r`n        self.morph_tagger: NewsMorphTagger = NewsMorphTagger(self.emb)`r`n        self.category_lemmas: dict[str, set[str]] = _build_category_lemma_map(`r`n            COLUMN_CATEGORY_RULES, self._lemmatize_text`r`n        )`r`n        self.mandatory_registry: list[MandatoryFeatureSpec] = [`r`n            self._prepare_registry_feature(feature) for feature in MANDATORY_FEATURE_REGISTRY`r`n        ]
        self._terms_cache: OrderedDict[str, ColumnTermPayload] = OrderedDict()
        self._group_catalog_cache: OrderedDict[frozenset[str], list[dict[str, object]]] = OrderedDict()

    def _lemmatize_text(self, value: str) -> list[str]:
        lemmas: list[str] = []
        doc = Doc(str(value))
        doc.segment(self.segmenter)
        doc.tag_morph(self.morph_tagger)
        for token in doc.tokens:
            token.lemmatize(self.morph_vocab)
            if token.lemma:
                lemmas.append(token.lemma)
        return lemmas

    def _normalize_text(self, value: str) -> str:
        return _normalize_column_text(value)

    def _extract_words(self, value: str) -> list[str]:
        return _extract_word_tokens(value)

    def _prepare_registry_feature(self, feature: MandatoryFeatureSpec) -> MandatoryFeatureSpec:
        return _prepare_registry_feature_payload(feature, self._normalize_text, self._extract_words)

    def _column_terms(self, column_name: str) -> ColumnTermPayload:
        cached_payload = self._terms_cache.get(column_name)
        if cached_payload is not None:
            self._terms_cache.move_to_end(column_name)
            return cached_payload

        payload = _build_column_term_payload(
            column_name,
            self._normalize_text,
            self._extract_words,
            self._lemmatize_text,
        )
        self._terms_cache[column_name] = payload
        if len(self._terms_cache) > 4096:
            self._terms_cache.popitem(last=False)
        return payload

    def _match_mandatory_feature(self, column_payload: ColumnTermPayload) -> ColumnMatchMetadata | None:
        for feature in self.mandatory_registry:
            match = _match_mandatory_feature_payload(column_payload, feature)
            if match:
                return match
        return None

    def _match_legacy_explicit(self, column_payload: ColumnTermPayload) -> ColumnMatchMetadata | None:
        original_name = str(column_payload["original_name"]).strip()
        exact_match = LEGACY_EXPLICIT_IMPORTANT_COLUMNS.get(original_name)
        if not exact_match:
            return None

        return _build_match_metadata(
            scope="legacy_explicit",
            feature_id="",
            feature_label=str(exact_match),
            rule_id="legacy_explicit_exact",
            matched_value=original_name,
            reason=f"Колонка сохранена по legacy-правилу точного совпадения '{original_name}'.",
            mandatory=False,
        )

    def _match_keyword_rule(self, column_payload: ColumnTermPayload) -> ColumnMatchMetadata | None:
        for rule in KEYWORD_IMPORTANCE_RULES:
            match = _match_keyword_rule_payload(column_payload, rule, self._normalize_text)
            if match:
                return match
        return None

    def match_column_metadata(self, col_name: str) -> ColumnMatchMetadata | None:
        return self._match_column_payload_metadata(self._column_terms(col_name))

    def _match_column_payload_metadata(self, column_payload: ColumnTermPayload) -> ColumnMatchMetadata | None:
        for matcher in (self._match_mandatory_feature, self._match_legacy_explicit, self._match_keyword_rule):
            match = matcher(column_payload)
            if match:
                return match
        return None

    def _important_label_from_payload(self, column_payload: ColumnTermPayload) -> str | None:
        return _feature_label_from_match(self._match_column_payload_metadata(column_payload))

    def classify_column(self, col_name: str) -> str | None:
        return self._classify_column_payload(self._column_terms(col_name))

    def _classify_column_payload(self, column_payload: ColumnTermPayload) -> str | None:
        return self._important_label_from_payload(column_payload)

    def _query_terms(self, query_text: str) -> list[dict[str, set[str]]]:
        def build_query_term(token: str) -> dict[str, set[str]]:
            return _build_query_term_payload(
                token,
                self._lemmatize_text,
                _fallback_query_variants_for_token(
                    token,
                    FALLBACK_IMPORTANT_PATTERNS,
                    self._normalize_text,
                ),
            )

        return _build_query_terms(
            query_text,
            self._normalize_text,
            self._extract_words,
            build_query_term,
        )

    def classify_column_groups(self, column_name: str) -> list[str]:
        return self._classify_column_payload_groups(self._column_terms(column_name))

    def _classify_column_payload_groups(self, column_payload: ColumnTermPayload) -> list[str]:
        return _matching_category_rule_ids(
            column_payload,
            COLUMN_CATEGORY_RULES,
            self.category_lemmas,
            self._normalize_text,
        )

    def _build_column_query_match(
        self,
        column_name: str,
        column_payload: ColumnTermPayload,
        query_terms: list[dict[str, set[str]]],
    ) -> dict[str, object] | None:
        matched_terms, score = _match_query_terms_in_payload(column_payload, query_terms)
        if not matched_terms:
            return None

        important_label = self._important_label_from_payload(column_payload)
        if important_label:
            important_label_normalized = self._normalize_text(important_label)
            score += _important_label_query_bonus(important_label_normalized, query_terms)

        group_ids = self._classify_column_payload_groups(column_payload)
        return _build_column_query_result(
            column_name=column_name,
            matched_terms=matched_terms,
            score=score,
            important_label=important_label or "",
            group_ids=group_ids,
        )

    def _build_column_category_match(
        self,
        column_name: str,
        column_payload: ColumnTermPayload,
        wanted: set[str],
    ) -> dict[str, object] | None:
        matched = [
            gid for gid in self._classify_column_payload_groups(column_payload)
            if gid in wanted
        ]
        if not matched:
            return None
        return _build_column_category_result(
            column_name=column_name,
            matched_groups=matched,
            important_label=self._important_label_from_payload(column_payload) or "",
        )

    def get_group_catalog(self, columns: list[str]) -> list[dict[str, object]]:
        cache_key = frozenset(columns)
        cached_catalog = self._group_catalog_cache.get(cache_key)
        if cached_catalog is not None:
            self._group_catalog_cache.move_to_end(cache_key)
            return cached_catalog

        grouped_columns = _build_grouped_columns_by_category(
            columns,
            COLUMN_CATEGORY_RULES,
            self.classify_column_groups,
        )

        result = [
            _build_group_catalog_entry(rule, grouped_columns[rule["id"]])
            for rule in COLUMN_CATEGORY_RULES
        ]
        self._group_catalog_cache[cache_key] = result
        if len(self._group_catalog_cache) > 32:
            self._group_catalog_cache.popitem(last=False)
        return result

    def get_mandatory_feature_catalog(self) -> list[dict[str, object]]:
        return get_mandatory_feature_catalog()

    def find_columns_by_categories(self, columns: list[str], group_ids: list[str]) -> list[dict[str, object]]:
        wanted = {gid for gid in group_ids if gid}
        if not wanted:
            return []
        return _collect_column_matches(
            columns,
            self._column_terms,
            lambda column_name, column_payload: self._build_column_category_match(column_name, column_payload, wanted),
        )

    def find_columns_by_query(self, columns: list[str], query_text: str) -> list[dict[str, object]]:
        query_terms = self._query_terms(query_text)
        if not query_terms:
            return []
        matches = _collect_column_matches(
            columns,
            self._column_terms,
            lambda column_name, column_payload: self._build_column_query_match(column_name, column_payload, query_terms),
        )
        return _sort_column_query_matches(_partition_column_query_matches(matches, len(query_terms)))


def get_column_matcher() -> NatashaColumnMatcher:
    global _COLUMN_MATCHER
    if _COLUMN_MATCHER is None:
        with _COLUMN_MATCHER_LOCK:
            if _COLUMN_MATCHER is None:
                _COLUMN_MATCHER = NatashaColumnMatcher()
    return _COLUMN_MATCHER


