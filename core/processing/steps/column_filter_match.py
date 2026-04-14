from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set

from natasha import MorphVocab, Doc, Segmenter, NewsEmbedding, NewsMorphTagger

from .column_definitions import (
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

_COLUMN_MATCHER: Optional["NatashaColumnMatcher"] = None

def _match_mandatory_feature_payload(
    column_payload: ColumnTermPayload,
    feature: MandatoryFeatureSpec,
) -> Optional[ColumnMatchMetadata]:
    exclude_tokens = list(feature.get("prepared_exclude_tokens", []))
    if exclude_tokens and _payload_has_excluded_token(column_payload, exclude_tokens):
        return None

    feature_id = str(feature["id"])
    feature_label = str(feature["label"])
    prepared_synonyms = list(feature.get("prepared_synonyms", []))
    normalized_name = str(column_payload["normalized_name"])
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

    for synonym in prepared_synonyms:
        tokens = list(synonym.get("tokens") or [])
        if len(tokens) > 1 and all(_payload_contains_fragment(column_payload, token) for token in tokens):
            return _build_match_metadata(
                scope="mandatory_registry",
                feature_id=feature_id,
                feature_label=feature_label,
                rule_id="mandatory_registry_synonym",
                matched_value=str(synonym["raw"]),
                reason=f"Колонка защищена обязательным реестром по синониму '{synonym['raw']}'.",
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

def _group_labels_for_ids(group_ids: List[str]) -> List[str]:
    return [rule["label"] for rule in COLUMN_CATEGORY_RULES if rule["id"] in group_ids]

def _wanted_group_ids(group_ids: List[str]) -> Set[str]:
    return {group_id for group_id in group_ids if group_id}

def _matching_group_ids(group_ids: List[str], wanted: Set[str]) -> List[str]:
    return [group_id for group_id in group_ids if group_id in wanted]

def _important_label_query_bonus(important_label_normalized: str, query_terms: List[Dict[str, Set[str]]]) -> int:
    if not important_label_normalized:
        return 0
    return 1 if any(term["token"] in important_label_normalized for term in query_terms) else 0

def _fallback_query_variants_for_token(
    token: str,
    fallback_patterns: List[tuple[List[str], str]],
    normalize_text: Callable[[str], str],
) -> Set[str]:
    variants: Set[str] = set()
    for parts, label in fallback_patterns:
        label_normalized = normalize_text(label)
        if label_normalized == token:
            variants.update(parts)
            continue

        matched_parts = {part for part in parts if token == part or token in part or part in token}
        if matched_parts:
            variants.update(matched_parts)
    return variants

def _build_query_term_payload(
    token: str,
    lemmatize_text: Callable[[str], List[str]],
    fallback_variants: Set[str],
) -> Dict[str, Set[str]]:
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
    extract_words: Callable[[str], List[str]],
    build_query_term: Callable[[str], Dict[str, Set[str]]],
) -> List[Dict[str, Set[str]]]:
    normalized_query = normalize_text(query_text)
    terms: List[Dict[str, Set[str]]] = []
    for token in extract_words(normalized_query):
        if len(token) < 2:
            continue
        terms.append(build_query_term(token))
    return terms

def _payload_matches_query_variants(column_payload: ColumnTermPayload, variants: Set[str]) -> bool:
    normalized_name, words, lemmas = _column_payload_parts(column_payload)
    return any(
        variant in normalized_name
        or variant in lemmas
        or any(word.startswith(variant) for word in words)
        for variant in variants
    )

def _match_query_terms_in_payload(
    column_payload: ColumnTermPayload,
    query_terms: List[Dict[str, Set[str]]],
) -> tuple[List[str], int]:
    matched_terms: List[str] = []
    score = 0
    for query_term in query_terms:
        if _payload_matches_query_variants(column_payload, query_term["variants"]):
            matched_terms.append(str(query_term["token"]))
            score += 4
    return matched_terms, score

def _payload_matches_category_rule(
    column_payload: ColumnTermPayload,
    category_rule: CategoryRule,
    category_lemmas: Set[str],
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
    category_rules: List[CategoryRule],
    category_lemmas: Dict[str, Set[str]],
    normalize_text: Callable[[str], str],
) -> List[str]:
    group_ids: List[str] = []
    for rule in category_rules:
        if _payload_matches_category_rule(
            column_payload,
            rule,
            category_lemmas.get(rule["id"], set()),
            normalize_text,
        ):
            group_ids.append(rule["id"])
    return group_ids

def _keyword_rule_match_specs(rule: MandatoryFeatureSpec) -> List[tuple[List[List[str]], str, str]]:
    return [
        (
            list(rule.get("include_all", [])),
            "keyword_include_all",
            "Колонка сохранена по keyword-правилу с обязательным набором токенов '{joined_tokens}'.",
        ),
        (
            list(rule.get("include_any", [])),
            "keyword_include_any",
            "Колонка сохранена по keyword-правилу с токеном '{joined_tokens}'.",
        ),
    ]

def _match_keyword_rule_payload(
    column_payload: ColumnTermPayload,
    rule: MandatoryFeatureSpec,
    normalize_text: Callable[[str], str],
) -> Optional[ColumnMatchMetadata]:
    exclude_tokens = _prepare_exclude_tokens(list(rule.get("exclude", [])), normalize_text)
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
    matched_terms: List[str],
    score: int,
    important_label: str,
    group_ids: List[str],
) -> Dict[str, object]:
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
    matched_groups: List[str],
    important_label: str,
) -> Dict[str, object]:
    return {
        "name": column_name,
        "group_ids": matched_groups,
        "group_labels": _group_labels_for_ids(matched_groups),
        "important_label": important_label,
    }

def _query_match_mode(matched_terms: List[str], query_term_count: int) -> str:
    return "full" if len(matched_terms) == query_term_count else "partial"

def _sort_column_query_matches(matches: List[Dict[str, object]]) -> List[Dict[str, object]]:
    matches.sort(key=lambda item: (-len(item["matched_terms"]), -int(item["score"]), item["name"].lower()))
    return matches

def _partition_column_query_matches(
    matches: List[Dict[str, object]],
    query_term_count: int,
) -> List[Dict[str, object]]:
    full_matches: List[Dict[str, object]] = []
    partial_matches: List[Dict[str, object]] = []
    for item in matches:
        item["match_mode"] = _query_match_mode(item["matched_terms"], query_term_count)
        if item["match_mode"] == "full":
            full_matches.append(item)
        else:
            partial_matches.append(item)
    return full_matches if full_matches else partial_matches

def _build_group_catalog_entry(rule: CategoryRule, group_columns: List[str]) -> GroupCatalogEntry:
    return {
        "id": rule["id"],
        "label": rule["label"],
        "description": rule["description"],
        "count": len(group_columns),
        "columns": group_columns,
    }

def _build_grouped_columns_by_category(
    columns: List[str],
    category_rules: List[CategoryRule],
    classify_column_groups: Callable[[str], List[str]],
) -> Dict[str, List[str]]:
    grouped_columns: Dict[str, List[str]] = {rule["id"]: [] for rule in category_rules}
    for column_name in columns:
        for group_id in classify_column_groups(column_name):
            grouped_columns[group_id].append(column_name)
    return grouped_columns

def _collect_column_matches(
    columns: List[str],
    column_terms: Callable[[str], Dict[str, object]],
    match_builder: Callable[[str, Dict[str, object]], Optional[Dict[str, object]]],
) -> List[Dict[str, object]]:
    matches: List[Dict[str, object]] = []
    for column_name in columns:
        column_payload = column_terms(column_name)
        item = match_builder(column_name, column_payload)
        if item is not None:
            matches.append(item)
    return matches

class NatashaColumnMatcher:
    """Переиспользуемый Natasha-поиск и доменный матчер по названиям колонок."""

    def __init__(self):
        self.morph_vocab = MorphVocab()
        self.segmenter = Segmenter()
        self.emb = NewsEmbedding()
        self.morph_tagger = NewsMorphTagger(self.emb)
        self.category_lemmas = _build_category_lemma_map(COLUMN_CATEGORY_RULES, self._lemmatize_text)
        self.mandatory_registry = [self._prepare_registry_feature(feature) for feature in MANDATORY_FEATURE_REGISTRY]

    def _lemmatize_text(self, value: str) -> List[str]:
        lemmas: List[str] = []
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

    def _extract_words(self, value: str) -> List[str]:
        return _extract_word_tokens(value)

    def _prepare_registry_feature(self, feature: MandatoryFeatureSpec) -> MandatoryFeatureSpec:
        return _prepare_registry_feature_payload(feature, self._normalize_text, self._extract_words)

    def _column_terms(self, column_name: str) -> ColumnTermPayload:
        return _build_column_term_payload(
            column_name,
            self._normalize_text,
            self._extract_words,
            self._lemmatize_text,
        )


    def _match_mandatory_feature(self, column_payload: ColumnTermPayload) -> Optional[ColumnMatchMetadata]:
        for feature in self.mandatory_registry:
            match = _match_mandatory_feature_payload(column_payload, feature)
            if match:
                return match
        return None


    def _match_legacy_explicit(self, column_payload: ColumnTermPayload) -> Optional[ColumnMatchMetadata]:
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

    def _match_keyword_rule(self, column_payload: ColumnTermPayload) -> Optional[ColumnMatchMetadata]:
        for rule in KEYWORD_IMPORTANCE_RULES:
            match = _match_keyword_rule_payload(column_payload, rule, self._normalize_text)
            if match:
                return match
        return None

    def match_column_metadata(self, col_name: str) -> Optional[ColumnMatchMetadata]:
        return self._match_column_payload_metadata(self._column_terms(col_name))

    def _match_column_payload_metadata(self, column_payload: ColumnTermPayload) -> Optional[ColumnMatchMetadata]:
        for matcher in (self._match_mandatory_feature, self._match_legacy_explicit, self._match_keyword_rule):
            match = matcher(column_payload)
            if match:
                return match
        return None

    def _important_label_from_payload(self, column_payload: ColumnTermPayload) -> Optional[str]:
        return _feature_label_from_match(self._match_column_payload_metadata(column_payload))

    def classify_column(self, col_name: str) -> Optional[str]:
        return self._classify_column_payload(self._column_terms(col_name))

    def _classify_column_payload(self, column_payload: ColumnTermPayload) -> Optional[str]:
        return self._important_label_from_payload(column_payload)

    def _query_terms(self, query_text: str) -> List[Dict[str, Set[str]]]:
        def build_query_term(token: str) -> Dict[str, Set[str]]:
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

    def classify_column_groups(self, column_name: str) -> List[str]:
        return self._classify_column_payload_groups(self._column_terms(column_name))

    def _classify_column_payload_groups(self, column_payload: ColumnTermPayload) -> List[str]:
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
        query_terms: List[Dict[str, Set[str]]],
    ) -> Optional[Dict[str, object]]:
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
        wanted: Set[str],
    ) -> Optional[Dict[str, object]]:
        matched_groups = _matching_group_ids(self._classify_column_payload_groups(column_payload), wanted)
        if not matched_groups:
            return None
        return _build_column_category_result(
            column_name=column_name,
            matched_groups=matched_groups,
            important_label=self._important_label_from_payload(column_payload) or "",
        )

    def get_group_catalog(self, columns: List[str]) -> List[Dict[str, object]]:
        grouped_columns = _build_grouped_columns_by_category(
            columns,
            COLUMN_CATEGORY_RULES,
            self.classify_column_groups,
        )

        return [
            _build_group_catalog_entry(rule, grouped_columns[rule["id"]])
            for rule in COLUMN_CATEGORY_RULES
        ]

    def get_mandatory_feature_catalog(self) -> List[Dict[str, object]]:
        return get_mandatory_feature_catalog()

    def find_columns_by_categories(self, columns: List[str], group_ids: List[str]) -> List[Dict[str, object]]:
        wanted = _wanted_group_ids(group_ids)
        if not wanted:
            return []
        return _collect_column_matches(
            columns,
            self._column_terms,
            lambda column_name, column_payload: self._build_column_category_match(column_name, column_payload, wanted),
        )

    def find_columns_by_query(self, columns: List[str], query_text: str) -> List[Dict[str, object]]:
        query_terms = self._query_terms(query_text)
        if not query_terms:
            return []
        matches = _collect_column_matches(
            columns,
            self._column_terms,
            lambda column_name, column_payload: self._build_column_query_match(column_name, column_payload, query_terms),
        )
        return _sort_column_query_matches(_partition_column_query_matches(matches, len(query_terms)))

def _legacy_get_mandatory_feature_catalog() -> List[Dict[str, object]]:
    return [
        {
            "id": feature["id"],
            "label": feature["label"],
            "description": feature["description"],
            "synonyms": list(feature.get("synonyms", [])),
        }
        for feature in MANDATORY_FEATURE_REGISTRY
    ]

def get_column_matcher() -> NatashaColumnMatcher:
    global _COLUMN_MATCHER
    if _COLUMN_MATCHER is None:
        _COLUMN_MATCHER = NatashaColumnMatcher()
    return _COLUMN_MATCHER
