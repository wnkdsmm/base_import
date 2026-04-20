import unittest
from collections import OrderedDict

from core.processing.steps import keep_important_columns
from core.processing.steps.keep_important_columns import NatashaColumnMatcher


COORDINATES_LABEL = "\u041a\u043e\u043e\u0440\u0434\u0438\u043d\u0430\u0442\u044b"
DISTRICT_LABEL = "\u0420\u0430\u0439\u043e\u043d"


class _CheapColumnMatcher(NatashaColumnMatcher):
    def __init__(self):
        # super().__init__() намеренно не вызывается: обходим инициализацию Natasha
        # (MorphVocab, Segmenter, NewsMorphTagger) ради скорости unit-тестов.
        # Все методы, зависящие от NLP-моделей, переопределены ниже.
        self.column_term_calls = []
        self._terms_cache: OrderedDict[str, object] = OrderedDict()
        self._group_catalog_cache: OrderedDict[frozenset[str], list[object]] = OrderedDict()
        self.category_lemmas = {
            rule["id"]: {
                lemma
                for keyword in rule["keywords"]
                for lemma in self._lemmatize_text(keyword)
            }
            for rule in keep_important_columns.COLUMN_CATEGORY_RULES
        }
        self.mandatory_registry = [
            self._prepare_registry_feature(feature)
            for feature in keep_important_columns.MANDATORY_FEATURE_REGISTRY
        ]

    def _lemmatize_text(self, value):
        return [str(value)]

    def _column_terms(self, column_name):
        self.column_term_calls.append(column_name)
        normalized_name = self._normalize_text(column_name)
        words = {word for word in self._extract_words(normalized_name) if word}
        return {
            "original_name": str(column_name),
            "normalized_name": normalized_name,
            "words": words,
            "lemmas": set(words),
        }


class NatashaColumnMatcherDecompositionTests(unittest.TestCase):
    def test_match_metadata_payload_keys_stay_stable(self):
        matcher = _CheapColumnMatcher()

        match = matcher.match_column_metadata("Latitude")

        self.assertEqual(
            set(match.keys()),
            {
                "scope",
                "feature_id",
                "feature_label",
                "rule_id",
                "matched_value",
                "reason",
                "mandatory",
            },
        )
        self.assertEqual(match["scope"], "mandatory_registry")
        self.assertEqual(match["feature_id"], "coordinates")
        self.assertTrue(match["mandatory"])

    def test_query_search_reuses_column_payload_for_classification(self):
        matcher = _CheapColumnMatcher()

        result = matcher.find_columns_by_query(["alpha column"], "alpha")

        self.assertEqual([item["name"] for item in result], ["alpha column"])
        self.assertEqual(matcher.column_term_calls, ["alpha column"])

    def test_mandatory_feature_matching_and_classification_share_payload_shape(self):
        matcher = _CheapColumnMatcher()
        payload = matcher._column_terms("Latitude")

        match = matcher._match_column_payload_metadata(payload)
        label = matcher._classify_column_payload(payload)

        self.assertEqual(match["feature_id"], "coordinates")
        self.assertEqual(match["rule_id"], "mandatory_registry_exact")
        self.assertEqual(label, COORDINATES_LABEL)
        self.assertEqual(matcher.column_term_calls, ["Latitude"])

    def test_mandatory_synonym_token_matching_keeps_report_payload(self):
        matcher = _CheapColumnMatcher()
        feature = next(
            item
            for item in matcher.mandatory_registry
            if any(len(synonym.get("tokens") or []) > 1 for synonym in item["prepared_synonyms"])
        )
        synonym = next(
            item
            for item in feature["prepared_synonyms"]
            if len(item.get("tokens") or []) > 1
        )
        column_name = f"prefix {' '.join(synonym['tokens'])} suffix"

        match = matcher.match_column_metadata(column_name)

        self.assertEqual(match["scope"], "mandatory_registry")
        self.assertEqual(match["feature_id"], feature["id"])
        self.assertEqual(match["feature_label"], feature["label"])
        self.assertEqual(match["rule_id"], "mandatory_registry_synonym")
        self.assertEqual(match["matched_value"], synonym["raw"])
        self.assertTrue(match["mandatory"])
        self.assertEqual(matcher.classify_column(column_name), feature["label"])

    def test_mandatory_exclude_tokens_block_domain_token_match(self):
        matcher = _CheapColumnMatcher()
        feature = next(
            item for item in keep_important_columns.MANDATORY_FEATURE_REGISTRY if item["id"] == "fire_date"
        )
        column_name = " ".join(feature["token_sets"][1] + [feature["exclude_tokens"][0]])

        self.assertIsNone(matcher.match_column_metadata(column_name))

    def test_keyword_include_all_matching_preserves_rule_payload(self):
        matcher = _CheapColumnMatcher()
        rule = next(
            item for item in keep_important_columns.KEYWORD_IMPORTANCE_RULES if item["id"] == "casualty_flag"
        )
        column_name = " ".join(rule["include_all"][0])

        match = matcher.match_column_metadata(column_name)

        self.assertEqual(match["scope"], "keyword_rule")
        self.assertEqual(match["feature_id"], rule["id"])
        self.assertEqual(match["feature_label"], rule["label"])
        self.assertEqual(match["rule_id"], "keyword_include_all")
        self.assertEqual(match["matched_value"], " + ".join(rule["include_all"][0]))
        self.assertFalse(match["mandatory"])
        self.assertEqual(matcher.classify_column(column_name), rule["label"])

    def test_keyword_exclude_tokens_prevent_false_positive(self):
        matcher = _CheapColumnMatcher()
        rule = next(
            item for item in keep_important_columns.KEYWORD_IMPORTANCE_RULES if item["id"] == "injuries_keyword"
        )
        column_name = f"{rule['include_any'][0][0]} {rule['exclude'][0]}"

        self.assertIsNone(matcher.match_column_metadata(column_name))

    def test_mandatory_registry_wins_over_keyword_rule_for_same_column(self):
        matcher = _CheapColumnMatcher()
        feature = next(
            item for item in matcher.mandatory_registry if item["id"] == "fatalities"
        )
        synonym = next(
            item
            for item in feature["prepared_synonyms"]
            if item["raw"] not in keep_important_columns.LEGACY_EXPLICIT_IMPORTANT_COLUMNS
            and "погиб" in item["normalized"]
        )

        match = matcher.match_column_metadata(str(synonym["raw"]))

        self.assertEqual(match["scope"], "mandatory_registry")
        self.assertEqual(match["feature_id"], "fatalities")
        self.assertEqual(match["rule_id"], "mandatory_registry_exact")
        self.assertTrue(match["mandatory"])

    def test_legacy_explicit_matching_reports_scope(self):
        matcher = _CheapColumnMatcher()
        column_name, expected_label = next(
            (column_name, label)
            for column_name, label in keep_important_columns.LEGACY_EXPLICIT_IMPORTANT_COLUMNS.items()
            if matcher._match_mandatory_feature(matcher._column_terms(column_name)) is None
        )

        match = matcher.match_column_metadata(column_name)

        self.assertEqual(match["scope"], "legacy_explicit")
        self.assertEqual(match["feature_label"], expected_label)
        self.assertEqual(match["rule_id"], "legacy_explicit_exact")
        self.assertEqual(match["matched_value"], column_name)
        self.assertFalse(match["mandatory"])

    def test_query_search_returns_important_label_without_rebuilding_payload(self):
        matcher = _CheapColumnMatcher()

        result = matcher.find_columns_by_query(["Latitude", "alpha column"], "lat")

        self.assertEqual(
            result,
            [
                {
                    "name": "Latitude",
                    "matched_terms": ["lat"],
                    "score": 4,
                    "important_label": COORDINATES_LABEL,
                    "group_ids": [],
                    "group_labels": [],
                    "match_mode": "full",
                }
            ],
        )
        self.assertEqual(matcher.column_term_calls, ["Latitude", "alpha column"])

    def test_query_search_expands_fallback_label_to_parts(self):
        matcher = _CheapColumnMatcher()
        parts, label = keep_important_columns.FALLBACK_IMPORTANT_PATTERNS[0]
        column_name = " ".join(parts)
        query_token = matcher._extract_words(matcher._normalize_text(label))[0]

        result = matcher.find_columns_by_query([column_name], label)

        self.assertEqual([item["name"] for item in result], [column_name])
        self.assertEqual(result[0]["matched_terms"], [query_token])
        self.assertEqual(result[0]["match_mode"], "full")

    def test_query_terms_drop_short_tokens_and_keep_fallback_variants(self):
        matcher = _CheapColumnMatcher()
        parts, label = keep_important_columns.FALLBACK_IMPORTANT_PATTERNS[0]

        query_terms = matcher._query_terms(f"a {label}")

        self.assertEqual([item["token"] for item in query_terms], [matcher._normalize_text(label)])
        self.assertTrue(set(parts).issubset(query_terms[0]["variants"]))

    def test_query_search_prefers_full_matches_and_keeps_sort_order(self):
        matcher = _CheapColumnMatcher()

        result = matcher.find_columns_by_query(["beta", "beta alpha", "alpha beta"], "alpha beta")

        self.assertEqual([item["name"] for item in result], ["alpha beta", "beta alpha", "beta"])
        self.assertEqual([item["match_mode"] for item in result], ["full", "full", "partial"])
        self.assertEqual([item["score"] for item in result], [10, 10, 6])
        self.assertEqual(matcher.column_term_calls, ["beta", "beta alpha", "alpha beta"])

    def test_query_search_returns_partial_matches_when_no_full_match(self):
        matcher = _CheapColumnMatcher()

        result = matcher.find_columns_by_query(["beta", "gamma"], "alpha beta")

        self.assertEqual(
            result,
            [
                {
                    "name": "beta",
                    "matched_terms": ["beta"],
                    "score": 6,
                    "important_label": "",
                    "group_ids": [],
                    "group_labels": [],
                    "match_mode": "partial",
                }
            ],
        )
        self.assertEqual(matcher.column_term_calls, ["beta", "gamma"])

    def test_query_search_skips_payload_build_when_query_terms_are_empty(self):
        matcher = _CheapColumnMatcher()

        result = matcher.find_columns_by_query(["alpha column"], "a")

        self.assertEqual(result, [])
        self.assertEqual(matcher.column_term_calls, [])

    def test_classify_column_groups_keeps_rule_order_for_multi_match_column(self):
        matcher = _CheapColumnMatcher()
        date_rule = next(item for item in keep_important_columns.COLUMN_CATEGORY_RULES if item["id"] == "dates")
        address_rule = next(item for item in keep_important_columns.COLUMN_CATEGORY_RULES if item["id"] == "address")

        group_ids = matcher.classify_column_groups(f"{date_rule['parts'][0]} {address_rule['parts'][0]}")

        self.assertEqual(group_ids, ["dates", "address"])

    def test_query_search_adds_important_label_bonus_from_same_payload(self):
        matcher = _CheapColumnMatcher()

        result = matcher.find_columns_by_query([DISTRICT_LABEL], DISTRICT_LABEL.lower())

        self.assertEqual(result[0]["name"], DISTRICT_LABEL)
        self.assertEqual(result[0]["important_label"], DISTRICT_LABEL)
        self.assertEqual(result[0]["score"], 7)
        self.assertEqual(result[0]["match_mode"], "full")
        self.assertEqual(matcher.column_term_calls, [DISTRICT_LABEL])

    def test_category_search_reuses_payload_for_group_and_label(self):
        matcher = _CheapColumnMatcher()
        rule = keep_important_columns.COLUMN_CATEGORY_RULES[0]
        column_name = f"{rule['parts'][0]} field"

        result = matcher.find_columns_by_categories([column_name], [rule["id"]])

        self.assertEqual([item["name"] for item in result], [column_name])
        self.assertEqual(result[0]["group_ids"], [rule["id"]])
        self.assertEqual(result[0]["group_labels"], [rule["label"]])
        self.assertEqual(matcher.column_term_calls, [column_name])

    def test_category_search_returns_important_label_from_same_payload(self):
        matcher = _CheapColumnMatcher()

        result = matcher.find_columns_by_categories([DISTRICT_LABEL], ["address"])

        self.assertEqual(result[0]["name"], DISTRICT_LABEL)
        self.assertEqual(result[0]["group_ids"], ["address"])
        self.assertEqual(result[0]["important_label"], DISTRICT_LABEL)
        self.assertEqual(matcher.column_term_calls, [DISTRICT_LABEL])

    def test_category_search_skips_payload_build_when_group_filter_is_empty(self):
        matcher = _CheapColumnMatcher()

        result = matcher.find_columns_by_categories([DISTRICT_LABEL], [])

        self.assertEqual(result, [])
        self.assertEqual(matcher.column_term_calls, [])

    def test_group_catalog_classifies_query_related_columns(self):
        matcher = _CheapColumnMatcher()
        rule = keep_important_columns.COLUMN_CATEGORY_RULES[0]
        column_name = f"{rule['parts'][0]} field"

        catalog = matcher.get_group_catalog([column_name, "unrelated"])

        matched_group = next(item for item in catalog if item["id"] == rule["id"])
        self.assertEqual(matched_group["columns"], [column_name])
        self.assertEqual(matched_group["count"], 1)


if __name__ == "__main__":
    unittest.main()
