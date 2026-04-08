import unittest

from core.processing.steps import keep_important_columns
from core.processing.steps.keep_important_columns import NatashaColumnMatcher


class _CheapColumnMatcher(NatashaColumnMatcher):
    def __init__(self):
        self.column_term_calls = []
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
        self.assertEqual(label, "Координаты")
        self.assertEqual(matcher.column_term_calls, ["Latitude"])

    def test_mandatory_exclude_tokens_block_domain_token_match(self):
        matcher = _CheapColumnMatcher()
        feature = next(item for item in keep_important_columns.MANDATORY_FEATURE_REGISTRY if item["id"] == "fire_date")
        column_name = " ".join(feature["token_sets"][1] + [feature["exclude_tokens"][0]])

        self.assertIsNone(matcher.match_column_metadata(column_name))

    def test_keyword_include_all_matching_preserves_rule_payload(self):
        matcher = _CheapColumnMatcher()
        rule = next(item for item in keep_important_columns.KEYWORD_IMPORTANCE_RULES if item["id"] == "casualty_flag")
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
        rule = next(item for item in keep_important_columns.KEYWORD_IMPORTANCE_RULES if item["id"] == "injuries_keyword")
        column_name = f"{rule['include_any'][0][0]} {rule['exclude'][0]}"

        self.assertIsNone(matcher.match_column_metadata(column_name))

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
                    "important_label": "Координаты",
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

    def test_query_search_prefers_full_matches_and_keeps_sort_order(self):
        matcher = _CheapColumnMatcher()

        result = matcher.find_columns_by_query(["beta", "beta alpha", "alpha beta"], "alpha beta")

        self.assertEqual([item["name"] for item in result], ["alpha beta", "beta alpha"])
        self.assertEqual([item["match_mode"] for item in result], ["full", "full"])
        self.assertEqual([item["score"] for item in result], [8, 8])
        self.assertEqual(matcher.column_term_calls, ["beta", "beta alpha", "alpha beta"])

    def test_query_search_adds_important_label_bonus_from_same_payload(self):
        matcher = _CheapColumnMatcher()

        result = matcher.find_columns_by_query(["Район"], "район")

        self.assertEqual(result[0]["name"], "Район")
        self.assertEqual(result[0]["important_label"], "Район")
        self.assertEqual(result[0]["score"], 5)
        self.assertEqual(result[0]["match_mode"], "full")
        self.assertEqual(matcher.column_term_calls, ["Район"])

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

        result = matcher.find_columns_by_categories(["Район"], ["address"])

        self.assertEqual(result[0]["name"], "Район")
        self.assertEqual(result[0]["group_ids"], ["address"])
        self.assertEqual(result[0]["important_label"], "Район")
        self.assertEqual(matcher.column_term_calls, ["Район"])

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
