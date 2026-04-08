import unittest

from core.processing.steps.keep_important_columns import NatashaColumnMatcher


class _CheapColumnMatcher(NatashaColumnMatcher):
    def __init__(self):
        self.column_term_calls = []
        self.category_lemmas = {}
        self.mandatory_registry = []

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


if __name__ == "__main__":
    unittest.main()
