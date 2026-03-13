import os
import re
from typing import Dict, List, Optional, Set

import pandas as pd
from natasha import MorphVocab, Doc, Segmenter, NewsEmbedding, NewsMorphTagger

from config.constants import PROFILING_CSV_SUFFIX, PROFILING_XLSX_SUFFIX, IMPORTANT_KEYWORDS
from pipeline import PipelineStep


EXPLICIT_IMPORTANT_COLUMNS = {
    "Количество погибших в КУП": "погибшие",
    "Количество травмированных в КУП": "травмированные",
    "Эвакуировано на пожаре": "эвакуация",
    "Эвакуировано детей": "эвакуация дети",
    "Спасено на пожаре": "спасенные",
    "Спасено детей": "спасенные дети",
}

FALLBACK_IMPORTANT_PATTERNS = [
    (["погибш"], "погибшие"),
    (["смерт"], "смерть"),
    (["гибел"], "гибель"),
    (["травм"], "травмы"),
    (["эваку"], "эвакуация"),
    (["спас"], "спасение"),
    (["эваку", "дет"], "эвакуация дети"),
    (["спас", "дет"], "спасенные дети"),
    (["ребен"], "дети"),
    (["дет"], "дети"),
]



_COLUMN_MATCHER: Optional["NatashaColumnMatcher"] = None


class NatashaColumnMatcher:
    """Переиспользуемый Natasha-поиск по названиям колонок."""

    def __init__(self):
        self.morph_vocab = MorphVocab()
        self.segmenter = Segmenter()
        self.emb = NewsEmbedding()
        self.morph_tagger = NewsMorphTagger(self.emb)
        self.important_lemmas = set()
        for keyword in IMPORTANT_KEYWORDS:
            for lemma in self._lemmatize_text(keyword):
                self.important_lemmas.add(lemma)

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
        text = str(value).lower().replace("ё", "е")
        text = re.sub(r"[_\-#/]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _match_fallback_pattern(self, normalized_name: str) -> Optional[str]:
        for parts, label in FALLBACK_IMPORTANT_PATTERNS:
            if all(part in normalized_name for part in parts):
                return label
        return None

    def classify_column(self, col_name: str) -> Optional[str]:
        normalized_name = self._normalize_text(col_name)
        exact_match = EXPLICIT_IMPORTANT_COLUMNS.get(str(col_name).strip())
        if exact_match:
            return exact_match

        lemma_match = None
        for word in re.findall(r"\w+", normalized_name):
            try:
                for lemma in self._lemmatize_text(word):
                    if lemma in self.important_lemmas:
                        lemma_match = lemma
                        break
                if lemma_match:
                    break
            except Exception:
                continue

        fallback_match = self._match_fallback_pattern(normalized_name)
        return lemma_match or fallback_match

    def _query_terms(self, query_text: str) -> List[Dict[str, Set[str]]]:
        normalized_query = self._normalize_text(query_text)
        terms: List[Dict[str, Set[str]]] = []
        for token in re.findall(r"\w+", normalized_query):
            if len(token) < 2:
                continue
            variants = {token}
            try:
                variants.update(self._lemmatize_text(token))
            except Exception:
                pass

            for parts, label in FALLBACK_IMPORTANT_PATTERNS:
                label_normalized = self._normalize_text(label)
                if label_normalized == token:
                    variants.update(parts)
                    continue

                matched_parts = {part for part in parts if token == part or token in part or part in token}
                if matched_parts:
                    variants.update(matched_parts)

            terms.append({"token": token, "variants": {variant for variant in variants if variant}})
        return terms

    def _column_terms(self, column_name: str) -> Dict[str, object]:
        normalized_name = self._normalize_text(column_name)
        words = {word for word in re.findall(r"\w+", normalized_name) if word}
        lemmas: Set[str] = set()
        for word in words:
            try:
                lemmas.update(self._lemmatize_text(word))
            except Exception:
                continue
        return {
            "normalized_name": normalized_name,
            "words": words,
            "lemmas": lemmas,
        }

    def _match_term(self, column_payload: Dict[str, object], variants: Set[str]) -> bool:
        normalized_name = str(column_payload["normalized_name"])
        words = column_payload["words"]
        lemmas = column_payload["lemmas"]
        return any(
            variant in normalized_name or
            variant in lemmas or
            any(word.startswith(variant) for word in words)
            for variant in variants
        )

    def find_columns_by_query(self, columns: List[str], query_text: str) -> List[Dict[str, object]]:
        query_terms = self._query_terms(query_text)
        if not query_terms:
            return []

        full_matches: List[Dict[str, object]] = []
        partial_matches: List[Dict[str, object]] = []

        for column_name in columns:
            column_payload = self._column_terms(column_name)
            matched_terms: List[str] = []
            score = 0

            for query_term in query_terms:
                if self._match_term(column_payload, query_term["variants"]):
                    matched_terms.append(str(query_term["token"]))
                    score += 4

            if not matched_terms:
                continue

            important_label = self.classify_column(column_name)
            if important_label:
                important_label_normalized = self._normalize_text(important_label)
                if any(term["token"] in important_label_normalized for term in query_terms):
                    score += 1

            item = {
                "name": column_name,
                "matched_terms": matched_terms,
                "score": score,
                "important_label": important_label or "",
            }
            if len(matched_terms) == len(query_terms):
                item["match_mode"] = "full"
                full_matches.append(item)
            else:
                item["match_mode"] = "partial"
                partial_matches.append(item)

        result = full_matches if full_matches else partial_matches
        result.sort(key=lambda item: (-len(item["matched_terms"]), -int(item["score"]), item["name"].lower()))
        return result



def get_column_matcher() -> NatashaColumnMatcher:
    global _COLUMN_MATCHER
    if _COLUMN_MATCHER is None:
        _COLUMN_MATCHER = NatashaColumnMatcher()
    return _COLUMN_MATCHER
class KeepImportantColumnsStep(PipelineStep):
    """
    Шаг интеллектуальной фильтрации колонок с использованием Natasha.
    Сохраняет колонки с важными признаками даже если profiling пометил их как candidate_to_drop.
    """

    def __init__(self):
        super().__init__("Keep Important Columns Report")

        try:
            self.morph_vocab = MorphVocab()
            self.segmenter = Segmenter()
            self.emb = NewsEmbedding()
            self.morph_tagger = NewsMorphTagger(self.emb)
        except Exception as exc:
            print(f"Ошибка инициализации Natasha: {exc}")
            raise

        self.important_lemmas = set()
        for keyword in IMPORTANT_KEYWORDS:
            for lemma in self._lemmatize_text(keyword):
                self.important_lemmas.add(lemma)

    def _lemmatize_text(self, value):
        lemmas = []
        doc = Doc(str(value))
        doc.segment(self.segmenter)
        doc.tag_morph(self.morph_tagger)
        for token in doc.tokens:
            token.lemmatize(self.morph_vocab)
            if token.lemma:
                lemmas.append(token.lemma)
        return lemmas

    def _normalize_text(self, value):
        text = str(value).lower().replace("ё", "е")
        text = re.sub(r"[_\-#/]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _match_fallback_pattern(self, normalized_name):
        for parts, label in FALLBACK_IMPORTANT_PATTERNS:
            if all(part in normalized_name for part in parts):
                return label
        return None

    def _is_important(self, col_name):
        normalized_name = self._normalize_text(col_name)

        exact_match = EXPLICIT_IMPORTANT_COLUMNS.get(str(col_name).strip())
        if exact_match:
            return exact_match

        lemma_match = None
        words = re.findall(r"\w+", normalized_name)
        for word in words:
            try:
                for lemma in self._lemmatize_text(word):
                    if lemma in self.important_lemmas:
                        lemma_match = lemma
                        break
                if lemma_match:
                    break
            except Exception:
                continue

        fallback_match = self._match_fallback_pattern(normalized_name)
        return lemma_match or fallback_match

    def run(self, settings):
        output_folder = settings.output_folder
        os.makedirs(output_folder, exist_ok=True)

        table_name = settings.project_name
        profile_csv = os.path.join(output_folder, f"{table_name}{PROFILING_CSV_SUFFIX}")
        updated_csv = os.path.join(output_folder, f"{table_name}_updated{PROFILING_CSV_SUFFIX}")
        updated_xlsx = os.path.join(output_folder, f"{table_name}_updated{PROFILING_XLSX_SUFFIX}")

        if not os.path.exists(profile_csv):
            raise FileNotFoundError(f"Не найден profiling report: {profile_csv}")

        profile_df = pd.read_csv(profile_csv)

        if "candidate_to_drop" not in profile_df.columns:
            raise KeyError("В отчете отсутствует колонка 'candidate_to_drop'")

        candidate_drop = profile_df[profile_df["candidate_to_drop"]]["column"].tolist()
        important_cols = {}

        for col in candidate_drop:
            match = self._is_important(col)
            if match:
                important_cols[col] = match

        if important_cols:
            profile_df.loc[profile_df["column"].isin(important_cols.keys()), "candidate_to_drop"] = False
            print("Важные колонки, сохраненные от удаления:")
            for col, match in important_cols.items():
                print(f"  - '{col}' -> '{match}'")
        else:
            print("Важных колонок для сохранения не найдено.")

        profile_df_sorted = profile_df.sort_values("candidate_to_drop", ascending=False)
        profile_df_sorted.to_csv(updated_csv, index=False, encoding="utf-8-sig")
        profile_df_sorted.to_excel(updated_xlsx, index=False, engine="openpyxl")
        print(f"Обновленный CSV: {updated_csv}")
        print(f"Обновленный XLSX: {updated_xlsx}")




