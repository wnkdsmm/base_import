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

COLUMN_CATEGORY_RULES = [
    {
        "id": "dates",
        "label": "Даты и время",
        "description": "Дата пожара, время, месяц, год, период",
        "keywords": ["дата", "время", "год", "месяц", "день", "час", "период"],
        "parts": ["дат", "времен", "год", "месяц", "день", "час", "период"],
    },
    {
        "id": "causes",
        "label": "Причины",
        "description": "Причины возгорания, источник, условия возникновения",
        "keywords": ["причина", "возгорание", "источник", "поджог", "аварийный режим"],
        "parts": ["причин", "возгоран", "источник", "поджог", "авар"],
    },
    {
        "id": "address",
        "label": "Адрес и география",
        "description": "Адрес, район, населенный пункт, улица, координаты",
        "keywords": ["адрес", "район", "город", "улица", "дом", "населенный пункт", "широта", "долгота", "координата"],
        "parts": ["адрес", "район", "город", "улиц", "дом", "населен", "пункт", "широт", "долгот", "координат", "посел", "село", "деревн"],
    },
    {
        "id": "fire_metrics",
        "label": "Пожар и площадь",
        "description": "Площадь пожара, номер, вид, развитие пожара",
        "keywords": ["пожар", "площадь", "очаг", "загорание"],
        "parts": ["пожар", "площад", "очаг", "загоран"],
    },
    {
        "id": "objects",
        "label": "Объект",
        "description": "Объект, здание, сооружение, категория, назначение",
        "keywords": ["объект", "здание", "сооружение", "категория", "помещение", "квартира"],
        "parts": ["объект", "здан", "сооруж", "категор", "помещен", "квартир"],
    },
    {
        "id": "people",
        "label": "Люди и последствия",
        "description": "Погибшие, травмированные, дети, спасенные, эвакуированные",
        "keywords": ["погибший", "травма", "ребенок", "эвакуация", "спасенный", "пострадавший"],
        "parts": ["погиб", "травм", "ребен", "дет", "эваку", "спас", "пострада"],
    },
    {
        "id": "damage",
        "label": "Ущерб и потери",
        "description": "Ущерб, уничтожено, повреждено, техника, скот, зерновые",
        "keywords": ["ущерб", "уничтожено", "повреждено", "техника", "скот", "зерновые", "корма"],
        "parts": ["ущерб", "уничтож", "поврежд", "техник", "скот", "зерн", "корм", "площад"],
    },
    {
        "id": "response",
        "label": "Реагирование",
        "description": "Пожарная часть, подразделение, расстояние, силы и средства",
        "keywords": ["пожарная часть", "удаленность", "подразделение", "силы", "средства"],
        "parts": ["пч", "удален", "подраздел", "сил", "средств"],
    },
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

        self.category_lemmas: Dict[str, Set[str]] = {}
        for rule in COLUMN_CATEGORY_RULES:
            lemmas: Set[str] = set()
            for keyword in rule["keywords"]:
                try:
                    lemmas.update(self._lemmatize_text(keyword))
                except Exception:
                    continue
            self.category_lemmas[rule["id"]] = lemmas

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
            any(str(word).startswith(variant) for word in words)
            for variant in variants
        )

    def _matches_category(self, column_payload: Dict[str, object], category_rule: Dict[str, object]) -> bool:
        normalized_name = str(column_payload["normalized_name"])
        words = {str(word) for word in column_payload["words"]}
        lemmas = {str(lemma) for lemma in column_payload["lemmas"]}

        if any(part in normalized_name for part in category_rule["parts"]):
            return True

        for keyword in category_rule["keywords"]:
            keyword_normalized = self._normalize_text(keyword)
            if keyword_normalized in normalized_name:
                return True

        category_lemmas = self.category_lemmas.get(category_rule["id"], set())
        if category_lemmas.intersection(lemmas):
            return True

        return bool(category_lemmas.intersection(words))

    def classify_column_groups(self, column_name: str) -> List[str]:
        column_payload = self._column_terms(column_name)
        group_ids: List[str] = []
        for rule in COLUMN_CATEGORY_RULES:
            if self._matches_category(column_payload, rule):
                group_ids.append(rule["id"])
        return group_ids

    def get_group_catalog(self, columns: List[str]) -> List[Dict[str, object]]:
        grouped_columns: Dict[str, List[str]] = {rule["id"]: [] for rule in COLUMN_CATEGORY_RULES}

        for column_name in columns:
            for group_id in self.classify_column_groups(column_name):
                grouped_columns[group_id].append(column_name)

        catalog: List[Dict[str, object]] = []
        for rule in COLUMN_CATEGORY_RULES:
            group_columns = grouped_columns[rule["id"]]
            catalog.append(
                {
                    "id": rule["id"],
                    "label": rule["label"],
                    "description": rule["description"],
                    "count": len(group_columns),
                    "columns": group_columns,
                }
            )
        return catalog

    def find_columns_by_categories(self, columns: List[str], group_ids: List[str]) -> List[Dict[str, object]]:
        wanted = {group_id for group_id in group_ids if group_id}
        if not wanted:
            return []

        result: List[Dict[str, object]] = []
        for column_name in columns:
            matched_groups = [group_id for group_id in self.classify_column_groups(column_name) if group_id in wanted]
            if not matched_groups:
                continue
            result.append(
                {
                    "name": column_name,
                    "group_ids": matched_groups,
                    "group_labels": [
                        rule["label"]
                        for rule in COLUMN_CATEGORY_RULES
                        if rule["id"] in matched_groups
                    ],
                    "important_label": self.classify_column(column_name) or "",
                }
            )
        return result

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

            group_ids = self.classify_column_groups(column_name)
            item = {
                "name": column_name,
                "matched_terms": matched_terms,
                "score": score,
                "important_label": important_label or "",
                "group_ids": group_ids,
                "group_labels": [rule["label"] for rule in COLUMN_CATEGORY_RULES if rule["id"] in group_ids],
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
        self.matcher = get_column_matcher()

    def _is_important(self, col_name):
        return self.matcher.classify_column(col_name)

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
