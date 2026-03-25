from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from natasha import MorphVocab, Doc, Segmenter, NewsEmbedding, NewsMorphTagger

from config.constants import PROFILING_CSV_SUFFIX, PROFILING_XLSX_SUFFIX
from core.processing.pipeline import PipelineStep


MANDATORY_FEATURE_REGISTRY = [
    {
        "id": "fire_date",
        "label": "Дата пожара",
        "description": "Дата возникновения пожара для сезонного анализа, ретроспективы и прогноза.",
        "synonyms": ["Дата возникновения пожара", "Дата пожара", "Дата возгорания", "Дата загорания"],
        "token_sets": [["дата", "возник", "пожар"], ["дата", "пожар"], ["дата", "возгоран"], ["дата", "загоран"]],
        "exclude_tokens": ["пм", "кнм", "заявлен", "сверк", "рождения"],
    },
    {
        "id": "district",
        "label": "Район",
        "description": "Административный или муниципальный район для территориальной аналитики.",
        "synonyms": ["Район", "Муниципальный район", "Административный район", "Район выезда подразделения", "Район пожара", "район города"],
        "token_sets": [["район"]],
        "exclude_tokens": [],
    },
    {
        "id": "locality",
        "label": "Населенный пункт",
        "description": "Населенный пункт или его устойчивый территориальный идентификатор.",
        "synonyms": ["Населенный пункт", "Населённый пункт", "ОКТМО. Текст", "Территориальная принадлежность"],
        "token_sets": [["населен", "пункт"], ["октмо"], ["территориальн", "принадлеж"]],
        "exclude_tokens": ["вид", "тип", "категор"],
    },
    {
        "id": "coordinates",
        "label": "Координаты",
        "description": "Широта, долгота или обобщенное поле координат для карт и геоаналитики.",
        "synonyms": ["Координаты", "Широта", "Долгота", "Latitude", "Longitude", "Lat", "Lon", "Lng", "GPS координаты"],
        "token_sets": [["координ"], ["широт"], ["долгот"], ["latitude"], ["longitude"], ["lat"], ["lon"], ["lng"], ["gps"]],
        "exclude_tokens": [],
    },
    {
        "id": "cause",
        "label": "Причина пожара",
        "description": "Причина пожара для профиля территории и риск-моделей.",
        "synonyms": ["Причина пожара (общая)", "Причина пожара для открытой территории", "Причина пожара для зданий (сооружений)", "Причина пожара", "Причина возгорания", "Причина"],
        "token_sets": [["причин", "пожар"], ["причин", "возгоран"], ["причин", "загора"], ["источник", "зажиган"]],
        "exclude_tokens": ["гибел"],
    },
    {
        "id": "object_category",
        "label": "Категория объекта",
        "description": "Категория или тип объекта пожара для сценарного анализа.",
        "synonyms": ["Категория объекта", "Категория объекта пожара", "Вид объекта", "Тип объекта", "Категория здания"],
        "token_sets": [["категор", "объект"], ["вид", "объект"], ["тип", "объект"], ["категор", "здан"], ["категор", "помещ"]],
        "exclude_tokens": [],
    },
    {
        "id": "report_time",
        "label": "Время сообщения",
        "description": "Время поступления сообщения о пожаре для оценки цепочки реагирования.",
        "synonyms": ["Время сообщения", "Время поступления сообщения", "Время вызова", "Время поступления сигнала"],
        "token_sets": [["время", "сообщ"], ["время", "вызов"], ["время", "поступлен", "сообщ"], ["время", "сигнал"]],
        "exclude_tokens": [],
    },
    {
        "id": "arrival_time",
        "label": "Время прибытия",
        "description": "Время прибытия подразделения на пожар для анализа доступности помощи.",
        "synonyms": ["Время прибытия 1-го ПП", "Время прибытия", "Время прибытия 1-го подразделения", "Время прибытия подразделения"],
        "token_sets": [["время", "прибыт"], ["прибыт", "пп"], ["прибыт", "подраздел"]],
        "exclude_tokens": [],
    },
    {
        "id": "distance_to_fire_station",
        "label": "Расстояние до пожарной части",
        "description": "Удаленность до ближайшей пожарной части для оценки времени прибытия и покрытия.",
        "synonyms": ["Удаленность от ближайшей ПЧ", "Удаленность до ближайшей ПЧ", "Расстояние до пожарной части", "Расстояние до ближайшей ПЧ"],
        "token_sets": [["удален", "пч"], ["расстоя", "пожарн", "част"], ["расстоя", "пч"]],
        "exclude_tokens": [],
    },
    {
        "id": "water_supply",
        "label": "Наличие водоснабжения",
        "description": "Наличие или описание водоснабжения на пожаре как фактор тяжести последствий.",
        "synonyms": ["Сведения о водоснабжении на пожаре", "Сведения о водоснабжении на пожар", "Количество записей о водоснабжении на пожаре", "Наличие водоснабжения", "Водоснабжение"],
        "token_sets": [["водоснабж"], ["водоисточ"], ["гидрант"], ["пожарн", "водоем"]],
        "exclude_tokens": [],
    },
    {
        "id": "fatalities",
        "label": "Погибшие",
        "description": "Количество погибших при пожаре для оценки тяжести происшествий.",
        "synonyms": ["Количество погибших в КУП", "Количество погибших", "Число погибших", "Кол-во погибших"],
        "token_sets": [["количеств", "погибш"]],
        "exclude_tokens": ["причин", "мест", "момент", "фио", "дата", "возраст", "пол", "уг", "сотрудник"],
    },
    {
        "id": "injuries",
        "label": "Травмированные",
        "description": "Количество травмированных при пожаре для оценки тяжести происшествий.",
        "synonyms": ["Количество травмированных в КУП", "Количество травмированных", "Число травмированных", "Кол-во травмированных"],
        "token_sets": [["количеств", "травм"]],
        "exclude_tokens": ["вид", "мест", "фио", "дата", "возраст", "пол", "ут", "сотрудник"],
    },
    {
        "id": "damage",
        "label": "Ущерб",
        "description": "Ущерб от пожара для оценки последствий и тяжести сценария.",
        "synonyms": ["Зарегистрированный ущерб от пожара", "Расчетный ущерб по пожару", "Материальный ущерб", "Прямой ущерб", "Ущерб"],
        "token_sets": [["ущерб"], ["материал", "ущерб"], ["прям", "ущерб"]],
        "exclude_tokens": [],
    },
    {
        "id": "settlement_type",
        "label": "Тип населенного пункта",
        "description": "Тип населенного пункта для выделения сельских территорий и контекста застройки.",
        "synonyms": ["Вид населенного пункта", "Вид населённого пункта", "Тип населенного пункта", "Тип населённого пункта", "Категория населенного пункта"],
        "token_sets": [["вид", "населен", "пункт"], ["тип", "населен", "пункт"], ["категор", "населен", "пункт"]],
        "exclude_tokens": [],
    },
]

LEGACY_EXPLICIT_IMPORTANT_COLUMNS = {
    "Количество погибших в КУП": "Погибшие",
    "Количество травмированных в КУП": "Травмированные",
    "Эвакуировано на пожаре": "Эвакуировано",
    "Эвакуировано детей": "Эвакуировано детей",
    "Спасено на пожаре": "Спасено",
    "Спасено детей": "Спасено детей",
}

KEYWORD_IMPORTANCE_RULES = [
    {"id": "casualty_flag", "label": "Есть травмированные или погибшие", "include_all": [["травм", "погиб"]], "exclude": []},
    {"id": "fatalities_keyword", "label": "Погибшие", "include_any": [["погибш"], ["смерт"], ["гибел"]], "exclude": ["причин", "мест", "момент", "фио", "дата", "возраст", "пол", "уг", "сотрудник"]},
    {"id": "injuries_keyword", "label": "Травмированные", "include_any": [["травм"]], "exclude": ["вид", "мест", "фио", "дата", "возраст", "пол", "ут", "сотрудник"]},
    {"id": "evacuated_children_keyword", "label": "Эвакуировано детей", "include_all": [["эваку", "дет"]], "exclude": []},
    {"id": "evacuated_keyword", "label": "Эвакуировано", "include_any": [["эваку"]], "exclude": ["дет"]},
    {"id": "rescued_children_keyword", "label": "Спасено детей", "include_all": [["спас", "дет"]], "exclude": []},
    {"id": "rescued_keyword", "label": "Спасено", "include_any": [["спас"]], "exclude": ["дет", "здан", "сооруж", "скот", "техник", "материал", "ценност"]},
]

FALLBACK_IMPORTANT_PATTERNS = [
    (["погибш"], "Погибшие"),
    (["смерт"], "Смерть"),
    (["гибел"], "Гибель"),
    (["травм"], "Травмы"),
    (["эваку"], "Эвакуация"),
    (["спас"], "Спасение"),
    (["эваку", "дет"], "Эвакуация детей"),
    (["спас", "дет"], "Спасено детей"),
    (["ребен"], "Дети"),
    (["дет"], "Дети"),
]

COLUMN_CATEGORY_RULES = [
    {"id": "dates", "label": "Даты и время", "description": "Дата пожара, время, месяц, год и другие временные признаки.", "keywords": ["дата", "время", "год", "месяц", "день", "час", "период"], "parts": ["дат", "времен", "год", "месяц", "день", "час", "период"]},
    {"id": "causes", "label": "Причины", "description": "Причины возгорания, источники и условия возникновения пожара.", "keywords": ["причина", "возгорание", "источник", "поджог", "аварийный режим"], "parts": ["причин", "возгоран", "источник", "поджог", "авар"]},
    {"id": "address", "label": "Адрес и география", "description": "Адрес, район, населенный пункт, улица и координаты.", "keywords": ["адрес", "район", "город", "улица", "дом", "населенный пункт", "широта", "долгота", "координата"], "parts": ["адрес", "район", "город", "улиц", "дом", "населен", "пункт", "широт", "долгот", "координат", "посел", "село", "деревн"]},
    {"id": "fire_metrics", "label": "Пожар и площадь", "description": "Площадь пожара, очаг, номер пожара и характеристики развития.", "keywords": ["пожар", "площадь", "очаг", "загорание"], "parts": ["пожар", "площад", "очаг", "загоран"]},
    {"id": "objects", "label": "Объект", "description": "Объект, здание, сооружение, категория и назначение.", "keywords": ["объект", "здание", "сооружение", "категория", "помещение", "квартира"], "parts": ["объект", "здан", "сооруж", "категор", "помещен", "квартир"]},
    {"id": "people", "label": "Люди и последствия", "description": "Погибшие, травмированные, дети, спасенные и эвакуированные.", "keywords": ["погибший", "травма", "ребенок", "эвакуация", "спасенный", "пострадавший"], "parts": ["погиб", "травм", "ребен", "дет", "эваку", "спас", "пострада"]},
    {"id": "damage", "label": "Ущерб и потери", "description": "Ущерб, уничтоженное и поврежденное имущество, техника, животные и ресурсы.", "keywords": ["ущерб", "уничтожено", "повреждено", "техника", "скот", "зерновые", "корма"], "parts": ["ущерб", "уничтож", "поврежд", "техник", "скот", "зерн", "корм", "площад"]},
    {"id": "response", "label": "Реагирование", "description": "Пожарная часть, удаленность, подразделение, силы и средства.", "keywords": ["пожарная часть", "удаленность", "подразделение", "силы", "средства"], "parts": ["пч", "удален", "подраздел", "сил", "средств"]},
]

PROTECTION_REPORT_DEFAULTS = {
    "profiling_candidate_to_drop": False,
    "mandatory_feature_detected": False,
    "protected_feature_id": "",
    "protected_feature_label": "",
    "protection_scope": "",
    "protection_rule": "",
    "protection_match": "",
    "protection_reason": "",
    "protected_from_drop": False,
}

PROTECTION_TEXT_COLUMNS = [
    "protected_feature_id",
    "protected_feature_label",
    "protection_scope",
    "protection_rule",
    "protection_match",
    "protection_reason",
]

PROTECTED_REPORT_COLUMNS = [
    "column",
    "dtype",
    "profiling_candidate_to_drop",
    "candidate_to_drop",
    "mandatory_feature_detected",
    "protected_feature_id",
    "protected_feature_label",
    "protection_scope",
    "protection_rule",
    "protection_match",
    "protection_reason",
    "drop_reasons",
]

_COLUMN_MATCHER: Optional["NatashaColumnMatcher"] = None

class NatashaColumnMatcher:
    """Переиспользуемый Natasha-поиск и доменный матчер по названиям колонок."""

    def __init__(self):
        self.morph_vocab = MorphVocab()
        self.segmenter = Segmenter()
        self.emb = NewsEmbedding()
        self.morph_tagger = NewsMorphTagger(self.emb)
        self.category_lemmas: Dict[str, Set[str]] = {}
        for rule in COLUMN_CATEGORY_RULES:
            lemmas: Set[str] = set()
            for keyword in rule["keywords"]:
                try:
                    lemmas.update(self._lemmatize_text(keyword))
                except Exception:
                    continue
            self.category_lemmas[rule["id"]] = lemmas

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
        text = str(value).lower().replace("ё", "е")
        text = re.sub(r"[_/#-]+", " ", text)
        text = re.sub(r"[^\w\s]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _extract_words(self, value: str) -> List[str]:
        return [word for word in re.findall(r"\w+", value) if word]

    def _prepare_registry_feature(self, feature: Dict[str, Any]) -> Dict[str, Any]:
        synonyms = []
        for synonym in feature.get("synonyms", []):
            normalized = self._normalize_text(synonym)
            synonyms.append({"raw": synonym, "normalized": normalized, "tokens": self._extract_words(normalized)})

        token_sets = [[self._normalize_text(token) for token in token_set if token] for token_set in feature.get("token_sets", [])]
        exclude_tokens = [self._normalize_text(token) for token in feature.get("exclude_tokens", []) if token]

        return {
            **feature,
            "prepared_synonyms": synonyms,
            "prepared_token_sets": token_sets,
            "prepared_exclude_tokens": exclude_tokens,
        }

    def _match_fallback_pattern(self, normalized_name: str) -> Optional[str]:
        for parts, label in FALLBACK_IMPORTANT_PATTERNS:
            if all(part in normalized_name for part in parts):
                return label
        return None

    def _column_terms(self, column_name: str) -> Dict[str, object]:
        normalized_name = self._normalize_text(column_name)
        words = {word for word in self._extract_words(normalized_name) if word}
        lemmas: Set[str] = set()
        for word in words:
            try:
                lemmas.update(self._lemmatize_text(word))
            except Exception:
                continue
        return {
            "original_name": str(column_name),
            "normalized_name": normalized_name,
            "words": words,
            "lemmas": lemmas,
        }

    def _contains_fragment(self, column_payload: Dict[str, object], fragment: str) -> bool:
        normalized_name = str(column_payload["normalized_name"])
        words = {str(word) for word in column_payload["words"]}
        lemmas = {str(lemma) for lemma in column_payload["lemmas"]}
        return bool(
            fragment
            and (
                fragment in normalized_name
                or any(fragment in word for word in words)
                or any(fragment in lemma for lemma in lemmas)
            )
        )

    def _matches_token_set(self, column_payload: Dict[str, object], token_set: List[str]) -> bool:
        return bool(token_set) and all(self._contains_fragment(column_payload, token) for token in token_set)

    def _has_excluded_token(self, column_payload: Dict[str, object], exclude_tokens: List[str]) -> bool:
        return any(self._contains_fragment(column_payload, token) for token in exclude_tokens)

    def _build_match(
        self,
        *,
        scope: str,
        feature_id: str,
        feature_label: str,
        rule_id: str,
        matched_value: str,
        reason: str,
        mandatory: bool,
    ) -> Dict[str, Any]:
        return {
            "scope": scope,
            "feature_id": feature_id,
            "feature_label": feature_label,
            "rule_id": rule_id,
            "matched_value": matched_value,
            "reason": reason,
            "mandatory": mandatory,
        }

    def _match_mandatory_feature(self, column_payload: Dict[str, object]) -> Optional[Dict[str, Any]]:
        normalized_name = str(column_payload["normalized_name"])
        for feature in self.mandatory_registry:
            exclude_tokens = feature.get("prepared_exclude_tokens", [])
            if exclude_tokens and self._has_excluded_token(column_payload, exclude_tokens):
                continue

            for synonym in feature.get("prepared_synonyms", []):
                if normalized_name == synonym["normalized"]:
                    return self._build_match(
                        scope="mandatory_registry",
                        feature_id=str(feature["id"]),
                        feature_label=str(feature["label"]),
                        rule_id="mandatory_registry_exact",
                        matched_value=str(synonym["raw"]),
                        reason=f"Колонка совпала с обязательным признаком '{feature['label']}' по точному имени.",
                        mandatory=True,
                    )

            for synonym in feature.get("prepared_synonyms", []):
                tokens = list(synonym.get("tokens") or [])
                if len(tokens) > 1 and all(self._contains_fragment(column_payload, token) for token in tokens):
                    return self._build_match(
                        scope="mandatory_registry",
                        feature_id=str(feature["id"]),
                        feature_label=str(feature["label"]),
                        rule_id="mandatory_registry_synonym",
                        matched_value=str(synonym["raw"]),
                        reason=f"Колонка защищена обязательным реестром по синониму '{synonym['raw']}'.",
                        mandatory=True,
                    )

            for token_set in feature.get("prepared_token_sets", []):
                if self._matches_token_set(column_payload, token_set):
                    joined_tokens = " + ".join(token_set)
                    return self._build_match(
                        scope="mandatory_registry",
                        feature_id=str(feature["id"]),
                        feature_label=str(feature["label"]),
                        rule_id="mandatory_registry_tokens",
                        matched_value=joined_tokens,
                        reason=f"Колонка защищена обязательным реестром по доменным токенам '{joined_tokens}'.",
                        mandatory=True,
                    )
        return None

    def _match_legacy_explicit(self, column_payload: Dict[str, object]) -> Optional[Dict[str, Any]]:
        original_name = str(column_payload["original_name"]).strip()
        exact_match = LEGACY_EXPLICIT_IMPORTANT_COLUMNS.get(original_name)
        if not exact_match:
            return None

        return self._build_match(
            scope="legacy_explicit",
            feature_id="",
            feature_label=str(exact_match),
            rule_id="legacy_explicit_exact",
            matched_value=original_name,
            reason=f"Колонка сохранена по legacy-правилу точного совпадения '{original_name}'.",
            mandatory=False,
        )

    def _match_keyword_rule(self, column_payload: Dict[str, object]) -> Optional[Dict[str, Any]]:
        for rule in KEYWORD_IMPORTANCE_RULES:
            exclude_tokens = [self._normalize_text(token) for token in rule.get("exclude", []) if token]
            if exclude_tokens and self._has_excluded_token(column_payload, exclude_tokens):
                continue

            for token_set in rule.get("include_all", []):
                if self._matches_token_set(column_payload, token_set):
                    joined_tokens = " + ".join(token_set)
                    return self._build_match(
                        scope="keyword_rule",
                        feature_id=str(rule.get("id") or ""),
                        feature_label=str(rule.get("label") or ""),
                        rule_id="keyword_include_all",
                        matched_value=joined_tokens,
                        reason=f"Колонка сохранена по keyword-правилу с обязательным набором токенов '{joined_tokens}'.",
                        mandatory=False,
                    )

            for token_set in rule.get("include_any", []):
                if self._matches_token_set(column_payload, token_set):
                    joined_tokens = " + ".join(token_set)
                    return self._build_match(
                        scope="keyword_rule",
                        feature_id=str(rule.get("id") or ""),
                        feature_label=str(rule.get("label") or ""),
                        rule_id="keyword_include_any",
                        matched_value=joined_tokens,
                        reason=f"Колонка сохранена по keyword-правилу с токеном '{joined_tokens}'.",
                        mandatory=False,
                    )
        return None

    def match_column_metadata(self, col_name: str) -> Optional[Dict[str, Any]]:
        column_payload = self._column_terms(col_name)
        for matcher in (self._match_mandatory_feature, self._match_legacy_explicit, self._match_keyword_rule):
            match = matcher(column_payload)
            if match:
                return match
        return None

    def classify_column(self, col_name: str) -> Optional[str]:
        match = self.match_column_metadata(col_name)
        if match:
            return str(match.get("feature_label") or "") or None
        return None

    def _query_terms(self, query_text: str) -> List[Dict[str, Set[str]]]:
        normalized_query = self._normalize_text(query_text)
        terms: List[Dict[str, Set[str]]] = []
        for token in self._extract_words(normalized_query):
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

    def _match_term(self, column_payload: Dict[str, object], variants: Set[str]) -> bool:
        normalized_name = str(column_payload["normalized_name"])
        words = {str(word) for word in column_payload["words"]}
        lemmas = {str(lemma) for lemma in column_payload["lemmas"]}
        return any(
            variant in normalized_name
            or variant in lemmas
            or any(word.startswith(variant) for word in words)
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

    def get_mandatory_feature_catalog(self) -> List[Dict[str, object]]:
        return [
            {
                "id": feature["id"],
                "label": feature["label"],
                "description": feature["description"],
                "synonyms": list(feature.get("synonyms", [])),
            }
            for feature in MANDATORY_FEATURE_REGISTRY
        ]

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
                    "group_labels": [rule["label"] for rule in COLUMN_CATEGORY_RULES if rule["id"] in matched_groups],
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


def get_mandatory_feature_catalog() -> List[Dict[str, object]]:
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

class KeepImportantColumnsStep(PipelineStep):
    """
    Шаг интеллектуальной фильтрации колонок.
    Сохраняет обязательные доменные признаки и legacy-важные колонки,
    даже если profiling пометил их как candidate_to_drop.
    """

    def __init__(self):
        super().__init__("Keep Important Columns Report")
        self.matcher = get_column_matcher()

    def _coerce_bool_series(self, series: pd.Series) -> pd.Series:
        if str(series.dtype) == "bool":
            return series.fillna(False)
        normalized = series.astype(str).str.strip().str.lower()
        return normalized.isin(["true", "1", "yes"])

    def _coerce_text_series(self, series: pd.Series) -> pd.Series:
        return series.astype("string").fillna("").astype(object)

    def _ensure_report_columns(self, profile_df: pd.DataFrame) -> pd.DataFrame:
        for column_name, default_value in PROTECTION_REPORT_DEFAULTS.items():
            if column_name not in profile_df.columns:
                profile_df[column_name] = default_value
        for column_name in PROTECTION_TEXT_COLUMNS:
            if column_name in profile_df.columns:
                profile_df[column_name] = self._coerce_text_series(profile_df[column_name])
        return profile_df

    def _build_protected_report(self, profile_df: pd.DataFrame) -> pd.DataFrame:
        protected_df = profile_df.loc[profile_df["protected_from_drop"] == True].copy()
        for column_name in PROTECTED_REPORT_COLUMNS:
            if column_name not in protected_df.columns:
                protected_df[column_name] = "" if column_name not in {"profiling_candidate_to_drop", "candidate_to_drop", "mandatory_feature_detected", "protected_from_drop"} else False
        return protected_df[PROTECTED_REPORT_COLUMNS].sort_values(
            by=["mandatory_feature_detected", "protected_feature_label", "column"],
            ascending=[False, True, True],
        )

    def run(self, settings):
        output_folder = settings.output_folder
        os.makedirs(output_folder, exist_ok=True)

        if hasattr(settings, "selected_table") and settings.selected_table:
            table_name = settings.selected_table
        else:
            table_name = settings.project_name

        profile_csv = os.path.join(output_folder, f"{table_name}{PROFILING_CSV_SUFFIX}")
        updated_csv = os.path.join(output_folder, f"{table_name}_updated{PROFILING_CSV_SUFFIX}")
        updated_xlsx = os.path.join(output_folder, f"{table_name}_updated{PROFILING_XLSX_SUFFIX}")
        protected_csv = os.path.join(output_folder, f"{table_name}_protected_columns_report.csv")
        protected_xlsx = os.path.join(output_folder, f"{table_name}_protected_columns_report.xlsx")

        if not os.path.exists(profile_csv):
            raise FileNotFoundError(f"Не найден отчёт профилирования: {profile_csv}")

        profile_df = pd.read_csv(profile_csv)
        profile_df = self._ensure_report_columns(profile_df.copy())

        if "candidate_to_drop" not in profile_df.columns:
            raise KeyError("В отчете отсутствует колонка 'candidate_to_drop'")

        profile_df["candidate_to_drop"] = self._coerce_bool_series(profile_df["candidate_to_drop"])
        profile_df["profiling_candidate_to_drop"] = self._coerce_bool_series(profile_df["profiling_candidate_to_drop"])
        profile_df["mandatory_feature_detected"] = self._coerce_bool_series(profile_df["mandatory_feature_detected"])
        profile_df["protected_from_drop"] = self._coerce_bool_series(profile_df["protected_from_drop"])

        protected_columns: List[Dict[str, Any]] = []
        for idx, row in profile_df.iterrows():
            column_name = str(row.get("column") or "").strip()
            if not column_name:
                continue

            match = self.matcher.match_column_metadata(column_name)
            if not match:
                continue

            profile_df.at[idx, "mandatory_feature_detected"] = bool(match.get("mandatory"))
            profile_df.at[idx, "protected_feature_id"] = str(match.get("feature_id") or "")
            profile_df.at[idx, "protected_feature_label"] = str(match.get("feature_label") or "")
            profile_df.at[idx, "protection_scope"] = str(match.get("scope") or "")
            profile_df.at[idx, "protection_rule"] = str(match.get("rule_id") or "")
            profile_df.at[idx, "protection_match"] = str(match.get("matched_value") or "")
            profile_df.at[idx, "protection_reason"] = str(match.get("reason") or "")

            if bool(profile_df.at[idx, "profiling_candidate_to_drop"]):
                profile_df.at[idx, "candidate_to_drop"] = False
                profile_df.at[idx, "protected_from_drop"] = True
                protected_columns.append(
                    {
                        "column": column_name,
                        "protected_feature_id": str(match.get("feature_id") or ""),
                        "protected_feature_label": str(match.get("feature_label") or ""),
                        "mandatory_feature_detected": bool(match.get("mandatory")),
                        "protection_scope": str(match.get("scope") or ""),
                        "protection_rule": str(match.get("rule_id") or ""),
                        "protection_match": str(match.get("matched_value") or ""),
                        "protection_reason": str(match.get("reason") or ""),
                        "drop_reasons": row.get("drop_reasons", []),
                    }
                )

        profile_df_sorted = profile_df.sort_values(
            by=["protected_from_drop", "candidate_to_drop", "null_ratio", "dominant_ratio"],
            ascending=[False, False, False, False],
        )
        protected_df = self._build_protected_report(profile_df_sorted)

        profile_df_sorted.to_csv(updated_csv, index=False, encoding="utf-8-sig")
        profile_df_sorted.to_excel(updated_xlsx, index=False, engine="openpyxl")
        protected_df.to_csv(protected_csv, index=False, encoding="utf-8-sig")
        protected_df.to_excel(protected_xlsx, index=False, engine="openpyxl")

        if protected_columns:
            print("Защищенные признаки от удаления:")
            for item in protected_columns:
                print(
                    "  - "
                    f"'{item['column']}' -> '{item['protected_feature_label']}' "
                    f"[{item['protection_rule']}; match={item['protection_match']}]"
                )
        else:
            print("Защищенных признаков не найдено.")

        print(f"Обновленный CSV: {updated_csv}")
        print(f"Обновленный XLSX: {updated_xlsx}")
        print(f"Отчет по защищенным признакам CSV: {protected_csv}")
        print(f"Отчет по защищенным признакам XLSX: {protected_xlsx}")

        return {
            "updated_csv": updated_csv,
            "updated_xlsx": updated_xlsx,
            "protected_report_csv": protected_csv,
            "protected_report_xlsx": protected_xlsx,
            "protected_columns": protected_columns,
            "protected_count": len(protected_columns),
            "mandatory_feature_catalog": self.matcher.get_mandatory_feature_catalog(),
        }
