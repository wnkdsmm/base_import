from __future__ import annotations

from app.domain.analytics_metadata import IMPACT_METRIC_CONFIG
from app.domain.fire_columns import (
    BUILDING_CATEGORY_COLUMN,
    CAUSE_COLUMN_CANDIDATES,
    COORDINATE_COLUMN_CANDIDATES,
    DATE_COLUMN,
    DEATHS_COLUMN_CANDIDATES,
    DISTRICT_COLUMN_CANDIDATES,
    FIRE_STATION_DISTANCE_COLUMN,
    FIRE_STATION_DISTANCE_COLUMN_CANDIDATES,
    FIRE_DATE_COLUMN_CANDIDATES,
    INJURIES_COLUMN_CANDIDATES,
    LOCALITY_COLUMN_CANDIDATES,
    OBJECT_CATEGORY_COLUMN_CANDIDATES,
    REGISTERED_DAMAGE_COLUMN,
    REPORT_TIME_COLUMN_CANDIDATES,
    ARRIVAL_TIME_COLUMN_CANDIDATES,
    SETTLEMENT_TYPE_COLUMN_CANDIDATES,
    WATER_SUPPLY_COLUMN_CANDIDATES,
)


def _impact_keyword_rule(rule_id: str, metric_id: str) -> dict[str, object]:
    config = IMPACT_METRIC_CONFIG[metric_id]
    rule: dict[str, object] = {
        "id": rule_id,
        "label": str(config["label"]),
        "exclude": list(config.get("exclude", [])),
    }
    if "include_all" in config:
        rule["include_all"] = [list(parts) for parts in config["include_all"]]
    if "include_any" in config:
        rule["include_any"] = [list(parts) for parts in config["include_any"]]
    return rule


MANDATORY_FEATURE_REGISTRY = [
    {
        "id": "fire_date",
        "label": "Дата пожара",
        "description": "Дата возникновения пожара для сезонного анализа, ретроспективы и прогноза.",
        "synonyms": list(FIRE_DATE_COLUMN_CANDIDATES),
        "token_sets": [["дата", "возник", "пожар"], ["дата", "пожар"], ["дата", "возгоран"], ["дата", "загоран"]],
        "exclude_tokens": ["пм", "кнм", "заявлен", "сверк", "рождения"],
    },
    {
        "id": "district",
        "label": "Район",
        "description": "Административный или муниципальный район для территориальной аналитики.",
        "synonyms": [
            "Район",
            "Муниципальный район",
            "Административный район",
            "Район выезда подразделения",
            "Район пожара",
            "район города",
        ],
        "token_sets": [["район"]],
        "exclude_tokens": [],
    },
    {
        "id": "locality",
        "label": "Населенный пункт",
        "description": "Населенный пункт или его устойчивый территориальный идентификатор.",
        "synonyms": list(LOCALITY_COLUMN_CANDIDATES),
        "token_sets": [["населен", "пункт"], ["октмо"], ["территориальн", "принадлеж"]],
        "exclude_tokens": ["вид", "тип", "категор"],
    },
    {
        "id": "coordinates",
        "label": "Координаты",
        "description": "Широта, долгота или обобщенное поле координат для карт и геоаналитики.",
        "synonyms": list(COORDINATE_COLUMN_CANDIDATES),
        "token_sets": [["координ"], ["широт"], ["долгот"], ["latitude"], ["longitude"], ["lat"], ["lon"], ["lng"], ["gps"]],
        "exclude_tokens": [],
    },
    {
        "id": "cause",
        "label": "Причина пожара",
        "description": "Причина пожара для профиля территории и риск-моделей.",
        "synonyms": [*CAUSE_COLUMN_CANDIDATES, "Причина возгорания"],
        "token_sets": [["причин", "пожар"], ["причин", "возгоран"], ["причин", "загора"], ["источник", "зажиган"]],
        "exclude_tokens": ["гибел"],
    },
    {
        "id": "object_category",
        "label": "Категория объекта",
        "description": "Категория или тип объекта пожара для сценарного анализа.",
        "synonyms": [*OBJECT_CATEGORY_COLUMN_CANDIDATES, "Вид объекта", "Тип объекта", BUILDING_CATEGORY_COLUMN],
        "token_sets": [["категор", "объект"], ["вид", "объект"], ["тип", "объект"], ["категор", "здан"], ["категор", "помещ"]],
        "exclude_tokens": [],
    },
    {
        "id": "report_time",
        "label": "Время сообщения",
        "description": "Время поступления сообщения о пожаре для оценки цепочки реагирования.",
        "synonyms": [*REPORT_TIME_COLUMN_CANDIDATES, "Время поступления сообщения", "Время вызова", "Время поступления сигнала"],
        "token_sets": [["время", "сообщ"], ["время", "вызов"], ["время", "поступлен", "сообщ"], ["время", "сигнал"]],
        "exclude_tokens": [],
    },
    {
        "id": "arrival_time",
        "label": "Время прибытия",
        "description": "Время прибытия подразделения на пожар для анализа доступности помощи.",
        "synonyms": [*ARRIVAL_TIME_COLUMN_CANDIDATES, "Время прибытия", "Время прибытия 1-го подразделения", "Время прибытия подразделения"],
        "token_sets": [["время", "прибыт"], ["прибыт", "пп"], ["прибыт", "подраздел"]],
        "exclude_tokens": [],
    },
    {
        "id": "distance_to_fire_station",
        "label": "Расстояние до пожарной части",
        "description": "Удаленность до ближайшей пожарной части для оценки времени прибытия и покрытия.",
        "synonyms": [*FIRE_STATION_DISTANCE_COLUMN_CANDIDATES, "Расстояние до пожарной части", "Расстояние до ближайшей ПЧ"],
        "token_sets": [["удален", "пч"], ["расстоя", "пожарн", "част"], ["расстоя", "пч"]],
        "exclude_tokens": [],
    },
    {
        "id": "water_supply",
        "label": "Наличие водоснабжения",
        "description": "Наличие или описание водоснабжения на пожаре как фактор тяжести последствий.",
        "synonyms": list(WATER_SUPPLY_COLUMN_CANDIDATES),
        "token_sets": [["водоснабж"], ["водоисточ"], ["гидрант"], ["пожарн", "водоем"]],
        "exclude_tokens": [],
    },
    {
        "id": "fatalities",
        "label": "Погибшие",
        "description": "Количество погибших при пожаре для оценки тяжести происшествий.",
        "synonyms": [*DEATHS_COLUMN_CANDIDATES, "Количество погибших", "Число погибших", "Кол-во погибших"],
        "token_sets": [["количеств", "погибш"]],
        "exclude_tokens": ["причин", "мест", "момент", "фио", "дата", "возраст", "пол", "уг", "сотрудник"],
    },
    {
        "id": "injuries",
        "label": "Травмированные",
        "description": "Количество травмированных при пожаре для оценки тяжести происшествий.",
        "synonyms": [*INJURIES_COLUMN_CANDIDATES, "Количество травмированных", "Число травмированных", "Кол-во травмированных"],
        "token_sets": [["количеств", "травм"]],
        "exclude_tokens": ["вид", "мест", "фио", "дата", "возраст", "пол", "ут", "сотрудник"],
    },
    {
        "id": "damage",
        "label": "Ущерб",
        "description": "Ущерб от пожара для оценки последствий и тяжести сценария.",
        "synonyms": [REGISTERED_DAMAGE_COLUMN, "Расчетный ущерб по пожару", "Материальный ущерб", "Прямой ущерб", "Ущерб"],
        "token_sets": [["ущерб"], ["материал", "ущерб"], ["прям", "ущерб"]],
        "exclude_tokens": [],
    },
    {
        "id": "settlement_type",
        "label": "Тип населенного пункта",
        "description": "Тип населенного пункта для выделения сельских территорий и контекста застройки.",
        "synonyms": [*SETTLEMENT_TYPE_COLUMN_CANDIDATES, "Тип населенного пункта", "Тип населённого пункта", "Категория населенного пункта"],
        "token_sets": [["вид", "населен", "пункт"], ["тип", "населен", "пункт"], ["категор", "населен", "пункт"]],
        "exclude_tokens": [],
    },
]

LEGACY_EXPLICIT_IMPORTANT_COLUMNS = {
    IMPACT_METRIC_CONFIG["deaths"]["preferred"][0]: IMPACT_METRIC_CONFIG["deaths"]["label"],
    IMPACT_METRIC_CONFIG["injuries"]["preferred"][0]: IMPACT_METRIC_CONFIG["injuries"]["label"],
    IMPACT_METRIC_CONFIG["evacuated"]["preferred"][0]: IMPACT_METRIC_CONFIG["evacuated"]["label"],
    IMPACT_METRIC_CONFIG["evacuated_children"]["preferred"][0]: IMPACT_METRIC_CONFIG["evacuated_children"]["label"],
    IMPACT_METRIC_CONFIG["rescued_total"]["preferred"][0]: IMPACT_METRIC_CONFIG["rescued_total"]["label"],
    IMPACT_METRIC_CONFIG["rescued_children"]["preferred"][0]: IMPACT_METRIC_CONFIG["rescued_children"]["label"],
}

KEYWORD_IMPORTANCE_RULES = [
    {
        "id": "casualty_flag",
        "label": "Есть травмированные или погибшие",
        "include_all": [["травм", "погиб"]],
        "exclude": [],
    },
    _impact_keyword_rule("fatalities_keyword", "deaths"),
    _impact_keyword_rule("injuries_keyword", "injuries"),
    _impact_keyword_rule("evacuated_children_keyword", "evacuated_children"),
    _impact_keyword_rule("evacuated_keyword", "evacuated"),
    _impact_keyword_rule("rescued_children_keyword", "rescued_children"),
    _impact_keyword_rule("rescued_keyword", "rescued_total"),
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
    {
        "id": "dates",
        "label": "Даты и время",
        "description": "Дата пожара, время, месяц, год и другие временные признаки.",
        "keywords": ["дата", "время", "год", "месяц", "день", "час", "период"],
        "parts": ["дат", "времен", "год", "месяц", "день", "час", "период"],
    },
    {
        "id": "causes",
        "label": "Причины",
        "description": "Причины возгорания, источники и условия возникновения пожара.",
        "keywords": ["причина", "возгорание", "источник", "поджог", "аварийный режим"],
        "parts": ["причин", "возгоран", "источник", "поджог", "авар"],
    },
    {
        "id": "address",
        "label": "Адрес и география",
        "description": "Адрес, район, населенный пункт, улица и координаты.",
        "keywords": ["адрес", "район", "город", "улица", "дом", "населенный пункт", "широта", "долгота", "координата"],
        "parts": ["адрес", "район", "город", "улиц", "дом", "населен", "пункт", "широт", "долгот", "координат", "посел", "село", "деревн"],
    },
    {
        "id": "fire_metrics",
        "label": "Пожар и площадь",
        "description": "Площадь пожара, очаг, номер пожара и характеристики развития.",
        "keywords": ["пожар", "площадь", "очаг", "загорание"],
        "parts": ["пожар", "площад", "очаг", "загоран"],
    },
    {
        "id": "objects",
        "label": "Объект",
        "description": "Объект, здание, сооружение, категория и назначение.",
        "keywords": ["объект", "здание", "сооружение", "категория", "помещение", "квартира"],
        "parts": ["объект", "здан", "сооруж", "категор", "помещен", "квартир"],
    },
    {
        "id": "people",
        "label": "Люди и последствия",
        "description": "Погибшие, травмированные, дети, спасенные и эвакуированные.",
        "keywords": ["погибший", "травма", "ребенок", "эвакуация", "спасенный", "пострадавший"],
        "parts": ["погиб", "травм", "ребен", "дет", "эваку", "спас", "пострада"],
    },
    {
        "id": "damage",
        "label": "Ущерб и потери",
        "description": "Ущерб, уничтоженное и поврежденное имущество, техника, животные и ресурсы.",
        "keywords": ["ущерб", "уничтожено", "повреждено", "техника", "скот", "зерновые", "корма"],
        "parts": ["ущерб", "уничтож", "поврежд", "техник", "скот", "зерн", "корм", "площад"],
    },
    {
        "id": "response",
        "label": "Реагирование",
        "description": "Пожарная часть, удаленность, подразделение, силы и средства.",
        "keywords": ["пожарная часть", "удаленность", "подразделение", "силы", "средства"],
        "parts": ["пч", "удален", "подраздел", "сил", "средств"],
    },
]


def get_mandatory_feature_catalog() -> list[dict[str, object]]:
    return [
        {
            "id": feature["id"],
            "label": feature["label"],
            "description": feature["description"],
            "synonyms": list(feature.get("synonyms", [])),
        }
        for feature in MANDATORY_FEATURE_REGISTRY
    ]
