from __future__ import annotations

from app.statistics_constants import AREA_COLUMN, BUILDING_CAUSE_COLUMN, DATE_COLUMN, GENERAL_CAUSE_COLUMN, OBJECT_CATEGORY_COLUMN, OPEN_AREA_CAUSE_COLUMN

DISTRICT_COLUMN_CANDIDATES = ["ОКТМО. Текст", "Район", "Муниципальный район", "Административный район", "Район выезда подразделения", "Район пожара", "Территория", "Территориальная принадлежность"]
CAUSE_COLUMN_CANDIDATES = [GENERAL_CAUSE_COLUMN, OPEN_AREA_CAUSE_COLUMN, BUILDING_CAUSE_COLUMN, "Причина пожара", "Причина"]
TEMPERATURE_COLUMN_CANDIDATES = ["Температура", "Температура воздуха", "Температура воздуха, С", "Температура воздуха, C", "Температура воздуха, °C", "Температура окружающей среды", "Температура окружающей среды на момент возникновения пожара"]
FIRE_AREA_COLUMN_CANDIDATES = [AREA_COLUMN, "Общая площадь объекта"]
TERRITORY_LABEL_COLUMN_CANDIDATES = ["ОКТМО. Текст", "Территориальная принадлежность", "Населенный пункт", "Населённый пункт"]
SETTLEMENT_TYPE_COLUMN_CANDIDATES = ["Вид населенного пункта", "Вид населённого пункта"]
BUILDING_CATEGORY_COLUMN_CANDIDATES = ["Категория здания"]
RISK_CATEGORY_COLUMN_CANDIDATES = ["Категория риска"]
FIRE_STATION_DISTANCE_COLUMN_CANDIDATES = ["Удаленность от ближайшей ПЧ", "Удаленность до ближайшей ПЧ"]
WATER_SUPPLY_COUNT_COLUMN_CANDIDATES = ["Количество записей о водоснабжении на пожаре"]
WATER_SUPPLY_DETAILS_COLUMN_CANDIDATES = ["Сведения о водоснабжении на пожаре"]
REPORT_TIME_COLUMN_CANDIDATES = ["Время сообщения"]
ARRIVAL_TIME_COLUMN_CANDIDATES = ["Время прибытия 1-го ПП"]
DETECTION_TIME_COLUMN_CANDIDATES = ["Время обнаружения"]
HEATING_TYPE_COLUMN_CANDIDATES = ["Вид отопления"]
CONSEQUENCE_COLUMN_CANDIDATES = ["Наличие последствий пожара"]
REGISTERED_DAMAGE_COLUMN_CANDIDATES = ["Зарегистрированный ущерб от пожара"]
DESTROYED_BUILDINGS_COLUMN_CANDIDATES = ["Здания (сооружения), уничтожено"]
DESTROYED_AREA_COLUMN_CANDIDATES = ["Площадь м2, уничтожено"]
CASUALTY_FLAG_COLUMN_CANDIDATES = ["Есть травмированные или погибшие"]
INJURIES_COLUMN_CANDIDATES = ["Количество травмированных в КУП"]
DEATHS_COLUMN_CANDIDATES = ["Количество погибших в КУП"]
LONG_RESPONSE_THRESHOLD_MINUTES = 20.0
MAX_TERRITORIES = 12