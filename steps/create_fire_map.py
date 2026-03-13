# create_fire_map.py
from pathlib import Path
import pandas as pd
from sqlalchemy import inspect

from config.db import engine
from config.settings import Settings

from map.fire_map_generator import MapCreator, Config


class CreateFireMapStep:
    """
    Шаг pipeline: создание интерактивной карты пожаров для одной таблицы
    """
    name = "CreateFireMapStep"

    def run(self, settings, table_name=None):
        """
        Создаёт карту для указанной таблицы.
        
        Args:
            settings: настройки проекта
            table_name: имя таблицы для визуализации (если None, используется project_name)
        
        Returns:
            Path: путь к созданному HTML файлу или None в случае ошибки
        """
        print("🗺 Создание карты пожаров")

        inspector = inspect(engine)
        
        # Определяем имя таблицы для обработки
        if table_name is None:
            # Используем project_name как имя таблицы по умолчанию
            table_name = settings.project_name
        
        # Проверяем существование таблицы
        if table_name not in inspector.get_table_names():
            print(f"⚠ Таблица '{table_name}' не найдена в базе данных")
            print(f"Доступные таблицы: {inspector.get_table_names()}")
            return None

        # Загружаем данные из таблицы
        try:
            query = f'SELECT * FROM "{table_name}" LIMIT 10000'
            df = pd.read_sql(query, engine)
            
            if df.empty:
                print(f"⚠ Таблица '{table_name}' пуста")
                return None
                
            print(f"✔ Загружена таблица '{table_name}' ({len(df)} записей)")
            
        except Exception as e:
            print(f"❌ Ошибка загрузки таблицы '{table_name}': {e}")
            return None

        # Создаём словарь с одной таблицей (для совместимости с MapCreator)
        tables_data = {table_name: df}

        # Создаём карту
        config = Config(output_dir=Path(settings.output_folder))
        creator = MapCreator(config)
        output = creator.create_map(tables_data)

        if output:
            print(f"✅ Карта создана: {output}")
            print(f"📊 Всего точек на карте: {len(df)}")
        else:
            print(f"❌ Не удалось создать карту для таблицы '{table_name}'")

        return output