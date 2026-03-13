import os
import pandas as pd
from sqlalchemy import text
from pipeline import PipelineStep
from config.db import engine
from config.constants import PROFILING_CSV_SUFFIX


class CreateCleanTableStep(PipelineStep):
    def __init__(self):
        super().__init__("Create Clean Table")

    def run(self, settings):
        output_folder = settings.output_folder
        os.makedirs(output_folder, exist_ok=True)

        # Определяем имя таблицы (приоритет: selected_table > project_name)
        if hasattr(settings, 'selected_table') and settings.selected_table:
            table_name = settings.selected_table
        else:
            table_name = settings.project_name

        source_table = table_name
        new_table = f"clean_{table_name}"
        profile_csv = os.path.join(output_folder, f"{table_name}{PROFILING_CSV_SUFFIX}")

        if not os.path.exists(profile_csv):
            raise FileNotFoundError(f"❌ Не найден profiling report: {profile_csv}")

        print(f"📦 Таблица: {table_name}")
        print(f"📂 Папка: {output_folder}")

        # Загрузка profiling report
        profile_df = pd.read_csv(profile_csv)
        keep_columns = profile_df[profile_df["candidate_to_drop"] == False]["column"].dropna().tolist()

        if not keep_columns:
            raise ValueError("❌ Нет колонок для сохранения!")

        print("✅ Колонки которые остаются:")
        print(keep_columns)

        # Создание SQL запроса
        columns_sql = ", ".join([f'"{col}"' for col in keep_columns])
        create_query = f"""
        DROP TABLE IF EXISTS "{new_table}";
        CREATE TABLE "{new_table}" AS
        SELECT {columns_sql}
        FROM "{source_table}";
        """

        print("🚀 Создаём очищенную таблицу...")
        with engine.begin() as conn:
            conn.execute(text(create_query))
        print(f"🔥 Таблица создана: {new_table}")

        # Экспорт в Excel
        export_file = os.path.join(output_folder, f"{new_table}.xlsx")
        df_export = pd.read_sql(f'SELECT * FROM "{new_table}"', engine)
        df_export.to_excel(export_file, index=False, engine="openpyxl")

        print(f"✅ Экспорт завершён: {export_file}")
        print(f"📊 Строк: {len(df_export)}")
        print(f"📊 Колонок: {len(df_export.columns)}")