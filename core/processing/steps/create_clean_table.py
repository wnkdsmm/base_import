import os

import pandas as pd
from sqlalchemy import text

from config.constants import PROFILING_CSV_SUFFIX
from config.db import engine
from core.processing.pipeline import PipelineStep


class CreateCleanTableStep(PipelineStep):
    def __init__(self):
        super().__init__("Create Clean Table")

    def run(self, settings):
        output_folder = settings.output_folder
        os.makedirs(output_folder, exist_ok=True)

        if hasattr(settings, "selected_table") and settings.selected_table:
            table_name = settings.selected_table
        else:
            table_name = settings.project_name

        source_table = table_name
        new_table = f"clean_{table_name}"
        profile_csv = os.path.join(output_folder, f"{table_name}{PROFILING_CSV_SUFFIX}")

        if not os.path.exists(profile_csv):
            raise FileNotFoundError(f"Не найден profiling report: {profile_csv}")

        print(f"Создаём clean-таблицу для: {table_name}")
        print(f"Используем отчёт: {profile_csv}")

        profile_df = pd.read_csv(profile_csv)
        keep_columns = profile_df.loc[profile_df["candidate_to_drop"] == False, "column"].dropna().tolist()
        removed_columns = profile_df.loc[profile_df["candidate_to_drop"] == True, "column"].dropna().tolist()

        if not keep_columns:
            raise ValueError("После profiling не осталось колонок для сохранения.")

        print(f"Колонок останется: {len(keep_columns)}")
        print(f"Колонок будет исключено: {len(removed_columns)}")

        columns_sql = ", ".join([f'"{col}"' for col in keep_columns])
        create_table_query = f'CREATE TABLE "{new_table}" AS SELECT {columns_sql} FROM "{source_table}"'

        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{new_table}"'))
            conn.execute(text(create_table_query))

        print(f"Таблица создана: {new_table}")

        export_file = os.path.join(output_folder, f"{new_table}.xlsx")
        df_export = pd.read_sql(f'SELECT * FROM "{new_table}"', engine)
        df_export.to_excel(export_file, index=False, engine="openpyxl")

        print(f"Excel-файл clean-таблицы: {export_file}")
        print(f"Строк в clean-таблице: {len(df_export)}")
        print(f"Колонок в clean-таблице: {len(df_export.columns)}")

        return {
            "source_table": source_table,
            "clean_table": new_table,
            "kept_columns": keep_columns,
            "removed_columns": removed_columns,
            "rows": int(len(df_export)),
            "columns": int(len(df_export.columns)),
            "export_file": export_file,
        }
