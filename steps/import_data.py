# import_data.py
from pipeline import PipelineStep
import pandas as pd
import os
from config.db import engine
from sqlalchemy import text

class ImportDataStep(PipelineStep):
    def __init__(self):
        super().__init__("Import Data")
        self.data = None

    def drop_table_if_exists(self, table_name):
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))

    def run(self, settings):
        input_file = settings.input_file
        output_folder = settings.output_folder
        project_name = settings.project_name

        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Файл не найден: {input_file}")

        ext = os.path.splitext(input_file)[1].lower()
        try:
            if ext in [".xls", ".xlsx"]:
                with open(input_file, "rb") as f:
                    self.data = pd.read_excel(f)
            elif ext == ".csv":
                self.data = pd.read_csv(input_file, encoding="utf-8-sig")
            else:
                raise ValueError("Поддерживаются только XLS, XLSX и CSV")
        except Exception as e:
            print(f"Ошибка чтения файла: {e}")
            raise

        # создаем папку если не существует
        os.makedirs(output_folder, exist_ok=True)

        # Сохраняем Excel в CSV
        csv_path = os.path.join(output_folder, f"{project_name}.csv")
        self.data.to_csv(csv_path, index=False, encoding="utf-8-sig")

        # Удаляем таблицу перед импортом
        self.drop_table_if_exists(project_name)

        # Загружаем данные в PostgreSQL
        try:
            self.data.to_sql(project_name, engine, if_exists="replace", index=False)
            print(f"Данные загружены в PostgreSQL: {project_name}")
        finally:
            # Закрываем соединения SQLAlchemy, чтобы пул обновился
            engine.dispose()