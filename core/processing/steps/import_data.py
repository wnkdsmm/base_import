# import_data.py
import os

import pandas as pd
from sqlalchemy import text

from config.db import engine
from core.processing.pipeline import PipelineStep


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
            raise FileNotFoundError(f"Р¤Р°Р№Р» РЅРµ РЅР°Р№РґРµРЅ: {input_file}")

        ext = os.path.splitext(input_file)[1].lower()
        try:
            if ext in [".xls", ".xlsx"]:
                with open(input_file, "rb") as f:
                    self.data = pd.read_excel(f)
            elif ext == ".csv":
                self.data = pd.read_csv(input_file, encoding="utf-8-sig")
            else:
                raise ValueError("РџРѕРґРґРµСЂР¶РёРІР°СЋС‚СЃСЏ С‚РѕР»СЊРєРѕ XLS, XLSX Рё CSV")
        except Exception as e:
            print(f"РћС€РёР±РєР° С‡С‚РµРЅРёСЏ С„Р°Р№Р»Р°: {e}")
            raise

        os.makedirs(output_folder, exist_ok=True)

        csv_path = os.path.join(output_folder, f"{project_name}.csv")
        self.data.to_csv(csv_path, index=False, encoding="utf-8-sig")

        self.drop_table_if_exists(project_name)

        try:
            self.data.to_sql(project_name, engine, if_exists="replace", index=False)
            print(f"Р”Р°РЅРЅС‹Рµ Р·Р°РіСЂСѓР¶РµРЅС‹ РІ PostgreSQL: {project_name}")
        finally:
            engine.dispose()
