# import_data.py
import logging
import os

import pandas as pd

from config.db import engine
from core.processing.pipeline import PipelineStep


logger = logging.getLogger(__name__)


class ImportDataStep(PipelineStep):
    def __init__(self):
        super().__init__("Import Data")
        self.data = None

    def run(self, settings):
        input_file = settings.input_file
        output_folder = settings.output_folder
        project_name = settings.project_name

        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Р¤Р°Р№Р» РЅРµ РЅР°Р№РґРµРЅ: {input_file}")

        ext = os.path.splitext(input_file)[1].lower()
        try:
            if ext in [".xls", ".xlsx"]:
                self.data = pd.read_excel(input_file)
            elif ext == ".csv":
                self.data = pd.read_csv(input_file, encoding="utf-8-sig")
            else:
                raise ValueError("РџРѕРґРґРµСЂР¶РёРІР°СЋС‚СЃСЏ С‚РѕР»СЊРєРѕ XLS, XLSX Рё CSV")
        except Exception:
            logger.exception("РћС€РёР±РєР° С‡С‚РµРЅРёСЏ С„Р°Р№Р»Р°: %s", input_file)
            raise

        os.makedirs(output_folder, exist_ok=True)

        csv_path = os.path.join(output_folder, f"{project_name}.csv")
        self.data.to_csv(csv_path, index=False, encoding="utf-8-sig")

        try:
            with engine.begin() as conn:
                self.data.to_sql(project_name, conn, if_exists="replace", index=False)
            settings._pipeline_source_df = self.data
            settings._pipeline_import_csv = csv_path
            logger.info("Р”Р°РЅРЅС‹Рµ Р·Р°РіСЂСѓР¶РµРЅС‹ РІ PostgreSQL: %s", project_name)
        except Exception:
            logger.exception("РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РїРёСЃР°С‚СЊ С‚Р°Р±Р»РёС†Сѓ РІ PostgreSQL: %s", project_name)
            raise
