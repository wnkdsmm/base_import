import logging
import os

import pandas as pd
from sqlalchemy import text

from config.constants import PROFILING_CSV_SUFFIX
from config.db import engine
from core.processing.pipeline import PipelineStep


logger = logging.getLogger(__name__)


def _coerce_bool_series(series: pd.Series) -> pd.Series:
    if str(series.dtype) == "bool":
        return series.fillna(False)
    normalized = series.astype(str).str.strip().str.lower()
    return normalized.isin(["true", "1", "yes"])


class CreateCleanTableStep(PipelineStep):
    def __init__(self):
        super().__init__("РЎРѕР·РґР°РЅРёРµ РѕС‡РёС‰РµРЅРЅРѕР№ С‚Р°Р±Р»РёС†С‹")

    def _resolve_profile_df(self, settings, profile_df: pd.DataFrame | None, profile_csv: str) -> pd.DataFrame:
        if isinstance(profile_df, pd.DataFrame):
            return profile_df.copy()

        cached_profile_df = getattr(settings, "_pipeline_profile_df", None)
        if isinstance(cached_profile_df, pd.DataFrame):
            return cached_profile_df.copy()

        if not os.path.exists(profile_csv):
            raise FileNotFoundError(f"РќРµ РЅР°Р№РґРµРЅ РѕС‚С‡С‘С‚ РїСЂРѕС„РёР»РёСЂРѕРІР°РЅРёСЏ: {profile_csv}")
        return pd.read_csv(profile_csv)

    def _resolve_source_df(self, settings, source_df: pd.DataFrame | None) -> pd.DataFrame | None:
        if isinstance(source_df, pd.DataFrame):
            return source_df

        cached_source_df = getattr(settings, "_pipeline_source_df", None)
        if isinstance(cached_source_df, pd.DataFrame):
            return cached_source_df

        return None

    def run(
        self,
        settings,
        profile_df: pd.DataFrame | None = None,
        source_df: pd.DataFrame | None = None,
    ):
        output_folder = settings.output_folder
        os.makedirs(output_folder, exist_ok=True)

        if hasattr(settings, "selected_table") and settings.selected_table:
            table_name = settings.selected_table
        else:
            table_name = settings.project_name

        source_table = table_name
        new_table = f"clean_{table_name}"
        profile_csv = os.path.join(output_folder, f"{table_name}{PROFILING_CSV_SUFFIX}")
        updated_profile_csv = os.path.join(output_folder, f"{table_name}_updated{PROFILING_CSV_SUFFIX}")

        if os.path.exists(updated_profile_csv):
            profile_csv = updated_profile_csv

        logger.info("РЎРѕР·РґР°С‘Рј РѕС‡РёС‰РµРЅРЅСѓСЋ С‚Р°Р±Р»РёС†Сѓ РґР»СЏ: %s", table_name)
        logger.info("РСЃРїРѕР»СЊР·СѓРµРј РѕС‚С‡С‘С‚: %s", profile_csv)

        resolved_profile_df = self._resolve_profile_df(settings, profile_df, profile_csv)
        if "candidate_to_drop" not in resolved_profile_df.columns:
            raise KeyError("Р’ РѕС‚С‡С‘С‚Рµ РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚ РєРѕР»РѕРЅРєР° 'candidate_to_drop'")

        candidate_mask = _coerce_bool_series(resolved_profile_df["candidate_to_drop"])
        keep_columns = resolved_profile_df.loc[~candidate_mask, "column"].dropna().tolist()
        removed_columns = resolved_profile_df.loc[candidate_mask, "column"].dropna().tolist()

        if not keep_columns:
            raise ValueError("РџРѕСЃР»Рµ РїСЂРѕС„РёР»РёСЂРѕРІР°РЅРёСЏ РЅРµ РѕСЃС‚Р°Р»РѕСЃСЊ РєРѕР»РѕРЅРѕРє РґР»СЏ СЃРѕС…СЂР°РЅРµРЅРёСЏ.")

        logger.info("РљРѕР»РѕРЅРѕРє РѕСЃС‚Р°РЅРµС‚СЃСЏ: %s", len(keep_columns))
        logger.info("РљРѕР»РѕРЅРѕРє Р±СѓРґРµС‚ РёСЃРєР»СЋС‡РµРЅРѕ: %s", len(removed_columns))

        columns_sql = ", ".join(f'"{col}"' for col in keep_columns)
        create_table_query = f'CREATE TABLE "{new_table}" AS SELECT {columns_sql} FROM "{source_table}"'

        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{new_table}"'))
            conn.execute(text(create_table_query))

        logger.info("РўР°Р±Р»РёС†Р° СЃРѕР·РґР°РЅР°: %s", new_table)

        export_file = os.path.join(output_folder, f"{new_table}.xlsx")
        resolved_source_df = self._resolve_source_df(settings, source_df)
        clean_df = None
        if resolved_source_df is not None:
            missing_columns = [column_name for column_name in keep_columns if column_name not in resolved_source_df.columns]
            if not missing_columns:
                clean_df = resolved_source_df.loc[:, keep_columns].copy()

        if clean_df is None:
            clean_df = pd.read_sql(f'SELECT {columns_sql} FROM "{new_table}"', engine)

        clean_df.to_excel(export_file, index=False, engine="openpyxl")

        logger.info("Excel-С„Р°Р№Р» РѕС‡РёС‰РµРЅРЅРѕР№ С‚Р°Р±Р»РёС†С‹: %s", export_file)
        logger.info("РЎС‚СЂРѕРє РІ РѕС‡РёС‰РµРЅРЅРѕР№ С‚Р°Р±Р»РёС†Рµ: %s", len(clean_df))
        logger.info("РљРѕР»РѕРЅРѕРє РІ РѕС‡РёС‰РµРЅРЅРѕР№ С‚Р°Р±Р»РёС†Рµ: %s", len(clean_df.columns))

        settings._pipeline_clean_df = clean_df

        return {
            "source_table": source_table,
            "clean_table": new_table,
            "kept_columns": keep_columns,
            "removed_columns": removed_columns,
            "rows": int(len(clean_df)),
            "columns": int(len(clean_df.columns)),
            "export_file": export_file,
        }
