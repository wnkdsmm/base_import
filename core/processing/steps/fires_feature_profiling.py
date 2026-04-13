import logging
import os
import warnings

import numpy as np
import pandas as pd

from config.constants import (
    DOMINANT_VALUE_THRESHOLD,
    LOW_VARIANCE_THRESHOLD,
    MISSING_LIKE_VALUES,
    NULL_THRESHOLD,
    PROFILING_CSV_SUFFIX,
    PROFILING_XLSX_SUFFIX,
)
from config.db import engine
from core.processing.pipeline import PipelineStep

warnings.filterwarnings("ignore")


logger = logging.getLogger(__name__)


class FiresFeatureProfilingStep(PipelineStep):
    def __init__(self, settings):
        super().__init__("Профилирование признаков пожаров")
        self.settings = settings

    def _get_thresholds(self) -> dict[str, float]:
        return {
            "null_threshold": float(getattr(self.settings, "null_threshold", NULL_THRESHOLD)),
            "low_variance_threshold": float(getattr(self.settings, "low_variance_threshold", LOW_VARIANCE_THRESHOLD)),
            "dominant_value_threshold": float(getattr(self.settings, "dominant_value_threshold", DOMINANT_VALUE_THRESHOLD)),
        }

    def _build_reason_definitions(self, thresholds: dict[str, float]) -> list[tuple[str, str, str]]:
        return [
            (
                "drop_null",
                "Много пропусков",
                f"Доля пустых значений выше {thresholds['null_threshold'] * 100:.0f}%.",
            ),
            (
                "drop_constant",
                "Константная колонка",
                "Во всей колонке только одно значение или данных нет совсем.",
            ),
            (
                "low_variance",
                "Почти нет разброса",
                f"Числовые значения почти не меняются, дисперсия ниже {thresholds['low_variance_threshold']}.",
            ),
            (
                "almost_constant",
                "Почти всегда одно и то же значение",
                f"Одно значение занимает больше {thresholds['dominant_value_threshold'] * 100:.0f}% колонки.",
            ),
        ]

    def _resolve_source_df(self, data: pd.DataFrame | None, table_name: str) -> pd.DataFrame:
        if isinstance(data, pd.DataFrame):
            return data

        cached_df = getattr(self.settings, "_pipeline_source_df", None)
        if isinstance(cached_df, pd.DataFrame):
            return cached_df

        logger.info("Читаем таблицу из базы данных...")
        try:
            return pd.read_sql(f'SELECT * FROM "{table_name}"', engine)
        except Exception:
            logger.exception("Не удалось прочитать таблицу: %s", table_name)
            raise

    def run(self, data=None):
        if hasattr(self.settings, "selected_table") and self.settings.selected_table:
            table_name = self.settings.selected_table
            output_folder = getattr(self.settings, "output_folder", "output")
        else:
            table_name = self.settings.project_name
            output_folder = self.settings.output_folder

        thresholds = self._get_thresholds()
        reason_definitions = self._build_reason_definitions(thresholds)

        os.makedirs(output_folder, exist_ok=True)
        output_csv = os.path.join(output_folder, f"{table_name}{PROFILING_CSV_SUFFIX}")
        output_xlsx = os.path.join(output_folder, f"{table_name}{PROFILING_XLSX_SUFFIX}")

        logger.info("Таблица для профилирования: %s", table_name)
        logger.info("Папка результатов: %s", output_folder)
        logger.info(
            "Пороги: пропуски > %.0f%%, доминирующее значение > %.0f%%, дисперсия < %s",
            thresholds["null_threshold"] * 100,
            thresholds["dominant_value_threshold"] * 100,
            thresholds["low_variance_threshold"],
        )

        df = self._resolve_source_df(data if isinstance(data, pd.DataFrame) else None, table_name)
        logger.info("Загружено строк: %s, колонок: %s", df.shape[0], df.shape[1])

        n_rows = len(df)
        string_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
        df_norm = pd.DataFrame(index=df.index)
        if string_cols:
            normalized_values = np.char.lower(np.char.strip(df[string_cols].astype(str).to_numpy(dtype=str)))
            df_norm = pd.DataFrame(normalized_values, index=df.index, columns=string_cols)

        report_rows = []
        for col in df.columns:
            col_data = df[col]

            if col in string_cols:
                col_norm = df_norm[col]
                null_ratio = max(col_data.isna().mean(), col_norm.isin(MISSING_LIKE_VALUES).mean())
                dominant_ratio = col_norm.value_counts(dropna=False, normalize=True).max()
                unique_count = col_data.nunique(dropna=True)
            else:
                null_ratio = col_data.isna().mean()
                dominant_ratio = col_data.value_counts(dropna=False, normalize=True).max()
                unique_count = col_data.nunique(dropna=True)

            if pd.isna(dominant_ratio):
                dominant_ratio = 0.0

            unique_ratio = (unique_count / n_rows) if n_rows else 0.0
            report_rows.append(
                {
                    "column": col,
                    "dtype": str(col_data.dtype),
                    "null_ratio": round(float(null_ratio), 4),
                    "unique_count": int(unique_count),
                    "unique_ratio": round(float(unique_ratio), 4),
                    "dominant_ratio": round(float(dominant_ratio), 4),
                }
            )

        profile_df = pd.DataFrame(report_rows)
        numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
        if numeric_cols:
            variances = df[numeric_cols].var()
            profile_df["variance"] = profile_df["column"].map(variances).fillna(1.0)
        else:
            profile_df["variance"] = 1.0

        profile_df["drop_null"] = profile_df["null_ratio"] > thresholds["null_threshold"]
        profile_df["drop_constant"] = profile_df["unique_count"] <= 1
        profile_df["low_variance"] = profile_df["variance"] < thresholds["low_variance_threshold"]
        profile_df["almost_constant"] = profile_df["dominant_ratio"] > thresholds["dominant_value_threshold"]
        profile_df["candidate_to_drop"] = (
            profile_df["drop_null"]
            | profile_df["drop_constant"]
            | profile_df["low_variance"]
            | profile_df["almost_constant"]
        )
        reason_ids = [reason_id for reason_id, _label, _description in reason_definitions]
        reason_labels = [label for _reason_id, label, _description in reason_definitions]
        reason_matrix = profile_df.loc[:, reason_ids].to_numpy(dtype=bool, copy=False)
        profile_df["drop_reasons"] = [
            [label for label, enabled in zip(reason_labels, row_flags) if enabled]
            for row_flags in reason_matrix
        ]
        profile_df["profiling_candidate_to_drop"] = profile_df["candidate_to_drop"]
        profile_df["mandatory_feature_detected"] = False
        profile_df["protected_feature_id"] = ""
        profile_df["protected_feature_label"] = ""
        profile_df["protection_scope"] = ""
        profile_df["protection_rule"] = ""
        profile_df["protection_match"] = ""
        profile_df["protection_reason"] = ""
        profile_df["protected_from_drop"] = False

        profile_df_sorted = profile_df.sort_values(
            by=["candidate_to_drop", "null_ratio", "dominant_ratio"],
            ascending=[False, False, False],
        )
        profile_df_sorted.to_csv(output_csv, index=False, encoding="utf-8-sig")
        profile_df_sorted.to_excel(output_xlsx, index=False, engine="openpyxl")

        candidates_df = profile_df_sorted[profile_df_sorted["candidate_to_drop"]].copy()
        candidates = candidates_df["column"].tolist()
        kept_columns = profile_df_sorted.loc[~profile_df_sorted["candidate_to_drop"], "column"].tolist()

        reason_summary = [
            {
                "id": reason_id,
                "label": label,
                "description": description,
                "count": int(profile_df_sorted[reason_id].sum()),
            }
            for reason_id, label, description in reason_definitions
        ]

        candidate_export_frame = pd.DataFrame(index=candidates_df.index)
        candidate_export_frame["column"] = candidates_df["column"].astype("string").fillna("").astype(object)
        candidate_export_frame["dtype"] = candidates_df["dtype"].astype("string").fillna("").astype(object)
        candidate_export_frame["null_ratio"] = pd.to_numeric(candidates_df["null_ratio"], errors="coerce").fillna(0.0).astype(float)
        candidate_export_frame["dominant_ratio"] = pd.to_numeric(candidates_df["dominant_ratio"], errors="coerce").fillna(0.0).astype(float)
        candidate_export_frame["unique_count"] = pd.to_numeric(candidates_df["unique_count"], errors="coerce").fillna(0).astype(int)
        candidate_export_frame["variance"] = pd.to_numeric(candidates_df["variance"], errors="coerce").fillna(0.0).astype(float)
        candidate_export_frame["reasons"] = (
            candidates_df["drop_reasons"]
            if "drop_reasons" in candidates_df.columns
            else [[] for _ in range(len(candidates_df))]
        )
        candidate_details = candidate_export_frame.to_dict(orient="records")

        logger.info("Профилирование завершено.")
        logger.info("Отчёт CSV: %s", output_csv)
        logger.info("Отчёт Excel: %s", output_xlsx)
        logger.info("Всего колонок: %s", len(profile_df_sorted))
        logger.info("Помечено на исключение: %s", len(candidates))
        logger.info("Останется в очищенной таблице: %s", len(kept_columns))

        self.settings._pipeline_source_df = df
        self.settings._pipeline_profile_df = profile_df_sorted

        return {
            "source_df": df,
            "profile_df": profile_df_sorted,
            "candidates": candidates,
            "table_name": table_name,
            "total_columns": int(len(profile_df_sorted)),
            "kept_columns": kept_columns,
            "reason_summary": reason_summary,
            "candidate_details": candidate_details,
            "report_csv": output_csv,
            "report_xlsx": output_xlsx,
            "thresholds": thresholds,
        }
