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


class FiresFeatureProfilingStep(PipelineStep):
    def __init__(self, settings):
        super().__init__("Fires Feature Profiling")
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

    def _build_drop_reasons(
        self,
        row: pd.Series,
        reason_definitions: list[tuple[str, str, str]],
    ) -> list[str]:
        reasons: list[str] = []
        for reason_id, label, _description in reason_definitions:
            if bool(row.get(reason_id)):
                reasons.append(label)
        return reasons

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

        print(f"Таблица для profiling: {table_name}")
        print(f"Папка результатов: {output_folder}")
        print(
            "Пороги: "
            f"пропуски > {thresholds['null_threshold'] * 100:.0f}%, "
            f"доминирующее значение > {thresholds['dominant_value_threshold'] * 100:.0f}%, "
            f"дисперсия < {thresholds['low_variance_threshold']}"
        )
        print("Читаем таблицу из базы данных...")

        try:
            df = pd.read_sql(f'SELECT * FROM "{table_name}"', engine)
        except Exception as exc:
            print(f"Не удалось прочитать таблицу: {exc}")
            raise

        print(f"Загружено строк: {df.shape[0]}, колонок: {df.shape[1]}")

        n_rows = len(df)
        string_cols = df.select_dtypes(include="object").columns.tolist()
        df_norm = pd.DataFrame(index=df.index)
        if string_cols:
            df_norm = df[string_cols].copy()
            df_norm = df_norm.apply(lambda col: col.astype(str).str.strip().str.lower())

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
        profile_df["drop_reasons"] = profile_df.apply(
            lambda row: self._build_drop_reasons(row, reason_definitions),
            axis=1,
        )

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

        candidate_details = []
        for _, row in candidates_df.iterrows():
            candidate_details.append(
                {
                    "column": row["column"],
                    "dtype": row["dtype"],
                    "null_ratio": float(row["null_ratio"]),
                    "dominant_ratio": float(row["dominant_ratio"]),
                    "unique_count": int(row["unique_count"]),
                    "variance": float(row["variance"]),
                    "reasons": row["drop_reasons"],
                }
            )

        print("Profiling завершён.")
        print(f"Отчёт CSV: {output_csv}")
        print(f"Отчёт Excel: {output_xlsx}")
        print(f"Всего колонок: {len(profile_df_sorted)}")
        print(f"Помечено на исключение: {len(candidates)}")
        print(f"Останется в clean-таблице: {len(kept_columns)}")

        return {
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
