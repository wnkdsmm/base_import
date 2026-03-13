import os
import pandas as pd
import numpy as np
from sqlalchemy import text
import warnings

from pipeline import PipelineStep
from config.db import engine
from config.constants import (
    NULL_THRESHOLD,
    LOW_VARIANCE_THRESHOLD,
    DOMINANT_VALUE_THRESHOLD,
    MISSING_LIKE_VALUES,
    PROFILING_CSV_SUFFIX,
    PROFILING_XLSX_SUFFIX
)

warnings.filterwarnings("ignore")


class FiresFeatureProfilingStep(PipelineStep):
    def __init__(self, settings):
        super().__init__("Fires Feature Profiling")
        self.settings = settings

    def run(self, data=None):
        # Определяем имя таблицы (приоритет: selected_table > project_name)
        if hasattr(self.settings, 'selected_table') and self.settings.selected_table:
            table_name = self.settings.selected_table
            # Если указан project_name, используем его для пути, но таблицу берем из selected_table
            output_folder = getattr(self.settings, 'output_folder', 'output')
        else:
            table_name = self.settings.project_name
            output_folder = self.settings.output_folder

        os.makedirs(output_folder, exist_ok=True)

        output_csv = os.path.join(output_folder, f"{table_name}{PROFILING_CSV_SUFFIX}")
        output_xlsx = os.path.join(output_folder, f"{table_name}{PROFILING_XLSX_SUFFIX}")

        print(f"📦 Таблица: {table_name}")
        print(f"📂 Output folder: {output_folder}")

        # Загрузка таблицы
        print(f"📥 Загружаем таблицу {table_name} ...")
        try:
            df = pd.read_sql(f'SELECT * FROM "{table_name}"', engine)
        except Exception as e:
            print("❌ Ошибка чтения таблицы:", e)
            raise

        print("📊 Размер таблицы:", df.shape)

        n_rows = len(df)

        # --- Подготовка временной нормализованной копии для строковых колонок ---
        string_cols = df.select_dtypes(include="object").columns.tolist()
        if string_cols:
            # Сохраняем оригинальные данные, нормализуем копию
            df_norm = df[string_cols].copy()
            df_norm = df_norm.apply(lambda x: x.astype(str).str.strip().str.lower())

        # Создание отчёта
        report = []
        for col in df.columns:
            col_data = df[col]

            # --- Расчёт метрик для каждой колонки ---
            if col in string_cols:
                col_norm = df_norm[col]
                # Расчёт null_ratio с учётом «пустых» значений из MISSING_LIKE_VALUES
                null_ratio = max(col_data.isna().mean(), col_norm.isin(MISSING_LIKE_VALUES).mean())
                # Доля самого частого значения
                dominant_ratio = col_norm.value_counts(dropna=False, normalize=True).max()
                # Количество уникальных значений
                unique_count = col_data.nunique(dropna=True)
            else:
                null_ratio = col_data.isna().mean()
                dominant_ratio = col_data.value_counts(dropna=False, normalize=True).max()
                unique_count = col_data.nunique(dropna=True)

            unique_ratio = unique_count / n_rows
            dtype = str(col_data.dtype)

            report.append({
                "column": col,
                # Тип данных колонки, как его определяет pandas
                "dtype": dtype,
                # Отношение «пустых» значений к общему числу строк
                "null_ratio": round(null_ratio, 4),
                # Количество уникальных значений в колонке, игнорируя NaN
                "unique_count": unique_count,
                # Отношение уникальных значений к общему числу строк
                "unique_ratio": round(unique_ratio, 4),
                # Доля самого частого значения в колонке
                "dominant_ratio": round(dominant_ratio, 4)
            })

        profile_df = pd.DataFrame(report)

        # --- Расчёт дисперсии для числовых колонок ---
        numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
        if numeric_cols:
            # Прямое присвоение, без merge
            profile_df["variance"] = profile_df["column"].map(df[numeric_cols].var())
        profile_df["variance"].fillna(1, inplace=True)

        # --- Флаги для удаления колонок ---
        profile_df["drop_null"] = profile_df["null_ratio"] > NULL_THRESHOLD
        profile_df["drop_constant"] = profile_df["unique_count"] <= 1
        profile_df["low_variance"] = profile_df["variance"] < LOW_VARIANCE_THRESHOLD
        profile_df["almost_constant"] = profile_df["dominant_ratio"] > DOMINANT_VALUE_THRESHOLD

        profile_df["candidate_to_drop"] = (
            profile_df["drop_null"] |
            profile_df["drop_constant"] |
            profile_df["low_variance"] |
            profile_df["almost_constant"]
        )

        # --- Сортировка и сохранение отчёта ---
        profile_df_sorted = profile_df.sort_values("candidate_to_drop", ascending=False)
        profile_df_sorted.to_csv(output_csv, index=False, encoding="utf-8-sig")
        profile_df_sorted.to_excel(output_xlsx, index=False, engine="openpyxl")

        print("✅ Отчёты сохранены")
        print("🔥 Колонки-кандидаты на удаление:")
        candidates = profile_df_sorted[profile_df_sorted["candidate_to_drop"]]["column"].tolist()
        print(candidates)
        
        # Сохраняем результат для следующего шага
        return {
            "profile_df": profile_df_sorted,
            "candidates": candidates,
            "table_name": table_name
        }