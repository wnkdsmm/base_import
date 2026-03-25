# feature_selection.py
import os

import numpy as np
import pandas as pd
from statsmodels.stats.outliers_influence import variance_inflation_factor

from config.constants import CORR_THRESHOLD, VIF_THRESHOLD
from config.db import engine
from core.processing.pipeline import PipelineStep


class FeatureSelectionStep(PipelineStep):
    def __init__(self):
        super().__init__("Отбор признаков")

    def run(self, settings):
        output_folder = settings.output_folder
        os.makedirs(output_folder, exist_ok=True)

        table_name = settings.project_name
        source_table = f"clean_{table_name}"
        final_table = f"final_{table_name}"

        print(f"Проект: {table_name}")
        print(f"Таблица: {source_table}")

        try:
            df = pd.read_sql(f'SELECT * FROM "{source_table}"', engine)
        except Exception as e:
            raise RuntimeError(f"Ошибка загрузки таблицы {source_table}: {e}")

        if df.empty:
            raise ValueError(f"Таблица {source_table} пуста")

        print("Размер:", df.shape)

        numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
        corr_drop = []

        if len(numeric_cols) > 1:
            corr_matrix = df[numeric_cols].corr().abs()
            upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
            corr_drop = [col for col in upper.columns if any(upper[col] > CORR_THRESHOLD)]

        df.drop(columns=corr_drop, inplace=True, errors="ignore")
        print("Удалены коррелирующие колонки:", corr_drop)

        numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
        vif_drop = []

        if len(numeric_cols) > 1:
            X = df[numeric_cols].fillna(0)
            while True:
                vif = pd.Series(
                    [variance_inflation_factor(X.values, i) for i in range(X.shape[1])],
                    index=X.columns
                )
                max_vif = vif.max()
                if max_vif < VIF_THRESHOLD:
                    break
                drop_feature = vif.idxmax()
                vif_drop.append(drop_feature)
                X = X.drop(columns=[drop_feature])

        df.drop(columns=vif_drop, inplace=True, errors="ignore")
        print("Удалены колонки по VIF:", vif_drop)

        print("Итоговых колонок:", len(df.columns))
        df.to_sql(final_table, engine, if_exists="replace", index=False)
        print("Таблица создана:", final_table)

        excel_path = os.path.join(output_folder, f"{final_table}.xlsx")
        df.to_excel(excel_path, index=False, engine="openpyxl")
        print("Excel сохранён:", excel_path)
        print("Строк:", len(df))
        print("Колонок:", len(df.columns))
