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
        super().__init__("Feature Selection")

    def run(self, settings):
        output_folder = settings.output_folder
        os.makedirs(output_folder, exist_ok=True)

        table_name = settings.project_name
        source_table = f"clean_{table_name}"
        final_table = f"final_{table_name}"

        print(f"рџ“¦ РџСЂРѕРµРєС‚: {table_name}")
        print(f"рџ“Ґ РўР°Р±Р»РёС†Р°: {source_table}")

        try:
            df = pd.read_sql(f'SELECT * FROM "{source_table}"', engine)
        except Exception as e:
            raise RuntimeError(f"вќЊ РћС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё С‚Р°Р±Р»РёС†С‹ {source_table}: {e}")

        if df.empty:
            raise ValueError(f"вќЊ РўР°Р±Р»РёС†Р° {source_table} РїСѓСЃС‚Р°")

        print("рџ“Љ Р Р°Р·РјРµСЂ:", df.shape)

        numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
        corr_drop = []

        if len(numeric_cols) > 1:
            corr_matrix = df[numeric_cols].corr().abs()
            upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
            corr_drop = [col for col in upper.columns if any(upper[col] > CORR_THRESHOLD)]

        df.drop(columns=corr_drop, inplace=True, errors="ignore")
        print("рџ”Ґ Correlated dropped:", corr_drop)

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
        print("рџљЂ VIF dropped:", vif_drop)

        print("вњ… Р¤РёРЅР°Р»СЊРЅС‹С… РєРѕР»РѕРЅРѕРє:", len(df.columns))
        df.to_sql(final_table, engine, if_exists="replace", index=False)
        print("рџ”Ґ РўР°Р±Р»РёС†Р° СЃРѕР·РґР°РЅР°:", final_table)

        excel_path = os.path.join(output_folder, f"{final_table}.xlsx")
        df.to_excel(excel_path, index=False, engine="openpyxl")
        print("рџ“Љ Excel СЃРѕС…СЂР°РЅС‘РЅ:", excel_path)
        print("рџ“Љ РЎС‚СЂРѕРє:", len(df))
        print("рџ“Љ РљРѕР»РѕРЅРѕРє:", len(df.columns))
