from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from config.constants import PROFILING_CSV_SUFFIX, PROFILING_XLSX_SUFFIX
from core.processing.pipeline import PipelineStep
from app.domain.column_matching import (
    COLUMN_CATEGORY_RULES,
    FALLBACK_IMPORTANT_PATTERNS,
    KEYWORD_IMPORTANCE_RULES,
    LEGACY_EXPLICIT_IMPORTANT_COLUMNS,
    MANDATORY_FEATURE_REGISTRY,
    get_mandatory_feature_catalog,
)

from .column_definitions import (
    PROTECTED_REPORT_COLUMNS,
    PROTECTION_REPORT_DEFAULTS,
    PROTECTION_TEXT_COLUMNS,
)
from .column_filters import (
    NatashaColumnMatcher,
    get_column_matcher,
)
from .column_transforms import (
    apply_match_results,
    build_protected_report,
    coerce_report_bool_columns,
    ensure_report_columns,
)
from ...types import ColumnMatchMetadata, KeepImportantColumnsResult


logger = logging.getLogger(__name__)


class KeepImportantColumnsStep(PipelineStep):
    """
    Шаг интеллектуальной фильтрации колонок.
    Сохраняет обязательные доменные признаки и legacy-важные колонки,
    даже если profiling пометил их как candidate_to_drop.
    """

    def __init__(self):
        super().__init__("Keep Important Columns Report")
        self.matcher = get_column_matcher()

    def run(self, settings, profile_df: Optional[pd.DataFrame] = None) -> KeepImportantColumnsResult:
        output_folder = settings.output_folder
        os.makedirs(output_folder, exist_ok=True)

        if hasattr(settings, "selected_table") and settings.selected_table:
            table_name = settings.selected_table
        else:
            table_name = settings.project_name

        profile_csv = os.path.join(output_folder, f"{table_name}{PROFILING_CSV_SUFFIX}")
        updated_csv = os.path.join(output_folder, f"{table_name}_updated{PROFILING_CSV_SUFFIX}")
        updated_xlsx = os.path.join(output_folder, f"{table_name}_updated{PROFILING_XLSX_SUFFIX}")
        protected_csv = os.path.join(output_folder, f"{table_name}_protected_columns_report.csv")
        protected_xlsx = os.path.join(output_folder, f"{table_name}_protected_columns_report.xlsx")

        if not os.path.exists(profile_csv):
            raise FileNotFoundError(f"Не найден отчёт профилирования: {profile_csv}")

        resolved_profile_df = profile_df
        if resolved_profile_df is None:
            cached_profile_df = getattr(settings, "_pipeline_profile_df", None)
            if isinstance(cached_profile_df, pd.DataFrame):
                resolved_profile_df = cached_profile_df
        if resolved_profile_df is None:
            resolved_profile_df = pd.read_csv(profile_csv)

        profile_df = ensure_report_columns(resolved_profile_df.copy())

        if "candidate_to_drop" not in profile_df.columns:
            raise KeyError("В отчете отсутствует колонка 'candidate_to_drop'")

        profile_df = coerce_report_bool_columns(profile_df)

        column_names = profile_df["column"].astype("string").fillna("").str.strip()
        matches: List[Optional[ColumnMatchMetadata]] = [
            self.matcher.match_column_metadata(column_name) if column_name else None
            for column_name in column_names.tolist()
        ]
        protected_columns = apply_match_results(profile_df, column_names, matches)

        profile_df_sorted = profile_df.sort_values(
            by=["protected_from_drop", "candidate_to_drop", "null_ratio", "dominant_ratio"],
            ascending=[False, False, False, False],
        )
        protected_df = build_protected_report(profile_df_sorted)

        profile_df_sorted.to_csv(updated_csv, index=False, encoding="utf-8-sig")
        profile_df_sorted.to_excel(updated_xlsx, index=False, engine="openpyxl")
        protected_df.to_csv(protected_csv, index=False, encoding="utf-8-sig")
        protected_df.to_excel(protected_xlsx, index=False, engine="openpyxl")

        if protected_columns:
            logger.info("Защищенные признаки от удаления:")
            for item in protected_columns:
                logger.info(
                    "  - '%s' -> '%s' [%s; match=%s]",
                    item["column"],
                    item["protected_feature_label"],
                    item["protection_rule"],
                    item["protection_match"],
                )
        else:
            logger.info("Защищенных признаков не найдено.")

        logger.info("Обновленный CSV: %s", updated_csv)
        logger.info("Обновленный XLSX: %s", updated_xlsx)
        logger.info("Отчет по защищенным признакам CSV: %s", protected_csv)
        logger.info("Отчет по защищенным признакам XLSX: %s", protected_xlsx)

        settings._pipeline_profile_df = profile_df_sorted
        settings._pipeline_protected_df = protected_df

        return {
            "updated_csv": updated_csv,
            "updated_xlsx": updated_xlsx,
            "protected_report_csv": protected_csv,
            "protected_report_xlsx": protected_xlsx,
            "profile_df": profile_df_sorted,
            "protected_df": protected_df,
            "protected_columns": protected_columns,
            "protected_count": len(protected_columns),
            "mandatory_feature_catalog": self.matcher.get_mandatory_feature_catalog(),
        }

