from __future__ import annotations

import io
import os
import shutil
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, Callable, Dict

import pandas as pd
from fastapi import UploadFile

from app.runtime_invalidation import invalidate_runtime_caches as shared_invalidate_runtime_caches
from app.state import UPLOAD_FOLDER, job_store
from config.constants import (
    DOMINANT_VALUE_THRESHOLD,
    LOW_VARIANCE_THRESHOLD,
    NULL_THRESHOLD,
    PROFILING_CSV_SUFFIX,
    PROFILING_XLSX_SUFFIX,
)
from config.paths import get_result_folder
from config.settings import Settings
from core.processing.steps.create_clean_table import CreateCleanTableStep
from core.processing.steps.fires_feature_profiling import FiresFeatureProfilingStep
from core.processing.steps.import_data import ImportDataStep
from core.processing.steps.keep_important_columns import KeepImportantColumnsStep


def add_log(session_id: str, job_id: str, message: str) -> None:
    """Compatibility shim for tests and legacy module-level patching."""
    job_store.add_log(session_id, job_id, message)


class _LiveLogStream(io.TextIOBase):
    def __init__(self, session_id: str, job_id: str) -> None:
        self._session_id = session_id
        self._job_id = job_id
        self._buffer = ""

    def write(self, text: str) -> int:
        if not text:
            return 0
        self._buffer += text.replace("\r\n", "\n").replace("\r", "\n")
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            line = line.strip()
            if line:
                add_log(self._session_id, self._job_id, line)
        return len(text)

    def flush(self) -> None:
        line = self._buffer.strip()
        if line:
            add_log(self._session_id, self._job_id, line)
        self._buffer = ""


def _normalize_thresholds(raw_thresholds: dict[str, Any] | None) -> dict[str, float]:
    raw_thresholds = raw_thresholds or {}

    def _read_percent(key: str, default: float) -> float:
        value = raw_thresholds.get(key, default)
        try:
            number = float(value)
        except (TypeError, ValueError):
            number = default
        if number > 1:
            number = number / 100
        return min(max(number, 0.0), 1.0)

    def _read_positive(key: str, default: float) -> float:
        value = raw_thresholds.get(key, default)
        try:
            number = float(value)
        except (TypeError, ValueError):
            number = default
        return max(number, 0.0)

    return {
        "null_threshold": _read_percent("null_threshold", NULL_THRESHOLD),
        "dominant_value_threshold": _read_percent("dominant_value_threshold", DOMINANT_VALUE_THRESHOLD),
        "low_variance_threshold": _read_positive("low_variance_threshold", LOW_VARIANCE_THRESHOLD),
    }



def _coerce_profile_bool(series: pd.Series) -> pd.Series:
    normalized = series.astype(str).str.strip().str.lower()
    return normalized.isin(["true", "1", "yes"])


def _profile_report_paths(output_folder: str, table_name: str) -> tuple[str, str]:
    updated_csv = os.path.join(output_folder, f"{table_name}_updated{PROFILING_CSV_SUFFIX}")
    updated_xlsx = os.path.join(output_folder, f"{table_name}_updated{PROFILING_XLSX_SUFFIX}")
    default_csv = os.path.join(output_folder, f"{table_name}{PROFILING_CSV_SUFFIX}")
    default_xlsx = os.path.join(output_folder, f"{table_name}{PROFILING_XLSX_SUFFIX}")
    if os.path.exists(updated_csv):
        return updated_csv, updated_xlsx if os.path.exists(updated_xlsx) else default_xlsx
    return default_csv, default_xlsx


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _load_profile_summary(
    output_folder: str,
    table_name: str,
    thresholds: dict[str, float],
    base_reason_summary: list[dict[str, Any]],
    profile_df: pd.DataFrame | None = None,
    report_csv: str | None = None,
    report_xlsx: str | None = None,
) -> dict[str, Any]:
    resolved_report_csv, resolved_report_xlsx = _profile_report_paths(output_folder, table_name)
    if report_csv:
        resolved_report_csv = report_csv
    if report_xlsx:
        resolved_report_xlsx = report_xlsx

    if profile_df is None:
        profile_df = pd.read_csv(resolved_report_csv)
    else:
        profile_df = profile_df.copy()

    reason_ids = [str(item.get("id") or "") for item in base_reason_summary if item.get("id")]
    bool_columns = ["candidate_to_drop", "profiling_candidate_to_drop", "mandatory_feature_detected", "protected_from_drop", *reason_ids]
    text_columns = [
        "protected_feature_id",
        "protected_feature_label",
        "protection_scope",
        "protection_rule",
        "protection_match",
        "protection_reason",
    ]
    for column_name in bool_columns:
        if column_name in profile_df.columns:
            profile_df[column_name] = _coerce_profile_bool(profile_df[column_name])
    for column_name in text_columns:
        if column_name in profile_df.columns:
            profile_df[column_name] = profile_df[column_name].astype("string").fillna("").astype(object)

    candidate_mask = profile_df["candidate_to_drop"] if "candidate_to_drop" in profile_df.columns else pd.Series(False, index=profile_df.index)
    protected_mask = profile_df["protected_from_drop"] if "protected_from_drop" in profile_df.columns else pd.Series(False, index=profile_df.index)

    candidates_df = profile_df.loc[candidate_mask].copy()
    protected_df = profile_df.loc[protected_mask].copy()
    kept_columns = profile_df.loc[~candidate_mask, "column"].dropna().tolist()

    reason_labels = {str(item.get("id") or ""): str(item.get("label") or "") for item in base_reason_summary if item.get("id")}
    reason_summary = []
    for item in base_reason_summary:
        reason_id = str(item.get("id") or "")
        count = int(candidates_df[reason_id].sum()) if reason_id and reason_id in candidates_df.columns else 0
        updated_item = dict(item)
        updated_item["count"] = count
        reason_summary.append(updated_item)

    reason_columns = [reason_id for reason_id in reason_labels if reason_id in candidates_df.columns]
    reason_names = [reason_labels[reason_id] for reason_id in reason_columns]
    if reason_columns:
        reason_matrix = candidates_df.loc[:, reason_columns].fillna(False).astype(bool).to_numpy(dtype=bool)
        reasons_by_row = [
            [label for label, enabled in zip(reason_names, row_flags) if enabled]
            for row_flags in reason_matrix
        ]
    else:
        reasons_by_row = [[] for _ in range(len(candidates_df))]

    candidate_export_frame = pd.DataFrame(index=candidates_df.index)
    candidate_export_frame["column"] = candidates_df["column"].astype("string").fillna("").astype(object) if "column" in candidates_df.columns else ""
    candidate_export_frame["dtype"] = candidates_df["dtype"].astype("string").fillna("").astype(object) if "dtype" in candidates_df.columns else ""
    candidate_export_frame["null_ratio"] = (
        pd.to_numeric(candidates_df["null_ratio"], errors="coerce").fillna(0.0).astype(float)
        if "null_ratio" in candidates_df.columns
        else 0.0
    )
    candidate_export_frame["dominant_ratio"] = (
        pd.to_numeric(candidates_df["dominant_ratio"], errors="coerce").fillna(0.0).astype(float)
        if "dominant_ratio" in candidates_df.columns
        else 0.0
    )
    candidate_export_frame["unique_count"] = (
        pd.to_numeric(candidates_df["unique_count"], errors="coerce").fillna(0).astype(int)
        if "unique_count" in candidates_df.columns
        else 0
    )
    candidate_export_frame["variance"] = (
        pd.to_numeric(candidates_df["variance"], errors="coerce").fillna(0.0).astype(float)
        if "variance" in candidates_df.columns
        else 0.0
    )
    candidate_export_frame["reasons"] = reasons_by_row
    candidate_details = candidate_export_frame.to_dict(orient="records")

    protected_export_frame = pd.DataFrame(index=protected_df.index)
    protected_export_frame["column"] = (
        protected_df["column"].astype("string").fillna("").astype(object)
        if "column" in protected_df.columns
        else ""
    )
    protected_export_frame["feature_id"] = (
        protected_df["protected_feature_id"].astype("string").fillna("").astype(object)
        if "protected_feature_id" in protected_df.columns
        else ""
    )
    protected_export_frame["feature_label"] = (
        protected_df["protected_feature_label"].astype("string").fillna("").astype(object)
        if "protected_feature_label" in protected_df.columns
        else ""
    )
    protected_export_frame["mandatory_feature_detected"] = (
        protected_df["mandatory_feature_detected"].fillna(False).astype(bool)
        if "mandatory_feature_detected" in protected_df.columns
        else False
    )
    protected_export_frame["protection_scope"] = (
        protected_df["protection_scope"].astype("string").fillna("").astype(object)
        if "protection_scope" in protected_df.columns
        else ""
    )
    protected_export_frame["protection_rule"] = (
        protected_df["protection_rule"].astype("string").fillna("").astype(object)
        if "protection_rule" in protected_df.columns
        else ""
    )
    protected_export_frame["protection_match"] = (
        protected_df["protection_match"].astype("string").fillna("").astype(object)
        if "protection_match" in protected_df.columns
        else ""
    )
    protected_export_frame["protection_reason"] = (
        protected_df["protection_reason"].astype("string").fillna("").astype(object)
        if "protection_reason" in protected_df.columns
        else ""
    )
    protected_export_frame["drop_reasons"] = protected_df["drop_reasons"] if "drop_reasons" in protected_df.columns else ""
    protected_details = protected_export_frame.to_dict(orient="records")

    return {
        "profile_df": profile_df,
        "candidates": candidates_df["column"].dropna().tolist() if "column" in candidates_df.columns else [],
        "table_name": table_name,
        "total_columns": int(len(profile_df)),
        "kept_columns": kept_columns,
        "reason_summary": reason_summary,
        "candidate_details": candidate_details,
        "protected_details": protected_details,
        "report_csv": resolved_report_csv,
        "report_xlsx": resolved_report_xlsx,
        "thresholds": thresholds,
    }


invalidate_runtime_caches = shared_invalidate_runtime_caches


def _invalidate_runtime_caches(session_id: str, job_id: str) -> None:
    shared_invalidate_runtime_caches(on_warning=lambda message: add_log(session_id, job_id, message))

def _build_upload_file_path(session_id: str, job_id: str, original_filename: str) -> Path:
    safe_name = Path(original_filename).name or "uploaded_file.xlsx"
    job_folder = UPLOAD_FOLDER / session_id / job_id
    job_folder.mkdir(parents=True, exist_ok=True)
    return job_folder / safe_name


def save_uploaded_file(
    file: UploadFile,
    session_id: str,
    job_id: str | None = None,
) -> dict[str, Any]:
    job = job_store.create_or_reset_job(session_id=session_id, kind="import", job_id=job_id)
    original_filename = Path(file.filename or "uploaded_file.xlsx").name or "uploaded_file.xlsx"
    file_path = _build_upload_file_path(session_id=session_id, job_id=job.job_id, original_filename=original_filename)

    with file_path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    job_store.set_uploaded_file(session_id, job.job_id, file_path, original_filename)
    add_log(session_id, job.job_id, f"Файл загружен: {original_filename}")

    return {
        "status": "uploaded",
        "filename": original_filename,
        "path": str(file_path),
        "job_id": job.job_id,
    }


def run_profiling_for_table(
    session_id: str,
    table_name: str,
    thresholds: dict[str, Any] | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    job = job_store.create_or_reset_job(session_id=session_id, kind="profiling", job_id=job_id)
    resolved_job_id = job.job_id

    if not table_name:
        message = "Не выбрана таблица для очистки."
        add_log(session_id, resolved_job_id, message)
        job_store.mark_job_status(session_id, resolved_job_id, "failed")
        return {
            "status": "error",
            "message": message,
            "job_id": resolved_job_id,
        }

    normalized_thresholds = _normalize_thresholds(thresholds)
    add_log(session_id, resolved_job_id, f"Запуск очистки для таблицы: {table_name}")
    job_store.mark_job_status(session_id, resolved_job_id, "running")

    log_stream = _LiveLogStream(session_id=session_id, job_id=resolved_job_id)
    final_status = "failed"

    try:
        settings = Settings(
            input_file=None,
            selected_table=table_name,
            output_folder=str(get_result_folder(table_name)),
        )
        settings.null_threshold = normalized_thresholds["null_threshold"]
        settings.dominant_value_threshold = normalized_thresholds["dominant_value_threshold"]
        settings.low_variance_threshold = normalized_thresholds["low_variance_threshold"]

        add_log(session_id, resolved_job_id, f"Таблица: {table_name}")
        add_log(
            session_id,
            resolved_job_id,
            "Пороги пользователя: "
            f"пропуски > {normalized_thresholds['null_threshold'] * 100:.0f}%, "
            f"доминирующее значение > {normalized_thresholds['dominant_value_threshold'] * 100:.0f}%, "
            f"дисперсия < {normalized_thresholds['low_variance_threshold']}",
        )
        add_log(session_id, resolved_job_id, f"Папка результатов: {settings.output_folder}")
        add_log(
            session_id,
            resolved_job_id,
            "Шаг 1 из 2. Анализируем колонки и собираем отчет по очистке.",
        )

        with redirect_stdout(log_stream):
            profiling_result = FiresFeatureProfilingStep(settings).run()
            source_df = profiling_result.get("source_df")
            keep_result = KeepImportantColumnsStep().run(
                settings,
                profile_df=profiling_result.get("profile_df"),
            )
            profiling_result = _load_profile_summary(
                output_folder=settings.output_folder,
                table_name=table_name,
                thresholds=profiling_result["thresholds"],
                base_reason_summary=profiling_result["reason_summary"],
                profile_df=keep_result.get("profile_df"),
                report_csv=keep_result.get("updated_csv"),
                report_xlsx=keep_result.get("updated_xlsx"),
            )
            add_log(
                session_id,
                resolved_job_id,
                "Шаг 2 из 2. Создаём очищенную таблицу без колонок-кандидатов на исключение.",
            )
            clean_result = CreateCleanTableStep().run(
                settings,
                profile_df=keep_result.get("profile_df"),
                source_df=source_df,
            )

        add_log(
            session_id,
            resolved_job_id,
            "Готово: "
            f"всего колонок {profiling_result['total_columns']}, "
            f"исключено {len(profiling_result['candidates'])}, "
            f"оставлено {len(clean_result['kept_columns'])}.",
        )
        add_log(session_id, resolved_job_id, f"Создана таблица: {clean_result['clean_table']}")
        add_log(session_id, resolved_job_id, f"Excel-файл очищенной таблицы: {clean_result['export_file']}")
        _invalidate_runtime_caches(session_id, resolved_job_id)
        final_status = "completed"

        return {
            "status": "success",
            "message": f"Очистка завершена для таблицы {table_name}.",
            "job_id": resolved_job_id,
            "table_name": table_name,
            "output_folder": settings.output_folder,
            "clean_table": clean_result["clean_table"],
            "summary": {
                "total_columns": profiling_result["total_columns"],
                "candidate_count": len(profiling_result["candidates"]),
                "kept_count": len(clean_result["kept_columns"]),
                "clean_rows": clean_result["rows"],
                "clean_columns": clean_result["columns"],
                "reason_summary": profiling_result["reason_summary"],
                "candidate_details": profiling_result["candidate_details"],
                "protected_count": len(profiling_result["protected_details"]),
                "protected_details": profiling_result["protected_details"],
                "mandatory_feature_catalog": keep_result.get("mandatory_feature_catalog", []),
                "kept_columns": clean_result["kept_columns"],
                "kept_preview": clean_result["kept_columns"][:24],
                "removed_preview": clean_result["removed_columns"][:24],
                "thresholds": profiling_result["thresholds"],
            },
            "files": {
                "profile_csv": profiling_result["report_csv"],
                "profile_xlsx": profiling_result["report_xlsx"],
                "protected_report_csv": keep_result.get("protected_report_csv", ""),
                "protected_report_xlsx": keep_result.get("protected_report_xlsx", ""),
                "clean_xlsx": clean_result["export_file"],
            },
        }
    except FileNotFoundError as exc:
        message = f"Не найден файл отчета: {exc}"
    except ValueError as exc:
        message = f"Ошибка данных: {exc}"
    except Exception as exc:
        message = f"Ошибка очистки: {exc}"
    finally:
        log_stream.flush()
        job_store.mark_job_status(session_id, resolved_job_id, final_status)

    add_log(session_id, resolved_job_id, message)
    return {
        "status": "error",
        "message": message,
        "job_id": resolved_job_id,
        "table_name": table_name,
    }


def import_uploaded_data(
    session_id: str,
    output_folder: str | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    job = job_store.resolve_job(session_id=session_id, job_id=job_id, kind="import")
    if job is None or job.current_file_path is None or not job.current_file_path.exists():
        return {
            "status": "Файл не загружен",
            "rows": 0,
            "columns": 0,
            "job_id": job_id or "",
        }

    resolved_job_id = job.job_id
    uploaded_file_path = job.current_file_path
    job_store.mark_job_status(session_id, resolved_job_id, "running")
    add_log(session_id, resolved_job_id, f"Запуск шага импорта для {uploaded_file_path}")

    settings = Settings(
        input_file=str(uploaded_file_path),
        output_folder=output_folder or None,
    )

    add_log(session_id, resolved_job_id, f"Имя проекта: {settings.project_name}")
    add_log(session_id, resolved_job_id, f"Папка результатов: {settings.output_folder}")

    step = ImportDataStep()
    final_status = "failed"

    try:
        step.run(settings)
        _invalidate_runtime_caches(session_id, resolved_job_id)
        add_log(session_id, resolved_job_id, f"Импорт завершён: {uploaded_file_path}")
        final_status = "completed"

        if step.data is not None:
            return {
                "status": "Импорт выполнен успешно",
                "rows": step.data.shape[0],
                "columns": step.data.shape[1],
                "project_name": settings.project_name,
                "output_folder": settings.output_folder,
                "job_id": resolved_job_id,
            }

        return {
            "status": "Импорт завершён, но данные недоступны",
            "rows": 0,
            "columns": 0,
            "project_name": settings.project_name,
            "job_id": resolved_job_id,
        }
    except Exception as exc:
        error_msg = f"Ошибка импорта: {exc}"
        add_log(session_id, resolved_job_id, error_msg)
        return {
            "status": error_msg,
            "rows": 0,
            "columns": 0,
            "job_id": resolved_job_id,
        }
    finally:
        job_store.clear_current_file(session_id, resolved_job_id)
        job_store.mark_job_status(session_id, resolved_job_id, final_status)
