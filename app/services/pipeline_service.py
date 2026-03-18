from __future__ import annotations

import io
import shutil
from contextlib import redirect_stdout
from typing import Any, Dict

from fastapi import UploadFile

from app.log_manager import add_log, clear_logs
from app.state import UPLOAD_FOLDER, upload_state
from config.constants import DOMINANT_VALUE_THRESHOLD, LOW_VARIANCE_THRESHOLD, NULL_THRESHOLD
from config.paths import get_result_folder
from config.settings import Settings
from core.processing.steps.create_clean_table import CreateCleanTableStep
from core.processing.steps.fires_feature_profiling import FiresFeatureProfilingStep
from core.processing.steps.import_data import ImportDataStep


class _LiveLogStream(io.TextIOBase):
    def __init__(self) -> None:
        self._buffer = ""

    def write(self, text: str) -> int:
        if not text:
            return 0
        self._buffer += text.replace("\r\n", "\n").replace("\r", "\n")
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            line = line.strip()
            if line:
                add_log(line)
        return len(text)

    def flush(self) -> None:
        line = self._buffer.strip()
        if line:
            add_log(line)
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


def save_uploaded_file(file: UploadFile) -> Dict[str, Any]:
    original_filename = file.filename or "uploaded_file.xlsx"
    file_path = UPLOAD_FOLDER / original_filename

    with file_path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    upload_state.set_uploaded_file(file_path, original_filename)
    add_log(f"Р¤Р°Р№Р» Р·Р°РіСЂСѓР¶РµРЅ: {original_filename}")

    return {
        "status": "uploaded",
        "filename": original_filename,
        "path": str(file_path),
    }


def run_profiling_for_table(
    table_name: str,
    thresholds: dict[str, Any] | None = None,
) -> Dict[str, Any]:
    if not table_name:
        return {
            "status": "error",
            "message": "Не выбрана таблица для profiling.",
        }

    clear_logs()
    normalized_thresholds = _normalize_thresholds(thresholds)
    add_log(f"Запуск profiling для таблицы: {table_name}")

    log_stream = _LiveLogStream()

    try:
        settings = Settings(
            input_file=None,
            selected_table=table_name,
            output_folder=str(get_result_folder(table_name)),
        )
        settings.null_threshold = normalized_thresholds["null_threshold"]
        settings.dominant_value_threshold = normalized_thresholds["dominant_value_threshold"]
        settings.low_variance_threshold = normalized_thresholds["low_variance_threshold"]

        add_log(f"Таблица: {table_name}")
        add_log(
            "Пороги пользователя: "
            f"пропуски > {normalized_thresholds['null_threshold'] * 100:.0f}%, "
            f"доминирующее значение > {normalized_thresholds['dominant_value_threshold'] * 100:.0f}%, "
            f"дисперсия < {normalized_thresholds['low_variance_threshold']}"
        )
        add_log(f"Папка результатов: {settings.output_folder}")
        add_log("Шаг 1 из 2. Анализируем колонки и собираем profiling-отчёт.")

        with redirect_stdout(log_stream):
            profiling_result = FiresFeatureProfilingStep(settings).run(settings)
            add_log("Шаг 2 из 2. Создаём clean-таблицу без колонок-кандидатов на исключение.")
            clean_result = CreateCleanTableStep().run(settings)

        log_stream.flush()

        add_log(
            "Готово: "
            f"всего колонок {profiling_result['total_columns']}, "
            f"исключено {len(profiling_result['candidates'])}, "
            f"оставлено {len(clean_result['kept_columns'])}."
        )
        add_log(f"Создана таблица: {clean_result['clean_table']}")
        add_log(f"Excel clean-таблицы: {clean_result['export_file']}")

        return {
            "status": "success",
            "message": f"Profiling и очистка завершены для таблицы {table_name}.",
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
                "kept_preview": clean_result["kept_columns"][:24],
                "removed_preview": clean_result["removed_columns"][:24],
                "thresholds": profiling_result["thresholds"],
            },
            "files": {
                "profile_csv": profiling_result["report_csv"],
                "profile_xlsx": profiling_result["report_xlsx"],
                "clean_xlsx": clean_result["export_file"],
            },
        }
    except FileNotFoundError as exc:
        message = f"Не найден файл отчёта: {exc}"
    except ValueError as exc:
        message = f"Ошибка данных: {exc}"
    except Exception as exc:
        message = f"Ошибка выполнения profiling: {exc}"

    log_stream.flush()
    add_log(message)
    return {
        "status": "error",
        "message": message,
        "table_name": table_name,
    }


def import_uploaded_data(output_folder: str | None = None) -> Dict[str, Any]:
    if not upload_state.has_uploaded_file():
        return {"status": "No file uploaded", "rows": 0, "columns": 0}

    clear_logs()
    uploaded_file_path = upload_state.current_file_path
    add_log(f"Р—Р°РїСѓСЃРє ImportDataStep РґР»СЏ {uploaded_file_path}")

    settings = Settings(
        input_file=str(uploaded_file_path),
        output_folder=output_folder or None,
    )

    add_log(f"Project name: {settings.project_name}")
    add_log(f"Output folder: {settings.output_folder}")

    step = ImportDataStep()

    try:
        step.run(settings)
        add_log(f"Import completed: {uploaded_file_path}")
        upload_state.clear_current_file()

        if step.data is not None:
            return {
                "status": "Import successful",
                "rows": step.data.shape[0],
                "columns": step.data.shape[1],
                "project_name": settings.project_name,
                "output_folder": settings.output_folder,
            }

        return {
            "status": "Import completed but no data available",
            "rows": 0,
            "columns": 0,
            "project_name": settings.project_name,
        }
    except Exception as exc:
        error_msg = f"Import failed: {exc}"
        add_log(error_msg)
        return {"status": error_msg, "rows": 0, "columns": 0}
