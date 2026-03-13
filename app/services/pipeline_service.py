from __future__ import annotations

import io
import shutil
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, Dict

from fastapi import UploadFile

from app.log_manager import add_log, clear_logs
from app.state import UPLOAD_FOLDER, upload_state
from config.settings import Settings
from steps.create_clean_table import CreateCleanTableStep
from steps.fires_feature_profiling import FiresFeatureProfilingStep
from steps.import_data import ImportDataStep


def save_uploaded_file(file: UploadFile) -> Dict[str, Any]:
    original_filename = file.filename or "uploaded_file.xlsx"
    file_path = UPLOAD_FOLDER / original_filename

    with file_path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    upload_state.set_uploaded_file(file_path, original_filename)
    add_log(f"Файл загружен: {original_filename}")

    return {
        "status": "uploaded",
        "filename": original_filename,
        "path": str(file_path),
    }


def run_profiling_for_table(table_name: str) -> Dict[str, Any]:
    if not table_name:
        return {"status": "No table selected"}

    clear_logs()
    add_log(f"Запуск profiling для таблицы: {table_name}")

    buffer = io.StringIO()

    try:
        settings = Settings(
            input_file=None,
            selected_table=table_name,
            output_folder=str(Path("results") / f"folder_{table_name}"),
        )

        add_log(f"Обрабатывается таблица: {table_name}")
        add_log(f"Папка результатов: {settings.output_folder}")

        with redirect_stdout(buffer):
            add_log("Шаг 1: profiling признаков")
            FiresFeatureProfilingStep(settings).run(settings)

            add_log("Шаг 2: создание clean-таблицы")
            CreateCleanTableStep().run(settings)

        clean_table_name = f"clean_{table_name}"
        profile_csv = Path(settings.output_folder) / f"{table_name}_profile.csv"
        clean_xlsx = Path(settings.output_folder) / f"{clean_table_name}.xlsx"

        add_log(f"Profiling сохранен: {profile_csv}")
        add_log(f"Чистая таблица: {clean_table_name}")
        add_log(f"Excel файл: {clean_xlsx}")
    except FileNotFoundError as exc:
        error_msg = f"File not found: {exc}"
        add_log(error_msg)
        return {"status": error_msg}
    except ValueError as exc:
        error_msg = f"Value error: {exc}"
        add_log(error_msg)
        return {"status": error_msg}
    except Exception as exc:
        error_msg = f"Error: {exc}"
        add_log(error_msg)
        return {"status": error_msg}

    for log_line in buffer.getvalue().splitlines():
        if log_line.strip():
            add_log(log_line)

    return {
        "status": f"Profiling and cleaning done for {table_name}",
        "output_folder": settings.output_folder,
        "clean_table": f"clean_{table_name}",
    }


def import_uploaded_data(output_folder: str | None = None) -> Dict[str, Any]:
    if not upload_state.has_uploaded_file():
        return {"status": "No file uploaded", "rows": 0, "columns": 0}

    clear_logs()
    uploaded_file_path = upload_state.current_file_path
    add_log(f"Запуск ImportDataStep для {uploaded_file_path}")

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
