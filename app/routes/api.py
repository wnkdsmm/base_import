from __future__ import annotations

import json
import logging
import os
from uuid import uuid4

from fastapi import APIRouter, Body, File, Form, Request, UploadFile
from fastapi.responses import Response

from app.db_views import create_modified_table, delete_table, delete_tables, get_table_columns, get_table_page, get_table_preview
from app.log_manager import clear_logs as clear_job_logs
from app.log_manager import get_logs
from app.services.access_points_service import get_access_points_data
from app.services.clustering_service import (
    get_clustering_data,
    get_clustering_job_status,
    start_clustering_job,
)
from app.services.forecasting_service import (
    get_forecasting_data,
    get_forecasting_decision_support_data,
    get_forecasting_decision_support_job_status,
    get_forecasting_metadata,
    start_forecasting_decision_support_job,
)
from app.services.ml_model_service import (
    get_ml_job_status,
    get_ml_model_data,
    start_ml_model_job,
)
from app.services.pipeline_service import import_uploaded_data, invalidate_runtime_caches, run_profiling_for_table, save_uploaded_file
from app.services.table_summary import build_table_page_summary
from app.state import SESSION_COOKIE_NAME, job_store
from app.statistics import get_dashboard_data
from core.processing.steps.keep_important_columns import get_column_matcher


router = APIRouter()
logger = logging.getLogger(__name__)
_LOCAL_RUNTIME_NAMES = {"local", "development", "dev", "debug", "test"}


def _ensure_session_id(request: Request) -> str:
    return job_store.ensure_session(request.cookies.get(SESSION_COOKIE_NAME))


def _coerce_string_list(value: object) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    if value is None:
        return []
    text = str(value).strip()
    return [text] if text else []


def utf8_json(payload: dict, status_code: int = 200, session_id: str | None = None) -> Response:
    response = Response(
        content=json.dumps(payload, ensure_ascii=False, default=str),
        status_code=status_code,
        media_type="application/json; charset=utf-8",
    )
    if session_id:
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=session_id,
            httponly=True,
            samesite="lax",
            path="/",
        )
    return response


def analytics_error_response(
    *,
    code: str,
    message: str,
    status_code: int,
    error_id: str | None = None,
    detail: str | None = None,
) -> Response:
    resolved_error_id = str(error_id or uuid4().hex)
    payload = {
        "ok": False,
        "error": {
            "code": code,
            "message": message,
            "status_code": status_code,
            "error_id": resolved_error_id,
        },
    }
    if detail and _should_expose_analytics_detail():
        payload["error"]["detail"] = detail
    return utf8_json(payload, status_code=status_code)


def _env_flag_enabled(*names: str) -> bool:
    for name in names:
        value = str(os.getenv(name, "")).strip().lower()
        if value in {"1", "true", "yes", "on"}:
            return True
    return False


def _runtime_name() -> str:
    for name in ("FIRE_MONITOR_ENV", "APP_ENV", "FASTAPI_ENV", "PYTHON_ENV", "ENV"):
        value = str(os.getenv(name, "")).strip().lower()
        if value:
            return value
    return "production"


def _is_local_or_debug_runtime() -> bool:
    if _env_flag_enabled("FIRE_MONITOR_DEBUG", "FASTAPI_DEBUG", "DEBUG"):
        return True
    return _runtime_name() in _LOCAL_RUNTIME_NAMES


def _should_expose_analytics_detail() -> bool:
    # Raw exception text stays server-side by default. Even in local/debug
    # runtimes it is returned only after an explicit opt-in.
    return _is_local_or_debug_runtime() and _env_flag_enabled("FIRE_MONITOR_EXPOSE_API_ERROR_DETAIL")


def _log_analytics_exception(*, code: str, status_code: int, error_id: str, exc: Exception) -> None:
    message = "Analytics API error [%s] %s (%s): %s"
    if status_code >= 500:
        logger.exception(message, error_id, code, status_code, exc)
        return
    logger.warning(message, error_id, code, status_code, exc, exc_info=True)


def analytics_exception_response(
    *,
    code: str,
    message: str,
    status_code: int,
    exc: Exception,
) -> Response:
    error_id = uuid4().hex
    _log_analytics_exception(code=code, status_code=status_code, error_id=error_id, exc=exc)
    return analytics_error_response(
        code=code,
        message=message,
        status_code=status_code,
        error_id=error_id,
        detail=str(exc),
    )


@router.get("/api/dashboard-data")
def dashboard_data_endpoint(table_name: str = "all", year: str = "all", group_column: str = ""):
    try:
        return get_dashboard_data(
            table_name=table_name,
            year=year,
            group_column=group_column,
            allow_fallback=False,
        )
    except ValueError as exc:
        return analytics_exception_response(
            code="dashboard_invalid_request",
            message=str(exc) or "Не удалось обработать параметры dashboard.",
            status_code=400,
            exc=exc,
        )
    except Exception as exc:
        return analytics_exception_response(
            code="dashboard_failed",
            message="Не удалось обновить dashboard. Попробуйте повторить запрос.",
            status_code=500,
            exc=exc,
        )


@router.get("/api/forecasting-data")
def forecasting_data_endpoint(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
    include_decision_support: bool = True,
):
    try:
        if include_decision_support:
            return get_forecasting_decision_support_data(
                table_name=table_name,
                district=district,
                cause=cause,
                object_category=object_category,
                temperature=temperature,
                forecast_days=forecast_days,
                history_window=history_window,
            )
        return get_forecasting_data(
            table_name=table_name,
            district=district,
            cause=cause,
            object_category=object_category,
            temperature=temperature,
            forecast_days=forecast_days,
            history_window=history_window,
            include_decision_support=False,
        )
    except ValueError as exc:
        return analytics_exception_response(
            code="forecasting_invalid_request",
            message=str(exc) or "Некорректные параметры прогноза.",
            status_code=400,
            exc=exc,
        )
    except Exception as exc:
        return analytics_exception_response(
            code="forecasting_failed",
            message="Не удалось собрать данные прогноза. Попробуйте повторить запрос.",
            status_code=500,
            exc=exc,
        )


@router.get("/api/forecasting-metadata")
def forecasting_metadata_endpoint(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
):
    try:
        return get_forecasting_metadata(
            table_name=table_name,
            district=district,
            cause=cause,
            object_category=object_category,
            temperature=temperature,
            forecast_days=forecast_days,
            history_window=history_window,
        )
    except ValueError as exc:
        return analytics_exception_response(
            code="forecasting_metadata_invalid_request",
            message=str(exc) or "Не удалось обработать параметры загрузки фильтров и признаков.",
            status_code=400,
            exc=exc,
        )
    except Exception as exc:
        return analytics_exception_response(
            code="forecasting_metadata_failed",
            message="Не удалось загрузить фильтры и признаки прогноза. Попробуйте повторить запрос.",
            status_code=500,
            exc=exc,
        )


@router.post("/api/forecasting-decision-support-jobs")
def start_forecasting_decision_support_job_endpoint(request: Request, payload: dict = Body(...)):
    session_id = _ensure_session_id(request)
    try:
        result = start_forecasting_decision_support_job(
            session_id=session_id,
            table_name=str(payload.get("table_name") or "all"),
            district=str(payload.get("district") or "all"),
            cause=str(payload.get("cause") or "all"),
            object_category=str(payload.get("object_category") or "all"),
            temperature=str(payload.get("temperature") or ""),
            forecast_days=str(payload.get("forecast_days") or "14"),
            history_window=str(payload.get("history_window") or "all"),
        )
        return utf8_json(result, session_id=session_id)
    except ValueError as exc:
        return analytics_exception_response(
            code="forecasting_decision_support_invalid_request",
            message=str(exc) or "Некорректные параметры для фонового блока поддержки решений.",
            status_code=400,
            exc=exc,
        )
    except Exception as exc:
        return analytics_exception_response(
            code="forecasting_decision_support_failed",
            message="Не удалось запустить фоновый расчет блока поддержки решений. Попробуйте повторить запрос.",
            status_code=500,
            exc=exc,
        )


@router.get("/api/forecasting-decision-support-jobs/{job_id}")
def forecasting_decision_support_job_status_endpoint(request: Request, job_id: str):
    session_id = _ensure_session_id(request)
    result = get_forecasting_decision_support_job_status(session_id=session_id, job_id=job_id)
    status_code = 404 if result.get("status") == "missing" else 200
    return utf8_json(result, status_code=status_code, session_id=session_id)


@router.get("/api/clustering-data")
def clustering_data_endpoint(
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: list[str] | None = None,
):
    try:
        return get_clustering_data(
            table_name=table_name,
            cluster_count=cluster_count,
            sample_limit=sample_limit,
            sampling_strategy=sampling_strategy,
            feature_columns=feature_columns or [],
        )
    except ValueError as exc:
        return analytics_exception_response(
            code="clustering_invalid_request",
            message=str(exc) or "Некорректные параметры кластеризации.",
            status_code=400,
            exc=exc,
        )
    except Exception as exc:
        return analytics_exception_response(
            code="clustering_failed",
            message="Не удалось собрать clustering-данные. Попробуйте повторить запрос.",
            status_code=500,
            exc=exc,
        )


@router.get("/api/access-points-data")
def access_points_data_endpoint(
    table_name: str = "all",
    district: str = "all",
    year: str = "all",
    limit: str = "25",
):
    try:
        return get_access_points_data(
            table_name=table_name,
            district=district,
            year=year,
            limit=limit,
        )
    except ValueError as exc:
        return analytics_exception_response(
            code="access_points_invalid_request",
            message=str(exc) or "Некорректные параметры рейтинга проблемных точек.",
            status_code=400,
            exc=exc,
        )
    except Exception as exc:
        return analytics_exception_response(
            code="access_points_failed",
            message="Не удалось построить рейтинг проблемных точек. Попробуйте повторить запрос.",
            status_code=500,
            exc=exc,
        )


@router.post("/api/clustering-jobs")
def start_clustering_job_endpoint(request: Request, payload: dict = Body(...)):
    session_id = _ensure_session_id(request)
    try:
        result = start_clustering_job(
            session_id=session_id,
            table_name=str(payload.get("table_name") or ""),
            cluster_count=str(payload.get("cluster_count") or "4"),
            sample_limit=str(payload.get("sample_limit") or "1000"),
            sampling_strategy=str(payload.get("sampling_strategy") or "stratified"),
            feature_columns=_coerce_string_list(payload.get("feature_columns")),
        )
        return utf8_json(result, session_id=session_id)
    except ValueError as exc:
        return analytics_exception_response(
            code="clustering_job_invalid_request",
            message=str(exc) or "Некорректные параметры для фоновой clustering-задачи.",
            status_code=400,
            exc=exc,
        )
    except Exception as exc:
        return analytics_exception_response(
            code="clustering_job_failed",
            message="Не удалось запустить фоновую clustering-задачу. Попробуйте повторить запрос.",
            status_code=500,
            exc=exc,
        )


@router.get("/api/clustering-jobs/{job_id}")
def clustering_job_status_endpoint(request: Request, job_id: str):
    session_id = _ensure_session_id(request)
    result = get_clustering_job_status(session_id=session_id, job_id=job_id)
    status_code = 404 if result.get("status") == "missing" else 200
    return utf8_json(result, status_code=status_code, session_id=session_id)


@router.get("/api/ml-model-data")
def ml_model_data_endpoint(
    table_name: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
):
    try:
        return get_ml_model_data(
            table_name=table_name,
            cause=cause,
            object_category=object_category,
            temperature=temperature,
            forecast_days=forecast_days,
            history_window=history_window,
        )
    except ValueError as exc:
        return analytics_exception_response(
            code="ml_model_invalid_request",
            message=str(exc) or "Не удалось обработать параметры ML-анализа.",
            status_code=400,
            exc=exc,
        )
    except Exception as exc:
        return analytics_exception_response(
            code="ml_model_failed",
            message="Не удалось рассчитать ML-анализ. Попробуйте повторить запрос.",
            status_code=500,
            exc=exc,
        )


@router.post("/api/ml-model-jobs")
def start_ml_model_job_endpoint(request: Request, payload: dict = Body(...)):
    session_id = _ensure_session_id(request)
    result = start_ml_model_job(
        session_id=session_id,
        table_name=str(payload.get("table_name") or "all"),
        cause=str(payload.get("cause") or "all"),
        object_category=str(payload.get("object_category") or "all"),
        temperature=str(payload.get("temperature") or ""),
        forecast_days=str(payload.get("forecast_days") or "14"),
        history_window=str(payload.get("history_window") or "all"),
    )
    return utf8_json(result, session_id=session_id)


@router.get("/api/ml-model-jobs/{job_id}")
def ml_model_job_status_endpoint(request: Request, job_id: str):
    session_id = _ensure_session_id(request)
    result = get_ml_job_status(session_id=session_id, job_id=job_id)
    status_code = 404 if result.get("status") == "missing" else 200
    return utf8_json(result, status_code=status_code, session_id=session_id)


@router.get("/api/tables/{table_name}/page")
def table_page_endpoint(table_name: str, page: int = 1, page_size: int = 100):
    try:
        table_page = get_table_page(table_name, page=page, page_size=page_size)
    except ValueError as exc:
        return utf8_json(
            {
                "ok": False,
                "table_name": table_name,
                "message": str(exc),
            },
            status_code=400,
        )
    except Exception as exc:
        return utf8_json(
            {
                "ok": False,
                "table_name": table_name,
                "message": f"Не удалось загрузить страницу таблицы: {exc}",
            },
            status_code=404,
        )

    table_summary = build_table_page_summary(
        table_name=table_name,
        columns=table_page["columns"],
        rows=table_page["rows"],
        total_rows=table_page["total_rows"],
        page_row_start=table_page["page_row_start"],
        page_row_end=table_page["page_row_end"],
    )
    return utf8_json(
        {
            "ok": True,
            "table_name": table_name,
            "columns": table_page["columns"],
            "rows": table_page["rows"],
            "pagination": table_page,
            "table_summary": table_summary,
            "message": "Страница таблицы загружена.",
        }
    )


@router.delete("/api/tables/{table_name}")
def delete_table_endpoint(table_name: str):
    try:
        result = delete_table(table_name)
    except ValueError as exc:
        return utf8_json(
            {
                "ok": False,
                "table_name": table_name,
                "message": str(exc),
            },
            status_code=404,
        )
    except Exception as exc:
        return utf8_json(
            {
                "ok": False,
                "table_name": table_name,
                "message": f"Не удалось удалить таблицу: {exc}",
            },
            status_code=500,
        )

    return utf8_json(
        {
            "ok": True,
            "table_name": result["table_name"],
            "remaining_tables": result["remaining_tables"],
            "remaining_count": result["remaining_count"],
            "message": f"Таблица {result['table_name']} удалена из базы данных.",
        }
    )


@router.post("/api/tables/delete")
def delete_tables_endpoint(payload: dict = Body(...)):
    table_names = [str(item).strip() for item in (payload.get("table_names") or []) if str(item).strip()]

    try:
        result = delete_tables(table_names)
    except ValueError as exc:
        return utf8_json(
            {
                "ok": False,
                "table_names": table_names,
                "message": str(exc),
            },
            status_code=400,
        )
    except Exception as exc:
        return utf8_json(
            {
                "ok": False,
                "table_names": table_names,
                "message": f"Не удалось удалить таблицы: {exc}",
            },
            status_code=500,
        )

    deleted_tables = result["deleted_tables"]
    deleted_count = len(deleted_tables)
    table_word = "таблица" if deleted_count == 1 else "таблицы" if 2 <= deleted_count <= 4 else "таблиц"
    return utf8_json(
        {
            "ok": True,
            "deleted_tables": deleted_tables,
            "remaining_tables": result["remaining_tables"],
            "remaining_count": result["remaining_count"],
            "message": f"Удалено {deleted_count} {table_word} из базы данных.",
        }
    )

@router.get("/api/column-search")
def column_search_endpoint(table_name: str = "", query: str = ""):
    query_text = query.strip()
    if not table_name:
        return utf8_json(
            {
                "table_name": "",
                "query": query_text,
                "count": 0,
                "columns": [],
                "groups": [],
                "preview_columns": [],
                "preview_rows": [],
                "message": "Выберите таблицу для поиска колонок.",
            }
        )

    try:
        columns = get_table_columns(table_name)
    except Exception as exc:
        return utf8_json(
            {
                "table_name": table_name,
                "query": query_text,
                "count": 0,
                "columns": [],
                "groups": [],
                "preview_columns": [],
                "preview_rows": [],
                "message": str(exc),
            }
        )

    try:
        matcher = get_column_matcher()
        groups = matcher.get_group_catalog(columns)
        matches = matcher.find_columns_by_query(columns, query_text) if query_text else []
    except Exception as exc:
        return utf8_json(
            {
                "table_name": table_name,
                "query": query_text,
                "count": 0,
                "columns": [],
                "groups": [],
                "preview_columns": [],
                "preview_rows": [],
                "message": f"Natasha-поиск не сработал: {exc}",
            }
        )

    preview_columns = []
    preview_rows = []
    if matches:
        try:
            preview_columns, preview_rows = get_table_preview(
                table_name,
                [item["name"] for item in matches],
                limit=100,
            )
        except Exception as exc:
            return utf8_json(
                {
                    "table_name": table_name,
                    "query": query_text,
                    "count": len(matches),
                    "columns": matches,
                    "groups": groups,
                    "message": f"Совпадения найдены, но превью таблицы не удалось загрузить: {exc}",
                    "preview_columns": [],
                    "preview_rows": [],
                }
            )

    if query_text:
        message = "Совпадения найдены." if matches else "Совпадений по этому запросу не найдено. Можно выбрать группы ниже."
    else:
        message = "Можно выбрать тематические группы или ввести слова для поиска колонок."

    return utf8_json(
        {
            "table_name": table_name,
            "query": query_text,
            "count": len(matches),
            "columns": matches,
            "groups": groups,
            "preview_columns": preview_columns,
            "preview_rows": preview_rows,
            "message": message,
        }
    )


@router.post("/api/column-search/preview")
def column_search_preview_endpoint(payload: dict = Body(...)):
    table_name = str(payload.get("table_name") or "").strip()
    selected_columns = [str(item) for item in (payload.get("selected_columns") or []) if item]

    if not table_name:
        return utf8_json(
            {
                "table_name": "",
                "preview_columns": [],
                "preview_rows": [],
                "message": "Не выбрана таблица.",
            }
        )

    if not selected_columns:
        return utf8_json(
            {
                "table_name": table_name,
                "preview_columns": [],
                "preview_rows": [],
                "message": "Выберите колонки или тематические группы для предпросмотра.",
            }
        )

    try:
        preview_columns, preview_rows = get_table_preview(
            table_name,
            selected_columns,
            limit=100,
        )
    except Exception as exc:
        return utf8_json(
            {
                "table_name": table_name,
                "preview_columns": [],
                "preview_rows": [],
                "message": f"Не удалось загрузить предпросмотр: {exc}",
            }
        )

    return utf8_json(
        {
            "table_name": table_name,
            "preview_columns": preview_columns,
            "preview_rows": preview_rows,
            "message": "Предпросмотр обновлен.",
        }
    )


@router.post("/api/column-search/create-modify-table")
def create_modify_table_endpoint(payload: dict = Body(...)):
    table_name = str(payload.get("table_name") or "").strip()
    query_text = str(payload.get("query") or "").strip()
    selected_columns = [str(item) for item in (payload.get("selected_columns") or []) if item]
    selected_groups = [str(item) for item in (payload.get("selected_groups") or []) if item]

    if not table_name:
        return utf8_json({"status": "error", "message": "Не выбрана исходная таблица."})

    try:
        all_columns = get_table_columns(table_name)
        matcher = get_column_matcher()
        group_matches = matcher.find_columns_by_categories(all_columns, selected_groups)
        query_matches = matcher.find_columns_by_query(all_columns, query_text) if query_text else []

        final_selected = set(selected_columns)
        final_selected.update(item["name"] for item in group_matches)

        if not final_selected and query_matches:
            final_selected.update(item["name"] for item in query_matches)

        ordered_columns = [column for column in all_columns if column in final_selected]
        created = create_modified_table(table_name, ordered_columns)
        preview_columns, preview_rows = get_table_preview(created["table_name"], created["selected_columns"], limit=100)
        invalidate_runtime_caches()
    except Exception as exc:
        return utf8_json({"status": "error", "message": str(exc)})

    replace_message = "Таблица была пересоздана." if created["replaced_existing"] else "Таблица создана."
    return utf8_json(
        {
            "status": "created",
            "message": replace_message,
            "source_table": table_name,
            "table_name": created["table_name"],
            "columns_count": len(created["selected_columns"]),
            "selected_columns": created["selected_columns"],
            "selected_groups": selected_groups,
            "preview_columns": preview_columns,
            "preview_rows": preview_rows,
        }
    )


@router.post("/upload")
async def upload_file(request: Request, file: UploadFile = File(...), job_id: str | None = Form(None)):
    session_id = _ensure_session_id(request)
    payload = save_uploaded_file(file=file, session_id=session_id, job_id=job_id)
    return utf8_json(payload, session_id=session_id)


@router.post("/import_data")
def import_data_endpoint(request: Request, output_folder: str | None = Form(None), job_id: str | None = Form(None)):
    session_id = _ensure_session_id(request)
    payload = import_uploaded_data(session_id=session_id, output_folder=output_folder, job_id=job_id)
    return utf8_json(payload, session_id=session_id)


@router.get("/logs")
def logs(request: Request, job_id: str | None = None):
    session_id = _ensure_session_id(request)
    resolved_job = job_store.resolve_job(session_id=session_id, job_id=job_id)
    resolved_job_id = resolved_job.job_id if resolved_job is not None else (job_id or "")
    status = resolved_job.status if resolved_job is not None else "missing"
    payload = {
        "job_id": resolved_job_id,
        "status": status,
        "logs": get_logs(session_id=session_id, job_id=resolved_job_id) if resolved_job_id else [],
    }
    return utf8_json(payload, session_id=session_id)


@router.post("/clear_logs")
def clear_logs_endpoint(request: Request, job_id: str | None = None):
    session_id = _ensure_session_id(request)
    resolved_job = job_store.resolve_job(session_id=session_id, job_id=job_id)
    if resolved_job is None:
        return utf8_json({"status": "missing", "job_id": job_id or ""}, session_id=session_id)

    clear_job_logs(session_id=session_id, job_id=resolved_job.job_id)
    pruned = job_store.prune_job_if_idle(session_id=session_id, job_id=resolved_job.job_id)
    return utf8_json({"status": "cleared", "job_id": resolved_job.job_id, "pruned": pruned}, session_id=session_id)


@router.get("/health")
def health_check(request: Request):
    session_id = _ensure_session_id(request)
    latest_import_job = job_store.resolve_job(session_id=session_id, kind="import")
    return utf8_json(
        {
            "status": "healthy",
            "uploaded_file": job_store.has_uploaded_file(session_id=session_id),
            "job_id": latest_import_job.job_id if latest_import_job is not None else "",
        },
        session_id=session_id,
    )


@router.post("/run_profiling")
def run_profiling_endpoint(request: Request, payload: dict = Body(...)):
    session_id = _ensure_session_id(request)
    raw_job_id = str(payload.get("job_id") or "").strip()
    result = run_profiling_for_table(
        session_id=session_id,
        table_name=str(payload.get("table") or ""),
        thresholds=payload.get("thresholds"),
        job_id=raw_job_id or None,
    )
    return utf8_json(result, session_id=session_id)
