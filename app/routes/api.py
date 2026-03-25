from __future__ import annotations

import json

from fastapi import APIRouter, Body, File, Form, Request, UploadFile
from fastapi.responses import Response

from app.db_views import create_modified_table, delete_table, delete_tables, get_table_columns, get_table_preview
from app.log_manager import clear_logs as clear_job_logs
from app.log_manager import get_logs
from app.services.clustering_service import get_clustering_data, get_clustering_shell_context
from app.services.dashboard_service import get_dashboard_page_context
from app.services.forecasting_service import get_forecasting_data, get_forecasting_page_context
from app.services.pipeline_service import import_uploaded_data, invalidate_runtime_caches, run_profiling_for_table, save_uploaded_file
from app.state import SESSION_COOKIE_NAME, job_store
from app.statistics import get_dashboard_data
from core.processing.steps.keep_important_columns import get_column_matcher


router = APIRouter()


def _ensure_session_id(request: Request) -> str:
    return job_store.ensure_session(request.cookies.get(SESSION_COOKIE_NAME))


def utf8_json(payload: dict, status_code: int = 200, session_id: str | None = None) -> Response:
    response = Response(
        content=json.dumps(payload, ensure_ascii=False),
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


@router.get("/api/dashboard-data")
def dashboard_data_endpoint(table_name: str = "all", year: str = "all", group_column: str = ""):
    try:
        return utf8_json(get_dashboard_data(table_name=table_name, year=year, group_column=group_column))
    except Exception:
        return utf8_json(
            get_dashboard_page_context(table_name=table_name, year=year, group_column=group_column)["initial_data"]
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
):
    try:
        return get_forecasting_data(
            table_name=table_name,
            district=district,
            cause=cause,
            object_category=object_category,
            temperature=temperature,
            forecast_days=forecast_days,
            history_window=history_window,
        )
    except Exception:
        return get_forecasting_page_context(
            table_name=table_name,
            district=district,
            cause=cause,
            object_category=object_category,
            temperature=temperature,
            forecast_days=forecast_days,
            history_window=history_window,
        )["initial_data"]


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
    except Exception:
        return get_clustering_shell_context(
            table_name=table_name,
            cluster_count=cluster_count,
            sample_limit=sample_limit,
            sampling_strategy=sampling_strategy,
            feature_columns=feature_columns or [],
        )["initial_data"]


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
