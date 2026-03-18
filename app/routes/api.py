from __future__ import annotations

import json

from fastapi import APIRouter, Body, File, UploadFile
from fastapi.responses import Response

from app.db_views import create_modified_table, get_table_columns, get_table_preview
from app.log_manager import clear_logs, get_logs
from app.services.forecasting_service import get_forecasting_data
from app.services.pipeline_service import import_uploaded_data, run_profiling_for_table, save_uploaded_file
from app.state import upload_state
from app.statistics import get_dashboard_data
from core.processing.steps.keep_important_columns import get_column_matcher


router = APIRouter()


def utf8_json(payload: dict, status_code: int = 200) -> Response:
    return Response(
        content=json.dumps(payload, ensure_ascii=True),
        status_code=status_code,
        media_type="application/json; charset=utf-8",
    )


@router.get("/api/dashboard-data")
def dashboard_data_endpoint(table_name: str = "all", year: str = "all", group_column: str = ""):
    return get_dashboard_data(table_name=table_name, year=year, group_column=group_column)


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
    return get_forecasting_data(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
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
async def upload_file(file: UploadFile = File(...)):
    return save_uploaded_file(file)


@router.get("/logs")
def logs():
    return utf8_json({"logs": get_logs()})


@router.post("/clear_logs")
def clear_logs_endpoint():
    clear_logs()
    return utf8_json({"status": "cleared"})


@router.get("/health")
def health_check():
    return {"status": "healthy", "uploaded_file": upload_state.has_uploaded_file()}


@router.post("/run_profiling")
def run_profiling_endpoint(payload: dict = Body(...)):
    return utf8_json(run_profiling_for_table(payload.get("table", ""), payload.get("thresholds")))
