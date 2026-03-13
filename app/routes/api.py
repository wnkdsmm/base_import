from __future__ import annotations

from fastapi import APIRouter, Body, File, Form, UploadFile

from app.db_views import create_modified_table, get_table_columns, get_table_preview
from app.log_manager import clear_logs, get_logs
from app.services.pipeline_service import import_uploaded_data, run_profiling_for_table, save_uploaded_file
from app.state import upload_state
from app.statistics import get_dashboard_data
from steps.keep_important_columns import get_column_matcher


router = APIRouter()


@router.get("/api/dashboard-data")
def dashboard_data_endpoint(table_name: str = "all", year: str = "all", group_column: str = ""):
    return get_dashboard_data(table_name=table_name, year=year, group_column=group_column)


@router.get("/api/column-search")
def column_search_endpoint(table_name: str = "", query: str = ""):
    query_text = query.strip()
    if not table_name:
        return {
            "table_name": "",
            "query": query_text,
            "count": 0,
            "columns": [],
            "groups": [],
            "preview_columns": [],
            "preview_rows": [],
            "message": "Выберите таблицу для поиска колонок.",
        }

    try:
        columns = get_table_columns(table_name)
    except Exception as exc:
        return {
            "table_name": table_name,
            "query": query_text,
            "count": 0,
            "columns": [],
            "groups": [],
            "preview_columns": [],
            "preview_rows": [],
            "message": str(exc),
        }

    try:
        matcher = get_column_matcher()
        groups = matcher.get_group_catalog(columns)
        matches = matcher.find_columns_by_query(columns, query_text) if query_text else []
    except Exception as exc:
        return {
            "table_name": table_name,
            "query": query_text,
            "count": 0,
            "columns": [],
            "groups": [],
            "preview_columns": [],
            "preview_rows": [],
            "message": f"Natasha-поиск не сработал: {exc}",
        }

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
            return {
                "table_name": table_name,
                "query": query_text,
                "count": len(matches),
                "columns": matches,
                "groups": groups,
                "message": f"Совпадения найдены, но превью таблицы не удалось загрузить: {exc}",
                "preview_columns": [],
                "preview_rows": [],
            }

    if query_text:
        message = "Совпадения найдены." if matches else "Совпадений по этому запросу не найдено. Можно выбрать группы ниже."
    else:
        message = "Можно выбрать тематические группы или ввести слова для поиска колонок."

    return {
        "table_name": table_name,
        "query": query_text,
        "count": len(matches),
        "columns": matches,
        "groups": groups,
        "preview_columns": preview_columns,
        "preview_rows": preview_rows,
        "message": message,
    }


@router.post("/api/column-search/create-modify-table")
def create_modify_table_endpoint(payload: dict = Body(...)):
    table_name = str(payload.get("table_name") or "").strip()
    query_text = str(payload.get("query") or "").strip()
    selected_columns = [str(item) for item in (payload.get("selected_columns") or []) if item]
    selected_groups = [str(item) for item in (payload.get("selected_groups") or []) if item]

    if not table_name:
        return {"status": "error", "message": "Не выбрана исходная таблица."}

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
        return {"status": "error", "message": str(exc)}

    replace_message = "Таблица была пересоздана." if created["replaced_existing"] else "Таблица создана." 
    return {
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


@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    return save_uploaded_file(file)


@router.get("/logs")
def logs():
    return {"logs": get_logs()}


@router.post("/clear_logs")
def clear_logs_endpoint():
    clear_logs()
    return {"status": "cleared"}


@router.get("/health")
def health_check():
    return {"status": "healthy", "uploaded_file": upload_state.has_uploaded_file()}


@router.post("/run_profiling")
def run_profiling_endpoint(payload: dict = Body(...)):
    return run_profiling_for_table(payload.get("table", ""))


@router.post("/import_data")
def import_data_endpoint(output_folder: str = Form(None)):
    return import_uploaded_data(output_folder=output_folder)
