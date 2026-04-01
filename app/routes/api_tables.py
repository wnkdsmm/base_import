from __future__ import annotations

from fastapi import APIRouter, Body

from app.db_views import delete_table, delete_tables
from app.services.table_workflows import build_table_page_api_payload

from .api_common import utf8_json


router = APIRouter()


@router.get("/api/tables/{table_name}/page")
def table_page_endpoint(table_name: str, page: int = 1, page_size: int = 100):
    try:
        payload = build_table_page_api_payload(table_name=table_name, page=page, page_size=page_size)
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
    return utf8_json(payload)


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
