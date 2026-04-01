from __future__ import annotations

from typing import Any, Sequence

from app.db_views import create_modified_table, get_table_columns, get_table_page, get_table_preview
from app.services.table_summary import build_table_page_summary
from core.processing.steps.keep_important_columns import get_column_matcher


def build_table_page_bundle(table_name: str, page: int = 1, page_size: int = 100) -> dict[str, Any]:
    table_page = get_table_page(table_name, page=page, page_size=page_size)
    table_summary = build_table_page_summary(
        table_name=table_name,
        columns=table_page["columns"],
        rows=table_page["rows"],
        total_rows=table_page["total_rows"],
        page_row_start=table_page["page_row_start"],
        page_row_end=table_page["page_row_end"],
    )
    return {
        "table_page": table_page,
        "table_summary": table_summary,
    }


def build_table_page_api_payload(table_name: str, page: int = 1, page_size: int = 100) -> dict[str, Any]:
    bundle = build_table_page_bundle(table_name=table_name, page=page, page_size=page_size)
    table_page = bundle["table_page"]
    return {
        "ok": True,
        "table_name": table_name,
        "columns": table_page["columns"],
        "rows": table_page["rows"],
        "pagination": table_page,
        "table_summary": bundle["table_summary"],
        "message": "Страница таблицы загружена.",
    }


def build_column_search_payload(table_name: str = "", query: str = "") -> dict[str, Any]:
    query_text = str(query or "").strip()
    normalized_table_name = str(table_name or "").strip()
    payload = {
        "table_name": normalized_table_name,
        "query": query_text,
        "count": 0,
        "columns": [],
        "groups": [],
        "preview_columns": [],
        "preview_rows": [],
    }

    if not normalized_table_name:
        payload["message"] = "Выберите таблицу для поиска колонок."
        return payload

    try:
        columns = get_table_columns(normalized_table_name)
    except Exception as exc:
        payload["message"] = str(exc)
        return payload

    try:
        matcher = get_column_matcher()
        groups = matcher.get_group_catalog(columns)
        matches = matcher.find_columns_by_query(columns, query_text) if query_text else []
    except Exception as exc:
        payload["message"] = f"Natasha-поиск не сработал: {exc}"
        return payload

    payload["count"] = len(matches)
    payload["columns"] = matches
    payload["groups"] = groups

    if matches:
        try:
            preview_columns, preview_rows = get_table_preview(
                normalized_table_name,
                [item["name"] for item in matches],
                limit=100,
            )
        except Exception as exc:
            payload["message"] = f"Совпадения найдены, но превью таблицы не удалось загрузить: {exc}"
            return payload
        payload["preview_columns"] = preview_columns
        payload["preview_rows"] = preview_rows

    if query_text:
        payload["message"] = "Совпадения найдены." if matches else "Совпадений по этому запросу не найдено. Можно выбрать группы ниже."
    else:
        payload["message"] = "Можно выбрать тематические группы или ввести слова для поиска колонок."
    return payload


def build_column_search_preview_payload(table_name: str = "", selected_columns: Sequence[str] | None = None) -> dict[str, Any]:
    normalized_table_name = str(table_name or "").strip()
    normalized_columns = [str(item) for item in (selected_columns or []) if str(item).strip()]

    if not normalized_table_name:
        return {
            "table_name": "",
            "preview_columns": [],
            "preview_rows": [],
            "message": "Не выбрана таблица.",
        }

    if not normalized_columns:
        return {
            "table_name": normalized_table_name,
            "preview_columns": [],
            "preview_rows": [],
            "message": "Выберите колонки или тематические группы для предпросмотра.",
        }

    try:
        preview_columns, preview_rows = get_table_preview(
            normalized_table_name,
            normalized_columns,
            limit=100,
        )
    except Exception as exc:
        return {
            "table_name": normalized_table_name,
            "preview_columns": [],
            "preview_rows": [],
            "message": f"Не удалось загрузить предпросмотр: {exc}",
        }

    return {
        "table_name": normalized_table_name,
        "preview_columns": preview_columns,
        "preview_rows": preview_rows,
        "message": "Предпросмотр обновлен.",
    }


def build_create_modify_table_payload(
    table_name: str = "",
    query: str = "",
    selected_columns: Sequence[str] | None = None,
    selected_groups: Sequence[str] | None = None,
) -> dict[str, Any]:
    normalized_table_name = str(table_name or "").strip()
    query_text = str(query or "").strip()
    normalized_columns = [str(item) for item in (selected_columns or []) if str(item).strip()]
    normalized_groups = [str(item) for item in (selected_groups or []) if str(item).strip()]

    if not normalized_table_name:
        return {"status": "error", "message": "Не выбрана исходная таблица."}

    try:
        all_columns = get_table_columns(normalized_table_name)
        matcher = get_column_matcher()
        group_matches = matcher.find_columns_by_categories(all_columns, normalized_groups)
        query_matches = matcher.find_columns_by_query(all_columns, query_text) if query_text else []

        final_selected = set(normalized_columns)
        final_selected.update(item["name"] for item in group_matches)
        if not final_selected and query_matches:
            final_selected.update(item["name"] for item in query_matches)

        ordered_columns = [column for column in all_columns if column in final_selected]
        created = create_modified_table(normalized_table_name, ordered_columns)
        preview_columns, preview_rows = get_table_preview(
            created["table_name"],
            created["selected_columns"],
            limit=100,
        )
    except Exception as exc:
        return {"status": "error", "message": str(exc)}

    replace_message = "Таблица была пересоздана." if created["replaced_existing"] else "Таблица создана."
    return {
        "status": "created",
        "message": replace_message,
        "source_table": normalized_table_name,
        "table_name": created["table_name"],
        "columns_count": len(created["selected_columns"]),
        "selected_columns": created["selected_columns"],
        "selected_groups": normalized_groups,
        "preview_columns": preview_columns,
        "preview_rows": preview_rows,
    }


__all__ = [
    "build_column_search_payload",
    "build_column_search_preview_payload",
    "build_create_modify_table_payload",
    "build_table_page_api_payload",
    "build_table_page_bundle",
]
