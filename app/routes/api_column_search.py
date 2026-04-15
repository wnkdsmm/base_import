from __future__ import annotations

from fastapi import APIRouter, Body

from app.services.table_workflows import (
    build_column_search_payload,
    build_column_search_preview_payload,
    build_create_modify_table_payload,
)

from .api_common import utf8_json


router = APIRouter()


@router.get("/api/column-search")
def column_search_endpoint(table_name: str = "", query: str = ""):
    return utf8_json(build_column_search_payload(table_name=table_name, query=query))


@router.post("/api/column-search/preview")
def column_search_preview_endpoint(payload: dict = Body(...)):
    return utf8_json(
        build_column_search_preview_payload(
            table_name=str(payload.get("table_name") or "").strip(),
            selected_columns=payload.get("selected_columns") or [],
        )
    )


@router.post("/api/column-search/create-modify-table")
def create_modify_table_endpoint(payload: dict = Body(...)):
    return utf8_json(
        build_create_modify_table_payload(
            table_name=str(payload.get("table_name") or "").strip(),
            query=str(payload.get("query") or "").strip(),
            selected_columns=payload.get("selected_columns") or [],
            selected_groups=payload.get("selected_groups") or [],
        )
    )
