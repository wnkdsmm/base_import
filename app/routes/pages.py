from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.db_views import get_all_tables, get_table_data
from app.services.clustering_service import get_clustering_page_context
from app.services.dashboard_service import get_dashboard_page_context
from app.services.fire_map_service import build_fire_map_html
from app.services.forecasting_service import get_forecasting_page_context
from app.services.ml_model_service import get_ml_model_page_context
from app.services.table_options import (
    get_column_search_table_options,
    get_fire_map_table_options,
    resolve_selected_table,
)
from config.paths import STATIC_DIR, TEMPLATES_DIR
from core.processing.steps.keep_important_columns import get_mandatory_feature_catalog


router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _static_version(filename: str) -> int:
    try:
        return int((STATIC_DIR / filename).stat().st_mtime_ns)
    except OSError:
        return 0


@router.get("/", response_class=HTMLResponse)
def home(request: Request, table_name: str = "all", year: str = "all", group_column: str = ""):
    dashboard = get_dashboard_page_context(table_name=table_name, year=year, group_column=group_column)
    return templates.TemplateResponse("index.html", {"request": request, "dashboard": dashboard})


@router.get("/forecasting", response_class=HTMLResponse)
def forecasting_page(
    request: Request,
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
):
    forecast = get_forecasting_page_context(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
    )
    return templates.TemplateResponse("forecasting.html", {"request": request, "forecast": forecast, "forecasting_css_version": _static_version("forecasting.css"), "forecasting_js_version": _static_version("js/forecasting.js")})


@router.get("/backtesting", response_class=HTMLResponse)
@router.get("/ml-model", response_class=HTMLResponse)
def ml_model_page(
    request: Request,
    table_name: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
):
    ml_model = get_ml_model_page_context(
        table_name=table_name,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
    )
    return templates.TemplateResponse("ml_model.html", {"request": request, "ml_model": ml_model})


@router.get("/clustering", response_class=HTMLResponse)
def clustering_page(
    request: Request,
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: list[str] | None = Query(None),
):
    clustering = get_clustering_page_context(
        table_name=table_name,
        cluster_count=cluster_count,
        sample_limit=sample_limit,
        sampling_strategy=sampling_strategy,
        feature_columns=feature_columns or [],
    )
    return templates.TemplateResponse(
        "clustering.html",
        {
            "request": request,
            "clustering": clustering,
            "clustering_css_version": _static_version("clustering.css"),
            "clustering_js_version": _static_version("js/clustering.js"),
        },
    )


@router.get("/column-search", response_class=HTMLResponse)
def column_search_page(request: Request, table_name: str = "", query: str = ""):
    table_options = get_column_search_table_options()
    selected_table = resolve_selected_table(table_options, table_name)

    return templates.TemplateResponse(
        "column_search.html",
        {
            "request": request,
            "table_options": table_options,
            "selected_table": selected_table,
            "initial_query": query,
        },
    )


@router.get("/fire-map", response_class=HTMLResponse)
def fire_map_page(request: Request, table_name: str = ""):
    table_options = get_fire_map_table_options()
    selected_table = resolve_selected_table(table_options, table_name)

    return templates.TemplateResponse(
        "fire_map.html",
        {
            "request": request,
            "table_options": table_options,
            "selected_table": selected_table,
            "tables_count": len(table_options),
        },
    )


@router.get("/fire-map/embed", response_class=HTMLResponse)
def fire_map_embed(request: Request, table_name: str = ""):
    table_options = get_fire_map_table_options()
    selected_table = resolve_selected_table(table_options, table_name)

    if not table_name or table_name != selected_table:
        return templates.TemplateResponse(
            "fire_map_error.html",
            {"request": request, "message": "Р’С‹Р±РµСЂРёС‚Рµ СЃСѓС‰РµСЃС‚РІСѓСЋС‰СѓСЋ С‚Р°Р±Р»РёС†Сѓ РґР»СЏ РїРѕСЃС‚СЂРѕРµРЅРёСЏ РєР°СЂС‚С‹."},
            status_code=400,
        )

    try:
        map_html = build_fire_map_html(table_name)
        if not map_html:
            return templates.TemplateResponse(
                "fire_map_error.html",
                {
                    "request": request,
                    "message": "Р”Р»СЏ РІС‹Р±СЂР°РЅРЅРѕР№ С‚Р°Р±Р»РёС†С‹ РЅРµ СѓРґР°Р»РѕСЃСЊ СЃРѕР±СЂР°С‚СЊ РєР°СЂС‚Сѓ. РџСЂРѕРІРµСЂСЊС‚Рµ РєРѕРѕСЂРґРёРЅР°С‚С‹ РЁРёСЂРѕС‚Р° Рё Р”РѕР»РіРѕС‚Р°.",
                },
                status_code=422,
            )
        return HTMLResponse(map_html)
    except Exception as exc:
        return templates.TemplateResponse(
            "fire_map_error.html",
            {"request": request, "message": str(exc)},
            status_code=500,
        )


@router.get("/tables", response_class=HTMLResponse)
async def list_tables(request: Request):
    tables = get_all_tables()
    return templates.TemplateResponse("tables.html", {"request": request, "tables": tables})


@router.get("/tables/{table_name}", response_class=HTMLResponse)
async def view_table(request: Request, table_name: str):
    try:
        columns, rows = get_table_data(table_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Table not found") from exc

    return templates.TemplateResponse(
        "table_view.html",
        {"request": request, "table_name": table_name, "columns": columns, "rows": rows},
    )


@router.get("/select_table", response_class=HTMLResponse)
def select_table(request: Request):
    tables = get_all_tables()
    return templates.TemplateResponse(
        "select_table.html",
        {
            "request": request,
            "tables": tables,
            "mandatory_feature_catalog": get_mandatory_feature_catalog(),
            "profiling_css_version": _static_version("profiling.css"),
        },
    )
