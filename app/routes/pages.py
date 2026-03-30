from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates

from app.db_views import DEFAULT_TABLE_PAGE_SIZE, TABLE_PAGE_SIZE_OPTIONS, get_all_tables, get_table_page
from app.plotly_bundle import get_plotly_bundle
from app.services.access_points_service import get_access_points_shell_context
from app.services.clustering_service import get_clustering_page_context
from app.services.dashboard_service import get_dashboard_page_context, get_dashboard_shell_context
from app.services.fire_map_service import build_fire_map_html, get_fire_map_page_context
from app.services.forecasting_service import get_forecasting_page_context, get_forecasting_shell_context
from app.services.ml_model_service import get_ml_model_shell_context
from app.services.table_options import (
    get_column_search_table_options,
    get_fire_map_table_options,
    resolve_selected_table,
)
from app.services.table_summary import build_table_page_summary
from config.constants import DOMINANT_VALUE_THRESHOLD, LOW_VARIANCE_THRESHOLD, NULL_THRESHOLD
from config.paths import STATIC_DIR, TEMPLATES_DIR
from core.processing.steps.keep_important_columns import get_mandatory_feature_catalog


router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _static_version(filename: str) -> int:
    try:
        return int((STATIC_DIR / filename).stat().st_mtime_ns)
    except OSError:
        return 0


def _base_template_context(request: Request, **context: object) -> dict[str, object]:
    return {
        "request": request,
        "style_css_version": _static_version("style.css"),
        "sidebar_js_version": _static_version("js/sidebar.js"),
        **context,
    }


PROFILING_DEFAULTS = {
    "null_threshold_percent": round(NULL_THRESHOLD * 100),
    "dominant_value_threshold_percent": round(DOMINANT_VALUE_THRESHOLD * 100),
    "low_variance_threshold": LOW_VARIANCE_THRESHOLD,
}

@router.get("/assets/plotly.js")
def plotly_bundle_asset() -> Response:
    return Response(
        content=get_plotly_bundle(),
        media_type="application/javascript; charset=utf-8",
        headers={"Cache-Control": "public, max-age=86400"},
    )


@router.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204, headers={"Cache-Control": "public, max-age=86400"})


def _download_text_response(text: str, filename: str) -> Response:
    return Response(
        content=text,
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/brief/dashboard.txt")
def dashboard_brief_download(table_name: str = "all", year: str = "all", group_column: str = "") -> Response:
    data = get_dashboard_page_context(table_name=table_name, year=year, group_column=group_column)["initial_data"]
    text = str((data.get("management") or {}).get("export_text") or "")
    return _download_text_response(text or "Управленческий бриф пока недоступен.", "dashboard-brief.txt")


@router.get("/brief/forecasting.txt")
def forecasting_brief_download(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> Response:
    data = get_forecasting_page_context(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
    )["initial_data"]
    text = str((data.get("executive_brief") or {}).get("export_text") or "")
    return _download_text_response(text or "Управленческий бриф пока недоступен.", "forecasting-brief.txt")


@router.get("/brief/fire-map.txt")
def fire_map_brief_download(table_name: str = "") -> Response:
    data = get_fire_map_page_context(table_name)
    text = str((data.get("executive_brief") or {}).get("export_text") or "")
    return _download_text_response(text or "Управленческий бриф по карте пока недоступен.", "fire-map-brief.txt")


@router.get("/", response_class=HTMLResponse)
def home(
    request: Request,
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
    mode: str = "full",
):
    use_full_context = str(mode).strip().lower() != "deferred"
    dashboard = (
        get_dashboard_page_context(table_name=table_name, year=year, group_column=group_column)
        if use_full_context
        else get_dashboard_shell_context(table_name=table_name, year=year, group_column=group_column)
    )
    return templates.TemplateResponse(
        "index.html",
        _base_template_context(
            request,
            dashboard=dashboard,
            dashboard_js_version=_static_version("js/dashboard.js"),
        ),
    )


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
    forecast = get_forecasting_shell_context(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
    )
    return templates.TemplateResponse(
        "forecasting.html",
        _base_template_context(
            request,
            forecast=forecast,
            forecasting_css_version=_static_version("forecasting.css"),
            forecasting_js_version=_static_version("js/forecasting.js"),
        ),
    )


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
    ml_model = get_ml_model_shell_context(
        table_name=table_name,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
    )
    return templates.TemplateResponse(
        "ml_model.html",
        _base_template_context(
            request,
            ml_model=ml_model,
            ml_model_css_version=_static_version("ml_model.css"),
            ml_model_js_version=_static_version("js/ml_model.js"),
        ),
    )


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
        _base_template_context(
            request,
            clustering=clustering,
            clustering_css_version=_static_version("clustering.css"),
            clustering_js_version=_static_version("js/clustering.js"),
        ),
    )


@router.get("/access-points", response_class=HTMLResponse)
def access_points_page(
    request: Request,
    table_name: str = "all",
    district: str = "all",
    year: str = "all",
    limit: str = "25",
):
    access_points = get_access_points_shell_context(
        table_name=table_name,
        district=district,
        year=year,
        limit=limit,
    )
    return templates.TemplateResponse(
        "access_points.html",
        _base_template_context(
            request,
            access_points=access_points,
            access_points_css_version=_static_version("access_points.css"),
            access_points_js_version=_static_version("js/access_points.js"),
        ),
    )


@router.get("/column-search", response_class=HTMLResponse)
def column_search_page(request: Request, table_name: str = "", query: str = ""):
    table_options = get_column_search_table_options()
    selected_table = resolve_selected_table(table_options, table_name)

    return templates.TemplateResponse(
        "column_search.html",
        _base_template_context(
            request,
            table_options=table_options,
            selected_table=selected_table,
            initial_query=query,
            column_search_js_version=_static_version("js/column_search.js"),
        ),
    )


@router.get("/fire-map", response_class=HTMLResponse)
def fire_map_page(request: Request, table_name: str = ""):
    fire_map = get_fire_map_page_context(table_name)

    return templates.TemplateResponse(
        "fire_map.html",
        _base_template_context(
            request,
            fire_map=fire_map,
        ),
    )


@router.get("/fire-map/embed", response_class=HTMLResponse)
def fire_map_embed(request: Request, table_name: str = ""):
    table_options = get_fire_map_table_options()
    selected_table = resolve_selected_table(table_options, table_name)

    if not table_name or table_name != selected_table:
        return templates.TemplateResponse(
            "fire_map_error.html",
            _base_template_context(
                request,
                message="Выберите существующую таблицу для построения карты.",
            ),
            status_code=400,
        )

    try:
        map_html = build_fire_map_html(table_name)
        if not map_html:
            return templates.TemplateResponse(
                "fire_map_error.html",
                _base_template_context(
                    request,
                    message="Для выбранной таблицы не удалось собрать карту. Проверьте координаты, даты и наличие записей.",
                ),
                status_code=422,
            )
        return HTMLResponse(map_html)
    except Exception as exc:
        return templates.TemplateResponse(
            "fire_map_error.html",
            _base_template_context(request, message=str(exc)),
            status_code=500,
        )


@router.get("/tables", response_class=HTMLResponse)
async def list_tables(request: Request):
    tables = get_all_tables()
    return templates.TemplateResponse(
        "tables.html",
        _base_template_context(
            request,
            tables=tables,
            import_js_version=_static_version("js/import.js"),
            tables_js_version=_static_version("js/tables.js"),
        ),
    )


@router.get("/tables/{table_name}", response_class=HTMLResponse)
async def view_table(
    request: Request,
    table_name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_TABLE_PAGE_SIZE, ge=1),
):
    try:
        table_page = get_table_page(table_name, page=page, page_size=page_size)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Table not found") from exc

    columns = table_page["columns"]
    rows = table_page["rows"]
    table_summary = build_table_page_summary(
        table_name=table_name,
        columns=columns,
        rows=rows,
        total_rows=table_page["total_rows"],
        page_row_start=table_page["page_row_start"],
        page_row_end=table_page["page_row_end"],
    )

    return templates.TemplateResponse(
        "table_view.html",
        _base_template_context(
            request,
            table_name=table_name,
            columns=columns,
            rows=rows,
            pagination=table_page,
            page_size_options=TABLE_PAGE_SIZE_OPTIONS,
            table_summary=table_summary,
            table_view_js_version=_static_version("js/table_view.js"),
        ),
    )


@router.get("/select_table", response_class=HTMLResponse)
def select_table(request: Request):
    tables = get_all_tables()
    return templates.TemplateResponse(
        "select_table.html",
        _base_template_context(
            request,
            tables=tables,
            mandatory_feature_catalog=get_mandatory_feature_catalog(),
            profiling_css_version=_static_version("profiling.css"),
            profiling_defaults=PROFILING_DEFAULTS,
        ),
    )
