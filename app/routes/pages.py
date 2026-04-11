from __future__ import annotations

import importlib
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response

from app.db_views import DEFAULT_TABLE_PAGE_SIZE, TABLE_PAGE_SIZE_OPTIONS
from app.domain.column_matching import get_mandatory_feature_catalog
from app.table_catalog import (
    get_user_table_options,
    resolve_selected_table_value,
)
from config.constants import DOMINANT_VALUE_THRESHOLD, LOW_VARIANCE_THRESHOLD, NULL_THRESHOLD

from .page_common import (
    ANALYTICS_PAGE_ASSETS,
    DASHBOARD_ONLY_ASSETS,
    PAGE_MISC_ASSETS,
    TABLE_VIEW_ASSETS,
    asset_versions,
    cached_text_response,
    download_text_response,
    empty_cached_response,
    render_context_page,
    render_template_page,
    resolve_page_mode_context,
    templates,
)


def get_column_search_table_options():
    return get_user_table_options()


def get_fire_map_table_options():
    return get_user_table_options()


def resolve_selected_table(table_options, table_name: str) -> str:
    return resolve_selected_table_value(table_options, table_name)


router = APIRouter()


def _lazy(module_path: str, attr: str):
    """Return a thin wrapper that resolves ``module_path:attr`` on first call.

    Heavy service modules are imported lazily so worker startup stays cheap;
    ``importlib.import_module`` is cached by ``sys.modules``, so the actual
    import only happens once.
    """

    def _invoke(*args, **kwargs):
        return getattr(importlib.import_module(module_path), attr)(*args, **kwargs)

    _invoke.__name__ = attr
    _invoke.__qualname__ = attr
    return _invoke


get_dashboard_page_context = _lazy("app.dashboard.service", "get_dashboard_page_context")
get_dashboard_shell_context = _lazy("app.dashboard.service", "get_dashboard_shell_context")
get_forecasting_page_context = _lazy("app.services.forecasting.core", "get_forecasting_page_context")
get_forecasting_shell_context = _lazy("app.services.forecasting.core", "get_forecasting_shell_context")
get_ml_model_shell_context = _lazy("app.services.ml_model.core", "get_ml_model_shell_context")
get_clustering_page_context = _lazy("app.services.clustering.core", "get_clustering_page_context")
get_access_points_shell_context = _lazy("app.services.access_points.core", "get_access_points_shell_context")
get_fire_map_page_context = _lazy("app.services.fire_map_service", "get_fire_map_page_context")
build_fire_map_html = _lazy("app.services.fire_map_service", "build_fire_map_html")
get_plotly_bundle = _lazy("app.plotly_bundle", "get_plotly_bundle")
get_all_tables = _lazy("app.table_metadata", "get_all_tables")
build_table_page_bundle = _lazy("app.services.table_workflows", "build_table_page_bundle")


def _extract_nested_text(payload: dict[str, Any], *path: str) -> str:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return ""
        current = current.get(key)
    return str(current or "")


def _download_brief_response(initial_data: dict[str, Any], filename: str, *path: str) -> Response:
    text = _extract_nested_text(initial_data, *path)
    return download_text_response(text or "Управленческий бриф пока недоступен.", filename)


PROFILING_DEFAULTS = {
    "null_threshold_percent": round(NULL_THRESHOLD * 100),
    "dominant_value_threshold_percent": round(DOMINANT_VALUE_THRESHOLD * 100),
    "low_variance_threshold": LOW_VARIANCE_THRESHOLD,
}


@router.get("/assets/plotly.js")
def plotly_bundle_asset() -> Response:
    return cached_text_response(get_plotly_bundle(), "application/javascript; charset=utf-8")


@router.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return empty_cached_response()


@router.get("/brief/dashboard.txt")
def dashboard_brief_download(table_name: str = "all", year: str = "all", group_column: str = "") -> Response:
    data = get_dashboard_page_context(table_name=table_name, year=year, group_column=group_column)["initial_data"]
    return _download_brief_response(data, "dashboard-brief.txt", "management", "export_text")


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
    return _download_brief_response(data, "forecasting-brief.txt", "executive_brief", "export_text")


@router.get("/", response_class=HTMLResponse)
def home(
    request: Request,
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
    mode: str = "full",
):
    dashboard = resolve_page_mode_context(
        mode=mode,
        page_loader=get_dashboard_page_context,
        shell_loader=get_dashboard_shell_context,
        page_kwargs={"table_name": table_name, "year": year, "group_column": group_column},
    )
    return render_context_page(
        request,
        "index.html",
        context_name="dashboard",
        context_value=dashboard,
        asset_files={
            **ANALYTICS_PAGE_ASSETS,
            "dashboard_js_version": "js/dashboard.js",
        },
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
    return render_context_page(
        request,
        "forecasting.html",
        context_name="forecast",
        context_value=forecast,
        asset_files={
            **ANALYTICS_PAGE_ASSETS,
            "forecasting_css_version": "forecasting.css",
            "forecasting_js_version": "js/forecasting.js",
        },
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
    page_kwargs = {
        "table_name": table_name,
        "cause": cause,
        "object_category": object_category,
        "temperature": temperature,
        "forecast_days": forecast_days,
        "history_window": history_window,
    }
    ml_model = get_ml_model_shell_context(**page_kwargs, prefer_cached=True)
    return render_context_page(
        request,
        "ml_model.html",
        context_name="ml_model",
        context_value=ml_model,
        asset_files={
            **ANALYTICS_PAGE_ASSETS,
            "ml_model_css_version": "ml_model.css",
            "ml_model_js_version": "js/ml_model.js",
        },
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
        cluster_count_is_explicit="cluster_count" in request.query_params,
    )
    return render_context_page(
        request,
        "clustering.html",
        context_name="clustering",
        context_value=clustering,
        asset_files={
            **ANALYTICS_PAGE_ASSETS,
            "clustering_css_version": "clustering.css",
            "clustering_js_version": "js/clustering.js",
        },
    )


@router.get("/access-points", response_class=HTMLResponse)
def access_points_page(
    request: Request,
    table_name: str = "all",
    district: str = "all",
    year: str = "all",
    limit: str = "25",
    feature_columns: list[str] | None = Query(None),
):
    access_points = get_access_points_shell_context(
        table_name=table_name,
        district=district,
        year=year,
        limit=limit,
        feature_columns=feature_columns or [],
    )
    return render_context_page(
        request,
        "access_points.html",
        context_name="access_points",
        context_value=access_points,
        asset_files={
            **ANALYTICS_PAGE_ASSETS,
            "access_points_css_version": "access_points.css",
            "access_points_js_version": "js/access_points.js",
        },
    )


@router.get("/column-search", response_class=HTMLResponse)
def column_search_page(request: Request, table_name: str = "", query: str = ""):
    table_options = get_column_search_table_options()
    selected_table = resolve_selected_table(table_options, table_name)
    return render_template_page(
        request,
        "column_search.html",
        table_options=table_options,
        selected_table=selected_table,
        initial_query=query,
        **asset_versions(
            **PAGE_MISC_ASSETS,
            column_search_js_version="js/column_search.js",
        ),
    )


@router.get("/fire-map", response_class=HTMLResponse)
def fire_map_page(request: Request, table_name: str = ""):
    fire_map = get_fire_map_page_context(table_name)
    return render_context_page(
        request,
        "fire_map.html",
        context_name="fire_map",
        context_value=fire_map,
        asset_files={**DASHBOARD_ONLY_ASSETS, **PAGE_MISC_ASSETS},
    )


@router.get("/fire-map/embed", response_class=HTMLResponse)
def fire_map_embed(request: Request, table_name: str = ""):
    table_options = get_fire_map_table_options()
    selected_table = resolve_selected_table(table_options, table_name)

    if not table_name or table_name != selected_table:
        return render_template_page(
            request,
            "fire_map_error.html",
            message="Выберите существующую таблицу для построения карты.",
            status_code=400,
            **asset_versions(**PAGE_MISC_ASSETS),
        )

    try:
        map_html = build_fire_map_html(table_name)
        if not map_html:
            return render_template_page(
                request,
                "fire_map_error.html",
                message="Для выбранной таблицы не удалось собрать карту. Проверьте координаты, даты и наличие записей.",
                status_code=422,
                **asset_versions(**PAGE_MISC_ASSETS),
            )
        return HTMLResponse(map_html)
    except Exception as exc:
        return render_template_page(
            request,
            "fire_map_error.html",
            message=str(exc),
            status_code=500,
            **asset_versions(**PAGE_MISC_ASSETS),
        )


@router.get("/tables", response_class=HTMLResponse)
async def list_tables(request: Request):
    tables = get_all_tables()
    return render_template_page(
        request,
        "tables.html",
        tables=tables,
        **asset_versions(
            **PAGE_MISC_ASSETS,
            import_js_version="js/import.js",
            tables_js_version="js/tables.js",
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
        table_bundle = build_table_page_bundle(table_name=table_name, page=page, page_size=page_size)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Table not found") from exc

    table_page = table_bundle["table_page"]
    return render_template_page(
        request,
        "table_view.html",
        table_name=table_name,
        columns=table_page["columns"],
        rows=table_page["rows"],
        pagination=table_page,
        page_size_options=TABLE_PAGE_SIZE_OPTIONS,
        table_summary=table_bundle["table_summary"],
        **asset_versions(
            **TABLE_VIEW_ASSETS,
            table_view_js_version="js/table_view.js",
        ),
    )


@router.get("/select_table", response_class=HTMLResponse)
def select_table(request: Request):
    tables = get_all_tables()
    return render_template_page(
        request,
        "select_table.html",
        tables=tables,
        mandatory_feature_catalog=get_mandatory_feature_catalog(),
        profiling_defaults=PROFILING_DEFAULTS,
        **asset_versions(
            profiling_css_version="profiling.css",
            select_table_js_version="js/select_table.js",
        ),
    )
