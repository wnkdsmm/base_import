from __future__ import annotations

from typing import Any, Callable
from urllib.parse import quote

from fastapi import Request
from fastapi.responses import Response
from fastapi.templating import Jinja2Templates

from config.paths import STATIC_DIR, TEMPLATES_DIR


templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
PUBLIC_CACHE_HEADERS = {"Cache-Control": "public, max-age=86400"}
_DEFAULT_TEMPLATE_ASSETS = {
    "base_css_version": "css/base.css",
    "layout_css_version": "css/layout.css",
    "shared_components_css_version": "css/shared-components.css",
    "analytics_shared_js_version": "js/analytics_shared.js",
    "sidebar_js_version": "js/sidebar.js",
}

ANALYTICS_PAGE_ASSETS = {
    "analytics_css_version": "css/analytics.css",
    "dashboard_css_version": "css/dashboard.css",
}

DASHBOARD_ONLY_ASSETS = {
    "dashboard_css_version": "css/dashboard.css",
}

COLUMN_SEARCH_ASSETS = {
    "column_search_css_version": "css/column_search.css",
}

FIRE_MAP_ASSETS = {
    "fire_map_css_version": "css/fire_map.css",
}

TABLES_ASSETS = {
    "tables_css_version": "css/tables.css",
}

TABLE_VIEW_ASSETS = {
    "analytics_css_version": "css/analytics.css",
    "tables_css_version": "css/tables.css",
}


def _url_path_segment(value: object) -> str:
    return quote(str(value or ""), safe="")


templates.env.filters["urlpath"] = _url_path_segment


def static_version(filename: str) -> int:
    try:
        return int((STATIC_DIR / filename).stat().st_mtime_ns)
    except OSError:
        return 0


def asset_versions(**assets: str) -> dict[str, int]:
    return {key: static_version(path) for key, path in assets.items()}


def build_template_context(request: Request, **context: object) -> dict[str, object]:
    return {
        "request": request,
        **asset_versions(**_DEFAULT_TEMPLATE_ASSETS),
        **context,
    }


def render_template_page(request: Request, template_name: str, **context: object) -> Response:
    status_code = context.pop("status_code", None)
    template_context = build_template_context(request, **context)
    if status_code is None:
        return templates.TemplateResponse(request, template_name, template_context)
    return templates.TemplateResponse(request, template_name, template_context, status_code=status_code)


def render_context_page(
    request: Request,
    template_name: str,
    *,
    context_name: str,
    context_value: object,
    asset_files: dict[str, str] | None = None,
    **context: object,
) -> Response:
    if asset_files:
        context.update(asset_versions(**asset_files))
    context[context_name] = context_value
    return render_template_page(request, template_name, **context)


def cached_text_response(content: str, media_type: str, *, status_code: int = 200) -> Response:
    return Response(
        content=content,
        status_code=status_code,
        media_type=media_type,
        headers=dict(PUBLIC_CACHE_HEADERS),
    )


def empty_cached_response(*, status_code: int = 204) -> Response:
    return Response(status_code=status_code, headers=dict(PUBLIC_CACHE_HEADERS))


def download_text_response(text: str, filename: str) -> Response:
    return Response(
        content=text,
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def resolve_page_mode_context(
    *,
    mode: str,
    page_loader: Callable[..., Any],
    shell_loader: Callable[..., Any],
    page_kwargs: dict[str, object],
    shell_kwargs: dict[str, object] | None = None,
) -> Any:
    if str(mode).strip().lower() != "deferred":
        return page_loader(**page_kwargs)
    return shell_loader(**(shell_kwargs or page_kwargs))
