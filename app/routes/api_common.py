from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable, TypeVar
from uuid import uuid4

from fastapi import Request
from fastapi.responses import Response

from app.state import SESSION_COOKIE_NAME, job_store


JsonPayload = dict[str, Any]
RouteResult = TypeVar("RouteResult")

logger = logging.getLogger("app.routes.api")
_LOCAL_RUNTIME_NAMES = {"local", "development", "dev", "debug", "test"}


def ensure_session_id(request: Request) -> str:
    return job_store.ensure_session(request.cookies.get(SESSION_COOKIE_NAME))


def coerce_string_list(value: object) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    if value is None:
        return []
    text = str(value).strip()
    return [text] if text else []


def utf8_json(payload: JsonPayload, status_code: int = 200, session_id: str | None = None) -> Response:
    response = Response(
        content=json.dumps(payload, ensure_ascii=False, default=str),
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


def analytics_error_response(
    *,
    code: str,
    message: str,
    status_code: int,
    error_id: str | None = None,
    detail: str | None = None,
) -> Response:
    resolved_error_id = str(error_id or uuid4().hex)
    payload = {
        "ok": False,
        "error": {
            "code": code,
            "message": message,
            "status_code": status_code,
            "error_id": resolved_error_id,
        },
    }
    if detail and should_expose_analytics_detail():
        payload["error"]["detail"] = detail
    return utf8_json(payload, status_code=status_code)


def env_flag_enabled(*names: str) -> bool:
    for name in names:
        value = str(os.getenv(name, "")).strip().lower()
        if value in {"1", "true", "yes", "on"}:
            return True
    return False


def runtime_name() -> str:
    for name in ("FIRE_MONITOR_ENV", "APP_ENV", "FASTAPI_ENV", "PYTHON_ENV", "ENV"):
        value = str(os.getenv(name, "")).strip().lower()
        if value:
            return value
    return "production"


def is_local_or_debug_runtime() -> bool:
    if env_flag_enabled("FIRE_MONITOR_DEBUG", "FASTAPI_DEBUG", "DEBUG"):
        return True
    return runtime_name() in _LOCAL_RUNTIME_NAMES


def should_expose_analytics_detail() -> bool:
    return is_local_or_debug_runtime() and env_flag_enabled("FIRE_MONITOR_EXPOSE_API_ERROR_DETAIL")


def log_analytics_exception(*, code: str, status_code: int, error_id: str, exc: Exception) -> None:
    message = "Analytics API error [%s] %s (%s): %s"
    if status_code >= 500:
        logger.exception(message, error_id, code, status_code, exc)
        return
    logger.warning(message, error_id, code, status_code, exc, exc_info=True)


def analytics_exception_response(
    *,
    code: str,
    message: str,
    status_code: int,
    exc: Exception,
) -> Response:
    error_id = uuid4().hex
    log_analytics_exception(code=code, status_code=status_code, error_id=error_id, exc=exc)
    return analytics_error_response(
        code=code,
        message=message,
        status_code=status_code,
        error_id=error_id,
        detail=str(exc),
    )


def run_analytics_request(
    action: Callable[[], RouteResult],
    *,
    invalid_code: str,
    invalid_message: str,
    failed_code: str,
    failed_message: str,
) -> RouteResult | Response:
    try:
        return action()
    except ValueError as exc:
        return analytics_exception_response(
            code=invalid_code,
            message=str(exc) or invalid_message,
            status_code=400,
            exc=exc,
        )
    except Exception as exc:
        return analytics_exception_response(
            code=failed_code,
            message=failed_message,
            status_code=500,
            exc=exc,
        )
