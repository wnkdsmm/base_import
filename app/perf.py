from __future__ import annotations

import contextvars
import json
import logging
import threading
import time
from contextlib import contextmanager
from functools import wraps
from typing import Any, Iterator

from sqlalchemy import event

_ACTIVE_TRACE: contextvars.ContextVar["PerformanceTrace | None"] = contextvars.ContextVar(
    "fire_monitor_active_perf_trace",
    default=None,
)
_INSTRUMENTED_ENGINES: set[int] = set()
_INSTRUMENTATION_LOCK = threading.Lock()
_LOGGER = logging.getLogger("app.performance")


def ensure_sqlalchemy_timing(engine: Any) -> None:
    engine_id = id(engine)
    with _INSTRUMENTATION_LOCK:
        if engine_id in _INSTRUMENTED_ENGINES:
            return
        event.listen(engine, "before_cursor_execute", _before_cursor_execute)
        event.listen(engine, "after_cursor_execute", _after_cursor_execute)
        event.listen(engine, "handle_error", _handle_error)
        _INSTRUMENTED_ENGINES.add(engine_id)


def _before_cursor_execute(conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool) -> None:
    context._perf_trace = _ACTIVE_TRACE.get()
    context._perf_started_at = time.perf_counter()


def _after_cursor_execute(conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool) -> None:
    _record_sql_duration(context)


def _handle_error(exception_context: Any) -> None:
    _record_sql_duration(getattr(exception_context, "execution_context", None))


def _record_sql_duration(context: Any) -> None:
    if context is None:
        return
    trace = getattr(context, "_perf_trace", None)
    started_at = getattr(context, "_perf_started_at", None)
    if trace is None or started_at is None:
        return
    elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    trace.add_duration("sql", elapsed_ms)
    trace.increment("sql_queries", 1)
    context._perf_trace = None
    context._perf_started_at = None


class PerformanceTrace:
    def __init__(self, event_name: str, **context: Any) -> None:
        self.event_name = event_name
        self.context: dict[str, Any] = {}
        self.total_ms = 0.0
        self._started_at = 0.0
        self._token: contextvars.Token[PerformanceTrace | None] | None = None
        self._apply_metrics(context)

    def __enter__(self) -> "PerformanceTrace":
        self._started_at = time.perf_counter()
        self._token = _ACTIVE_TRACE.set(self)
        return self

    def __exit__(self, exc_type: Any, exc: BaseException | None, tb: Any) -> None:
        self.total_ms = round((time.perf_counter() - self._started_at) * 1000.0, 2)
        self.context["total_ms"] = self.total_ms
        if "status" not in self.context:
            self.context["status"] = "failed" if exc_type else "ok"
        if exc_type and "error_type" not in self.context:
            self.context["error_type"] = exc_type.__name__
        if self._token is not None:
            _ACTIVE_TRACE.reset(self._token)
            self._token = None
        _LOGGER.debug("%s", self.render())

    @contextmanager
    def span(self, name: str, **context: Any) -> Iterator["PerformanceTrace"]:
        started_at = time.perf_counter()
        try:
            yield self
        finally:
            self.add_duration(name, (time.perf_counter() - started_at) * 1000.0)
            self._apply_metrics(context)

    def add_duration(self, name: str, elapsed_ms: float) -> None:
        key = name if name.endswith("_ms") else f"{name}_ms"
        current_value = float(self.context.get(key, 0.0) or 0.0)
        self.context[key] = round(current_value + float(elapsed_ms), 2)

    def set(self, key: str, value: Any) -> None:
        if value is None:
            return
        self.context[key] = value

    def update(self, **context: Any) -> None:
        self._apply_metrics(context)

    def increment(self, key: str, value: int = 1) -> None:
        current_value = int(self.context.get(key, 0) or 0)
        self.context[key] = current_value + int(value)

    def fail(self, exc: BaseException, *, status: str = "failed", **context: Any) -> None:
        self.context["status"] = status
        self.context["error_type"] = exc.__class__.__name__
        self._apply_metrics(context)

    def render(self) -> str:
        payload = {"event": self.event_name, **self.context}
        return f"performance {json.dumps(payload, ensure_ascii=False, default=_json_default, sort_keys=True)}"

    def _apply_metrics(self, context: dict[str, Any]) -> None:
        for key, value in context.items():
            if value is None:
                continue
            self.context[key] = value


def perf_trace(event_name: str, **context: Any) -> PerformanceTrace:
    return PerformanceTrace(event_name, **context)


def current_perf_trace() -> PerformanceTrace | None:
    return _ACTIVE_TRACE.get()


def profiled(event_name: str, *, engine: Any | None = None):
    def decorator(func: Any) -> Any:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if engine is not None:
                ensure_sqlalchemy_timing(engine)
            with perf_trace(event_name) as trace:
                return func(*args, **kwargs)

        return wrapper

    return decorator


def _json_default(value: Any) -> Any:
    if isinstance(value, (set, tuple)):
        return list(value)
    return str(value)
