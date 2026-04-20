from __future__ import annotations

from typing import Any, Callable, Sequence

from app.cache import CopyingTtlCache


class QueryBuilder:
    def __init__(
        self,
        hook_resolver: Callable[[str | None, Any]] = None,
        *,
        cache: CopyingTtlCache | None = None,
        cache_ttl_seconds: float = 120.0,
    ) -> None:
        self._hook_resolver = hook_resolver
        # SQL builder cache stores immutable/read-only payloads (query text, counters, flags),
        # so we can skip freeze/clone overhead safely.
        self._cache = cache or CopyingTtlCache(ttl_seconds=cache_ttl_seconds, skip_freeze=True)

    @property
    def cache(self) -> CopyingTtlCache:
        return self._cache

    def set_hook_resolver(self, hook_resolver: Callable[[str | None, Any]]) -> None:
        self._hook_resolver = hook_resolver

    def _resolve_hook(self, name: str, default: Callable[..., Any]) -> Callable[..., Any]:
        if self._hook_resolver is None:
            return default
        try:
            candidate = self._hook_resolver(name)
        except Exception:
            return default
        return candidate if callable(candidate) else default

    def _build_sql_cache_key(self, prefix: str, source_tables: Sequence[str], *parts: Any) -> tuple[Any, ...]:
        return (prefix, *tuple(source_tables), *parts)
