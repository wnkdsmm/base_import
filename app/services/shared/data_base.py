from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, TypeVar

from config.db import engine

_T = TypeVar("_T")


class DataLoader:
    """Base helper for service data modules (DB fetch -> filter -> cache)."""

    def __init__(self, *, cache: Any | None = None, cache_namespace: str = "data_loader") -> None:
        self._cache = cache
        self._cache_namespace = cache_namespace

    @property
    def cache(self) -> Any | None:
        return self._cache

    def clear_cache(self) -> None:
        if self._cache is not None and hasattr(self._cache, "clear"):
            self._cache.clear()

    def build_cache_key(self, *parts: Any, prefix: str | None = None) -> tuple[Any, ...]:
        normalized_parts = tuple(self._normalize_cache_part(part) for part in parts)
        return (prefix or self._cache_namespace, *normalized_parts)

    def cache_get(self, key: tuple[Any, ...]) -> Any:
        if self._cache is None:
            return None
        return self._cache.get(key)

    def cache_set(self, key: tuple[Any, ...], payload: _T) -> _T:
        if self._cache is None:
            return payload
        return self._cache.set(key, payload)

    def get_or_build_cached(self, key: tuple[Any, ...], builder: Callable[[], _T]) -> _T:
        cached = self.cache_get(key)
        if cached is not None:
            return cached
        return self.cache_set(key, builder())

    @staticmethod
    def execute_rows(query: Any, params: Mapping[str, Any] | None = None, *, mappings: bool = False) -> list[Any]:
        with engine.connect() as conn:
            result = conn.execute(query, dict(params or {}))
            if mappings:
                return list(result.mappings().all())
            return list(result)

    @staticmethod
    def execute_scalar(query: Any, params: Mapping[str, Any] | None = None) -> Any:
        with engine.connect() as conn:
            return conn.execute(query, dict(params or {})).scalar()

    @staticmethod
    def normalize_value(value: Any, *, default: str = "all") -> str:
        normalized = str(value or "").strip()
        return normalized or default

    @staticmethod
    def resolve_option_value(options: Sequence[Dict[str, str]], selected_value: object, default: str = "all") -> str:
        normalized = DataLoader.normalize_value(selected_value, default=default)
        available = {str(option.get("value") or "") for option in options}
        if normalized in available:
            return normalized
        return str(options[0].get("value") or default) if options else default

    @staticmethod
    def clamp_int_choice(value: Any, allowed_values: Sequence[int], default: int) -> int:
        try:
            parsed = int(str(value).strip())
        except Exception:
            return default
        if parsed in allowed_values:
            return parsed
        return min(allowed_values, key=lambda item: abs(item - parsed))

    @staticmethod
    def collect_with_notes(
        items: Iterable[Any],
        loader: Callable[[Any], _T],
        *,
        note_builder: Callable[[Any, Exception], str] | None = None,
    ) -> tuple[list[_T], list[str]]:
        payloads: list[_T] = []
        notes: list[str] = []
        for item in items:
            try:
                payloads.append(loader(item))
            except Exception as exc:
                if note_builder is None:
                    notes.append(str(exc))
                else:
                    notes.append(note_builder(item, exc))
        return payloads, notes

    @staticmethod
    def filter_records(records: Sequence[_T], *predicates: Callable[[_T], bool]) -> list[_T]:
        filtered = list(records)
        for predicate in predicates:
            filtered = [item for item in filtered if predicate(item)]
        return filtered

    @staticmethod
    def _normalize_cache_part(part: Any) -> Any:
        if isinstance(part, str):
            return part.strip()
        if isinstance(part, list):
            return tuple(DataLoader._normalize_cache_part(item) for item in part)
        if isinstance(part, tuple):
            return tuple(DataLoader._normalize_cache_part(item) for item in part)
        if isinstance(part, dict):
            return tuple(
                (str(key), DataLoader._normalize_cache_part(value))
                for key, value in sorted(part.items(), key=lambda item: str(item[0]))
            )
        return part


__all__ = ["DataLoader"]
