from __future__ import annotations

"""Dashboard cache orchestration.

This module keeps dashboard-specific cache keys and invalidation logic on top of
generic primitives from ``app.cache``.
"""

from typing import Any

from app.db_metadata import get_table_signature_cached, invalidate_db_metadata_cache
from app.cache import CopyingTtlCache, clone_mutable_payload, freeze_mutable_payload
from app.table_catalog import select_user_table_names
from app.statistics_constants import DASHBOARD_CACHE_TTL_SECONDS, METADATA_CACHE_TTL_SECONDS

from .metadata import _collect_dashboard_metadata

_DASHBOARD_METADATA_CACHE = CopyingTtlCache[tuple[str, ...], dict[str, Any]](
    ttl_seconds=METADATA_CACHE_TTL_SECONDS,
    storer=freeze_mutable_payload,
    loader=clone_mutable_payload,
)
_DASHBOARD_CACHE = CopyingTtlCache[tuple[Any, ...], dict[str, Any]](
    ttl_seconds=DASHBOARD_CACHE_TTL_SECONDS,
    storer=freeze_mutable_payload,
    loader=clone_mutable_payload,
)


def _current_dashboard_table_names() -> tuple[str, ...]:
    return tuple(sorted(select_user_table_names(list(get_table_signature_cached()))))


def _metadata_table_names(metadata: dict[str, Any | None]) -> tuple[str, ...]:
    if not metadata:
        return ()
    cached_signature = metadata.get("table_signature")
    if cached_signature:
        return tuple(str(table_name) for table_name in cached_signature)
    return tuple(sorted(table["name"] for table in metadata.get("tables", [])))


def _invalidate_dashboard_caches() -> None:
    invalidate_db_metadata_cache()
    _DASHBOARD_METADATA_CACHE.clear()
    _DASHBOARD_CACHE.clear()


def _collect_dashboard_metadata_cached() -> dict[str, Any]:
    current_table_names = _current_dashboard_table_names()
    cached_value = _DASHBOARD_METADATA_CACHE.get(current_table_names)
    if cached_value is not None and _metadata_table_names(cached_value) == current_table_names:
        return cached_value

    metadata = _collect_dashboard_metadata(current_table_names)
    _DASHBOARD_METADATA_CACHE.clear()
    _DASHBOARD_CACHE.clear()
    _DASHBOARD_METADATA_CACHE.set(current_table_names, metadata)
    return metadata


def _get_dashboard_cache(cache_key: tuple[Any, ...]) -> dict[str, Any | None]:
    return _DASHBOARD_CACHE.get(cache_key)


def _set_dashboard_cache(cache_key: tuple[Any, ...], value: dict[str, Any]) -> None:
    _DASHBOARD_CACHE.set(cache_key, value)


__all__ = [
    "_collect_dashboard_metadata_cached",
    "_current_dashboard_table_names",
    "_get_dashboard_cache",
    "_invalidate_dashboard_caches",
    "_metadata_table_names",
    "_set_dashboard_cache",
]

