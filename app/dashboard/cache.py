from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from app.db_metadata import get_table_signature_cached, invalidate_db_metadata_cache
from app.runtime_cache import CopyingTtlCache
from app.statistics_constants import DASHBOARD_CACHE_TTL_SECONDS, METADATA_CACHE_TTL_SECONDS

from .metadata import _collect_dashboard_metadata
from .utils import _select_tables

_DASHBOARD_METADATA_CACHE = CopyingTtlCache[Tuple[str, ...], Dict[str, Any]](ttl_seconds=METADATA_CACHE_TTL_SECONDS)
_DASHBOARD_CACHE = CopyingTtlCache[Tuple[Any, ...], Dict[str, Any]](ttl_seconds=DASHBOARD_CACHE_TTL_SECONDS)


def _current_dashboard_table_names() -> Tuple[str, ...]:
    return tuple(sorted(_select_tables(list(get_table_signature_cached()))))


def _metadata_table_names(metadata: Optional[Dict[str, Any]]) -> Tuple[str, ...]:
    if not metadata:
        return ()
    return tuple(sorted(table["name"] for table in metadata.get("tables", [])))


def _invalidate_dashboard_caches() -> None:
    invalidate_db_metadata_cache()
    _DASHBOARD_METADATA_CACHE.clear()
    _DASHBOARD_CACHE.clear()


def _collect_dashboard_metadata_cached() -> Dict[str, Any]:
    current_table_names = _current_dashboard_table_names()
    cached_value = _DASHBOARD_METADATA_CACHE.get(current_table_names)
    if cached_value is not None and _metadata_table_names(cached_value) == current_table_names:
        return cached_value

    metadata = _collect_dashboard_metadata(current_table_names)
    _DASHBOARD_METADATA_CACHE.clear()
    _DASHBOARD_CACHE.clear()
    return _DASHBOARD_METADATA_CACHE.set(current_table_names, metadata)


def _get_dashboard_cache(cache_key: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    return _DASHBOARD_CACHE.get(cache_key)


def _set_dashboard_cache(cache_key: Tuple[Any, ...], value: Dict[str, Any]) -> None:
    _DASHBOARD_CACHE.set(cache_key, value)


__all__ = [
    "_collect_dashboard_metadata_cached",
    "_current_dashboard_table_names",
    "_get_dashboard_cache",
    "_invalidate_dashboard_caches",
    "_metadata_table_names",
    "_set_dashboard_cache",
]
