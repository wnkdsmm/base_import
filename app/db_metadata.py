from __future__ import annotations

from typing import Callable, List, Optional, Tuple

from sqlalchemy import inspect

from app.runtime_cache import CopyingTtlCache
from config.db import engine

_METADATA_CACHE_TTL_SECONDS = 60.0
_TABLE_NAMES_CACHE_KEY = "__table_names__"
_TABLE_NAMES_CACHE = CopyingTtlCache[str, Tuple[str, ...]](ttl_seconds=_METADATA_CACHE_TTL_SECONDS)
_TABLE_COLUMNS_CACHE = CopyingTtlCache[str, Tuple[str, ...]](ttl_seconds=_METADATA_CACHE_TTL_SECONDS)
_TABLE_ORDER_CACHE_INVALIDATORS: list[Callable[[], None]] = []


def register_table_order_cache_invalidator(invalidator: Callable[[], None]) -> None:
    if invalidator in _TABLE_ORDER_CACHE_INVALIDATORS:
        return
    _TABLE_ORDER_CACHE_INVALIDATORS.append(invalidator)


def invalidate_table_order_caches() -> None:
    for invalidator in list(_TABLE_ORDER_CACHE_INVALIDATORS):
        try:
            invalidator()
        except Exception:
            continue



def invalidate_db_metadata_cache(table_name: Optional[str] = None) -> None:
    _TABLE_NAMES_CACHE.delete(_TABLE_NAMES_CACHE_KEY)
    if table_name is None:
        _TABLE_COLUMNS_CACHE.clear()
    else:
        _TABLE_COLUMNS_CACHE.delete(str(table_name))

    invalidate_table_order_caches()



def get_table_names_cached(force_refresh: bool = False) -> List[str]:
    if not force_refresh:
        cached = _TABLE_NAMES_CACHE.get(_TABLE_NAMES_CACHE_KEY)
        if cached is not None:
            return list(cached)

    inspector = inspect(engine)
    table_names = tuple(inspector.get_table_names())
    _TABLE_NAMES_CACHE.set(_TABLE_NAMES_CACHE_KEY, table_names)
    return list(table_names)



def table_exists_cached(table_name: str) -> bool:
    return str(table_name) in set(get_table_names_cached())



def get_table_columns_cached(table_name: str, force_refresh: bool = False) -> List[str]:
    normalized_name = str(table_name)
    if not normalized_name:
        raise ValueError("Table name is required")

    if not force_refresh:
        cached = _TABLE_COLUMNS_CACHE.get(normalized_name)
        if cached is not None:
            return list(cached)

    table_names = get_table_names_cached(force_refresh=force_refresh)
    if normalized_name not in table_names:
        raise ValueError(f"Table '{normalized_name}' does not exist")

    inspector = inspect(engine)
    columns = tuple(column["name"] for column in inspector.get_columns(normalized_name))
    _TABLE_COLUMNS_CACHE.set(normalized_name, columns)
    return list(columns)



def get_table_column_set_cached(table_name: str, force_refresh: bool = False) -> set[str]:
    return set(get_table_columns_cached(table_name, force_refresh=force_refresh))



def get_table_signature_cached() -> Tuple[str, ...]:
    return tuple(sorted(get_table_names_cached()))


__all__ = [
    "get_table_column_set_cached",
    "get_table_columns_cached",
    "get_table_names_cached",
    "get_table_signature_cached",
    "invalidate_db_metadata_cache",
    "invalidate_table_order_caches",
    "register_table_order_cache_invalidator",
    "table_exists_cached",
]
