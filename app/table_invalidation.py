from __future__ import annotations

from typing import Callable

from app.db_metadata import invalidate_db_metadata_cache
from app.runtime_invalidation import invalidate_service_caches


def invalidate_table_related_caches(
    table_name: str | None = None,
    *,
    on_warning: Callable[[str], None] | None = None,
) -> None:
    invalidate_db_metadata_cache(table_name=table_name)
    invalidate_service_caches(on_warning=on_warning)


__all__ = ["invalidate_table_related_caches"]
