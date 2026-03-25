from __future__ import annotations

from pathlib import Path

from app.runtime_cache import CopyingTtlCache
from config.paths import get_result_folder
from config.settings import Settings
from core.processing.steps.create_fire_map import CreateFireMapStep

_FIRE_MAP_CACHE = CopyingTtlCache(ttl_seconds=120.0, copier=lambda value: value)



def clear_fire_map_cache() -> None:
    _FIRE_MAP_CACHE.clear()



def build_fire_map_html(table_name: str) -> str:
    normalized_table_name = str(table_name or "").strip()
    if not normalized_table_name:
        return ""

    cached_html = _FIRE_MAP_CACHE.get(normalized_table_name)
    if cached_html is not None:
        return cached_html

    settings = Settings(
        input_file=None,
        selected_table=normalized_table_name,
        output_folder=str(get_result_folder(normalized_table_name)),
    )
    output_path = CreateFireMapStep().run(settings, table_name=normalized_table_name)
    if not output_path:
        return ""

    html = Path(output_path).read_text(encoding="utf-8")
    if not html:
        return ""
    return _FIRE_MAP_CACHE.set(normalized_table_name, html)