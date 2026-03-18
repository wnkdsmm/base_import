from __future__ import annotations

from pathlib import Path

from config.paths import get_result_folder
from config.settings import Settings
from core.processing.steps.create_fire_map import CreateFireMapStep


def build_fire_map_html(table_name: str) -> str:
    settings = Settings(
        input_file=None,
        selected_table=table_name,
        output_folder=str(get_result_folder(table_name)),
    )
    output_path = CreateFireMapStep().run(settings, table_name=table_name)
    if not output_path:
        return ""
    return Path(output_path).read_text(encoding="utf-8")
