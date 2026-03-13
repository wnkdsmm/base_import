from __future__ import annotations

from pathlib import Path

from config.settings import Settings
from steps.create_fire_map import CreateFireMapStep


def build_fire_map_html(table_name: str) -> str:
    settings = Settings(
        input_file=None,
        selected_table=table_name,
        output_folder=str(Path("results") / f"folder_{table_name}"),
    )
    output_path = CreateFireMapStep().run(settings, table_name=table_name)
    if not output_path:
        return ""
    return Path(output_path).read_text(encoding="utf-8")
