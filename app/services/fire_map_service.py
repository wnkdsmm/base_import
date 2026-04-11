from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from app.runtime_cache import CopyingTtlCache
from app.table_catalog import get_user_table_options, resolve_selected_table_value
from app.services.executive_brief import (
    build_executive_brief_from_risk_payload,
    empty_executive_brief,
)
from app.services.forecast_risk.core import build_decision_support_payload
from config.paths import get_result_folder
from config.settings import Settings
from core.processing.steps.create_fire_map import CreateFireMapStep

_FIRE_MAP_CACHE = CopyingTtlCache(ttl_seconds=120.0, copier=lambda value: value)
_FIRE_MAP_BRIEF_CACHE = CopyingTtlCache(ttl_seconds=120.0)



def clear_fire_map_cache() -> None:
    _FIRE_MAP_CACHE.clear()
    _FIRE_MAP_BRIEF_CACHE.clear()



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



def get_fire_map_page_context(table_name: str = "") -> Dict[str, Any]:
    generated_at = datetime.now().strftime("%d.%m.%Y %H:%M")
    table_options = get_user_table_options()
    selected_table = resolve_selected_table_value(table_options, table_name)
    brief = empty_executive_brief()
    risk_prediction: Dict[str, Any] = {
        "territories": [],
        "notes": [],
    }

    if selected_table:
        cached = _FIRE_MAP_BRIEF_CACHE.get(selected_table)
        if cached is not None:
            brief = dict(cached.get("brief") or brief)
            risk_prediction = dict(cached.get("risk_prediction") or risk_prediction)
        else:
            try:
                risk_prediction = build_decision_support_payload(
                    source_tables=[selected_table],
                    selected_district="all",
                    selected_cause="all",
                    selected_object_category="all",
                    history_window="all",
                    planning_horizon_days=14,
                )
                brief = build_executive_brief_from_risk_payload(
                    risk_prediction,
                    notes=risk_prediction.get("notes"),
                )
            except Exception as exc:
                brief = empty_executive_brief()
                brief["notes"] = [f"Территориальный приоритет на карте временно недоступен: {exc}"]
                risk_prediction = {"territories": [], "notes": list(brief["notes"])}

            _FIRE_MAP_BRIEF_CACHE.set(
                selected_table,
                {
                    "brief": brief,
                    "risk_prediction": risk_prediction,
                },
            )

    return {
        "generated_at": generated_at,
        "table_options": table_options,
        "tables_count": len(table_options),
        "selected_table": selected_table,
        "executive_brief": brief,
        "risk_prediction": risk_prediction,
        "has_data": bool(selected_table),
    }
