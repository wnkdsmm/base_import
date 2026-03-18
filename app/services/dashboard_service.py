from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from app.statistics import build_dashboard_context, get_dashboard_data


def get_dashboard_page_context(
    table_name: str = "all",
    year: str = "all",
    group_column: str = "",
) -> Dict[str, Any]:
    try:
        return build_dashboard_context(table_name=table_name, year=year, group_column=group_column)
    except Exception as exc:
        return {
            "generated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "filters": {
                "tables": [{"value": "all", "label": "Все таблицы"}],
                "years": [],
                "group_columns": [],
            },
            "initial_data": get_dashboard_data(),
            "errors": [str(exc)],
            "has_data": False,
        }
