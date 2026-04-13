from __future__ import annotations

from typing import Any, Dict, Sequence

from .charts_impl import build_access_points_points_scatter_chart


def _build_points_scatter_chart(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    return build_access_points_points_scatter_chart(rows)
