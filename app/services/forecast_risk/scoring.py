from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from .profiles import DEFAULT_RISK_WEIGHT_MODE
from .scoring_compute import _build_territory_rows as _build_territory_rows_impl
from .scoring_ranking import _top_territory_lead as _top_territory_lead_impl


def _build_territory_rows(
    records: Sequence[dict[str, Any]],
    planning_horizon_days: int,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
    profile_override: Optional[dict[str, Any]] = None,
) -> List[dict[str, Any]]:
    return _build_territory_rows_impl(
        records=records,
        planning_horizon_days=planning_horizon_days,
        weight_mode=weight_mode,
        profile_override=profile_override,
    )


def _top_territory_lead(top_territory: Optional[dict[str, Any]]) -> str:
    return _top_territory_lead_impl(top_territory)
