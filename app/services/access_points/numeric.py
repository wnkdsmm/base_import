from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from app.services.forecast_risk.utils import _clamp


def _share(numerator: float, denominator: float) -> float:
    try:
        numeric_numerator = float(numerator)
        numeric_denominator = float(denominator)
    except Exception:
        return 0.0
    if numeric_denominator <= 0 or not math.isfinite(numeric_numerator) or not math.isfinite(numeric_denominator):
        return 0.0
    return numeric_numerator / numeric_denominator


def _safe_mean(total: float, count: int) -> float | None:
    if count <= 0:
        return None
    return _share(total, count)


def _normalize_nullable_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if pd.isna(numeric) or not math.isfinite(numeric):
        return None
    return numeric


def _normalize_float(value: Any, default: float = 0.0) -> float:
    numeric = _normalize_nullable_float(value)
    return float(default) if numeric is None else numeric


def _normalize_share(value: Any, default: float = 0.0) -> float:
    return _clamp(_normalize_float(value, default), 0.0, 1.0)


def _normalize_coordinate(value: Any, lower_bound: float, upper_bound: float) -> float | None:
    coordinate = _normalize_nullable_float(value)
    if coordinate is None or not (lower_bound <= coordinate <= upper_bound):
        return None
    return coordinate


def _finite_numeric_series(values: Any, default: float | None = None) -> pd.Series:
    raw_values = values if isinstance(values, pd.Series) else pd.Series(values)
    numeric_values = pd.to_numeric(raw_values, errors="coerce")
    if not isinstance(numeric_values, pd.Series):
        numeric_values = pd.Series(numeric_values, index=getattr(raw_values, "index", None))
    numeric_values = numeric_values.astype(float)
    finite_values = numeric_values.where(pd.notna(numeric_values) & np.isfinite(numeric_values))
    if default is not None:
        finite_values = finite_values.fillna(float(default))
    return finite_values


def _normalize_share_series(values: Any, default: float = 0.0) -> pd.Series:
    return _finite_numeric_series(values, default=default).clip(lower=0.0, upper=1.0)


def _finite_numeric_max(values: Any, default: float = 0.0) -> float:
    finite_values = _finite_numeric_series(values).dropna()
    return float(finite_values.max()) if not finite_values.empty else float(default)
