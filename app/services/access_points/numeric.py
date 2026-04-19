from __future__ import annotations

import math
from typing import Any, Sequence

import numpy as np
import pandas as pd


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


def _normalize_coordinate(value: Any, lower_bound: float, upper_bound: float) -> float | None:
    coordinate = _normalize_nullable_float(value)
    if coordinate is None or not (lower_bound <= coordinate <= upper_bound):
        return None
    return coordinate


def _finite_numeric_frame(frame: pd.DataFrame) -> pd.DataFrame:
    numeric_frame = pd.DataFrame(index=frame.index)
    for column in frame.columns:
        numeric_values = pd.to_numeric(frame[column], errors="coerce")
        numeric_values = numeric_values.astype(float)
        numeric_frame[column] = numeric_values.where(pd.notna(numeric_values) & np.isfinite(numeric_values))
    return numeric_frame


def _finite_numeric_columns(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    source = pd.DataFrame(index=frame.index)
    for column in columns:
        source[column] = frame[column] if column in frame.columns else np.nan
    return _finite_numeric_frame(source)


def _clip_share_series(values: pd.Series, default: float = 0.0) -> pd.Series:
    return values.fillna(float(default)).clip(lower=0.0, upper=1.0)


def _nullable_series_values(values: pd.Series) -> np.ndarray:
    return values.astype(object).where(values.notna(), None).to_numpy(copy=False)


def _finite_series_max(values: pd.Series, default: float = 0.0) -> float:
    finite_values = values.dropna()
    return float(finite_values.max()) if not finite_values.empty else float(default)
