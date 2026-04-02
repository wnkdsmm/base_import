from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .constants import FEATURE_COLUMNS, NON_TEMPERATURE_FEATURE_COLUMNS


def _build_history_frame(history_tail: List[Dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            'date': pd.to_datetime([item['date'] for item in history_tail]),
            'count': [float(item['count']) for item in history_tail],
            'avg_temperature': [item.get('avg_temperature') for item in history_tail],
        }
    ).sort_values('date').reset_index(drop=True)
    frame['avg_temperature'] = pd.to_numeric(frame['avg_temperature'], errors='coerce')
    return frame


def _prepare_reference_frame(frame: pd.DataFrame) -> pd.DataFrame:
    reference = frame.copy().sort_values('date').reset_index(drop=True)
    if 'weekday' not in reference.columns:
        reference['weekday'] = reference['date'].dt.weekday.astype(int)
    if 'event' not in reference.columns:
        reference['event'] = (reference['count'] > 0).astype(int)
    if 'avg_temperature' not in reference.columns:
        reference['avg_temperature'] = np.nan
    return reference


def _feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result['weekday'] = result['date'].dt.weekday.astype(int)
    result['month'] = result['date'].dt.month.astype(int)
    for lag in (1, 7, 14):
        result[f'lag_{lag}'] = result['count'].shift(lag)
    result['rolling_7'] = result['count'].shift(1).rolling(7).mean()
    result['rolling_28'] = result['count'].shift(1).rolling(28).mean()
    result['trend_gap'] = result['rolling_7'] - result['rolling_28']
    return result


def _build_design_matrix(
    frame: pd.DataFrame,
    expected_columns: Optional[List[str]] = None,
    feature_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    selected_columns = feature_columns or FEATURE_COLUMNS
    design = frame[selected_columns].copy()
    design['weekday'] = design['weekday'].astype(int).astype(str)
    design['month'] = design['month'].astype(int).astype(str)
    design = pd.get_dummies(design, columns=['weekday', 'month'], prefix=['weekday', 'month'], dtype=float, drop_first=True)
    if expected_columns is not None:
        design = design.reindex(columns=expected_columns, fill_value=0.0)
    return design.astype(float)


def _build_backtest_seed_dataset(frame: pd.DataFrame) -> pd.DataFrame:
    from .training_temperature import _temperature_source_series

    seed_frame = _prepare_reference_frame(frame)
    seed_frame['temp_value'] = _temperature_source_series(seed_frame)
    featured = _feature_frame(seed_frame)
    dataset = featured.dropna(subset=NON_TEMPERATURE_FEATURE_COLUMNS + ['count']).copy().reset_index(drop=True)
    dataset['event'] = (dataset['count'] > 0).astype(int)
    return dataset


def _prepare_training_dataset(
    frame: pd.DataFrame,
    temperature_stats: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    from .training_temperature import (
        _apply_temperature_statistics,
        _fit_temperature_statistics,
        _temperature_feature_columns,
    )

    if temperature_stats is None:
        temperature_stats = _fit_temperature_statistics(frame)
    prepared = _apply_temperature_statistics(frame, temperature_stats)
    featured = _feature_frame(prepared)
    feature_columns = _temperature_feature_columns(temperature_stats)
    dataset = featured.dropna(subset=feature_columns + ['count']).copy().reset_index(drop=True)
    dataset['event'] = (dataset['count'] > 0).astype(int)
    return prepared, dataset, temperature_stats
