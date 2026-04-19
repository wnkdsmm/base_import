from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ..ml_model_config_types import (
    FEATURE_COLUMNS,
    LAG_DAYS_BIWEEK,
    LAG_DAYS_SHORT,
    LAG_DAYS_WEEK,
    MIN_TEMPERATURE_COVERAGE,
    NON_TEMPERATURE_FEATURE_COLUMNS,
    ROLLING_WINDOW_LONG_DAYS,
    ROLLING_WINDOW_SHORT_DAYS,
)


def _prepare_reference_frame(frame: pd.DataFrame) -> pd.DataFrame:
    reference = frame.copy().sort_values('date').reset_index(drop=True)
    if 'weekday' not in reference.columns:
        reference['weekday'] = reference['date'].dt.weekday.astype(int)
    if 'event' not in reference.columns:
        reference['event'] = (reference['count'] > 0).astype(int)
    if 'avg_temperature' not in reference.columns:
        reference['avg_temperature'] = np.nan
    return reference


def _detect_trend_warning(dataset: pd.DataFrame) -> Optional[str]:
    if len(dataset) < 60:
        return None
    ordered = dataset.sort_values('date').reset_index(drop=True)
    count_series = pd.to_numeric(ordered['count'], errors='coerce')
    valid = count_series.notna()
    if int(valid.sum()) < 60:
        return None
    counts = count_series.loc[valid].to_numpy(dtype=float)
    time_index = np.arange(len(counts), dtype=float)
    try:
        slope_per_day, _ = np.polyfit(time_index, counts, 1)
    except Exception:
        return None
    slope_per_100_days = float(slope_per_day) * 100.0
    mean_count = float(np.mean(counts))
    relative_trend = slope_per_100_days / max(mean_count, 0.1)
    if abs(relative_trend) <= 0.30:
        return None
    relative_percent = round(relative_trend * 100.0)
    return (
        f"Обнаружен выраженный тренд: среднее число пожаров изменяется на ~{relative_percent}% за 100 дней. "
        "Модель на основе лагов может систематически ошибаться."
    )


def _build_history_frame(history_tail: List[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            'date': pd.to_datetime([item['date'] for item in history_tail]),
            'count': [float(item['count']) for item in history_tail],
            'avg_temperature': [item.get('avg_temperature') for item in history_tail],
        }
    ).sort_values('date').reset_index(drop=True)
    frame['avg_temperature'] = pd.to_numeric(frame['avg_temperature'], errors='coerce')
    return frame
def _feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result['weekday'] = result['date'].dt.weekday.astype(int)
    result['month'] = result['date'].dt.month.astype(int)
    for lag in (LAG_DAYS_SHORT, LAG_DAYS_WEEK, LAG_DAYS_BIWEEK):
        result[f'lag_{lag}'] = result['count'].shift(lag)
    result['rolling_7'] = result['count'].shift(LAG_DAYS_SHORT).rolling(ROLLING_WINDOW_SHORT_DAYS).mean()
    result['rolling_28'] = result['count'].shift(LAG_DAYS_SHORT).rolling(ROLLING_WINDOW_LONG_DAYS).mean()
    result['trend_gap'] = result['rolling_7'] - result['rolling_28']
    return result


def _build_design_matrix(
    frame: pd.DataFrame,
    expected_columns: Optional[List[str]] = None,
    feature_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    selected_columns = feature_columns or FEATURE_COLUMNS
    if expected_columns is not None:
        design = pd.DataFrame(0.0, index=frame.index, columns=expected_columns, dtype=float)
        for column in selected_columns:
            if column in ('weekday', 'month'):
                values = pd.to_numeric(frame[column], errors='coerce') if column in frame.columns else pd.Series(index=frame.index, dtype=float)
                prefix = f'{column}_'
                for expected_column in expected_columns:
                    if not expected_column.startswith(prefix):
                        continue
                    try:
                        category_value = int(expected_column[len(prefix):])
                    except ValueError:
                        continue
                    design.loc[values == category_value, expected_column] = 1.0
                continue
            if column in design.columns and column in frame.columns:
                design[column] = pd.to_numeric(frame[column], errors='coerce').astype(float)
        return design.astype(float)

    design = frame[selected_columns].copy()
    design['weekday'] = design['weekday'].astype(int).astype(str)
    design['month'] = design['month'].astype(int).astype(str)
    design = pd.get_dummies(design, columns=['weekday', 'month'], prefix=['weekday', 'month'], dtype=float, drop_first=True)
    return design.astype(float)


def _build_design_row(
    feature_row: Dict[str, float],
    expected_columns: Optional[List[str]] = None,
    feature_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    selected_columns = feature_columns or FEATURE_COLUMNS
    if expected_columns is None:
        return _build_design_matrix(pd.DataFrame([feature_row], columns=selected_columns), feature_columns=selected_columns)

    row_values = {column: 0.0 for column in expected_columns}
    for column in selected_columns:
        value = feature_row.get(column, 0.0)
        if column in ('weekday', 'month'):
            dummy_column = f'{column}_{int(float(value))}'
            if dummy_column in row_values:
                row_values[dummy_column] = 1.0
            continue
        if column in row_values:
            row_values[column] = float(value)
    return pd.DataFrame([row_values], columns=expected_columns, dtype=float)


def _build_backtest_seed_dataset(
    frame: pd.DataFrame,
    *,
    frame_is_prepared: bool = False,
) -> pd.DataFrame:
    from .training_temperature import _temperature_source_series

    seed_frame = frame if frame_is_prepared else _prepare_reference_frame(frame)
    seed_frame['temp_value'] = _temperature_source_series(seed_frame)
    featured = _feature_frame(seed_frame)
    temp_coverage = featured['temp_value'].notna().mean() if 'temp_value' in featured.columns else 0.0
    filter_columns = (
        NON_TEMPERATURE_FEATURE_COLUMNS + ['temp_value', 'count']
        if temp_coverage >= MIN_TEMPERATURE_COVERAGE
        else NON_TEMPERATURE_FEATURE_COLUMNS + ['count']
    )
    valid_rows = featured[filter_columns].notna().all(axis=1)
    dataset = featured.loc[valid_rows].reset_index(drop=True)
    return dataset


def _prepare_training_dataset(
    frame: pd.DataFrame,
    temperature_stats: Optional[dict[str, Any]] = None,
    *,
    frame_is_prepared: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    from .training_temperature import (
        _apply_temperature_statistics,
        _fit_temperature_statistics,
        _temperature_feature_columns,
    )

    if temperature_stats is None:
        temperature_stats = _fit_temperature_statistics(frame, frame_is_prepared=frame_is_prepared)
    prepared = _apply_temperature_statistics(frame, temperature_stats, frame_is_prepared=frame_is_prepared)
    featured = _feature_frame(prepared)
    feature_columns = _temperature_feature_columns(temperature_stats)
    valid_rows = featured[feature_columns + ['count']].notna().all(axis=1)
    dataset = featured.loc[valid_rows].reset_index(drop=True)
    return prepared, dataset, temperature_stats


