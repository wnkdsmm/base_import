from __future__ import annotations

import math
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from app.services.forecasting.data import _build_forecast_rows as _build_scenario_forecast_rows

from .training_forecast import _bound_probability


def _baseline_expected_count(train: pd.DataFrame, target_date: pd.Timestamp) -> float:
    recent_mean = float(train['count'].tail(28).mean()) if not train.empty else 0.0
    same_weekday = train.loc[train['weekday'] == int(target_date.weekday()), 'count'].tail(8)
    if len(same_weekday) >= 3:
        return max(0.0, float(0.6 * same_weekday.mean() + 0.4 * recent_mean))
    return max(0.0, recent_mean)


def _baseline_event_probability(train: pd.DataFrame, target_date: pd.Timestamp) -> Optional[float]:
    if train.empty:
        return None
    recent_rate = float(train['event'].tail(28).mean())
    same_weekday = train.loc[train['weekday'] == int(target_date.weekday()), 'event'].tail(8)
    if len(same_weekday) >= 3:
        probability = 0.6 * float(same_weekday.mean()) + 0.4 * recent_rate
    else:
        probability = recent_rate
    return _bound_probability(probability)


def _scenario_reference_forecast(
    train: pd.DataFrame,
    test: pd.DataFrame,
    temperature_stats: Optional[Dict[str, Any]] = None,
) -> Tuple[float, Optional[float]]:
    if train.empty:
        return 0.0, None

    temperature_usable = bool((temperature_stats or {}).get('usable', True))
    temperature_value = None
    if temperature_usable and 'temp_value' in test.columns:
        candidate = test['temp_value'].iloc[0]
        if not pd.isna(candidate):
            temperature_value = float(candidate)

    train_history = [
        {
            'date': pd.Timestamp(row.date),
            'count': float(row.count),
            'avg_temperature': None if (not temperature_usable or pd.isna(row.avg_temperature)) else float(row.avg_temperature),
        }
        for row in train[['date', 'count', 'avg_temperature']].itertuples(index=False)
    ]
    forecast_rows = _build_scenario_forecast_rows(train_history, 1, temperature_value)
    if not forecast_rows:
        target_date = pd.Timestamp(test['date'].iloc[0])
        fallback_count = _baseline_expected_count(train, target_date)
        return fallback_count, _bound_probability(1.0 - math.exp(-max(0.0, fallback_count)))

    row = forecast_rows[0]
    probability = row.get('fire_probability')
    return max(0.0, float(row.get('forecast_value', 0.0))), _bound_probability(probability if probability is not None else 0.0)
