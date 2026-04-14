from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple

import numpy as np

from ..ml_model_types import BacktestWindowRow, COUNT_MODEL_KEYS, MIN_POSITIVE_PREDICTION


def _optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    return float(value)


def _nan_or_float(value: Any) -> float:
    return np.nan if value is None else float(value)


def _optional_float_array(values: Sequence[Any]) -> np.ndarray:
    return np.asarray(
        [_nan_or_float(value) for value in values],
        dtype=float,
    )


def _empty_float_array() -> np.ndarray:
    return np.asarray([], dtype=float)


def _empty_int_array() -> np.ndarray:
    return np.asarray([], dtype=int)


def _nan_float_array(length: int) -> np.ndarray:
    return np.full(length, np.nan, dtype=float)


def _selected_count_prediction(
    row: dict[str, Any] | BacktestWindowRow,
    selected_count_model_key: str,
) -> Optional[float]:
    if selected_count_model_key == 'seasonal_baseline':
        return row.get('baseline_count')
    if selected_count_model_key == 'heuristic_forecast':
        return row.get('heuristic_count')
    return row.get('predictions', {}).get(selected_count_model_key)


def _selected_count_arrays_from_rows(
    rows: List[BacktestWindowRow],
    selected_count_model_key: str,
    *,
    include_event_probabilities: Optional[bool] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    predictions: List[float] = []
    collect_event_probabilities = (
        selected_count_model_key in COUNT_MODEL_KEYS
        if include_event_probabilities is None
        else bool(include_event_probabilities)
    )
    event_probabilities: List[float] = []
    for row in rows:
        predictions.append(float(_selected_count_prediction(row, selected_count_model_key)))
        if collect_event_probabilities:
            event_probabilities.append(
                _nan_or_float(row.predicted_event_probabilities.get(selected_count_model_key))
            )
    prediction_array = np.asarray(predictions, dtype=float)
    if not collect_event_probabilities:
        return prediction_array, _nan_float_array(len(rows))
    return prediction_array, np.asarray(event_probabilities, dtype=float)


def _selected_count_predictions(
    rows: List[BacktestWindowRow],
    selected_count_model_key: str,
) -> np.ndarray:
    return _selected_count_arrays_from_rows(
        rows,
        selected_count_model_key,
        include_event_probabilities=False,
    )[0]


def _optional_probability_from_array(value: Any) -> Optional[float]:
    return None if not np.isfinite(value) else float(value)


def _lead_time_label(horizon_days: int) -> str:
    return '1 day' if int(horizon_days) == 1 else f'{int(horizon_days)} days'


def _lead_time_validation_horizons(max_horizon_days: int) -> List[int]:
    return list(range(1, max(1, int(max_horizon_days)) + 1))


def _estimate_overdispersion_ratio(counts: np.ndarray) -> float:
    values = (
        counts.astype(float, copy=False)
        if isinstance(counts, np.ndarray)
        else np.asarray(counts, dtype=float)
    )
    if values.size == 0:
        return 1.0
    mean_value = max(float(np.mean(values)), MIN_POSITIVE_PREDICTION)
    variance = float(np.var(values, ddof=1)) if values.size > 1 else float(np.var(values))
    return max(variance / mean_value, 1.0)
