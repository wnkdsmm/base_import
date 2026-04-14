from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import numpy as np

from app.services.forecasting.utils import _format_number, _format_percent


def _resolve_interval_calibration(interval_calibration: dict[str, Any], horizon_days: int) -> dict[str, Any]:
    if 'absolute_error_quantile' in interval_calibration:
        return interval_calibration

    calibration_map = interval_calibration.get('by_horizon') if isinstance(interval_calibration, dict) else None
    if calibration_map is None and isinstance(interval_calibration, dict):
        calibration_map = interval_calibration
    if not calibration_map:
        raise KeyError(f'Prediction interval calibration for horizon {horizon_days} is unavailable.')

    direct_match = calibration_map.get(horizon_days) or calibration_map.get(str(horizon_days))
    if direct_match is not None:
        return direct_match

    available_horizons = []
    for key in calibration_map.keys():
        try:
            available_horizons.append(int(key))
        except (TypeError, ValueError):
            continue
    if not available_horizons:
        raise KeyError(f'Prediction interval calibration for horizon {horizon_days} is unavailable.')

    fallback_horizon = max((candidate for candidate in available_horizons if candidate <= horizon_days), default=max(available_horizons))
    fallback = calibration_map.get(fallback_horizon) or calibration_map.get(str(fallback_horizon))
    if fallback is None:
        raise KeyError(f'Prediction interval calibration for horizon {horizon_days} is unavailable.')
    return fallback


def _format_ratio_percent(value: Optional[float]) -> str:
    if value is None:
        return 'вЂ”'
    return f"{_format_number(float(value) * 100.0)}%"


def _forecast_interval_coverage_metadata(calibration: dict[str, Any]) -> dict[str, Any]:
    validated_coverage = calibration.get('validated_coverage')
    return {
        'prediction_interval_coverage_validated': bool(calibration.get('coverage_validated', False)),
        'prediction_interval_coverage': validated_coverage,
        'prediction_interval_coverage_display': _format_ratio_percent(validated_coverage),
    }


def _prediction_interval_margin(prediction: float, calibration: dict[str, Any]) -> float:
    center = max(0.0, float(prediction))
    minimum_floor_raw = calibration.get('minimum_absolute_error_quantile')
    minimum_floor = None if minimum_floor_raw is None else max(0.0, float(minimum_floor_raw or 0.0))
    adaptive_bins = calibration.get('adaptive_bins') or []
    edge_values = calibration.get('adaptive_bin_edges') or []
    if adaptive_bins:
        bin_index = int(np.searchsorted(np.asarray(edge_values, dtype=float), center, side='right'))
        bin_index = min(max(bin_index, 0), len(adaptive_bins) - 1)
        bin_quantile = adaptive_bins[bin_index].get('absolute_error_quantile')
        if bin_quantile is not None:
            margin = max(0.0, float(bin_quantile))
            return max(minimum_floor, margin) if minimum_floor is not None else margin

    fallback_margin = max(0.0, float(calibration.get('absolute_error_quantile', 0.0)))
    return max(minimum_floor, fallback_margin) if minimum_floor is not None else fallback_margin


def _count_interval(prediction: float, calibration: dict[str, Any]) -> Tuple[float, float]:
    margin = _prediction_interval_margin(prediction, calibration)
    center = max(0.0, float(prediction))
    lower = max(0.0, center - margin)
    upper = max(lower, center + margin)
    return lower, upper


def _interval_coverage(
    actuals: np.ndarray,
    predictions: np.ndarray,
    calibration: dict[str, Any],
) -> Optional[float]:
    actual_values = np.asarray(actuals, dtype=float)
    prediction_values = np.asarray(predictions, dtype=float)
    if actual_values.size == 0 or prediction_values.size == 0 or actual_values.size != prediction_values.size:
        return None

    covered = []
    for actual_value, prediction_value in zip(actual_values, prediction_values):
        lower_bound, upper_bound = _count_interval(float(prediction_value), calibration)
        covered.append(lower_bound <= float(actual_value) <= upper_bound)
    return float(np.mean(covered)) if covered else None


def _risk_index(prediction: float, sorted_history_counts: np.ndarray) -> float:
    if sorted_history_counts.size == 0:
        return 0.0
    rank = float(np.searchsorted(sorted_history_counts, prediction, side='right'))
    return min(100.0, max(0.0, rank / float(sorted_history_counts.size) * 100.0))


def _risk_band_from_index(risk_index: float) -> Tuple[str, str]:
    if risk_index >= 90.0:
        return 'РћС‡РµРЅСЊ РІС‹СЃРѕРєРёР№', 'critical'
    if risk_index >= 75.0:
        return 'Р’С‹СЃРѕРєРёР№', 'high'
    if risk_index >= 50.0:
        return 'РЎСЂРµРґРЅРёР№', 'medium'
    if risk_index >= 25.0:
        return 'РќРёР¶Рµ СЃСЂРµРґРЅРµРіРѕ', 'low'
    return 'РќРёР·РєРёР№', 'minimal'


def _bound_probability(value: float) -> float:
    return min(1.0, max(0.0, float(value)))


def _format_probability(value: Optional[float]) -> str:
    if value is None:
        return 'вЂ”'
    return _format_percent(float(value) * 100.0)
