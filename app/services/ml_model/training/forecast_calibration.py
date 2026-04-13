from __future__ import annotations

from datetime import date
import math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .forecast_bounds import _count_interval, _format_ratio_percent
from ..ml_model_types import (
    MIN_INTERVAL_BIN_RESIDUALS,
    MIN_INTERVAL_CALIBRATION_WINDOWS,
    MIN_INTERVAL_EVALUATION_WINDOWS,
    PREDICTION_INTERVAL_BLOCKED_CV_LABEL,
    PREDICTION_INTERVAL_CALIBRATION_FRACTION,
    PREDICTION_INTERVAL_FIXED_CHRONO_LABEL,
    PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL,
    PREDICTION_INTERVAL_LEVEL,
    PREDICTION_INTERVAL_METHOD_LABEL,
    PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL,
    PREDICTION_INTERVAL_TARGET_BINS,
)


def _prediction_interval_level_display(level: float) -> str:
    return f'{int(round(float(level) * 100.0))}%'


def _prediction_interval_absolute_error_quantile(residuals: np.ndarray, level: float) -> float:
    residual_values = np.sort(np.asarray(residuals, dtype=float))
    residual_count = int(residual_values.size)
    if residual_count == 0:
        return 0.0
    rank = max(1, int(math.ceil((residual_count + 1) * float(level))))
    return float(residual_values[min(residual_count - 1, rank - 1)])


def _build_prediction_interval_bins(
    predictions: np.ndarray,
    residuals: np.ndarray,
    level: float,
    global_quantile: float,
) -> Dict[str, Any]:
    prediction_values = np.asarray(predictions, dtype=float)
    residual_values = np.asarray(residuals, dtype=float)
    residual_count = int(residual_values.size)
    target_bin_count = min(PREDICTION_INTERVAL_TARGET_BINS, max(1, residual_count // MIN_INTERVAL_BIN_RESIDUALS))
    if target_bin_count < 2:
        return {
            'strategy': 'global_absolute_error_quantile',
            'edges': [],
            'bins': [],
            'bin_count': 0,
        }

    raw_edges = np.quantile(
        prediction_values,
        [index / target_bin_count for index in range(1, target_bin_count)],
    )
    edge_values = []
    for raw_edge in np.atleast_1d(raw_edges).tolist():
        edge_value = float(raw_edge)
        if edge_values and math.isclose(edge_values[-1], edge_value, rel_tol=1e-9, abs_tol=1e-9):
            continue
        edge_values.append(edge_value)

    if not edge_values:
        return {
            'strategy': 'global_absolute_error_quantile',
            'edges': [],
            'bins': [],
            'bin_count': 0,
        }

    bin_assignments = np.searchsorted(np.asarray(edge_values, dtype=float), prediction_values, side='right')
    bins = []
    for bin_index in range(len(edge_values) + 1):
        mask = bin_assignments == bin_index
        bin_residuals = residual_values[mask]
        bin_predictions = prediction_values[mask]
        if bin_residuals.size >= MIN_INTERVAL_BIN_RESIDUALS:
            bin_quantile = _prediction_interval_absolute_error_quantile(bin_residuals, level)
            fallback_used = False
        else:
            bin_quantile = float(global_quantile)
            fallback_used = True

        lower_edge = None if bin_index == 0 else float(edge_values[bin_index - 1])
        upper_edge = None if bin_index == len(edge_values) else float(edge_values[bin_index])
        bins.append(
            {
                'bin_index': bin_index,
                'prediction_min': float(np.min(bin_predictions)) if bin_predictions.size else lower_edge,
                'prediction_max': float(np.max(bin_predictions)) if bin_predictions.size else upper_edge,
                'lower_edge': lower_edge,
                'upper_edge': upper_edge,
                'residual_count': int(bin_residuals.size),
                'absolute_error_quantile': float(bin_quantile),
                'fallback_to_global': fallback_used,
            }
        )

    return {
        'strategy': 'predicted_count_quantiles',
        'edges': edge_values,
        'bins': bins,
        'bin_count': len(bins),
    }


def _build_prediction_interval_calibration(
    actuals: np.ndarray,
    predictions: np.ndarray,
    level: float = PREDICTION_INTERVAL_LEVEL,
    method_label: str = PREDICTION_INTERVAL_METHOD_LABEL,
) -> Dict[str, Any]:
    actual_values = np.asarray(actuals, dtype=float)
    prediction_values = np.asarray(predictions, dtype=float)
    residuals = np.abs(actual_values - prediction_values)
    residual_count = int(residuals.size)
    quantile = _prediction_interval_absolute_error_quantile(residuals, level)
    adaptive_binning = _build_prediction_interval_bins(prediction_values, residuals, level, quantile)

    return {
        'level': float(level),
        'level_display': _prediction_interval_level_display(level),
        'absolute_error_quantile': quantile,
        'residual_count': residual_count,
        'adaptive_binning_strategy': adaptive_binning['strategy'],
        'adaptive_bin_count': adaptive_binning['bin_count'],
        'adaptive_bin_edges': adaptive_binning['edges'],
        'adaptive_bins': adaptive_binning['bins'],
        'method_label': method_label,
    }


def _copy_prediction_interval_calibration(
    calibration: Dict[str, Any],
    *,
    method_label: str,
) -> Dict[str, Any]:
    copied = dict(calibration)
    copied['method_label'] = method_label
    copied['adaptive_bin_edges'] = list(calibration.get('adaptive_bin_edges') or [])
    copied['adaptive_bins'] = [dict(row) for row in calibration.get('adaptive_bins') or []]
    return copied


class _PredictionIntervalCalibrationCache:
    def __init__(self, actuals: np.ndarray, predictions: np.ndarray, level: float) -> None:
        self._actuals = np.asarray(actuals, dtype=float)
        self._predictions = np.asarray(predictions, dtype=float)
        self._level = float(level)
        self._by_prefix: Dict[int, Dict[str, Any]] = {}

    def for_prefix(self, prefix_windows: int, *, method_label: str) -> Dict[str, Any]:
        prefix = max(0, min(int(prefix_windows), self._actuals.size, self._predictions.size))
        calibration = self._by_prefix.get(prefix)
        if calibration is None:
            calibration = _build_prediction_interval_calibration(
                self._actuals[:prefix],
                self._predictions[:prefix],
                level=self._level,
            )
            self._by_prefix[prefix] = calibration
        return _copy_prediction_interval_calibration(calibration, method_label=method_label)


def _split_prediction_interval_windows(total_windows: int) -> Optional[Tuple[int, int]]:
    minimum_windows = MIN_INTERVAL_CALIBRATION_WINDOWS + MIN_INTERVAL_EVALUATION_WINDOWS
    if total_windows < minimum_windows:
        return None

    calibration_windows = int(math.floor(total_windows * PREDICTION_INTERVAL_CALIBRATION_FRACTION))
    calibration_windows = max(MIN_INTERVAL_CALIBRATION_WINDOWS, calibration_windows)
    calibration_windows = min(calibration_windows, total_windows - MIN_INTERVAL_EVALUATION_WINDOWS)
    evaluation_windows = total_windows - calibration_windows
    if calibration_windows < MIN_INTERVAL_CALIBRATION_WINDOWS or evaluation_windows < MIN_INTERVAL_EVALUATION_WINDOWS:
        return None
    return calibration_windows, evaluation_windows


def _prediction_interval_validation_blocks(calibration_windows: int, total_windows: int) -> List[np.ndarray]:
    evaluation_indices = np.arange(calibration_windows, total_windows, dtype=int)
    evaluation_count = int(evaluation_indices.size)
    if evaluation_count <= 0:
        return []
    if evaluation_count >= 12:
        block_count = 4
    elif evaluation_count >= 6:
        block_count = 3
    else:
        block_count = 2
    return [block for block in np.array_split(evaluation_indices, block_count) if block.size]


def _prediction_interval_window_date_label(window_date: Any) -> str:
    if isinstance(window_date, pd.Timestamp):
        return window_date.date().isoformat()
    if isinstance(window_date, date):
        return window_date.isoformat()
    return str(window_date)


def _prediction_interval_range_labels(
    window_dates: List[Any],
    calibration_windows: int,
    evaluation_prefix: str = 'later',
) -> Tuple[str, str]:
    calibration_end = (
        _prediction_interval_window_date_label(window_dates[calibration_windows - 1])
        if window_dates and calibration_windows > 0
        else None
    )
    evaluation_count = max(0, len(window_dates) - calibration_windows)
    evaluation_start = (
        _prediction_interval_window_date_label(window_dates[calibration_windows])
        if evaluation_count > 0 and len(window_dates) > calibration_windows
        else None
    )
    calibration_label = f'first {calibration_windows} windows'
    if calibration_end:
        calibration_label = f'{calibration_label} through {calibration_end}'
    evaluation_label = f'{evaluation_prefix} {evaluation_count} windows'
    if evaluation_start:
        evaluation_label = f'{evaluation_label} from {evaluation_start}'
    return calibration_label, evaluation_label


def _prediction_interval_coverage_flags(
    actuals: np.ndarray,
    predictions: np.ndarray,
    calibration: Dict[str, Any],
) -> List[bool]:
    actual_values = np.asarray(actuals, dtype=float)
    prediction_values = np.asarray(predictions, dtype=float)
    covered: List[bool] = []
    for actual_value, prediction_value in zip(actual_values, prediction_values):
        lower_bound, upper_bound = _count_interval(float(prediction_value), calibration)
        covered.append(lower_bound <= float(actual_value) <= upper_bound)
    return covered


def _prediction_interval_stability_summary(
    covered_flags: List[bool],
    level: float,
) -> Dict[str, Any]:
    flag_values = np.asarray(covered_flags, dtype=float)
    if flag_values.size == 0:
        return {
            'coverage': None,
            'coverage_gap': float('inf'),
            'segment_coverages': [],
            'segment_coverage_std': float('inf'),
            'stability_score': float('inf'),
        }

    if flag_values.size >= 12:
        segment_count = 4
    elif flag_values.size >= 6:
        segment_count = 3
    else:
        segment_count = 2
    segments = [segment for segment in np.array_split(flag_values, segment_count) if segment.size]
    segment_coverages = [float(np.mean(segment)) for segment in segments]
    coverage = float(np.mean(flag_values))
    coverage_gap = abs(coverage - float(level))
    segment_coverage_std = float(np.std(segment_coverages)) if len(segment_coverages) > 1 else 0.0
    return {
        'coverage': coverage,
        'coverage_gap': coverage_gap,
        'segment_coverages': segment_coverages,
        'segment_coverage_std': segment_coverage_std,
        'stability_score': coverage_gap + segment_coverage_std,
    }


def _build_prediction_interval_candidate(
    scheme_key: str,
    scheme_label: str,
    level: float,
    calibration_windows: int,
    evaluation_windows: int,
    calibration_range_label: str,
    evaluation_range_label: str,
    covered_flags: List[bool],
    calibration_refresh_count: int,
    validation_block_count: int,
) -> Dict[str, Any]:
    summary = _prediction_interval_stability_summary(covered_flags, level)
    return {
        'scheme_key': scheme_key,
        'scheme_label': scheme_label,
        'coverage': summary['coverage'],
        'coverage_display': _format_ratio_percent(summary['coverage']),
        'coverage_gap': summary['coverage_gap'],
        'segment_coverages': summary['segment_coverages'],
        'segment_coverage_std': summary['segment_coverage_std'],
        'stability_score': summary['stability_score'],
        'calibration_window_count': calibration_windows,
        'evaluation_window_count': evaluation_windows,
        'calibration_window_range_label': calibration_range_label,
        'evaluation_window_range_label': evaluation_range_label,
        'calibration_refresh_count': calibration_refresh_count,
        'validation_block_count': validation_block_count,
        'covered_flags': list(covered_flags),
    }


def _evaluate_fixed_chrono_prediction_interval(
    actuals: np.ndarray,
    predictions: np.ndarray,
    window_dates: List[Any],
    calibration_windows: int,
    level: float,
    calibration_cache: Optional[_PredictionIntervalCalibrationCache] = None,
) -> Dict[str, Any]:
    calibration_range_label, evaluation_range_label = _prediction_interval_range_labels(
        window_dates,
        calibration_windows,
    )
    calibration_cache = calibration_cache or _PredictionIntervalCalibrationCache(actuals, predictions, level)
    calibration = calibration_cache.for_prefix(
        calibration_windows,
        method_label=f'{PREDICTION_INTERVAL_METHOD_LABEL}; validation baseline: {PREDICTION_INTERVAL_FIXED_CHRONO_LABEL}',
    )
    covered_flags = _prediction_interval_coverage_flags(
        actuals[calibration_windows:],
        predictions[calibration_windows:],
        calibration,
    )
    candidate = _build_prediction_interval_candidate(
        scheme_key='fixed_chrono_split',
        scheme_label=PREDICTION_INTERVAL_FIXED_CHRONO_LABEL,
        level=level,
        calibration_windows=calibration_windows,
        evaluation_windows=max(0, len(window_dates) - calibration_windows),
        calibration_range_label=calibration_range_label,
        evaluation_range_label=evaluation_range_label,
        covered_flags=covered_flags,
        calibration_refresh_count=1,
        validation_block_count=1 if covered_flags else 0,
    )
    candidate['calibration'] = calibration
    return candidate


def _evaluate_blocked_prediction_interval(
    actuals: np.ndarray,
    predictions: np.ndarray,
    window_dates: List[Any],
    calibration_windows: int,
    level: float,
    calibration_cache: Optional[_PredictionIntervalCalibrationCache] = None,
) -> Dict[str, Any]:
    calibration_range_label, evaluation_range_label = _prediction_interval_range_labels(
        window_dates,
        calibration_windows,
        evaluation_prefix='blocked evaluation',
    )
    covered_flags: List[bool] = []
    blocks = _prediction_interval_validation_blocks(calibration_windows, len(window_dates))
    calibration_cache = calibration_cache or _PredictionIntervalCalibrationCache(actuals, predictions, level)
    for block in blocks:
        block_start = int(block[0])
        calibration = calibration_cache.for_prefix(
            block_start,
            method_label=f'{PREDICTION_INTERVAL_METHOD_LABEL}; validation candidate: {PREDICTION_INTERVAL_BLOCKED_CV_LABEL}',
        )
        covered_flags.extend(
            _prediction_interval_coverage_flags(
                actuals[block],
                predictions[block],
                calibration,
            )
        )
    return _build_prediction_interval_candidate(
        scheme_key='blocked_forward_cv',
        scheme_label=PREDICTION_INTERVAL_BLOCKED_CV_LABEL,
        level=level,
        calibration_windows=calibration_windows,
        evaluation_windows=max(0, len(window_dates) - calibration_windows),
        calibration_range_label=calibration_range_label,
        evaluation_range_label=evaluation_range_label,
        covered_flags=covered_flags,
        calibration_refresh_count=len(blocks),
        validation_block_count=len(blocks),
    )


def _evaluate_rolling_prediction_interval(
    actuals: np.ndarray,
    predictions: np.ndarray,
    window_dates: List[Any],
    calibration_windows: int,
    level: float,
    calibration_cache: Optional[_PredictionIntervalCalibrationCache] = None,
) -> Dict[str, Any]:
    calibration_range_label, evaluation_range_label = _prediction_interval_range_labels(
        window_dates,
        calibration_windows,
        evaluation_prefix='rolling evaluation',
    )
    covered_flags: List[bool] = []
    calibration_cache = calibration_cache or _PredictionIntervalCalibrationCache(actuals, predictions, level)
    for index in range(calibration_windows, len(window_dates)):
        calibration = calibration_cache.for_prefix(
            index,
            method_label=f'{PREDICTION_INTERVAL_METHOD_LABEL}; validation candidate: {PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL}',
        )
        covered_flags.extend(
            _prediction_interval_coverage_flags(
                actuals[index:index + 1],
                predictions[index:index + 1],
                calibration,
            )
        )
    evaluation_windows = max(0, len(window_dates) - calibration_windows)
    return _build_prediction_interval_candidate(
        scheme_key='rolling_split_conformal',
        scheme_label=PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL,
        level=level,
        calibration_windows=calibration_windows,
        evaluation_windows=evaluation_windows,
        calibration_range_label=calibration_range_label,
        evaluation_range_label=evaluation_range_label,
        covered_flags=covered_flags,
        calibration_refresh_count=evaluation_windows,
        validation_block_count=evaluation_windows,
    )


def _prediction_interval_candidate_sort_key(candidate: Dict[str, Any]) -> Tuple[float, float, float, int]:
    preference_rank = 0 if candidate.get('scheme_key') == 'rolling_split_conformal' else 1
    return (
        float(candidate.get('stability_score', float('inf'))),
        float(candidate.get('segment_coverage_std', float('inf'))),
        float(candidate.get('coverage_gap', float('inf'))),
        preference_rank,
    )


def _prediction_interval_horizon_prefix(horizon_days: Optional[int]) -> str:
    if horizon_days is None:
        return ''
    day_label = '1-day lead' if int(horizon_days) == 1 else f'{int(horizon_days)}-day lead'
    return f'For the {day_label}, '


def _build_prediction_interval_validation_explanation(
    selected_candidate: Dict[str, Any],
    runner_up_candidate: Optional[Dict[str, Any]],
    reference_candidate: Optional[Dict[str, Any]],
    horizon_days: Optional[int] = None,
) -> str:
    selected_label = selected_candidate.get('scheme_label') or PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL
    runner_up_label = runner_up_candidate.get('scheme_label') if runner_up_candidate else None

    if runner_up_candidate is not None:
        score_gap = float(runner_up_candidate.get('stability_score', float('inf'))) - float(
            selected_candidate.get('stability_score', float('inf'))
        )
        if score_gap > 1e-9:
            comparison_text = f'it was more stable on later windows than {runner_up_label}'
        else:
            comparison_text = f'it stayed at least as stable as {runner_up_label} while refreshing calibration more often'
    else:
        comparison_text = 'it gave the most stable forward-only out-of-sample coverage among the available validation schemes'

    reference_text = ''
    if reference_candidate is not None:
        if float(selected_candidate.get('stability_score', float('inf'))) + 1e-9 < float(
            reference_candidate.get('stability_score', float('inf'))
        ):
            reference_text = ' and improved coverage stability versus the previous fixed 60/40 chrono split'
        else:
            reference_text = ' while remaining at least as stable as the previous fixed 60/40 chrono split'

    return (
        f'{_prediction_interval_horizon_prefix(horizon_days)}'
        f'{selected_label} was selected for validated out-of-sample coverage because {comparison_text}{reference_text}. '
        f'{PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL} was not adopted because an honest time-series variant would '
        'require leave-one-block-out refits for every checkpoint.'
    )


def _evaluate_prediction_interval_backtest(
    actuals: np.ndarray,
    predictions: np.ndarray,
    window_dates: List[Any],
    level: float = PREDICTION_INTERVAL_LEVEL,
    horizon_days: Optional[int] = None,
) -> Dict[str, Any]:
    actual_values = np.asarray(actuals, dtype=float)
    prediction_values = np.asarray(predictions, dtype=float)
    normalized_dates = list(window_dates)
    total_windows = min(actual_values.size, prediction_values.size, len(normalized_dates))
    actual_values = actual_values[:total_windows]
    prediction_values = prediction_values[:total_windows]
    normalized_dates = normalized_dates[:total_windows]

    split = _split_prediction_interval_windows(total_windows)
    if split is None:
        calibration = _build_prediction_interval_calibration(
            actual_values,
            prediction_values,
            level=level,
            method_label=f'{PREDICTION_INTERVAL_METHOD_LABEL} (validated out-of-sample coverage unavailable)',
        )
        note = (
            f'{_prediction_interval_horizon_prefix(horizon_days)}'
            'Validated out-of-sample coverage is unavailable because the backtest has too few rolling-origin windows '
            'for forward-only interval validation.'
        )
        calibration.update(
            coverage_validated=False,
            validated_coverage=None,
            coverage_note=note,
            calibration_window_count=total_windows,
            evaluation_window_count=0,
            calibration_window_range_label='all available backtest windows',
            evaluation_window_range_label='not available',
            validation_scheme_key='not_validated',
            validation_scheme_label='validated out-of-sample coverage unavailable',
            validation_scheme_explanation=note,
        )
        return {
            'calibration': calibration,
            'coverage': None,
            'coverage_validated': False,
            'coverage_note': note,
            'calibration_window_count': total_windows,
            'evaluation_window_count': 0,
            'calibration_window_range_label': 'all available backtest windows',
            'evaluation_window_range_label': 'not available',
            'validation_scheme_key': 'not_validated',
            'validation_scheme_label': 'validated out-of-sample coverage unavailable',
            'validation_scheme_explanation': note,
            'reference_candidate': None,
            'runner_up_candidate': None,
        }

    calibration_windows, _ = split
    calibration_cache = _PredictionIntervalCalibrationCache(actual_values, prediction_values, level)
    fixed_candidate = _evaluate_fixed_chrono_prediction_interval(
        actual_values,
        prediction_values,
        normalized_dates,
        calibration_windows,
        level,
        calibration_cache=calibration_cache,
    )
    blocked_candidate = _evaluate_blocked_prediction_interval(
        actual_values,
        prediction_values,
        normalized_dates,
        calibration_windows,
        level,
        calibration_cache=calibration_cache,
    )
    rolling_candidate = _evaluate_rolling_prediction_interval(
        actual_values,
        prediction_values,
        normalized_dates,
        calibration_windows,
        level,
        calibration_cache=calibration_cache,
    )
    selectable_candidates = [blocked_candidate, rolling_candidate]
    ranking = sorted(selectable_candidates, key=_prediction_interval_candidate_sort_key)
    selected_candidate = ranking[0]
    runner_up_candidate = ranking[1] if len(ranking) > 1 else None
    validation_explanation = _build_prediction_interval_validation_explanation(
        selected_candidate,
        runner_up_candidate,
        fixed_candidate,
        horizon_days=horizon_days,
    )

    calibration = calibration_cache.for_prefix(
        total_windows,
        method_label=(
            f'{PREDICTION_INTERVAL_METHOD_LABEL}; validated by {selected_candidate["scheme_label"]}'
        ),
    )
    note = (
        f'{validation_explanation} Coverage is evaluated only on '
        f'{selected_candidate["evaluation_window_range_label"]} after an initial calibration prefix of '
        f'{selected_candidate["calibration_window_range_label"]}. Deployment intervals are recalibrated on all '
        'available rolling-origin residuals after validation.'
    )
    calibration.update(
        coverage_validated=True,
        validated_coverage=selected_candidate['coverage'],
        coverage_note=note,
        calibration_window_count=selected_candidate['calibration_window_count'],
        evaluation_window_count=selected_candidate['evaluation_window_count'],
        calibration_window_range_label=selected_candidate['calibration_window_range_label'],
        evaluation_window_range_label=selected_candidate['evaluation_window_range_label'],
        validation_scheme_key=selected_candidate['scheme_key'],
        validation_scheme_label=selected_candidate['scheme_label'],
        validation_scheme_explanation=validation_explanation,
        reference_scheme_key=fixed_candidate['scheme_key'],
        reference_scheme_label=fixed_candidate['scheme_label'],
    )
    return {
        'calibration': calibration,
        'coverage': selected_candidate['coverage'],
        'coverage_validated': True,
        'coverage_note': note,
        'calibration_window_count': selected_candidate['calibration_window_count'],
        'evaluation_window_count': selected_candidate['evaluation_window_count'],
        'calibration_window_range_label': selected_candidate['calibration_window_range_label'],
        'evaluation_window_range_label': selected_candidate['evaluation_window_range_label'],
        'validation_scheme_key': selected_candidate['scheme_key'],
        'validation_scheme_label': selected_candidate['scheme_label'],
        'validation_scheme_explanation': validation_explanation,
        'reference_candidate': fixed_candidate,
        'runner_up_candidate': runner_up_candidate,
    }
