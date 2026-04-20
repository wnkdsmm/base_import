from __future__ import annotations

from datetime import date
import math
from typing import Any

import numpy as np
import pandas as pd

from .calibration_compute import (
    _PredictionIntervalCalibrationCache,
    _build_prediction_interval_calibration,
    _copy_prediction_interval_calibration,
    _evaluate_blocked_prediction_interval,
    _evaluate_fixed_chrono_prediction_interval,
    _evaluate_rolling_prediction_interval,
    _prediction_interval_candidate_sort_key,
    _split_prediction_interval_windows,
)
from .forecast_bounds import _count_interval, _format_ratio_percent
from ..ml_model_interval_types import MIN_INTERVAL_BIN_RESIDUALS, MIN_INTERVAL_CALIBRATION_WINDOWS, MIN_INTERVAL_EVALUATION_WINDOWS, PredictionIntervalCalibration, PREDICTION_INTERVAL_BLOCKED_CV_LABEL, PREDICTION_INTERVAL_CALIBRATION_FRACTION, PREDICTION_INTERVAL_FIXED_CHRONO_LABEL, PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL, PREDICTION_INTERVAL_LEVEL, PREDICTION_INTERVAL_METHOD_LABEL, PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL, PREDICTION_INTERVAL_TARGET_BINS
from .types import (
    PredictionIntervalBacktestEvaluation,
    PredictionIntervalBinsResult,
    PredictionIntervalCandidate,
    PredictionIntervalStabilitySummary,
)


def _prediction_interval_horizon_prefix(horizon_days: int | None) -> str:
    if horizon_days is None:
        return ''
    day_label = '1-day lead' if int(horizon_days) == 1 else f'{int(horizon_days)}-day lead'
    return f'For the {day_label}, '


def _build_prediction_interval_validation_explanation(
    selected_candidate: PredictionIntervalCandidate,
    runner_up_candidate: PredictionIntervalCandidate | None,
    reference_candidate: PredictionIntervalCandidate | None,
    horizon_days: int | None = None,
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
    window_dates: list[Any],
    level: float = PREDICTION_INTERVAL_LEVEL,
    horizon_days: int | None = None,
) -> PredictionIntervalBacktestEvaluation:
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

__all__ = [
    '_prediction_interval_horizon_prefix',
    '_build_prediction_interval_validation_explanation',
    '_evaluate_prediction_interval_backtest',
]
