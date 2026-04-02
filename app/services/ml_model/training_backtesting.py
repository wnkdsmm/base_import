from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score

from app.perf import current_perf_trace
from app.services.forecasting.data import _build_forecast_rows as _build_scenario_forecast_rows
from app.services.model_quality import compute_classification_metrics, compute_count_metrics, relative_delta

from .constants import (
    CLASSIFICATION_THRESHOLD,
    COUNT_MODEL_KEYS,
    COUNT_MODEL_LABELS,
    COUNT_MODEL_SELECTION_TOLERANCE,
    COUNT_SELECTION_RULE,
    EVENT_BASELINE_METHOD_LABEL,
    EVENT_BASELINE_ROLE_LABEL,
    EVENT_CLASSIFIER_ROLE_LABEL,
    EVENT_HEURISTIC_METHOD_LABEL,
    EVENT_HEURISTIC_ROLE_LABEL,
    EVENT_MODEL_LABEL,
    EVENT_PROBABILITY_REASON_SATURATED_EVENT_RATE,
    EVENT_PROBABILITY_REASON_SINGLE_CLASS_EVALUATION,
    EVENT_PROBABILITY_REASON_TOO_FEW_COMPARABLE_WINDOWS,
    EVENT_RATE_SATURATION_MARGIN,
    EVENT_SELECTION_RULE,
    EXPLAINABLE_COUNT_MODEL_KEY,
    MAX_BACKTEST_POINTS,
    MIN_BACKTEST_POINTS,
    MIN_FEATURE_ROWS,
    MIN_POSITIVE_PREDICTION,
    ROLLING_MIN_TRAIN_ROWS,
)
from .runtime import MlProgressCallback, _emit_progress
from .training_dataset import _build_design_matrix, _prepare_reference_frame, _prepare_training_dataset
from .training_forecast import _bound_probability, _count_interval, _evaluate_prediction_interval_backtest, _format_ratio_percent
from .training_models import (
    _fit_count_model_from_design,
    _fit_event_model_from_design,
    _predict_count_from_design,
    _predict_event_probability_from_design,
)
from .training_temperature import _fit_temperature_statistics, _temperature_feature_columns


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


def _run_backtest(
    history_frame: pd.DataFrame,
    dataset: pd.DataFrame,
    progress_callback: MlProgressCallback = None,
) -> Dict[str, Any]:
    perf = current_perf_trace()
    history_frame = _prepare_reference_frame(history_frame)
    dataset = dataset.sort_values('date').reset_index(drop=True)
    history_dates = history_frame['date'].to_numpy(dtype='datetime64[ns]')
    min_train_rows = min(max(ROLLING_MIN_TRAIN_ROWS, MIN_FEATURE_ROWS), len(dataset) - MIN_BACKTEST_POINTS)
    available_backtest_points = len(dataset) - min_train_rows
    if perf is not None:
        perf.update(
            history_rows=len(history_frame),
            dataset_rows=len(dataset),
            min_train_rows=min_train_rows,
            available_backtest_points=available_backtest_points,
        )
    if available_backtest_points < MIN_BACKTEST_POINTS:
        return {
            'is_ready': False,
            'message': (
                f'Для rolling-origin backtesting нужно хотя бы {MIN_BACKTEST_POINTS} одношаговых проверок, '
                f'а сейчас после лагов доступно только {available_backtest_points}.'
            ),
        }

    start_index = max(min_train_rows, len(dataset) - min(MAX_BACKTEST_POINTS, available_backtest_points))
    total_windows = len(dataset) - start_index
    if perf is not None:
        perf.update(total_windows=total_windows)
    _emit_progress(
        progress_callback,
        'ml_backtest.running',
        f"Backtesting запущен: {total_windows} rolling-origin окон для проверки на истории.",
    )
    rows: List[Dict[str, Any]] = []

    for index in range(start_index, len(dataset)):
        test_date = pd.Timestamp(dataset['date'].iloc[index])
        train_cutoff = int(np.searchsorted(history_dates, test_date.to_datetime64(), side='left'))
        window_cutoff = int(np.searchsorted(history_dates, test_date.to_datetime64(), side='right'))
        reference_train = _prepare_reference_frame(history_frame.iloc[:train_cutoff])
        if reference_train.empty:
            continue

        _, window_dataset, window_temperature_stats = _prepare_training_dataset(
            history_frame.iloc[:window_cutoff],
            temperature_stats=_fit_temperature_statistics(reference_train),
        )
        model_train = window_dataset.loc[window_dataset['date'] < test_date].copy()
        test = window_dataset.loc[window_dataset['date'] == test_date].tail(1).copy()
        if model_train.empty or test.empty:
            continue

        window_feature_columns = _temperature_feature_columns(window_temperature_stats)
        model_train_design = _build_design_matrix(model_train, feature_columns=window_feature_columns)
        test_design = _build_design_matrix(test, model_train_design.columns, feature_columns=window_feature_columns)
        actual_count = float(test['count'].iloc[0])
        actual_event = int(test['event'].iloc[0])
        baseline_count = _baseline_expected_count(reference_train, test_date)
        baseline_event_probability = _baseline_event_probability(reference_train, test_date)
        heuristic_count, heuristic_event_probability = _scenario_reference_forecast(
            reference_train,
            test,
            temperature_stats=window_temperature_stats,
        )

        model_predictions: Dict[str, Optional[float]] = {key: None for key in COUNT_MODEL_KEYS}
        y_train_count = model_train['count'].to_numpy(dtype=float)
        for model_key in COUNT_MODEL_KEYS:
            model_bundle = _fit_count_model_from_design(model_key, model_train_design, y_train_count)
            if model_bundle is None:
                continue
            prediction = float(_predict_count_from_design(model_bundle, test_design)[0])
            model_predictions[model_key] = prediction

        event_prediction = None
        y_train_event = model_train['event'].to_numpy(dtype=int)
        event_bundle = _fit_event_model_from_design(model_train_design, y_train_event)
        if event_bundle is not None:
            event_prediction = float(_predict_event_probability_from_design(event_bundle, test_design)[0])

        rows.append(
            {
                'date': test_date.date().isoformat(),
                'actual_count': actual_count,
                'baseline_count': baseline_count,
                'heuristic_count': heuristic_count,
                'actual_event': actual_event,
                'baseline_event_probability': baseline_event_probability,
                'heuristic_event_probability': heuristic_event_probability,
                'predictions': model_predictions,
                'predicted_event_probability': event_prediction,
            }
        )
        completed_windows = index - start_index + 1
        if completed_windows == 1 or completed_windows == total_windows or completed_windows % 5 == 0:
            _emit_progress(
                progress_callback,
                'ml_backtest.running',
                f"Backtesting: обработано {completed_windows} из {total_windows} окон.",
            )

    valid_rows = [row for row in rows if any(row['predictions'].get(model_key) is not None for model_key in COUNT_MODEL_KEYS)]
    if perf is not None:
        perf.update(valid_windows=len(valid_rows))
    if len(valid_rows) < MIN_BACKTEST_POINTS:
        return {
            'is_ready': False,
            'message': 'Не удалось собрать достаточное количество валидных окон для проверки на истории.',
        }

    actuals = np.asarray([row['actual_count'] for row in valid_rows], dtype=float)
    baseline_predictions = np.asarray([row['baseline_count'] for row in valid_rows], dtype=float)
    heuristic_predictions = np.asarray([row['heuristic_count'] for row in valid_rows], dtype=float)
    baseline_metrics = compute_count_metrics(actuals, baseline_predictions)
    heuristic_metrics = compute_count_metrics(actuals, heuristic_predictions, baseline_metrics)

    count_metrics: Dict[str, Dict[str, Optional[float]]] = {}
    for model_key in COUNT_MODEL_KEYS:
        model_predictions = [row['predictions'].get(model_key) for row in valid_rows]
        if any(prediction is None for prediction in model_predictions):
            continue
        predictions = np.asarray([float(prediction) for prediction in model_predictions], dtype=float)
        count_metrics[model_key] = compute_count_metrics(actuals, predictions, baseline_metrics)

    if not count_metrics:
        return {
            'is_ready': False,
            'message': 'Не удалось обучить ни одной интерпретируемой модели по числу пожаров для проверки на истории.',
        }

    selected_count_model_key, selected_metrics, selection_context = _select_count_method(
        baseline_metrics,
        heuristic_metrics,
        count_metrics,
    )
    overdispersion_ratio = _estimate_overdispersion_ratio(dataset['count'].to_numpy(dtype=float))
    selection_details = _build_count_selection_details(
        selected_count_model_key=selected_count_model_key,
        selected_metrics=selected_metrics,
        count_metrics=count_metrics,
        baseline_metrics=baseline_metrics,
        heuristic_metrics=heuristic_metrics,
        overdispersion_ratio=overdispersion_ratio,
        raw_best_key=selection_context.get('raw_best_key'),
        tie_break_reason=selection_context.get('tie_break_reason'),
    )
    selected_predictions = np.asarray(
        [_selected_count_prediction(row, selected_count_model_key) for row in valid_rows],
        dtype=float,
    )
    prediction_interval_backtest = _evaluate_prediction_interval_backtest(
        actuals,
        selected_predictions,
        [row['date'] for row in valid_rows],
    )
    prediction_interval_calibration = prediction_interval_backtest['calibration']
    prediction_interval_coverage = prediction_interval_backtest['coverage']

    backtest_rows = []
    for row in valid_rows:
        predicted_count = float(_selected_count_prediction(row, selected_count_model_key))
        lower_bound, upper_bound = _count_interval(predicted_count, prediction_interval_calibration)
        backtest_rows.append(
            {
                'date': row['date'],
                'actual_count': row['actual_count'],
                'predicted_count': predicted_count,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound,
                'baseline_count': row['baseline_count'],
                'heuristic_count': row['heuristic_count'],
                'actual_event': row['actual_event'],
                'predicted_event_probability': row['predicted_event_probability'],
                'baseline_event_probability': row['baseline_event_probability'],
                'heuristic_event_probability': row['heuristic_event_probability'],
            }
        )

    event_metrics = _compute_event_metrics(backtest_rows)
    _emit_progress(
        progress_callback,
        'ml_backtest.completed',
        f"Backtesting завершён: {len(backtest_rows)} валидных окон, метрики качества готовы.",
    )

    if perf is not None:
        perf.update(
            completed_windows=len(rows),
            valid_windows=len(valid_rows),
            candidate_models=len(count_metrics),
            payload_rows=len(backtest_rows),
        )

    return {
        'is_ready': True,
        'message': '',
        'rows': backtest_rows,
        'window_rows': valid_rows,
        'baseline_metrics': baseline_metrics,
        'heuristic_metrics': heuristic_metrics,
        'count_metrics': count_metrics,
        'count_comparison_rows': _build_count_comparison_rows(
            baseline_metrics=baseline_metrics,
            heuristic_metrics=heuristic_metrics,
            count_metrics=count_metrics,
            selected_count_model_key=selected_count_model_key,
        ),
        'selected_count_model_key': selected_count_model_key,
        'selected_count_model_reason': selection_details['long'],
        'selected_count_model_reason_short': selection_details['short'],
        'selected_metrics': selected_metrics,
        'prediction_interval_calibration': prediction_interval_calibration,
        'event_metrics': event_metrics,
        'backtest_overview': {
            'folds': len(backtest_rows),
            'min_train_rows': min_train_rows,
            'validation_horizon_days': 1,
            'selection_rule': COUNT_SELECTION_RULE,
            'event_selection_rule': EVENT_SELECTION_RULE,
            'classification_threshold': CLASSIFICATION_THRESHOLD,
            'event_backtest_event_rate': event_metrics.get('event_rate'),
            'event_probability_informative': event_metrics.get('event_probability_informative', False),
            'event_probability_note': event_metrics.get('event_probability_note'),
            'event_probability_reason_code': event_metrics.get('event_probability_reason_code'),
            'candidate_model_labels': _available_count_model_labels(count_metrics),
            'dispersion_ratio': overdispersion_ratio,
            'prediction_interval_level': prediction_interval_calibration['level'],
            'prediction_interval_level_display': prediction_interval_calibration['level_display'],
            'prediction_interval_coverage': prediction_interval_coverage,
            'prediction_interval_coverage_display': _format_ratio_percent(prediction_interval_coverage),
            'prediction_interval_method_label': prediction_interval_calibration['method_label'],
            'prediction_interval_coverage_validated': prediction_interval_backtest['coverage_validated'],
            'prediction_interval_coverage_note': prediction_interval_backtest['coverage_note'],
            'prediction_interval_calibration_windows': prediction_interval_backtest['calibration_window_count'],
            'prediction_interval_evaluation_windows': prediction_interval_backtest['evaluation_window_count'],
            'prediction_interval_validation_scheme_key': prediction_interval_backtest.get('validation_scheme_key'),
            'prediction_interval_validation_scheme_label': prediction_interval_backtest.get('validation_scheme_label'),
            'prediction_interval_validation_explanation': prediction_interval_backtest.get('validation_scheme_explanation'),
            'prediction_interval_calibration_range_label': prediction_interval_backtest['calibration_window_range_label'],
            'prediction_interval_evaluation_range_label': prediction_interval_backtest['evaluation_window_range_label'],
            'rolling_scheme_label': f'Rolling-origin backtesting (expanding window): {len(backtest_rows)} одношаговых окон',
        },
    }


def _estimate_overdispersion_ratio(counts: np.ndarray) -> float:
    values = np.asarray(counts, dtype=float)
    if values.size == 0:
        return 1.0
    mean_value = max(float(np.mean(values)), MIN_POSITIVE_PREDICTION)
    variance = float(np.var(values, ddof=1)) if values.size > 1 else float(np.var(values))
    return max(variance / mean_value, 1.0)


def _available_count_model_labels(count_metrics: Dict[str, Dict[str, Optional[float]]]) -> List[str]:
    labels = [
        COUNT_MODEL_LABELS.get('seasonal_baseline', 'Сезонная базовая модель'),
        COUNT_MODEL_LABELS.get('heuristic_forecast', 'Сценарный эвристический прогноз'),
    ]
    labels.extend(COUNT_MODEL_LABELS.get(model_key, model_key) for model_key in COUNT_MODEL_KEYS if model_key in count_metrics)
    return labels


def _all_count_metrics(
    baseline_metrics: Dict[str, Optional[float]],
    heuristic_metrics: Dict[str, Optional[float]],
    count_metrics: Dict[str, Dict[str, Optional[float]]],
) -> Dict[str, Dict[str, Optional[float]]]:
    metrics: Dict[str, Dict[str, Optional[float]]] = {
        'seasonal_baseline': baseline_metrics,
        'heuristic_forecast': heuristic_metrics,
    }
    metrics.update({model_key: values for model_key, values in count_metrics.items() if values})
    return metrics


def _metrics_within_selection_tolerance(
    candidate_metrics: Dict[str, Optional[float]],
    reference_metrics: Dict[str, Optional[float]],
) -> bool:
    return _within_relative_margin(
        candidate_metrics.get('poisson_deviance'),
        reference_metrics.get('poisson_deviance'),
        COUNT_MODEL_SELECTION_TOLERANCE,
    ) and _within_relative_margin(
        candidate_metrics.get('mae'),
        reference_metrics.get('mae'),
        COUNT_MODEL_SELECTION_TOLERANCE,
    ) and _within_relative_margin(
        candidate_metrics.get('rmse'),
        reference_metrics.get('rmse'),
        COUNT_MODEL_SELECTION_TOLERANCE,
    )


def _select_count_method(
    baseline_metrics: Dict[str, Optional[float]],
    heuristic_metrics: Dict[str, Optional[float]],
    count_metrics: Dict[str, Dict[str, Optional[float]]],
) -> Tuple[str, Dict[str, Optional[float]], Dict[str, Any]]:
    all_metrics = _all_count_metrics(baseline_metrics, heuristic_metrics, count_metrics)
    ranking = sorted(all_metrics.items(), key=lambda item: _metric_sort_key(item[1]))
    raw_best_key, raw_best_metrics = ranking[0]

    selected_key = raw_best_key
    tie_break_reason = None
    if raw_best_key in COUNT_MODEL_KEYS and _metrics_within_selection_tolerance(heuristic_metrics, raw_best_metrics):
        selected_key = 'heuristic_forecast'
        tie_break_reason = 'heuristic_over_ml'
    elif raw_best_key in COUNT_MODEL_KEYS:
        selected_ml_key, _ = _select_count_model(count_metrics)
        if selected_ml_key != raw_best_key:
            selected_key = selected_ml_key
            tie_break_reason = 'poisson_over_ml'

    runner_up_key = raw_best_key if raw_best_key != selected_key else None
    if runner_up_key is None:
        runner_up_key = next((candidate_key for candidate_key, _ in ranking if candidate_key != selected_key), None)

    return selected_key, all_metrics[selected_key], {
        'raw_best_key': raw_best_key,
        'raw_best_metrics': raw_best_metrics,
        'runner_up_key': runner_up_key,
        'tie_break_reason': tie_break_reason,
        'all_metrics': all_metrics,
    }


def _build_count_selection_details(
    selected_count_model_key: str,
    selected_metrics: Dict[str, Optional[float]],
    count_metrics: Dict[str, Dict[str, Optional[float]]],
    baseline_metrics: Dict[str, Optional[float]],
    heuristic_metrics: Dict[str, Optional[float]],
    overdispersion_ratio: float,
    raw_best_key: Optional[str] = None,
    tie_break_reason: Optional[str] = None,
) -> Dict[str, str]:
    all_metrics = _all_count_metrics(baseline_metrics, heuristic_metrics, count_metrics)
    ranking = sorted(all_metrics.items(), key=lambda item: _metric_sort_key(item[1]))
    raw_best_key = raw_best_key or ranking[0][0]
    runner_up_key = raw_best_key if raw_best_key != selected_count_model_key else None
    if runner_up_key is None:
        runner_up_key = next((candidate_key for candidate_key, _ in ranking if candidate_key != selected_count_model_key), None)

    selected_label = COUNT_MODEL_LABELS.get(selected_count_model_key, selected_count_model_key)
    runner_up_label = COUNT_MODEL_LABELS.get(runner_up_key, runner_up_key) if runner_up_key else None
    baseline_delta = selected_metrics.get('mae_delta_vs_baseline')

    if selected_count_model_key == 'seasonal_baseline':
        short_reason = 'Выбран seasonal baseline: на rolling-origin окнах это был лучший рабочий метод среди всех кандидатов.'
        long_reason = (
            'Seasonal baseline стал рабочим count-методом по результатам rolling-origin backtesting, '
            'потому что по правилу отбора обошёл heuristic forecast и обучаемые count-model.'
        )
    elif selected_count_model_key == 'heuristic_forecast' and tie_break_reason == 'heuristic_over_ml' and raw_best_key in COUNT_MODEL_KEYS:
        short_reason = (
            'Выбран heuristic forecast: он почти не хуже лучшей count-model, '
            'а explainability tie-break сохраняет более объяснимый метод.'
        )
        long_reason = (
            f'Лучшей обучаемой count-model по метрикам была {COUNT_MODEL_LABELS.get(raw_best_key, raw_best_key)}, '
            f'но heuristic forecast уступил менее чем на {int(COUNT_MODEL_SELECTION_TOLERANCE * 100)}% '
            'по Poisson deviance, MAE и RMSE. По explainability tie-break рабочим методом оставлен '
            'heuristic forecast.'
        )
    elif selected_count_model_key == 'heuristic_forecast':
        short_reason = 'Выбран heuristic forecast: на rolling-origin окнах это был лучший рабочий метод среди всех кандидатов.'
        long_reason = (
            'Heuristic forecast стал рабочим count-методом по результатам rolling-origin backtesting, '
            'потому что обошёл seasonal baseline и обучаемые count-model по правилу отбора.'
        )
    elif selected_count_model_key == EXPLAINABLE_COUNT_MODEL_KEY and raw_best_key != selected_count_model_key:
        short_reason = 'Выбрана регрессия Пуассона: качество близко к лучшей count-model, а интерпретация проще.'
        long_reason = (
            'Регрессия Пуассона оставлена рабочим count-методом, потому что на rolling-origin backtesting '
            f'её Poisson deviance, MAE и RMSE отличаются от лидера {COUNT_MODEL_LABELS.get(raw_best_key, raw_best_key)} '
            f'менее чем на {int(COUNT_MODEL_SELECTION_TOLERANCE * 100)}%, но эта модель проще для интерпретации.'
        )
    elif selected_count_model_key in {'negative_binomial', 'tweedie'} and overdispersion_ratio > 1.15:
        short_reason = f'Выбрана {selected_label}: ряд пере-дисперсный, и эта модель лучше удерживает deviance.'
        long_reason = (
            f'{selected_label} выбрана как рабочий count-метод, потому что ряд показывает пере-дисперсию '
            f'(отношение variance/mean = {overdispersion_ratio:.2f}), а на rolling-origin backtesting именно '
            'эта модель дала наименьшую Poisson deviance при сохранении интерпретируемой GLM-структуры.'
        )
    else:
        short_reason = f'Выбрана {selected_label}: лучший баланс deviance, MAE и RMSE на rolling-origin.'
        long_reason = (
            f'{selected_label} выбрана по результатам rolling-origin backtesting, потому что показала лучший '
            'результат по Poisson deviance, MAE и RMSE среди всех count-кандидатов.'
        )

    if runner_up_label:
        long_reason += f' Ближайший альтернативный кандидат: {runner_up_label}.'
    if baseline_delta is not None:
        long_reason += f' Изменение MAE относительно seasonal baseline составило {baseline_delta * 100.0:+.1f}%.'
    heuristic_improvement = relative_delta(selected_metrics.get('mae'), heuristic_metrics.get('mae'))
    if heuristic_improvement is not None and selected_count_model_key != 'heuristic_forecast':
        long_reason += f' Изменение MAE относительно heuristic forecast составило {heuristic_improvement * 100.0:+.1f}%.'

    return {
        'short': short_reason,
        'long': long_reason,
    }


def _select_count_model(count_metrics: Dict[str, Dict[str, Optional[float]]]) -> Tuple[str, Dict[str, Optional[float]]]:
    ranking = sorted(count_metrics.items(), key=lambda item: _metric_sort_key(item[1]))
    best_key, best_metrics = ranking[0]
    for candidate_key in COUNT_MODEL_KEYS:
        candidate_metrics = count_metrics.get(candidate_key)
        if not candidate_metrics or candidate_key == best_key:
            continue
        if _within_relative_margin(candidate_metrics.get('poisson_deviance'), best_metrics.get('poisson_deviance'), COUNT_MODEL_SELECTION_TOLERANCE) and _within_relative_margin(
            candidate_metrics.get('mae'), best_metrics.get('mae'), COUNT_MODEL_SELECTION_TOLERANCE
        ) and _within_relative_margin(candidate_metrics.get('rmse'), best_metrics.get('rmse'), COUNT_MODEL_SELECTION_TOLERANCE):
            return candidate_key, candidate_metrics
    return best_key, best_metrics


def _metric_sort_key(metrics: Dict[str, Optional[float]]) -> Tuple[float, float, float, float]:
    return (
        metrics.get('poisson_deviance') if metrics.get('poisson_deviance') is not None else float('inf'),
        metrics.get('mae') if metrics.get('mae') is not None else float('inf'),
        metrics.get('rmse') if metrics.get('rmse') is not None else float('inf'),
        metrics.get('smape') if metrics.get('smape') is not None else float('inf'),
    )


def _within_relative_margin(candidate: Optional[float], reference: Optional[float], tolerance: float) -> bool:
    if candidate is None or reference is None:
        return False
    if reference <= MIN_POSITIVE_PREDICTION:
        return candidate <= reference + tolerance
    return candidate <= reference * (1.0 + tolerance)


def _build_count_comparison_rows(
    baseline_metrics: Dict[str, Optional[float]],
    heuristic_metrics: Dict[str, Optional[float]],
    count_metrics: Dict[str, Dict[str, Optional[float]]],
    selected_count_model_key: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = [
        {
            'method_key': 'seasonal_baseline',
            'method_label': COUNT_MODEL_LABELS.get('seasonal_baseline', 'Сезонная базовая модель'),
            'role_label': 'Базовая модель',
            'is_selected': selected_count_model_key == 'seasonal_baseline',
            **baseline_metrics,
        },
        {
            'method_key': 'heuristic_forecast',
            'method_label': COUNT_MODEL_LABELS.get('heuristic_forecast', 'Сценарный эвристический прогноз'),
            'role_label': 'Сценарный прогноз',
            'is_selected': selected_count_model_key == 'heuristic_forecast',
            **heuristic_metrics,
        },
    ]
    for model_key in COUNT_MODEL_KEYS:
        metrics = count_metrics.get(model_key)
        if not metrics:
            continue
        rows.append(
            {
                'method_key': model_key,
                'method_label': COUNT_MODEL_LABELS.get(model_key, model_key),
                'role_label': 'Интерпретируемая count-модель',
                'is_selected': model_key == selected_count_model_key,
                **metrics,
            }
        )
    return rows


def _selected_count_prediction(row: Dict[str, Any], selected_count_model_key: str) -> Optional[float]:
    if selected_count_model_key == 'seasonal_baseline':
        return row.get('baseline_count')
    if selected_count_model_key == 'heuristic_forecast':
        return row.get('heuristic_count')
    return row.get('predictions', {}).get(selected_count_model_key)


def _event_rate(actuals: np.ndarray) -> Optional[float]:
    if actuals.size == 0:
        return None
    return float(np.mean(actuals))


def _has_both_event_classes(actuals: np.ndarray) -> bool:
    return np.unique(np.asarray(actuals, dtype=int)).size > 1


def _event_rate_is_saturated(event_rate: Optional[float]) -> bool:
    if event_rate is None:
        return True
    return event_rate <= EVENT_RATE_SATURATION_MARGIN or event_rate >= (1.0 - EVENT_RATE_SATURATION_MARGIN)


def _event_probability_note(
    reason_code: Optional[str],
    *,
    rows_used: int,
    event_rate: Optional[float],
) -> Optional[str]:
    if reason_code == EVENT_PROBABILITY_REASON_TOO_FEW_COMPARABLE_WINDOWS:
        if rows_used > 0:
            return (
                'Вероятностный блок события пожара скрыт: в rolling-origin backtesting доступно только '
                f'{rows_used} сопоставимых окон, где можно корректно сравнить вероятности.'
            )
        return (
            'Вероятностный блок события пожара скрыт: в rolling-origin backtesting слишком мало сопоставимых окон, '
            'где можно корректно сравнить вероятности.'
        )
    if reason_code == EVENT_PROBABILITY_REASON_SINGLE_CLASS_EVALUATION:
        class_note = (
            'только дни с пожаром'
            if event_rate is not None and event_rate >= 0.5
            else 'только дни без пожара'
        )
        if rows_used > 0:
            return (
                'Вероятностный блок события пожара скрыт: '
                f'все {rows_used} evaluation-окон rolling-origin backtesting относятся к одному классу ({class_note}), '
                'поэтому вероятностная валидация некорректна.'
            )
        return (
            'Вероятностный блок события пожара скрыт: в evaluation-окнах rolling-origin backtesting наблюдался '
            f'только один класс ({class_note}), поэтому вероятностная валидация некорректна.'
        )
    if reason_code == EVENT_PROBABILITY_REASON_SATURATED_EVENT_RATE and event_rate is not None:
        return (
            'Вероятность P(>=1 пожара) скрыта: '
            f'доля события в evaluation-окнах rolling-origin backtesting составила {event_rate * 100.0:.1f}%, '
            'поэтому событие почти тривиально и неинформативно.'
        )
    return None


def _empty_event_metrics(
    *,
    rows_used: int,
    event_rate: Optional[float],
    evaluation_has_both_classes: bool,
    reason_code: Optional[str],
) -> Dict[str, Optional[float]]:
    return {
        'available': False,
        'logistic_available': False,
        'selected_model_key': None,
        'selected_model_label': None,
        'brier_score': None,
        'baseline_brier_score': None,
        'heuristic_brier_score': None,
        'roc_auc': None,
        'baseline_roc_auc': None,
        'heuristic_roc_auc': None,
        'f1': None,
        'baseline_f1': None,
        'heuristic_f1': None,
        'log_loss': None,
        'baseline_log_loss': None,
        'heuristic_log_loss': None,
        'comparison_rows': [],
        'rows_used': rows_used,
        'selection_rule': EVENT_SELECTION_RULE,
        'event_rate': event_rate,
        'evaluation_has_both_classes': evaluation_has_both_classes,
        'event_probability_informative': False,
        'event_probability_note': _event_probability_note(
            reason_code,
            rows_used=rows_used,
            event_rate=event_rate,
        ),
        'event_probability_reason_code': reason_code,
    }


def _normalized_event_model_label(selected_model_key: Optional[str], fallback_label: Optional[str]) -> Optional[str]:
    if selected_model_key == 'event_baseline':
        return EVENT_BASELINE_METHOD_LABEL
    if selected_model_key == 'heuristic_probability':
        return EVENT_HEURISTIC_METHOD_LABEL
    if selected_model_key == 'logistic_regression':
        return EVENT_MODEL_LABEL
    return fallback_label


def _normalize_event_comparison_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized_rows: List[Dict[str, Any]] = []
    for row in rows:
        normalized_row = dict(row)
        method_key = str(normalized_row.get('method_key') or '')
        if method_key == 'event_baseline':
            normalized_row['method_label'] = EVENT_BASELINE_METHOD_LABEL
            normalized_row['role_label'] = EVENT_BASELINE_ROLE_LABEL
        elif method_key == 'heuristic_probability':
            normalized_row['method_label'] = EVENT_HEURISTIC_METHOD_LABEL
            normalized_row['role_label'] = EVENT_HEURISTIC_ROLE_LABEL
        elif method_key == 'logistic_regression':
            normalized_row['method_label'] = EVENT_MODEL_LABEL
            normalized_row['role_label'] = EVENT_CLASSIFIER_ROLE_LABEL
        normalized_rows.append(normalized_row)
    return normalized_rows


def _compute_event_metrics(rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    common_rows = [
        row
        for row in rows
        if row['baseline_event_probability'] is not None and row['heuristic_event_probability'] is not None
    ]
    if len(common_rows) < MIN_BACKTEST_POINTS:
        return _empty_event_metrics(
            rows_used=len(common_rows),
            event_rate=None,
            evaluation_has_both_classes=False,
            reason_code=EVENT_PROBABILITY_REASON_TOO_FEW_COMPARABLE_WINDOWS,
        )

    classifier_rows = [row for row in common_rows if row['predicted_event_probability'] is not None]
    logistic_available = len(classifier_rows) >= MIN_BACKTEST_POINTS
    evaluation_rows = classifier_rows if logistic_available else common_rows

    actuals = np.asarray([int(row['actual_event']) for row in evaluation_rows], dtype=int)
    event_rate = _event_rate(actuals)
    evaluation_has_both_classes = _has_both_event_classes(actuals)
    if not evaluation_has_both_classes:
        return _empty_event_metrics(
            rows_used=len(evaluation_rows),
            event_rate=event_rate,
            evaluation_has_both_classes=False,
            reason_code=EVENT_PROBABILITY_REASON_SINGLE_CLASS_EVALUATION,
        )

    event_probability_informative = not _event_rate_is_saturated(event_rate)
    event_probability_reason_code = (
        EVENT_PROBABILITY_REASON_SATURATED_EVENT_RATE if not event_probability_informative else None
    )
    event_probability_note = _event_probability_note(
        event_probability_reason_code,
        rows_used=len(evaluation_rows),
        event_rate=event_rate,
    )

    baseline_probabilities = np.asarray([float(row['baseline_event_probability']) for row in evaluation_rows], dtype=float)
    heuristic_probabilities = np.asarray([float(row['heuristic_event_probability']) for row in evaluation_rows], dtype=float)

    heuristic_metrics = compute_classification_metrics(
        actuals,
        heuristic_probabilities,
        baseline_probabilities,
        threshold=CLASSIFICATION_THRESHOLD,
    )
    baseline_roc_auc = _safe_roc_auc(actuals, baseline_probabilities)
    heuristic_roc_auc = _safe_roc_auc(actuals, heuristic_probabilities)
    baseline_log_loss = _safe_log_loss(actuals, baseline_probabilities)
    heuristic_log_loss = _safe_log_loss(actuals, heuristic_probabilities)

    comparison_rows = [
        {
            'method_key': 'event_baseline',
            'method_label': EVENT_BASELINE_METHOD_LABEL,
            'role_label': EVENT_BASELINE_ROLE_LABEL,
            'brier_score': heuristic_metrics.get('baseline_brier_score'),
            'roc_auc': baseline_roc_auc,
            'f1': heuristic_metrics.get('baseline_f1'),
            'log_loss': baseline_log_loss,
            'is_selected': False,
        },
        {
            'method_key': 'heuristic_probability',
            'method_label': EVENT_HEURISTIC_METHOD_LABEL,
            'role_label': EVENT_HEURISTIC_ROLE_LABEL,
            'brier_score': heuristic_metrics.get('brier_score'),
            'roc_auc': heuristic_roc_auc,
            'f1': heuristic_metrics.get('f1'),
            'log_loss': heuristic_log_loss,
            'is_selected': True,
        },
    ]

    selected_model_key = 'heuristic_probability'
    selected_model_label = EVENT_HEURISTIC_METHOD_LABEL
    selected_metrics = heuristic_metrics
    selected_roc_auc = heuristic_roc_auc
    selected_log_loss = heuristic_log_loss

    if logistic_available:
        classifier_probabilities = np.asarray([float(row['predicted_event_probability']) for row in evaluation_rows], dtype=float)
        classifier_metrics = compute_classification_metrics(
            actuals,
            classifier_probabilities,
            baseline_probabilities,
            threshold=CLASSIFICATION_THRESHOLD,
        )
        classifier_roc_auc = _safe_roc_auc(actuals, classifier_probabilities)
        classifier_log_loss = _safe_log_loss(actuals, classifier_probabilities)
        classifier_selected = (
            event_probability_informative
            and bool(classifier_metrics.get('available'))
            and _event_metric_sort_key(
                classifier_metrics.get('brier_score'),
                classifier_log_loss,
                classifier_roc_auc,
                classifier_metrics.get('f1'),
            ) < _event_metric_sort_key(
                heuristic_metrics.get('brier_score'),
                heuristic_log_loss,
                heuristic_roc_auc,
                heuristic_metrics.get('f1'),
            )
        )
        comparison_rows.append(
            {
                'method_key': 'logistic_regression',
                'method_label': EVENT_MODEL_LABEL,
                'role_label': EVENT_CLASSIFIER_ROLE_LABEL,
                'brier_score': classifier_metrics.get('brier_score'),
                'roc_auc': classifier_roc_auc,
                'f1': classifier_metrics.get('f1'),
                'log_loss': classifier_log_loss,
                'is_selected': classifier_selected,
            }
        )
        if classifier_selected:
            comparison_rows[1]['is_selected'] = False
            selected_model_key = 'logistic_regression'
            selected_model_label = EVENT_MODEL_LABEL
            selected_metrics = classifier_metrics
            selected_roc_auc = classifier_roc_auc
            selected_log_loss = classifier_log_loss

    return {
        'available': True,
        'logistic_available': logistic_available,
        'selected_model_key': selected_model_key,
        'selected_model_label': _normalized_event_model_label(selected_model_key, selected_model_label),
        'brier_score': selected_metrics.get('brier_score'),
        'baseline_brier_score': heuristic_metrics.get('baseline_brier_score'),
        'heuristic_brier_score': heuristic_metrics.get('brier_score'),
        'roc_auc': selected_roc_auc,
        'baseline_roc_auc': baseline_roc_auc,
        'heuristic_roc_auc': heuristic_roc_auc,
        'f1': selected_metrics.get('f1'),
        'baseline_f1': heuristic_metrics.get('baseline_f1'),
        'heuristic_f1': heuristic_metrics.get('f1'),
        'log_loss': selected_log_loss,
        'baseline_log_loss': baseline_log_loss,
        'heuristic_log_loss': heuristic_log_loss,
        'comparison_rows': _normalize_event_comparison_rows(comparison_rows),
        'rows_used': len(evaluation_rows),
        'selection_rule': EVENT_SELECTION_RULE,
        'event_rate': event_rate,
        'evaluation_has_both_classes': evaluation_has_both_classes,
        'event_probability_informative': event_probability_informative,
        'event_probability_note': event_probability_note,
        'event_probability_reason_code': event_probability_reason_code,
    }


def _event_metric_sort_key(
    brier_score: Optional[float],
    log_loss_value: Optional[float],
    roc_auc: Optional[float],
    f1_score: Optional[float],
) -> Tuple[float, float, float, float]:
    return (
        brier_score if brier_score is not None else float('inf'),
        log_loss_value if log_loss_value is not None else float('inf'),
        -(roc_auc if roc_auc is not None else -1.0),
        -(f1_score if f1_score is not None else -1.0),
    )


def _safe_roc_auc(actuals: np.ndarray, probabilities: np.ndarray) -> Optional[float]:
    if len(np.unique(actuals)) <= 1:
        return None
    return float(roc_auc_score(actuals, probabilities))


def _safe_log_loss(actuals: np.ndarray, probabilities: np.ndarray) -> Optional[float]:
    if actuals.size == 0:
        return None
    clipped = np.clip(np.asarray(probabilities, dtype=float), 0.001, 0.999)
    return float(log_loss(actuals, clipped, labels=[0, 1]))
