from __future__ import annotations

import math
import warnings
from collections import defaultdict
from contextlib import nullcontext
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
try:
    from joblib import parallel_backend
except Exception:  # pragma: no cover - optional dependency
    parallel_backend = None

from sklearn.metrics import log_loss, roc_auc_score

try:
    from sklearn.inspection import permutation_importance
except Exception:  # pragma: no cover - graceful fallback for older sklearn
    permutation_importance = None

try:
    from sklearn.linear_model import LogisticRegression, PoissonRegressor, TweedieRegressor
except Exception:  # pragma: no cover - graceful fallback for older sklearn
    from sklearn.linear_model import LogisticRegression

    PoissonRegressor = None
    TweedieRegressor = None

try:
    from sklearn.compose import ColumnTransformer
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
except Exception:  # pragma: no cover - graceful fallback for older sklearn
    ColumnTransformer = None
    ConvergenceWarning = None
    Pipeline = None
    StandardScaler = None

try:
    import statsmodels.api as sm
    from statsmodels.tools.sm_exceptions import (
        ConvergenceWarning as StatsmodelsConvergenceWarning,
        HessianInversionWarning,
        PerfectSeparationWarning,
    )
except Exception:  # pragma: no cover - optional dependency
    sm = None
    StatsmodelsConvergenceWarning = None
    HessianInversionWarning = None
    PerfectSeparationWarning = None

from app.perf import current_perf_trace, profiled
from app.services.forecasting.data import (
    _build_forecast_rows as _build_scenario_forecast_rows,
    _temperature_quality_from_daily_history,
)
from app.services.forecasting.utils import _format_number, _format_percent
from app.services.model_quality import compute_classification_metrics, compute_count_metrics, relative_delta

from .constants import (
    COUNT_MODEL_LABELS,
    COUNT_MODEL_SELECTION_TOLERANCE,
    EVENT_MODEL_LABEL,
    EVENT_RATE_SATURATION_MARGIN,
    FEATURE_LABELS,
    MAX_BACKTEST_POINTS,
    MAX_HISTORY_POINTS,
    MIN_BACKTEST_POINTS,
    MIN_DAILY_HISTORY,
    MIN_EVENT_CLASS_COUNT,
    MIN_FEATURE_ROWS,
    MIN_TEMPERATURE_COVERAGE,
    MIN_TEMPERATURE_NON_NULL_DAYS,
    PERMUTATION_REPEATS,
    ROLLING_MIN_TRAIN_ROWS,
    _LOGISTIC_PARAMS,
    _POISSON_PARAMS,
    _TWEEDIE_PARAMS,
)

FEATURE_COLUMNS = ['temp_value', 'weekday', 'month', 'lag_1', 'lag_7', 'lag_14', 'rolling_7', 'rolling_28', 'trend_gap']
NON_TEMPERATURE_FEATURE_COLUMNS = [column for column in FEATURE_COLUMNS if column != 'temp_value']
COUNT_MODEL_CONTINUOUS_COLUMNS = ['temp_value', 'lag_1', 'lag_7', 'lag_14', 'rolling_7', 'rolling_28', 'trend_gap']
COUNT_MODEL_KEYS = ['poisson', 'negative_binomial', 'tweedie']
EXPLAINABLE_COUNT_MODEL_KEY = 'poisson'
MIN_POSITIVE_PREDICTION = 1e-6
CLASSIFICATION_THRESHOLD = 0.5
PREDICTION_INTERVAL_LEVEL = 0.8
PREDICTION_INTERVAL_CALIBRATION_FRACTION = 0.6
MIN_INTERVAL_CALIBRATION_WINDOWS = 6
MIN_INTERVAL_EVALUATION_WINDOWS = 4
PREDICTION_INTERVAL_TARGET_BINS = 3
MIN_INTERVAL_BIN_RESIDUALS = 2
PREDICTION_INTERVAL_METHOD_LABEL = 'Adaptive conformal interval with predicted-count bins'
PREDICTION_INTERVAL_FIXED_CHRONO_LABEL = 'Fixed 60/40 chrono split conformal'
PREDICTION_INTERVAL_BLOCKED_CV_LABEL = 'Blocked forward CV conformal'
PREDICTION_INTERVAL_ROLLING_SPLIT_LABEL = 'Forward rolling split conformal'
PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL = 'Jackknife+ for time series'
EVENT_SELECTION_RULE = (
    'Минимум Brier score, затем log-loss и ROC-AUC; при близком качестве '
    'сохраняется более простой интерпретируемый метод.'
)

COUNT_SELECTION_RULE = (
    'Минимум Poisson deviance, затем MAE и RMSE среди seasonal baseline, heuristic forecast и count-model; '
    'если heuristic forecast почти не хуже лучшей count-model, сохраняется более объяснимый рабочий метод; внутри ML-паритета предпочитается Poisson.'
)

EVENT_BASELINE_METHOD_LABEL = 'Сезонная событийная базовая модель'
EVENT_BASELINE_ROLE_LABEL = 'Базовая модель'
EVENT_HEURISTIC_METHOD_LABEL = 'Сценарная эвристическая вероятность'
EVENT_HEURISTIC_ROLE_LABEL = 'Сценарный прогноз'
EVENT_CLASSIFIER_ROLE_LABEL = 'Классификатор'
EVENT_PROBABILITY_REASON_TOO_FEW_COMPARABLE_WINDOWS = 'too_few_comparable_windows'
EVENT_PROBABILITY_REASON_SINGLE_CLASS_EVALUATION = 'single_class_evaluation'
EVENT_PROBABILITY_REASON_SATURATED_EVENT_RATE = 'saturated_event_rate'
WARNING_INSTABILITY_MESSAGE_TOKENS = (
    'perfect separation',
    'perfect prediction',
    'parameter may not be identified',
    'parameters are not identified',
    'failed to converge',
    'did not converge',
    'singular',
    'hessian',
)

MlProgressCallback = Optional[Callable[[str, str], None]]


def _emit_progress(progress_callback: MlProgressCallback, phase: str, message: str) -> None:
    if progress_callback is None:
        return
    try:
        progress_callback(phase, message)
    except Exception:
        return


def _apply_history_window(records: List[Dict[str, Any]], history_window: str) -> List[Dict[str, Any]]:
    if not records or history_window == 'all':
        return records
    latest_year = max(item['date'].year for item in records)
    min_year = latest_year - 2 if history_window == 'recent_3' else latest_year - 4
    return [item for item in records if item['date'].year >= min_year]



def _train_ml_model(
    daily_history: List[Dict[str, Any]],
    forecast_days: int,
    scenario_temperature: Optional[float],
    progress_callback: MlProgressCallback = None,
) -> Dict[str, Any]:
    perf = current_perf_trace()
    if len(daily_history) < MIN_DAILY_HISTORY or forecast_days <= 0:
        return _empty_ml_result(
            f'Для ML-блока нужно минимум {MIN_DAILY_HISTORY} дней непрерывной дневной истории, чтобы выполнить rolling-origin backtesting и обучить модель.'
        )

    _emit_progress(progress_callback, 'ml_model.running', 'Подготавливаем признаки и обучающую выборку для ML-модели.')
    feature_prep_context = perf.span('feature_prep') if perf is not None else nullcontext()
    with feature_prep_context:
        history_tail = daily_history[-MAX_HISTORY_POINTS:]
        frame = _build_history_frame(history_tail)
        dataset = _build_backtest_seed_dataset(frame)
        if perf is not None:
            perf.update(history_points=len(history_tail), feature_rows=len(dataset))
    if len(dataset) < MIN_FEATURE_ROWS:
        return _empty_ml_result(
            f'После формирования лагов и скользящих признаков осталось только {len(dataset)} наблюдений: этого мало для корректного rolling-origin backtesting.'
        )

    _emit_progress(progress_callback, 'ml_backtest.pending', 'Готовим rolling-origin backtesting на выбранной истории.')
    backtest = _run_backtest(frame.copy(), dataset, progress_callback=progress_callback)
    if not backtest['is_ready']:
        _emit_progress(progress_callback, 'ml_backtest.failed', backtest['message'])
        return _empty_ml_result(backtest['message'])

    final_frame, final_dataset, final_temperature_stats = _prepare_training_dataset(frame.copy())
    if len(final_dataset) < MIN_FEATURE_ROWS:
        return _empty_ml_result(
            f'После подготовки полной обучающей выборки осталось только {len(final_dataset)} наблюдений: этого мало для итогового обучения ML-модели.'
        )

    selected_count_model_key = backtest['selected_count_model_key']
    feature_columns = _temperature_feature_columns(final_temperature_stats)
    _emit_progress(
        progress_callback,
        'ml_model.running',
        f"Обучаем итоговую count-модель {COUNT_MODEL_LABELS.get(selected_count_model_key, selected_count_model_key)} на полной истории.",
    )
    final_count_model = (
        _fit_count_model(selected_count_model_key, final_dataset, feature_columns=feature_columns)
        if selected_count_model_key in COUNT_MODEL_KEYS
        else None
    )
    if selected_count_model_key in COUNT_MODEL_KEYS and final_count_model is None:
        return _empty_ml_result('Не удалось обучить итоговую модель по числу пожаров на полной выборке.')

    selected_event_model_key = backtest['event_metrics'].get('selected_model_key')
    classifier_validated = (
        selected_event_model_key == 'logistic_regression'
        and backtest['event_metrics'].get('available', False)
        and backtest['event_metrics'].get('logistic_available', False)
        and backtest['event_metrics'].get('event_probability_informative', False)
        and _can_train_event_model(final_dataset['event'])
    )
    final_event_model = _fit_event_model(final_dataset, feature_columns=feature_columns) if classifier_validated else None

    _emit_progress(progress_callback, 'ml_model.running', 'Строим прогноз по будущим датам и интервалы неопределённости.')
    forecast_rows = _build_future_forecast_rows(
        frame=final_frame,
        selected_count_model_key=selected_count_model_key,
        count_model=final_count_model,
        event_model=final_event_model,
        forecast_days=forecast_days,
        scenario_temperature=scenario_temperature,
        interval_calibration=backtest['prediction_interval_calibration'],
        temperature_stats=final_temperature_stats,
    )

    _emit_progress(progress_callback, 'ml_model.running', 'Оцениваем важность признаков и собираем итоговый ML-отчёт.')
    result_render_context = perf.span('result_render') if perf is not None else nullcontext()
    with result_render_context:
        feature_importance = _build_feature_importance(final_count_model, final_dataset) if final_count_model is not None else []
        backtest_rows = [
            {
                'date': row['date'],
                'actual_count': round(float(row['actual_count']), 3),
                'predicted_count': round(float(row['predicted_count']), 3),
                'lower_bound': round(float(row['lower_bound']), 3),
                'upper_bound': round(float(row['upper_bound']), 3),
                'baseline_count': round(float(row['baseline_count']), 3),
                'heuristic_count': round(float(row['heuristic_count']), 3),
                'actual_event': int(row['actual_event']),
                'predicted_event_probability': round(float(row['predicted_event_probability']), 4)
                if row['predicted_event_probability'] is not None
                else None,
                'baseline_event_probability': round(float(row['baseline_event_probability']), 4)
                if row['baseline_event_probability'] is not None
                else None,
                'heuristic_event_probability': round(float(row['heuristic_event_probability']), 4)
                if row['heuristic_event_probability'] is not None
                else None,
            }
            for row in backtest['rows']
        ]

        overview = backtest['backtest_overview']
        if perf is not None:
            perf.update(
                forecast_rows=len(forecast_rows),
                feature_importance_rows=len(feature_importance),
                backtest_rows=len(backtest_rows),
            )
        return {
            'is_ready': True,
            'forecast_rows': forecast_rows,
            'feature_importance': feature_importance,
            'backtest_rows': backtest_rows,
            'count_mae': backtest['selected_metrics']['mae'],
            'count_rmse': backtest['selected_metrics']['rmse'],
            'count_smape': backtest['selected_metrics']['smape'],
            'count_poisson_deviance': backtest['selected_metrics']['poisson_deviance'],
            'baseline_count_mae': backtest['baseline_metrics']['mae'],
            'baseline_count_rmse': backtest['baseline_metrics']['rmse'],
            'baseline_count_smape': backtest['baseline_metrics']['smape'],
            'baseline_count_poisson_deviance': backtest['baseline_metrics']['poisson_deviance'],
            'heuristic_count_mae': backtest['heuristic_metrics']['mae'],
            'heuristic_count_rmse': backtest['heuristic_metrics']['rmse'],
            'heuristic_count_smape': backtest['heuristic_metrics']['smape'],
            'heuristic_count_poisson_deviance': backtest['heuristic_metrics']['poisson_deviance'],
            'count_vs_baseline_delta': backtest['selected_metrics']['mae_delta_vs_baseline'],
            'brier_score': backtest['event_metrics']['brier_score'],
            'baseline_brier_score': backtest['event_metrics']['baseline_brier_score'],
            'heuristic_brier_score': backtest['event_metrics'].get('heuristic_brier_score'),
            'roc_auc': backtest['event_metrics']['roc_auc'],
            'baseline_roc_auc': backtest['event_metrics'].get('baseline_roc_auc'),
            'heuristic_roc_auc': backtest['event_metrics'].get('heuristic_roc_auc'),
            'f1_score': backtest['event_metrics']['f1'],
            'baseline_f1_score': backtest['event_metrics']['baseline_f1'],
            'heuristic_f1_score': backtest['event_metrics'].get('heuristic_f1'),
            'log_loss': backtest['event_metrics']['log_loss'],
            'baseline_log_loss': backtest['event_metrics'].get('baseline_log_loss'),
            'heuristic_log_loss': backtest['event_metrics'].get('heuristic_log_loss'),
            'count_comparison_rows': backtest['count_comparison_rows'],
            'event_comparison_rows': backtest['event_metrics'].get('comparison_rows', []),
            'backtest_overview': overview,
            'selected_count_model_key': selected_count_model_key,
            'selected_count_model_reason': backtest.get('selected_count_model_reason'),
            'selected_count_model_reason_short': backtest.get('selected_count_model_reason_short'),
            'candidate_count_model_labels': overview.get('candidate_model_labels', []),
            'selected_event_model_key': backtest['event_metrics'].get('selected_model_key'),
            'selected_event_model_label': backtest['event_metrics'].get('selected_model_label'),
            'top_feature_label': feature_importance[0]['label'] if feature_importance else '-',
            'count_model_label': COUNT_MODEL_LABELS.get(selected_count_model_key, selected_count_model_key),
            'prediction_interval_level': overview.get('prediction_interval_level'),
            'prediction_interval_level_display': overview.get('prediction_interval_level_display'),
            'prediction_interval_coverage': overview.get('prediction_interval_coverage'),
            'prediction_interval_coverage_display': overview.get('prediction_interval_coverage_display'),
            'prediction_interval_method_label': overview.get('prediction_interval_method_label'),
            'event_model_label': EVENT_MODEL_LABEL if final_event_model is not None else None,
            'event_backtest_available': backtest['event_metrics'].get('available', False),
            'event_probability_enabled': final_event_model is not None,
            'event_probability_note': backtest['event_metrics'].get('event_probability_note'),
            'event_probability_reason_code': backtest['event_metrics'].get('event_probability_reason_code'),
            'temperature_feature_enabled': bool(final_temperature_stats.get('usable')),
            'temperature_non_null_days': int(final_temperature_stats.get('non_null_days', 0) or 0),
            'temperature_total_days': int(final_temperature_stats.get('total_days', 0) or 0),
            'temperature_coverage': float(final_temperature_stats.get('coverage', 0.0) or 0.0),
            'temperature_note': final_temperature_stats.get('note'),
            'backtest_method_label': overview.get('rolling_scheme_label')
            or f'Rolling-origin backtesting: {len(backtest_rows)} одношаговых окон',
            'classifier_ready': final_event_model is not None,
            'message': '',
        }


def _empty_ml_result(message: str) -> Dict[str, Any]:
    return {
        'is_ready': False,
        'forecast_rows': [],
        'feature_importance': [],
        'backtest_rows': [],
        'count_mae': None,
        'count_rmse': None,
        'count_smape': None,
        'count_poisson_deviance': None,
        'baseline_count_mae': None,
        'baseline_count_rmse': None,
        'baseline_count_smape': None,
        'baseline_count_poisson_deviance': None,
        'heuristic_count_mae': None,
        'heuristic_count_rmse': None,
        'heuristic_count_smape': None,
        'heuristic_count_poisson_deviance': None,
        'count_vs_baseline_delta': None,
        'brier_score': None,
        'baseline_brier_score': None,
        'heuristic_brier_score': None,
        'roc_auc': None,
        'baseline_roc_auc': None,
        'heuristic_roc_auc': None,
        'f1_score': None,
        'baseline_f1_score': None,
        'heuristic_f1_score': None,
        'log_loss': None,
        'baseline_log_loss': None,
        'heuristic_log_loss': None,
        'count_comparison_rows': [],
        'event_comparison_rows': [],
        'backtest_overview': {
            'folds': 0,
            'min_train_rows': 0,
            'validation_horizon_days': 1,
            'selection_rule': COUNT_SELECTION_RULE,
            'event_selection_rule': EVENT_SELECTION_RULE,
            'classification_threshold': CLASSIFICATION_THRESHOLD,
            'event_backtest_event_rate': None,
            'event_probability_informative': False,
            'event_probability_note': None,
            'event_probability_reason_code': None,
            'candidate_model_labels': [],
            'dispersion_ratio': None,
            'prediction_interval_level': PREDICTION_INTERVAL_LEVEL,
            'prediction_interval_level_display': f'{int(round(PREDICTION_INTERVAL_LEVEL * 100))}%',
            'prediction_interval_coverage': None,
            'prediction_interval_coverage_display': '—',
            'prediction_interval_method_label': PREDICTION_INTERVAL_METHOD_LABEL,
            'prediction_interval_coverage_validated': False,
            'prediction_interval_coverage_note': 'Validated out-of-sample coverage is unavailable because backtesting was not run.',
            'prediction_interval_calibration_windows': 0,
            'prediction_interval_evaluation_windows': 0,
            'prediction_interval_validation_scheme_key': 'not_validated',
            'prediction_interval_validation_scheme_label': 'validated out-of-sample coverage unavailable',
            'prediction_interval_validation_explanation': 'Validated out-of-sample coverage is unavailable because backtesting was not run.',
            'prediction_interval_calibration_range_label': '—',
            'prediction_interval_evaluation_range_label': '—',
            'rolling_scheme_label': 'Проверка на истории не выполнена',
        },
        'selected_count_model_key': EXPLAINABLE_COUNT_MODEL_KEY,
        'selected_count_model_reason': '',
        'selected_count_model_reason_short': '',
        'candidate_count_model_labels': [],
        'selected_event_model_key': 'heuristic_probability',
        'selected_event_model_label': 'Сценарная эвристическая вероятность',
        'top_feature_label': '-',
        'count_model_label': COUNT_MODEL_LABELS.get(EXPLAINABLE_COUNT_MODEL_KEY, 'Регрессия Пуассона'),
        'prediction_interval_level': PREDICTION_INTERVAL_LEVEL,
        'prediction_interval_level_display': f'{int(round(PREDICTION_INTERVAL_LEVEL * 100))}%',
        'prediction_interval_coverage': None,
        'prediction_interval_coverage_display': '—',
        'prediction_interval_method_label': PREDICTION_INTERVAL_METHOD_LABEL,
        'event_model_label': None,
        'event_backtest_available': False,
        'event_probability_enabled': False,
        'event_probability_note': None,
        'event_probability_reason_code': None,
        'temperature_feature_enabled': False,
        'temperature_non_null_days': 0,
        'temperature_total_days': 0,
        'temperature_coverage': 0.0,
        'temperature_note': None,
        'backtest_method_label': 'Проверка на истории не выполнена',
        'classifier_ready': False,
        'message': message,
    }


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


def _temperature_source_series(frame: pd.DataFrame) -> pd.Series:
    if 'avg_temperature' in frame.columns:
        return pd.to_numeric(frame['avg_temperature'], errors='coerce')
    if 'temp_value' in frame.columns:
        return pd.to_numeric(frame['temp_value'], errors='coerce')
    return pd.Series(np.nan, index=frame.index, dtype=float)


def _temperature_quality_summary(frame: pd.DataFrame) -> Dict[str, Any]:
    reference = _prepare_reference_frame(frame)
    total_days = int(len(reference))
    temperature_source = _temperature_source_series(reference)
    history_rows = [
        {
            'avg_temperature': None if pd.isna(value) else float(value),
        }
        for value in temperature_source.tolist()
    ]
    return _temperature_quality_from_daily_history(history_rows)
    non_null_days = int(temperature_source.notna().sum())
    coverage = (float(non_null_days) / float(total_days)) if total_days > 0 else 0.0
    usable = (
        total_days > 0
        and non_null_days >= MIN_TEMPERATURE_NON_NULL_DAYS
        and coverage >= MIN_TEMPERATURE_COVERAGE
    )
    if non_null_days <= 0:
        quality_key = 'missing'
        quality_label = 'Нет измерений'
    elif usable:
        quality_key = 'good'
        quality_label = 'Достаточное покрытие'
    else:
        quality_key = 'sparse'
        quality_label = 'Низкое покрытие'
    return {
        'quality_key': quality_key,
        'quality_label': quality_label,
        'non_null_days': non_null_days,
        'total_days': total_days,
        'coverage': coverage,
        'usable': usable,
    }


def _temperature_quality_note(temperature_stats: Dict[str, Any]) -> str:
    non_null_days = int(temperature_stats.get('non_null_days', 0) or 0)
    total_days = int(temperature_stats.get('total_days', 0) or 0)
    coverage = float(temperature_stats.get('coverage', 0.0) or 0.0)
    coverage_display = _format_percent(coverage * 100.0)
    if temperature_stats.get('usable'):
        return (
            f'Температурных дней с непустым значением: {non_null_days} из {total_days} '
            f'({coverage_display}); температурный признак используется в ML.'
        )
    return (
        f'Температурных дней с непустым значением: {non_null_days} из {total_days} '
        f'({coverage_display}); это ниже порога {MIN_TEMPERATURE_NON_NULL_DAYS} дней и {int(MIN_TEMPERATURE_COVERAGE * 100)}% покрытия, '
        'поэтому температурный признак исключён из ML и исторических температурных fallback-статистик.'
    )


def _temperature_feature_columns(temperature_stats: Optional[Dict[str, Any]]) -> List[str]:
    if temperature_stats is not None and not bool(temperature_stats.get('usable', True)):
        return NON_TEMPERATURE_FEATURE_COLUMNS
    return FEATURE_COLUMNS


def _fit_temperature_statistics(frame: pd.DataFrame) -> Dict[str, Any]:
    reference = _prepare_reference_frame(frame)
    quality = _temperature_quality_summary(reference)
    if reference.empty:
        return {
            'monthly': {},
            'overall': 0.0,
            **quality,
            'note': _temperature_quality_note(quality),
        }

    if not quality['usable']:
        return {
            'monthly': {},
            'overall': 0.0,
            **quality,
            'note': _temperature_quality_note(quality),
        }

    temperature_source = _temperature_source_series(reference)
    monthly = {
        int(month): float(value)
        for month, value in temperature_source.groupby(reference['date'].dt.month).mean().dropna().items()
    }
    overall = float(temperature_source.mean()) if temperature_source.notna().any() else 0.0
    return {
        'monthly': monthly,
        'overall': overall,
        **quality,
        'note': _temperature_quality_note(quality),
    }


def _apply_temperature_statistics(frame: pd.DataFrame, temperature_stats: Dict[str, Any]) -> pd.DataFrame:
    result = _prepare_reference_frame(frame)
    temperature_source = _temperature_source_series(result)
    if not bool(temperature_stats.get('usable', True)):
        result['avg_temperature'] = np.nan
        result['temp_value'] = 0.0
        return result

    monthly_fill = result['date'].dt.month.map(temperature_stats.get('monthly', {}))
    overall_temperature = float(temperature_stats.get('overall', 0.0))
    result['temp_value'] = temperature_source.fillna(monthly_fill).fillna(overall_temperature).fillna(0.0)
    return result


def _build_backtest_seed_dataset(frame: pd.DataFrame) -> pd.DataFrame:
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
    if temperature_stats is None:
        temperature_stats = _fit_temperature_statistics(frame)
    prepared = _apply_temperature_statistics(frame, temperature_stats)
    featured = _feature_frame(prepared)
    feature_columns = _temperature_feature_columns(temperature_stats)
    dataset = featured.dropna(subset=feature_columns + ['count']).copy().reset_index(drop=True)
    dataset['event'] = (dataset['count'] > 0).astype(int)
    return prepared, dataset, temperature_stats


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



def _build_count_model(model_key: str):
    if model_key == 'poisson':
        if PoissonRegressor is None:
            return None
        return PoissonRegressor(**_POISSON_PARAMS)
    if model_key == 'tweedie':
        if TweedieRegressor is None:
            return None
        return TweedieRegressor(**_TWEEDIE_PARAMS)
    raise ValueError(f'Unsupported count model: {model_key}')


def _count_model_scaled_columns(columns: List[str]) -> List[str]:
    return [column for column in COUNT_MODEL_CONTINUOUS_COLUMNS if column in columns]


def _build_count_model_pipeline(model_key: str, X_train: pd.DataFrame):
    model = _build_count_model(model_key)
    if model is None:
        return None

    scaled_columns = _count_model_scaled_columns(list(X_train.columns))
    if ColumnTransformer is None or Pipeline is None or StandardScaler is None or not scaled_columns:
        return model

    preprocessor = ColumnTransformer(
        transformers=[
            ('scaled_continuous', StandardScaler(), scaled_columns),
        ],
        remainder='passthrough',
        sparse_threshold=0.0,
    )
    return Pipeline(
        steps=[
            ('preprocess', preprocessor),
            ('model', model),
        ]
    )


def _prepare_statsmodels_count_design(
    X_train: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[str], Optional[Any]]:
    prepared = X_train.copy()
    scaled_columns = _count_model_scaled_columns(list(prepared.columns))
    if StandardScaler is None or not scaled_columns:
        return prepared, [], None

    scaler = StandardScaler()
    prepared.loc[:, scaled_columns] = scaler.fit_transform(prepared[scaled_columns])
    return prepared, scaled_columns, scaler


def _warning_indicates_unstable_fit(warning_item: warnings.WarningMessage) -> bool:
    warning_categories = (
        ConvergenceWarning,
        StatsmodelsConvergenceWarning,
        PerfectSeparationWarning,
        HessianInversionWarning,
    )
    if any(category is not None and issubclass(warning_item.category, category) for category in warning_categories):
        return True

    message = str(warning_item.message).lower()
    return any(token in message for token in WARNING_INSTABILITY_MESSAGE_TOKENS)


def _has_warning_instability(caught_warnings: List[warnings.WarningMessage]) -> bool:
    return any(_warning_indicates_unstable_fit(item) for item in caught_warnings)


def _fit_with_convergence_guard(model: Any, X_train: pd.DataFrame, y_train: np.ndarray) -> bool:
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter('always')
        if ConvergenceWarning is not None:
            warnings.simplefilter('always', ConvergenceWarning)
        model.fit(X_train, y_train)
    return not _has_warning_instability(caught_warnings)



def _fit_count_model(model_key: str, frame: pd.DataFrame, feature_columns: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    X_train = _build_design_matrix(frame, feature_columns=feature_columns)
    y_train = frame['count'].to_numpy(dtype=float)
    return _fit_count_model_from_design(model_key, X_train, y_train)



def _fit_count_model_from_design(model_key: str, X_train: pd.DataFrame, y_train: np.ndarray) -> Optional[Dict[str, Any]]:
    if model_key == 'negative_binomial':
        return _fit_negative_binomial_model_from_design(X_train, y_train)

    model = _build_count_model_pipeline(model_key, X_train)
    if model is None:
        return None

    try:
        if not _fit_with_convergence_guard(model, X_train, y_train):
            return None
    except Exception:
        return None
    return {
        'key': model_key,
        'backend': 'sklearn',
        'model': model,
        'columns': list(X_train.columns),
    }



def _fit_negative_binomial_model(frame: pd.DataFrame, feature_columns: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    X_train = _build_design_matrix(frame, feature_columns=feature_columns)
    y_train = frame['count'].to_numpy(dtype=float)
    return _fit_negative_binomial_model_from_design(X_train, y_train)



def _fit_negative_binomial_model_from_design(X_train: pd.DataFrame, y_train: np.ndarray) -> Optional[Dict[str, Any]]:
    if sm is None:
        return None

    alpha = _estimate_negative_binomial_alpha(y_train)
    try:
        prepared_X, scaled_columns, scaler = _prepare_statsmodels_count_design(X_train)
        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter('always')
            if ConvergenceWarning is not None:
                warnings.simplefilter('always', ConvergenceWarning)
            if StatsmodelsConvergenceWarning is not None:
                warnings.simplefilter('always', StatsmodelsConvergenceWarning)
            if PerfectSeparationWarning is not None:
                warnings.simplefilter('always', PerfectSeparationWarning)
            if HessianInversionWarning is not None:
                warnings.simplefilter('always', HessianInversionWarning)
            exog = sm.add_constant(prepared_X, has_constant='add')
            model = sm.GLM(y_train, exog, family=sm.families.NegativeBinomial(alpha=alpha))
            result = model.fit(maxiter=300, disp=0)
    except Exception:
        return None
    if _has_warning_instability(caught_warnings):
        return None
    if getattr(result, 'converged', True) is False:
        return None
    return {
        'key': 'negative_binomial',
        'backend': 'statsmodels',
        'model': result,
        'columns': list(X_train.columns),
        'alpha': alpha,
        'scaled_columns': scaled_columns,
        'scaler': scaler,
    }



def _predict_count_model(model_bundle: Dict[str, Any], frame: pd.DataFrame) -> np.ndarray:
    X = _build_design_matrix(frame, model_bundle['columns'])
    return _predict_count_from_design(model_bundle, X)



def _predict_count_from_design(model_bundle: Dict[str, Any], X: pd.DataFrame) -> np.ndarray:
    X = X.reindex(columns=model_bundle['columns'], fill_value=0.0)
    if model_bundle.get('backend') == 'statsmodels':
        scaled_columns = list(model_bundle.get('scaled_columns') or [])
        scaler = model_bundle.get('scaler')
        prepared_X = X.copy()
        if scaler is not None and scaled_columns:
            prepared_X.loc[:, scaled_columns] = scaler.transform(prepared_X[scaled_columns])
        exog = sm.add_constant(prepared_X, has_constant='add')
        predictions = np.asarray(model_bundle['model'].predict(exog), dtype=float)
    else:
        predictions = np.asarray(model_bundle['model'].predict(X), dtype=float)
    return np.clip(predictions, 0.0, None)



def _estimate_negative_binomial_alpha(counts: np.ndarray) -> float:
    values = np.asarray(counts, dtype=float)
    if values.size <= 1:
        return 0.25
    mean_value = max(float(np.mean(values)), MIN_POSITIVE_PREDICTION)
    variance = float(np.var(values, ddof=1))
    alpha = max((variance - mean_value) / max(mean_value ** 2, MIN_POSITIVE_PREDICTION), 1e-4)
    return min(alpha, 5.0)

def _can_train_event_model(event_series: pd.Series) -> bool:
    positives = int(event_series.sum())
    negatives = int(len(event_series) - positives)
    return positives >= MIN_EVENT_CLASS_COUNT and negatives >= MIN_EVENT_CLASS_COUNT



def _fit_event_model(frame: pd.DataFrame, feature_columns: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    X_train = _build_design_matrix(frame, feature_columns=feature_columns)
    y_train = frame['event'].to_numpy(dtype=int)
    return _fit_event_model_from_design(X_train, y_train)



def _fit_event_model_from_design(X_train: pd.DataFrame, y_train: np.ndarray) -> Optional[Dict[str, Any]]:
    if not _can_train_event_model(pd.Series(y_train)):
        return None
    model = LogisticRegression(**_LOGISTIC_PARAMS)
    try:
        model.fit(X_train, y_train)
    except Exception:
        return None
    return {
        'model': model,
        'columns': list(X_train.columns),
    }



def _predict_event_probability(model_bundle: Dict[str, Any], frame: pd.DataFrame) -> np.ndarray:
    X = _build_design_matrix(frame, model_bundle['columns'])
    return _predict_event_probability_from_design(model_bundle, X)



def _predict_event_probability_from_design(model_bundle: Dict[str, Any], X: pd.DataFrame) -> np.ndarray:
    X = X.reindex(columns=model_bundle['columns'], fill_value=0.0)
    probabilities = np.asarray(model_bundle['model'].predict_proba(X)[:, 1], dtype=float)
    return probabilities



def _prepare_reference_frame(frame: pd.DataFrame) -> pd.DataFrame:
    reference = frame.copy().sort_values('date').reset_index(drop=True)
    if 'weekday' not in reference.columns:
        reference['weekday'] = reference['date'].dt.weekday.astype(int)
    if 'event' not in reference.columns:
        reference['event'] = (reference['count'] > 0).astype(int)
    if 'avg_temperature' not in reference.columns:
        reference['avg_temperature'] = np.nan
    return reference



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

@profiled('ml_backtest')
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


def _compute_event_metrics_legacy(rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    common_rows = [
        row
        for row in rows
        if row['baseline_event_probability'] is not None and row['heuristic_event_probability'] is not None
    ]
    if len(common_rows) < MIN_BACKTEST_POINTS:
        return {
            'available': False,
            'logistic_available': False,
            'selected_model_key': 'heuristic_probability',
            'selected_model_label': 'Сценарная эвристическая вероятность',
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
            'rows_used': 0,
            'selection_rule': EVENT_SELECTION_RULE,
        }

    classifier_rows = [row for row in common_rows if row['predicted_event_probability'] is not None]
    logistic_available = len(classifier_rows) >= MIN_BACKTEST_POINTS
    evaluation_rows = classifier_rows if logistic_available else common_rows

    actuals = np.asarray([int(row['actual_event']) for row in evaluation_rows], dtype=int)
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
            'is_selected': not logistic_available,
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
        classifier_selected = _event_metric_sort_key(
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
    }



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


def _history_records_from_frame(frame: pd.DataFrame, temperature_usable: bool = True) -> List[Dict[str, Any]]:
    return [
        {
            'date': pd.Timestamp(row.date),
            'count': float(row.count),
            'avg_temperature': None if (not temperature_usable or pd.isna(row.avg_temperature)) else float(row.avg_temperature),
        }
        for row in frame[['date', 'count', 'avg_temperature']].itertuples(index=False)
    ]


def _predict_future_count(
    selected_count_model_key: str,
    history_records: List[Dict[str, Any]],
    history_counts: List[float],
    target_date: date,
    temp_value: float,
    count_model: Optional[Dict[str, Any]],
    temperature_usable: bool,
) -> float:
    if selected_count_model_key == 'seasonal_baseline':
        reference_train = _prepare_reference_frame(pd.DataFrame(history_records))
        return float(_baseline_expected_count(reference_train, pd.Timestamp(target_date)))

    if selected_count_model_key == 'heuristic_forecast':
        forecast_rows = _build_scenario_forecast_rows(history_records, 1, temp_value if temperature_usable else None)
        if forecast_rows:
            return max(0.0, float(forecast_rows[0].get('forecast_value', 0.0)))
        reference_train = _prepare_reference_frame(pd.DataFrame(history_records))
        return float(_baseline_expected_count(reference_train, pd.Timestamp(target_date)))

    if count_model is None:
        return 0.0

    feature_row = _future_feature_row(history_counts, target_date, temp_value)
    feature_frame = pd.DataFrame([feature_row], columns=FEATURE_COLUMNS)
    return float(_predict_count_model(count_model, feature_frame)[0])

def _build_future_forecast_rows(
    frame: pd.DataFrame,
    selected_count_model_key: str,
    count_model: Optional[Dict[str, Any]],
    event_model: Optional[Dict[str, Any]],
    forecast_days: int,
    scenario_temperature: Optional[float],
    interval_calibration: Dict[str, Any],
    temperature_stats: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    temperature_usable = bool((temperature_stats or {}).get('usable', True))
    monthly_temp = frame.groupby(frame['date'].dt.month)['temp_value'].mean().to_dict() if temperature_usable else {}
    overall_temp = float(frame['temp_value'].mean()) if temperature_usable and not frame.empty else 0.0
    history_counts = list(frame['count'].astype(float))
    history_records = _history_records_from_frame(frame, temperature_usable=temperature_usable)
    sorted_history_counts = np.sort(np.asarray(history_counts, dtype=float)) if history_counts else np.asarray([], dtype=float)
    last_date = frame['date'].dt.date.iloc[-1]

    interval_label = str(
        interval_calibration.get('level_display')
        or f'{int(round(float(interval_calibration.get("level", PREDICTION_INTERVAL_LEVEL)) * 100.0))}%'
    )
    forecast_rows: List[Dict[str, Any]] = []
    for step in range(1, forecast_days + 1):
        target_date = last_date + timedelta(days=step)
        historical_temp_value = (
            float(monthly_temp.get(target_date.month, overall_temp))
            if temperature_usable and (monthly_temp or not frame.empty)
            else None
        )
        temp_value = scenario_temperature if temperature_usable and scenario_temperature is not None else historical_temp_value
        model_temp_value = float(temp_value) if temp_value is not None else 0.0
        feature_row = _future_feature_row(history_counts, target_date, model_temp_value)
        feature_frame = pd.DataFrame([feature_row], columns=FEATURE_COLUMNS)

        point_prediction = _predict_future_count(
            selected_count_model_key=selected_count_model_key,
            history_records=history_records,
            history_counts=history_counts,
            target_date=target_date,
            temp_value=model_temp_value,
            count_model=count_model,
            temperature_usable=temperature_usable,
        )
        lower_bound, upper_bound = _count_interval(point_prediction, interval_calibration)
        risk_index = _risk_index(point_prediction, sorted_history_counts)
        risk_level_label, risk_level_tone = _risk_band_from_index(risk_index)
        event_probability = None
        if event_model is not None:
            event_probability = float(_predict_event_probability(event_model, feature_frame)[0])

        forecast_rows.append(
            {
                'date': target_date.isoformat(),
                'date_display': target_date.strftime('%d.%m.%Y'),
                'forecast_value': round(point_prediction, 3),
                'forecast_value_display': _format_number(point_prediction),
                'lower_bound': round(lower_bound, 3),
                'lower_bound_display': _format_number(lower_bound),
                'upper_bound': round(upper_bound, 3),
                'upper_bound_display': _format_number(upper_bound),
                'range_label': f'{interval_label} interval',
                'range_display': f"{interval_label}: {_format_number(lower_bound)} - {_format_number(upper_bound)} пожара",
                'temperature_display': f"{_format_number(temp_value)} В°C",
                'risk_index': round(risk_index, 1),
                'risk_index_display': f"{int(round(risk_index))} / 100",
                'risk_level_label': risk_level_label,
                'risk_level_tone': risk_level_tone,
                'event_probability': round(event_probability, 4) if event_probability is not None else None,
                'event_probability_display': _format_probability(event_probability) if event_probability is not None else '—',
            }
        )
        history_counts.append(point_prediction)
        history_records.append(
            {
                'date': pd.Timestamp(target_date),
                'count': point_prediction,
                'avg_temperature': temp_value if temperature_usable else None,
            }
        )

    return forecast_rows

def _future_feature_row(history_counts: List[float], target_date: date, temp_value: float) -> Dict[str, float]:
    def lag_value(offset: int) -> float:
        if len(history_counts) >= offset:
            return float(history_counts[-offset])
        return float(np.mean(history_counts)) if history_counts else 0.0

    rolling_7 = float(np.mean(history_counts[-7:])) if history_counts else 0.0
    rolling_28 = float(np.mean(history_counts[-28:])) if history_counts else rolling_7
    return {
        'temp_value': float(temp_value),
        'weekday': float(target_date.weekday()),
        'month': float(target_date.month),
        'lag_1': lag_value(1),
        'lag_7': lag_value(7),
        'lag_14': lag_value(14),
        'rolling_7': rolling_7,
        'rolling_28': rolling_28,
        'trend_gap': rolling_7 - rolling_28,
    }

def _format_ratio_percent(value: Optional[float]) -> str:
    if value is None:
        return '—'
    return f"{_format_number(float(value) * 100.0)}%"


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
    edge_values: List[float] = []
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
    bins: List[Dict[str, Any]] = []
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
) -> Dict[str, Any]:
    calibration_range_label, evaluation_range_label = _prediction_interval_range_labels(
        window_dates,
        calibration_windows,
    )
    calibration = _build_prediction_interval_calibration(
        actuals[:calibration_windows],
        predictions[:calibration_windows],
        level=level,
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
) -> Dict[str, Any]:
    calibration_range_label, evaluation_range_label = _prediction_interval_range_labels(
        window_dates,
        calibration_windows,
        evaluation_prefix='blocked evaluation',
    )
    covered_flags: List[bool] = []
    blocks = _prediction_interval_validation_blocks(calibration_windows, len(window_dates))
    for block in blocks:
        block_start = int(block[0])
        calibration = _build_prediction_interval_calibration(
            actuals[:block_start],
            predictions[:block_start],
            level=level,
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
) -> Dict[str, Any]:
    calibration_range_label, evaluation_range_label = _prediction_interval_range_labels(
        window_dates,
        calibration_windows,
        evaluation_prefix='rolling evaluation',
    )
    covered_flags: List[bool] = []
    for index in range(calibration_windows, len(window_dates)):
        calibration = _build_prediction_interval_calibration(
            actuals[:index],
            predictions[:index],
            level=level,
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


def _build_prediction_interval_validation_explanation(
    selected_candidate: Dict[str, Any],
    runner_up_candidate: Optional[Dict[str, Any]],
    reference_candidate: Optional[Dict[str, Any]],
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
        f'{selected_label} was selected for validated out-of-sample coverage because {comparison_text}{reference_text}. '
        f'{PREDICTION_INTERVAL_JACKKNIFE_PLUS_LABEL} was not adopted because an honest time-series variant would '
        'require leave-one-block-out refits for every checkpoint.'
    )


def _evaluate_prediction_interval_backtest(
    actuals: np.ndarray,
    predictions: np.ndarray,
    window_dates: List[Any],
    level: float = PREDICTION_INTERVAL_LEVEL,
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
            'Validated out-of-sample coverage is unavailable because the backtest has too few rolling-origin windows '
            'for forward-only interval validation.'
        )
        calibration.update(
            coverage_validated=False,
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
    fixed_candidate = _evaluate_fixed_chrono_prediction_interval(
        actual_values,
        prediction_values,
        normalized_dates,
        calibration_windows,
        level,
    )
    blocked_candidate = _evaluate_blocked_prediction_interval(
        actual_values,
        prediction_values,
        normalized_dates,
        calibration_windows,
        level,
    )
    rolling_candidate = _evaluate_rolling_prediction_interval(
        actual_values,
        prediction_values,
        normalized_dates,
        calibration_windows,
        level,
    )
    selectable_candidates = [blocked_candidate, rolling_candidate]
    ranking = sorted(selectable_candidates, key=_prediction_interval_candidate_sort_key)
    selected_candidate = ranking[0]
    runner_up_candidate = ranking[1] if len(ranking) > 1 else None
    validation_explanation = _build_prediction_interval_validation_explanation(
        selected_candidate,
        runner_up_candidate,
        fixed_candidate,
    )

    calibration = _build_prediction_interval_calibration(
        actual_values,
        prediction_values,
        level=level,
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


def _prediction_interval_margin(prediction: float, calibration: Dict[str, Any]) -> float:
    center = max(0.0, float(prediction))
    adaptive_bins = calibration.get('adaptive_bins') or []
    edge_values = calibration.get('adaptive_bin_edges') or []
    if adaptive_bins:
        bin_index = int(np.searchsorted(np.asarray(edge_values, dtype=float), center, side='right'))
        bin_index = min(max(bin_index, 0), len(adaptive_bins) - 1)
        bin_quantile = adaptive_bins[bin_index].get('absolute_error_quantile')
        if bin_quantile is not None:
            return max(0.0, float(bin_quantile))
    return max(0.0, float(calibration.get('absolute_error_quantile', 0.0)))


def _count_interval(prediction: float, calibration: Dict[str, Any]) -> Tuple[float, float]:
    margin = _prediction_interval_margin(prediction, calibration)
    center = max(0.0, float(prediction))
    lower = max(0.0, center - margin)
    upper = max(lower, center + margin)
    return lower, upper


def _interval_coverage(
    actuals: np.ndarray,
    predictions: np.ndarray,
    calibration: Dict[str, Any],
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
        return 'Очень высокий', 'critical'
    if risk_index >= 75.0:
        return 'Высокий', 'high'
    if risk_index >= 50.0:
        return 'Средний', 'medium'
    if risk_index >= 25.0:
        return 'Ниже среднего', 'low'
    return 'Низкий', 'minimal'



def _build_feature_importance(model_bundle: Dict[str, Any], dataset: pd.DataFrame) -> List[Dict[str, Any]]:
    design = _build_design_matrix(dataset, model_bundle['columns'])
    target = dataset['count'].to_numpy(dtype=float)
    grouped_scores: Dict[str, float] = defaultdict(float)

    if permutation_importance is not None and model_bundle.get('backend') == 'sklearn':
        sample_size = min(len(design), 180)
        sample_X = design.tail(sample_size)
        sample_y = target[-sample_size:]
        try:
            # Use threads inside the web process to avoid spawning loky workers on every request.
            if parallel_backend is not None:
                with parallel_backend('threading', n_jobs=-1):
                    result = permutation_importance(
                        model_bundle['model'],
                        sample_X,
                        sample_y,
                        n_repeats=PERMUTATION_REPEATS,
                        random_state=42,
                        scoring='neg_mean_absolute_error',
                        n_jobs=-1,
                    )
            else:
                result = permutation_importance(
                    model_bundle['model'],
                    sample_X,
                    sample_y,
                    n_repeats=PERMUTATION_REPEATS,
                    random_state=42,
                    scoring='neg_mean_absolute_error',
                    n_jobs=1,
                )
            for column_name, score in zip(sample_X.columns, result.importances_mean):
                grouped_scores[_aggregate_feature_name(column_name)] += max(0.0, float(score))
        except Exception:
            grouped_scores.clear()

    if not grouped_scores:
        fallback = _fallback_feature_importance(model_bundle)
        for column_name, score in fallback.items():
            grouped_scores[_aggregate_feature_name(column_name)] += max(0.0, float(score))

    total_score = sum(grouped_scores.values())
    if total_score <= 0:
        return []

    items = []
    for feature_name, score in sorted(grouped_scores.items(), key=lambda item: item[1], reverse=True):
        share = score / total_score
        items.append(
            {
                'feature': feature_name,
                'label': FEATURE_LABELS.get(feature_name, feature_name),
                'importance': round(float(share), 4),
                'importance_display': _format_number(float(share) * 100.0),
            }
        )
    return items



def _fallback_feature_importance(model_bundle: Dict[str, Any]) -> Dict[str, float]:
    model = model_bundle['model']
    columns = model_bundle['columns']
    if hasattr(model, 'feature_importances_'):
        values = [float(item) for item in getattr(model, 'feature_importances_')]
        return dict(zip(columns, values))
    if hasattr(model, 'coef_'):
        raw = np.asarray(getattr(model, 'coef_'), dtype=float).reshape(-1)
        return dict(zip(columns, np.abs(raw)))
    if hasattr(model, 'params'):
        params = getattr(model, 'params')
        param_values = np.asarray(params, dtype=float).reshape(-1)
        if param_values.size == len(columns) + 1:
            param_values = param_values[1:]
        return dict(zip(columns, np.abs(param_values[: len(columns)])))
    return {column_name: 0.0 for column_name in columns}

def _aggregate_feature_name(column_name: str) -> str:
    if column_name.startswith('weekday_'):
        return 'weekday'
    if column_name.startswith('month_'):
        return 'month'
    return column_name



def _bound_probability(value: float) -> float:
    return min(1.0, max(0.0, float(value)))



def _format_probability(value: Optional[float]) -> str:
    if value is None:
        return '—'
    return _format_percent(float(value) * 100.0)
