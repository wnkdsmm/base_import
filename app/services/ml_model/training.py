from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import log_loss, roc_auc_score

try:
    from sklearn.inspection import permutation_importance
except Exception:  # pragma: no cover - graceful fallback for older sklearn
    permutation_importance = None

try:
    from sklearn.linear_model import LogisticRegression, PoissonRegressor
except Exception:  # pragma: no cover - graceful fallback for older sklearn
    from sklearn.linear_model import LogisticRegression

    PoissonRegressor = None

from app.services.forecasting.data import _build_forecast_rows as _build_scenario_forecast_rows
from app.services.forecasting.utils import _format_number, _format_percent
from app.services.model_quality import compute_classification_metrics, compute_count_metrics

from .constants import (
    COUNT_MODEL_LABELS,
    EVENT_MODEL_LABEL,
    FEATURE_LABELS,
    MAX_BACKTEST_POINTS,
    MAX_HISTORY_POINTS,
    MIN_BACKTEST_POINTS,
    MIN_DAILY_HISTORY,
    MIN_EVENT_CLASS_COUNT,
    MIN_FEATURE_ROWS,
    PERMUTATION_REPEATS,
    _LOGISTIC_PARAMS,
    _POISSON_PARAMS,
    _RF_PARAMS,
)

FEATURE_COLUMNS = ['temp_value', 'weekday', 'month', 'lag_1', 'lag_7', 'lag_14', 'rolling_7', 'rolling_28', 'trend_gap']
COUNT_MODEL_KEYS = ['poisson', 'random_forest']
EXPLAINABLE_COUNT_MODEL_KEY = 'poisson'
MIN_POSITIVE_PREDICTION = 1e-6
CLASSIFICATION_THRESHOLD = 0.5
COUNT_SELECTION_RULE = (
    'Минимум девиации Пуассона, затем MAE; при близком качестве приоритет у более объяснимой регрессии Пуассона, '
    'а сценарный прогноз показывается как внешний бенчмарк.'
)


def _apply_history_window(records: List[Dict[str, Any]], history_window: str) -> List[Dict[str, Any]]:
    if not records or history_window == 'all':
        return records
    latest_year = max(item['date'].year for item in records)
    min_year = latest_year - 2 if history_window == 'recent_3' else latest_year - 4
    return [item for item in records if item['date'].year >= min_year]



def _train_ml_model(daily_history: List[Dict[str, Any]], forecast_days: int, scenario_temperature: Optional[float]) -> Dict[str, Any]:
    if len(daily_history) < MIN_DAILY_HISTORY or forecast_days <= 0:
        return _empty_ml_result(
            f'Для ML-блока нужно минимум {MIN_DAILY_HISTORY} дней непрерывной дневной истории, чтобы выполнить rolling-origin backtesting и обучить модель.'
        )

    history_tail = daily_history[-MAX_HISTORY_POINTS:]
    frame = pd.DataFrame(
        {
            'date': pd.to_datetime([item['date'] for item in history_tail]),
            'count': [float(item['count']) for item in history_tail],
            'avg_temperature': [item.get('avg_temperature') for item in history_tail],
        }
    ).sort_values('date').reset_index(drop=True)
    frame['temp_value'] = frame['avg_temperature']
    frame['temp_value'] = frame.groupby(frame['date'].dt.month)['temp_value'].transform(lambda series: series.fillna(series.mean()))
    frame['temp_value'] = frame['temp_value'].fillna(frame['temp_value'].mean()).fillna(0.0)

    featured = _feature_frame(frame)
    dataset = featured.dropna(subset=FEATURE_COLUMNS + ['count']).copy().reset_index(drop=True)
    dataset['event'] = (dataset['count'] > 0).astype(int)
    if len(dataset) < MIN_FEATURE_ROWS:
        return _empty_ml_result(
            f'После формирования лагов и скользящих признаков осталось только {len(dataset)} наблюдений: этого мало для корректного rolling-origin backtesting.'
        )

    backtest = _run_backtest(frame.copy(), dataset)
    if not backtest['is_ready']:
        return _empty_ml_result(backtest['message'])

    selected_count_model_key = backtest['selected_count_model_key']
    final_count_model = _fit_count_model(selected_count_model_key, dataset)
    if final_count_model is None:
        return _empty_ml_result('Не удалось обучить итоговую модель по числу пожаров на полной выборке.')

    classifier_validated = backtest['event_metrics'].get('logistic_available', False) and _can_train_event_model(dataset['event'])
    final_event_model = _fit_event_model(dataset) if classifier_validated else None

    dispersion = _estimate_dispersion(dataset['count'].to_numpy(dtype=float), backtest['selected_metrics']['rmse'])
    forecast_rows = _build_future_forecast_rows(
        frame=frame,
        count_model=final_count_model,
        event_model=final_event_model,
        forecast_days=forecast_days,
        scenario_temperature=scenario_temperature,
        dispersion=dispersion,
    )

    feature_importance = _build_feature_importance(final_count_model, dataset)
    backtest_rows = [
        {
            'date': row['date'],
            'actual_count': round(float(row['actual_count']), 3),
            'predicted_count': round(float(row['predicted_count']), 3),
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
        'count_comparison_rows': backtest['count_comparison_rows'],
        'event_comparison_rows': backtest['event_metrics'].get('comparison_rows', []),
        'backtest_overview': backtest['backtest_overview'],
        'selected_count_model_key': selected_count_model_key,
        'selected_event_model_key': backtest['event_metrics'].get('selected_model_key'),
        'selected_event_model_label': backtest['event_metrics'].get('selected_model_label'),
        'top_feature_label': feature_importance[0]['label'] if feature_importance else '-',
        'count_model_label': COUNT_MODEL_LABELS.get(selected_count_model_key, selected_count_model_key),
        'event_model_label': EVENT_MODEL_LABEL if final_event_model is not None else None,
        'event_backtest_available': backtest['event_metrics'].get('available', False),
        'backtest_method_label': f"Скользящая проверка на истории (rolling-origin): {len(backtest_rows)} одношаговых окон",
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
        'count_comparison_rows': [],
        'event_comparison_rows': [],
        'backtest_overview': {
            'folds': 0,
            'min_train_rows': 0,
            'validation_horizon_days': 1,
            'selection_rule': COUNT_SELECTION_RULE,
            'classification_threshold': CLASSIFICATION_THRESHOLD,
        },
        'selected_count_model_key': EXPLAINABLE_COUNT_MODEL_KEY,
        'selected_event_model_key': 'heuristic_probability',
        'selected_event_model_label': 'Сценарная эвристическая вероятность',
        'top_feature_label': '-',
        'count_model_label': COUNT_MODEL_LABELS.get(EXPLAINABLE_COUNT_MODEL_KEY, 'Регрессия Пуассона'),
        'event_model_label': None,
        'event_backtest_available': False,
        'backtest_method_label': 'Проверка на истории не выполнена',
        'classifier_ready': False,
        'message': message,
    }



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



def _build_design_matrix(frame: pd.DataFrame, expected_columns: Optional[List[str]] = None) -> pd.DataFrame:
    design = frame[FEATURE_COLUMNS].copy()
    design['weekday'] = design['weekday'].astype(int).astype(str)
    design['month'] = design['month'].astype(int).astype(str)
    design = pd.get_dummies(design, columns=['weekday', 'month'], prefix=['weekday', 'month'], dtype=float)
    if expected_columns is not None:
        design = design.reindex(columns=expected_columns, fill_value=0.0)
    return design.astype(float)



def _build_count_model(model_key: str):
    if model_key == 'poisson':
        if PoissonRegressor is None:
            return None
        return PoissonRegressor(**_POISSON_PARAMS)
    if model_key == 'random_forest':
        return RandomForestRegressor(**_RF_PARAMS)
    raise ValueError(f'Unsupported count model: {model_key}')



def _fit_count_model(model_key: str, frame: pd.DataFrame) -> Optional[Dict[str, Any]]:
    model = _build_count_model(model_key)
    if model is None:
        return None
    X_train = _build_design_matrix(frame)
    y_train = frame['count'].to_numpy(dtype=float)
    try:
        model.fit(X_train, y_train)
    except Exception:
        return None
    return {
        'key': model_key,
        'model': model,
        'columns': list(X_train.columns),
    }



def _predict_count_model(model_bundle: Dict[str, Any], frame: pd.DataFrame) -> np.ndarray:
    X = _build_design_matrix(frame, model_bundle['columns'])
    predictions = np.asarray(model_bundle['model'].predict(X), dtype=float)
    return np.clip(predictions, 0.0, None)



def _can_train_event_model(event_series: pd.Series) -> bool:
    positives = int(event_series.sum())
    negatives = int(len(event_series) - positives)
    return positives >= MIN_EVENT_CLASS_COUNT and negatives >= MIN_EVENT_CLASS_COUNT



def _fit_event_model(frame: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if not _can_train_event_model(frame['event']):
        return None
    model = LogisticRegression(**_LOGISTIC_PARAMS)
    X_train = _build_design_matrix(frame)
    y_train = frame['event'].to_numpy(dtype=int)
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
    probabilities = np.asarray(model_bundle['model'].predict_proba(X)[:, 1], dtype=float)
    return np.clip(probabilities, 0.001, 0.999)



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
    return _clip_probability(probability)



def _scenario_reference_forecast(train: pd.DataFrame, test: pd.DataFrame) -> Tuple[float, Optional[float]]:
    if train.empty:
        return 0.0, None

    temperature_value = None
    if 'temp_value' in test.columns:
        candidate = test['temp_value'].iloc[0]
        if not pd.isna(candidate):
            temperature_value = float(candidate)

    train_history = [
        {
            'date': pd.Timestamp(row.date),
            'count': float(row.count),
            'avg_temperature': None if pd.isna(row.avg_temperature) else float(row.avg_temperature),
        }
        for row in train[['date', 'count', 'avg_temperature']].itertuples(index=False)
    ]
    forecast_rows = _build_scenario_forecast_rows(train_history, 1, temperature_value)
    if not forecast_rows:
        target_date = pd.Timestamp(test['date'].iloc[0])
        fallback_count = _baseline_expected_count(train, target_date)
        return fallback_count, _clip_probability(1.0 - math.exp(-max(0.0, fallback_count)))

    row = forecast_rows[0]
    probability = row.get('fire_probability')
    return max(0.0, float(row.get('forecast_value', 0.0))), _clip_probability(probability if probability is not None else 0.0)

def _run_backtest(history_frame: pd.DataFrame, dataset: pd.DataFrame) -> Dict[str, Any]:
    history_frame = history_frame.sort_values('date').reset_index(drop=True)
    dataset = dataset.reset_index(drop=True)
    min_train_rows = min(28, max(14, len(dataset) // 2))
    available_backtest_points = len(dataset) - min_train_rows
    if available_backtest_points < MIN_BACKTEST_POINTS:
        return {
            'is_ready': False,
            'message': (
                f'Для rolling-origin backtesting нужно хотя бы {MIN_BACKTEST_POINTS} одношаговых проверок, '
                f'а сейчас после лагов доступно только {available_backtest_points}.'
            ),
        }

    start_index = len(dataset) - min(MAX_BACKTEST_POINTS, available_backtest_points)
    rows: List[Dict[str, Any]] = []

    for index in range(start_index, len(dataset)):
        model_train = dataset.iloc[:index].copy()
        test = dataset.iloc[index:index + 1].copy()
        if model_train.empty or test.empty:
            continue

        test_date = pd.Timestamp(test['date'].iloc[0])
        history_train = history_frame.loc[history_frame['date'] < test_date].copy()
        reference_train = _prepare_reference_frame(history_train)
        if reference_train.empty:
            continue

        actual_count = float(test['count'].iloc[0])
        actual_event = int(test['event'].iloc[0])
        baseline_count = _baseline_expected_count(reference_train, test_date)
        baseline_event_probability = _baseline_event_probability(reference_train, test_date)
        heuristic_count, heuristic_event_probability = _scenario_reference_forecast(reference_train, test)

        model_predictions: Dict[str, Optional[float]] = {key: None for key in COUNT_MODEL_KEYS}
        for model_key in COUNT_MODEL_KEYS:
            model_bundle = _fit_count_model(model_key, model_train)
            if model_bundle is None:
                continue
            prediction = float(_predict_count_model(model_bundle, test)[0])
            model_predictions[model_key] = prediction

        event_prediction = None
        event_bundle = _fit_event_model(model_train)
        if event_bundle is not None:
            event_prediction = float(_predict_event_probability(event_bundle, test)[0])

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

    valid_rows = [row for row in rows if any(row['predictions'].get(model_key) is not None for model_key in COUNT_MODEL_KEYS)]
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
            'message': 'Не удалось обучить ни одной модели по числу пожаров для проверки на истории.',
        }

    selected_count_model_key, selected_metrics = _select_count_model(count_metrics)

    backtest_rows = []
    for row in valid_rows:
        backtest_rows.append(
            {
                'date': row['date'],
                'actual_count': row['actual_count'],
                'predicted_count': row['predictions'][selected_count_model_key],
                'baseline_count': row['baseline_count'],
                'heuristic_count': row['heuristic_count'],
                'actual_event': row['actual_event'],
                'predicted_event_probability': row['predicted_event_probability'],
                'baseline_event_probability': row['baseline_event_probability'],
                'heuristic_event_probability': row['heuristic_event_probability'],
            }
        )

    event_metrics = _compute_event_metrics(backtest_rows)

    return {
        'is_ready': True,
        'message': '',
        'rows': backtest_rows,
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
        'selected_metrics': selected_metrics,
        'event_metrics': event_metrics,
        'backtest_overview': {
            'folds': len(backtest_rows),
            'min_train_rows': min_train_rows,
            'validation_horizon_days': 1,
            'selection_rule': COUNT_SELECTION_RULE,
            'classification_threshold': CLASSIFICATION_THRESHOLD,
        },
    }



def _select_count_model(count_metrics: Dict[str, Dict[str, Optional[float]]]) -> Tuple[str, Dict[str, Optional[float]]]:
    best_key, best_metrics = min(count_metrics.items(), key=lambda item: _metric_sort_key(item[1]))
    explainable_metrics = count_metrics.get(EXPLAINABLE_COUNT_MODEL_KEY)
    if explainable_metrics and best_key != EXPLAINABLE_COUNT_MODEL_KEY:
        if _within_relative_margin(explainable_metrics.get('poisson_deviance'), best_metrics.get('poisson_deviance'), 0.05) and _within_relative_margin(
            explainable_metrics.get('mae'), best_metrics.get('mae'), 0.05
        ):
            return EXPLAINABLE_COUNT_MODEL_KEY, explainable_metrics
    return best_key, best_metrics



def _metric_sort_key(metrics: Dict[str, Optional[float]]) -> Tuple[float, float, float]:
    return (
        metrics.get('poisson_deviance') if metrics.get('poisson_deviance') is not None else float('inf'),
        metrics.get('mae') if metrics.get('mae') is not None else float('inf'),
        metrics.get('rmse') if metrics.get('rmse') is not None else float('inf'),
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
            'is_selected': False,
            **baseline_metrics,
        },
        {
            'method_key': 'heuristic_forecast',
            'method_label': COUNT_MODEL_LABELS.get('heuristic_forecast', 'Сценарный эвристический прогноз'),
            'role_label': 'Сценарный прогноз',
            'is_selected': False,
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
                'role_label': 'Обучаемая модель',
                'is_selected': model_key == selected_count_model_key,
                **metrics,
            }
        )
    return rows

def _compute_event_metrics(rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
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
            'comparison_rows': [],
            'rows_used': 0,
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

    comparison_rows = [
        {
            'method_key': 'event_baseline',
            'method_label': 'Сезонная событийная базовая модель',
            'role_label': 'Базовая модель',
            'brier_score': heuristic_metrics.get('baseline_brier_score'),
            'roc_auc': baseline_roc_auc,
            'f1': heuristic_metrics.get('baseline_f1'),
            'is_selected': False,
        },
        {
            'method_key': 'heuristic_probability',
            'method_label': 'Сценарная эвристическая вероятность',
            'role_label': 'Сценарный прогноз',
            'brier_score': heuristic_metrics.get('brier_score'),
            'roc_auc': heuristic_roc_auc,
            'f1': heuristic_metrics.get('f1'),
            'is_selected': not logistic_available,
        },
    ]

    selected_model_key = 'heuristic_probability'
    selected_model_label = 'Сценарная эвристическая вероятность'
    selected_metrics = heuristic_metrics
    selected_roc_auc = heuristic_roc_auc
    selected_log_loss = _safe_log_loss(actuals, heuristic_probabilities)

    if logistic_available:
        classifier_probabilities = np.asarray([float(row['predicted_event_probability']) for row in evaluation_rows], dtype=float)
        classifier_metrics = compute_classification_metrics(
            actuals,
            classifier_probabilities,
            baseline_probabilities,
            threshold=CLASSIFICATION_THRESHOLD,
        )
        classifier_roc_auc = _safe_roc_auc(actuals, classifier_probabilities)
        comparison_rows.append(
            {
                'method_key': 'logistic_regression',
                'method_label': EVENT_MODEL_LABEL,
                'role_label': 'Классификатор',
                'brier_score': classifier_metrics.get('brier_score'),
                'roc_auc': classifier_roc_auc,
                'f1': classifier_metrics.get('f1'),
                'is_selected': True,
            }
        )
        selected_model_key = 'logistic_regression'
        selected_model_label = EVENT_MODEL_LABEL
        selected_metrics = classifier_metrics
        selected_roc_auc = classifier_roc_auc
        selected_log_loss = _safe_log_loss(actuals, classifier_probabilities)

    return {
        'available': True,
        'logistic_available': logistic_available,
        'selected_model_key': selected_model_key,
        'selected_model_label': selected_model_label,
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
        'comparison_rows': comparison_rows,
        'rows_used': len(evaluation_rows),
    }



def _safe_roc_auc(actuals: np.ndarray, probabilities: np.ndarray) -> Optional[float]:
    if len(np.unique(actuals)) <= 1:
        return None
    return float(roc_auc_score(actuals, probabilities))



def _safe_log_loss(actuals: np.ndarray, probabilities: np.ndarray) -> Optional[float]:
    if actuals.size == 0:
        return None
    clipped = np.clip(np.asarray(probabilities, dtype=float), 0.001, 0.999)
    return float(log_loss(actuals, clipped, labels=[0, 1]))



def _build_future_forecast_rows(
    frame: pd.DataFrame,
    count_model: Dict[str, Any],
    event_model: Optional[Dict[str, Any]],
    forecast_days: int,
    scenario_temperature: Optional[float],
    dispersion: float,
) -> List[Dict[str, Any]]:
    monthly_temp = frame.groupby(frame['date'].dt.month)['temp_value'].mean().to_dict()
    overall_temp = float(frame['temp_value'].mean()) if not frame.empty else 0.0
    history_counts = list(frame['count'].astype(float))
    sorted_history_counts = np.sort(np.asarray(history_counts, dtype=float)) if history_counts else np.asarray([], dtype=float)
    last_date = frame['date'].dt.date.iloc[-1]

    forecast_rows: List[Dict[str, Any]] = []
    for step in range(1, forecast_days + 1):
        target_date = last_date + timedelta(days=step)
        temp_value = scenario_temperature if scenario_temperature is not None else float(monthly_temp.get(target_date.month, overall_temp))
        feature_row = _future_feature_row(history_counts, target_date, temp_value)
        feature_frame = pd.DataFrame([feature_row], columns=FEATURE_COLUMNS)

        point_prediction = float(_predict_count_model(count_model, feature_frame)[0])
        lower_bound, upper_bound = _count_interval(point_prediction, dispersion)
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
                'range_display': f"{_format_number(lower_bound)} - {_format_number(upper_bound)} пожара",
                'temperature_display': f"{_format_number(temp_value)} °C",
                'risk_index': round(risk_index, 1),
                'risk_index_display': f"{int(round(risk_index))} / 100",
                'risk_level_label': risk_level_label,
                'risk_level_tone': risk_level_tone,
                'event_probability': round(event_probability, 4) if event_probability is not None else None,
                'event_probability_display': _format_probability(event_probability) if event_probability is not None else '—',
            }
        )
        history_counts.append(point_prediction)

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

def _estimate_dispersion(counts: np.ndarray, rmse: Optional[float]) -> float:
    counts = np.asarray(counts, dtype=float)
    if counts.size == 0:
        return 1.0
    mean_value = max(float(np.mean(counts)), MIN_POSITIVE_PREDICTION)
    variance = float(np.var(counts))
    raw_dispersion = max(1.0, variance / mean_value)
    if rmse is not None:
        raw_dispersion = max(raw_dispersion, 1.0 + (float(rmse) / max(1.0, math.sqrt(mean_value))) * 0.25)
    return min(raw_dispersion, 12.0)



def _count_interval(prediction: float, dispersion: float) -> Tuple[float, float]:
    std = math.sqrt(max(prediction, 0.2) * max(dispersion, 1.0))
    margin = 1.64 * std
    lower = max(0.0, prediction - margin)
    upper = max(lower, prediction + margin)
    return lower, upper



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

    if permutation_importance is not None:
        sample_size = min(len(design), 180)
        sample_X = design.tail(sample_size)
        sample_y = target[-sample_size:]
        try:
            result = permutation_importance(
                model_bundle['model'],
                sample_X,
                sample_y,
                n_repeats=PERMUTATION_REPEATS,
                random_state=42,
                scoring='neg_mean_absolute_error',
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
    return {column_name: 0.0 for column_name in columns}



def _aggregate_feature_name(column_name: str) -> str:
    if column_name.startswith('weekday_'):
        return 'weekday'
    if column_name.startswith('month_'):
        return 'month'
    return column_name



def _clip_probability(value: float) -> float:
    return min(0.999, max(0.001, float(value)))



def _format_probability(value: Optional[float]) -> str:
    if value is None:
        return '—'
    return _format_percent(float(value) * 100.0)