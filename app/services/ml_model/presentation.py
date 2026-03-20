from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np

from app.services.forecasting.utils import (
    _format_integer,
    _format_number,
    _format_period,
    _format_signed_percent,
    _history_window_label,
)

from .constants import MODEL_NAME


def _empty_light_chart(title: str, empty_message: str, kind: str = 'line') -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        'title': title,
        'kind': kind,
        'empty_message': empty_message,
    }
    if kind == 'bars':
        payload['items'] = []
    else:
        payload['value_format'] = 'count'
        payload['legend'] = []
        payload['series'] = {
            'history': [],
            'backtest_actual': [],
            'backtest_predicted': [],
            'forecast': [],
            'forecast_band': [],
        }
    return payload


def _build_forecast_chart(daily_history: List[Dict[str, Any]], ml_result: Dict[str, Any]) -> Dict[str, Any]:
    title = 'ML-прогноз ожидаемого числа пожаров'
    if not daily_history or not ml_result.get('is_ready'):
        return _empty_light_chart(title, ml_result.get('message') or 'Недостаточно данных для построения прогноза.')

    history_tail = daily_history[-120:]
    history_points = [
        {
            'x': item['date'].isoformat(),
            'y': round(float(item['count']), 3),
        }
        for item in history_tail
    ]
    backtest_actual = [
        {'x': item['date'], 'y': round(float(item.get('actual_count', 0.0)), 3)}
        for item in ml_result.get('backtest_rows', [])
    ]
    backtest_predicted = [
        {'x': item['date'], 'y': round(float(item.get('predicted_count', 0.0)), 3)}
        for item in ml_result.get('backtest_rows', [])
    ]
    forecast_points = [
        {
            'x': item['date'],
            'y': round(float(item.get('forecast_value', 0.0)), 3),
        }
        for item in ml_result.get('forecast_rows', [])
    ]
    forecast_band = [
        {
            'x': item['date'],
            'low': round(float(item.get('lower_bound', 0.0)), 3),
            'high': round(float(item.get('upper_bound', 0.0)), 3),
        }
        for item in ml_result.get('forecast_rows', [])
    ]

    return {
        'title': title,
        'kind': 'line',
        'empty_message': '',
        'value_format': 'count',
        'legend': [
            {'label': 'История', 'color': '#F97316'},
            {'label': 'Backtest факт', 'color': '#94A3B8'},
            {'label': 'Backtest прогноз', 'color': '#64748B'},
            {'label': 'ML-прогноз', 'color': '#0F766E'},
        ],
        'series': {
            'history': history_points,
            'backtest_actual': backtest_actual,
            'backtest_predicted': backtest_predicted,
            'forecast': forecast_points,
            'forecast_band': forecast_band,
        },
    }


def _build_importance_chart(feature_importance: List[Dict[str, Any]]) -> Dict[str, Any]:
    title = 'Важность признаков ML-блока'
    if not feature_importance:
        return _empty_light_chart(title, 'Модель пока не обучена.', kind='bars')
    top_items = feature_importance[:8]
    return {
        'title': title,
        'kind': 'bars',
        'empty_message': '',
        'items': [
            {
                'label': item['label'],
                'value': item['importance'],
                'value_display': item['importance_display'],
            }
            for item in top_items
        ],
    }


def _build_summary(
    selected_table: str,
    selected_cause: str,
    selected_object_category: str,
    daily_history: List[Dict[str, Any]],
    filtered_records: List[Dict[str, Any]],
    ml_result: Dict[str, Any],
    history_window: str,
    scenario_temperature: Optional[float],
) -> Dict[str, Any]:
    history_dates = [item['date'] for item in daily_history]
    slice_parts = []
    if selected_cause != 'all':
        slice_parts.append(f'Причина: {selected_cause}')
    if selected_object_category != 'all':
        slice_parts.append(f'Категория: {selected_object_category}')

    forecast_rows = ml_result.get('forecast_rows', [])
    average_expected_count = (
        float(np.mean([float(item.get('forecast_value', 0.0)) for item in forecast_rows])) if forecast_rows else 0.0
    )
    predicted_total = sum(float(item.get('forecast_value', 0.0)) for item in forecast_rows)
    peak_row = max(forecast_rows, key=lambda item: float(item.get('forecast_value', 0.0))) if forecast_rows else None
    elevated_risk_days = sum(1 for item in forecast_rows if float(item.get('risk_index', 0.0)) >= 75.0)
    has_event_classifier = bool(ml_result.get('classifier_ready'))

    event_probabilities = [
        float(item.get('event_probability'))
        for item in forecast_rows
        if item.get('event_probability') is not None
    ]
    average_event_probability = float(np.mean(event_probabilities)) if event_probabilities else None
    peak_event_row = (
        max(
            (item for item in forecast_rows if item.get('event_probability') is not None),
            key=lambda item: float(item.get('event_probability', 0.0)),
        )
        if event_probabilities
        else None
    )

    return {
        'selected_table_label': 'Все таблицы' if selected_table == 'all' else (selected_table or 'Нет таблицы'),
        'slice_label': ' | '.join(slice_parts) if slice_parts else 'Все пожары выбранной истории',
        'history_period_label': _format_period(history_dates),
        'history_window_label': _history_window_label(history_window),
        'model_label': MODEL_NAME,
        'count_model_label': ml_result.get('count_model_label') or MODEL_NAME,
        'event_model_label': ml_result.get('event_model_label') or 'Не обучен',
        'backtest_method_label': ml_result.get('backtest_method_label') or 'Backtesting не выполнен',
        'fires_count_display': _format_integer(len(filtered_records)),
        'history_days_display': _format_integer(len(daily_history)),
        'forecast_days_display': _format_integer(len(forecast_rows)),
        'last_observed_date': history_dates[-1].strftime('%d.%m.%Y') if history_dates else '-',
        'count_mae_display': _format_number(ml_result['count_mae']) if ml_result.get('count_mae') is not None else '-',
        'count_rmse_display': _format_number(ml_result['count_rmse']) if ml_result.get('count_rmse') is not None else '-',
        'count_poisson_deviance_display': (
            _format_number(ml_result['count_poisson_deviance']) if ml_result.get('count_poisson_deviance') is not None else '-'
        ),
        'baseline_count_mae_display': (
            _format_number(ml_result['baseline_count_mae']) if ml_result.get('baseline_count_mae') is not None else '-'
        ),
        'baseline_count_rmse_display': (
            _format_number(ml_result['baseline_count_rmse']) if ml_result.get('baseline_count_rmse') is not None else '-'
        ),
        'mae_vs_baseline_display': (
            _format_signed_percent(ml_result['count_vs_baseline_delta']) if ml_result.get('count_vs_baseline_delta') is not None else '-'
        ),
        'brier_display': _format_number(ml_result['brier_score']) if ml_result.get('brier_score') is not None else '—',
        'baseline_brier_display': (
            _format_number(ml_result['baseline_brier_score']) if ml_result.get('baseline_brier_score') is not None else '—'
        ),
        'roc_auc_display': _format_number(ml_result['roc_auc']) if ml_result.get('roc_auc') is not None else '—',
        'log_loss_display': _format_number(ml_result['log_loss']) if ml_result.get('log_loss') is not None else '—',
        'top_feature_label': ml_result.get('top_feature_label') or '-',
        'temperature_scenario_display': (
            f"{_format_number(scenario_temperature)} °C" if scenario_temperature is not None else 'Историческая температура'
        ),
        'predicted_total_display': _format_number(predicted_total),
        'average_expected_count_display': _format_number(average_expected_count),
        'peak_expected_count_display': peak_row['forecast_value_display'] if peak_row else '0',
        'peak_expected_count_day_display': peak_row['date_display'] if peak_row else '-',
        'elevated_risk_days_display': _format_integer(elevated_risk_days),
        'average_event_probability_display': (
            f"{_format_number(average_event_probability * 100.0)}%" if average_event_probability is not None else '—'
        ),
        'peak_event_probability_display': peak_event_row['event_probability_display'] if peak_event_row else '—',
        'peak_event_probability_day_display': peak_event_row['date_display'] if peak_event_row else '-',
        'event_probability_enabled': has_event_classifier,
    }


def _build_notes(
    preload_notes: List[str],
    metadata_items: List[Dict[str, Any]],
    filtered_records: List[Dict[str, Any]],
    daily_history: List[Dict[str, Any]],
    ml_result: Dict[str, Any],
    scenario_temperature: Optional[float],
    source_tables: List[str],
) -> List[str]:
    notes = list(preload_notes)
    if len(source_tables) > 1:
        notes.append(f'ML-модель обучается сразу по {len(source_tables)} таблицам.')
    if not filtered_records:
        notes.append('После выбранных фильтров не осталось исторических пожаров для обучения ML-модели.')
    if ml_result.get('message'):
        notes.append(ml_result['message'])
    if scenario_temperature is not None and not any(item['resolved_columns'].get('temperature') for item in metadata_items):
        notes.append('Температура задана вручную, но температурная колонка в таблицах не найдена: сценарное значение используется только для будущих дат.')

    if ml_result.get('is_ready'):
        notes.append('Основная ML-задача теперь сформулирована как прогноз ожидаемого числа пожаров в день, а не как псевдовероятность из регрессии по count data.')
        notes.append('Качество оценивается rolling-origin backtesting: каждая fold обучается только на прошлом и предсказывает следующий день.')
        notes.append('Для сравнения всегда считается сезонный baseline по неделе/недавней истории; метрика MAE показывается и для модели, и для baseline.')
        notes.append('Интервалы неопределенности построены вокруг ожидаемого числа пожаров с поправкой на эмпирическую дисперсию ряда.')
        if ml_result.get('classifier_ready'):
            notes.append('Вероятность fire/no-fire выводится только из отдельного бинарного классификатора. Она не вычисляется из ожидаемого count через 1-exp(-count).')
        else:
            notes.append('Отдельный fire/no-fire классификатор для текущего среза не показан: вместо ложной вероятности интерфейс выводит индекс риска и ожидаемую интенсивность.')
    else:
        notes.append('Пока данных недостаточно для корректного backtesting, поэтому ML-блок остаётся в безопасном режиме без прогноза.')

    if len(daily_history) < 60:
        notes.append('Истории меньше 60 дней: для корректной ML-валидации этого обычно недостаточно.')

    return notes
