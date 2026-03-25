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



def _format_optional_number(value: Optional[float]) -> str:
    return _format_number(value) if value is not None else '—'



def _format_optional_percent(value: Optional[float]) -> str:
    return f"{_format_number(value)}%" if value is not None else '—'



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
            {'label': 'Проверка на истории: факт', 'color': '#94A3B8'},
            {'label': 'Проверка на истории: прогноз', 'color': '#64748B'},
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
        'event_backtest_model_label': ml_result.get('selected_event_model_label') or 'Не показан',
        'backtest_method_label': ml_result.get('backtest_method_label') or 'Проверка на истории не выполнена',
        'fires_count_display': _format_integer(len(filtered_records)),
        'history_days_display': _format_integer(len(daily_history)),
        'forecast_days_display': _format_integer(len(forecast_rows)),
        'last_observed_date': history_dates[-1].strftime('%d.%m.%Y') if history_dates else '-',
        'count_mae_display': _format_number(ml_result['count_mae']) if ml_result.get('count_mae') is not None else '-',
        'count_rmse_display': _format_number(ml_result['count_rmse']) if ml_result.get('count_rmse') is not None else '-',
        'count_smape_display': _format_optional_percent(ml_result.get('count_smape')),
        'count_poisson_deviance_display': (
            _format_number(ml_result['count_poisson_deviance']) if ml_result.get('count_poisson_deviance') is not None else '-'
        ),
        'baseline_count_mae_display': (
            _format_number(ml_result['baseline_count_mae']) if ml_result.get('baseline_count_mae') is not None else '-'
        ),
        'baseline_count_rmse_display': (
            _format_number(ml_result['baseline_count_rmse']) if ml_result.get('baseline_count_rmse') is not None else '-'
        ),
        'baseline_count_smape_display': _format_optional_percent(ml_result.get('baseline_count_smape')),
        'heuristic_count_mae_display': _format_optional_number(ml_result.get('heuristic_count_mae')),
        'heuristic_count_rmse_display': _format_optional_number(ml_result.get('heuristic_count_rmse')),
        'heuristic_count_smape_display': _format_optional_percent(ml_result.get('heuristic_count_smape')),
        'heuristic_count_poisson_deviance_display': _format_optional_number(ml_result.get('heuristic_count_poisson_deviance')),
        'mae_vs_baseline_display': (
            _format_signed_percent(ml_result['count_vs_baseline_delta']) if ml_result.get('count_vs_baseline_delta') is not None else '-'
        ),
        'brier_display': _format_optional_number(ml_result.get('brier_score')),
        'baseline_brier_display': _format_optional_number(ml_result.get('baseline_brier_score')),
        'heuristic_brier_display': _format_optional_number(ml_result.get('heuristic_brier_score')),
        'roc_auc_display': _format_optional_number(ml_result.get('roc_auc')),
        'baseline_roc_auc_display': _format_optional_number(ml_result.get('baseline_roc_auc')),
        'heuristic_roc_auc_display': _format_optional_number(ml_result.get('heuristic_roc_auc')),
        'f1_display': _format_optional_number(ml_result.get('f1_score')),
        'baseline_f1_display': _format_optional_number(ml_result.get('baseline_f1_score')),
        'heuristic_f1_display': _format_optional_number(ml_result.get('heuristic_f1_score')),
        'log_loss_display': _format_optional_number(ml_result.get('log_loss')),
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
        'event_backtest_available': bool(ml_result.get('event_backtest_available')),
    }

def _build_quality_assessment(ml_result: Dict[str, Any]) -> Dict[str, Any]:
    overview = ml_result.get('backtest_overview', {}) or {}
    count_rows = [
        {
            'method_label': row.get('method_label', 'Метод'),
            'role_label': row.get('role_label', ''),
            'selection_label': 'Выбранная ML-модель' if row.get('is_selected') else 'Сравнение',
            'mae_display': _format_optional_number(row.get('mae')),
            'rmse_display': _format_optional_number(row.get('rmse')),
            'smape_display': _format_optional_percent(row.get('smape')),
            'poisson_display': _format_optional_number(row.get('poisson_deviance')),
            'mae_delta_display': _format_signed_percent(row.get('mae_delta_vs_baseline')) if row.get('mae_delta_vs_baseline') is not None else '0%',
        }
        for row in ml_result.get('count_comparison_rows', [])
    ]
    event_rows = [
        {
            'method_label': row.get('method_label', 'Метод'),
            'role_label': row.get('role_label', ''),
            'selection_label': 'Рабочий метод' if row.get('is_selected') else 'Сравнение',
            'brier_display': _format_optional_number(row.get('brier_score')),
            'roc_auc_display': _format_optional_number(row.get('roc_auc')),
            'f1_display': _format_optional_number(row.get('f1')),
        }
        for row in ml_result.get('event_comparison_rows', [])
    ]

    metric_cards = [
        {
            'label': 'MAE по числу пожаров',
            'value': _format_optional_number(ml_result.get('count_mae')),
            'meta': (
                f"база: {_format_optional_number(ml_result.get('baseline_count_mae'))}; "
                f"сценарный прогноз: {_format_optional_number(ml_result.get('heuristic_count_mae'))}"
            ),
        },
        {
            'label': 'RMSE по числу пожаров',
            'value': _format_optional_number(ml_result.get('count_rmse')),
            'meta': (
                f"база: {_format_optional_number(ml_result.get('baseline_count_rmse'))}; "
                f"сценарный прогноз: {_format_optional_number(ml_result.get('heuristic_count_rmse'))}"
            ),
        },
        {
            'label': 'SMAPE по числу пожаров',
            'value': _format_optional_percent(ml_result.get('count_smape')),
            'meta': (
                f"база: {_format_optional_percent(ml_result.get('baseline_count_smape'))}; "
                f"сценарный прогноз: {_format_optional_percent(ml_result.get('heuristic_count_smape'))}"
            ),
        },
    ]
    if ml_result.get('event_backtest_available'):
        metric_cards.extend(
            [
                {
                    'label': 'Показатель Брайера',
                    'value': _format_optional_number(ml_result.get('brier_score')),
                    'meta': (
                        f"база: {_format_optional_number(ml_result.get('baseline_brier_score'))}; "
                        f"сценарный прогноз: {_format_optional_number(ml_result.get('heuristic_brier_score'))}"
                    ),
                },
                {
                    'label': 'ROC-AUC',
                    'value': _format_optional_number(ml_result.get('roc_auc')),
                    'meta': (
                        f"база: {_format_optional_number(ml_result.get('baseline_roc_auc'))}; "
                        f"сценарный прогноз: {_format_optional_number(ml_result.get('heuristic_roc_auc'))}"
                    ),
                },
                {
                    'label': 'F1',
                    'value': _format_optional_number(ml_result.get('f1_score')),
                    'meta': (
                        f"база: {_format_optional_number(ml_result.get('baseline_f1_score'))}; "
                        f"сценарный прогноз: {_format_optional_number(ml_result.get('heuristic_f1_score'))}"
                    ),
                },
            ]
        )

    dissertation_points: List[str] = []
    if ml_result.get('is_ready'):
        folds = int(overview.get('folds') or 0)
        min_train_rows = int(overview.get('min_train_rows') or 0)
        dissertation_points.append(
            f"Валидация выполнена по схеме rolling-origin backtesting: {folds} одношаговых окон, минимальное обучающее окно {min_train_rows} дней."
        )
        dissertation_points.append(
            f"Сезонная базовая модель на тех же окнах дала MAE {_format_optional_number(ml_result.get('baseline_count_mae'))}, RMSE {_format_optional_number(ml_result.get('baseline_count_rmse'))} и SMAPE {_format_optional_percent(ml_result.get('baseline_count_smape'))}."
        )
        dissertation_points.append(
            f"Текущий сценарный эвристический прогноз показал MAE {_format_optional_number(ml_result.get('heuristic_count_mae'))}, RMSE {_format_optional_number(ml_result.get('heuristic_count_rmse'))} и SMAPE {_format_optional_percent(ml_result.get('heuristic_count_smape'))}."
        )
        dissertation_points.append(
            f"Лучшая обучаемая count-модель ({ml_result.get('count_model_label') or 'модель'}) показала MAE {_format_optional_number(ml_result.get('count_mae'))}, RMSE {_format_optional_number(ml_result.get('count_rmse'))} и SMAPE {_format_optional_percent(ml_result.get('count_smape'))}; изменение MAE относительно сезонной базы составило {_format_signed_percent(ml_result.get('count_vs_baseline_delta')) if ml_result.get('count_vs_baseline_delta') is not None else '—'}."
        )
        if ml_result.get('event_backtest_available'):
            dissertation_points.append(
                f"Для события «пожар / нет пожара» рабочий метод ({ml_result.get('selected_event_model_label') or 'метод'}) дал ROC-AUC {_format_optional_number(ml_result.get('roc_auc'))}, F1 {_format_optional_number(ml_result.get('f1_score'))} и показатель Брайера {_format_optional_number(ml_result.get('brier_score'))}; сценарная эвристика на тех же окнах дала ROC-AUC {_format_optional_number(ml_result.get('heuristic_roc_auc'))}, F1 {_format_optional_number(ml_result.get('heuristic_f1_score'))} и показатель Брайера {_format_optional_number(ml_result.get('heuristic_brier_score'))}."
            )
        else:
            dissertation_points.append(
                'Для события «пожар / нет пожара» история пока не дала достаточно окон, чтобы корректно сравнить вероятностные методы.'
            )
    else:
        dissertation_points.append('Качество ML-блока пока не подтверждено: истории недостаточно для корректной проверки на истории.')

    methodology_items = [
        {
            'label': 'Схема валидации',
            'value': ml_result.get('backtest_method_label') or 'Проверка на истории не выполнена',
            'meta': 'каждое окно обучается только на прошлом',
        },
        {
            'label': 'Минимум обучающего окна',
            'value': _format_integer(overview.get('min_train_rows') or 0),
            'meta': 'дней истории на одно окно',
        },
        {
            'label': 'Правило выбора',
            'value': str(overview.get('selection_rule') or 'Минимум девиации Пуассона, затем MAE'),
            'meta': 'по результатам проверки на истории',
        },
        {
            'label': 'Порог классификации',
            'value': _format_optional_number(overview.get('classification_threshold')),
            'meta': 'используется только для F1',
        },
    ]

    return {
        'ready': bool(ml_result.get('is_ready')),
        'title': 'Оценка качества ML-блока',
        'subtitle': 'На одной и той же истории сравниваются сезонная база, текущий сценарный прогноз и обучаемые модели для счетных данных пожаров.',
        'methodology_items': methodology_items,
        'metric_cards': metric_cards,
        'count_table': {
            'title': 'Сравнение по числу пожаров',
            'rows': count_rows,
            'empty_message': 'Сравнение сезонной базы, сценарного прогноза и обучаемых моделей появится после проверки на истории.',
        },
        'event_table': {
            'title': 'Сравнение по вероятности события пожара',
            'rows': event_rows,
            'empty_message': 'Недостаточно окон для сравнения вероятности события пожара.',
        },
        'dissertation_points': dissertation_points,
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
        notes.append('Основная ML-задача сформулирована как прогноз ожидаемого числа пожаров в день, а не как псевдовероятность из регрессии по числу пожаров.')
        notes.append('Панель качества теперь напрямую сравнивает сезонную базу, текущий сценарный прогноз и обучаемые модели по MAE, RMSE, SMAPE и девиации Пуассона.')
        notes.append('Скользящая проверка на истории гарантирует, что каждое окно использует только прошлую историю и не подглядывает в будущее.')
        notes.append('Если по качеству несколько моделей близки, блок отдает приоритет более объяснимой регрессии Пуассона и сохраняет сценарный прогноз как внешний бенчмарк.')
        notes.append('Интервалы неопределенности построены вокруг ожидаемого числа пожаров с поправкой на эмпирическую дисперсию ряда.')
        if ml_result.get('event_backtest_available'):
            notes.append('Для события «пожар / нет пожара» дополнительно считаются ROC-AUC, F1 и показатель Брайера для сезонной базы, сценарной эвристики и, если хватает истории, отдельного логистического классификатора.')
        else:
            notes.append('Для события «пожар / нет пожара» пока недостаточно одношаговых окон, поэтому блок не делает сильных выводов по вероятностной задаче.')
        if not ml_result.get('classifier_ready'):
            notes.append('Отдельный классификатор события пока не используется для будущих дат, поэтому интерфейс опирается на ожидаемое число пожаров и риск-индекс.')
    else:
        notes.append('Пока данных недостаточно для корректной проверки на истории, поэтому ML-блок остаётся в безопасном режиме без прогноза.')

    if len(daily_history) < 60:
        notes.append('Истории меньше 60 дней: для корректной ML-валидации этого обычно недостаточно.')

    return notes