from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.services.forecasting.core import _build_feature_cards
from app.services.forecasting.data import (
    _build_daily_history_sql,
    _build_forecasting_table_options,
    _build_option_catalog_sql,
    _collect_forecasting_metadata,
    _resolve_forecasting_selection,
    _selected_source_tables,
    clear_forecasting_sql_cache,
)
from app.services.forecasting.utils import (
    _format_datetime,
    _format_float_for_input,
    _history_window_label,
    _parse_float,
    _parse_forecast_days,
    _parse_history_window,
    _resolve_option_value,
)

from .constants import FORECAST_DAY_OPTIONS, HISTORY_WINDOW_OPTIONS, MODEL_NAME, _CACHE_LIMIT, _ML_CACHE
from .presentation import (
    _build_forecast_chart,
    _build_importance_chart,
    _build_notes,
    _build_quality_assessment,
    _build_summary,
    _empty_light_chart,
)
from .training import _empty_ml_result, _train_ml_model

ML_PREDICTIVE_BLOCK_DESCRIPTION = (
    'Это отдельный ML-блок для временного ряда пожаров. '
    'Основная модель прогнозирует ожидаемое число пожаров по дням, а не превращает прогноз по числу пожаров в псевдовероятность. '
    'Панель качества напрямую сравнивает сезонную базу, текущий сценарный прогноз и обучаемые count-модели через rolling-origin backtesting. '
    'Если данных хватает, дополнительно показывается отдельная вероятность события «пожар / нет пожара» из бинарного классификатора. '
    'Все проверки обучаются только на прошлом и не используют будущие наблюдения.'
)


def get_ml_model_page_context(
    table_name: str = 'all',
    cause: str = 'all',
    object_category: str = 'all',
    temperature: str = '',
    forecast_days: str = '14',
    history_window: str = 'all',
) -> Dict[str, Any]:
    try:
        initial_data = get_ml_model_data(
            table_name=table_name,
            cause=cause,
            object_category=object_category,
            temperature=temperature,
            forecast_days=forecast_days,
            history_window=history_window,
        )
    except Exception as exc:
        table_options = _build_forecasting_table_options()
        selected_table = _resolve_forecasting_selection(table_options, table_name)
        days_ahead = _parse_forecast_days(forecast_days)
        selected_history_window = _parse_history_window(history_window)
        initial_data = _empty_ml_model_data(
            table_options,
            selected_table,
            days_ahead,
            temperature,
            selected_history_window,
        )
        initial_data['notes'].append('ML-страница открыта в безопасном режиме: обучение временно отключено из-за внутренней ошибки.')
        initial_data['notes'].append(f'Техническая причина: {exc}')
        initial_data['model_description'] = (
            'ML-блок открыт без обучения, чтобы интерфейс оставался доступен даже при проблеме в данных или признаках.'
        )
    return {
        'generated_at': _format_datetime(datetime.now()),
        'initial_data': initial_data,
        'plotly_js': '',
        'has_data': bool(initial_data['filters']['available_tables']),
    }


def get_ml_model_shell_context(
    table_name: str = 'all',
    cause: str = 'all',
    object_category: str = 'all',
    temperature: str = '',
    forecast_days: str = '14',
    history_window: str = 'all',
) -> Dict[str, Any]:
    table_options = _build_forecasting_table_options()
    selected_table = _resolve_forecasting_selection(table_options, table_name)
    days_ahead = _parse_forecast_days(forecast_days)
    selected_history_window = _parse_history_window(history_window)
    initial_data = _empty_ml_model_data(
        table_options,
        selected_table,
        days_ahead,
        temperature,
        selected_history_window,
    )
    initial_data['bootstrap_mode'] = 'deferred'
    initial_data['filters']['cause'] = cause or 'all'
    initial_data['filters']['object_category'] = object_category or 'all'
    return {
        'generated_at': _format_datetime(datetime.now()),
        'initial_data': initial_data,
        'plotly_js': '',
        'has_data': bool(initial_data['filters']['available_tables']),
    }


def get_ml_model_data(
    table_name: str = 'all',
    cause: str = 'all',
    object_category: str = 'all',
    temperature: str = '',
    forecast_days: str = '14',
    history_window: str = 'all',
) -> Dict[str, Any]:
    table_options = _build_forecasting_table_options()
    selected_table = _resolve_forecasting_selection(table_options, table_name)
    source_tables = _selected_source_tables(table_options, selected_table)
    days_ahead = _parse_forecast_days(forecast_days)
    selected_history_window = _parse_history_window(history_window)
    scenario_temperature = _parse_float(temperature)
    cache_key = (
        selected_table,
        cause or 'all',
        object_category or 'all',
        _format_float_for_input(scenario_temperature) if scenario_temperature is not None else '',
        days_ahead,
        selected_history_window,
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    base = _empty_ml_model_data(table_options, selected_table, days_ahead, temperature, selected_history_window)
    if not source_tables:
        base['notes'].append('Нет доступных таблиц для ML-модели.')
        return _cache_store(cache_key, base)

    metadata_items, preload_notes = _collect_forecasting_metadata(source_tables)
    option_catalog = _build_option_catalog_sql(
        source_tables,
        history_window=selected_history_window,
        metadata_items=metadata_items,
    )
    selected_cause = _resolve_option_value(option_catalog['causes'], cause)
    selected_object_category = _resolve_option_value(option_catalog['object_categories'], object_category)

    daily_history = _build_daily_history_sql(
        source_tables,
        history_window=selected_history_window,
        cause=selected_cause,
        object_category=selected_object_category,
        metadata_items=metadata_items,
    )
    filtered_records_count = int(sum(int(item.get('count') or 0) for item in daily_history))
    ml_result = _train_ml_model(daily_history, days_ahead, scenario_temperature)
    summary = _build_summary(
        selected_table=selected_table,
        selected_cause=selected_cause,
        selected_object_category=selected_object_category,
        daily_history=daily_history,
        filtered_records_count=filtered_records_count,
        ml_result=ml_result,
        history_window=selected_history_window,
        scenario_temperature=scenario_temperature,
    )

    payload = {
        'generated_at': _format_datetime(datetime.now()),
        'has_data': filtered_records_count > 0,
        'model_description': ML_PREDICTIVE_BLOCK_DESCRIPTION,
        'summary': summary,
        'quality_assessment': _build_quality_assessment(ml_result),
        'features': _build_feature_cards(metadata_items),
        'charts': {
            'forecast': _build_forecast_chart(daily_history, ml_result),
            'importance': _build_importance_chart(ml_result.get('feature_importance', [])),
        },
        'forecast_rows': ml_result.get('forecast_rows', []),
        'feature_importance': ml_result.get('feature_importance', []),
        'notes': _build_notes(preload_notes, metadata_items, filtered_records_count, daily_history, ml_result, scenario_temperature, source_tables),
        'filters': {
            'table_name': selected_table,
            'cause': selected_cause,
            'object_category': selected_object_category,
            'temperature': temperature if scenario_temperature is None else _format_float_for_input(scenario_temperature),
            'forecast_days': str(days_ahead),
            'history_window': selected_history_window,
            'available_tables': table_options,
            'available_causes': option_catalog['causes'],
            'available_object_categories': option_catalog['object_categories'],
            'available_forecast_days': [{'value': str(item), 'label': f'{item} дней'} for item in FORECAST_DAY_OPTIONS],
            'available_history_windows': HISTORY_WINDOW_OPTIONS,
        },
    }
    return _cache_store(cache_key, payload)

def _cache_get(cache_key: Tuple[str, str, str, str, int, str]) -> Optional[Dict[str, Any]]:
    cached = _ML_CACHE.get(cache_key)
    if cached is None:
        return None
    _ML_CACHE.move_to_end(cache_key)
    return deepcopy(cached)


def _cache_store(cache_key: Tuple[str, str, str, str, int, str], payload: Dict[str, Any]) -> Dict[str, Any]:
    _ML_CACHE[cache_key] = deepcopy(payload)
    _ML_CACHE.move_to_end(cache_key)
    while len(_ML_CACHE) > _CACHE_LIMIT:
        _ML_CACHE.popitem(last=False)
    return deepcopy(payload)



def clear_ml_model_cache() -> None:
    _ML_CACHE.clear()
    clear_forecasting_sql_cache()


def _empty_ml_model_data(
    table_options: List[Dict[str, str]],
    selected_table: str,
    forecast_days: int,
    temperature: str,
    history_window: str,
) -> Dict[str, Any]:
    empty_result = _empty_ml_result('Недостаточно данных для обучения модели.')
    return {
        'generated_at': _format_datetime(datetime.now()),
        'has_data': False,
        'model_description': '',
        'summary': {
            'selected_table_label': 'Все таблицы' if selected_table == 'all' else (selected_table or 'Нет таблицы'),
            'slice_label': 'Все пожары',
            'history_period_label': 'Нет данных',
            'history_window_label': _history_window_label(history_window),
            'model_label': MODEL_NAME,
            'count_model_label': 'Регрессия Пуассона',
            'event_model_label': 'Не обучен',
            'event_backtest_model_label': 'Не показан',
            'backtest_method_label': 'Проверка на истории не выполнена',
            'fires_count_display': '0',
            'history_days_display': '0',
            'forecast_days_display': str(forecast_days),
            'last_observed_date': '-',
            'count_mae_display': '-',
            'count_rmse_display': '-',
            'count_smape_display': '—',
            'count_poisson_deviance_display': '-',
            'baseline_count_mae_display': '-',
            'baseline_count_rmse_display': '-',
            'baseline_count_smape_display': '—',
            'heuristic_count_mae_display': '-',
            'heuristic_count_rmse_display': '-',
            'heuristic_count_smape_display': '—',
            'heuristic_count_poisson_deviance_display': '-',
            'mae_vs_baseline_display': '-',
            'brier_display': '—',
            'baseline_brier_display': '—',
            'heuristic_brier_display': '—',
            'roc_auc_display': '—',
            'baseline_roc_auc_display': '—',
            'heuristic_roc_auc_display': '—',
            'f1_display': '—',
            'baseline_f1_display': '—',
            'heuristic_f1_display': '—',
            'log_loss_display': '—',
            'top_feature_label': '-',
            'temperature_scenario_display': temperature.strip() or 'Историческая температура',
            'predicted_total_display': '0',
            'average_expected_count_display': '0',
            'peak_expected_count_display': '0',
            'peak_expected_count_day_display': '-',
            'elevated_risk_days_display': '0',
            'average_event_probability_display': '—',
            'peak_event_probability_display': '—',
            'peak_event_probability_day_display': '-',
            'event_probability_enabled': False,
            'event_backtest_available': False,
        },
        'quality_assessment': _build_quality_assessment(empty_result),
        'features': [],
        'charts': {
            'forecast': _empty_light_chart('ML-прогноз ожидаемого числа пожаров', 'Недостаточно данных для обучения модели.'),
            'importance': _empty_light_chart('Важность признаков ML-блока', 'Модель пока не обучена.', kind='bars'),
        },
        'forecast_rows': [],
        'feature_importance': [],
        'notes': [],
        'filters': {
            'table_name': selected_table,
            'cause': 'all',
            'object_category': 'all',
            'temperature': temperature,
            'forecast_days': str(forecast_days),
            'history_window': history_window,
            'available_tables': table_options,
            'available_causes': [{'value': 'all', 'label': 'Все причины'}],
            'available_object_categories': [{'value': 'all', 'label': 'Все категории'}],
            'available_forecast_days': [{'value': str(item), 'label': f'{item} дней'} for item in FORECAST_DAY_OPTIONS],
            'available_history_windows': HISTORY_WINDOW_OPTIONS,
        },
    }
