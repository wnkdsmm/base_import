from __future__ import annotations

import math
from collections import OrderedDict
from copy import deepcopy
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

from app.services.forecasting_service import (
    _build_daily_history,
    _build_feature_cards,
    _build_forecasting_table_options,
    _build_option_catalog,
    _collect_forecasting_inputs,
    _empty_chart_bundle,
    _format_datetime,
    _format_float_for_input,
    _format_integer,
    _format_number,
    _format_period,
    _history_window_label,
    _parse_float,
    _parse_forecast_days,
    _parse_history_window,
    _resolve_option_value,
    _resolve_forecasting_selection,
    _selected_source_tables,
)

MODEL_NAME = 'Random Forest Regressor'
FORECAST_DAY_OPTIONS = [7, 14, 30]
HISTORY_WINDOW_OPTIONS = [
    {'value': 'all', 'label': 'Все годы'},
    {'value': 'recent_3', 'label': 'Последние 3 года'},
    {'value': 'recent_5', 'label': 'Последние 5 лет'},
]
FEATURE_LABELS = {
    'temp_value': 'Температура',
    'weekday': 'День недели',
    'month': 'Месяц',
    'lag_1': 'Пожары вчера',
    'lag_7': 'Пожары 7 дней назад',
    'lag_14': 'Пожары 14 дней назад',
    'rolling_7': 'Среднее за 7 дней',
    'rolling_28': 'Среднее за 28 дней',
    'trend_gap': 'Разница 7/28 дней',
}
_CACHE_LIMIT = 12
_ML_CACHE: 'OrderedDict[Tuple[str, str, str, str, int, str], Dict[str, Any]]' = OrderedDict()

_RF_PARAMS = {
    'n_estimators': 120,
    'max_depth': 8,
    'min_samples_leaf': 2,
    'random_state': 42,
    'n_jobs': -1,
}


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
        initial_data['notes'].append('ML-СЃС‚СЂР°РЅРёС†Р° РѕС‚РєСЂС‹С‚Р° РІ Р±РµР·РѕРїР°СЃРЅРѕРј СЂРµР¶РёРјРµ: РѕР±СѓС‡РµРЅРёРµ РјРѕРґРµР»Рё РІСЂРµРјРµРЅРЅРѕ РѕС‚РєР»СЋС‡РµРЅРѕ РёР·-Р·Р° РІРЅСѓС‚СЂРµРЅРЅРµР№ РѕС€РёР±РєРё.')
        initial_data['notes'].append(f'РўРµС…РЅРёС‡РµСЃРєР°СЏ РїСЂРёС‡РёРЅР°: {exc}')
        initial_data['model_description'] = (
            'РЎС‚СЂР°РЅРёС†Р° РѕС‚РєСЂС‹С‚Р° Р±РµР· РѕР±СѓС‡РµРЅРёСЏ, С‡С‚РѕР±С‹ РёРЅС‚РµСЂС„РµР№СЃ РѕСЃС‚Р°РІР°Р»СЃСЏ РґРѕСЃС‚СѓРїРµРЅ '
            'РґР°Р¶Рµ РїСЂРё РѕС€РёР±РєРµ РІ РґР°РЅРЅС‹С… РёР»Рё РїСЂРёР·РЅР°РєР°С….'
        )
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

    records, metadata_items, preload_notes = _collect_forecasting_inputs(source_tables)
    scoped_records = _apply_history_window(records, selected_history_window)
    option_catalog = _build_option_catalog(scoped_records)
    selected_cause = _resolve_option_value(option_catalog['causes'], cause)
    selected_object_category = _resolve_option_value(option_catalog['object_categories'], object_category)

    filtered_records = [
        item for item in scoped_records
        if (selected_cause == 'all' or item['cause'] == selected_cause)
        and (selected_object_category == 'all' or item['object_category'] == selected_object_category)
    ]

    daily_history = _build_daily_history(filtered_records)
    ml_result = _train_ml_model(daily_history, days_ahead, scenario_temperature)
    summary = _build_summary(
        selected_table=selected_table,
        selected_cause=selected_cause,
        selected_object_category=selected_object_category,
        daily_history=daily_history,
        filtered_records=filtered_records,
        ml_result=ml_result,
        history_window=selected_history_window,
        scenario_temperature=scenario_temperature,
    )

    payload = {
        'generated_at': _format_datetime(datetime.now()),
        'has_data': bool(filtered_records),
        'model_description': ('ML-модель оценивает риск по дневной истории пожаров и показывает результат в виде вероятности возникновения пожара на каждую дату.'),
        'summary': summary,
        'features': _build_feature_cards(metadata_items),
        'charts': {
            'forecast': _build_forecast_chart(daily_history, ml_result),
            'importance': _build_importance_chart(ml_result.get('feature_importance', [])),
        },
        'forecast_rows': ml_result.get('forecast_rows', []),
        'feature_importance': ml_result.get('feature_importance', []),
        'notes': _build_notes(preload_notes, metadata_items, filtered_records, daily_history, ml_result, scenario_temperature, source_tables),
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


def _empty_ml_model_data(table_options: List[Dict[str, str]], selected_table: str, forecast_days: int, temperature: str, history_window: str) -> Dict[str, Any]:
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
            'fires_count_display': '0',
            'history_days_display': '0',
            'forecast_days_display': str(forecast_days),
            'last_observed_date': '-',
            'mae_display': '-',
            'rmse_display': '-',
            'top_feature_label': '-',
            'temperature_scenario_display': temperature.strip() or 'Историческая температура',
            'predicted_total_display': '0',
            'average_probability_display': '0%',
            'peak_probability_display': '0%',
            'peak_probability_day_display': '-',
            'high_risk_days_display': '0',
        },
        'features': [],
        'charts': {
            'forecast': _empty_light_chart('Вероятность возникновения пожара (ML)', 'Недостаточно данных для обучения модели.'),
            'importance': _empty_light_chart('Важность признаков ML-модели', 'Модель пока не обучена.'),
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


def _empty_light_chart(title: str, empty_message: str, kind: str = 'line') -> Dict[str, Any]:
    return {'title': title, 'kind': kind, 'empty_message': empty_message}


def _apply_history_window(records: List[Dict[str, Any]], history_window: str) -> List[Dict[str, Any]]:
    if not records or history_window == 'all':
        return records
    latest_year = max(item['date'].year for item in records)
    min_year = latest_year - 2 if history_window == 'recent_3' else latest_year - 4
    return [item for item in records if item['date'].year >= min_year]


def _train_ml_model(daily_history: List[Dict[str, Any]], forecast_days: int, scenario_temperature: Optional[float]) -> Dict[str, Any]:
    if len(daily_history) < 45 or forecast_days <= 0:
        return _empty_ml_result('Недостаточно истории для обучения ML-модели.')

    history_tail = daily_history[-900:]
    frame = pd.DataFrame({
        'date': pd.to_datetime([item['date'] for item in history_tail]),
        'count': [float(item['count']) for item in history_tail],
        'avg_temperature': [item.get('avg_temperature') for item in history_tail],
    }).sort_values('date').reset_index(drop=True)
    frame['temp_value'] = frame['avg_temperature']
    frame['temp_value'] = frame.groupby(frame['date'].dt.month)['temp_value'].transform(lambda series: series.fillna(series.mean()))
    frame['temp_value'] = frame['temp_value'].fillna(frame['temp_value'].mean()).fillna(0.0)

    featured = _feature_frame(frame)
    feature_columns = ['temp_value', 'weekday', 'month', 'lag_1', 'lag_7', 'lag_14', 'rolling_7', 'rolling_28', 'trend_gap']
    dataset = featured.dropna(subset=feature_columns + ['count']).copy()
    if len(dataset) < 30:
        return _empty_ml_result('После формирования признаков наблюдений осталось слишком мало.')

    test_size = min(max(10, len(dataset) // 6), 30)
    if len(dataset) - test_size < 20:
        test_size = max(7, len(dataset) // 4)
    train = dataset.iloc[:-test_size]
    test = dataset.iloc[-test_size:]
    if train.empty or test.empty:
        return _empty_ml_result('Недостаточно данных для обучения и контроля качества.')

    model = _build_regressor()
    model.fit(train[feature_columns], train['count'])
    test_predictions = model.predict(test[feature_columns])
    actual_values = test['count'].to_numpy(dtype=float)
    residuals = test_predictions - actual_values
    abs_residuals = np.abs(residuals)
    mae = float(np.mean(np.abs(residuals)))
    rmse = float(math.sqrt(np.mean(residuals ** 2)))
    residual_margin = max(0.35, float(np.quantile(abs_residuals, 0.65)))

    final_model = _build_regressor()
    final_model.fit(dataset[feature_columns], dataset['count'])

    monthly_temp = frame.groupby(frame['date'].dt.month)['temp_value'].mean().to_dict()
    overall_temp = float(frame['temp_value'].mean()) if not frame.empty else 0.0
    history_counts = list(frame['count'].astype(float))
    last_date = frame['date'].dt.date.iloc[-1]
    forecast_rows: List[Dict[str, Any]] = []

    for step in range(1, forecast_days + 1):
        target_date = last_date + timedelta(days=step)
        temp_value = scenario_temperature if scenario_temperature is not None else float(monthly_temp.get(target_date.month, overall_temp))
        row_frame = pd.DataFrame([_future_feature_row(history_counts, target_date, temp_value)], columns=feature_columns)

        point_prediction = max(0.0, float(final_model.predict(row_frame)[0]))
        tree_predictions = np.array([float(tree.predict(row_frame)[0]) for tree in final_model.estimators_], dtype=float)
        tree_lower = float(np.quantile(tree_predictions, 0.25))
        tree_upper = float(np.quantile(tree_predictions, 0.75))
        lower_bound = max(0.0, min(tree_lower, point_prediction - residual_margin))
        upper_bound = max(lower_bound, max(tree_upper, point_prediction + residual_margin))

        fire_probability = _probability_from_count(point_prediction)
        lower_probability = _probability_from_count(lower_bound)
        upper_probability = _probability_from_count(upper_bound)

        # Keep the interval informative for humans: not too wide and not too narrow.
        raw_half_width = max(0.0, (upper_probability - lower_probability) / 2.0)
        half_width = min(0.12, max(0.03, raw_half_width))
        lower_probability = max(0.0, fire_probability - half_width)
        upper_probability = min(0.995, fire_probability + half_width)
        risk_level_label, risk_level_tone = _risk_band(fire_probability)

        forecast_rows.append({
            'date': target_date.isoformat(),
            'date_display': target_date.strftime('%d.%m.%Y'),
            'forecast_value': round(point_prediction, 2),
            'forecast_value_display': _format_integer(point_prediction),
            'lower_bound': round(lower_bound, 2),
            'lower_bound_display': _format_integer(lower_bound),
            'upper_bound': round(upper_bound, 2),
            'upper_bound_display': _format_integer(upper_bound),
            'temperature_display': _format_number(temp_value),
            'fire_probability': round(fire_probability, 4),
            'lower_probability': round(lower_probability, 4),
            'upper_probability': round(upper_probability, 4),
            'fire_probability_display': _format_probability(fire_probability),
            'fire_probability_range_display': f'{_format_probability(lower_probability)} - {_format_probability(upper_probability)}',
            'risk_level_label': risk_level_label,
            'risk_level_tone': risk_level_tone,
        })
        history_counts.append(point_prediction)

    feature_importance = [
        {
            'feature': name,
            'label': FEATURE_LABELS.get(name, name),
            'importance': round(float(score), 4),
            'importance_display': _format_number(float(score) * 100),
        }
        for name, score in sorted(zip(feature_columns, final_model.feature_importances_), key=lambda item: item[1], reverse=True)
    ]

    backtest_rows = [
        {
            'date': item.date().isoformat(),
            'actual': round(float(actual), 2),
            'predicted': round(float(predicted), 2),
            'actual_probability': round(_probability_from_count(float(actual)) * 100.0, 2),
            'predicted_probability': round(_probability_from_count(float(predicted)) * 100.0, 2),
        }
        for item, actual, predicted in zip(test['date'], actual_values, test_predictions)
    ]

    return {
        'is_ready': True,
        'forecast_rows': forecast_rows,
        'feature_importance': feature_importance,
        'backtest_rows': backtest_rows,
        'mae': mae,
        'rmse': rmse,
        'top_feature_label': feature_importance[0]['label'] if feature_importance else '-',
        'message': '',
    }


def _empty_ml_result(message: str) -> Dict[str, Any]:
    return {
        'is_ready': False,
        'forecast_rows': [],
        'feature_importance': [],
        'backtest_rows': [],
        'mae': None,
        'rmse': None,
        'top_feature_label': '-',
        'message': message,
    }


def _feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result['weekday'] = result['date'].dt.weekday.astype(float)
    result['month'] = result['date'].dt.month.astype(float)
    for lag in (1, 7, 14):
        result[f'lag_{lag}'] = result['count'].shift(lag)
    result['rolling_7'] = result['count'].shift(1).rolling(7).mean()
    result['rolling_28'] = result['count'].shift(1).rolling(28).mean()
    result['trend_gap'] = result['rolling_7'] - result['rolling_28']
    return result


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


def _build_regressor() -> RandomForestRegressor:
    return RandomForestRegressor(**_RF_PARAMS)


def _probability_from_count(value: float) -> float:
    safe_value = max(0.0, float(value))
    return max(0.0, min(0.995, 1.0 - math.exp(-safe_value)))


def _format_percent(value: float) -> str:
    rounded = round(float(value), 1)
    if abs(rounded - round(rounded)) < 1e-9:
        return f'{int(round(rounded))}%'
    return f"{str(rounded).replace('.', ',')}%"


def _format_probability(value: float) -> str:
    return _format_percent(value * 100.0)


def _risk_band(probability: float) -> Tuple[str, str]:
    if probability >= 0.75:
        return 'Очень высокий', 'critical'
    if probability >= 0.50:
        return 'Высокий', 'high'
    if probability >= 0.30:
        return 'Средний', 'medium'
    if probability >= 0.15:
        return 'Ниже среднего', 'low'
    return 'Низкий', 'minimal'


def _build_forecast_chart(daily_history: List[Dict[str, Any]], ml_result: Dict[str, Any]) -> Dict[str, Any]:
    title = 'Вероятность возникновения пожара (ML)'
    if not daily_history or not ml_result.get('is_ready'):
        return _empty_light_chart(title, ml_result.get('message') or 'Недостаточно данных для построения прогноза.')

    history_tail = daily_history[-120:]
    history_points = [
        {
            'x': item['date'].isoformat(),
            'y': round(_probability_from_count(float(item['count'])) * 100.0, 2),
        }
        for item in history_tail
    ]
    backtest_actual = [
        {'x': item['date'], 'y': round(float(item.get('actual_probability', 0.0)), 2)}
        for item in ml_result.get('backtest_rows', [])
    ]
    backtest_predicted = [
        {'x': item['date'], 'y': round(float(item.get('predicted_probability', 0.0)), 2)}
        for item in ml_result.get('backtest_rows', [])
    ]
    forecast_points = [
        {
            'x': item['date'],
            'y': round(float(item.get('fire_probability', 0.0)) * 100.0, 2),
        }
        for item in ml_result.get('forecast_rows', [])
    ]
    forecast_band = [
        {
            'x': item['date'],
            'low': round(float(item.get('lower_probability', _probability_from_count(float(item.get('lower_bound', 0.0))))) * 100.0, 2),
            'high': round(float(item.get('upper_probability', _probability_from_count(float(item.get('upper_bound', 0.0))))) * 100.0, 2),
        }
        for item in ml_result.get('forecast_rows', [])
    ]

    return {
        'title': title,
        'kind': 'line',
        'empty_message': '',
        'value_format': 'percent',
        'legend': [
            {'label': 'История', 'color': '#F97316'},
            {'label': 'Backtest', 'color': '#64748B'},
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
    title = 'Важность признаков ML-модели'
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


def _build_summary(selected_table: str, selected_cause: str, selected_object_category: str, daily_history: List[Dict[str, Any]], filtered_records: List[Dict[str, Any]], ml_result: Dict[str, Any], history_window: str, scenario_temperature: Optional[float]) -> Dict[str, str]:
    history_dates = [item['date'] for item in daily_history]
    slice_parts = []
    if selected_cause != 'all':
        slice_parts.append(f'Причина: {selected_cause}')
    if selected_object_category != 'all':
        slice_parts.append(f'Категория: {selected_object_category}')

    forecast_rows = ml_result.get('forecast_rows', [])
    average_probability = (
        float(np.mean([float(item.get('fire_probability', 0.0)) for item in forecast_rows]))
        if forecast_rows
        else 0.0
    )
    peak_row = max(forecast_rows, key=lambda item: float(item.get('fire_probability', 0.0))) if forecast_rows else None
    high_risk_days = sum(1 for item in forecast_rows if float(item.get('fire_probability', 0.0)) >= 0.5)

    return {
        'selected_table_label': 'Все таблицы' if selected_table == 'all' else (selected_table or 'Нет таблицы'),
        'slice_label': ' | '.join(slice_parts) if slice_parts else 'Все пожары выбранной истории',
        'history_period_label': _format_period(history_dates),
        'history_window_label': _history_window_label(history_window),
        'model_label': MODEL_NAME,
        'fires_count_display': _format_integer(len(filtered_records)),
        'history_days_display': _format_integer(len(daily_history)),
        'forecast_days_display': _format_integer(len(forecast_rows)),
        'last_observed_date': history_dates[-1].strftime('%d.%m.%Y') if history_dates else '-',
        'mae_display': _format_number(ml_result['mae']) if ml_result.get('mae') is not None else '-',
        'rmse_display': _format_number(ml_result['rmse']) if ml_result.get('rmse') is not None else '-',
        'top_feature_label': ml_result.get('top_feature_label') or '-',
        'temperature_scenario_display': _format_number(scenario_temperature) if scenario_temperature is not None else 'Историческая температура',
        'predicted_total_display': _format_integer(sum(float(item.get('forecast_value', 0.0)) for item in forecast_rows)),
        'average_probability_display': _format_probability(average_probability),
        'peak_probability_display': peak_row['fire_probability_display'] if peak_row else '0%',
        'peak_probability_day_display': peak_row['date_display'] if peak_row else '-',
        'high_risk_days_display': _format_integer(high_risk_days),
    }


def _build_notes(preload_notes: List[str], metadata_items: List[Dict[str, Any]], filtered_records: List[Dict[str, Any]], daily_history: List[Dict[str, Any]], ml_result: Dict[str, Any], scenario_temperature: Optional[float], source_tables: List[str]) -> List[str]:
    notes = list(preload_notes)
    if len(source_tables) > 1:
        notes.append(f'ML-модель обучается сразу по {len(source_tables)} таблицам.')
    if not filtered_records:
        notes.append('После выбранных фильтров нет исторических пожаров для обучения модели.')
    if len(daily_history) < 45:
        notes.append('Истории меньше 45 дней, поэтому модель не обучается.')
    if ml_result.get('message'):
        notes.append(ml_result['message'])
    if scenario_temperature is not None and not any(item['resolved_columns'].get('temperature') for item in metadata_items):
        notes.append('Температура задана вручную, но температурная колонка в таблицах не найдена.')
    notes.append('Вероятность показывает шанс хотя бы одного пожара в выбранный день.')
    notes.append('Графики собраны на легком SVG, поэтому страница открывается быстрее.')
    return notes


