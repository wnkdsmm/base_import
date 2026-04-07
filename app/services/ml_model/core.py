from __future__ import annotations

from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from app.perf import current_perf_trace, profiled
from app.services.forecasting.data import (
    _build_daily_history_sql,
    _build_forecasting_table_options,
    _build_option_catalog_sql,
    _collect_forecasting_metadata,
    _count_forecasting_records_sql,
    _resolve_forecasting_selection,
    _selected_source_table_notes,
    _selected_source_tables,
    _temperature_quality_from_daily_history,
    clear_forecasting_sql_cache,
)
from app.services.forecasting.utils import (
    _format_datetime,
    _format_float_for_input,
    _parse_float,
    _parse_forecast_days,
    _parse_history_window,
    _resolve_option_value,
)
from config.db import engine

from .constants import ML_CACHE_SCHEMA_VERSION, _CACHE_LIMIT, _ML_CACHE
from .payloads import _build_ml_payload, _compact_ui_notes, _empty_ml_model_data
from .runtime import MlProgressCallback, _emit_progress
from .training import _train_ml_model


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
        initial_data['notes'] = _compact_ui_notes(initial_data['notes'])
        initial_data['model_description'] = (
            'ML-прогноз временно открыт без обучения, чтобы экран оставался доступен даже при проблеме в данных или признаках. '
            'Его задача не меняется: оценить ожидаемое число пожаров по датам, а не ранжировать территории.'
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
    prefer_cached: bool = False,
) -> Dict[str, Any]:
    request_state = _build_ml_request_state(
        table_name=table_name,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
    )
    cached = _cache_get(request_state['cache_key']) if prefer_cached else None
    if cached is not None:
        return {
            'generated_at': _format_datetime(datetime.now()),
            'initial_data': cached,
            'plotly_js': '',
            'has_data': bool(cached['filters']['available_tables']),
        }

    table_options = request_state['table_options']
    selected_table = request_state['selected_table']
    days_ahead = request_state['days_ahead']
    selected_history_window = request_state['selected_history_window']
    initial_data = _empty_ml_model_data(
        table_options,
        selected_table,
        days_ahead,
        temperature,
        selected_history_window,
    )
    initial_data['bootstrap_mode'] = 'deferred'
    initial_data['charts']['importance']['empty_message'] = (
        'Собираем драйверы прогноза: блок заполнится после фонового расчёта.'
    )
    initial_data['notes'].extend(request_state['source_table_notes'])
    initial_data['notes'] = _compact_ui_notes(initial_data['notes'])
    initial_data['filters']['cause'] = cause or 'all'
    initial_data['filters']['object_category'] = object_category or 'all'
    return {
        'generated_at': _format_datetime(datetime.now()),
        'initial_data': initial_data,
        'plotly_js': '',
        'has_data': bool(initial_data['filters']['available_tables']),
    }


@profiled('ml_model', engine=engine)
def get_ml_model_data(
    table_name: str = 'all',
    cause: str = 'all',
    object_category: str = 'all',
    temperature: str = '',
    forecast_days: str = '14',
    history_window: str = 'all',
    progress_callback: MlProgressCallback = None,
) -> Dict[str, Any]:
    perf = current_perf_trace()
    request_state = _build_ml_request_state(
        table_name=table_name,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
    )
    table_options = request_state['table_options']
    selected_table = request_state['selected_table']
    source_tables = request_state['source_tables']
    source_table_notes = request_state['source_table_notes']
    days_ahead = request_state['days_ahead']
    selected_history_window = request_state['selected_history_window']
    scenario_temperature = request_state['scenario_temperature']
    cache_key = request_state['cache_key']
    if perf is not None:
        perf.update(
            requested_table=table_name,
            requested_cause=cause,
            requested_object_category=object_category,
            selected_table=selected_table,
            source_tables=len(source_tables),
            forecast_horizon_days=days_ahead,
            history_window=selected_history_window,
        )
    cached = _cache_get(cache_key)
    if cached is not None:
        if perf is not None:
            perf.update(cache_hit=True, payload_has_data=bool(cached.get('has_data')))
        _emit_progress(progress_callback, 'ml_model.completed', 'Результат ML-анализа взят из кэша.')
        return cached

    if perf is not None:
        perf.update(cache_hit=False)
    base = _empty_ml_model_data(table_options, selected_table, days_ahead, temperature, selected_history_window)
    if not source_tables:
        base['notes'].extend(source_table_notes)
        base['notes'].append('Нет доступных таблиц для ML-модели.')
        base['notes'] = _compact_ui_notes(base['notes'])
        if perf is not None:
            perf.update(payload_has_data=False, payload_notes=len(base['notes']))
        return _cache_store(cache_key, base)

    _emit_progress(progress_callback, 'ml_model.running', 'Собираем SQL-агрегаты и доступные фильтры для ML-прогноза.')
    filter_prep_context = perf.span('filter_prep') if perf is not None else nullcontext()
    with filter_prep_context:
        metadata_items, preload_notes = _collect_forecasting_metadata(source_tables)
        option_catalog = _build_option_catalog_sql(
            source_tables,
            history_window=selected_history_window,
            metadata_items=metadata_items,
        )
        selected_cause = _resolve_option_value(option_catalog['causes'], cause)
        selected_object_category = _resolve_option_value(option_catalog['object_categories'], object_category)
        if perf is not None:
            perf.update(
                metadata_tables=len(metadata_items),
                available_causes=len(option_catalog['causes']),
                available_object_categories=len(option_catalog['object_categories']),
            )

    aggregation_context = perf.span('aggregation') if perf is not None else nullcontext()
    with aggregation_context:
        daily_history = _build_daily_history_sql(
            source_tables,
            history_window=selected_history_window,
            cause=selected_cause,
            object_category=selected_object_category,
            metadata_items=metadata_items,
        )
        filtered_records_count = _count_forecasting_records_sql(
            source_tables,
            history_window=selected_history_window,
            cause=selected_cause,
            object_category=selected_object_category,
            metadata_items=metadata_items,
        )
        if perf is not None:
            perf.update(input_rows=filtered_records_count, history_days=len(daily_history))
    _emit_progress(
        progress_callback,
        'ml_model.running',
        f"Подготовлен дневной ряд: {len(daily_history)} дней истории, {filtered_records_count} пожаров после фильтров.",
    )
    model_training_context = perf.span('model_training') if perf is not None else nullcontext()
    with model_training_context:
        ml_result = _train_ml_model(
            daily_history,
            days_ahead,
            scenario_temperature,
            progress_callback=progress_callback,
        )
        temperature_quality = _temperature_quality_from_daily_history(daily_history)
    _emit_progress(progress_callback, 'ml_model.running', 'Формируем итоговые метрики, графики и таблицы ML-прогноза.')
    payload_render_context = perf.span('payload_render') if perf is not None else nullcontext()
    with payload_render_context:
        payload = _build_ml_payload(
            table_options=table_options,
            selected_table=selected_table,
            selected_cause=selected_cause,
            selected_object_category=selected_object_category,
            temperature=temperature,
            days_ahead=days_ahead,
            selected_history_window=selected_history_window,
            option_catalog=option_catalog,
            filtered_records_count=filtered_records_count,
            metadata_items=metadata_items,
            preload_notes=preload_notes,
            source_table_notes=source_table_notes,
            source_tables=source_tables,
            daily_history=daily_history,
            ml_result=ml_result,
            scenario_temperature=scenario_temperature,
            temperature_quality=temperature_quality,
        )
        if perf is not None:
            perf.update(
                payload_has_data=bool(payload['has_data']),
                payload_notes=len(payload['notes']),
                feature_importance_rows=len(payload['feature_importance']),
                forecast_rows=len(payload['forecast_rows']),
            )
    _emit_progress(progress_callback, 'ml_model.completed', 'ML-анализ завершён, результат готов к выдаче.')
    return _cache_store(cache_key, payload)


def _cache_get(cache_key: Tuple[int, str, str, str, str, int, str]) -> Optional[Dict[str, Any]]:
    cached = _ML_CACHE.get(cache_key)
    if cached is None:
        return None
    _ML_CACHE.move_to_end(cache_key)
    return deepcopy(cached)


def _cache_store(cache_key: Tuple[int, str, str, str, str, int, str], payload: Dict[str, Any]) -> Dict[str, Any]:
    _ML_CACHE[cache_key] = deepcopy(payload)
    _ML_CACHE.move_to_end(cache_key)
    while len(_ML_CACHE) > _CACHE_LIMIT:
        _ML_CACHE.popitem(last=False)
    return deepcopy(payload)


def _build_ml_request_state(
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
    source_table_notes = _selected_source_table_notes(table_options, selected_table)
    days_ahead = _parse_forecast_days(forecast_days)
    selected_history_window = _parse_history_window(history_window)
    scenario_temperature = _parse_float(temperature)
    cache_key = (
        ML_CACHE_SCHEMA_VERSION,
        selected_table,
        cause or 'all',
        object_category or 'all',
        _format_float_for_input(scenario_temperature) if scenario_temperature is not None else '',
        days_ahead,
        selected_history_window,
    )
    return {
        'table_options': table_options,
        'selected_table': selected_table,
        'source_tables': source_tables,
        'source_table_notes': source_table_notes,
        'days_ahead': days_ahead,
        'selected_history_window': selected_history_window,
        'scenario_temperature': scenario_temperature,
        'cache_key': cache_key,
    }


def clear_ml_model_cache() -> None:
    _ML_CACHE.clear()
    clear_forecasting_sql_cache()
