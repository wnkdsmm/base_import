from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from app.perf import current_perf_trace, profiled
from app.runtime_cache import build_immutable_payload_lru_cache
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
from app.services.shared.request_state import build_ml_request_state as _build_ml_request_state_impl
from config.db import engine

from .constants import ML_CACHE_SCHEMA_VERSION, _CACHE_LIMIT
from .data_access import (
    clear_ml_model_input_cache,
    load_ml_aggregation_inputs as _load_ml_aggregation_inputs_impl,
    load_ml_filter_bundle as _load_ml_filter_bundle_impl,
)
from .payloads import _build_ml_payload, _compact_ui_notes, _empty_ml_model_data
from .runtime import MlProgressCallback, _emit_progress
from .training import _train_ml_model, clear_training_artifact_cache

_ML_CACHE = build_immutable_payload_lru_cache(max_size=_CACHE_LIMIT)


def _build_ml_context(initial_data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'generated_at': _format_datetime(datetime.now()),
        'initial_data': initial_data,
        'plotly_js': '',
        'has_data': bool(initial_data['filters']['available_tables']),
    }


def _build_ml_deferred_shell_data(
    request_state: Dict[str, Any],
    *,
    cause: str,
    object_category: str,
    temperature: str,
) -> Dict[str, Any]:
    initial_data = _empty_ml_model_data(
        request_state['table_options'],
        request_state['selected_table'],
        request_state['days_ahead'],
        temperature,
        request_state['selected_history_window'],
    )
    initial_data['bootstrap_mode'] = 'deferred'
    initial_data['charts']['importance']['empty_message'] = (
        'Собираем драйверы прогноза: блок заполнится после фонового расчёта.'
    )
    initial_data['notes'].extend(request_state['source_table_notes'])
    initial_data['notes'] = _compact_ui_notes(initial_data['notes'])
    initial_data['filters']['cause'] = cause or 'all'
    initial_data['filters']['object_category'] = object_category or 'all'
    return initial_data


def _build_no_source_ml_payload(
    base_payload: Dict[str, Any],
    *,
    source_table_notes: list[str],
) -> Dict[str, Any]:
    base_payload['notes'].extend(source_table_notes)
    base_payload['notes'].append('\u041d\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b\u0445 \u0442\u0430\u0431\u043b\u0438\u0446 \u0434\u043b\u044f ML-\u043c\u043e\u0434\u0435\u043b\u0438.')
    base_payload['notes'] = _compact_ui_notes(base_payload['notes'])
    return base_payload


def _load_ml_filter_bundle(
    *,
    source_tables: list[str],
    selected_history_window: str,
    cause: str,
    object_category: str,
) -> Dict[str, Any]:
    return _load_ml_filter_bundle_impl(
        source_tables=source_tables,
        selected_history_window=selected_history_window,
        cause=cause,
        object_category=object_category,
        collect_forecasting_metadata=_collect_forecasting_metadata,
        build_option_catalog_sql=_build_option_catalog_sql,
        resolve_option_value=_resolve_option_value,
    )


def _load_ml_aggregation_inputs(
    *,
    source_tables: list[str],
    selected_history_window: str,
    filter_bundle: Dict[str, Any],
) -> Dict[str, Any]:
    return _load_ml_aggregation_inputs_impl(
        source_tables=source_tables,
        selected_history_window=selected_history_window,
        filter_bundle=filter_bundle,
        build_daily_history_sql=_build_daily_history_sql,
        count_forecasting_records_sql=_count_forecasting_records_sql,
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
        initial_data['notes'].append('ML-\u0441\u0442\u0440\u0430\u043d\u0438\u0446\u0430 \u043e\u0442\u043a\u0440\u044b\u0442\u0430 \u0432 \u0431\u0435\u0437\u043e\u043f\u0430\u0441\u043d\u043e\u043c \u0440\u0435\u0436\u0438\u043c\u0435: \u043e\u0431\u0443\u0447\u0435\u043d\u0438\u0435 \u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e \u043e\u0442\u043a\u043b\u044e\u0447\u0435\u043d\u043e \u0438\u0437-\u0437\u0430 \u0432\u043d\u0443\u0442\u0440\u0435\u043d\u043d\u0435\u0439 \u043e\u0448\u0438\u0431\u043a\u0438.')
        initial_data['notes'].append(f'\u0422\u0435\u0445\u043d\u0438\u0447\u0435\u0441\u043a\u0430\u044f \u043f\u0440\u0438\u0447\u0438\u043d\u0430: {exc}')
        initial_data['notes'] = _compact_ui_notes(initial_data['notes'])
        initial_data['model_description'] = (
            'ML-\u043f\u0440\u043e\u0433\u043d\u043e\u0437 \u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e \u043e\u0442\u043a\u0440\u044b\u0442 \u0431\u0435\u0437 \u043e\u0431\u0443\u0447\u0435\u043d\u0438\u044f, \u0447\u0442\u043e\u0431\u044b \u044d\u043a\u0440\u0430\u043d \u043e\u0441\u0442\u0430\u0432\u0430\u043b\u0441\u044f \u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d \u0434\u0430\u0436\u0435 \u043f\u0440\u0438 \u043f\u0440\u043e\u0431\u043b\u0435\u043c\u0435 \u0432 \u0434\u0430\u043d\u043d\u044b\u0445 \u0438\u043b\u0438 \u043f\u0440\u0438\u0437\u043d\u0430\u043a\u0430\u0445. '
            '\u0415\u0433\u043e \u0437\u0430\u0434\u0430\u0447\u0430 \u043d\u0435 \u043c\u0435\u043d\u044f\u0435\u0442\u0441\u044f: \u043e\u0446\u0435\u043d\u0438\u0442\u044c \u043e\u0436\u0438\u0434\u0430\u0435\u043c\u043e\u0435 \u0447\u0438\u0441\u043b\u043e \u043f\u043e\u0436\u0430\u0440\u043e\u0432 \u043f\u043e \u0434\u0430\u0442\u0430\u043c, \u0430 \u043d\u0435 \u0440\u0430\u043d\u0436\u0438\u0440\u043e\u0432\u0430\u0442\u044c \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0438.'
        )
    return _build_ml_context(initial_data)


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
        return _build_ml_context(cached)

    initial_data = _build_ml_deferred_shell_data(
        request_state,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
    )
    return _build_ml_context(initial_data)


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
        _emit_progress(progress_callback, 'ml_model.completed', '\u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442 ML-\u0430\u043d\u0430\u043b\u0438\u0437\u0430 \u0432\u0437\u044f\u0442 \u0438\u0437 \u043a\u044d\u0448\u0430.')
        return cached

    if perf is not None:
        perf.update(cache_hit=False)
    base = _empty_ml_model_data(table_options, selected_table, days_ahead, temperature, selected_history_window)
    if not source_tables:
        base = _build_no_source_ml_payload(base, source_table_notes=source_table_notes)
        if perf is not None:
            perf.update(payload_has_data=False, payload_notes=len(base['notes']))
        return _cache_store(cache_key, base)

    _emit_progress(progress_callback, 'ml_model.running', '\u0421\u043e\u0431\u0438\u0440\u0430\u0435\u043c SQL-\u0430\u0433\u0440\u0435\u0433\u0430\u0442\u044b \u0438 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b\u0435 \u0444\u0438\u043b\u044c\u0442\u0440\u044b \u0434\u043b\u044f ML-\u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430.')
    filter_prep_context = perf.span('filter_prep') if perf is not None else nullcontext()
    with filter_prep_context:
        filter_bundle = _load_ml_filter_bundle(
            source_tables=source_tables,
            selected_history_window=selected_history_window,
            cause=cause,
            object_category=object_category,
        )
        metadata_items = filter_bundle['metadata_items']
        preload_notes = filter_bundle['preload_notes']
        option_catalog = filter_bundle['option_catalog']
        selected_cause = filter_bundle['selected_cause']
        selected_object_category = filter_bundle['selected_object_category']
        if perf is not None:
            perf.update(
                metadata_tables=len(metadata_items),
                available_causes=len(option_catalog['causes']),
                available_object_categories=len(option_catalog['object_categories']),
            )

    aggregation_context = perf.span('aggregation') if perf is not None else nullcontext()
    with aggregation_context:
        aggregation_inputs = _load_ml_aggregation_inputs(
            source_tables=source_tables,
            selected_history_window=selected_history_window,
            filter_bundle=filter_bundle,
        )
        daily_history = aggregation_inputs['daily_history']
        filtered_records_count = aggregation_inputs['filtered_records_count']
        if perf is not None:
            perf.update(input_rows=filtered_records_count, history_days=len(daily_history))
    _emit_progress(
        progress_callback,
        'ml_model.running',
        f"\u041f\u043e\u0434\u0433\u043e\u0442\u043e\u0432\u043b\u0435\u043d \u0434\u043d\u0435\u0432\u043d\u043e\u0439 \u0440\u044f\u0434: {len(daily_history)} \u0434\u043d\u0435\u0439 \u0438\u0441\u0442\u043e\u0440\u0438\u0438, {filtered_records_count} \u043f\u043e\u0436\u0430\u0440\u043e\u0432 \u043f\u043e\u0441\u043b\u0435 \u0444\u0438\u043b\u044c\u0442\u0440\u043e\u0432.",
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
    _emit_progress(progress_callback, 'ml_model.running', '\u0424\u043e\u0440\u043c\u0438\u0440\u0443\u0435\u043c \u0438\u0442\u043e\u0433\u043e\u0432\u044b\u0435 \u043c\u0435\u0442\u0440\u0438\u043a\u0438, \u0433\u0440\u0430\u0444\u0438\u043a\u0438 \u0438 \u0442\u0430\u0431\u043b\u0438\u0446\u044b ML-\u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430.')
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
    _emit_progress(progress_callback, 'ml_model.completed', 'ML-\u0430\u043d\u0430\u043b\u0438\u0437 \u0437\u0430\u0432\u0435\u0440\u0448\u0451\u043d, \u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442 \u0433\u043e\u0442\u043e\u0432 \u043a \u0432\u044b\u0434\u0430\u0447\u0435.')
    return _cache_store(cache_key, payload)


def _cache_get(cache_key: Tuple[int, str, str, str, str, int, str]) -> Optional[Dict[str, Any]]:
    return _ML_CACHE.get(cache_key)


def _cache_store(cache_key: Tuple[int, str, str, str, str, int, str], payload: Dict[str, Any]) -> Dict[str, Any]:
    _ML_CACHE.set(cache_key, payload)
    return payload


def _build_ml_request_state(
    table_name: str = 'all',
    cause: str = 'all',
    object_category: str = 'all',
    temperature: str = '',
    forecast_days: str = '14',
    history_window: str = 'all',
) -> Dict[str, Any]:
    return _build_ml_request_state_impl(
        table_name=table_name,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
        cache_schema_version=ML_CACHE_SCHEMA_VERSION,
        table_options_builder=_build_forecasting_table_options,
        selection_resolver=_resolve_forecasting_selection,
        source_tables_resolver=_selected_source_tables,
        source_notes_resolver=_selected_source_table_notes,
        forecast_days_parser=_parse_forecast_days,
        history_window_parser=_parse_history_window,
        temperature_parser=_parse_float,
        temperature_formatter=_format_float_for_input,
    )


def clear_ml_model_cache() -> None:
    _ML_CACHE.clear()
    clear_ml_model_input_cache()
    clear_training_artifact_cache()
    clear_forecasting_sql_cache()


