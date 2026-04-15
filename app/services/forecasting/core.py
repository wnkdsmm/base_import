# Compatibility re-export layer. Import directly from submodules in new code.

from .forecasting_bootstrap import (
    annotations,
    datetime,
    get_plotly_bundle,
    ForecastPayload,
    ForecastingContext,
    ForecastingRequestState,
    TableOption,
    _empty_forecasting_data,
    _build_forecasting_request_state_impl,
    _build_base_forecast_loading_message,
    _build_forecasting_shell_data_impl,
    _build_metadata_loading_message,
    _build_forecasting_table_options,
    _resolve_forecasting_selection,
    _selected_source_table_notes,
    _selected_source_tables,
    _format_datetime,
    _parse_forecast_days,
    _parse_history_window,
    _build_forecasting_context,
    _build_forecasting_page_fallback_initial_data,
    _build_forecasting_shell_fallback_initial_data,
    _finalize_metadata_without_sources,
    _build_no_source_forecasting_payload,
    _build_forecasting_request_state,
    _build_forecasting_shell_data,
)

from .forecasting_pipeline import (
    annotations,
    nullcontext,
    Callable,
    current_perf_trace,
    profiled,
    build_immutable_payload_ttl_cache,
    build_executive_brief_from_risk_payload,
    compose_executive_brief_text,
    build_decision_support_payload,
    _build_forecasting_cache_key,
    _emit_forecasting_progress,
    engine,
    _build_forecasting_base_payload_impl,
    _build_forecasting_metadata_payload_impl,
    _complete_forecasting_decision_support_payload_impl,
    _build_base_forecast_loading_message,
    _build_decision_support_followup_message,
    _build_metadata_loading_message,
    _build_pending_decision_support_payload,
    _build_pending_executive_brief,
    _build_shell_risk_prediction,
    _build_slice_label,
    _build_forecast_breakdown_chart,
    _build_forecast_chart,
    _build_geo_chart,
    _build_weekday_chart,
    FORECAST_DAY_OPTIONS,
    HISTORY_WINDOW_OPTIONS,
    SCENARIO_FORECAST_DESCRIPTION,
    _build_daily_history_sql,
    _build_forecast_rows,
    _build_forecasting_table_options,
    _build_option_catalog_sql,
    _build_weekday_profile,
    _collect_forecasting_metadata,
    _count_forecasting_records_sql,
    _resolve_forecasting_selection,
    _selected_source_table_notes,
    _selected_source_tables,
    _temperature_quality_from_daily_history,
    clear_forecasting_sql_cache,
    clear_forecasting_input_cache,
    _empty_forecasting_data,
    _build_feature_cards,
    _build_feature_cards_with_quality,
    _build_insights,
    _build_notes,
    _build_summary,
    _build_scenario_quality_assessment,
    _run_scenario_backtesting,
    ForecastPayload,
    ForecastingDeps,
    ForecastingPayload,
    ForecastingRequestState,
    TableOption,
    _format_datetime,
    _format_float_for_input,
    _history_window_label,
    _parse_float,
    _resolve_option_value,
    _build_forecasting_context,
    _build_forecasting_page_fallback_initial_data,
    _build_forecasting_request_state,
    _build_forecasting_shell_data,
    _finalize_metadata_without_sources,
    _FORECASTING_CACHE,
    clear_forecasting_cache,
    _resolve_cached_forecasting_shell_payload,
    get_forecasting_page_context,
    get_forecasting_shell_context,
    get_forecasting_metadata,
    get_forecasting_data,
    get_forecasting_decision_support_data,
    _forecasting_assembly_dependencies,
)

from . import forecasting_bootstrap as _forecasting_bootstrap_mod
from . import forecasting_pipeline as _forecasting_pipeline_mod

# Backward-compatible symbols historically imported from forecasting.core.
from .forecasting_pipeline import _FORECASTING_CACHE
from .presentation import _build_feature_cards_with_quality


_PATCHABLE_SYMBOLS = [
    "_build_forecasting_table_options",
    "_resolve_forecasting_selection",
    "_selected_source_tables",
    "_selected_source_table_notes",
    "_parse_forecast_days",
    "_parse_history_window",
    "_collect_forecasting_metadata",
    "_build_option_catalog_sql",
    "_resolve_option_value",
    "_build_daily_history_sql",
    "_count_forecasting_records_sql",
    "_build_forecast_rows",
    "_build_forecast_chart",
    "_build_forecast_breakdown_chart",
    "_build_weekday_chart",
    "_build_geo_chart",
    "_build_weekday_profile",
    "_build_insights",
    "_build_notes",
    "_build_summary",
    "_build_feature_cards",
    "_build_feature_cards_with_quality",
    "_build_scenario_quality_assessment",
    "_run_scenario_backtesting",
    "_temperature_quality_from_daily_history",
    "build_decision_support_payload",
]


def _sync_forecasting_core_symbols() -> None:
    for name in _PATCHABLE_SYMBOLS:
        if name in globals():
            setattr(_forecasting_bootstrap_mod, name, globals()[name])
            setattr(_forecasting_pipeline_mod, name, globals()[name])


def get_forecasting_page_context(*args, **kwargs):
    _sync_forecasting_core_symbols()
    return _forecasting_pipeline_mod.get_forecasting_page_context(*args, **kwargs)


def get_forecasting_shell_context(*args, **kwargs):
    _sync_forecasting_core_symbols()
    return _forecasting_pipeline_mod.get_forecasting_shell_context(*args, **kwargs)


def get_forecasting_metadata(*args, **kwargs):
    _sync_forecasting_core_symbols()
    return _forecasting_pipeline_mod.get_forecasting_metadata(*args, **kwargs)


def get_forecasting_data(*args, **kwargs):
    _sync_forecasting_core_symbols()
    return _forecasting_pipeline_mod.get_forecasting_data(*args, **kwargs)


def get_forecasting_decision_support_data(*args, **kwargs):
    _sync_forecasting_core_symbols()
    return _forecasting_pipeline_mod.get_forecasting_decision_support_data(*args, **kwargs)


__all__ = [
    'annotations',
    'datetime',
    'get_plotly_bundle',
    'ForecastPayload',
    'ForecastingContext',
    'ForecastingRequestState',
    'TableOption',
    '_empty_forecasting_data',
    '_build_forecasting_request_state_impl',
    '_build_base_forecast_loading_message',
    '_build_forecasting_shell_data_impl',
    '_build_metadata_loading_message',
    '_build_forecasting_table_options',
    '_resolve_forecasting_selection',
    '_selected_source_table_notes',
    '_selected_source_tables',
    '_format_datetime',
    '_parse_forecast_days',
    '_parse_history_window',
    '_build_forecasting_context',
    '_build_forecasting_page_fallback_initial_data',
    '_build_forecasting_shell_fallback_initial_data',
    '_finalize_metadata_without_sources',
    '_build_no_source_forecasting_payload',
    '_build_forecasting_request_state',
    '_build_forecasting_shell_data',
    'nullcontext',
    'Callable',
    'current_perf_trace',
    'profiled',
    'build_immutable_payload_ttl_cache',
    'build_executive_brief_from_risk_payload',
    'compose_executive_brief_text',
    'build_decision_support_payload',
    '_build_forecasting_cache_key',
    '_emit_forecasting_progress',
    'engine',
    '_build_forecasting_base_payload_impl',
    '_build_forecasting_metadata_payload_impl',
    '_complete_forecasting_decision_support_payload_impl',
    '_build_decision_support_followup_message',
    '_build_pending_decision_support_payload',
    '_build_pending_executive_brief',
    '_build_shell_risk_prediction',
    '_build_slice_label',
    '_build_forecast_breakdown_chart',
    '_build_forecast_chart',
    '_build_geo_chart',
    '_build_weekday_chart',
    'FORECAST_DAY_OPTIONS',
    'HISTORY_WINDOW_OPTIONS',
    'SCENARIO_FORECAST_DESCRIPTION',
    '_build_daily_history_sql',
    '_build_forecast_rows',
    '_build_option_catalog_sql',
    '_build_weekday_profile',
    '_collect_forecasting_metadata',
    '_count_forecasting_records_sql',
    '_temperature_quality_from_daily_history',
    'clear_forecasting_sql_cache',
    'clear_forecasting_input_cache',
    '_build_feature_cards',
    '_build_feature_cards_with_quality',
    '_build_insights',
    '_build_notes',
    '_build_summary',
    '_build_scenario_quality_assessment',
    '_run_scenario_backtesting',
    'ForecastingDeps',
    'ForecastingPayload',
    '_format_float_for_input',
    '_history_window_label',
    '_parse_float',
    '_resolve_option_value',
    '_FORECASTING_CACHE',
    'clear_forecasting_cache',
    '_resolve_cached_forecasting_shell_payload',
    'get_forecasting_page_context',
    'get_forecasting_shell_context',
    'get_forecasting_metadata',
    'get_forecasting_data',
    'get_forecasting_decision_support_data',
    '_forecasting_assembly_dependencies',
    '_PATCHABLE_SYMBOLS',
    '_sync_forecasting_core_symbols',
]
