from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from app.services.forecasting.presentation import _build_feature_cards_with_quality
from app.services.forecasting.utils import _format_datetime, _format_float_for_input, _history_window_label

from .ml_model_types import (
    FORECAST_DAY_OPTIONS,
    HISTORY_WINDOW_OPTIONS,
    ML_PREDICTIVE_BLOCK_DESCRIPTION,
    MODEL_NAME,
)
from .training.presentation import (
    _build_forecast_chart,
    _build_importance_chart,
    _build_notes,
    _build_quality_assessment,
    _build_summary,
    _empty_light_chart,
)
from .training.training_result import _empty_ml_result


def _compact_ui_notes(items: List[Any], limit: int = 2) -> List[str]:
    notes: List[str] = []
    for item in items:
        text = str(item).strip() if item is not None else ''
        if not text or text in notes:
            continue
        notes.append(text)
        if len(notes) >= limit:
            break
    return notes


def _build_ml_payload(
    *,
    table_options: List[Dict[str, str]],
    selected_table: str,
    selected_cause: str,
    selected_object_category: str,
    temperature: str,
    days_ahead: int,
    selected_history_window: str,
    option_catalog: Dict[str, List[Dict[str, str]]],
    filtered_records_count: int,
    metadata_items: List[dict[str, Any]],
    preload_notes: List[str],
    source_table_notes: List[str],
    source_tables: List[str],
    daily_history: List[dict[str, Any]],
    ml_result: dict[str, Any],
    scenario_temperature: Any,
    temperature_quality: dict[str, Any],
) -> dict[str, Any]:
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
    return {
        'generated_at': _format_datetime(datetime.now()),
        'has_data': filtered_records_count > 0,
        'model_description': ML_PREDICTIVE_BLOCK_DESCRIPTION,
        'summary': summary,
        'quality_assessment': _build_quality_assessment(ml_result),
        'features': _build_feature_cards_with_quality(metadata_items, temperature_quality=temperature_quality),
        'charts': {
            'forecast': _build_forecast_chart(daily_history, ml_result),
            'importance': _build_importance_chart(
                ml_result.get('feature_importance', []),
                note=str(ml_result.get('feature_importance_note') or '').strip(),
            ),
        },
        'forecast_rows': ml_result.get('forecast_rows', []),
        'feature_importance': ml_result.get('feature_importance', []),
        'notes': _compact_ui_notes(
            source_table_notes
            + _build_notes(
                preload_notes,
                metadata_items,
                filtered_records_count,
                daily_history,
                ml_result,
                scenario_temperature,
                source_tables,
            )
        ),
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
            'available_forecast_days': [{'value': str(item), 'label': f'{item} РґРЅРµР№'} for item in FORECAST_DAY_OPTIONS],
            'available_history_windows': HISTORY_WINDOW_OPTIONS,
        },
    }


def _empty_ml_model_data(
    table_options: List[Dict[str, str]],
    selected_table: str,
    forecast_days: int,
    temperature: str,
    history_window: str,
) -> dict[str, Any]:
    empty_result = _empty_ml_result('РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РѕР±СѓС‡РµРЅРёСЏ РјРѕРґРµР»Рё.')
    return {
        'generated_at': _format_datetime(datetime.now()),
        'has_data': False,
        'model_description': '',
        'summary': {
            'selected_table_label': 'Р’СЃРµ С‚Р°Р±Р»РёС†С‹' if selected_table == 'all' else (selected_table or 'РќРµС‚ С‚Р°Р±Р»РёС†С‹'),
            'slice_label': 'Р’СЃРµ РїРѕР¶Р°СЂС‹',
            'hero_summary': 'РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РєСЂР°С‚РєРёР№ РІС‹РІРѕРґ РїРѕ РѕР¶РёРґР°РµРјРѕРјСѓ С‡РёСЃР»Сѓ РїРѕР¶Р°СЂРѕРІ РЅР° Р±Р»РёР¶Р°Р№С€РёРµ РґР°С‚С‹.',
            'history_period_label': 'РќРµС‚ РґР°РЅРЅС‹С…',
            'history_window_label': _history_window_label(history_window),
            'model_label': MODEL_NAME,
            'count_model_label': 'Р РµРіСЂРµСЃСЃРёСЏ РџСѓР°СЃСЃРѕРЅР°',
            'event_model_label': 'РќРµ РѕР±СѓС‡РµРЅ',
            'event_backtest_model_label': 'РќРµ РїРѕРєР°Р·Р°РЅ',
            'backtest_method_label': 'РџСЂРѕРІРµСЂРєР° РЅР° РёСЃС‚РѕСЂРёРё РЅРµ РІС‹РїРѕР»РЅРµРЅР°',
            'fires_count_display': '0',
            'history_days_display': '0',
            'forecast_days_display': str(forecast_days),
            'last_observed_date': '-',
            'count_mae_display': '-',
            'count_rmse_display': '-',
            'count_smape_display': 'вЂ”',
            'count_poisson_deviance_display': '-',
            'baseline_count_mae_display': '-',
            'baseline_count_rmse_display': '-',
            'baseline_count_smape_display': 'вЂ”',
            'heuristic_count_mae_display': '-',
            'heuristic_count_rmse_display': '-',
            'heuristic_count_smape_display': 'вЂ”',
            'heuristic_count_poisson_deviance_display': '-',
            'mae_vs_baseline_display': '-',
            'brier_display': 'вЂ”',
            'baseline_brier_display': 'вЂ”',
            'heuristic_brier_display': 'вЂ”',
            'roc_auc_display': 'вЂ”',
            'baseline_roc_auc_display': 'вЂ”',
            'heuristic_roc_auc_display': 'вЂ”',
            'f1_display': 'вЂ”',
            'baseline_f1_display': 'вЂ”',
            'heuristic_f1_display': 'вЂ”',
            'log_loss_display': 'вЂ”',
            'top_feature_label': '-',
            'temperature_scenario_display': temperature.strip() or 'РСЃС‚РѕСЂРёС‡РµСЃРєР°СЏ С‚РµРјРїРµСЂР°С‚СѓСЂР°',
            'predicted_total_display': '0',
            'average_expected_count_display': '0',
            'peak_expected_count_display': '0',
            'peak_expected_count_day_display': '-',
            'elevated_risk_days_display': '0',
            'average_event_probability_display': 'вЂ”',
            'peak_event_probability_display': 'вЂ”',
            'peak_event_probability_day_display': '-',
            'event_probability_enabled': False,
            'event_backtest_available': False,
        },
        'quality_assessment': _build_quality_assessment(empty_result),
        'features': [],
        'charts': {
            'forecast': _empty_light_chart('ML-РїСЂРѕРіРЅРѕР· РѕР¶РёРґР°РµРјРѕРіРѕ С‡РёСЃР»Р° РїРѕР¶Р°СЂРѕРІ', 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РѕР±СѓС‡РµРЅРёСЏ РјРѕРґРµР»Рё.'),
            'importance': _build_importance_chart([], note=''),
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
            'available_causes': [{'value': 'all', 'label': 'Р’СЃРµ РїСЂРёС‡РёРЅС‹'}],
            'available_object_categories': [{'value': 'all', 'label': 'Р’СЃРµ РєР°С‚РµРіРѕСЂРёРё'}],
            'available_forecast_days': [{'value': str(item), 'label': f'{item} РґРЅРµР№'} for item in FORECAST_DAY_OPTIONS],
            'available_history_windows': HISTORY_WINDOW_OPTIONS,
        },
    }
