from __future__ import annotations

from typing import Any, List, Optional

import numpy as np

from app.services.forecasting.utils import _format_integer, _format_number, _format_period, _history_window_label

from ..ml_model_types import MODEL_NAME
from .types import ForecastingDailyHistoryRow, TrainingFeatureImportanceRow, TrainingMlResultPayload
from .presentation_backtesting import _prediction_interval_display_context
from .presentation_meta import (
    MISSING_DISPLAY,
    _event_probability_context,
    _format_optional_integer,
    _format_optional_number,
    _format_optional_percent,
    _format_optional_signed_percent,
    _format_optional_text,
    _format_row_display,
)

def _empty_light_chart(title: str, empty_message: str, kind: str = 'line') -> dict[str, Any]:  # one-off
    payload: dict[str, Any] = {
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


def _build_forecast_chart(daily_history: List[ForecastingDailyHistoryRow], ml_result: TrainingMlResultPayload) -> dict[str, Any]:  # one-off
    title = 'ML-РїСЂРѕРіРЅРѕР· РѕР¶РёРґР°РµРјРѕРіРѕ С‡РёСЃР»Р° РїРѕР¶Р°СЂРѕРІ'
    if not daily_history or not ml_result.get('is_ready'):
        return _empty_light_chart(title, ml_result.get('message') or 'РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР°РЅРЅС‹С… РґР»СЏ РїРѕСЃС‚СЂРѕРµРЅРёСЏ РїСЂРѕРіРЅРѕР·Р°.')

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
            {'label': 'РСЃС‚РѕСЂРёСЏ', 'color': '#F97316'},
            {'label': 'РџСЂРѕРІРµСЂРєР° РЅР° РёСЃС‚РѕСЂРёРё: С„Р°РєС‚', 'color': '#94A3B8'},
            {'label': 'РџСЂРѕРІРµСЂРєР° РЅР° РёСЃС‚РѕСЂРёРё: РїСЂРѕРіРЅРѕР·', 'color': '#64748B'},
            {'label': 'ML-РїСЂРѕРіРЅРѕР·', 'color': '#0F766E'},
        ],
        'series': {
            'history': history_points,
            'backtest_actual': backtest_actual,
            'backtest_predicted': backtest_predicted,
            'forecast': forecast_points,
            'forecast_band': forecast_band,
        },
    }


def _build_importance_chart(feature_importance: List[TrainingFeatureImportanceRow], note: str = '') -> dict[str, Any]:  # one-off
    title = 'Р’Р°Р¶РЅРѕСЃС‚СЊ РїСЂРёР·РЅР°РєРѕРІ ML-Р±Р»РѕРєР°'
    if not feature_importance:
        payload = _empty_light_chart(title, 'РњРѕРґРµР»СЊ РµС‰С‘ РЅРµ РѕР±СѓС‡РµРЅР°: РІР°Р¶РЅРѕСЃС‚СЊ РїСЂРёР·РЅР°РєРѕРІ РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р°.', kind='bars')
        payload['note'] = note
        return payload
    top_items = feature_importance[:8]
    return {
        'title': title,
        'kind': 'bars',
        'empty_message': '',
        'note': note,
        'items': [
            {
                'label': item['label'],
                'value': item['importance'],
                'value_display': item['importance_display'],
            }
            for item in top_items
        ],
    }


# intentionally separate from access_points/presentation.py::_build_summary and
# forecasting/presentation.py::_build_summary:
# ML-training summary exposes model diagnostics/backtest/event-probability details.
def _build_summary(
    selected_table: str,
    selected_cause: str,
    selected_object_category: str,
    daily_history: List[ForecastingDailyHistoryRow],
    filtered_records_count: int,
    ml_result: TrainingMlResultPayload,
    history_window: str,
    scenario_temperature: Optional[float],
) -> dict[str, Any]:  # one-off
    history_dates = [item['date'] for item in daily_history]
    slice_parts = []
    if selected_cause != 'all':
        slice_parts.append(f'РџСЂРёС‡РёРЅР°: {selected_cause}')
    if selected_object_category != 'all':
        slice_parts.append(f'РљР°С‚РµРіРѕСЂРёСЏ: {selected_object_category}')

    forecast_rows = ml_result.get('forecast_rows', [])
    average_expected_count = (
        float(np.mean([float(item.get('forecast_value', 0.0)) for item in forecast_rows])) if forecast_rows else None
    )
    predicted_total = sum(float(item.get('forecast_value', 0.0)) for item in forecast_rows) if forecast_rows else None
    peak_row = max(forecast_rows, key=lambda item: float(item.get('forecast_value', 0.0))) if forecast_rows else None
    elevated_risk_days = sum(1 for item in forecast_rows if float(item.get('risk_index', 0.0)) >= 75.0) if forecast_rows else None
    event_probability_enabled = bool(ml_result.get('event_probability_enabled', ml_result.get('classifier_ready')))
    has_event_classifier = event_probability_enabled

    event_probabilities = (
        [
            float(item.get('event_probability'))
            for item in forecast_rows
            if item.get('event_probability') is not None
        ]
        if event_probability_enabled
        else []
    )
    average_event_probability = float(np.mean(event_probabilities)) if event_probabilities else None
    peak_event_row = (
        max(
            (item for item in forecast_rows if item.get('event_probability') is not None),
            key=lambda item: float(item.get('event_probability', 0.0)),
        )
        if event_probabilities
        else None
    )
    backtest_overview = ml_result.get('backtest_overview', {}) or {}
    interval_context = _prediction_interval_display_context(ml_result, backtest_overview)
    event_context = _event_probability_context(ml_result, backtest_overview)
    hero_summary = (
        f"РџРёРє РїРѕ РіРѕСЂРёР·РѕРЅС‚Сѓ РѕР¶РёРґР°РµС‚СЃСЏ {_format_optional_text(peak_row.get('date_display'))}: "
        f"РѕР¶РёРґР°РµРјРѕРµ С‡РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ вЂ” {_format_row_display(peak_row, 'forecast_value_display', 'forecast_value', _format_optional_number)}. "
        f"РЎСЂРµРґРЅРµРµ РѕР¶РёРґР°РµРјРѕРµ Р·РЅР°С‡РµРЅРёРµ РїРѕ РґРЅСЏРј вЂ” {_format_optional_number(average_expected_count)}."
        if peak_row
        else 'РџРѕСЃР»Рµ СЂР°СЃС‡РµС‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РєСЂР°С‚РєРёР№ РІС‹РІРѕРґ РїРѕ РѕР¶РёРґР°РµРјРѕРјСѓ С‡РёСЃР»Сѓ РїРѕР¶Р°СЂРѕРІ РЅР° Р±Р»РёР¶Р°Р№С€РёРµ РґР°С‚С‹.'
    )

    return {
        'selected_table_label': 'Р’СЃРµ С‚Р°Р±Р»РёС†С‹' if selected_table == 'all' else (selected_table or 'РќРµС‚ С‚Р°Р±Р»РёС†С‹'),
        'slice_label': ' | '.join(slice_parts) if slice_parts else 'Р’СЃРµ РїРѕР¶Р°СЂС‹ РІС‹Р±СЂР°РЅРЅРѕР№ РёСЃС‚РѕСЂРёРё',
        'hero_summary': hero_summary,
        'history_period_label': _format_period(history_dates),
        'history_window_label': _history_window_label(history_window),
        'model_label': MODEL_NAME,
        'count_model_label': ml_result.get('count_model_label') or MODEL_NAME,
        'event_model_label': ml_result.get('event_model_label') or 'РќРµ РѕР±СѓС‡РµРЅ',
        'event_backtest_model_label': ml_result.get('selected_event_model_label') or 'РќРµ РїРѕРєР°Р·Р°РЅ',
        'backtest_method_label': ml_result.get('backtest_method_label') or 'РџСЂРѕРІРµСЂРєР° РЅР° РёСЃС‚РѕСЂРёРё РЅРµ РІС‹РїРѕР»РЅРµРЅР°',
        'fires_count_display': _format_integer(filtered_records_count),
        'history_days_display': _format_integer(len(daily_history)),
        'forecast_days_display': _format_integer(len(forecast_rows)),
        'last_observed_date': history_dates[-1].strftime('%d.%m.%Y') if history_dates else MISSING_DISPLAY,
        'count_mae_display': _format_optional_number(ml_result.get('count_mae')),
        'count_rmse_display': _format_optional_number(ml_result.get('count_rmse')),
        'count_smape_display': _format_optional_percent(ml_result.get('count_smape')),
        'count_poisson_deviance_display': _format_optional_number(ml_result.get('count_poisson_deviance')),
        'baseline_count_mae_display': _format_optional_number(ml_result.get('baseline_count_mae')),
        'baseline_count_rmse_display': _format_optional_number(ml_result.get('baseline_count_rmse')),
        'baseline_count_smape_display': _format_optional_percent(ml_result.get('baseline_count_smape')),
        'heuristic_count_mae_display': _format_optional_number(ml_result.get('heuristic_count_mae')),
        'heuristic_count_rmse_display': _format_optional_number(ml_result.get('heuristic_count_rmse')),
        'heuristic_count_smape_display': _format_optional_percent(ml_result.get('heuristic_count_smape')),
        'heuristic_count_poisson_deviance_display': _format_optional_number(ml_result.get('heuristic_count_poisson_deviance')),
        'mae_vs_baseline_display': _format_optional_signed_percent(ml_result.get('count_vs_baseline_delta')),
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
        'top_feature_label': _format_optional_text(ml_result.get('top_feature_label')),
        'temperature_scenario_display': (
            f"{_format_number(scenario_temperature)} В°C" if scenario_temperature is not None else 'РСЃС‚РѕСЂРёС‡РµСЃРєР°СЏ С‚РµРјРїРµСЂР°С‚СѓСЂР°'
        ),
        'predicted_total_display': _format_optional_number(predicted_total),
        'average_expected_count_display': _format_optional_number(average_expected_count),
        'peak_expected_count_display': _format_row_display(
            peak_row,
            'forecast_value_display',
            'forecast_value',
            _format_optional_number,
        ),
        'peak_expected_count_day_display': _format_optional_text(peak_row.get('date_display') if peak_row else None),
        'elevated_risk_days_display': _format_optional_integer(elevated_risk_days),
        'average_event_probability_display': _format_optional_percent(
            average_event_probability * 100.0 if average_event_probability is not None else None
        ),
        'peak_event_probability_display': _format_row_display(
            peak_event_row,
            'event_probability_display',
            'event_probability',
            lambda item: _format_optional_percent(float(item) * 100.0 if item is not None else None),
        ),
        'peak_event_probability_day_display': _format_optional_text(peak_event_row.get('date_display') if peak_event_row else None),
        'event_probability_enabled': has_event_classifier,
        'event_backtest_available': bool(ml_result.get('event_backtest_available')),
        'event_probability_note': event_context['note'],
        'event_probability_reason_code': event_context['reason_code'],
        'prediction_interval_level_display': interval_context['level_display'],
        'prediction_interval_coverage_display': interval_context['coverage_display'],
        'prediction_interval_method_label': interval_context['method_label_display'],
    }



__all__ = [
    '_build_forecast_chart',
    '_build_importance_chart',
    '_build_summary',
    '_empty_light_chart',
]
