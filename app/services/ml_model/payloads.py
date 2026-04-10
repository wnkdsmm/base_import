from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from app.services.forecasting.presentation import _build_feature_cards_with_quality
from app.services.forecasting.utils import _format_datetime, _format_float_for_input, _history_window_label

from .constants import (
    FORECAST_DAY_OPTIONS,
    HISTORY_WINDOW_OPTIONS,
    ML_PREDICTIVE_BLOCK_DESCRIPTION,
    MODEL_NAME,
)
from .presentation import (
    _build_forecast_chart,
    _build_importance_chart,
    _build_notes,
    _build_quality_assessment,
    _build_summary,
    _empty_light_chart,
)
from .training_result import _empty_ml_result


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
    metadata_items: List[Dict[str, Any]],
    preload_notes: List[str],
    source_table_notes: List[str],
    source_tables: List[str],
    daily_history: List[Dict[str, Any]],
    ml_result: Dict[str, Any],
    scenario_temperature: Any,
    temperature_quality: Dict[str, Any],
) -> Dict[str, Any]:
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
            'available_forecast_days': [{'value': str(item), 'label': f'{item} дней'} for item in FORECAST_DAY_OPTIONS],
            'available_history_windows': HISTORY_WINDOW_OPTIONS,
        },
    }


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
            'hero_summary': 'После расчета здесь появится краткий вывод по ожидаемому числу пожаров на ближайшие даты.',
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
            'available_causes': [{'value': 'all', 'label': 'Все причины'}],
            'available_object_categories': [{'value': 'all', 'label': 'Все категории'}],
            'available_forecast_days': [{'value': str(item), 'label': f'{item} дней'} for item in FORECAST_DAY_OPTIONS],
            'available_history_windows': HISTORY_WINDOW_OPTIONS,
        },
    }
