from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional

import numpy as np

from app.services.forecasting.utils import (
    _format_integer,
    _format_number,
    _format_period,
    _format_signed_percent,
    _history_window_label,
)

from .constants import MODEL_NAME

MISSING_DISPLAY = '—'
INTERVAL_SCHEME_LABELS = {
    'Forward rolling split conformal': 'скользящая проверка по истории',
    'Blocked forward CV conformal': 'блочная проверка по истории',
    'Fixed 60/40 chrono split conformal': 'фиксированное хронологическое разбиение 60/40',
    'Jackknife+ for time series': 'jackknife+ для временного ряда',
    'validated out-of-sample coverage unavailable': 'проверка покрытия пока недоступна',
}
INTERVAL_METHOD_LABELS = {
    'Adaptive conformal interval with predicted-count bins': 'Адаптивный конформный интервал по группам ожидаемого числа пожаров',
}
_FIRST_WINDOWS_RE = re.compile(r'^first (\d+) windows(?: through (.+))?$')
_LATER_WINDOWS_RE = re.compile(r'^later (\d+) windows(?: from (.+))?$')
_ROLLING_WINDOWS_RE = re.compile(r'^rolling evaluation (\d+) windows(?: from (.+))?$')
_BLOCKED_WINDOWS_RE = re.compile(r'^blocked evaluation (\d+) windows(?: from (.+))?$')
_LEAD_TIME_PREFIX_RE = re.compile(r'^For the (\d+)-day lead, (.+)$')


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

def _is_missing_metric(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ''
    try:
        return bool(np.isnan(value))
    except TypeError:
        return False


def _format_optional_value(value: Any, formatter: Callable[[Any], str]) -> str:
    return formatter(value) if not _is_missing_metric(value) else MISSING_DISPLAY


def _format_optional_number(value: Any) -> str:
    return _format_optional_value(value, lambda item: _format_number(float(item)))


def _format_optional_percent(value: Any) -> str:
    return _format_optional_value(value, lambda item: f"{_format_number(float(item))}%")


def _format_optional_signed_percent(value: Any) -> str:
    return _format_optional_value(value, lambda item: _format_signed_percent(float(item)))


def _format_optional_integer(value: Any) -> str:
    return _format_optional_value(value, lambda item: _format_integer(int(item)))


def _format_optional_text(value: Any) -> str:
    return _format_optional_value(value, lambda item: str(item).strip())


def _first_present(*values: Any) -> Any:
    for value in values:
        if not _is_missing_metric(value):
            return value
    return None


def _format_first_present(formatter: Callable[[Any], str], *values: Any) -> str:
    return _format_optional_value(_first_present(*values), formatter)


def _format_row_display(
    row: Optional[Dict[str, Any]],
    display_key: str,
    raw_key: str,
    raw_formatter: Callable[[Any], str],
) -> str:
    if not row:
        return MISSING_DISPLAY
    display_value = row.get(display_key)
    if not _is_missing_metric(display_value):
        return _format_optional_text(display_value)
    return raw_formatter(row.get(raw_key))


def _selection_label(is_selected: Any) -> str:
    return 'Рабочий метод' if bool(is_selected) else 'Сравнение'


def _sentence_case(text: str) -> str:
    if not text:
        return ''
    return text[:1].upper() + text[1:]


def _translate_interval_scheme_label(label: Any) -> str:
    if _is_missing_metric(label):
        return ''
    normalized = str(label).strip()
    return INTERVAL_SCHEME_LABELS.get(normalized, normalized)


def _translate_interval_method_label(raw_label: Any) -> str:
    if _is_missing_metric(raw_label):
        return MISSING_DISPLAY

    normalized = str(raw_label).strip()
    if not normalized:
        return MISSING_DISPLAY

    unavailable_suffix = ' (validated out-of-sample coverage unavailable)'
    if normalized.endswith(unavailable_suffix):
        base_label = normalized[: -len(unavailable_suffix)].strip()
        translated_base = INTERVAL_METHOD_LABELS.get(base_label, base_label)
        return f'{translated_base}; проверка покрытия на отложенных окнах пока недоступна'

    if '; validated by ' in normalized:
        base_label, scheme_label = normalized.split('; validated by ', 1)
        translated_base = INTERVAL_METHOD_LABELS.get(base_label.strip(), base_label.strip())
        translated_scheme = _translate_interval_scheme_label(scheme_label)
        return f'{translated_base}; проверка схемой: {translated_scheme}'

    if '; validation baseline: ' in normalized:
        base_label, scheme_label = normalized.split('; validation baseline: ', 1)
        translated_base = INTERVAL_METHOD_LABELS.get(base_label.strip(), base_label.strip())
        translated_scheme = _translate_interval_scheme_label(scheme_label)
        return f'{translated_base}; базовая схема проверки: {translated_scheme}'

    if '; validation candidate: ' in normalized:
        base_label, scheme_label = normalized.split('; validation candidate: ', 1)
        translated_base = INTERVAL_METHOD_LABELS.get(base_label.strip(), base_label.strip())
        translated_scheme = _translate_interval_scheme_label(scheme_label)
        return f'{translated_base}; кандидат проверки: {translated_scheme}'

    return INTERVAL_METHOD_LABELS.get(normalized, normalized)


def _translate_interval_validation_explanation(explanation: Any) -> str:
    if _is_missing_metric(explanation):
        return ''

    text = str(explanation).strip()
    if not text:
        return ''

    exact_replacements = {
        'Validated out-of-sample coverage is unavailable because backtesting was not run.': (
            'Покрытие на отложенных окнах пока недоступно: проверка интервалов на истории ещё не запускалась.'
        ),
        'Validated out-of-sample coverage is unavailable because the backtest has too few rolling-origin windows for forward-only interval validation.': (
            'Покрытие на отложенных окнах пока недоступно: в проверке на истории слишком мало скользящих окон для честной последовательной проверки интервала.'
        ),
    }
    lead_time_match = _LEAD_TIME_PREFIX_RE.match(text)
    if lead_time_match:
        lead_days, remainder = lead_time_match.groups()
        translated_remainder = exact_replacements.get(remainder)
        if translated_remainder:
            return f'Для горизонта {lead_days} дней: {translated_remainder}'
    if text in exact_replacements:
        return exact_replacements[text]

    for source, target in {**INTERVAL_METHOD_LABELS, **INTERVAL_SCHEME_LABELS}.items():
        text = text.replace(source, target)

    replacements = (
        (' was selected for validated out-of-sample coverage because ', ' выбрана для проверки покрытия на отложенных окнах, потому что '),
        ('it was more stable on later windows than ', 'она оказалась стабильнее на поздних окнах, чем '),
        ('it stayed at least as stable as ', 'она сохранила не меньшую стабильность, чем '),
        (' while refreshing calibration more often', ', при этом калибровка обновлялась чаще'),
        ('it gave the most stable forward-only out-of-sample coverage among the available validation schemes', 'она дала самое стабильное покрытие на отложенных окнах среди доступных временных схем проверки'),
        (' and improved coverage stability versus the previous fixed 60/40 chrono split', ' и улучшила стабильность покрытия по сравнению с прежним фиксированным хронологическим разбиением 60/40'),
        (' while remaining at least as stable as the previous fixed 60/40 chrono split', ' и при этом осталась не менее стабильной, чем прежнее фиксированное хронологическое разбиение 60/40'),
        (' was not adopted because an honest time-series variant would require leave-one-block-out refits for every checkpoint.', ' не выбрана, потому что честный вариант для временного ряда потребовал бы переобучения модели с исключением каждого блока по очереди на каждом контрольном шаге.'),
    )
    for source, target in replacements:
        text = text.replace(source, target)

    return _sentence_case(text)


def _translate_interval_range_label(label: Any) -> str:
    if _is_missing_metric(label):
        return ''

    normalized = str(label).strip()
    if not normalized:
        return ''
    if normalized == 'all available backtest windows':
        return 'все доступные окна проверки на истории'
    if normalized == 'not available':
        return 'недоступно'

    match = _FIRST_WINDOWS_RE.match(normalized)
    if match:
        count, end_date = match.groups()
        return f'первых {count} окнах до {end_date}' if end_date else f'первых {count} окнах'

    match = _LATER_WINDOWS_RE.match(normalized)
    if match:
        count, start_date = match.groups()
        return f'последних {count} окнах начиная с {start_date}' if start_date else f'последних {count} окнах'

    match = _ROLLING_WINDOWS_RE.match(normalized)
    if match:
        count, start_date = match.groups()
        return f'{count} окнах скользящей оценки начиная с {start_date}' if start_date else f'{count} окнах скользящей оценки'

    match = _BLOCKED_WINDOWS_RE.match(normalized)
    if match:
        count, start_date = match.groups()
        return f'{count} окнах блочной оценки начиная с {start_date}' if start_date else f'{count} окнах блочной оценки'

    return normalized


def _prediction_interval_scheme_label(overview: Dict[str, Any]) -> str:
    raw_label = overview.get('prediction_interval_validation_scheme_label')
    if _is_missing_metric(raw_label):
        return ''
    return _translate_interval_scheme_label(raw_label)


def _prediction_interval_method_label(ml_result: Dict[str, Any], overview: Dict[str, Any]) -> str:
    explicit_label = _first_present(
        ml_result.get('prediction_interval_method_label'),
        overview.get('prediction_interval_method_label'),
    )
    return _translate_interval_method_label(explicit_label)


def _prediction_interval_display_context(
    ml_result: Dict[str, Any],
    overview: Dict[str, Any],
) -> Dict[str, str]:
    method_label = _prediction_interval_method_label(ml_result, overview)
    method_label_display = _format_optional_text(method_label)
    level_display = _format_first_present(
        lambda item: str(item).strip(),
        ml_result.get('prediction_interval_level_display'),
        overview.get('prediction_interval_level_display'),
    )
    coverage_display = _format_first_present(
        lambda item: str(item).strip(),
        ml_result.get('prediction_interval_coverage_display'),
        overview.get('prediction_interval_coverage_display'),
    )
    return {
        'level_display': level_display,
        'coverage_display': coverage_display,
        'method_label_display': method_label_display,
        'method_label': method_label,
        'quality_note': _prediction_interval_quality_note(overview, coverage_display),
    }


def _event_probability_context(
    ml_result: Dict[str, Any],
    overview: Dict[str, Any],
) -> Dict[str, Optional[str]]:
    reason_code = _first_present(
        ml_result.get('event_probability_reason_code'),
        overview.get('event_probability_reason_code'),
    )
    note = _first_present(
        ml_result.get('event_probability_note'),
        overview.get('event_probability_note'),
    )
    normalized_reason_code = None if _is_missing_metric(reason_code) else str(reason_code).strip()
    normalized_note = None if _is_missing_metric(note) else str(note).strip()
    return {
        'reason_code': normalized_reason_code,
        'note': normalized_note,
    }


def _comparison_metric_card(
    label: str,
    value: Any,
    baseline_value: Any,
    heuristic_value: Any,
    formatter: Callable[[Any], str],
) -> Dict[str, str]:
    return {
        'label': label,
        'value': formatter(value),
        'meta': f"seasonal baseline: {formatter(baseline_value)}; heuristic forecast: {formatter(heuristic_value)}",
    }
def _count_comparison_row(row: Dict[str, Any]) -> Dict[str, str]:
    return {
        'method_label': row.get('method_label', 'Метод'),
        'role_label': row.get('role_label', ''),
        'selection_label': _selection_label(row.get('is_selected')),
        'mae_display': _format_optional_number(row.get('mae')),
        'rmse_display': _format_optional_number(row.get('rmse')),
        'smape_display': _format_optional_percent(row.get('smape')),
        'poisson_display': _format_optional_number(row.get('poisson_deviance')),
        'mae_delta_display': _format_optional_signed_percent(row.get('mae_delta_vs_baseline')),
    }


def _prediction_interval_quality_note(
    overview: Dict[str, Any],
    interval_coverage_display: str,
) -> str:
    validated_flag = overview.get('prediction_interval_coverage_validated')
    is_validated = (
        bool(validated_flag)
        if validated_flag is not None
        else interval_coverage_display not in {MISSING_DISPLAY, '-'}
    )
    scheme_label = _prediction_interval_scheme_label(overview) or 'проверка на истории'
    calibration_windows = int(overview.get('prediction_interval_calibration_windows') or 0)
    evaluation_windows = int(overview.get('prediction_interval_evaluation_windows') or 0)
    translated_explanation = _translate_interval_validation_explanation(
        _first_present(
            overview.get('prediction_interval_validation_explanation'),
            overview.get('prediction_interval_coverage_note'),
        )
    )
    calibration_range = _translate_interval_range_label(overview.get('prediction_interval_calibration_range_label'))
    evaluation_range = _translate_interval_range_label(overview.get('prediction_interval_evaluation_range_label'))

    if is_validated:
        parts: List[str] = []
        if translated_explanation:
            parts.append(translated_explanation)
        if evaluation_range and calibration_range:
            parts.append(
                f'Покрытие оценивается только на {evaluation_range} после начальной калибровки на {calibration_range}.'
            )
        elif calibration_windows and evaluation_windows:
            parts.append(
                f'Покрытие оценивается только на {evaluation_windows} окнах после начальной калибровки на {calibration_windows} окнах.'
            )
        else:
            parts.append(f'Покрытие проверено схемой: {scheme_label}.')
        parts.append('После проверки рабочие интервалы перекалибруются на всех доступных остатках скользящей проверки.')
        return ' '.join(part for part in parts if part)

    if translated_explanation:
        return translated_explanation

    if calibration_windows or evaluation_windows:
        return 'Покрытие на отложенных окнах пока не показывается: для честной временной проверки пока недостаточно скользящих окон.'
    return 'Покрытие на отложенных окнах пока не показывается: проверка интервалов на истории ещё не запускалась.'


def _join_meta_parts(*parts: Any) -> str:
    values: List[str] = []
    for part in parts:
        if _is_missing_metric(part):
            continue
        text = str(part).strip()
        if not text or text in values:
            continue
        values.append(text)
    return '; '.join(values)


def _prediction_interval_card_label(level_display: str) -> str:
    if level_display in {MISSING_DISPLAY, '-', ''}:
        return 'Покрытие интервала на отложенных окнах'
    return f'Покрытие {level_display} интервала на отложенных окнах'


def _build_prediction_interval_card(
    interval_context: Dict[str, str],
    interval_meta: str,
) -> Dict[str, str]:
    return {
        'label': _prediction_interval_card_label(interval_context['level_display']),
        'value': interval_context['coverage_display'],
        'meta': interval_meta,
    }


def _comparison_method_labels(ml_result: Dict[str, Any], overview: Dict[str, Any]) -> str:
    labels: List[str] = []
    for source in (
        overview.get('candidate_model_labels') or [],
        ml_result.get('candidate_count_model_labels') or [],
    ):
        for item in source:
            if _is_missing_metric(item):
                continue
            text = str(item).strip()
            if text and text not in labels:
                labels.append(text)

    if not labels:
        for row in ml_result.get('count_comparison_rows', []):
            label = row.get('method_label')
            if _is_missing_metric(label):
                continue
            text = str(label).strip()
            if text and text not in labels:
                labels.append(text)

    return ', '.join(labels) if labels else MISSING_DISPLAY


def _methodology_item(label: str, value: str, meta: str = '') -> Dict[str, str]:
    return {
        'label': label,
        'value': value,
        'meta': meta,
    }


def _model_choice_section(ml_result: Dict[str, Any], overview: Dict[str, Any]) -> Dict[str, Any]:
    working_method = _format_optional_text(ml_result.get('count_model_label'))
    short_reason = _format_optional_text(ml_result.get('selected_count_model_reason_short'))
    long_reason = _format_optional_text(ml_result.get('selected_count_model_reason'))
    top_feature_label = _format_optional_text(ml_result.get('top_feature_label'))

    return {
        'title': 'Почему выбран рабочий метод',
        'lead': (
            short_reason
            if short_reason != MISSING_DISPLAY
            else f'Рабочим count-методом оставлен {working_method}.'
        ),
        'body': (
            long_reason
            if long_reason != MISSING_DISPLAY
            else 'Выбор закреплён по результатам одинаковой проверки на истории для всех кандидатов.'
        ),
        'facts': [
            {
                'label': 'Рабочий count-метод',
                'value': working_method,
                'meta': _format_optional_text(ml_result.get('selected_count_model_key')),
            },
            {
                'label': 'Правило выбора',
                'value': _format_optional_text(overview.get('selection_rule')),
                'meta': _format_optional_text(overview.get('rolling_scheme_label')),
            },
            {
                'label': 'Главный признак',
                'value': top_feature_label,
                'meta': 'Permutation importance' if top_feature_label != MISSING_DISPLAY else '',
            },
        ],
    }


def _dissertation_points(
    ml_result: Dict[str, Any],
    interval_meta: str,
    event_context: Dict[str, Optional[str]],
) -> List[str]:
    points: List[str] = []
    for item in (
        ml_result.get('selected_count_model_reason_short'),
        ml_result.get('selected_count_model_reason'),
        interval_meta,
        event_context.get('note'),
    ):
        if _is_missing_metric(item):
            continue
        text = str(item).strip()
        if text and text not in points:
            points.append(text)

    if not points:
        points.append(
            'ML-блок сравнивает count-методы на одной и той же истории и отдельно показывает устойчивость прогноза.'
        )
    return points


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



def _build_importance_chart(feature_importance: List[Dict[str, Any]], note: str = '') -> Dict[str, Any]:
    title = 'Важность признаков ML-блока'
    if not feature_importance:
        payload = _empty_light_chart(title, 'Модель ещё не обучена: важность признаков появится после расчёта.', kind='bars')
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



def _build_summary(
    selected_table: str,
    selected_cause: str,
    selected_object_category: str,
    daily_history: List[Dict[str, Any]],
    filtered_records_count: int,
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
        f"Пик по горизонту ожидается {_format_optional_text(peak_row.get('date_display'))}: "
        f"ожидаемое число пожаров — {_format_row_display(peak_row, 'forecast_value_display', 'forecast_value', _format_optional_number)}. "
        f"Среднее ожидаемое значение по дням — {_format_optional_number(average_expected_count)}."
        if peak_row
        else 'После расчета здесь появится краткий вывод по ожидаемому числу пожаров на ближайшие даты.'
    )

    return {
        'selected_table_label': 'Все таблицы' if selected_table == 'all' else (selected_table or 'Нет таблицы'),
        'slice_label': ' | '.join(slice_parts) if slice_parts else 'Все пожары выбранной истории',
        'hero_summary': hero_summary,
        'history_period_label': _format_period(history_dates),
        'history_window_label': _history_window_label(history_window),
        'model_label': MODEL_NAME,
        'count_model_label': ml_result.get('count_model_label') or MODEL_NAME,
        'event_model_label': ml_result.get('event_model_label') or 'Не обучен',
        'event_backtest_model_label': ml_result.get('selected_event_model_label') or 'Не показан',
        'backtest_method_label': ml_result.get('backtest_method_label') or 'Проверка на истории не выполнена',
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
            f"{_format_number(scenario_temperature)} °C" if scenario_temperature is not None else 'Историческая температура'
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

def _build_quality_assessment(ml_result: Dict[str, Any]) -> Dict[str, Any]:
    overview = ml_result.get('backtest_overview', {}) or {}
    event_context = _event_probability_context(ml_result, overview)
    interval_context = _prediction_interval_display_context(ml_result, overview)
    count_rows = [_count_comparison_row(row) for row in ml_result.get('count_comparison_rows', [])]
    interval_meta = _join_meta_parts(
        interval_context['method_label_display'],
        interval_context['quality_note'],
    )

    count_metric_cards = [
        _comparison_metric_card(
            'MAE по числу пожаров',
            ml_result.get('count_mae'),
            ml_result.get('baseline_count_mae'),
            ml_result.get('heuristic_count_mae'),
            _format_optional_number,
        ),
        _comparison_metric_card(
            'RMSE по числу пожаров',
            ml_result.get('count_rmse'),
            ml_result.get('baseline_count_rmse'),
            ml_result.get('heuristic_count_rmse'),
            _format_optional_number,
        ),
        _comparison_metric_card(
            'sMAPE по числу пожаров',
            ml_result.get('count_smape'),
            ml_result.get('baseline_count_smape'),
            ml_result.get('heuristic_count_smape'),
            _format_optional_percent,
        ),
        _comparison_metric_card(
            'Poisson deviance',
            ml_result.get('count_poisson_deviance'),
            ml_result.get('baseline_count_poisson_deviance'),
            ml_result.get('heuristic_count_poisson_deviance'),
            _format_optional_number,
        ),
    ]
    event_metric_cards: List[Dict[str, str]] = []
    if ml_result.get('event_backtest_available'):
        event_metric_cards.extend(
            [
                _comparison_metric_card(
                    'Brier score',
                    ml_result.get('brier_score'),
                    ml_result.get('baseline_brier_score'),
                    ml_result.get('heuristic_brier_score'),
                    _format_optional_number,
                ),
                _comparison_metric_card(
                    'ROC-AUC',
                    ml_result.get('roc_auc'),
                    ml_result.get('baseline_roc_auc'),
                    ml_result.get('heuristic_roc_auc'),
                    _format_optional_number,
                ),
                _comparison_metric_card(
                    'F1',
                    ml_result.get('f1_score'),
                    ml_result.get('baseline_f1_score'),
                    ml_result.get('heuristic_f1_score'),
                    _format_optional_number,
                ),
                _comparison_metric_card(
                    'Log-loss',
                    ml_result.get('log_loss'),
                    ml_result.get('baseline_log_loss'),
                    ml_result.get('heuristic_log_loss'),
                    _format_optional_number,
                ),
            ]
        )

    return {
        'ready': bool(ml_result.get('is_ready')),
        'title': 'Оценка качества ML-блока',
        'subtitle': 'Ключевые метрики и сравнение методов на одной и той же истории. Блок проверяет именно прогноз числа пожаров, а не приоритет территорий.',
        'methodology_items': [
            _methodology_item(
                'Схема валидации',
                _format_optional_text(overview.get('rolling_scheme_label')),
                _join_meta_parts(
                    _format_optional_text(overview.get('validation_horizon_label') or overview.get('validation_horizon_days')),
                    _format_optional_text(overview.get('prediction_interval_validation_scheme_label')),
                ),
            ),
            _methodology_item(
                'Минимум обучающего окна',
                _format_optional_integer(overview.get('min_train_rows')),
            ),
            _methodology_item(
                'Сравниваемые count-методы',
                _comparison_method_labels(ml_result, overview),
            ),
            _methodology_item(
                'Индекс пере-дисперсии',
                _format_optional_number(overview.get('dispersion_ratio')),
            ),
            _methodology_item(
                'Правило выбора',
                _format_optional_text(overview.get('selection_rule')),
            ),
            _methodology_item(
                'Интервал прогноза',
                interval_context['level_display'],
                interval_meta,
            ),
        ],
        'interval_card': _build_prediction_interval_card(interval_context, interval_meta),
        'metric_cards': count_metric_cards,
        'event_metric_cards': event_metric_cards,
        'model_choice': _model_choice_section(ml_result, overview),
        'count_table': {
            'title': 'Сравнение по числу пожаров',
            'rows': count_rows,
            'empty_message': 'Сравнение seasonal baseline, heuristic forecast и count-model появится после проверки на истории.',
        },
        'event_probability_reason_code': event_context['reason_code'],
        'dissertation_points': _dissertation_points(ml_result, interval_meta, event_context),
    }

def _build_notes(
    preload_notes: List[str],
    metadata_items: List[Dict[str, Any]],
    filtered_records_count: int,
    daily_history: List[Dict[str, Any]],
    ml_result: Dict[str, Any],
    scenario_temperature: Optional[float],
    source_tables: List[str],
) -> List[str]:
    notes: List[str] = []

    def append_note(value: Any) -> None:
        if value is None:
            return
        text = str(value).strip()
        if not text or text in notes:
            return
        notes.append(text)

    for note in preload_notes:
        append_note(note)

    overview = ml_result.get('backtest_overview', {}) or {}
    event_context = _event_probability_context(ml_result, overview)
    if event_context['note'] and not ml_result.get('event_backtest_available'):
        append_note(event_context['note'])

    if filtered_records_count <= 0:
        append_note('После выбранных фильтров не осталось исторических пожаров для обучения ML-модели.')
    if ml_result.get('message'):
        append_note(ml_result['message'])
    if not ml_result.get('is_ready') and filtered_records_count > 0:
        append_note('Истории пока недостаточно, чтобы показать устойчивый ML-прогноз и проверку качества.')
    if len(daily_history) < 60:
        append_note('Истории меньше 60 дней: для корректной ML-валидации этого обычно недостаточно.')
    if scenario_temperature is not None and not any(item['resolved_columns'].get('temperature') for item in metadata_items):
        append_note(
            'Температура задана вручную, но температурная колонка в таблицах не найдена: '
            'сценарное значение используется только для будущих дат.'
        )

    if ml_result.get('temperature_note'):
        append_note(ml_result['temperature_note'])

    if len(source_tables) > 1 and not notes:
        append_note(f'ML-модель собирает общий прогноз сразу по {len(source_tables)} таблицам.')

    append_note('ML-экран показывает ожидаемое число пожаров по датам и не заменяет сценарный прогноз по вероятности пожара или ранжирование территорий.')

    return notes
