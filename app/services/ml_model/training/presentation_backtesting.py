from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional

from .presentation_meta import (
    MISSING_DISPLAY,
    _event_probability_context,
    _first_present,
    _format_first_present,
    _format_optional_integer,
    _format_optional_number,
    _format_optional_percent,
    _format_optional_signed_percent,
    _format_optional_text,
    _is_missing_metric,
)

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


def _event_comparison_row(row: Dict[str, Any]) -> Dict[str, str]:
    return {
        'method_label': row.get('method_label', 'Метод'),
        'role_label': row.get('role_label', ''),
        'selection_label': _selection_label(row.get('is_selected')),
        'brier_display': _format_optional_number(row.get('brier_score')),
        'roc_auc_display': _format_optional_number(row.get('roc_auc')),
        'f1_display': _format_optional_number(row.get('f1')),
        'log_loss_display': _format_optional_number(row.get('log_loss')),
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


def _build_event_table(
    ml_result: Dict[str, Any],
    event_context: Dict[str, Optional[str]],
) -> Dict[str, Any]:
    rows = [_event_comparison_row(row) for row in ml_result.get('event_comparison_rows', [])]
    return {
        'title': 'Сравнение по вероятности события пожара',
        'rows': rows,
        'empty_message': (
            event_context['note']
            or 'Сравнение seasonal baseline, heuristic probability и classifier появится после проверки на истории.'
        ),
        'reason_code': event_context['reason_code'],
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


def _build_quality_assessment(ml_result: Dict[str, Any]) -> Dict[str, Any]:
    overview = ml_result.get('backtest_overview', {}) or {}
    event_context = _event_probability_context(ml_result, overview)
    interval_context = _prediction_interval_display_context(ml_result, overview)
    count_rows = [_count_comparison_row(row) for row in ml_result.get('count_comparison_rows', [])]
    event_table = _build_event_table(ml_result, event_context)
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
        'event_table': event_table,
        'event_probability_reason_code': event_context['reason_code'],
        'dissertation_points': _dissertation_points(ml_result, interval_meta, event_context),
    }



__all__ = [
    'INTERVAL_METHOD_LABELS',
    'INTERVAL_SCHEME_LABELS',
    '_BLOCKED_WINDOWS_RE',
    '_FIRST_WINDOWS_RE',
    '_LATER_WINDOWS_RE',
    '_LEAD_TIME_PREFIX_RE',
    '_ROLLING_WINDOWS_RE',
    '_build_event_table',
    '_build_prediction_interval_card',
    '_build_quality_assessment',
    '_comparison_method_labels',
    '_comparison_metric_card',
    '_count_comparison_row',
    '_dissertation_points',
    '_event_comparison_row',
    '_join_meta_parts',
    '_methodology_item',
    '_model_choice_section',
    '_prediction_interval_card_label',
    '_prediction_interval_display_context',
    '_prediction_interval_method_label',
    '_prediction_interval_quality_note',
    '_prediction_interval_scheme_label',
    '_selection_label',
    '_sentence_case',
    '_translate_interval_method_label',
    '_translate_interval_range_label',
    '_translate_interval_scheme_label',
    '_translate_interval_validation_explanation',
]
