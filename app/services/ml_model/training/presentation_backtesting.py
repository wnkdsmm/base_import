from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional

from ..ml_model_types import BacktestOverview, CountComparisonRow, EventComparisonRow
from .types import (
    BacktestEventTable,
    BacktestQualityAssessment,
    MlBacktestPresentationResult,
    ModelChoiceSection,
    PredictionIntervalDisplayContext,
)
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
    'Forward rolling split conformal': 'СЃРєРѕР»СЊР·СЏС‰Р°СЏ РїСЂРѕРІРµСЂРєР° РїРѕ РёСЃС‚РѕСЂРёРё',
    'Blocked forward CV conformal': 'Р±Р»РѕС‡РЅР°СЏ РїСЂРѕРІРµСЂРєР° РїРѕ РёСЃС‚РѕСЂРёРё',
    'Fixed 60/40 chrono split conformal': 'С„РёРєСЃРёСЂРѕРІР°РЅРЅРѕРµ С…СЂРѕРЅРѕР»РѕРіРёС‡РµСЃРєРѕРµ СЂР°Р·Р±РёРµРЅРёРµ 60/40',
    'Jackknife+ for time series': 'jackknife+ РґР»СЏ РІСЂРµРјРµРЅРЅРѕРіРѕ СЂСЏРґР°',
    'validated out-of-sample coverage unavailable': 'РїСЂРѕРІРµСЂРєР° РїРѕРєСЂС‹С‚РёСЏ РїРѕРєР° РЅРµРґРѕСЃС‚СѓРїРЅР°',
}
INTERVAL_METHOD_LABELS = {
    'Adaptive conformal interval with predicted-count bins': 'РђРґР°РїС‚РёРІРЅС‹Р№ РєРѕРЅС„РѕСЂРјРЅС‹Р№ РёРЅС‚РµСЂРІР°Р» РїРѕ РіСЂСѓРїРїР°Рј РѕР¶РёРґР°РµРјРѕРіРѕ С‡РёСЃР»Р° РїРѕР¶Р°СЂРѕРІ',
}
_FIRST_WINDOWS_RE = re.compile(r'^first (\d+) windows(?: through (.+))?$')
_LATER_WINDOWS_RE = re.compile(r'^later (\d+) windows(?: from (.+))?$')
_ROLLING_WINDOWS_RE = re.compile(r'^rolling evaluation (\d+) windows(?: from (.+))?$')
_BLOCKED_WINDOWS_RE = re.compile(r'^blocked evaluation (\d+) windows(?: from (.+))?$')
_LEAD_TIME_PREFIX_RE = re.compile(r'^For the (\d+)-day lead, (.+)$')


def _selection_label(is_selected: Any) -> str:
    return 'Р Р°Р±РѕС‡РёР№ РјРµС‚РѕРґ' if bool(is_selected) else 'РЎСЂР°РІРЅРµРЅРёРµ'


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
        return f'{translated_base}; РїСЂРѕРІРµСЂРєР° РїРѕРєСЂС‹С‚РёСЏ РЅР° РѕС‚Р»РѕР¶РµРЅРЅС‹С… РѕРєРЅР°С… РїРѕРєР° РЅРµРґРѕСЃС‚СѓРїРЅР°'

    if '; validated by ' in normalized:
        base_label, scheme_label = normalized.split('; validated by ', 1)
        translated_base = INTERVAL_METHOD_LABELS.get(base_label.strip(), base_label.strip())
        translated_scheme = _translate_interval_scheme_label(scheme_label)
        return f'{translated_base}; РїСЂРѕРІРµСЂРєР° СЃС…РµРјРѕР№: {translated_scheme}'

    if '; validation baseline: ' in normalized:
        base_label, scheme_label = normalized.split('; validation baseline: ', 1)
        translated_base = INTERVAL_METHOD_LABELS.get(base_label.strip(), base_label.strip())
        translated_scheme = _translate_interval_scheme_label(scheme_label)
        return f'{translated_base}; Р±Р°Р·РѕРІР°СЏ СЃС…РµРјР° РїСЂРѕРІРµСЂРєРё: {translated_scheme}'

    if '; validation candidate: ' in normalized:
        base_label, scheme_label = normalized.split('; validation candidate: ', 1)
        translated_base = INTERVAL_METHOD_LABELS.get(base_label.strip(), base_label.strip())
        translated_scheme = _translate_interval_scheme_label(scheme_label)
        return f'{translated_base}; РєР°РЅРґРёРґР°С‚ РїСЂРѕРІРµСЂРєРё: {translated_scheme}'

    return INTERVAL_METHOD_LABELS.get(normalized, normalized)


def _translate_interval_validation_explanation(explanation: Any) -> str:
    if _is_missing_metric(explanation):
        return ''

    text = str(explanation).strip()
    if not text:
        return ''

    exact_replacements = {
        'Validated out-of-sample coverage is unavailable because backtesting was not run.': (
            'РџРѕРєСЂС‹С‚РёРµ РЅР° РѕС‚Р»РѕР¶РµРЅРЅС‹С… РѕРєРЅР°С… РїРѕРєР° РЅРµРґРѕСЃС‚СѓРїРЅРѕ: РїСЂРѕРІРµСЂРєР° РёРЅС‚РµСЂРІР°Р»РѕРІ РЅР° РёСЃС‚РѕСЂРёРё РµС‰С‘ РЅРµ Р·Р°РїСѓСЃРєР°Р»Р°СЃСЊ.'
        ),
        'Validated out-of-sample coverage is unavailable because the backtest has too few rolling-origin windows for forward-only interval validation.': (
            'РџРѕРєСЂС‹С‚РёРµ РЅР° РѕС‚Р»РѕР¶РµРЅРЅС‹С… РѕРєРЅР°С… РїРѕРєР° РЅРµРґРѕСЃС‚СѓРїРЅРѕ: РІ РїСЂРѕРІРµСЂРєРµ РЅР° РёСЃС‚РѕСЂРёРё СЃР»РёС€РєРѕРј РјР°Р»Рѕ СЃРєРѕР»СЊР·СЏС‰РёС… РѕРєРѕРЅ РґР»СЏ С‡РµСЃС‚РЅРѕР№ РїРѕСЃР»РµРґРѕРІР°С‚РµР»СЊРЅРѕР№ РїСЂРѕРІРµСЂРєРё РёРЅС‚РµСЂРІР°Р»Р°.'
        ),
    }
    lead_time_match = _LEAD_TIME_PREFIX_RE.match(text)
    if lead_time_match:
        lead_days, remainder = lead_time_match.groups()
        translated_remainder = exact_replacements.get(remainder)
        if translated_remainder:
            return f'Р”Р»СЏ РіРѕСЂРёР·РѕРЅС‚Р° {lead_days} РґРЅРµР№: {translated_remainder}'
    if text in exact_replacements:
        return exact_replacements[text]

    for source, target in {**INTERVAL_METHOD_LABELS, **INTERVAL_SCHEME_LABELS}.items():
        text = text.replace(source, target)

    replacements = (
        (' was selected for validated out-of-sample coverage because ', ' РІС‹Р±СЂР°РЅР° РґР»СЏ РїСЂРѕРІРµСЂРєРё РїРѕРєСЂС‹С‚РёСЏ РЅР° РѕС‚Р»РѕР¶РµРЅРЅС‹С… РѕРєРЅР°С…, РїРѕС‚РѕРјСѓ С‡С‚Рѕ '),
        ('it was more stable on later windows than ', 'РѕРЅР° РѕРєР°Р·Р°Р»Р°СЃСЊ СЃС‚Р°Р±РёР»СЊРЅРµРµ РЅР° РїРѕР·РґРЅРёС… РѕРєРЅР°С…, С‡РµРј '),
        ('it stayed at least as stable as ', 'РѕРЅР° СЃРѕС…СЂР°РЅРёР»Р° РЅРµ РјРµРЅСЊС€СѓСЋ СЃС‚Р°Р±РёР»СЊРЅРѕСЃС‚СЊ, С‡РµРј '),
        (' while refreshing calibration more often', ', РїСЂРё СЌС‚РѕРј РєР°Р»РёР±СЂРѕРІРєР° РѕР±РЅРѕРІР»СЏР»Р°СЃСЊ С‡Р°С‰Рµ'),
        ('it gave the most stable forward-only out-of-sample coverage among the available validation schemes', 'РѕРЅР° РґР°Р»Р° СЃР°РјРѕРµ СЃС‚Р°Р±РёР»СЊРЅРѕРµ РїРѕРєСЂС‹С‚РёРµ РЅР° РѕС‚Р»РѕР¶РµРЅРЅС‹С… РѕРєРЅР°С… СЃСЂРµРґРё РґРѕСЃС‚СѓРїРЅС‹С… РІСЂРµРјРµРЅРЅС‹С… СЃС…РµРј РїСЂРѕРІРµСЂРєРё'),
        (' and improved coverage stability versus the previous fixed 60/40 chrono split', ' Рё СѓР»СѓС‡С€РёР»Р° СЃС‚Р°Р±РёР»СЊРЅРѕСЃС‚СЊ РїРѕРєСЂС‹С‚РёСЏ РїРѕ СЃСЂР°РІРЅРµРЅРёСЋ СЃ РїСЂРµР¶РЅРёРј С„РёРєСЃРёСЂРѕРІР°РЅРЅС‹Рј С…СЂРѕРЅРѕР»РѕРіРёС‡РµСЃРєРёРј СЂР°Р·Р±РёРµРЅРёРµРј 60/40'),
        (' while remaining at least as stable as the previous fixed 60/40 chrono split', ' Рё РїСЂРё СЌС‚РѕРј РѕСЃС‚Р°Р»Р°СЃСЊ РЅРµ РјРµРЅРµРµ СЃС‚Р°Р±РёР»СЊРЅРѕР№, С‡РµРј РїСЂРµР¶РЅРµРµ С„РёРєСЃРёСЂРѕРІР°РЅРЅРѕРµ С…СЂРѕРЅРѕР»РѕРіРёС‡РµСЃРєРѕРµ СЂР°Р·Р±РёРµРЅРёРµ 60/40'),
        (' was not adopted because an honest time-series variant would require leave-one-block-out refits for every checkpoint.', ' РЅРµ РІС‹Р±СЂР°РЅР°, РїРѕС‚РѕРјСѓ С‡С‚Рѕ С‡РµСЃС‚РЅС‹Р№ РІР°СЂРёР°РЅС‚ РґР»СЏ РІСЂРµРјРµРЅРЅРѕРіРѕ СЂСЏРґР° РїРѕС‚СЂРµР±РѕРІР°Р» Р±С‹ РїРµСЂРµРѕР±СѓС‡РµРЅРёСЏ РјРѕРґРµР»Рё СЃ РёСЃРєР»СЋС‡РµРЅРёРµРј РєР°Р¶РґРѕРіРѕ Р±Р»РѕРєР° РїРѕ РѕС‡РµСЂРµРґРё РЅР° РєР°Р¶РґРѕРј РєРѕРЅС‚СЂРѕР»СЊРЅРѕРј С€Р°РіРµ.'),
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
        return 'РІСЃРµ РґРѕСЃС‚СѓРїРЅС‹Рµ РѕРєРЅР° РїСЂРѕРІРµСЂРєРё РЅР° РёСЃС‚РѕСЂРёРё'
    if normalized == 'not available':
        return 'РЅРµРґРѕСЃС‚СѓРїРЅРѕ'

    match = _FIRST_WINDOWS_RE.match(normalized)
    if match:
        count, end_date = match.groups()
        return f'РїРµСЂРІС‹С… {count} РѕРєРЅР°С… РґРѕ {end_date}' if end_date else f'РїРµСЂРІС‹С… {count} РѕРєРЅР°С…'

    match = _LATER_WINDOWS_RE.match(normalized)
    if match:
        count, start_date = match.groups()
        return f'РїРѕСЃР»РµРґРЅРёС… {count} РѕРєРЅР°С… РЅР°С‡РёРЅР°СЏ СЃ {start_date}' if start_date else f'РїРѕСЃР»РµРґРЅРёС… {count} РѕРєРЅР°С…'

    match = _ROLLING_WINDOWS_RE.match(normalized)
    if match:
        count, start_date = match.groups()
        return f'{count} РѕРєРЅР°С… СЃРєРѕР»СЊР·СЏС‰РµР№ РѕС†РµРЅРєРё РЅР°С‡РёРЅР°СЏ СЃ {start_date}' if start_date else f'{count} РѕРєРЅР°С… СЃРєРѕР»СЊР·СЏС‰РµР№ РѕС†РµРЅРєРё'

    match = _BLOCKED_WINDOWS_RE.match(normalized)
    if match:
        count, start_date = match.groups()
        return f'{count} РѕРєРЅР°С… Р±Р»РѕС‡РЅРѕР№ РѕС†РµРЅРєРё РЅР°С‡РёРЅР°СЏ СЃ {start_date}' if start_date else f'{count} РѕРєРЅР°С… Р±Р»РѕС‡РЅРѕР№ РѕС†РµРЅРєРё'

    return normalized


def _prediction_interval_scheme_label(overview: BacktestOverview) -> str:
    raw_label = overview.get('prediction_interval_validation_scheme_label')
    if _is_missing_metric(raw_label):
        return ''
    return _translate_interval_scheme_label(raw_label)


def _prediction_interval_method_label(ml_result: MlBacktestPresentationResult, overview: BacktestOverview) -> str:
    explicit_label = _first_present(
        ml_result.get('prediction_interval_method_label'),
        overview.get('prediction_interval_method_label'),
    )
    return _translate_interval_method_label(explicit_label)


def _prediction_interval_display_context(
    ml_result: MlBacktestPresentationResult,
    overview: BacktestOverview,
) -> PredictionIntervalDisplayContext:
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


def _count_comparison_row(row: CountComparisonRow) -> Dict[str, str]:
    return {
        'method_label': row.get('method_label', 'РњРµС‚РѕРґ'),
        'role_label': row.get('role_label', ''),
        'selection_label': _selection_label(row.get('is_selected')),
        'mae_display': _format_optional_number(row.get('mae')),
        'rmse_display': _format_optional_number(row.get('rmse')),
        'smape_display': _format_optional_percent(row.get('smape')),
        'poisson_display': _format_optional_number(row.get('poisson_deviance')),
        'mae_delta_display': _format_optional_signed_percent(row.get('mae_delta_vs_baseline')),
    }


def _event_comparison_row(row: EventComparisonRow) -> Dict[str, str]:
    return {
        'method_label': row.get('method_label', 'РњРµС‚РѕРґ'),
        'role_label': row.get('role_label', ''),
        'selection_label': _selection_label(row.get('is_selected')),
        'brier_display': _format_optional_number(row.get('brier_score')),
        'roc_auc_display': _format_optional_number(row.get('roc_auc')),
        'f1_display': _format_optional_number(row.get('f1')),
        'log_loss_display': _format_optional_number(row.get('log_loss')),
    }


def _prediction_interval_quality_note(
    overview: BacktestOverview,
    interval_coverage_display: str,
) -> str:
    validated_flag = overview.get('prediction_interval_coverage_validated')
    is_validated = (
        bool(validated_flag)
        if validated_flag is not None
        else interval_coverage_display not in {MISSING_DISPLAY, '-'}
    )
    scheme_label = _prediction_interval_scheme_label(overview) or 'РїСЂРѕРІРµСЂРєР° РЅР° РёСЃС‚РѕСЂРёРё'
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
                f'РџРѕРєСЂС‹С‚РёРµ РѕС†РµРЅРёРІР°РµС‚СЃСЏ С‚РѕР»СЊРєРѕ РЅР° {evaluation_range} РїРѕСЃР»Рµ РЅР°С‡Р°Р»СЊРЅРѕР№ РєР°Р»РёР±СЂРѕРІРєРё РЅР° {calibration_range}.'
            )
        elif calibration_windows and evaluation_windows:
            parts.append(
                f'РџРѕРєСЂС‹С‚РёРµ РѕС†РµРЅРёРІР°РµС‚СЃСЏ С‚РѕР»СЊРєРѕ РЅР° {evaluation_windows} РѕРєРЅР°С… РїРѕСЃР»Рµ РЅР°С‡Р°Р»СЊРЅРѕР№ РєР°Р»РёР±СЂРѕРІРєРё РЅР° {calibration_windows} РѕРєРЅР°С….'
            )
        else:
            parts.append(f'РџРѕРєСЂС‹С‚РёРµ РїСЂРѕРІРµСЂРµРЅРѕ СЃС…РµРјРѕР№: {scheme_label}.')
        parts.append('РџРѕСЃР»Рµ РїСЂРѕРІРµСЂРєРё СЂР°Р±РѕС‡РёРµ РёРЅС‚РµСЂРІР°Р»С‹ РїРµСЂРµРєР°Р»РёР±СЂСѓСЋС‚СЃСЏ РЅР° РІСЃРµС… РґРѕСЃС‚СѓРїРЅС‹С… РѕСЃС‚Р°С‚РєР°С… СЃРєРѕР»СЊР·СЏС‰РµР№ РїСЂРѕРІРµСЂРєРё.')
        return ' '.join(part for part in parts if part)

    if translated_explanation:
        return translated_explanation

    if calibration_windows or evaluation_windows:
        return 'РџРѕРєСЂС‹С‚РёРµ РЅР° РѕС‚Р»РѕР¶РµРЅРЅС‹С… РѕРєРЅР°С… РїРѕРєР° РЅРµ РїРѕРєР°Р·С‹РІР°РµС‚СЃСЏ: РґР»СЏ С‡РµСЃС‚РЅРѕР№ РІСЂРµРјРµРЅРЅРѕР№ РїСЂРѕРІРµСЂРєРё РїРѕРєР° РЅРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ СЃРєРѕР»СЊР·СЏС‰РёС… РѕРєРѕРЅ.'
    return 'РџРѕРєСЂС‹С‚РёРµ РЅР° РѕС‚Р»РѕР¶РµРЅРЅС‹С… РѕРєРЅР°С… РїРѕРєР° РЅРµ РїРѕРєР°Р·С‹РІР°РµС‚СЃСЏ: РїСЂРѕРІРµСЂРєР° РёРЅС‚РµСЂРІР°Р»РѕРІ РЅР° РёСЃС‚РѕСЂРёРё РµС‰С‘ РЅРµ Р·Р°РїСѓСЃРєР°Р»Р°СЃСЊ.'


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
        return 'РџРѕРєСЂС‹С‚РёРµ РёРЅС‚РµСЂРІР°Р»Р° РЅР° РѕС‚Р»РѕР¶РµРЅРЅС‹С… РѕРєРЅР°С…'
    return f'РџРѕРєСЂС‹С‚РёРµ {level_display} РёРЅС‚РµСЂРІР°Р»Р° РЅР° РѕС‚Р»РѕР¶РµРЅРЅС‹С… РѕРєРЅР°С…'


def _build_prediction_interval_card(
    interval_context: PredictionIntervalDisplayContext,
    interval_meta: str,
) -> Dict[str, str]:
    return {
        'label': _prediction_interval_card_label(interval_context['level_display']),
        'value': interval_context['coverage_display'],
        'meta': interval_meta,
    }


def _build_event_table(
    ml_result: MlBacktestPresentationResult,
    event_context: Dict[str, Optional[str]],
) -> BacktestEventTable:
    rows = [_event_comparison_row(row) for row in ml_result.get('event_comparison_rows', [])]
    return {
        'title': 'РЎСЂР°РІРЅРµРЅРёРµ РїРѕ РІРµСЂРѕСЏС‚РЅРѕСЃС‚Рё СЃРѕР±С‹С‚РёСЏ РїРѕР¶Р°СЂР°',
        'rows': rows,
        'empty_message': (
            event_context['note']
            or 'РЎСЂР°РІРЅРµРЅРёРµ seasonal baseline, heuristic probability Рё classifier РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ РїСЂРѕРІРµСЂРєРё РЅР° РёСЃС‚РѕСЂРёРё.'
        ),
        'reason_code': event_context['reason_code'],
    }


def _comparison_method_labels(ml_result: MlBacktestPresentationResult, overview: BacktestOverview) -> str:
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


def _model_choice_section(ml_result: MlBacktestPresentationResult, overview: BacktestOverview) -> ModelChoiceSection:
    working_method = _format_optional_text(ml_result.get('count_model_label'))
    short_reason = _format_optional_text(ml_result.get('selected_count_model_reason_short'))
    long_reason = _format_optional_text(ml_result.get('selected_count_model_reason'))
    top_feature_label = _format_optional_text(ml_result.get('top_feature_label'))

    return {
        'title': 'РџРѕС‡РµРјСѓ РІС‹Р±СЂР°РЅ СЂР°Р±РѕС‡РёР№ РјРµС‚РѕРґ',
        'lead': (
            short_reason
            if short_reason != MISSING_DISPLAY
            else f'Р Р°Р±РѕС‡РёРј count-РјРµС‚РѕРґРѕРј РѕСЃС‚Р°РІР»РµРЅ {working_method}.'
        ),
        'body': (
            long_reason
            if long_reason != MISSING_DISPLAY
            else 'Р’С‹Р±РѕСЂ Р·Р°РєСЂРµРїР»С‘РЅ РїРѕ СЂРµР·СѓР»СЊС‚Р°С‚Р°Рј РѕРґРёРЅР°РєРѕРІРѕР№ РїСЂРѕРІРµСЂРєРё РЅР° РёСЃС‚РѕСЂРёРё РґР»СЏ РІСЃРµС… РєР°РЅРґРёРґР°С‚РѕРІ.'
        ),
        'facts': [
            {
                'label': 'Р Р°Р±РѕС‡РёР№ count-РјРµС‚РѕРґ',
                'value': working_method,
                'meta': _format_optional_text(ml_result.get('selected_count_model_key')),
            },
            {
                'label': 'РџСЂР°РІРёР»Рѕ РІС‹Р±РѕСЂР°',
                'value': _format_optional_text(overview.get('selection_rule')),
                'meta': _format_optional_text(overview.get('rolling_scheme_label')),
            },
            {
                'label': 'Р“Р»Р°РІРЅС‹Р№ РїСЂРёР·РЅР°Рє',
                'value': top_feature_label,
                'meta': 'Permutation importance' if top_feature_label != MISSING_DISPLAY else '',
            },
        ],
    }


def _dissertation_points(
    ml_result: MlBacktestPresentationResult,
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
            'ML-Р±Р»РѕРє СЃСЂР°РІРЅРёРІР°РµС‚ count-РјРµС‚РѕРґС‹ РЅР° РѕРґРЅРѕР№ Рё С‚РѕР№ Р¶Рµ РёСЃС‚РѕСЂРёРё Рё РѕС‚РґРµР»СЊРЅРѕ РїРѕРєР°Р·С‹РІР°РµС‚ СѓСЃС‚РѕР№С‡РёРІРѕСЃС‚СЊ РїСЂРѕРіРЅРѕР·Р°.'
        )
    return points


def _build_quality_assessment(ml_result: MlBacktestPresentationResult) -> BacktestQualityAssessment:
    overview = BacktestOverview.coerce(ml_result.get('backtest_overview', {}) or {})
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
            'MAE РїРѕ С‡РёСЃР»Сѓ РїРѕР¶Р°СЂРѕРІ',
            ml_result.get('count_mae'),
            ml_result.get('baseline_count_mae'),
            ml_result.get('heuristic_count_mae'),
            _format_optional_number,
        ),
        _comparison_metric_card(
            'RMSE РїРѕ С‡РёСЃР»Сѓ РїРѕР¶Р°СЂРѕРІ',
            ml_result.get('count_rmse'),
            ml_result.get('baseline_count_rmse'),
            ml_result.get('heuristic_count_rmse'),
            _format_optional_number,
        ),
        _comparison_metric_card(
            'sMAPE РїРѕ С‡РёСЃР»Сѓ РїРѕР¶Р°СЂРѕРІ',
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
        'title': 'РћС†РµРЅРєР° РєР°С‡РµСЃС‚РІР° ML-Р±Р»РѕРєР°',
        'subtitle': 'РљР»СЋС‡РµРІС‹Рµ РјРµС‚СЂРёРєРё Рё СЃСЂР°РІРЅРµРЅРёРµ РјРµС‚РѕРґРѕРІ РЅР° РѕРґРЅРѕР№ Рё С‚РѕР№ Р¶Рµ РёСЃС‚РѕСЂРёРё. Р‘Р»РѕРє РїСЂРѕРІРµСЂСЏРµС‚ РёРјРµРЅРЅРѕ РїСЂРѕРіРЅРѕР· С‡РёСЃР»Р° РїРѕР¶Р°СЂРѕРІ, Р° РЅРµ РїСЂРёРѕСЂРёС‚РµС‚ С‚РµСЂСЂРёС‚РѕСЂРёР№.',
        'methodology_items': [
            _methodology_item(
                'РЎС…РµРјР° РІР°Р»РёРґР°С†РёРё',
                _format_optional_text(overview.get('rolling_scheme_label')),
                _join_meta_parts(
                    _format_optional_text(overview.get('validation_horizon_label') or overview.get('validation_horizon_days')),
                    _format_optional_text(overview.get('prediction_interval_validation_scheme_label')),
                ),
            ),
            _methodology_item(
                'РњРёРЅРёРјСѓРј РѕР±СѓС‡Р°СЋС‰РµРіРѕ РѕРєРЅР°',
                _format_optional_integer(overview.get('min_train_rows')),
            ),
            _methodology_item(
                'РЎСЂР°РІРЅРёРІР°РµРјС‹Рµ count-РјРµС‚РѕРґС‹',
                _comparison_method_labels(ml_result, overview),
            ),
            _methodology_item(
                'РРЅРґРµРєСЃ РїРµСЂРµ-РґРёСЃРїРµСЂСЃРёРё',
                _format_optional_number(overview.get('dispersion_ratio')),
            ),
            _methodology_item(
                'РџСЂР°РІРёР»Рѕ РІС‹Р±РѕСЂР°',
                _format_optional_text(overview.get('selection_rule')),
            ),
            _methodology_item(
                'РРЅС‚РµСЂРІР°Р» РїСЂРѕРіРЅРѕР·Р°',
                interval_context['level_display'],
                interval_meta,
            ),
        ],
        'interval_card': _build_prediction_interval_card(interval_context, interval_meta),
        'metric_cards': count_metric_cards,
        'event_metric_cards': event_metric_cards,
        'model_choice': _model_choice_section(ml_result, overview),
        'count_table': {
            'title': 'РЎСЂР°РІРЅРµРЅРёРµ РїРѕ С‡РёСЃР»Сѓ РїРѕР¶Р°СЂРѕРІ',
            'rows': count_rows,
            'empty_message': 'РЎСЂР°РІРЅРµРЅРёРµ seasonal baseline, heuristic forecast Рё count-model РїРѕСЏРІРёС‚СЃСЏ РїРѕСЃР»Рµ РїСЂРѕРІРµСЂРєРё РЅР° РёСЃС‚РѕСЂРёРё.',
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
