from __future__ import annotations

from typing import Any, Dict, List, Optional


from .presentation_format import (
    MISSING_DISPLAY,
    _first_present,
    _format_first_present,
    _format_optional_integer,
    _format_optional_number,
    _format_optional_percent,
    _format_optional_signed_percent,
    _format_optional_text,
    _format_optional_value,
    _format_row_display,
    _is_missing_metric,
)




def _event_probability_context(
    ml_result: dict[str, Any],
    overview: dict[str, Any],
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


def _build_notes(
    preload_notes: List[str],
    metadata_items: List[dict[str, Any]],
    filtered_records_count: int,
    daily_history: List[dict[str, Any]],
    ml_result: dict[str, Any],
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



__all__ = [
    'MISSING_DISPLAY',
    '_build_notes',
    '_event_probability_context',
    '_first_present',
    '_format_first_present',
    '_format_optional_integer',
    '_format_optional_number',
    '_format_optional_percent',
    '_format_optional_signed_percent',
    '_format_optional_text',
    '_format_optional_value',
    '_format_row_display',
    '_is_missing_metric',
]


