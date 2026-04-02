from __future__ import annotations

from typing import Any, Dict, List, Sequence

from .constants import LONG_RESPONSE_THRESHOLD_MINUTES
from .utils import _unique_non_empty


def _build_risk_notes(
    feature_cards: Sequence[Dict[str, Any]],
    preload_notes: Sequence[str],
    weight_profile: Dict[str, Any],
    historical_validation: Dict[str, Any],
) -> List[str]:
    notes = list(preload_notes[:2])
    missing_labels = {item["label"] for item in feature_cards if item["status"] == "missing"}
    partial_labels = {item["label"] for item in feature_cards if item["status"] == "partial"}

    if "Подъездные пути" in missing_labels:
        notes.append("Колонки о состоянии или протяжённости подъездных путей не найдены, поэтому логистический риск оценивается по удалённости от ПЧ и фактическому времени прибытия.")
    if "Плотность населения" in missing_labels:
        notes.append("Плотность населения не найдена в текущих таблицах, поэтому демографическая нагрузка пока не влияет на интегральный балл напрямую.")
    if "Погодные условия" in missing_labels or "Погодные условия" in partial_labels:
        notes.append("Погодные условия представлены не полностью; сейчас сервис использует доступную температуру как основной погодный сигнал.")

    notes.append("Итоговый риск всегда раскладывается на четыре компонента: частоту пожаров, тяжесть последствий, долгое прибытие и дефицит водоснабжения.")
    notes.append("Логистический компонент остаётся объяснимым: сервис отдельно показывает travel-time, покрытие ПЧ, сервисную зону и логистический приоритет территории.")
    notes.append(
        f"Текущий профиль весов: {weight_profile.get('mode_label') or 'Экспертные веса'} ({weight_profile.get('status_label') or 'активен'}); "
        "для сельских территорий вес логистики и водоснабжения увеличивается автоматически."
    )
    if weight_profile.get("calibration_summary"):
        notes.append(str(weight_profile.get("calibration_summary")))
    notes.append(
        f"Большое время прибытия считается по порогу {int(LONG_RESPONSE_THRESHOLD_MINUTES)} минут от сообщения до прибытия первого подразделения."
    )
    notes.append(
        f"Историческая проверка ранжирования: {historical_validation.get('status_label') or 'пока без проверки'}. "
        "Это rolling-origin backtesting по историческим окнам, а не ручная экспертная оценка."
    )
    notes.append(
        "Компонентный балл риска не равен ни сценарию по дням, ни ML-прогнозу количества пожаров. "
        "Он показывает управленческий приоритет территории и объясняет, почему она стоит выше или ниже в списке."
    )
    return _unique_non_empty(notes)[:7]
