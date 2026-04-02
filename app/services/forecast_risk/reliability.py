from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from .utils import _clamp, _format_integer, _format_number, _format_probability


def _attach_ranking_reliability(
    territories: Sequence[Dict[str, Any]],
    quality_passport: Dict[str, Any],
    historical_validation: Dict[str, Any],
) -> list[Dict[str, Any]]:
    if not territories:
        return []

    annotated = [dict(item) for item in territories]
    metrics = historical_validation.get("metrics_raw") or {}
    windows_count = int(metrics.get("windows_count") or 0)
    k_value = int(metrics.get("k_value") or 3)
    validation_ready = bool(historical_validation.get("has_metrics")) and windows_count >= 3
    passport_score = float(quality_passport.get("confidence_score") or 0.0) / 100.0
    objective_score = float(metrics.get("objective_score") or 0.0)
    topk_capture = float(metrics.get("topk_capture_rate") or 0.0)
    precision_at_k = float(metrics.get("precision_at_k") or 0.0)
    ndcg_at_k = float(metrics.get("ndcg_at_k") or 0.0)

    for index, territory in enumerate(annotated):
        history_support = min(1.0, float(territory.get("history_count") or 0.0) / 8.0)
        if index == 0:
            margin_support = min(1.0, float(territory.get("ranking_gap_to_next") or 0.0) / 8.0)
        else:
            margin_support = 1.0 - min(1.0, float(territory.get("ranking_gap_to_top") or 0.0) / 12.0)
        local_support = _clamp(0.58 * margin_support + 0.42 * history_support, 0.15, 1.0)

        if validation_ready:
            confidence_norm = _clamp(0.42 * passport_score + 0.38 * objective_score + 0.20 * local_support, 0.18, 0.96)
        else:
            confidence_norm = _clamp(0.67 * passport_score + 0.33 * local_support, 0.16, 0.88)

        if index == 0:
            confidence_norm = _clamp(confidence_norm + 0.03, 0.18, 0.96)
        elif index >= 3:
            confidence_norm = _clamp(confidence_norm - 0.04, 0.16, 0.92)

        confidence_score = int(round(confidence_norm * 100.0))
        label, tone, prefix = _ranking_confidence_state(confidence_score)

        if validation_ready:
            history_clause = (
                f"rolling-origin проверка на {_format_integer(windows_count)} окнах даёт Top-{k_value} capture "
                f"{_format_probability(topk_capture)}, Precision@{k_value} {_format_probability(precision_at_k)} "
                f"и NDCG@{k_value} {_format_number(ndcg_at_k)}"
            )
        else:
            history_clause = (
                f"полной rolling-origin проверки пока нет, поэтому опора идёт на паспорт данных "
                f"{quality_passport.get('confidence_score_display') or '0 / 100'}"
            )

        margin_clause = (
            f"отрыв от следующей территории {territory.get('ranking_gap_to_next_display') or '0 баллов'}"
            if index == 0
            else f"отставание от лидера {territory.get('ranking_gap_to_top_display') or '0 баллов'}"
        )
        component_clause = territory.get("ranking_component_lead") or territory.get("drivers_display") or "компоненты риска территории"
        territory.update(
            {
                "ranking_confidence_score": confidence_score,
                "ranking_confidence_display": f"{confidence_score} / 100",
                "ranking_confidence_label": label,
                "ranking_confidence_tone": tone,
                "ranking_confidence_note": f"{prefix}: {history_clause}; {margin_clause}; основной вклад дают {component_clause}.",
            }
        )

    return annotated


def _top_territory_confidence_payload(
    top_territory: Optional[Dict[str, Any]],
    quality_passport: Dict[str, Any],
) -> Dict[str, str]:
    if top_territory:
        return {
            "label": top_territory.get("ranking_confidence_label") or "Умеренная",
            "score_display": top_territory.get("ranking_confidence_display") or quality_passport.get("confidence_score_display") or "0 / 100",
            "tone": top_territory.get("ranking_confidence_tone") or quality_passport.get("confidence_tone") or "fire",
            "note": top_territory.get("ranking_confidence_note") or quality_passport.get("validation_summary") or "Пояснение появится после расчёта.",
        }

    return {
        "label": quality_passport.get("confidence_label") or "Ограниченная",
        "score_display": quality_passport.get("confidence_score_display") or "0 / 100",
        "tone": quality_passport.get("confidence_tone") or "fire",
        "note": quality_passport.get("validation_summary") or "Пояснение появится после расчёта.",
    }


def _ranking_confidence_state(score: int) -> tuple[str, str, str]:
    if score >= 82:
        return "Высокая", "forest", "Вывод подтверждается уверенно"
    if score >= 64:
        return "Рабочая", "sky", "Вывод подтверждается на рабочем уровне"
    if score >= 46:
        return "Умеренная", "sand", "Вывод полезен для приоритизации, но требует локальной проверки"
    return "Ограниченная", "fire", "Вывод стоит использовать как сигнал к дополнительной проверке"


def _build_summary_cards(
    territories: Sequence[Dict[str, Any]],
    weight_profile: Dict[str, Any],
    historical_validation: Dict[str, Any],
    quality_passport: Dict[str, Any],
) -> list[Dict[str, str]]:
    if not territories:
        return []

    lead = territories[0]
    cards = [
        {
            "label": "Территория первого приоритета",
            "value": lead.get("label") or "-",
            "meta": lead.get("ranking_reason") or lead.get("drivers_display") or "После расчёта здесь появится объяснение лидерства.",
            "tone": lead.get("priority_tone") or "sand",
        },
        {
            "label": "Надёжность вывода",
            "value": lead.get("ranking_confidence_label") or "Умеренная",
            "meta": lead.get("ranking_confidence_note") or "После расчёта здесь появится оценка надёжности вывода по ранжированию.",
            "tone": lead.get("ranking_confidence_tone") or "fire",
        },
        {
            "label": "Профиль весов",
            "value": weight_profile.get("status_label") or "Активный профиль",
            "meta": weight_profile.get("mode_label") or "Адаптивные веса",
            "tone": weight_profile.get("status_tone") or "forest",
        },
        {
            "label": "Качество данных",
            "value": quality_passport.get("confidence_label") or "Ограниченная",
            "meta": quality_passport.get("validation_summary") or "Паспорт качества появится после расчёта.",
            "tone": quality_passport.get("confidence_tone") or "fire",
        },
    ]

    metrics = historical_validation.get("metrics_raw") or {}
    if historical_validation.get("has_metrics"):
        k_value = int(metrics.get("k_value") or 3)
        cards.extend(
            [
                {
                    "label": "Top-1 hit",
                    "value": _format_probability(float(metrics.get("top1_hit_rate") or 0.0)),
                    "meta": "Как часто первая территория действительно горела в следующем окне",
                    "tone": "sky",
                },
                {
                    "label": f"Top-{k_value} capture",
                    "value": _format_probability(float(metrics.get("topk_capture_rate") or 0.0)),
                    "meta": "Какая доля будущих пожаров попадала в верхнюю часть рейтинга",
                    "tone": "forest",
                },
                {
                    "label": f"Precision@{k_value}",
                    "value": _format_probability(float(metrics.get("precision_at_k") or 0.0)),
                    "meta": "Какая доля территорий в верхней части рейтинга действительно подтверждалась пожаром",
                    "tone": "sky",
                },
                {
                    "label": f"NDCG@{k_value}",
                    "value": _format_number(float(metrics.get("ndcg_at_k") or 0.0)),
                    "meta": "Насколько порядок территорий совпадал с реальной концентрацией пожаров",
                    "tone": "sand",
                },
            ]
        )
    else:
        cards.append(
            {
                "label": "Историческая проверка",
                "value": historical_validation.get("status_label") or "Пока без проверки",
                "meta": historical_validation.get("summary") or "Метрики появятся после накопления истории.",
                "tone": historical_validation.get("status_tone") or "sand",
            }
        )

    return cards
