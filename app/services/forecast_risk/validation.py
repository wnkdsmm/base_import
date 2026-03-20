from __future__ import annotations

from collections import Counter
from datetime import timedelta
from statistics import mean
from typing import Any, Dict, List, Sequence

from .profiles import DEFAULT_RISK_WEIGHT_MODE, get_risk_weight_profile
from .scoring import _build_territory_rows
from .utils import _format_integer, _format_probability


def build_historical_validation_payload(
    records: Sequence[Dict[str, Any]],
    planning_horizon_days: int,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
) -> Dict[str, Any]:
    profile = get_risk_weight_profile(weight_mode)
    payload = empty_historical_validation_payload(profile.get("mode_label") or "Экспертные веса")
    if not records:
        payload["summary"] = "Для исторической проверки ranking пока нет записей."
        return payload

    history_start = min(record["date"] for record in records)
    history_end = max(record["date"] for record in records)
    history_days = max(1, (history_end - history_start).days + 1)
    horizon_days = max(7, int(planning_horizon_days or 14))
    min_training_days = max(180, horizon_days * 6)

    if history_days < min_training_days + horizon_days:
        payload["summary"] = (
            "Истории пока недостаточно для черновой проверки ranking на исторических окнах. "
            "Нужно больше наблюдений или более короткий горизонт."
        )
        payload["notes"] = [
            f"Сейчас доступно {_format_integer(history_days)} дней истории.",
            f"Для черновой проверки желательно не меньше {_format_integer(min_training_days + horizon_days)} дней.",
            "Структура под валидацию уже заложена, поэтому после накопления истории метрики появятся автоматически.",
        ]
        return payload

    step_days = max(horizon_days, 30)
    earliest_cutoff = history_start + timedelta(days=min_training_days - 1)
    latest_cutoff = history_end - timedelta(days=horizon_days)
    cutoffs: List[Any] = []
    cursor = earliest_cutoff
    while cursor <= latest_cutoff:
        cutoffs.append(cursor)
        cursor += timedelta(days=step_days)
    cutoffs = cutoffs[-6:]

    windows: List[Dict[str, Any]] = []
    skipped_no_future = 0
    skipped_no_rows = 0
    for cutoff in cutoffs:
        train_records = [record for record in records if record["date"] <= cutoff]
        future_end = cutoff + timedelta(days=horizon_days)
        future_records = [record for record in records if cutoff < record["date"] <= future_end]
        if not future_records:
            skipped_no_future += 1
            continue
        predicted_rows = _build_territory_rows(train_records, horizon_days, weight_mode=weight_mode)
        if not predicted_rows:
            skipped_no_rows += 1
            continue

        actual_counts = Counter(
            (record.get("territory_label") or record.get("district") or "Территория не указана")
            for record in future_records
        )
        total_future_incidents = sum(actual_counts.values())
        top1_label = predicted_rows[0]["label"] if predicted_rows else "-"
        top1_hit = 1 if actual_counts.get(top1_label, 0) > 0 else 0
        top3_labels = [row["label"] for row in predicted_rows[:3]]
        top3_capture = (
            sum(actual_counts.get(label, 0) for label in top3_labels) / total_future_incidents if total_future_incidents else 0.0
        )
        high_risk_labels = [row["label"] for row in predicted_rows if row.get("risk_score", 0.0) >= 67.0]
        if high_risk_labels:
            high_risk_precision = sum(1 for label in high_risk_labels if actual_counts.get(label, 0) > 0) / len(high_risk_labels)
        else:
            high_risk_precision = 0.0

        windows.append(
            {
                "cutoff": cutoff,
                "future_end": future_end,
                "top1_hit": top1_hit,
                "top3_capture": top3_capture,
                "high_risk_precision": high_risk_precision,
                "future_incidents": total_future_incidents,
                "top_label": top1_label,
                "top3_labels": top3_labels,
                "summary_card": {
                    "label": f"Окно до {cutoff.strftime('%d.%m.%Y')}",
                    "risk_display": _format_probability(top3_capture),
                    "meta": (
                        f"Top-1: {'да' if top1_hit else 'нет'} | будущих пожаров: {_format_integer(total_future_incidents)} | "
                        f"лидер: {top1_label}"
                    ),
                },
            }
        )

    if not windows:
        payload["summary"] = "Окна для проверки есть, но в них пока не удалось собрать устойчивую оценку ranking."
        payload["notes"] = [
            f"Окон без будущих пожаров: {_format_integer(skipped_no_future)}.",
            f"Окон без расчётного ranking: {_format_integer(skipped_no_rows)}.",
            "После расширения истории эта панель начнет показывать top-k метрики автоматически.",
        ]
        return payload

    top1_hit_rate = mean(item["top1_hit"] for item in windows)
    top3_capture_rate = mean(item["top3_capture"] for item in windows)
    high_risk_precision = mean(item["high_risk_precision"] for item in windows)

    if len(windows) >= 4 and top3_capture_rate >= 0.60:
        status_label = "Есть рабочий исторический сигнал"
        status_tone = "forest"
    elif len(windows) >= 3 and top3_capture_rate >= 0.45:
        status_label = "Проверка рабочая, но умеренная"
        status_tone = "sky"
    else:
        status_label = "Проверка частичная"
        status_tone = "sand"

    payload.update(
        {
            "has_metrics": True,
            "status_label": status_label,
            "status_tone": status_tone,
            "summary": (
                "Это черновая проверка ranking на исторических окнах: сервис строит ранжирование по прошлой истории и "
                f"смотрит, куда реально пришли пожары в следующие {horizon_days} дней."
            ),
            "metric_cards": [
                {
                    "label": "Окон оценено",
                    "value": _format_integer(len(windows)),
                    "meta": f"Пропущено без будущих пожаров: {_format_integer(skipped_no_future)}",
                },
                {
                    "label": "Top-1 hit",
                    "value": _format_probability(top1_hit_rate),
                    "meta": "Как часто территория-лидер действительно горела в следующем окне",
                },
                {
                    "label": "Top-3 capture",
                    "value": _format_probability(top3_capture_rate),
                    "meta": "Какая доля будущих пожаров попадала в top-3 территорий",
                },
                {
                    "label": "Precision high risk",
                    "value": _format_probability(high_risk_precision),
                    "meta": "Доля high-risk территорий, которые подтвердились пожаром в следующем окне",
                },
            ],
            "notes": [
                "Проверка пока не заменяет полноценный backtesting и не учитывает все организационные меры, принятые после пожаров.",
                "Калибруемый режим может использовать эти же окна для подбора весов без смены структуры объяснений.",
                f"Текущий профиль: {profile.get('mode_label') or 'Экспертные веса'}.",
            ],
            "recent_windows": [item["summary_card"] for item in windows[-3:]][::-1],
        }
    )
    return payload


def empty_historical_validation_payload(mode_label: str = "Экспертные веса") -> Dict[str, Any]:
    return {
        "title": "Черновая историческая проверка ranking",
        "mode_label": mode_label,
        "has_metrics": False,
        "status_label": "Пока без проверки",
        "status_tone": "fire",
        "summary": "После расчета здесь появится заготовка под историческую проверку ranking.",
        "metric_cards": [
            {"label": "Окон оценено", "value": "0", "meta": "Нет данных"},
            {"label": "Top-1 hit", "value": "0%", "meta": "Нет данных"},
            {"label": "Top-3 capture", "value": "0%", "meta": "Нет данных"},
            {"label": "Precision high risk", "value": "0%", "meta": "Нет данных"},
        ],
        "notes": [
            "Эта панель предназначена для последующей проверки качества ranking на исторических данных.",
            "Метрики появятся автоматически, когда истории и окон станет достаточно.",
        ],
        "recent_windows": [],
    }
