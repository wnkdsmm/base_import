from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import timedelta
import math
from statistics import mean
from typing import Any, Sequence

from .profile_resolution import resolve_weight_profile_for_records
from .profiles import DEFAULT_RISK_WEIGHT_MODE, get_risk_weight_profile, resolve_component_weights
from .scoring import _build_territory_rows
from .types import (
    HistoricalValidationPayload,
    HistoricalWindow,
    HistoricalWindowsBundle,
    RiskEventRecord,
    RiskProfile,
    RiskScore,
    ValidationEvaluation,
    ValidationMetricsRaw,
    ValidationRankedRow,
    ValidationWindowMetrics,
)
from .utils import _clamp, _format_decimal, _format_integer, _format_probability, _unique_non_empty

DEFAULT_RANKING_K = 3
MIN_VALIDATION_WINDOWS = 3


def build_historical_validation_payload(
    records: Sequence[RiskEventRecord],
    planning_horizon_days: int,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
    profile_override: RiskProfile | None = None,
) -> HistoricalValidationPayload:
    profile = deepcopy(profile_override) if profile_override is not None else get_risk_weight_profile(weight_mode)
    payload = empty_historical_validation_payload(profile.get("mode_label") or "Адаптивные веса")
    if not records:
        payload["summary"] = "Для исторической проверки ранжирования пока нет записей."
        return payload

    windows_bundle = _build_historical_windows(records, planning_horizon_days)
    if not windows_bundle.get("is_ready"):
        payload["summary"] = windows_bundle.get("reason") or "Истории пока недостаточно для проверки ранжирования на исторических окнах."
        payload["notes"] = _unique_non_empty(
            [
                f"Сейчас доступно {_format_integer(windows_bundle.get('history_days') or 0)} дней истории.",
                f"Для проверки желательно не меньше {_format_integer((windows_bundle.get('min_training_days') or 0) + (windows_bundle.get('horizon_days') or 0))} дней.",
                "Метрики появятся автоматически, когда накопится достаточно исторических окон.",
            ]
        )
        return payload

    evaluation = _evaluate_profile_on_windows(profile, windows_bundle.get("windows") or [], DEFAULT_RANKING_K)
    if not evaluation.get("has_metrics"):
        payload["summary"] = "Окна для проверки есть, но в них пока не удалось собрать устойчивую оценку ranking-качества."
        payload["notes"] = _unique_non_empty(
            [
                f"Окон без расчётного ранжирования: {_format_integer(evaluation.get('skipped_no_rows') or 0)}.",
                f"Окон с метриками: {_format_integer(len(evaluation.get('window_metrics') or []))}.",
                "После расширения истории панель начнет показывать ranking-метрики автоматически.",
            ]
        )
        return payload

    aggregate = evaluation.get("aggregate") or {}
    k_value = int(aggregate.get("k_value") or DEFAULT_RANKING_K)
    status_label, status_tone = _validation_status(aggregate)
    recent_windows = [item.get("summary_card") for item in (evaluation.get("window_metrics") or [])[-3:] if item.get("summary_card")][::-1]
    summary = (
        "Это rolling-origin проверка ranking-блока: сервис строит приоритеты только по прошлой истории окна и смотрит, "
        f"куда реально пришли пожары в следующие {aggregate.get('horizon_days') or windows_bundle.get('horizon_days') or max(7, int(planning_horizon_days or 14))} дней."
    )

    notes = [
        f"Профиль весов для проверки: {profile.get('mode_label') or 'Адаптивные веса'} ({profile.get('status_label') or 'активен'}).",
        f"Ключевая метрика ranking-качества: NDCG@{k_value} {_format_decimal(float(aggregate.get('ndcg_at_k') or 0.0))}.",
        f"Top-{k_value} capture на исторических окнах: {_format_probability(float(aggregate.get('topk_capture_rate') or 0.0))}.",
        f"Precision@{k_value} на исторических окнах: {_format_probability(float(aggregate.get('precision_at_k') or 0.0))}.",
    ]
    calibration_summary = str((profile.get("calibration") or {}).get("summary") or "").strip()
    if calibration_summary:
        notes.append(calibration_summary)
    notes.extend(list((profile.get("calibration") or {}).get("notes") or [])[:2])

    payload.update(
        {
            "has_metrics": True,
            "status_label": status_label,
            "status_tone": status_tone,
            "summary": summary,
            "metric_cards": [
                {
                    "label": "Окон оценено",
                    "value": _format_integer(aggregate.get("windows_count") or 0),
                    "meta": f"Горизонт: {_format_integer(aggregate.get('horizon_days') or 0)} дней",
                },
                {
                    "label": "Top-1 hit",
                    "value": _format_probability(float(aggregate.get("top1_hit_rate") or 0.0)),
                    "meta": "Как часто территория-лидер действительно горела в следующем окне",
                },
                {
                    "label": f"Top-{k_value} capture",
                    "value": _format_probability(float(aggregate.get("topk_capture_rate") or 0.0)),
                    "meta": "Какая доля будущих пожаров попадала в верхнюю часть списка",
                },
                {
                    "label": f"Precision@{k_value}",
                    "value": _format_probability(float(aggregate.get("precision_at_k") or 0.0)),
                    "meta": "Какая доля территорий в top-k подтверждалась пожаром",
                },
                {
                    "label": f"NDCG@{k_value}",
                    "value": _format_decimal(float(aggregate.get("ndcg_at_k") or 0.0)),
                    "meta": "Насколько порядок ранжирования совпадал с фактической концентрацией пожаров",
                },
            ],
            "notes": _unique_non_empty(notes),
            "recent_windows": recent_windows,
            "metrics_raw": {
                "windows_count": int(aggregate.get("windows_count") or 0),
                "horizon_days": int(aggregate.get("horizon_days") or 0),
                "k_value": k_value,
                "top1_hit_rate": float(aggregate.get("top1_hit_rate") or 0.0),
                "topk_capture_rate": float(aggregate.get("topk_capture_rate") or 0.0),
                "precision_at_k": float(aggregate.get("precision_at_k") or 0.0),
                "ndcg_at_k": float(aggregate.get("ndcg_at_k") or 0.0),
                "objective_score": float(aggregate.get("objective_score") or 0.0),
            },
        }
    )
    return payload


def empty_historical_validation_payload(mode_label: str = "Адаптивные веса") -> HistoricalValidationPayload:
    return {
        "title": "Историческая проверка ranking-качества",
        "mode_label": mode_label,
        "has_metrics": False,
        "status_label": "Пока без проверки",
        "status_tone": "fire",
        "summary": "После расчёта здесь появится проверка того, насколько верхние территории реально совпадали с будущими очагами на исторических окнах.",
        "metric_cards": [
            {"label": "Окон оценено", "value": "0", "meta": "Нет данных"},
            {"label": "Top-1 hit", "value": "0%", "meta": "Нет данных"},
            {"label": "Top-3 capture", "value": "0%", "meta": "Нет данных"},
            {"label": "Precision@3", "value": "0%", "meta": "Нет данных"},
            {"label": "NDCG@3", "value": "0", "meta": "Нет данных"},
        ],
        "notes": [
            "Эта панель показывает rolling-origin проверку ranking-качества на исторических окнах.",
            "Если истории мало, сервис автоматически остается на экспертном fallback-профиле.",
        ],
        "recent_windows": [],
        "metrics_raw": {
            "windows_count": 0,
            "horizon_days": 0,
            "k_value": DEFAULT_RANKING_K,
            "top1_hit_rate": 0.0,
            "topk_capture_rate": 0.0,
            "precision_at_k": 0.0,
            "ndcg_at_k": 0.0,
            "objective_score": 0.0,
        },
    }


def _build_historical_windows(
    records: Sequence[RiskEventRecord],
    planning_horizon_days: int,
) -> HistoricalWindowsBundle:
    if not records:
        return {
            "is_ready": False,
            "reason": "Для проверки ранжирования пока нет записей.",
            "history_days": 0,
            "horizon_days": max(7, int(planning_horizon_days or 14)),
            "min_training_days": 0,
            "windows": [],
        }

    history_start = min(record["date"] for record in records)
    history_end = max(record["date"] for record in records)
    history_days = max(1, (history_end - history_start).days + 1)
    horizon_days = max(7, int(planning_horizon_days or 14))
    min_training_days = max(180, horizon_days * 6)

    if history_days < min_training_days + horizon_days:
        return {
            "is_ready": False,
            "reason": (
                "Истории пока недостаточно для проверки ranking-блока на исторических окнах. "
                "Нужно больше наблюдений или более короткий горизонт."
            ),
            "history_days": history_days,
            "horizon_days": horizon_days,
            "min_training_days": min_training_days,
            "windows": [],
        }

    step_days = max(horizon_days, 30)
    earliest_cutoff = history_start + timedelta(days=min_training_days - 1)
    latest_cutoff = history_end - timedelta(days=horizon_days)
    cutoffs: list[Any] = []
    cursor = earliest_cutoff
    while cursor <= latest_cutoff:
        cutoffs.append(cursor)
        cursor += timedelta(days=step_days)
    cutoffs = cutoffs[-12:]

    windows: list[HistoricalWindow] = []
    skipped_no_future = 0
    for cutoff in cutoffs:
        train_records = [record for record in records if record["date"] <= cutoff]
        future_end = cutoff + timedelta(days=horizon_days)
        future_records = [record for record in records if cutoff < record["date"] <= future_end]
        if not future_records:
            skipped_no_future += 1
            continue
        windows.append(
            {
                "cutoff": cutoff,
                "future_end": future_end,
                "horizon_days": horizon_days,
                "train_records": train_records,
                "future_records": future_records,
            }
        )

    return {
        "is_ready": len(windows) >= MIN_VALIDATION_WINDOWS,
        "reason": "" if len(windows) >= MIN_VALIDATION_WINDOWS else "Не удалось собрать достаточное число исторических окон для ranking-проверки.",
        "history_days": history_days,
        "horizon_days": horizon_days,
        "min_training_days": min_training_days,
        "windows": windows,
        "skipped_no_future": skipped_no_future,
    }


def _evaluate_profile_on_windows(
    profile: RiskProfile,
    windows: Sequence[HistoricalWindow],
    ranking_k: int,
) -> ValidationEvaluation:
    window_metrics: list[ValidationWindowMetrics] = []
    skipped_no_rows = 0

    for window in windows:
        predicted_rows = _rerank_predicted_rows_for_profile(
            window.get("predicted_rows") or [],
            profile,
        )
        if not predicted_rows:
            predicted_rows = _build_territory_rows(
                window.get("train_records") or [],
                int(window.get("horizon_days") or ranking_k or DEFAULT_RANKING_K),
                weight_mode=profile.get("mode") or DEFAULT_RISK_WEIGHT_MODE,
                profile_override=profile,
            )
        if not predicted_rows:
            skipped_no_rows += 1
            continue
        metrics = _evaluate_ranking_window(
            predicted_rows=predicted_rows,
            future_records=window.get("future_records") or [],
            ranking_k=ranking_k,
            cutoff=window.get("cutoff"),
        )
        if metrics is not None:
            window_metrics.append(metrics)

    aggregate = _aggregate_window_metrics(window_metrics, ranking_k)
    aggregate["horizon_days"] = int((windows[0].get("horizon_days") if windows else 0) or 0)
    return {
        "has_metrics": bool(window_metrics),
        "window_metrics": window_metrics,
        "aggregate": aggregate,
        "skipped_no_rows": skipped_no_rows,
    }


def _rerank_predicted_rows_for_profile(
    predicted_rows: Sequence[RiskScore],
    profile: RiskProfile,
) -> list[ValidationRankedRow]:
    if not predicted_rows:
        return []

    urban_weights = {
        item["key"]: float(item.get("weight") or 0.0)
        for item in resolve_component_weights(profile, is_rural=False)
    }
    rural_weights = {
        item["key"]: float(item.get("weight") or 0.0)
        for item in resolve_component_weights(profile, is_rural=True)
    }

    reranked: list[ValidationRankedRow] = []
    for row in predicted_rows:
        component_scores = row.get("component_scores") or []
        weight_map = rural_weights if row.get("is_rural") else urban_weights
        risk_score = _clamp(
            sum(
                float(component.get("score") or 0.0) * weight_map.get(component.get("key"), 0.0)
                for component in component_scores
            ),
            1.0,
            99.0,
        )
        reranked.append(
            {
                "label": row.get("label") or "Территория",
                "risk_score": round(risk_score, 1),
                "history_pressure": float(row.get("history_pressure") or 0.0),
            }
        )

    reranked.sort(key=lambda item: (item["risk_score"], item["history_pressure"]), reverse=True)
    return reranked


def _evaluate_ranking_window(
    predicted_rows: Sequence[ValidationRankedRow],
    future_records: Sequence[RiskEventRecord],
    ranking_k: int,
    cutoff: Any,
) -> ValidationWindowMetrics | None:
    if not predicted_rows or not future_records:
        return None

    actual_counts = Counter(
        (record.get("territory_label") or record.get("district") or "Территория не указана")
        for record in future_records
    )
    total_future_incidents = sum(actual_counts.values())
    if total_future_incidents <= 0:
        return None

    effective_k = max(1, min(int(ranking_k or DEFAULT_RANKING_K), len(predicted_rows)))
    top_labels = [row.get("label") or "Территория" for row in predicted_rows[:effective_k]]
    top1_label = top_labels[0] if top_labels else "-"
    top1_hit = 1.0 if actual_counts.get(top1_label, 0) > 0 else 0.0
    topk_capture = sum(actual_counts.get(label, 0) for label in top_labels) / total_future_incidents
    precision_at_k = sum(1 for label in top_labels if actual_counts.get(label, 0) > 0) / max(1, effective_k)
    ndcg_at_k = _compute_ndcg_at_k(actual_counts, top_labels, effective_k)

    return {
        "cutoff": cutoff,
        "top_label": top1_label,
        "future_incidents": total_future_incidents,
        "top1_hit": top1_hit,
        "topk_capture": topk_capture,
        "precision_at_k": precision_at_k,
        "ndcg_at_k": ndcg_at_k,
        "summary_card": {
            "label": f"Окно до {cutoff.strftime('%d.%m.%Y') if cutoff else '-'}",
            "risk_display": f"Top-{effective_k}: {_format_probability(topk_capture)}",
            "meta": (
                f"Top-1: {'да' if top1_hit >= 0.5 else 'нет'} | Precision@{effective_k}: {_format_probability(precision_at_k)} | "
                f"NDCG@{effective_k}: {_format_decimal(ndcg_at_k)} | будущих пожаров: {_format_integer(total_future_incidents)}"
            ),
        },
    }


def _aggregate_window_metrics(
    window_metrics: Sequence[ValidationWindowMetrics],
    ranking_k: int,
) -> ValidationMetricsRaw:
    if not window_metrics:
        return {
            "windows_count": 0,
            "horizon_days": 0,
            "k_value": ranking_k,
            "top1_hit_rate": 0.0,
            "topk_capture_rate": 0.0,
            "precision_at_k": 0.0,
            "ndcg_at_k": 0.0,
            "objective_score": 0.0,
        }

    aggregate = {
        "windows_count": len(window_metrics),
        "horizon_days": 0,
        "k_value": ranking_k,
        "top1_hit_rate": mean(float(item.get("top1_hit") or 0.0) for item in window_metrics),
        "topk_capture_rate": mean(float(item.get("topk_capture") or 0.0) for item in window_metrics),
        "precision_at_k": mean(float(item.get("precision_at_k") or 0.0) for item in window_metrics),
        "ndcg_at_k": mean(float(item.get("ndcg_at_k") or 0.0) for item in window_metrics),
    }
    aggregate["objective_score"] = _ranking_objective(aggregate)
    return aggregate


def _ranking_objective(metrics: ValidationMetricsRaw) -> float:
    top1 = float(metrics.get("top1_hit_rate") or 0.0)
    capture = float(metrics.get("topk_capture_rate") or 0.0)
    precision = float(metrics.get("precision_at_k") or 0.0)
    ndcg = float(metrics.get("ndcg_at_k") or 0.0)
    return 0.24 * top1 + 0.31 * capture + 0.20 * precision + 0.25 * ndcg


def _validation_status(aggregate: ValidationMetricsRaw) -> tuple[str, str]:
    windows_count = int(aggregate.get("windows_count") or 0)
    topk_capture = float(aggregate.get("topk_capture_rate") or 0.0)
    ndcg = float(aggregate.get("ndcg_at_k") or 0.0)
    if windows_count >= 4 and topk_capture >= 0.60 and ndcg >= 0.62:
        return "Исторический сигнал устойчив", "forest"
    if windows_count >= 3 and topk_capture >= 0.45 and ndcg >= 0.48:
        return "Исторический сигнал рабочий", "sky"
    return "Проверка частичная", "sand"


def _compute_ndcg_at_k(actual_counts: Counter, ranked_labels: Sequence[str], k_value: int) -> float:
    if not ranked_labels or k_value <= 0:
        return 0.0
    effective_k = max(1, min(k_value, len(ranked_labels)))
    predicted_relevance = [min(float(actual_counts.get(label, 0)), 5.0) for label in ranked_labels[:effective_k]]
    ideal_relevance = sorted((min(float(v), 5.0) for v in actual_counts.values()), reverse=True)[:effective_k]
    dcg = _discounted_gain(predicted_relevance)
    idcg = _discounted_gain(ideal_relevance)
    if idcg <= 0:
        return 0.0
    return dcg / idcg


def _discounted_gain(relevance_values: Sequence[float]) -> float:
    gain = 0.0
    for index, relevance in enumerate(relevance_values):
        gain += (math.pow(2.0, float(relevance)) - 1.0) / math.log2(index + 2.0)
    return gain
