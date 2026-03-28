from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import timedelta
import math
from statistics import mean
from typing import Any, Dict, List, Optional, Sequence

from .profiles import DEFAULT_RISK_WEIGHT_MODE, get_risk_weight_profile, resolve_component_weights
from .scoring import _build_territory_rows
from .utils import _clamp, _format_integer, _format_number, _format_probability, _unique_non_empty

DEFAULT_RANKING_K = 3
MIN_VALIDATION_WINDOWS = 3
MIN_CALIBRATION_WINDOWS = 4
MIN_COMPONENT_WEIGHT = 0.10
MAX_COMPONENT_WEIGHT = 0.55
CALIBRATION_REGULARIZATION = 0.08
MIN_CALIBRATION_IMPROVEMENT = 0.015



def resolve_weight_profile_for_records(
    records: Sequence[Dict[str, Any]],
    planning_horizon_days: int,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
) -> Dict[str, Any]:
    requested_profile = get_risk_weight_profile(weight_mode)
    if weight_mode == "expert":
        profile = deepcopy(requested_profile)
        calibration = profile.get("calibration") or {}
        calibration["ready"] = False
        calibration["used_fallback"] = False
        calibration["summary"] = "Экспертный режим выбран явно, поэтому автоматическая калибровка по истории не применялась."
        calibration["notes"] = _unique_non_empty(list(calibration.get("notes") or []) + [calibration["summary"]])
        profile["calibration"] = calibration
        return profile

    expert_profile = get_risk_weight_profile("expert")
    windows_bundle = _build_historical_windows(records, planning_horizon_days)
    if not windows_bundle.get("is_ready") or len(windows_bundle.get("windows") or []) < MIN_CALIBRATION_WINDOWS:
        return _build_expert_fallback_profile(
            requested_profile=requested_profile,
            expert_profile=expert_profile,
            windows_bundle=windows_bundle,
            reason=(
                windows_bundle.get("reason")
                or "Истории пока недостаточно для устойчивой калибровки весов по историческим окнам."
            ),
        )

    evaluation_windows = _prepare_evaluation_windows(
        windows_bundle.get("windows") or [],
        expert_profile,
    )

    candidates = _generate_weight_candidates(expert_profile)
    evaluations: List[Dict[str, Any]] = []
    expert_entry: Optional[Dict[str, Any]] = None

    for candidate in candidates:
        candidate_profile = _profile_with_weights(requested_profile, expert_profile, candidate["weights"])
        evaluation = _evaluate_profile_on_windows(candidate_profile, evaluation_windows, DEFAULT_RANKING_K)
        if not evaluation.get("has_metrics"):
            continue

        aggregate = evaluation.get("aggregate") or {}
        objective = _ranking_objective(aggregate)
        regularized_objective = objective - CALIBRATION_REGULARIZATION * _weight_distance(
            candidate["weights"],
            expert_profile.get("component_weights") or {},
        )
        entry = {
            "key": candidate["key"],
            "label": candidate["label"],
            "weights": candidate["weights"],
            "evaluation": evaluation,
            "aggregate": aggregate,
            "objective": objective,
            "regularized_objective": regularized_objective,
        }
        evaluations.append(entry)
        if candidate["key"] == "expert":
            expert_entry = entry

    if not evaluations or expert_entry is None:
        return _build_expert_fallback_profile(
            requested_profile=requested_profile,
            expert_profile=expert_profile,
            windows_bundle=windows_bundle,
            reason="Не удалось устойчиво оценить ни один набор весов на исторических окнах.",
        )

    best_entry = max(
        evaluations,
        key=lambda item: (
            item.get("regularized_objective", 0.0),
            item.get("aggregate", {}).get("ndcg_at_k", 0.0),
            item.get("aggregate", {}).get("topk_capture_rate", 0.0),
        ),
    )
    improvement = float(best_entry.get("regularized_objective", 0.0) - expert_entry.get("regularized_objective", 0.0))

    if best_entry["key"] == "expert" or improvement < MIN_CALIBRATION_IMPROVEMENT:
        return _build_expert_retained_profile(
            requested_profile=requested_profile,
            expert_profile=expert_profile,
            expert_entry=expert_entry,
            best_entry=best_entry,
            candidate_count=len(evaluations),
            improvement=improvement,
        )

    return _build_calibrated_profile(
        requested_profile=requested_profile,
        expert_profile=expert_profile,
        selected_entry=best_entry,
        expert_entry=expert_entry,
        candidate_count=len(evaluations),
        improvement=improvement,
    )



def build_historical_validation_payload(
    records: Sequence[Dict[str, Any]],
    planning_horizon_days: int,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
    profile_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
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



def empty_historical_validation_payload(mode_label: str = "Адаптивные веса") -> Dict[str, Any]:
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



def _build_historical_windows(records: Sequence[Dict[str, Any]], planning_horizon_days: int) -> Dict[str, Any]:
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
    cutoffs: List[Any] = []
    cursor = earliest_cutoff
    while cursor <= latest_cutoff:
        cutoffs.append(cursor)
        cursor += timedelta(days=step_days)
    cutoffs = cutoffs[-6:]

    windows: List[Dict[str, Any]] = []
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



def _generate_weight_candidates(profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    base_weights = {key: float(value) for key, value in (profile.get("component_weights") or {}).items()}
    component_order = list(profile.get("component_order") or base_weights.keys())
    candidates: List[Dict[str, Any]] = []
    seen = set()

    def add_candidate(key: str, label: str, weights: Dict[str, float]) -> None:
        normalized = _normalize_component_weights(weights, component_order)
        if not normalized:
            return
        signature = tuple((component, round(normalized.get(component, 0.0), 4)) for component in component_order)
        if signature in seen:
            return
        seen.add(signature)
        candidates.append({"key": key, "label": label, "weights": normalized})

    add_candidate("expert", "Экспертный профиль", base_weights)
    add_candidate("balanced", "Сбалансированный профиль", {component: 1.0 / max(1, len(component_order)) for component in component_order})

    for receiver in component_order:
        focused = {component: 0.18 for component in component_order}
        focused[receiver] = 0.46
        add_candidate(f"focus_{receiver}", f"Фокус: {receiver}", focused)

    for donor in component_order:
        for receiver in component_order:
            if donor == receiver:
                continue
            for shift in (0.04, 0.08):
                shifted = dict(base_weights)
                shifted[donor] = shifted.get(donor, 0.0) - shift
                shifted[receiver] = shifted.get(receiver, 0.0) + shift
                add_candidate(
                    f"shift_{donor}_to_{receiver}_{int(round(shift * 100))}",
                    f"Сдвиг {donor}->{receiver}",
                    shifted,
                )
    return candidates



def _normalize_component_weights(weights: Dict[str, float], component_order: Sequence[str]) -> Optional[Dict[str, float]]:
    cleaned = {component: max(0.0, float(weights.get(component, 0.0))) for component in component_order}
    total = sum(cleaned.values())
    if total <= 0:
        return None
    normalized = {component: cleaned[component] / total for component in component_order}
    if any(value < MIN_COMPONENT_WEIGHT - 1e-9 for value in normalized.values()):
        return None
    if any(value > MAX_COMPONENT_WEIGHT + 1e-9 for value in normalized.values()):
        return None
    return normalized



def _evaluate_profile_on_windows(
    profile: Dict[str, Any],
    windows: Sequence[Dict[str, Any]],
    ranking_k: int,
) -> Dict[str, Any]:
    window_metrics: List[Dict[str, Any]] = []
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


def _prepare_evaluation_windows(
    windows: Sequence[Dict[str, Any]],
    profile: Dict[str, Any],
) -> List[Dict[str, Any]]:
    prepared: List[Dict[str, Any]] = []
    for window in windows:
        prepared_window = dict(window)
        prepared_window["predicted_rows"] = _build_territory_rows(
            window.get("train_records") or [],
            int(window.get("horizon_days") or DEFAULT_RANKING_K),
            weight_mode=profile.get("mode") or DEFAULT_RISK_WEIGHT_MODE,
            profile_override=profile,
        )
        prepared.append(prepared_window)
    return prepared


def _rerank_predicted_rows_for_profile(
    predicted_rows: Sequence[Dict[str, Any]],
    profile: Dict[str, Any],
) -> List[Dict[str, Any]]:
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

    reranked: List[Dict[str, Any]] = []
    for row in predicted_rows:
        component_scores = row.get("component_scores") or []
        weight_map = rural_weights if row.get("is_rural") else urban_weights
        risk_score = _clamp(
            sum(float(component.get("score") or 0.0) * weight_map.get(component.get("key"), 0.0) for component in component_scores),
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
    predicted_rows: Sequence[Dict[str, Any]],
    future_records: Sequence[Dict[str, Any]],
    ranking_k: int,
    cutoff: Any,
) -> Optional[Dict[str, Any]]:
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



def _aggregate_window_metrics(window_metrics: Sequence[Dict[str, Any]], ranking_k: int) -> Dict[str, Any]:
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



def _ranking_objective(metrics: Dict[str, Any]) -> float:
    top1 = float(metrics.get("top1_hit_rate") or 0.0)
    capture = float(metrics.get("topk_capture_rate") or 0.0)
    precision = float(metrics.get("precision_at_k") or 0.0)
    ndcg = float(metrics.get("ndcg_at_k") or 0.0)
    return 0.24 * top1 + 0.31 * capture + 0.20 * precision + 0.25 * ndcg



def _weight_distance(candidate_weights: Dict[str, float], base_weights: Dict[str, float]) -> float:
    return sum(abs(float(candidate_weights.get(key, 0.0)) - float(base_weights.get(key, 0.0))) for key in base_weights)


def _build_metric_comparison(selected_metrics: Dict[str, Any], expert_metrics: Dict[str, Any]) -> Dict[str, Any]:
    comparison = {
        "top1_hit_delta": float(selected_metrics.get("top1_hit_rate") or 0.0) - float(expert_metrics.get("top1_hit_rate") or 0.0),
        "topk_capture_delta": float(selected_metrics.get("topk_capture_rate") or 0.0) - float(expert_metrics.get("topk_capture_rate") or 0.0),
        "precision_at_k_delta": float(selected_metrics.get("precision_at_k") or 0.0) - float(expert_metrics.get("precision_at_k") or 0.0),
        "ndcg_at_k_delta": float(selected_metrics.get("ndcg_at_k") or 0.0) - float(expert_metrics.get("ndcg_at_k") or 0.0),
    }
    k_value = int(selected_metrics.get("k_value") or expert_metrics.get("k_value") or DEFAULT_RANKING_K)
    comparison["summary"] = (
        f"Сравнение с экспертным профилем: Top-1 { _format_delta(comparison['top1_hit_delta'], percent=True) }, "
        f"Top-{k_value} capture { _format_delta(comparison['topk_capture_delta'], percent=True) }, "
        f"Precision@{k_value} { _format_delta(comparison['precision_at_k_delta'], percent=True) }, "
        f"NDCG@{k_value} { _format_delta(comparison['ndcg_at_k_delta']) }."
    )
    return comparison



def _profile_with_weights(
    requested_profile: Dict[str, Any],
    expert_profile: Dict[str, Any],
    component_weights: Dict[str, float],
) -> Dict[str, Any]:
    profile = deepcopy(requested_profile)
    profile["component_weights"] = dict(component_weights)
    profile["expert_component_weights"] = deepcopy(expert_profile.get("component_weights") or {})
    profile["rural_weight_shift"] = deepcopy(expert_profile.get("rural_weight_shift") or {})
    profile["components"] = deepcopy(expert_profile.get("components") or {})
    profile["component_order"] = list(expert_profile.get("component_order") or component_weights.keys())
    profile["thresholds"] = deepcopy(expert_profile.get("thresholds") or {})
    profile["defaults"] = deepcopy(expert_profile.get("defaults") or {})
    return profile



def _build_expert_fallback_profile(
    requested_profile: Dict[str, Any],
    expert_profile: Dict[str, Any],
    windows_bundle: Dict[str, Any],
    reason: str,
) -> Dict[str, Any]:
    profile = _profile_with_weights(requested_profile, expert_profile, expert_profile.get("component_weights") or {})
    profile["status_label"] = "Экспертный fallback"
    profile["status_tone"] = "sand"
    profile["description"] = (
        "Истории пока недостаточно для устойчивой калибровки весов, поэтому сервис использует экспертный профиль как резервный режим."
    )
    summary = (
        f"Калибровка по истории не включилась: {reason} Сейчас доступно {_format_integer(windows_bundle.get('history_days') or 0)} дней истории."
    )
    notes = _unique_non_empty(list(profile.get("notes") or []) + [summary])
    calibration = deepcopy(profile.get("calibration") or {})
    calibration.update(
        {
            "ready": False,
            "used_fallback": True,
            "windows_used": len(windows_bundle.get("windows") or []),
            "candidate_count": 0,
            "summary": summary,
            "notes": _unique_non_empty(list(calibration.get("notes") or []) + [reason, "Использованы экспертные веса как fallback."]),
        }
    )
    profile["notes"] = notes
    profile["calibration"] = calibration
    return profile



def _build_expert_retained_profile(
    requested_profile: Dict[str, Any],
    expert_profile: Dict[str, Any],
    expert_entry: Dict[str, Any],
    best_entry: Dict[str, Any],
    candidate_count: int,
    improvement: float,
) -> Dict[str, Any]:
    profile = _profile_with_weights(requested_profile, expert_profile, expert_profile.get("component_weights") or {})
    profile["status_label"] = "Экспертные веса удержаны"
    profile["status_tone"] = "sky"
    summary = (
        "Исторические окна проверены, но альтернативные веса не показали устойчивого выигрыша по ranking-метрикам. "
        "Сервис оставил экспертный профиль как более стабильный."
    )
    notes = _unique_non_empty(list(profile.get("notes") or []) + [summary])
    calibration = deepcopy(profile.get("calibration") or {})
    calibration.update(
        {
            "ready": True,
            "used_fallback": True,
            "windows_used": int((expert_entry.get("aggregate") or {}).get("windows_count") or 0),
            "candidate_count": candidate_count,
            "summary": summary,
            "selected_metrics": deepcopy(expert_entry.get("aggregate") or {}),
            "expert_metrics": deepcopy(expert_entry.get("aggregate") or {}),
            "comparison": _build_metric_comparison(expert_entry.get("aggregate") or {}, expert_entry.get("aggregate") or {}),
            "objective_improvement": float(improvement),
            "notes": _unique_non_empty(
                list(calibration.get("notes") or [])
                + [
                    f"Лучший альтернативный профиль дал прирост всего {_format_decimal(improvement)} к целевой функции ranking-качества.",
                    _build_metric_comparison(expert_entry.get("aggregate") or {}, expert_entry.get("aggregate") or {}).get("summary") or "",
                    "Разница оказалась недостаточной, поэтому сервис оставил экспертные веса.",
                ]
            ),
        }
    )
    profile["notes"] = notes
    profile["calibration"] = calibration
    return profile



def _build_calibrated_profile(
    requested_profile: Dict[str, Any],
    expert_profile: Dict[str, Any],
    selected_entry: Dict[str, Any],
    expert_entry: Dict[str, Any],
    candidate_count: int,
    improvement: float,
) -> Dict[str, Any]:
    profile = _profile_with_weights(requested_profile, expert_profile, selected_entry.get("weights") or {})
    profile["status_label"] = "Калиброван по истории"
    profile["status_tone"] = "forest"
    summary = (
        f"Веса подобраны по историческим окнам: Top-{int((selected_entry.get('aggregate') or {}).get('k_value') or DEFAULT_RANKING_K)} capture "
        f"{_format_probability(float((selected_entry.get('aggregate') or {}).get('topk_capture_rate') or 0.0))}, "
        f"Precision@{int((selected_entry.get('aggregate') or {}).get('k_value') or DEFAULT_RANKING_K)} "
        f"{_format_probability(float((selected_entry.get('aggregate') or {}).get('precision_at_k') or 0.0))}, "
        f"NDCG@{int((selected_entry.get('aggregate') or {}).get('k_value') or DEFAULT_RANKING_K)} {_format_decimal(float((selected_entry.get('aggregate') or {}).get('ndcg_at_k') or 0.0))}."
    )
    notes = _unique_non_empty(list(profile.get("notes") or []) + [summary])
    calibration = deepcopy(profile.get("calibration") or {})
    calibration.update(
        {
            "ready": True,
            "used_fallback": False,
            "windows_used": int((selected_entry.get("aggregate") or {}).get("windows_count") or 0),
            "candidate_count": candidate_count,
            "summary": summary,
            "selected_metrics": deepcopy(selected_entry.get("aggregate") or {}),
            "expert_metrics": deepcopy(expert_entry.get("aggregate") or {}),
            "comparison": _build_metric_comparison(selected_entry.get("aggregate") or {}, expert_entry.get("aggregate") or {}),
            "objective_improvement": float(improvement),
            "notes": _unique_non_empty(
                list(calibration.get("notes") or [])
                + [
                    f"Относительно экспертного профиля целевая функция ranking-качества улучшилась на {_format_decimal(improvement)}.",
                    (_build_metric_comparison(selected_entry.get("aggregate") or {}, expert_entry.get("aggregate") or {}).get("summary") or ""),
                    "Сигналы внутри компонентов не менялись; подстроены только веса четырех компонент риска.",
                ]
            ),
        }
    )
    profile["notes"] = notes
    profile["calibration"] = calibration
    return profile



def _validation_status(aggregate: Dict[str, Any]) -> tuple[str, str]:
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
    predicted_relevance = [float(actual_counts.get(label, 0)) for label in ranked_labels[:effective_k]]
    ideal_relevance = sorted((float(value) for value in actual_counts.values()), reverse=True)[:effective_k]
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



def _format_delta(value: float, percent: bool = False) -> str:
    numeric = float(value or 0.0)
    prefix = "+" if numeric > 0 else ""
    if percent:
        return prefix + _format_probability(numeric)
    return prefix + _format_decimal(numeric)


def _format_decimal(value: float) -> str:
    rounded = round(float(value), 3)
    if abs(rounded - round(rounded)) < 1e-9:
        return str(int(round(rounded)))
    return str(rounded).replace(".", ",")
