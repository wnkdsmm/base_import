from __future__ import annotations

from copy import deepcopy
from typing import Any, Sequence

from .profiles import DEFAULT_RISK_WEIGHT_MODE, get_risk_weight_profile
from .scoring import _build_territory_rows
from .types import (
    CalibrationComparison,
    CalibrationEntry,
    CalibrationMetrics,
    RiskProfile,
    WeightCandidate,
)
from .utils import _format_decimal, _format_integer, _format_probability, _unique_non_empty

MIN_CALIBRATION_WINDOWS = 4
MIN_COMPONENT_WEIGHT = 0.10
MAX_COMPONENT_WEIGHT = 0.55
CALIBRATION_REGULARIZATION = 0.08
MIN_CALIBRATION_IMPROVEMENT = 0.015


def resolve_weight_profile_for_records(
    records: Sequence[dict[str, Any]],
    planning_horizon_days: int,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
    *,
    enable_calibration: bool = True,
    disabled_summary: str = "",
) -> RiskProfile:
    requested_profile = get_risk_weight_profile(weight_mode)
    if not enable_calibration:
        return _build_uncalibrated_profile(
            requested_profile=requested_profile,
            summary=disabled_summary
            or "Автоматическая калибровка весов временно отключена для облегченного сценария чтения; используется базовый профиль.",
        )

    from . import validation as validation_module

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
    windows_bundle = validation_module._build_historical_windows(records, planning_horizon_days)
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
    evaluations: list[CalibrationEntry] = []
    expert_entry: CalibrationEntry | None = None

    for candidate in candidates:
        candidate_profile = _profile_with_weights(requested_profile, expert_profile, candidate["weights"])
        evaluation = validation_module._evaluate_profile_on_windows(
            candidate_profile,
            evaluation_windows,
            validation_module.DEFAULT_RANKING_K,
        )
        if not evaluation.get("has_metrics"):
            continue

        aggregate = evaluation.get("aggregate") or {}
        objective = validation_module._ranking_objective(aggregate)
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
    improvement = float(
        best_entry.get("regularized_objective", 0.0) - expert_entry.get("regularized_objective", 0.0)
    )

    if best_entry["key"] == "expert" or improvement < MIN_CALIBRATION_IMPROVEMENT:
        return _build_expert_retained_profile(
            requested_profile=requested_profile,
            expert_profile=expert_profile,
            expert_entry=expert_entry,
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


def _build_uncalibrated_profile(
    *,
    requested_profile: RiskProfile,
    summary: str,
) -> RiskProfile:
    profile = deepcopy(requested_profile)
    calibration = deepcopy(profile.get("calibration") or {})
    calibration.update(
        {
            "ready": False,
            "used_fallback": False,
            "windows_used": 0,
            "candidate_count": 0,
            "summary": summary,
            "notes": _unique_non_empty(list(calibration.get("notes") or []) + [summary]),
        }
    )
    profile["calibration"] = calibration
    return profile


def _generate_weight_candidates(profile: RiskProfile) -> list[WeightCandidate]:
    base_weights = {key: float(value) for key, value in (profile.get("component_weights") or {}).items()}
    component_order = list(profile.get("component_order") or base_weights.keys())
    candidates: list[WeightCandidate] = []
    seen = set()

    def add_candidate(key: str, label: str, weights: dict[str, float]) -> None:
        normalized = _normalize_component_weights(weights, component_order)
        if not normalized:
            return
        signature = tuple((component, round(normalized.get(component, 0.0), 4)) for component in component_order)
        if signature in seen:
            return
        seen.add(signature)
        candidates.append({"key": key, "label": label, "weights": normalized})

    add_candidate("expert", "Экспертный профиль", base_weights)
    add_candidate(
        "balanced",
        "Сбалансированный профиль",
        {component: 1.0 / max(1, len(component_order)) for component in component_order},
    )

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


def _normalize_component_weights(
    weights: dict[str, float],
    component_order: Sequence[str],
) -> dict[str, float | None]:
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


def _prepare_evaluation_windows(
    windows: Sequence[dict[str, Any]],
    profile: RiskProfile,
) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for window in windows:
        prepared_window = dict(window)
        prepared_window["predicted_rows"] = _build_territory_rows(
            window.get("train_records") or [],
            int(window.get("horizon_days") or 3),
            weight_mode=profile.get("mode") or DEFAULT_RISK_WEIGHT_MODE,
            profile_override=profile,
        )
        prepared.append(prepared_window)
    return prepared


def _weight_distance(candidate_weights: dict[str, float], base_weights: dict[str, float]) -> float:
    return sum(abs(float(candidate_weights.get(key, 0.0)) - float(base_weights.get(key, 0.0))) for key in base_weights)


def _build_metric_comparison(
    selected_metrics: CalibrationMetrics,
    expert_metrics: CalibrationMetrics,
) -> CalibrationComparison:
    comparison = {
        "top1_hit_delta": float(selected_metrics.get("top1_hit_rate") or 0.0)
        - float(expert_metrics.get("top1_hit_rate") or 0.0),
        "topk_capture_delta": float(selected_metrics.get("topk_capture_rate") or 0.0)
        - float(expert_metrics.get("topk_capture_rate") or 0.0),
        "precision_at_k_delta": float(selected_metrics.get("precision_at_k") or 0.0)
        - float(expert_metrics.get("precision_at_k") or 0.0),
        "ndcg_at_k_delta": float(selected_metrics.get("ndcg_at_k") or 0.0)
        - float(expert_metrics.get("ndcg_at_k") or 0.0),
    }
    k_value = int(selected_metrics.get("k_value") or expert_metrics.get("k_value") or 3)
    comparison["summary"] = (
        f"Сравнение с экспертным профилем: Top-1 {_format_delta(comparison['top1_hit_delta'], percent=True)}, "
        f"Top-{k_value} capture {_format_delta(comparison['topk_capture_delta'], percent=True)}, "
        f"Precision@{k_value} {_format_delta(comparison['precision_at_k_delta'], percent=True)}, "
        f"NDCG@{k_value} {_format_delta(comparison['ndcg_at_k_delta'])}."
    )
    return comparison


def _profile_with_weights(
    requested_profile: RiskProfile,
    expert_profile: RiskProfile,
    component_weights: dict[str, float],
) -> RiskProfile:
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
    requested_profile: RiskProfile,
    expert_profile: RiskProfile,
    windows_bundle: dict[str, Any],
    reason: str,
) -> RiskProfile:
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
            "notes": _unique_non_empty(
                list(calibration.get("notes") or [])
                + [reason, "Использованы экспертные веса как fallback."]
            ),
        }
    )
    profile["notes"] = notes
    profile["calibration"] = calibration
    return profile


def _build_expert_retained_profile(
    requested_profile: RiskProfile,
    expert_profile: RiskProfile,
    expert_entry: CalibrationEntry,
    candidate_count: int,
    improvement: float,
) -> RiskProfile:
    profile = _profile_with_weights(requested_profile, expert_profile, expert_profile.get("component_weights") or {})
    profile["status_label"] = "Экспертные веса удержаны"
    profile["status_tone"] = "sky"
    summary = (
        "Исторические окна проверены, но альтернативные веса не показали устойчивого выигрыша по ranking-метрикам. "
        "Сервис оставил экспертный профиль как более стабильный."
    )
    notes = _unique_non_empty(list(profile.get("notes") or []) + [summary])
    calibration = deepcopy(profile.get("calibration") or {})
    expert_comparison = _build_metric_comparison(
        expert_entry.get("aggregate") or {},
        expert_entry.get("aggregate") or {},
    )
    calibration.update(
        {
            "ready": True,
            "used_fallback": True,
            "windows_used": int((expert_entry.get("aggregate") or {}).get("windows_count") or 0),
            "candidate_count": candidate_count,
            "summary": summary,
            "selected_metrics": deepcopy(expert_entry.get("aggregate") or {}),
            "expert_metrics": deepcopy(expert_entry.get("aggregate") or {}),
            "comparison": expert_comparison,
            "objective_improvement": float(improvement),
            "notes": _unique_non_empty(
                list(calibration.get("notes") or [])
                + [
                    f"Лучший альтернативный профиль дал прирост всего {_format_decimal(improvement)} к целевой функции ranking-качества.",
                    expert_comparison.get("summary") or "",
                    "Разница оказалась недостаточной, поэтому сервис оставил экспертные веса.",
                ]
            ),
        }
    )
    profile["notes"] = notes
    profile["calibration"] = calibration
    return profile


def _build_calibrated_profile(
    requested_profile: RiskProfile,
    expert_profile: RiskProfile,
    selected_entry: CalibrationEntry,
    expert_entry: CalibrationEntry,
    candidate_count: int,
    improvement: float,
) -> RiskProfile:
    profile = _profile_with_weights(requested_profile, expert_profile, selected_entry.get("weights") or {})
    profile["status_label"] = "Калиброван по истории"
    profile["status_tone"] = "forest"
    k_value = int((selected_entry.get("aggregate") or {}).get("k_value") or 3)
    comparison = _build_metric_comparison(
        selected_entry.get("aggregate") or {},
        expert_entry.get("aggregate") or {},
    )
    summary = (
        f"Веса подобраны по историческим окнам: Top-{k_value} capture "
        f"{_format_probability(float((selected_entry.get('aggregate') or {}).get('topk_capture_rate') or 0.0))}, "
        f"Precision@{k_value} {_format_probability(float((selected_entry.get('aggregate') or {}).get('precision_at_k') or 0.0))}, "
        f"NDCG@{k_value} {_format_decimal(float((selected_entry.get('aggregate') or {}).get('ndcg_at_k') or 0.0))}."
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
            "comparison": comparison,
            "objective_improvement": float(improvement),
            "notes": _unique_non_empty(
                list(calibration.get("notes") or [])
                + [
                    f"Относительно экспертного профиля целевая функция ranking-качества улучшилась на {_format_decimal(improvement)}.",
                    comparison.get("summary") or "",
                    "Сигналы внутри компонентов не менялись; подстроены только веса четырех компонентов риска.",
                ]
            ),
        }
    )
    profile["notes"] = notes
    profile["calibration"] = calibration
    return profile


def _format_delta(value: float, percent: bool = False) -> str:
    numeric = float(value or 0.0)
    prefix = "+" if numeric > 0 else ""
    if percent:
        return prefix + _format_probability(numeric)
    return prefix + _format_decimal(numeric)
