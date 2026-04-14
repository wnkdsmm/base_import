from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Sequence

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
            or "РђРІС‚РѕРјР°С‚РёС‡РµСЃРєР°СЏ РєР°Р»РёР±СЂРѕРІРєР° РІРµСЃРѕРІ РІСЂРµРјРµРЅРЅРѕ РѕС‚РєР»СЋС‡РµРЅР° РґР»СЏ РѕР±Р»РµРіС‡РµРЅРЅРѕРіРѕ СЃС†РµРЅР°СЂРёСЏ С‡С‚РµРЅРёСЏ; РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ Р±Р°Р·РѕРІС‹Р№ РїСЂРѕС„РёР»СЊ.",
        )

    from . import validation as validation_module

    if weight_mode == "expert":
        profile = deepcopy(requested_profile)
        calibration = profile.get("calibration") or {}
        calibration["ready"] = False
        calibration["used_fallback"] = False
        calibration["summary"] = "Р­РєСЃРїРµСЂС‚РЅС‹Р№ СЂРµР¶РёРј РІС‹Р±СЂР°РЅ СЏРІРЅРѕ, РїРѕСЌС‚РѕРјСѓ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєР°СЏ РєР°Р»РёР±СЂРѕРІРєР° РїРѕ РёСЃС‚РѕСЂРёРё РЅРµ РїСЂРёРјРµРЅСЏР»Р°СЃСЊ."
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
                or "РСЃС‚РѕСЂРёРё РїРѕРєР° РЅРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР»СЏ СѓСЃС‚РѕР№С‡РёРІРѕР№ РєР°Р»РёР±СЂРѕРІРєРё РІРµСЃРѕРІ РїРѕ РёСЃС‚РѕСЂРёС‡РµСЃРєРёРј РѕРєРЅР°Рј."
            ),
        )

    evaluation_windows = _prepare_evaluation_windows(
        windows_bundle.get("windows") or [],
        expert_profile,
    )
    candidates = _generate_weight_candidates(expert_profile)
    evaluations: List[CalibrationEntry] = []
    expert_entry: Optional[CalibrationEntry] = None

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
            reason="РќРµ СѓРґР°Р»РѕСЃСЊ СѓСЃС‚РѕР№С‡РёРІРѕ РѕС†РµРЅРёС‚СЊ РЅРё РѕРґРёРЅ РЅР°Р±РѕСЂ РІРµСЃРѕРІ РЅР° РёСЃС‚РѕСЂРёС‡РµСЃРєРёС… РѕРєРЅР°С….",
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


def _generate_weight_candidates(profile: RiskProfile) -> List[WeightCandidate]:
    base_weights = {key: float(value) for key, value in (profile.get("component_weights") or {}).items()}
    component_order = list(profile.get("component_order") or base_weights.keys())
    candidates: List[WeightCandidate] = []
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

    add_candidate("expert", "Р­РєСЃРїРµСЂС‚РЅС‹Р№ РїСЂРѕС„РёР»СЊ", base_weights)
    add_candidate(
        "balanced",
        "РЎР±Р°Р»Р°РЅСЃРёСЂРѕРІР°РЅРЅС‹Р№ РїСЂРѕС„РёР»СЊ",
        {component: 1.0 / max(1, len(component_order)) for component in component_order},
    )

    for receiver in component_order:
        focused = {component: 0.18 for component in component_order}
        focused[receiver] = 0.46
        add_candidate(f"focus_{receiver}", f"Р¤РѕРєСѓСЃ: {receiver}", focused)

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
                    f"РЎРґРІРёРі {donor}->{receiver}",
                    shifted,
                )
    return candidates


def _normalize_component_weights(
    weights: Dict[str, float],
    component_order: Sequence[str],
) -> Optional[Dict[str, float]]:
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
) -> List[dict[str, Any]]:
    prepared: List[dict[str, Any]] = []
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


def _weight_distance(candidate_weights: Dict[str, float], base_weights: Dict[str, float]) -> float:
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
        f"РЎСЂР°РІРЅРµРЅРёРµ СЃ СЌРєСЃРїРµСЂС‚РЅС‹Рј РїСЂРѕС„РёР»РµРј: Top-1 {_format_delta(comparison['top1_hit_delta'], percent=True)}, "
        f"Top-{k_value} capture {_format_delta(comparison['topk_capture_delta'], percent=True)}, "
        f"Precision@{k_value} {_format_delta(comparison['precision_at_k_delta'], percent=True)}, "
        f"NDCG@{k_value} {_format_delta(comparison['ndcg_at_k_delta'])}."
    )
    return comparison


def _profile_with_weights(
    requested_profile: RiskProfile,
    expert_profile: RiskProfile,
    component_weights: Dict[str, float],
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
    profile["status_label"] = "Р­РєСЃРїРµСЂС‚РЅС‹Р№ fallback"
    profile["status_tone"] = "sand"
    profile["description"] = (
        "РСЃС‚РѕСЂРёРё РїРѕРєР° РЅРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР»СЏ СѓСЃС‚РѕР№С‡РёРІРѕР№ РєР°Р»РёР±СЂРѕРІРєРё РІРµСЃРѕРІ, РїРѕСЌС‚РѕРјСѓ СЃРµСЂРІРёСЃ РёСЃРїРѕР»СЊР·СѓРµС‚ СЌРєСЃРїРµСЂС‚РЅС‹Р№ РїСЂРѕС„РёР»СЊ РєР°Рє СЂРµР·РµСЂРІРЅС‹Р№ СЂРµР¶РёРј."
    )
    summary = (
        f"РљР°Р»РёР±СЂРѕРІРєР° РїРѕ РёСЃС‚РѕСЂРёРё РЅРµ РІРєР»СЋС‡РёР»Р°СЃСЊ: {reason} РЎРµР№С‡Р°СЃ РґРѕСЃС‚СѓРїРЅРѕ {_format_integer(windows_bundle.get('history_days') or 0)} РґРЅРµР№ РёСЃС‚РѕСЂРёРё."
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
                + [reason, "РСЃРїРѕР»СЊР·РѕРІР°РЅС‹ СЌРєСЃРїРµСЂС‚РЅС‹Рµ РІРµСЃР° РєР°Рє fallback."]
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
    profile["status_label"] = "Р­РєСЃРїРµСЂС‚РЅС‹Рµ РІРµСЃР° СѓРґРµСЂР¶Р°РЅС‹"
    profile["status_tone"] = "sky"
    summary = (
        "РСЃС‚РѕСЂРёС‡РµСЃРєРёРµ РѕРєРЅР° РїСЂРѕРІРµСЂРµРЅС‹, РЅРѕ Р°Р»СЊС‚РµСЂРЅР°С‚РёРІРЅС‹Рµ РІРµСЃР° РЅРµ РїРѕРєР°Р·Р°Р»Рё СѓСЃС‚РѕР№С‡РёРІРѕРіРѕ РІС‹РёРіСЂС‹С€Р° РїРѕ ranking-РјРµС‚СЂРёРєР°Рј. "
        "РЎРµСЂРІРёСЃ РѕСЃС‚Р°РІРёР» СЌРєСЃРїРµСЂС‚РЅС‹Р№ РїСЂРѕС„РёР»СЊ РєР°Рє Р±РѕР»РµРµ СЃС‚Р°Р±РёР»СЊРЅС‹Р№."
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
                    f"Р›СѓС‡С€РёР№ Р°Р»СЊС‚РµСЂРЅР°С‚РёРІРЅС‹Р№ РїСЂРѕС„РёР»СЊ РґР°Р» РїСЂРёСЂРѕСЃС‚ РІСЃРµРіРѕ {_format_decimal(improvement)} Рє С†РµР»РµРІРѕР№ С„СѓРЅРєС†РёРё ranking-РєР°С‡РµСЃС‚РІР°.",
                    expert_comparison.get("summary") or "",
                    "Р Р°Р·РЅРёС†Р° РѕРєР°Р·Р°Р»Р°СЃСЊ РЅРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕР№, РїРѕСЌС‚РѕРјСѓ СЃРµСЂРІРёСЃ РѕСЃС‚Р°РІРёР» СЌРєСЃРїРµСЂС‚РЅС‹Рµ РІРµСЃР°.",
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
    profile["status_label"] = "РљР°Р»РёР±СЂРѕРІР°РЅ РїРѕ РёСЃС‚РѕСЂРёРё"
    profile["status_tone"] = "forest"
    k_value = int((selected_entry.get("aggregate") or {}).get("k_value") or 3)
    comparison = _build_metric_comparison(
        selected_entry.get("aggregate") or {},
        expert_entry.get("aggregate") or {},
    )
    summary = (
        f"Р’РµСЃР° РїРѕРґРѕР±СЂР°РЅС‹ РїРѕ РёСЃС‚РѕСЂРёС‡РµСЃРєРёРј РѕРєРЅР°Рј: Top-{k_value} capture "
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
                    f"РћС‚РЅРѕСЃРёС‚РµР»СЊРЅРѕ СЌРєСЃРїРµСЂС‚РЅРѕРіРѕ РїСЂРѕС„РёР»СЏ С†РµР»РµРІР°СЏ С„СѓРЅРєС†РёСЏ ranking-РєР°С‡РµСЃС‚РІР° СѓР»СѓС‡С€РёР»Р°СЃСЊ РЅР° {_format_decimal(improvement)}.",
                    comparison.get("summary") or "",
                    "РЎРёРіРЅР°Р»С‹ РІРЅСѓС‚СЂРё РєРѕРјРїРѕРЅРµРЅС‚РѕРІ РЅРµ РјРµРЅСЏР»РёСЃСЊ; РїРѕРґСЃС‚СЂРѕРµРЅС‹ С‚РѕР»СЊРєРѕ РІРµСЃР° С‡РµС‚С‹СЂРµС… РєРѕРјРїРѕРЅРµРЅС‚РѕРІ СЂРёСЃРєР°.",
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
