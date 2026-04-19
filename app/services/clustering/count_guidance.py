from __future__ import annotations

from typing import Any

from .types import (
    ClusterCountGuidance,
    ClusterCountGuidanceContext,
    ClusterCountRecommendationMessages,
    QualityDiagnostics,
)
from .utils import _format_integer

__all__ = [
    "_build_cluster_count_guidance",
]


def _build_cluster_count_guidance_context(
    requested_cluster_count: int,
    current_cluster_count: int,
    diagnostics: QualityDiagnostics | None = None,
    adjusted_requested_cluster_count: int | None = None,
    cluster_count_is_explicit: bool = False,
) -> ClusterCountGuidanceContext:
    raw_recommended_k = (diagnostics or {}).get("best_quality_k")
    best_silhouette_k = (diagnostics or {}).get("best_silhouette_k")
    best_gap_k = (diagnostics or {}).get("best_gap_k")
    requested_cluster_count = int(requested_cluster_count)
    adjusted_requested_cluster_count = int(
        adjusted_requested_cluster_count if adjusted_requested_cluster_count is not None else requested_cluster_count
    )
    current_cluster_count = int(current_cluster_count)
    request_adjusted = requested_cluster_count != adjusted_requested_cluster_count
    recommendation_gap = bool(raw_recommended_k) and int(raw_recommended_k) != current_cluster_count
    has_recommended_k = bool(raw_recommended_k)
    auto_switched_to_recommended = (
        not cluster_count_is_explicit and adjusted_requested_cluster_count != current_cluster_count
    )
    return {
        "recommended_k": int(raw_recommended_k) if has_recommended_k else raw_recommended_k,
        "best_silhouette_k": best_silhouette_k,
        "best_gap_k": best_gap_k,
        "requested_cluster_count": requested_cluster_count,
        "adjusted_requested_cluster_count": adjusted_requested_cluster_count,
        "current_cluster_count": current_cluster_count,
        "request_adjusted": request_adjusted,
        "recommendation_gap": recommendation_gap,
        "has_recommended_k": has_recommended_k,
        "auto_switched_to_recommended": auto_switched_to_recommended,
    }


def _cluster_count_suggested_label(cluster_count_is_explicit: bool) -> str:
    return "Автоматический выбор" if not cluster_count_is_explicit else "Рекомендуемое значение"


def _initial_cluster_count_recommendation_messages(
    current_cluster_count: int,
    *,
    cluster_count_is_explicit: bool,
) -> ClusterCountRecommendationMessages:
    return {
        "suggested_label": _cluster_count_suggested_label(cluster_count_is_explicit),
        "suggested_note": "Рекомендация по числу групп появится, когда хватит данных для сравнения нескольких вариантов.",
        "current_note": f"Сейчас страница показывает {current_cluster_count} группы.",
        "quality_note": "",
        "notes_message": "",
        "model_note": "",
    }


def _recommended_cluster_count_messages(
    context: ClusterCountGuidanceContext,
    *,
    cluster_count_is_explicit: bool,
) -> ClusterCountRecommendationMessages:
    recommended_k = context["recommended_k"]
    best_gap_k = context["best_gap_k"]
    current_cluster_count = context["current_cluster_count"]
    adjusted_requested_cluster_count = context["adjusted_requested_cluster_count"]
    recommendation_gap = context["recommendation_gap"]
    auto_switched_to_recommended = context["auto_switched_to_recommended"]

    def _append_gap_note(note: str) -> str:
        if best_gap_k is None:
            return note
        if best_gap_k == recommended_k:
            gap_note = f"Gap statistic подтверждает рекомендацию: k={recommended_k}."
        else:
            gap_note = (
                f"По критерию Gap statistic оптимально k={best_gap_k}, "
                f"по совокупному качеству — k={recommended_k}."
            )
        return gap_note if not note else f"{note} {gap_note}".strip()

    if cluster_count_is_explicit and recommendation_gap:
        quality_note = (
            f"Выбранное вручную число групп ({_format_integer(current_cluster_count)}) не совпадает с рекомендацией; "
            f"по совокупности метрик лучше выглядит k={_format_integer(recommended_k)}."
        )
        return {
            "suggested_note": (
                f"Диагностика рекомендует {recommended_k} группы, "
                f"но сейчас сохранено выбранное вручную значение: {current_cluster_count}."
            ),
            "current_note": (
                f"Сейчас используется выбранное вручную значение: {current_cluster_count} группы; "
                f"диагностика советует {recommended_k}."
            ),
            "quality_note": quality_note,
            "model_note": (
                f"Число групп зафиксировано вручную на уровне {current_cluster_count}, "
                "поэтому страница не переключает его автоматически."
            ),
            "notes_message": quality_note,
        }

    if cluster_count_is_explicit:
        quality_note = f"Выбранное вручную число групп ({_format_integer(current_cluster_count)}) совпадает с рекомендацией."
        return {
            "suggested_note": f"Диагностика подтверждает выбранное вручную значение: {current_cluster_count} группы.",
            "current_note": f"Сейчас используется выбранное вручную значение: {current_cluster_count} группы, и оно совпадает с рекомендацией.",
            "quality_note": quality_note,
            "model_note": f"Число групп задано вручную: {current_cluster_count}. Это же значение рекомендует диагностика.",
            "notes_message": quality_note,
        }

    suggested_note = f"Автоматический выбор использует {current_cluster_count} группы как лучший вариант по совокупности метрик."
    if auto_switched_to_recommended:
        quality_note = (
            "Страница автоматически подстроила число групп под рекомендацию диагностики: "
            f"вместо стартового k={_format_integer(adjusted_requested_cluster_count)} "
            f"используется k={_format_integer(current_cluster_count)}."
        )
        quality_note = _append_gap_note(quality_note)
        model_note = (
            f"По умолчанию страница показывает рекомендованное число групп: {current_cluster_count} "
            f"вместо стартового {adjusted_requested_cluster_count}."
        )
        return {
            "suggested_note": suggested_note,
            "current_note": model_note,
            "quality_note": quality_note,
            "model_note": model_note,
            "notes_message": quality_note,
        }

    quality_note = (
        "Текущее число групп уже совпадает с рекомендацией диагностики: "
        f"k={_format_integer(current_cluster_count)}."
    )
    quality_note = _append_gap_note(quality_note)
    model_note = f"По умолчанию страница показывает рекомендованное число групп: {current_cluster_count}."
    return {
        "suggested_note": suggested_note,
        "current_note": model_note,
        "quality_note": quality_note,
        "model_note": model_note,
        "notes_message": quality_note,
    }


def _build_cluster_count_recommendation_context(
    context: ClusterCountGuidanceContext,
    *,
    cluster_count_is_explicit: bool,
) -> ClusterCountRecommendationMessages:
    current_cluster_count = context["current_cluster_count"]
    messages = _initial_cluster_count_recommendation_messages(
        current_cluster_count,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )

    if context["has_recommended_k"]:
        messages.update(
            _recommended_cluster_count_messages(
                context,
                cluster_count_is_explicit=cluster_count_is_explicit,
            )
        )

    return messages


def _apply_cluster_count_adjustment_warning(
    context: ClusterCountGuidanceContext,
    messages: ClusterCountRecommendationMessages,
) -> ClusterCountRecommendationMessages:
    if not context["request_adjusted"]:
        return messages

    adjusted = dict(messages)
    requested_cluster_count = context["requested_cluster_count"]
    adjusted_requested_cluster_count = context["adjusted_requested_cluster_count"]
    current_note = adjusted["current_note"]
    suggested_note = adjusted["suggested_note"]
    quality_note = adjusted["quality_note"]
    model_note = adjusted["model_note"]
    adjustment_note = (
        f"Запрошенное число групп ({_format_integer(requested_cluster_count)}) автоматически скорректировано до "
        f"{_format_integer(adjusted_requested_cluster_count)} из-за ограничений текущей выборки."
    )
    adjusted["current_note"] = adjustment_note if not current_note else f"{adjustment_note} {current_note}".strip()
    adjusted["suggested_note"] = f"{adjustment_note} {suggested_note}".strip()
    adjusted["quality_note"] = adjustment_note if not quality_note else f"{adjustment_note} {quality_note}".strip()
    adjusted["model_note"] = adjustment_note if not model_note else f"{adjustment_note} {model_note}".strip()
    adjusted["notes_message"] = adjusted["quality_note"]
    return adjusted


def _build_cluster_count_guidance(
    requested_cluster_count: int,
    current_cluster_count: int,
    diagnostics: QualityDiagnostics | None = None,
    adjusted_requested_cluster_count: int | None = None,
    cluster_count_is_explicit: bool = False,
) -> ClusterCountGuidance:
    guidance_context = _build_cluster_count_guidance_context(
        requested_cluster_count=requested_cluster_count,
        current_cluster_count=current_cluster_count,
        diagnostics=diagnostics,
        adjusted_requested_cluster_count=adjusted_requested_cluster_count,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    recommendation_context = _build_cluster_count_recommendation_context(
        guidance_context,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    recommendation_context = _apply_cluster_count_adjustment_warning(guidance_context, recommendation_context)
    recommended_k = guidance_context["recommended_k"]
    best_silhouette_k = guidance_context["best_silhouette_k"]
    best_gap_k = guidance_context["best_gap_k"]
    recommendation_gap = guidance_context["recommendation_gap"]
    request_adjusted = guidance_context["request_adjusted"]
    return {
        "recommended_cluster_count": recommended_k,
        "best_silhouette_k": best_silhouette_k,
        "best_gap_k": best_gap_k,
        "has_recommendation_gap": recommendation_gap,
        "request_adjusted": request_adjusted,
        "suggested_label": recommendation_context["suggested_label"],
        "suggested_note": recommendation_context["suggested_note"],
        "current_note": recommendation_context["current_note"],
        "quality_note": recommendation_context["quality_note"],
        "notes_message": recommendation_context["notes_message"],
        "model_note": recommendation_context["model_note"],
    }
