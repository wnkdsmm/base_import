from __future__ import annotations

from typing import Any, Dict, List, Sequence

from .constants import (
    CLUSTER_COUNT_OPTIONS,
    LOW_SUPPORT_TERRITORY_THRESHOLD,
    RATE_SMOOTHING_PRIOR_STRENGTH,
    STABILITY_RESAMPLE_RATIO,
)
from .utils import _format_integer, _format_number, _format_percent

__all__ = [
    "_apply_cluster_count_adjustment_warning",
    "_build_ablation_warning_note",
    "_build_cluster_count_guidance",
    "_build_cluster_count_guidance_context",
    "_build_cluster_count_methodology_meta",
    "_build_cluster_count_recommendation_context",
    "_build_cluster_shape_note",
    "_build_clustering_quality_assessment",
    "_build_configuration_recommendation_note",
    "_build_feature_selection_quality_label_context",
    "_build_method_comparison_scope_note",
    "_build_method_recommendation_note",
    "_build_quality_dissertation_points",
    "_build_quality_method_comparison_rows",
    "_build_quality_methodology_items",
    "_build_quality_metric_cards",
    "_build_quality_note_context",
    "_build_stability_note",
    "_cluster_count_suggested_label",
    "_empty_clustering_quality_assessment",
    "_format_configuration_label",
    "_format_quality_method_selection_label",
    "_initial_cluster_count_recommendation_messages",
    "_recommended_cluster_count_messages",
    "_resolve_method_algorithm_key",
    "_resolve_quality_configuration_context",
    "_summarize_segmentation_strength",
]


def _format_configuration_label(configuration: Dict[str, Any] | None) -> str:
    if not configuration:
        return "—"
    method_label = str(configuration.get("method_label") or "Метод")
    cluster_count = configuration.get("cluster_count")
    if cluster_count:
        return f"{method_label}, k={_format_integer(cluster_count)}"
    return method_label


def _empty_clustering_quality_assessment() -> Dict[str, Any]:
    return {
        "ready": False,
        "title": "Оценка качества кластеризации",
        "subtitle": "После расчета здесь появятся метрики качества, устойчивость на повторных подвыборках и сравнение алгоритмов.",
        "metric_cards": [],
        "methodology_items": [],
        "comparison_rows": [],
        "dissertation_points": ["Пока недостаточно данных для расчета метрик качества кластеризации."],
    }


def _build_configuration_recommendation_note(
    working_configuration: Dict[str, Any] | None,
    recommended_configuration: Dict[str, Any] | None,
    *,
    cluster_count_is_explicit: bool,
) -> str:
    working_label = _format_configuration_label(working_configuration)
    recommended_label = _format_configuration_label(recommended_configuration)
    if not recommended_configuration or working_label == recommended_label:
        if cluster_count_is_explicit:
            return f"На пользовательском k текущий вывод уже использует лучшую сопоставимую конфигурацию: {working_label}."
        return f"По умолчанию страница сразу показывает рекомендуемую конфигурацию: {working_label}."
    if cluster_count_is_explicit:
        return (
            f"На пользовательском k текущий вывод использует лучшую сопоставимую конфигурацию {working_label}, "
            f"но по всему доступному диапазону убедительнее выглядит {recommended_label}."
        )
    return f"Рабочий вывод построен по конфигурации {working_label}, а recommendation engine выбирает {recommended_label}."


def _resolve_quality_configuration_context(
    *,
    method_comparison: Sequence[Dict[str, Any]],
    diagnostics: Dict[str, Any] | None,
    cluster_count: int,
) -> Dict[str, Any]:
    diagnostics = diagnostics or {}
    recommended_configuration = dict(diagnostics.get("best_configuration") or {})
    recommended_k = int(recommended_configuration.get("cluster_count") or diagnostics.get("best_quality_k") or cluster_count)
    selected_method = next(
        (row for row in method_comparison if row.get("is_selected")),
        method_comparison[0] if method_comparison else None,
    )
    recommended_row = next((row for row in method_comparison if row.get("is_recommended")), selected_method)
    working_configuration = {**dict(selected_method or {}), "cluster_count": cluster_count}
    effective_recommended_configuration = (
        recommended_configuration
        or {**dict(recommended_row or {}), "cluster_count": recommended_k}
    )
    return {
        "recommended_configuration": recommended_configuration,
        "recommended_k": recommended_k,
        "best_silhouette_k": diagnostics.get("best_silhouette_k"),
        "selected_method": selected_method,
        "recommended_row": recommended_row,
        "working_configuration": working_configuration,
        "effective_recommended_configuration": effective_recommended_configuration,
        "recommended_method": effective_recommended_configuration or recommended_row or selected_method,
        "working_config_label": _format_configuration_label(working_configuration),
        "recommended_config_label": _format_configuration_label(effective_recommended_configuration),
    }


def _build_feature_selection_quality_label_context(
    feature_selection_report: Dict[str, Any] | None,
) -> Dict[str, Any]:
    report = feature_selection_report or {}
    return {
        "mode_label": str(report.get("volume_role_label") or "Профиль территории"),
        "mode_note": str(report.get("volume_note") or ""),
        "weighting_label": str(report.get("weighting_label") or "Равный вес территорий"),
        "weighting_note": str(report.get("weighting_note") or ""),
        "weighting_meta": str(report.get("weighting_meta") or ""),
        "ablation_rows": list(report.get("ablation_rows") or []),
    }


def _build_ablation_warning_note(ablation_rows: Sequence[Dict[str, Any]]) -> str:
    negative_adds = [
        row for row in ablation_rows if row.get("direction") == "add" and float(row.get("delta_score") or 0.0) < 0.0
    ]
    if not negative_adds:
        return ""

    worst_feature = min(negative_adds, key=lambda item: float(item.get("delta_score") or 0.0))
    return (
        f"В малом ablation-анализе признак '{worst_feature['feature']}' не вошёл в default feature set, "
        "потому что его добавление ухудшало качество кластеризации."
    )


def _format_quality_method_selection_label(row: Dict[str, Any]) -> str:
    if row.get("is_selected") and row.get("is_recommended"):
        return "Рабочий и лучший на текущем k"
    if row.get("is_selected"):
        return "Рабочий вывод"
    if row.get("is_recommended"):
        return "Лучше на текущем k"
    return "Сравнение"


def _build_quality_method_comparison_rows(
    method_comparison: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return [
        {
            "method_label": row.get("method_label", "Метод"),
            "selection_label": _format_quality_method_selection_label(row),
            "silhouette_display": _format_number(row.get("silhouette"), 3),
            "davies_display": _format_number(row.get("davies_bouldin"), 3),
            "calinski_display": _format_number(row.get("calinski_harabasz"), 1),
            "balance_display": _format_percent(row.get("cluster_balance_ratio") or 0.0),
        }
        for row in method_comparison
    ]


def _build_quality_dissertation_points(
    *,
    segmentation_note: str,
    method_note: str,
    cluster_count_guidance: Dict[str, Any],
    recommended_config_label: str,
    working_config_label: str,
    recommended_k: int | None,
    best_silhouette_k: Any,
    stability_note: str,
    low_support_display: str,
    selected_features: Sequence[str],
    comparison_scope_note: str,
    cluster_shape_note: str,
    weighting_note: str,
    mode_note: str,
    ablation_note: str,
) -> List[str]:
    dissertation_points = [
        segmentation_note,
        method_note,
        str(cluster_count_guidance.get("quality_note") or ""),
        (
            f"Рекомендуемая конфигурация по совокупности метрик: {recommended_config_label}."
            if recommended_config_label != working_config_label
            else f"Рабочая конфигурация {working_config_label} уже совпадает с recommendation engine."
        ),
        (
            f"Пик silhouette отдельно приходится на k={_format_integer(best_silhouette_k)}, поэтому выбор k лучше читать вместе с балансом кластеров и риском микрокластеров."
            if recommended_k and best_silhouette_k and recommended_k != best_silhouette_k
            else "Silhouette, баланс и размеры кластеров не дают заметного конфликта по выбору k."
        ),
        stability_note,
        f"{low_support_display} территорий имеют не более {LOW_SUPPORT_TERRITORY_THRESHOLD} пожаров, поэтому долевые признаки считаются через empirical Bayes shrinkage к глобальному среднему: вместо raw 0/1 к доле добавляются около {int(RATE_SMOOTHING_PRIOR_STRENGTH)} псевдо-наблюдений из общего профиля.",
        f"Сравнение методов выполнено на том же наборе признаков: {', '.join(selected_features)}.",
    ]
    if comparison_scope_note:
        dissertation_points.append(comparison_scope_note)
    if cluster_shape_note:
        dissertation_points.append(cluster_shape_note)
    if weighting_note:
        dissertation_points.append(weighting_note)
    if mode_note:
        dissertation_points.append(mode_note)
    if ablation_note:
        dissertation_points.append(ablation_note)
    return [item for item in dissertation_points if str(item).strip()]


def _build_quality_note_context(
    *,
    clustering: Dict[str, Any],
    selected_method: Dict[str, Any] | None,
    working_configuration: Dict[str, Any],
    effective_recommended_configuration: Dict[str, Any],
    recommended_method: Dict[str, Any],
    cluster_count: int,
    recommended_k: int | None,
    method_comparison: Sequence[Dict[str, Any]],
    feature_selection_report: Dict[str, Any] | None,
    resample_share_label: str,
    cluster_count_is_explicit: bool,
) -> Dict[str, Any]:
    segmentation_summary = _summarize_segmentation_strength(
        clustering,
        selected_method=selected_method,
        recommended_method=recommended_method,
        cluster_count=cluster_count,
        recommended_k=recommended_k,
    )
    label_context = _build_feature_selection_quality_label_context(feature_selection_report)
    return {
        "segmentation_summary": segmentation_summary,
        "stability_note": _build_stability_note(clustering, resample_share_label),
        "method_note": _build_configuration_recommendation_note(
            working_configuration,
            effective_recommended_configuration,
            cluster_count_is_explicit=cluster_count_is_explicit,
        ),
        "comparison_scope_note": _build_method_comparison_scope_note(method_comparison),
        "cluster_shape_note": _build_cluster_shape_note(clustering),
        "label_context": label_context,
        "ablation_note": _build_ablation_warning_note(label_context["ablation_rows"]),
    }


def _build_quality_metric_cards(clustering: Dict[str, Any], resample_share_label: str) -> List[Dict[str, Any]]:
    return [
        {"label": "Коэффициент силуэта", "value": _format_number(clustering.get("silhouette"), 3), "meta": "выше — лучше"},
        {"label": "Индекс Дэвиса-Болдина", "value": _format_number(clustering.get("davies_bouldin"), 3), "meta": "ниже - лучше"},
        {"label": "Индекс Калински-Харабаза", "value": _format_number(clustering.get("calinski_harabasz"), 1), "meta": "выше - лучше"},
        {
            "label": "Баланс кластеров",
            "value": _format_percent(clustering.get("cluster_balance_ratio") or 0.0),
            "meta": f"min/max: {_format_integer(clustering.get('smallest_cluster_size'))} / {_format_integer(clustering.get('largest_cluster_size'))}",
        },
        {"label": "Устойчивость на подвыборках", "value": _format_number(clustering.get("stability_ari"), 3), "meta": f"повторные {resample_share_label}-подвыборки"},
    ]


def _build_quality_methodology_items(
    *,
    cluster_count: int,
    selected_features: Sequence[str],
    selected_method: Dict[str, Any] | None,
    recommended_method: Dict[str, Any],
    working_config_label: str,
    recommended_config_label: str,
    segmentation_label: str,
    mode_label: str,
    weighting_label: str,
    weighting_meta: str,
    cluster_count_guidance: Dict[str, Any],
    low_support_display: str,
    explained_variance: Any,
    cluster_count_is_explicit: bool,
) -> List[Dict[str, Any]]:
    return [
        {"label": "Рабочая конфигурация", "value": working_config_label, "meta": "по ней построены текущие кластеры на странице"},
        {"label": "Рекомендуемая конфигурация", "value": recommended_config_label, "meta": "лучший bundle mode / weighting / method / k в доступном диапазоне"},
        {"label": "Режим выбора k", "value": "Пользовательский k" if cluster_count_is_explicit else "Автовыбор", "meta": str(cluster_count_guidance.get("current_note") or "")},
        {"label": "Рабочий метод", "value": str((selected_method or {}).get("method_label") or "KMeans"), "meta": "лучший метод среди сопоставимых вариантов на текущем k"},
        {"label": "Рекомендация по методу", "value": str((recommended_method or {}).get("method_label") or "KMeans"), "meta": "какой алгоритм выигрывает в рекомендуемой конфигурации"},
        {"label": "Сила сегментации", "value": segmentation_label, "meta": "сводная оценка по silhouette / DB / устойчивости / размерам кластеров"},
        {"label": "Режим типологии", "value": mode_label, "meta": "что именно кластеризуется по умолчанию"},
        {"label": "Весы территорий", "value": weighting_label, "meta": weighting_meta or "как нагрузка влияет на центры или почему sample weights не используются"},
        {
            "label": "Число кластеров",
            "value": _format_integer(cluster_count),
            "meta": str(cluster_count_guidance.get("methodology_meta") or f"диагностика ограничена диапазоном {CLUSTER_COUNT_OPTIONS[0]}..{CLUSTER_COUNT_OPTIONS[-1]}, как в UI"),
        },
        {"label": "Признаков", "value": _format_integer(len(selected_features)), "meta": "выбраны по малому ablation-анализу и вкладу в silhouette / DB / CH"},
        {"label": "Низкая поддержка", "value": low_support_display, "meta": f"территории с ≤{LOW_SUPPORT_TERRITORY_THRESHOLD} пожарами сглажены к общему уровню"},
        {"label": "Покрытие PCA", "value": _format_percent(explained_variance or 0.0), "meta": "доля дисперсии на 2D-проекции"},
    ]


def _build_clustering_quality_assessment(
    clustering: Dict[str, Any],
    method_comparison: Sequence[Dict[str, Any]],
    cluster_count: int,
    selected_features: Sequence[str],
    diagnostics: Dict[str, Any] | None = None,
    support_summary: Dict[str, Any] | None = None,
    feature_selection_report: Dict[str, Any] | None = None,
    requested_cluster_count: int | None = None,
    resolved_requested_cluster_count: int | None = None,
    cluster_count_is_explicit: bool = False,
    cluster_count_guidance: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    if clustering.get("silhouette") is None:
        payload = _empty_clustering_quality_assessment()
        payload["dissertation_points"] = ["В текущем срезе кластеризация построена, но внутренних метрик пока недостаточно для устойчивой интерпретации качества."]
        return payload

    low_support_share = float((support_summary or {}).get("low_support_share") or 0.0)
    low_support_display = _format_percent(low_support_share)
    resample_share_label = f"{int(round(STABILITY_RESAMPLE_RATIO * 100.0))}%"
    quality_context = _resolve_quality_configuration_context(
        method_comparison=method_comparison,
        diagnostics=diagnostics,
        cluster_count=cluster_count,
    )
    recommended_k = quality_context["recommended_k"]
    best_silhouette_k = quality_context["best_silhouette_k"]
    cluster_count_guidance = cluster_count_guidance or _build_cluster_count_guidance(
        requested_cluster_count=requested_cluster_count or cluster_count,
        current_cluster_count=cluster_count,
        diagnostics=diagnostics,
        adjusted_requested_cluster_count=resolved_requested_cluster_count,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    selected_method = quality_context["selected_method"]
    working_configuration = quality_context["working_configuration"]
    effective_recommended_configuration = quality_context["effective_recommended_configuration"]
    recommended_method = quality_context["recommended_method"]
    working_config_label = quality_context["working_config_label"]
    recommended_config_label = quality_context["recommended_config_label"]

    note_context = _build_quality_note_context(
        clustering=clustering,
        selected_method=selected_method,
        working_configuration=working_configuration,
        effective_recommended_configuration=effective_recommended_configuration,
        recommended_method=recommended_method,
        cluster_count=cluster_count,
        recommended_k=recommended_k,
        method_comparison=method_comparison,
        feature_selection_report=feature_selection_report,
        resample_share_label=resample_share_label,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    segmentation_summary = note_context["segmentation_summary"]
    label_context = note_context["label_context"]
    mode_label = label_context["mode_label"]
    mode_note = label_context["mode_note"]
    weighting_label = label_context["weighting_label"]
    weighting_note = label_context["weighting_note"]
    weighting_meta = label_context["weighting_meta"]
    comparison_rows = _build_quality_method_comparison_rows(method_comparison)
    dissertation_points = _build_quality_dissertation_points(
        segmentation_note=segmentation_summary["note"],
        method_note=note_context["method_note"],
        cluster_count_guidance=cluster_count_guidance,
        recommended_config_label=recommended_config_label,
        working_config_label=working_config_label,
        recommended_k=recommended_k,
        best_silhouette_k=best_silhouette_k,
        stability_note=note_context["stability_note"],
        low_support_display=low_support_display,
        selected_features=selected_features,
        comparison_scope_note=note_context["comparison_scope_note"],
        cluster_shape_note=note_context["cluster_shape_note"],
        weighting_note=weighting_note,
        mode_note=mode_note,
        ablation_note=note_context["ablation_note"],
    )

    return {
        "ready": True,
        "title": "Оценка качества кластеризации",
        "subtitle": "Ниже собраны честная оценка силы сегментации, устойчивость на повторных подвыборках и recommendation engine по конфигурации mode / weighting / method / k.",
        "metric_cards": _build_quality_metric_cards(clustering, resample_share_label),
        "methodology_items": _build_quality_methodology_items(
            cluster_count=cluster_count,
            selected_features=selected_features,
            selected_method=selected_method,
            recommended_method=recommended_method,
            working_config_label=working_config_label,
            recommended_config_label=recommended_config_label,
            segmentation_label=segmentation_summary["label"],
            mode_label=mode_label,
            weighting_label=weighting_label,
            weighting_meta=weighting_meta,
            cluster_count_guidance=cluster_count_guidance,
            low_support_display=low_support_display,
            explained_variance=clustering.get("explained_variance"),
            cluster_count_is_explicit=cluster_count_is_explicit,
        ),
        "comparison_rows": comparison_rows,
        "dissertation_points": dissertation_points,
    }


def _build_cluster_count_guidance_context(
    requested_cluster_count: int,
    current_cluster_count: int,
    diagnostics: Dict[str, Any] | None = None,
    adjusted_requested_cluster_count: int | None = None,
    cluster_count_is_explicit: bool = False,
) -> Dict[str, Any]:
    raw_recommended_k = (diagnostics or {}).get("best_quality_k")
    best_silhouette_k = (diagnostics or {}).get("best_silhouette_k")
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
        "requested_cluster_count": requested_cluster_count,
        "adjusted_requested_cluster_count": adjusted_requested_cluster_count,
        "current_cluster_count": current_cluster_count,
        "request_adjusted": request_adjusted,
        "recommendation_gap": recommendation_gap,
        "has_recommended_k": has_recommended_k,
        "auto_switched_to_recommended": auto_switched_to_recommended,
    }


def _cluster_count_suggested_label(cluster_count_is_explicit: bool) -> str:
    return "Автовыбор k" if not cluster_count_is_explicit else "Рекомендуемый k"


def _initial_cluster_count_recommendation_messages(
    current_cluster_count: int,
    *,
    cluster_count_is_explicit: bool,
) -> Dict[str, str]:
    return {
        "suggested_label": _cluster_count_suggested_label(cluster_count_is_explicit),
        "suggested_note": "Диагностика k появится, когда хватит данных для сравнения нескольких вариантов.",
        "current_note": f"Сейчас основной вывод показан для k={_format_integer(current_cluster_count)}.",
        "quality_note": "",
        "notes_message": "",
        "model_note": "",
    }


def _recommended_cluster_count_messages(
    context: Dict[str, Any],
    *,
    cluster_count_is_explicit: bool,
) -> Dict[str, str]:
    recommended_k = context["recommended_k"]
    current_cluster_count = context["current_cluster_count"]
    adjusted_requested_cluster_count = context["adjusted_requested_cluster_count"]
    recommendation_gap = context["recommendation_gap"]
    auto_switched_to_recommended = context["auto_switched_to_recommended"]

    if cluster_count_is_explicit and recommendation_gap:
        quality_note = (
            f"Пользовательский k={_format_integer(current_cluster_count)} не совпадает с рекомендацией diagnostics; "
            f"по совокупности метрик лучше выглядит k={_format_integer(recommended_k)}."
        )
        return {
            "suggested_note": (
                f"Диагностика рекомендует k={_format_integer(recommended_k)}; "
                f"сейчас сохранён пользовательский k={_format_integer(current_cluster_count)}."
            ),
            "current_note": (
                f"Сейчас показан пользовательский k={_format_integer(current_cluster_count)}; "
                f"диагностика рекомендует k={_format_integer(recommended_k)}."
            ),
            "quality_note": quality_note,
            "model_note": (
                f"Пользователь зафиксировал k={_format_integer(current_cluster_count)}, "
                "поэтому страница не переключает число кластеров автоматически."
            ),
            "notes_message": quality_note,
        }

    if cluster_count_is_explicit:
        quality_note = f"Пользовательский k={_format_integer(current_cluster_count)} согласован с recommendation engine."
        return {
            "suggested_note": f"Диагностика подтверждает пользовательский k={_format_integer(current_cluster_count)}.",
            "current_note": f"Пользовательский k={_format_integer(current_cluster_count)} совпадает с рекомендацией диагностики.",
            "quality_note": quality_note,
            "model_note": f"Пользовательский k={_format_integer(current_cluster_count)} совпадает с рекомендуемым значением.",
            "notes_message": quality_note,
        }

    suggested_note = f"Автовыбор использует k={_format_integer(current_cluster_count)} как лучший вариант по совокупности метрик."
    if auto_switched_to_recommended:
        quality_note = (
            "Рабочий вывод автоматически синхронизирован с recommendation engine: "
            f"вместо стартового k={_format_integer(adjusted_requested_cluster_count)} "
            f"показан k={_format_integer(current_cluster_count)}."
        )
        model_note = (
            f"По умолчанию страница показывает рекомендуемый k={_format_integer(current_cluster_count)} "
            f"вместо стартового k={_format_integer(adjusted_requested_cluster_count)}."
        )
        return {
            "suggested_note": suggested_note,
            "current_note": model_note,
            "quality_note": quality_note,
            "model_note": model_note,
            "notes_message": quality_note,
        }

    quality_note = (
        "Рабочий вывод уже синхронизирован с recommendation engine "
        f"по числу кластеров: k={_format_integer(current_cluster_count)}."
    )
    model_note = f"По умолчанию страница показывает рекомендуемый k={_format_integer(current_cluster_count)}."
    return {
        "suggested_note": suggested_note,
        "current_note": model_note,
        "quality_note": quality_note,
        "model_note": model_note,
        "notes_message": quality_note,
    }


def _build_cluster_count_recommendation_context(
    context: Dict[str, Any],
    *,
    cluster_count_is_explicit: bool,
) -> Dict[str, str]:
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
    context: Dict[str, Any],
    messages: Dict[str, str],
) -> Dict[str, str]:
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
        f"Запрошенное значение k={_format_integer(requested_cluster_count)} автоматически скорректировано до "
        f"k={_format_integer(adjusted_requested_cluster_count)} из-за ограничений выбранной выборки."
    )
    adjusted["current_note"] = adjustment_note if not current_note else f"{adjustment_note} {current_note}".strip()
    adjusted["suggested_note"] = f"{adjustment_note} {suggested_note}".strip()
    adjusted["quality_note"] = adjustment_note if not quality_note else f"{adjustment_note} {quality_note}".strip()
    adjusted["model_note"] = adjustment_note if not model_note else f"{adjustment_note} {model_note}".strip()
    adjusted["notes_message"] = adjusted["quality_note"]
    return adjusted


def _build_cluster_count_methodology_meta(
    context: Dict[str, Any],
    *,
    cluster_count_is_explicit: bool,
) -> str:
    recommended_k = context["recommended_k"]
    best_silhouette_k = context["best_silhouette_k"]
    requested_cluster_count = context["requested_cluster_count"]
    adjusted_requested_cluster_count = context["adjusted_requested_cluster_count"]
    current_cluster_count = context["current_cluster_count"]
    request_adjusted = context["request_adjusted"]
    auto_switched_to_recommended = context["auto_switched_to_recommended"]
    meta_parts = [
        f"{'пользовательский' if cluster_count_is_explicit else 'рабочий'} k={_format_integer(current_cluster_count)}"
    ]
    if request_adjusted:
        meta_parts.append(f"запрошено k={_format_integer(requested_cluster_count)}")
    elif auto_switched_to_recommended:
        meta_parts.append(f"стартовый k={_format_integer(adjusted_requested_cluster_count)}")
    if recommended_k:
        meta_parts.append(f"рекомендуемое k={_format_integer(recommended_k)}")
    if best_silhouette_k:
        meta_parts.append(f"пик silhouette на k={_format_integer(best_silhouette_k)}")
    if not recommended_k and not best_silhouette_k:
        meta_parts.append(
            f"диагностика ограничена диапазоном {CLUSTER_COUNT_OPTIONS[0]}..{CLUSTER_COUNT_OPTIONS[-1]}, как в UI"
        )
    return "; ".join(meta_parts)


def _build_cluster_count_guidance(
    requested_cluster_count: int,
    current_cluster_count: int,
    diagnostics: Dict[str, Any] | None = None,
    adjusted_requested_cluster_count: int | None = None,
    cluster_count_is_explicit: bool = False,
) -> Dict[str, Any]:
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
    recommendation_gap = guidance_context["recommendation_gap"]
    request_adjusted = guidance_context["request_adjusted"]
    methodology_meta = _build_cluster_count_methodology_meta(
        guidance_context,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    return {
        "recommended_cluster_count": recommended_k,
        "best_silhouette_k": best_silhouette_k,
        "has_recommendation_gap": recommendation_gap,
        "request_adjusted": request_adjusted,
        "suggested_label": recommendation_context["suggested_label"],
        "suggested_note": recommendation_context["suggested_note"],
        "current_note": recommendation_context["current_note"],
        "quality_note": recommendation_context["quality_note"],
        "notes_message": recommendation_context["notes_message"],
        "model_note": recommendation_context["model_note"],
        "methodology_meta": methodology_meta,
    }


def _summarize_segmentation_strength(
    clustering: Dict[str, Any],
    selected_method: Dict[str, Any] | None = None,
    recommended_method: Dict[str, Any] | None = None,
    cluster_count: int | None = None,
    recommended_k: int | None = None,
) -> Dict[str, str]:
    silhouette = float(clustering.get("silhouette") or 0.0)
    davies_bouldin = float(clustering.get("davies_bouldin") or 0.0)
    balance_ratio = float(clustering.get("cluster_balance_ratio") or 0.0)
    stability_ari = float(clustering.get("stability_ari") or 0.0)
    initialization_ari = float(clustering.get("initialization_ari") or 0.0)
    has_microclusters = bool(clustering.get("has_microclusters"))
    selected_algorithm_key = _resolve_method_algorithm_key(selected_method)
    recommended_algorithm_key = _resolve_method_algorithm_key(recommended_method)
    algorithm_mismatch = bool(selected_method and recommended_method) and selected_algorithm_key != recommended_algorithm_key
    configuration_mismatch = bool(selected_method and recommended_method) and (
        (selected_method or {}).get("method_key") != (recommended_method or {}).get("method_key")
    )
    k_mismatch = bool(recommended_k and cluster_count) and int(recommended_k) != int(cluster_count)
    stability_gap = initialization_ari - stability_ari if initialization_ari else 0.0
    requires_caution = configuration_mismatch or k_mismatch or stability_gap >= 0.18

    if (
        not has_microclusters
        and silhouette >= 0.40
        and davies_bouldin <= 1.00
        and stability_ari >= 0.70
        and balance_ratio >= 0.18
        and not requires_caution
    ):
        return {
            "label": "Сильная",
            "note": "Сегментация выглядит сильной: метрики согласованы между собой, кластеры заметно отделяются и в целом воспроизводятся на повторных подвыборках.",
        }
    if not has_microclusters and silhouette >= 0.25 and davies_bouldin <= 1.30 and stability_ari >= 0.45 and balance_ratio >= 0.10:
        caution_suffix = ""
        if algorithm_mismatch:
            caution_suffix = " При этом итог лучше трактовать осторожнее: для текущего среза уже виден более убедительный альтернативный метод."
        elif configuration_mismatch:
            caution_suffix = " При этом итог лучше трактовать осторожнее: на том же наборе признаков более убедительно выглядит другая конфигурация весов или параметров."
        elif k_mismatch:
            caution_suffix = " При этом итог лучше трактовать осторожнее: рабочее число кластеров не совпадает с рекомендацией по совокупности метрик."
        elif stability_gap >= 0.18:
            caution_suffix = " При этом итог лучше трактовать осторожнее: устойчивость на одном и том же датасете заметно выше, чем на повторных подвыборках."
        return {
            "label": "Умеренная",
            "note": (
                "Сегментация выглядит умеренной: типология уже читается, но часть границ между кластерами остаётся чувствительной к составу данных или к балансу размеров групп."
                f"{caution_suffix}"
            ),
        }
    return {
        "label": "Слабая",
        "note": "Сегментация выглядит слабой: либо метрики между собой не согласованы, либо разбиение слишком чувствительно к составу выборки, либо его качество проседает из-за микрокластеров и дисбаланса.",
    }


def _build_stability_note(clustering: Dict[str, Any], resample_share_label: str) -> str:
    stability_ari = clustering.get("stability_ari")
    initialization_ari = clustering.get("initialization_ari")
    if stability_ari is None:
        return "Оценить устойчивость на повторных подвыборках не удалось: в текущем срезе слишком мало территорий для надёжного сравнения пересэмплов."
    if initialization_ari is None:
        return f"Устойчивость ARI = {_format_number(stability_ari, 3)} считается на повторных {resample_share_label}-подвыборках, а не только на смене random_state."

    gap = float(initialization_ari) - float(stability_ari)
    if gap >= 0.15:
        return (
            f"На одном и том же датасете KMeans почти не чувствителен к random_state (ARI {_format_number(initialization_ari, 3)}), "
            f"но на повторных {resample_share_label}-подвыборках устойчивость заметно ниже (ARI {_format_number(stability_ari, 3)}), поэтому уверенность в сегментации не стоит завышать."
        )
    return (
        f"Устойчивость на повторных {resample_share_label}-подвыборках составляет ARI {_format_number(stability_ari, 3)}; "
        f"разница с проверкой только по random_state умеренная (ARI {_format_number(initialization_ari, 3)})."
    )


def _build_method_recommendation_note(
    selected_method: Dict[str, Any] | None,
    recommended_method: Dict[str, Any] | None,
) -> str:
    selected_label = str((selected_method or {}).get("method_label") or "KMeans")
    recommended_label = str((recommended_method or {}).get("method_label") or selected_label)
    if not selected_method:
        return f"Для текущего среза рабочим методом остаётся {recommended_label}."
    if (recommended_method or {}).get("method_key") != (selected_method or {}).get("method_key"):
        if _resolve_method_algorithm_key(recommended_method) == _resolve_method_algorithm_key(selected_method):
            return (
                f"На странице сейчас показан вывод {selected_label}, но на том же алгоритме более убедительно выглядит "
                f"конфигурация {recommended_label}: так эффект стратегии весов не смешивается с эффектом самого метода."
            )
        return (
            f"Текущий вывод на странице построен методом {selected_label}, но по совокупности метрик и размеров кластеров для этого среза лучше выглядит {recommended_label}."
        )
    return f"{selected_label} остаётся предпочтительным методом: альтернативы не дают более сильного качества без ухудшения размеров кластеров."


def _build_method_comparison_scope_note(method_comparison: Sequence[Dict[str, Any]]) -> str:
    selected_method = next((row for row in method_comparison if row.get("is_selected")), None)
    if not selected_method:
        return ""
    selected_algorithm = _resolve_method_algorithm_key(selected_method)
    selected_key = str((selected_method or {}).get("method_key") or "")
    same_algorithm_alternatives = [
        row
        for row in method_comparison
        if row is not selected_method
        and _resolve_method_algorithm_key(row) == selected_algorithm
        and str(row.get("method_key") or "") != selected_key
    ]
    if not same_algorithm_alternatives:
        return ""
    return (
        "Для честного сравнения влияние весов вынесено отдельно: рядом с рабочей конфигурацией KMeans показан KMeans "
        "с другой стратегией весов, поэтому рекомендация по методу не смешивает эффект алгоритма и эффект весов."
    )


def _resolve_method_algorithm_key(method_row: Dict[str, Any] | None) -> str:
    if not method_row:
        return ""
    return str(method_row.get("algorithm_key") or method_row.get("method_key") or "")


def _build_cluster_shape_note(clustering: Dict[str, Any]) -> str:
    smallest_cluster_size = int(clustering.get("smallest_cluster_size") or 0)
    largest_cluster_size = int(clustering.get("largest_cluster_size") or 0)
    balance_ratio = float(clustering.get("cluster_balance_ratio") or 0.0)
    microcluster_threshold = int(clustering.get("microcluster_threshold") or 0)
    if clustering.get("has_microclusters"):
        return (
            f"Есть микрокластеры: самый маленький кластер содержит {_format_integer(smallest_cluster_size)} территорий при пороге предупреждения {_format_integer(microcluster_threshold)}, "
            "поэтому часть сегментации может держаться на очень малой группе наблюдений."
        )
    if balance_ratio < 0.12:
        return (
            f"Кластеры заметно несбалансированы: min/max = {_format_integer(smallest_cluster_size)} / {_format_integer(largest_cluster_size)} "
            f"({ _format_percent(balance_ratio) }), поэтому результат стоит трактовать осторожнее."
        )
    if balance_ratio < 0.18:
        return (
            f"Кластеры умеренно несбалансированы: min/max = {_format_integer(smallest_cluster_size)} / {_format_integer(largest_cluster_size)} "
            f"({ _format_percent(balance_ratio) })."
        )
    return ""
