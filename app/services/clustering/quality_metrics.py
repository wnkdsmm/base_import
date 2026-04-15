from __future__ import annotations

from typing import Any, List, Sequence

from .constants import LOW_SUPPORT_TERRITORY_THRESHOLD, STABILITY_RESAMPLE_RATIO
from .count_guidance import _build_cluster_count_guidance
from .types import (
    ClusterCountGuidance,
    ClusterLabel,
    ClusterMethod,
    ClusterMetrics,
    ClusteringQualityAssessment,
    FeatureAblationRow,
    FeatureSelectionReport,
    MethodComparisonRow,
    QualityConfigurationContext,
    QualityDiagnostics,
    QualityLabelContext,
    QualityNoteContext,
    QualityScore,
    SupportSummary,
)
from .utils import _format_integer, _format_number, _format_percent

__all__ = [
    "_build_clustering_quality_assessment",
    "_empty_clustering_quality_assessment",
]


def _format_configuration_label(configuration: ClusterMethod | None) -> str:
    if not configuration:
        return "—"
    method_label = str(configuration.get("method_label") or "Метод")
    cluster_count = configuration.get("cluster_count")
    if cluster_count:
        return f"{method_label}, k={_format_integer(cluster_count)}"
    return method_label


def _empty_clustering_quality_assessment() -> ClusteringQualityAssessment:
    return {
        "ready": False,
        "title": "Оценка качества кластеризации",
        "subtitle": "После расчета здесь появится понятная сводка: насколько группы различаются, насколько результат устойчив и какая настройка выглядит лучшей.",
        "metric_cards": [],
        "methodology_items": [],
        "comparison_rows": [],
        "dissertation_points": ["Пока недостаточно данных для расчета метрик качества кластеризации."],
    }


def _build_configuration_recommendation_note(
    working_configuration: ClusterMethod | None,
    recommended_configuration: ClusterMethod | None,
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
    return (
        f"Сейчас страница построена по конфигурации {working_label}, "
        f"но по всему доступному диапазону лучше выглядит {recommended_label}."
    )


def _resolve_quality_configuration_context(
    *,
    method_comparison: Sequence[ClusterMethod],
    diagnostics: QualityDiagnostics | None,
    cluster_count: int,
) -> QualityConfigurationContext:
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
        "recommended_k": recommended_k,
        "best_silhouette_k": diagnostics.get("best_silhouette_k"),
        "selected_method": selected_method,
        "working_configuration": working_configuration,
        "effective_recommended_configuration": effective_recommended_configuration,
        "recommended_method": effective_recommended_configuration or recommended_row or selected_method,
        "working_config_label": _format_configuration_label(working_configuration),
        "recommended_config_label": _format_configuration_label(effective_recommended_configuration),
    }


def _build_feature_selection_quality_label_context(
    feature_selection_report: FeatureSelectionReport | None,
) -> QualityLabelContext:
    report = feature_selection_report or {}
    return {
        "mode_label": str(report.get("volume_role_label") or "Профиль территории"),
        "mode_note": str(report.get("volume_note") or ""),
        "weighting_label": str(report.get("weighting_label") or "Равный вес территорий"),
        "weighting_note": str(report.get("weighting_note") or ""),
        "weighting_meta": str(report.get("weighting_meta") or ""),
        "ablation_rows": list(report.get("ablation_rows") or []),
    }


def _build_ablation_warning_note(ablation_rows: Sequence[FeatureAblationRow]) -> str:
    negative_adds = [
        row for row in ablation_rows if row.get("direction") == "add" and float(row.get("delta_score") or 0.0) < 0.0
    ]
    if not negative_adds:
        return ""

    worst_feature = min(negative_adds, key=lambda item: float(item.get("delta_score") or 0.0))
    return (
        f"В пробном сравнении признаков колонка '{worst_feature['feature']}' не вошла в итоговый набор, "
        "потому что с ней кластеры разделялись хуже."
    )


def _format_quality_method_selection_label(row: ClusterMethod) -> str:
    if row.get("is_selected") and row.get("is_recommended"):
        return "Рабочий и лучший на текущем k"
    if row.get("is_selected"):
        return "Рабочий вывод"
    if row.get("is_recommended"):
        return "Лучше на текущем k"
    return "Сравнение"


def _build_quality_method_comparison_rows(
    method_comparison: Sequence[ClusterMethod],
) -> List[MethodComparisonRow]:
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
    cluster_count_guidance: ClusterCountGuidance,
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
            f"Если смотреть на весь доступный диапазон настроек, лучшей выглядит конфигурация {recommended_config_label}."
            if recommended_config_label != working_config_label
            else f"Текущая конфигурация {working_config_label} уже совпадает с лучшей найденной."
        ),
        (
            f"По чёткости границ лучший результат отдельно даёт k={_format_integer(best_silhouette_k)}, "
            "но итоговое число групп всё равно выбирается вместе с проверкой баланса размеров."
            if recommended_k and best_silhouette_k and recommended_k != best_silhouette_k
            else "Основные показатели качества не спорят между собой по выбору числа групп."
        ),
        stability_note,
        (
            f"У {low_support_display} территорий пожаров немного, поэтому их долевые показатели слегка "
            "подтянуты к общему уровню, чтобы единичные случаи не искажали разбиение."
        ),
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
    clustering: ClusterMetrics,
    selected_method: ClusterMethod | None,
    working_configuration: ClusterMethod,
    effective_recommended_configuration: ClusterMethod,
    recommended_method: ClusterMethod,
    cluster_count: int,
    recommended_k: int | None,
    method_comparison: Sequence[ClusterMethod],
    feature_selection_report: FeatureSelectionReport | None,
    resample_share_label: str,
    cluster_count_is_explicit: bool,
) -> QualityNoteContext:
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


def _build_quality_metric_cards(clustering: ClusterMetrics, resample_share_label: str) -> List[QualityScore]:
    return [
        {
            "label": "Насколько кластеры отделены",
            "value": _format_number(clustering.get("silhouette"), 3),
            "meta": "Чем выше значение, тем заметнее границы между группами",
        },
        {
            "label": "Насколько кластеры смешиваются",
            "value": _format_number(clustering.get("davies_bouldin"), 3),
            "meta": "Чем ниже значение, тем меньше соседние группы заходят друг в друга",
        },
        {
            "label": "Насколько группы собраны плотно",
            "value": _format_number(clustering.get("calinski_harabasz"), 1),
            "meta": "Чем выше значение, тем собраннее территории внутри своих групп",
        },
        {
            "label": "Насколько группы равномерны",
            "value": _format_percent(clustering.get("cluster_balance_ratio") or 0.0),
            "meta": (
                f"Размер самой маленькой и самой большой группы: "
                f"{_format_integer(clustering.get('smallest_cluster_size'))} / "
                f"{_format_integer(clustering.get('largest_cluster_size'))}"
            ),
        },
        {
            "label": "Насколько результат повторяется",
            "value": _format_number(clustering.get("stability_ari"), 3),
            "meta": f"Проверено на повторных {resample_share_label}-подвыборках",
        },
    ]


def _build_quality_methodology_items(
    *,
    selected_features: Sequence[str],
    selected_method: ClusterMethod | None,
    working_config_label: str,
    recommended_config_label: str,
    segmentation_label: str,
    mode_label: str,
    weighting_label: str,
    weighting_meta: str,
    low_support_display: str,
    explained_variance: Any,
) -> List[QualityScore]:
    return [
        {
            "label": "Текущая настройка",
            "value": working_config_label,
            "meta": "Именно по этой настройке построены кластеры на странице",
        },
        {
            "label": "Лучшая найденная настройка",
            "value": recommended_config_label,
            "meta": "Лучшая комбинация режима, весов, метода и числа кластеров в доступном диапазоне",
        },
        {
            "label": "Метод сейчас",
            "value": str((selected_method or {}).get("method_label") or "KMeans"),
            "meta": "Лучший среди сопоставимых вариантов при текущем числе групп",
        },
        {
            "label": "Насколько кластеры различимы",
            "value": segmentation_label,
            "meta": "Итоговая оценка по разделению групп, устойчивости и их размерам",
        },
        {
            "label": "Что именно кластеризуем",
            "value": mode_label,
            "meta": "Какой профиль территории сравнивается по умолчанию",
        },
        {
            "label": "Как учитывается нагрузка",
            "value": weighting_label,
            "meta": weighting_meta or "Показывает, влияет ли число пожаров на положение территории в кластере",
        },
        {
            "label": "Сколько признаков вошло в расчёт",
            "value": _format_integer(len(selected_features)),
            "meta": "Отобраны потому, что на текущем срезе лучше разделяют территории",
        },
        {
            "label": "Территории с короткой историей",
            "value": low_support_display,
            "meta": f"Для территорий с ≤{LOW_SUPPORT_TERRITORY_THRESHOLD} пожарами значения сглажены, чтобы убрать шум",
        },
        {
            "label": "Насколько 2D-карта отражает картину",
            "value": _format_percent(explained_variance or 0.0),
            "meta": "Сколько общей картины сохраняется, когда данные сводим к плоской карте",
        },
    ]


def _build_clustering_quality_assessment(
    clustering: ClusterMetrics,
    method_comparison: Sequence[ClusterMethod],
    cluster_count: int,
    selected_features: Sequence[str],
    diagnostics: QualityDiagnostics | None = None,
    support_summary: SupportSummary | None = None,
    feature_selection_report: FeatureSelectionReport | None = None,
    requested_cluster_count: int | None = None,
    resolved_requested_cluster_count: int | None = None,
    cluster_count_is_explicit: bool = False,
    cluster_count_guidance: ClusterCountGuidance | None = None,
) -> ClusteringQualityAssessment:
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
        "subtitle": "Ниже показано, насколько группы действительно различаются, насколько результат устойчив при повторном расчете и какая настройка кластеризации выглядит лучшей.",
        "metric_cards": _build_quality_metric_cards(clustering, resample_share_label),
        "methodology_items": _build_quality_methodology_items(
            selected_features=selected_features,
            selected_method=selected_method,
            working_config_label=working_config_label,
            recommended_config_label=recommended_config_label,
            segmentation_label=segmentation_summary["label"],
            mode_label=mode_label,
            weighting_label=weighting_label,
            weighting_meta=weighting_meta,
            low_support_display=low_support_display,
            explained_variance=clustering.get("explained_variance"),
        ),
        "comparison_rows": comparison_rows,
        "dissertation_points": dissertation_points,
    }


def _summarize_segmentation_strength(
    clustering: ClusterMetrics,
    selected_method: ClusterMethod | None = None,
    recommended_method: ClusterMethod | None = None,
    cluster_count: int | None = None,
    recommended_k: int | None = None,
) -> ClusterLabel:
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


def _build_stability_note(clustering: ClusterMetrics, resample_share_label: str) -> str:
    stability_ari = clustering.get("stability_ari")
    initialization_ari = clustering.get("initialization_ari")
    if stability_ari is None:
        return "Оценить устойчивость на повторных подвыборках не удалось: в текущем срезе слишком мало территорий для надёжного сравнения пересэмплов."
    if initialization_ari is None:
        return (
            f"Проверка на повторных {resample_share_label}-подвыборках дала "
            f"{_format_number(stability_ari, 3)}: так видно, насколько результат повторяется не только на тех же данных."
        )

    gap = float(initialization_ari) - float(stability_ari)
    if gap >= 0.15:
        return (
            f"На одних и тех же данных разбиение почти не меняется ({_format_number(initialization_ari, 3)}), "
            f"но на повторных {resample_share_label}-подвыборках устойчивость заметно ниже "
            f"({_format_number(stability_ari, 3)}), поэтому результат чувствителен к составу выборки."
        )
    return (
        f"На повторных {resample_share_label}-подвыборках устойчивость составляет "
        f"{_format_number(stability_ari, 3)}; это близко к проверке на тех же данных "
        f"({_format_number(initialization_ari, 3)})."
    )


def _build_method_recommendation_note(
    selected_method: ClusterMethod | None,
    recommended_method: ClusterMethod | None,
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


def _build_method_comparison_scope_note(method_comparison: Sequence[ClusterMethod]) -> str:
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


def _resolve_method_algorithm_key(method_row: ClusterMethod | None) -> str:
    if not method_row:
        return ""
    return str(method_row.get("algorithm_key") or method_row.get("method_key") or "")


def _build_cluster_shape_note(clustering: ClusterMetrics) -> str:
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
