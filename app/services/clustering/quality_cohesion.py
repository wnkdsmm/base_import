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

__all__ = [
    '_summarize_segmentation_strength',
    '_build_stability_note',
    '_build_method_recommendation_note',
    '_build_method_comparison_scope_note',
    '_resolve_method_algorithm_key',
    '_build_cluster_shape_note',
]
