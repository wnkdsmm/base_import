from __future__ import annotations

from typing import Sequence

from config.constants import LOW_SUPPORT_TERRITORY_THRESHOLD
from .quality_assessment import (
    compute_method_algorithm_key,
    compute_segmentation_strength,
)
from .types import ClusterLabel, ClusterMethod, ClusterMetrics
from .utils import _format_integer, _format_number, _format_percent


def _summarize_segmentation_strength(
    clustering: ClusterMetrics,
    selected_method: ClusterMethod | None = None,
    recommended_method: ClusterMethod | None = None,
    cluster_count: int | None = None,
    recommended_k: int | None = None,
) -> ClusterLabel:
    return compute_segmentation_strength(
        clustering,
        selected_method=selected_method,
        recommended_method=recommended_method,
        cluster_count=cluster_count,
        recommended_k=recommended_k,
    )


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
    return compute_method_algorithm_key(method_row)


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
    'compute_method_algorithm_key',
    'compute_segmentation_strength',
]
