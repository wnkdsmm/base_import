from __future__ import annotations

import math
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA

from app.services.model_quality import compute_clustering_metrics

from .analysis_stats import (
    _build_sample_weights,
    _cluster_quality_score,
    _cluster_shape_diagnostics,
    _compute_cluster_inertia,
    _compute_gap_statistic,
    _compute_pca_projection,
    _derive_cluster_centers,
    _estimate_best_k_gap,
    _estimate_elbow_k,
    _estimate_kmeans_initialization_stability,
    _estimate_resampled_stability,
    _fit_clustering_labels,
    _fit_weighted_kmeans,
    _prepare_model_inputs,
    _restore_raw_centers,
)
from .constants import (
    CARD_TONES,
    CLUSTER_COUNT_OPTIONS,
    FEATURE_METADATA,
    FEATURE_SELECTION_MIN_IMPROVEMENT,
    LOW_SUPPORT_TERRITORY_THRESHOLD,
    MAX_K_DIAGNOSTICS,
    MODEL_N_INIT,
    WEIGHTING_STRATEGY_INCIDENT_LOG,
    WEIGHTING_STRATEGY_INCIDENT_LOG_LABEL,
    WEIGHTING_STRATEGY_NOT_APPLICABLE,
    WEIGHTING_STRATEGY_UNIFORM,
    WEIGHTING_STRATEGY_UNIFORM_LABEL,
)
from .types import (
    ClusteringDiagnosticsResult,
    ClusteringMethodCandidate,
    ClusteringMethodRow,
    ClusteringRunResult,
    FeatureSelectionReport,
)
from .utils import _format_integer, _format_number, _format_percent


def _primary_method_key(weighting_strategy: str) -> str:
    return f"kmeans_{weighting_strategy}"


def _build_method_candidates(weighting_strategy: str) -> List[ClusteringMethodCandidate]:
    candidates = [
        {
            "method_key": _primary_method_key(weighting_strategy),
            "algorithm_key": "kmeans",
            "method_label": _build_kmeans_method_label(weighting_strategy, selected_weighting_strategy=weighting_strategy),
            "weighting_strategy": weighting_strategy,
        }
    ]
    if weighting_strategy != WEIGHTING_STRATEGY_UNIFORM:
        candidates.append(
            {
                "method_key": _primary_method_key(WEIGHTING_STRATEGY_UNIFORM),
                "algorithm_key": "kmeans",
                "method_label": _build_kmeans_method_label(
                    WEIGHTING_STRATEGY_UNIFORM,
                    selected_weighting_strategy=weighting_strategy,
                ),
                "weighting_strategy": WEIGHTING_STRATEGY_UNIFORM,
            }
        )
    candidates.extend(
        [
            {
                "method_key": "agglomerative",
                "algorithm_key": "agglomerative",
                "method_label": "Агломеративная кластеризация (Ward)",
                "weighting_strategy": WEIGHTING_STRATEGY_NOT_APPLICABLE,
            },
            {
                "method_key": "birch",
                "algorithm_key": "birch",
                "method_label": "Birch",
                "weighting_strategy": WEIGHTING_STRATEGY_NOT_APPLICABLE,
            },
        ]
    )
    return candidates


def _run_clustering(
    cluster_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    cluster_count: int,
    weighting_strategy: str = WEIGHTING_STRATEGY_INCIDENT_LOG,
    algorithm_key: str = "kmeans",
    method_key: str | None = None,
    prepared_model_inputs: tuple[pd.DataFrame, np.ndarray, Any, set[str], np.ndarray] | None = None,
) -> ClusteringRunResult:
    if prepared_model_inputs is None:
        _, scaled_points, scaler, transformed_columns, sample_weights = _prepare_model_inputs(
            cluster_frame,
            entity_frame,
            weighting_strategy=weighting_strategy,
        )
    else:
        _, scaled_points, scaler, transformed_columns, sample_weights = prepared_model_inputs
    if algorithm_key == "kmeans":
        model = _fit_weighted_kmeans(scaled_points, sample_weights, cluster_count, random_state=42, n_init=MODEL_N_INIT)
        labels = model.labels_
        scaled_centers = model.cluster_centers_
        transformed_centers = scaler.inverse_transform(model.cluster_centers_)
        raw_centers = _restore_raw_centers(transformed_centers, cluster_frame.columns, transformed_columns)
        inertia = float(model.inertia_)
        initialization_ari = _estimate_kmeans_initialization_stability(scaled_points, cluster_count, sample_weights)
    else:
        labels = _fit_clustering_labels(
            scaled_points,
            cluster_count,
            algorithm_key=algorithm_key,
            sample_weights=sample_weights,
            random_state=42,
            n_init=MODEL_N_INIT,
        )
        raw_centers, scaled_centers = _derive_cluster_centers(cluster_frame, scaled_points, labels, cluster_count)
        inertia = _compute_cluster_inertia(scaled_points, labels, scaled_centers=scaled_centers)
        initialization_ari = None
    cluster_labels = _cluster_labels(cluster_count)
    pca_projection_result = _compute_pca_projection(
        scaled_points=scaled_points,
        labels=labels,
        cluster_labels=cluster_labels,
    )
    metrics = compute_clustering_metrics(scaled_points, labels)
    shape_diagnostics = _cluster_shape_diagnostics(metrics, len(cluster_frame))
    stability_ari = _estimate_resampled_stability(
        scaled_points,
        cluster_count,
        sample_weights,
        algorithm_key=algorithm_key,
    )

    pca = PCA(n_components=2)
    pca_points = pca.fit_transform(scaled_points)

    return {
        "labels": labels,
        "scaled_points": scaled_points,
        "scaled_centers": scaled_centers,
        "raw_centers": raw_centers,
        "silhouette": metrics.get("silhouette"),
        "davies_bouldin": metrics.get("davies_bouldin"),
        "calinski_harabasz": metrics.get("calinski_harabasz"),
        "cluster_balance_ratio": metrics.get("cluster_balance_ratio"),
        "smallest_cluster_size": metrics.get("smallest_cluster_size"),
        "largest_cluster_size": metrics.get("largest_cluster_size"),
        "quality_score": _cluster_quality_score(metrics, len(cluster_frame)),
        "shape_penalty": shape_diagnostics["shape_penalty"],
        "has_microclusters": shape_diagnostics["has_microclusters"],
        "has_balance_warning": shape_diagnostics["has_balance_warning"],
        "microcluster_threshold": shape_diagnostics["microcluster_threshold"],
        "stability_ari": stability_ari,
        "initialization_ari": initialization_ari,
        "inertia": inertia,
        "pca_points": pca_points,
        "pca_projection": pca_projection_result,
        "explained_variance": float(sum(pca_projection_result["explained_variance"])),
        "algorithm_key": algorithm_key,
        "method_key": method_key or (_primary_method_key(weighting_strategy) if algorithm_key == "kmeans" else algorithm_key),
        "weighting_strategy": weighting_strategy,
    }


def _evaluate_cluster_counts(
    cluster_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    weighting_strategy: str = WEIGHTING_STRATEGY_INCIDENT_LOG,
) -> ClusteringDiagnosticsResult:
    if len(cluster_frame) < 3:
        return {"rows": [], "best_silhouette_k": None, "best_quality_k": None, "elbow_k": None}
    available_ks = [
        cluster_count
        for cluster_count in CLUSTER_COUNT_OPTIONS
        if 2 <= cluster_count <= min(MAX_K_DIAGNOSTICS, len(cluster_frame) - 1)
    ]
    prepared_inputs = _prepare_model_inputs(
        cluster_frame,
        entity_frame,
        weighting_strategy=weighting_strategy,
    )
    _, scaled_points, _, _, sample_weights = prepared_inputs
    gap_scores = _compute_gap_statistic(
        scaled_points,
        sample_weights,
        k_range=available_ks,
        n_references=10,
        random_state=42,
    )

    rows: List[ClusteringMethodRow] = []
    method_rows_by_cluster_count: Dict[int, List[ClusteringMethodRow]] = {}
    for cluster_count in available_ks:
        method_rows = _compare_clustering_methods(
            cluster_frame,
            entity_frame,
            cluster_count,
            weighting_strategy=weighting_strategy,
            prepared_model_inputs=prepared_inputs,
        )
        method_rows_by_cluster_count[cluster_count] = method_rows
        best_row = next((item for item in method_rows if item.get("is_recommended")), None)
        if best_row is None:
            best_row = next((item for item in method_rows if item.get("is_selected")), None)
        if best_row is None:
            continue
        rows.append({**best_row, "cluster_count": cluster_count})

    best_silhouette_row = None
    scored_rows = [item for item in rows if item["silhouette"] is not None]
    if scored_rows:
        best_silhouette_row = max(scored_rows, key=lambda item: (item["silhouette"], -item["cluster_count"]))
    best_quality_row = max(rows, key=lambda item: _diagnostics_row_sort_key(item), default=None)

    return {
        "rows": rows,
        "method_rows_by_cluster_count": method_rows_by_cluster_count,
        "best_silhouette_k": best_silhouette_row["cluster_count"] if best_silhouette_row else None,
        "best_quality_k": best_quality_row["cluster_count"] if best_quality_row else None,
        "best_gap_k": _estimate_best_k_gap(gap_scores),
        "best_configuration": dict(best_quality_row) if best_quality_row else None,
        "elbow_k": _estimate_elbow_k(rows),
    }


def _diagnostics_row_sort_key(result: ClusteringMethodRow) -> tuple[float, float, float, float, float]:
    davies_bouldin = result.get("davies_bouldin", float("inf"))
    davies_value = float("inf") if davies_bouldin is None else float(davies_bouldin)
    return (
        float(result.get("quality_score", float("-inf"))),
        float(result.get("silhouette", float("-inf"))),
        -float(davies_value if math.isfinite(davies_value) else 1e9),
        float(result.get("cluster_balance_ratio", 0.0)),
        -float(result.get("shape_penalty", 0.0)),
    )


def _compare_clustering_methods(
    cluster_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    cluster_count: int,
    weighting_strategy: str = WEIGHTING_STRATEGY_INCIDENT_LOG,
    selected_method_key: str | None = None,
    prepared_model_inputs: tuple[pd.DataFrame, np.ndarray, Any, set[str], np.ndarray] | None = None,
) -> List[ClusteringMethodRow]:
    if prepared_model_inputs is None:
        _, scaled_points, _, _, _ = _prepare_model_inputs(
            cluster_frame,
            entity_frame,
            weighting_strategy=weighting_strategy,
        )
    else:
        _, scaled_points, _, _, _ = prepared_model_inputs
    rows: List[ClusteringMethodRow] = []
    row_count = len(cluster_frame)
    primary_method_key = selected_method_key or _primary_method_key(weighting_strategy)

    def _append_method_row(candidate: ClusteringMethodCandidate) -> None:
        try:
            row_weighting_strategy = str(candidate.get("weighting_strategy") or WEIGHTING_STRATEGY_NOT_APPLICABLE)
            sample_weights = _build_sample_weights(entity_frame, weighting_strategy=row_weighting_strategy)
            labels = _fit_clustering_labels(
                scaled_points,
                cluster_count,
                algorithm_key=str(candidate["algorithm_key"]),
                sample_weights=sample_weights,
                random_state=42,
                n_init=MODEL_N_INIT,
            )
        except Exception:
            return
        metrics = compute_clustering_metrics(scaled_points, labels)
        quality_score = _cluster_quality_score(metrics, row_count)
        shape_diagnostics = _cluster_shape_diagnostics(metrics, row_count)
        inertia = _compute_cluster_inertia(scaled_points, labels)
        rows.append(
            {
                "method_key": candidate["method_key"],
                "algorithm_key": candidate["algorithm_key"],
                "method_label": candidate["method_label"],
                "is_selected": candidate["method_key"] == primary_method_key,
                "weighting_strategy": row_weighting_strategy,
                "inertia": inertia,
                "silhouette": metrics.get("silhouette"),
                "davies_bouldin": metrics.get("davies_bouldin"),
                "calinski_harabasz": metrics.get("calinski_harabasz"),
                "cluster_balance_ratio": metrics.get("cluster_balance_ratio"),
                "smallest_cluster_size": metrics.get("smallest_cluster_size"),
                "largest_cluster_size": metrics.get("largest_cluster_size"),
                "quality_score": quality_score,
                "shape_penalty": shape_diagnostics["shape_penalty"],
                "has_microclusters": shape_diagnostics["has_microclusters"],
                "has_balance_warning": shape_diagnostics["has_balance_warning"],
            }
        )

    for candidate in _build_method_candidates(weighting_strategy):
        _append_method_row(candidate)
    recommended_method = _select_recommended_method_row(rows)
    recommended_key = recommended_method["method_key"] if recommended_method else None
    for row in rows:
        row["is_recommended"] = row.get("method_key") == recommended_key
    return rows


def _build_kmeans_method_label(weighting_strategy: str, selected_weighting_strategy: str) -> str:
    if weighting_strategy == WEIGHTING_STRATEGY_UNIFORM:
        if selected_weighting_strategy == WEIGHTING_STRATEGY_UNIFORM:
            return "KMeans"
        return f"KMeans ({WEIGHTING_STRATEGY_UNIFORM_LABEL.lower()})"
    return f"KMeans ({WEIGHTING_STRATEGY_INCIDENT_LOG_LABEL.lower()})"


def _select_recommended_method_row(method_rows: Sequence[ClusteringMethodRow]) -> ClusteringMethodRow | None:
    if not method_rows:
        return None
    current_row = next((row for row in method_rows if row.get("is_selected")), None)
    if current_row is None:
        current_row = next(
            (
                row
                for row in method_rows
                if str(row.get("algorithm_key") or row.get("method_key") or "").startswith("kmeans")
            ),
            method_rows[0],
        )
    best_row = max(method_rows, key=_diagnostics_row_sort_key)
    if best_row.get("method_key") == current_row.get("method_key"):
        return current_row

    quality_gap = float(best_row.get("quality_score") or 0.0) - float(current_row.get("quality_score") or 0.0)
    balance_gap = float(best_row.get("cluster_balance_ratio") or 0.0) - float(current_row.get("cluster_balance_ratio") or 0.0)
    smallest_gap = int(best_row.get("smallest_cluster_size") or 0) - int(current_row.get("smallest_cluster_size") or 0)
    if (
        quality_gap >= 0.01
        and not bool(best_row.get("has_microclusters"))
        and float(best_row.get("shape_penalty") or 0.0) <= float(current_row.get("shape_penalty") or 0.0) + 0.01
        and balance_gap >= -0.05
        and smallest_gap >= -2
    ):
        return best_row
    return current_row


def _build_cluster_profiles(
    cluster_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    labels: np.ndarray,
    raw_centers: np.ndarray,
    cluster_labels: Sequence[str],
) -> List[Dict[str, str]]:
    profiles = []
    total_entities = len(cluster_frame)
    total_incidents = int(entity_frame["Число пожаров"].sum()) if "Число пожаров" in entity_frame.columns else 0
    columns = list(cluster_frame.columns)
    overall_mean = cluster_frame.mean(numeric_only=True)
    overall_std = cluster_frame.std(numeric_only=True).replace(0, 1.0).fillna(1.0)

    for cluster_id, cluster_label in enumerate(cluster_labels):
        mask = labels == cluster_id
        size = int(np.sum(mask))
        if size <= 0:
            continue

        center = pd.Series(raw_centers[cluster_id], index=columns)
        deltas = ((center - overall_mean) / overall_std).replace([np.inf, -np.inf], 0.0).fillna(0.0)
        dominant_columns = list(deltas.abs().sort_values(ascending=False).head(3).index)
        title_parts = [_feature_phrase(column, float(deltas[column])) for column in dominant_columns[:2]]
        title_parts = [part for part in title_parts if part]
        segment_title = " и ".join(title_parts).capitalize() if title_parts else "Смешанный профиль риска"
        summary_parts = []
        for column in dominant_columns:
            direction = "выше" if float(deltas[column]) >= 0 else "ниже"
            summary_parts.append(f"{column}: {direction} среднего ({_format_feature_value(column, center[column])})")

        cluster_incidents = int(entity_frame.loc[mask, "Число пожаров"].sum()) if "Число пожаров" in entity_frame.columns else 0
        profiles.append(
            {
                "cluster_label": cluster_label,
                "segment_title": segment_title,
                "size_display": _format_integer(size),
                "share_display": _format_percent(size / total_entities if total_entities else 0.0),
                "incidents_display": _format_integer(cluster_incidents),
                "incident_share_display": _format_percent(cluster_incidents / total_incidents if total_incidents else 0.0),
                "dominant_feature": dominant_columns[0] if dominant_columns else "—",
                "dominant_value": _format_feature_value(dominant_columns[0], center[dominant_columns[0]]) if dominant_columns else "—",
                "summary": "; ".join(summary_parts[:3]) + ".",
                "tone": CARD_TONES[cluster_id % len(CARD_TONES)],
            }
        )
    return profiles


def _build_centroid_table(
    cluster_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    labels: np.ndarray,
    raw_centers: np.ndarray,
    cluster_labels: Sequence[str],
    cluster_profiles: Sequence[Dict[str, str]],
) -> Tuple[List[str], List[List[str]]]:
    columns = ["Кластер", "Профиль", "Территорий", "Пожаров в истории"] + list(cluster_frame.columns)
    rows: List[List[str]] = []
    titles = {item["cluster_label"]: item.get("segment_title", "") for item in cluster_profiles}
    for cluster_id, cluster_label in enumerate(cluster_labels):
        mask = labels == cluster_id
        size = int(np.sum(mask))
        cluster_incidents = int(entity_frame.loc[mask, "Число пожаров"].sum()) if "Число пожаров" in entity_frame.columns else 0
        row = [cluster_label, titles.get(cluster_label, "—"), _format_integer(size), _format_integer(cluster_incidents)]
        row.extend(_format_feature_value(feature_name, value) for feature_name, value in zip(cluster_frame.columns, raw_centers[cluster_id]))
        rows.append(row)
    return columns, rows


def _build_representative_rows(
    cluster_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    labels: np.ndarray,
    scaled_points: np.ndarray,
    scaled_centers: np.ndarray,
    cluster_labels: Sequence[str],
) -> Tuple[List[str], List[List[str]]]:
    columns = ["Кластер", "Территория", "Район", "Тип территории"] + list(cluster_frame.columns)
    rows: List[List[str]] = []
    distances = np.linalg.norm(scaled_points - scaled_centers[labels], axis=1)

    for cluster_id, cluster_label in enumerate(cluster_labels):
        cluster_indexes = np.where(labels == cluster_id)[0]
        nearest_indexes = cluster_indexes[np.argsort(distances[cluster_indexes])[: min(3, len(cluster_indexes))]]
        for row_index in nearest_indexes:
            entity_row = entity_frame.iloc[row_index]
            row_values = [
                cluster_label,
                str(entity_row.get("Территория", "—")),
                str(entity_row.get("Район", "—")),
                str(entity_row.get("Тип территории", "—")),
            ]
            for column in cluster_frame.columns:
                row_values.append(_format_feature_value(column, cluster_frame.iloc[row_index][column]))
            rows.append(row_values)
    return columns, rows


def _build_notes(
    cluster_profiles: Sequence[Dict[str, str]],
    silhouette: float | None,
    selected_features: Sequence[str],
    diagnostics: ClusteringDiagnosticsResult,
    total_incidents: int,
    total_entities: int,
    sampled_entities: int,
    support_summary: Dict[str, float] | None = None,
    stability_ari: float | None = None,
    feature_selection_report: FeatureSelectionReport | None = None,
) -> List[str]:
    notes = [
        f"Кластеризация построена по { _format_integer(sampled_entities) } территориям/населённым пунктам, агрегированным из { _format_integer(total_incidents) } пожаров.",
    ]
    if total_entities > sampled_entities:
        sampling_strategy = str((feature_selection_report or {}).get("sampling_strategy") or "").strip().lower()
        if sampling_strategy == "stratified":
            sampling_note = (
                f"До кластеризации были собраны агрегаты по { _format_integer(total_entities) } территориям, "
                "а затем отобрана подвыборка с пропорциональной стратификацией."
            )
        elif sampling_strategy == "random":
            sampling_note = (
                f"До кластеризации были собраны агрегаты по { _format_integer(total_entities) } территориям, "
                "а затем отобрана равномерная случайная подвыборка."
            )
        elif sampling_strategy == "systematic":
            sampling_note = (
                f"До кластеризации были собраны агрегаты по { _format_integer(total_entities) } территориям, "
                "а затем выполнен систематический отбор подвыборки."
            )
        else:
            sampling_note = (
                f"До кластеризации были собраны агрегаты по { _format_integer(total_entities) } территориям, "
                "а затем отобрана подвыборка."
            )
        notes.append(sampling_note)
    if support_summary:
        low_support_share = float(support_summary.get("low_support_share") or 0.0)
        if low_support_share > 0.2:
            algorithm_key = str((feature_selection_report or {}).get("selected_algorithm_key") or "kmeans")
            if bool((feature_selection_report or {}).get("uses_incident_weights")):
                notes.append(
                    f"{_format_percent(low_support_share)} территорий имеют не более {LOW_SUPPORT_TERRITORY_THRESHOLD} пожаров, поэтому для них редкие значения сглажены к общему уровню, а территории с более длинной историей влияют на результат немного сильнее."
                )
            elif algorithm_key == "kmeans":
                notes.append(
                    f"{_format_percent(low_support_share)} территорий имеют не более {LOW_SUPPORT_TERRITORY_THRESHOLD} пожаров, поэтому для них редкие значения сглажены к общему уровню, а все территории сохраняют одинаковый вес в расчёте."
                )
            else:
                notes.append(
                    f"{_format_percent(low_support_share)} территорий имеют не более {LOW_SUPPORT_TERRITORY_THRESHOLD} пожаров, поэтому для них редкие значения сглажены к общему уровню, а выбранный алгоритм не добавляет отдельные веса территориям."
                )

    if feature_selection_report:
        notes.append(str(feature_selection_report.get("volume_note") or ""))
        weighting_note = str(feature_selection_report.get("weighting_note") or "")
        if weighting_note:
            notes.append(weighting_note)
        negative_adds = [
            row
            for row in feature_selection_report.get("ablation_rows") or []
            if row.get("direction") == "add" and float(row.get("delta_score") or 0.0) < -FEATURE_SELECTION_MIN_IMPROVEMENT
        ]
        if negative_adds:
            worst_feature = min(negative_adds, key=lambda item: float(item.get("delta_score") or 0.0))
            notes.append(
                f"В пробном сравнении признак '{worst_feature['feature']}' не вошёл в итоговый набор, потому что с ним группы разделялись хуже."
            )

    if silhouette is None:
        notes.append("Показатель отделённости групп не рассчитан: для этого нужно больше территорий, чем кластеров, и хотя бы две непустые группы.")
    elif silhouette < 0.2:
        notes.append("Кластеры отделены слабо: профиль территорий плавный, поэтому результат стоит трактовать как предварительную типологию, а не как жёсткое разбиение.")
    elif silhouette < 0.4:
        notes.append("Разделение умеренное: типы территорий уже читаются, но между ними остаются заметные переходные зоны.")
    else:
        notes.append("Кластеры отделены достаточно чётко для исследовательского сравнения типов территорий риска.")

    if stability_ari is not None:
        if stability_ari < 0.45:
            notes.append(
                f"На повторных подвыборках устойчивость низкая ({_format_number(stability_ari, 3)}): сегментация заметно меняется от состава выборки, поэтому кластеры лучше проверять по представителям и центрам."
            )
        elif stability_ari < 0.7:
            notes.append(
                f"На повторных подвыборках устойчивость умеренная ({_format_number(stability_ari, 3)}): общая типология сохраняется, но границы между соседними кластерами ещё чувствительны к составу данных."
            )
        else:
            notes.append(
                f"На повторных подвыборках сегментация выглядит воспроизводимой ({_format_number(stability_ari, 3)}), хотя это всё равно не гарантирует идеальную устойчивость на новых периодах."
            )

    best_quality_k = diagnostics.get("best_quality_k")
    best_silhouette_k = diagnostics.get("best_silhouette_k")
    elbow_k = diagnostics.get("elbow_k")
    available_k_label = f"{CLUSTER_COUNT_OPTIONS[0]}..{CLUSTER_COUNT_OPTIONS[-1]}"
    if best_quality_k and best_silhouette_k and best_quality_k != best_silhouette_k:
        notes.append(
            f"В доступном диапазоне k={available_k_label} по совокупности показателей и размеров групп лучше выглядит k={best_quality_k}, хотя по чёткости границ отдельно лидирует k={best_silhouette_k}."
        )
    elif best_quality_k:
        notes.append(
            f"В доступном диапазоне k={available_k_label} по совокупности показателей и размеров групп наиболее убедительно выглядит k={best_quality_k}."
        )
    elif best_silhouette_k and elbow_k and best_silhouette_k != elbow_k:
        notes.append(
            f"В доступном диапазоне k={available_k_label} по чёткости границ лучший результат даёт k={best_silhouette_k}, а заметный перелом кривой начинается около k={elbow_k}."
        )
    elif best_silhouette_k:
        notes.append(f"В доступном диапазоне k={available_k_label} по чёткости границ лучший результат даёт k={best_silhouette_k}.")
    elif elbow_k:
        notes.append(f"В доступном диапазоне k={available_k_label} кривая внутригруппового разброса заметно меняется около k={elbow_k}.")

    notes.append(f"В расчёте участвовали признаки: {', '.join(selected_features)}.")
    if cluster_profiles:
        largest = max(cluster_profiles, key=lambda item: int(item["size_display"].replace(" ", "")))
        notes.append(
            f"Самый крупный тип территорий сейчас — {largest['cluster_label']} ({largest['share_display']} выборки): {largest['segment_title'].lower()}."
        )
    notes.append(
        "Кластеры полезны как типология территорий, но соседние группы могут пересекаться, поэтому итог лучше проверять по профилям, центрам и типичным территориям внутри каждого кластера."
    )
    return notes


def _cluster_labels(cluster_count: int) -> List[str]:
    return [f"Тип {index + 1}" for index in range(cluster_count)]


def _feature_phrase(column_name: str, delta_score: float) -> str:
    meta = FEATURE_METADATA.get(column_name, {})
    if delta_score >= 0:
        return meta.get("high_phrase", column_name)
    return meta.get("low_phrase", column_name)


def _format_feature_value(column_name: str, value: Any) -> str:
    if column_name.startswith("Доля") or column_name.startswith("Покрытие"):
        return _format_percent(float(value))
    return _format_number(value, 2)
