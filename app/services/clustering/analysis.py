from __future__ import annotations

import math
from itertools import combinations
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering, Birch, KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import adjusted_rand_score
from sklearn.preprocessing import StandardScaler

from app.services.model_quality import compute_clustering_metrics

from .constants import (
    AUTO_DEFAULT_EXCLUDED_FEATURES,
    CARD_TONES,
    CLUSTER_COUNT_OPTIONS,
    DEFAULT_CLUSTER_FEATURES,
    DEFAULT_CLUSTER_MODE_LOAD,
    DEFAULT_CLUSTER_MODE_LOAD_LABEL,
    DEFAULT_CLUSTER_MODE_PROFILE,
    DEFAULT_CLUSTER_MODE_PROFILE_LABEL,
    DEFAULT_FEATURE_TARGET_COUNT,
    FEATURE_METADATA,
    FEATURE_SELECTION_MIN_IMPROVEMENT,
    LOG_SCALE_FEATURES,
    LOW_SUPPORT_TERRITORY_THRESHOLD,
    MAX_K_DIAGNOSTICS,
    MIN_DEFAULT_FEATURE_COUNT,
    PROFILE_MODE_EXCLUDED_FEATURES,
    PROFILE_MODE_SCORE_TOLERANCE,
    PROFILE_MODE_SILHOUETTE_TOLERANCE,
    RATE_SMOOTHING_PRIOR_STRENGTH,
    STABILITY_RANDOM_SEEDS,
    STABILITY_RESAMPLE_RATIO,
    VOLUME_DOMINANCE_MIN_SCORE_DELTA,
    VOLUME_DOMINANCE_RATIO,
    WEIGHTING_STRATEGY_INCIDENT_LOG,
    WEIGHTING_STRATEGY_INCIDENT_LOG_LABEL,
    WEIGHTING_STRATEGY_NOT_APPLICABLE,
    WEIGHTING_STRATEGY_NOT_APPLICABLE_LABEL,
    WEIGHTING_STRATEGY_UNIFORM,
    WEIGHTING_STRATEGY_UNIFORM_LABEL,
)
from .utils import _format_integer, _format_number, _format_percent


def _weighting_strategy_for_mode(mode_key: str) -> str:
    if mode_key == DEFAULT_CLUSTER_MODE_PROFILE:
        return WEIGHTING_STRATEGY_UNIFORM
    return WEIGHTING_STRATEGY_INCIDENT_LOG


def _describe_weighting_strategy(mode_label: str, weighting_strategy: str) -> tuple[str, str, str]:
    if weighting_strategy == WEIGHTING_STRATEGY_NOT_APPLICABLE:
        return (
            WEIGHTING_STRATEGY_NOT_APPLICABLE_LABEL,
            (
                f"В рабочем выводе используется метод без sample weights: режим '{mode_label}' задаётся набором признаков, "
                "а центры/границы кластеров не смещаются дополнительным весом по числу пожаров."
            ),
            "Выбранный алгоритм не использует sample_weight; влияние нагрузки возможно только через явные признаки.",
        )
    if weighting_strategy == WEIGHTING_STRATEGY_UNIFORM:
        return (
            WEIGHTING_STRATEGY_UNIFORM_LABEL,
            f"В режиме '{mode_label}' KMeans обучается с равным весом территорий: число пожаров не смещает центры кластеров.",
            "Все территории одинаково влияют на центры KMeans.",
        )
    return (
        WEIGHTING_STRATEGY_INCIDENT_LOG_LABEL,
        (
            f"В режиме '{mode_label}' KMeans использует умеренные log-веса по числу пожаров, "
            "поэтому территории с большей историей немного сильнее влияют на центры кластеров."
        ),
        "Вес = log1p(число пожаров), затем нормировка к среднему весу 1.0.",
    )


def _build_clustering_mode_context(
    selected_features: Sequence[str],
    feature_selection_report: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    context = dict(feature_selection_report or {})
    mode_key = str(
        context.get("selected_mode_key")
        or (
            DEFAULT_CLUSTER_MODE_LOAD
            if "Число пожаров" in selected_features
            else DEFAULT_CLUSTER_MODE_PROFILE
        )
    )
    mode_label = str(
        context.get("selected_mode_label")
        or (
            DEFAULT_CLUSTER_MODE_LOAD_LABEL
            if mode_key == DEFAULT_CLUSTER_MODE_LOAD
            else DEFAULT_CLUSTER_MODE_PROFILE_LABEL
        )
    )
    volume_role_code = str(context.get("volume_role_code") or mode_key)
    volume_role_label = str(context.get("volume_role_label") or mode_label)
    volume_note = str(
        context.get("volume_note")
        or (
            "Выбран режим типологии без компоненты нагрузки: число пожаров не используется ни как признак, ни как скрытый вес территории."
            if mode_key == DEFAULT_CLUSTER_MODE_PROFILE
            else "Выбран режим типологии с компонентой нагрузки: число пожаров может влиять и как признак, и через умеренные веса территории."
    )
    )
    weighting_strategy = str(context.get("weighting_strategy") or _weighting_strategy_for_mode(mode_key))
    weighting_label, weighting_note, weighting_meta = _describe_weighting_strategy(mode_label, weighting_strategy)
    return {
        **context,
        "selected_features": list(selected_features),
        "selected_mode_key": mode_key,
        "selected_mode_label": mode_label,
        "volume_role_code": volume_role_code,
        "volume_role_label": volume_role_label,
        "volume_note": volume_note,
        "weighting_strategy": weighting_strategy,
        "weighting_label": weighting_label,
        "weighting_note": weighting_note,
        "weighting_meta": weighting_meta,
        "uses_incident_weights": weighting_strategy == WEIGHTING_STRATEGY_INCIDENT_LOG,
        "uses_sample_weights": weighting_strategy == WEIGHTING_STRATEGY_INCIDENT_LOG,
    }


def _build_runtime_clustering_context(
    feature_selection_report: Dict[str, Any] | None,
    *,
    method_label: str,
    algorithm_key: str,
    weighting_strategy: str,
) -> Dict[str, Any]:
    context = dict(feature_selection_report or {})
    mode_label = str(
        context.get("selected_mode_label")
        or context.get("volume_role_label")
        or DEFAULT_CLUSTER_MODE_PROFILE_LABEL
    )
    base_weighting = str(context.get("weighting_strategy") or "")
    weighting_label, weighting_note, weighting_meta = _describe_weighting_strategy(mode_label, weighting_strategy)
    if base_weighting and base_weighting != weighting_strategy:
        weighting_note = (
            f"{weighting_note} На текущем срезе рабочий вывод использует именно эту стратегию, "
            "потому что в честном сравнении на том же наборе признаков она оказалась сильнее базовой стратегии режима."
        )
    context.update(
        {
            "weighting_strategy": weighting_strategy,
            "weighting_label": weighting_label,
            "weighting_note": weighting_note,
            "weighting_meta": weighting_meta,
            "uses_incident_weights": weighting_strategy == WEIGHTING_STRATEGY_INCIDENT_LOG,
            "uses_sample_weights": weighting_strategy == WEIGHTING_STRATEGY_INCIDENT_LOG,
            "selected_method_label": method_label,
            "selected_algorithm_key": algorithm_key,
        }
    )
    return context


def _primary_method_key(weighting_strategy: str) -> str:
    return f"kmeans_{weighting_strategy}"


def _build_method_candidates(weighting_strategy: str) -> List[Dict[str, Any]]:
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
) -> Dict[str, Any]:
    _, scaled_points, scaler, transformed_columns, sample_weights = _prepare_model_inputs(
        cluster_frame,
        entity_frame,
        weighting_strategy=weighting_strategy,
    )
    if algorithm_key == "kmeans":
        model = _fit_weighted_kmeans(scaled_points, sample_weights, cluster_count, random_state=42, n_init=40)
        labels = model.labels_
        _validate_cluster_labels(labels, cluster_count)
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
            n_init=40,
        )
        raw_centers, scaled_centers = _derive_cluster_centers(cluster_frame, scaled_points, labels, cluster_count)
        inertia = _compute_cluster_inertia(scaled_points, labels, scaled_centers=scaled_centers)
        initialization_ari = None
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
        "explained_variance": float(np.sum(pca.explained_variance_ratio_)),
        "algorithm_key": algorithm_key,
        "method_key": method_key or (_primary_method_key(weighting_strategy) if algorithm_key == "kmeans" else algorithm_key),
        "weighting_strategy": weighting_strategy,
    }


def _evaluate_cluster_counts(
    cluster_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    weighting_strategy: str = WEIGHTING_STRATEGY_INCIDENT_LOG,
) -> Dict[str, Any]:
    if len(cluster_frame) < 3:
        return {"rows": [], "best_silhouette_k": None, "best_quality_k": None, "elbow_k": None}
    available_ks = [
        cluster_count
        for cluster_count in CLUSTER_COUNT_OPTIONS
        if 2 <= cluster_count <= min(MAX_K_DIAGNOSTICS, len(cluster_frame) - 1)
    ]

    rows: List[Dict[str, Any]] = []
    method_rows_by_cluster_count: Dict[int, List[Dict[str, Any]]] = {}
    for cluster_count in available_ks:
        method_rows = _compare_clustering_methods(
            cluster_frame,
            entity_frame,
            cluster_count,
            weighting_strategy=weighting_strategy,
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
        "best_configuration": dict(best_quality_row) if best_quality_row else None,
        "elbow_k": _estimate_elbow_k(rows),
    }


def _build_default_feature_selection_analysis(
    feature_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    available_features: Sequence[str],
    cluster_count: int,
) -> Dict[str, Any]:
    ordered_features = [
        feature
        for feature in available_features
        if feature in feature_frame.columns and feature not in AUTO_DEFAULT_EXCLUDED_FEATURES
    ]
    if len(ordered_features) < 2:
        selected_features = list(ordered_features)
        return _build_clustering_mode_context(
            selected_features,
            {
                "selected_features": selected_features,
                "selected_mode_key": DEFAULT_CLUSTER_MODE_PROFILE,
                "selected_mode_label": DEFAULT_CLUSTER_MODE_PROFILE_LABEL,
                "mode_candidates": [],
                "ablation_rows": [],
                "volume_role_code": DEFAULT_CLUSTER_MODE_PROFILE,
                "volume_role_label": DEFAULT_CLUSTER_MODE_PROFILE_LABEL,
                "volume_note": "Из-за малого числа доступных признаков кластеризация описывает только тот профиль, который удалось собрать из текущего среза.",
                "selection_note": "Базовый набор собран по малому ablation-анализу, но в текущем срезе доступно слишком мало признаков для полноценного сравнения режимов.",
            },
        )

    mode_candidates: List[Dict[str, Any]] = []
    for mode_key, mode_label, excluded_features in [
        (DEFAULT_CLUSTER_MODE_PROFILE, DEFAULT_CLUSTER_MODE_PROFILE_LABEL, PROFILE_MODE_EXCLUDED_FEATURES),
        (DEFAULT_CLUSTER_MODE_LOAD, DEFAULT_CLUSTER_MODE_LOAD_LABEL, set()),
    ]:
        weighting_strategy = _weighting_strategy_for_mode(mode_key)
        candidate_pool = [feature for feature in ordered_features if feature not in excluded_features]
        if len(candidate_pool) < 2:
            continue
        selected_features = _choose_features_from_pool(
            feature_frame=feature_frame,
            entity_frame=entity_frame,
            available_features=candidate_pool,
            cluster_count=cluster_count,
            weighting_strategy=weighting_strategy,
        )
        evaluation = _evaluate_feature_subset(
            feature_frame=feature_frame,
            entity_frame=entity_frame,
            selected_features=selected_features,
            cluster_count=cluster_count,
            weighting_strategy=weighting_strategy,
        )
        ablation_rows = _build_feature_ablation_rows(
            feature_frame=feature_frame,
            entity_frame=entity_frame,
            selected_features=selected_features,
            candidate_features=ordered_features,
            cluster_count=cluster_count,
            weighting_strategy=weighting_strategy,
        )
        mode_candidates.append(
            {
                "mode_key": mode_key,
                "mode_label": mode_label,
                "selected_features": selected_features,
                "ablation_rows": ablation_rows,
                "weighting_strategy": weighting_strategy,
                **evaluation,
            }
        )

    if not mode_candidates:
        fallback = ordered_features[: max(2, min(len(ordered_features), MIN_DEFAULT_FEATURE_COUNT))]
        return _build_clustering_mode_context(
            fallback,
            {
                "selected_features": fallback,
                "selected_mode_key": DEFAULT_CLUSTER_MODE_PROFILE,
                "selected_mode_label": DEFAULT_CLUSTER_MODE_PROFILE_LABEL,
                "mode_candidates": [],
                "ablation_rows": [],
                "volume_role_code": DEFAULT_CLUSTER_MODE_PROFILE,
                "volume_role_label": DEFAULT_CLUSTER_MODE_PROFILE_LABEL,
                "volume_note": "Сравнить profile/load режимы не удалось, поэтому выбран самый устойчивый доступный набор признаков.",
                "selection_note": "Базовый набор собран по малому ablation-анализу, но режимы profile/load сравнить не удалось из-за ограниченного числа признаков.",
            },
        )

    selected_mode = _pick_default_feature_selection_mode(mode_candidates)
    volume_role = _summarize_volume_role(selected_mode)

    return _build_clustering_mode_context(
        list(selected_mode["selected_features"]),
        {
            "selected_features": list(selected_mode["selected_features"]),
            "selected_mode_key": selected_mode["mode_key"],
            "selected_mode_label": selected_mode["mode_label"],
            "mode_candidates": mode_candidates,
            "ablation_rows": selected_mode["ablation_rows"],
            "volume_role_code": volume_role["code"],
            "volume_role_label": volume_role["label"],
            "volume_note": volume_role["note"],
            "weighting_strategy": selected_mode.get("weighting_strategy"),
            "selection_note": (
                "Базовый набор собран по малому ablation-анализу: сначала сравниваются режимы "
                f"'{DEFAULT_CLUSTER_MODE_PROFILE_LABEL}' и '{DEFAULT_CLUSTER_MODE_LOAD_LABEL}', "
                "после чего в выбранном режиме проверяется вклад каждого признака в silhouette / DB / CH."
            ),
        },
    )


def _select_default_cluster_features(
    feature_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    available_features: Sequence[str],
    cluster_count: int,
) -> List[str]:
    return list(
        _build_default_feature_selection_analysis(
            feature_frame=feature_frame,
            entity_frame=entity_frame,
            available_features=available_features,
            cluster_count=cluster_count,
        )["selected_features"]
    )


def _choose_features_from_pool(
    feature_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    available_features: Sequence[str],
    cluster_count: int,
    weighting_strategy: str = WEIGHTING_STRATEGY_INCIDENT_LOG,
) -> List[str]:
    ordered_features = [feature for feature in available_features if feature in feature_frame.columns]
    if len(ordered_features) < 2:
        return list(ordered_features)

    feature_order = {feature: index for index, feature in enumerate(ordered_features)}
    evaluation_cache: Dict[Tuple[str, ...], Dict[str, float]] = {}
    preferred_features = [feature for feature in DEFAULT_CLUSTER_FEATURES if feature in feature_order]
    anchor_feature = preferred_features[0] if preferred_features else ordered_features[0]

    def _normalize_subset(subset: Sequence[str]) -> Tuple[str, ...]:
        return tuple(sorted(dict.fromkeys(subset), key=lambda feature: feature_order[feature]))

    def _evaluate_subset(subset: Sequence[str]) -> Dict[str, float]:
        normalized_subset = _normalize_subset(subset)
        if normalized_subset not in evaluation_cache:
            evaluation_cache[normalized_subset] = _evaluate_feature_subset(
                feature_frame=feature_frame,
                entity_frame=entity_frame,
                selected_features=normalized_subset,
                cluster_count=cluster_count,
                weighting_strategy=weighting_strategy,
            )
        return evaluation_cache[normalized_subset]

    pair_candidates = [
        (anchor_feature, feature)
        for feature in ordered_features
        if feature != anchor_feature
    ]
    if not pair_candidates:
        pair_candidates = list(combinations(ordered_features, 2))

    best_pair: Tuple[str, ...] | None = None
    best_result: Dict[str, float] | None = None
    for pair in pair_candidates:
        result = _evaluate_subset(pair)
        if best_result is None or _subset_result_sort_key(result) > _subset_result_sort_key(best_result):
            best_pair = _normalize_subset(pair)
            best_result = result

    if best_pair is None or best_result is None:
        fallback = [feature for feature in preferred_features if feature in ordered_features]
        if len(fallback) >= 2:
            return fallback
        return ordered_features[:2]

    selected = list(best_pair)
    current_result = best_result
    remaining = [feature for feature in ordered_features if feature not in selected]
    target_feature_count = min(DEFAULT_FEATURE_TARGET_COUNT, len(ordered_features))

    while remaining and len(selected) < target_feature_count:
        candidate_rows = [
            (feature, _evaluate_subset([*selected, feature]))
            for feature in remaining
        ]
        best_feature, candidate_result = max(candidate_rows, key=lambda item: _subset_result_sort_key(item[1]))
        if candidate_result["score"] < current_result["score"] + FEATURE_SELECTION_MIN_IMPROVEMENT:
            break
        selected.append(best_feature)
        remaining.remove(best_feature)
        current_result = candidate_result

    improved = True
    while improved and len(selected) > 2:
        improved = False
        removal_rows = [
            (
                feature,
                _evaluate_subset([selected_feature for selected_feature in selected if selected_feature != feature]),
            )
            for feature in selected
            if feature != anchor_feature
        ]
        if not removal_rows:
            break
        feature_to_remove, removal_result = max(removal_rows, key=lambda item: _subset_result_sort_key(item[1]))
        if removal_result["score"] >= current_result["score"] + (FEATURE_SELECTION_MIN_IMPROVEMENT / 2.0):
            selected.remove(feature_to_remove)
            current_result = removal_result
            improved = True

    min_feature_count = min(max(2, MIN_DEFAULT_FEATURE_COUNT), len(ordered_features))
    while len(selected) < min_feature_count:
        candidate_rows = [
            (feature, _evaluate_subset([*selected, feature]))
            for feature in ordered_features
            if feature not in selected
        ]
        if not candidate_rows:
            break
        best_feature, candidate_result = max(candidate_rows, key=lambda item: _subset_result_sort_key(item[1]))
        selected.append(best_feature)
        current_result = candidate_result

    selected_set = set(selected)
    prioritized = [feature for feature in DEFAULT_CLUSTER_FEATURES if feature in selected_set]
    remainder = [feature for feature in ordered_features if feature in selected_set and feature not in prioritized]
    return prioritized + remainder


def _pick_default_feature_selection_mode(mode_candidates: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    load_mode = next((item for item in mode_candidates if item["mode_key"] == DEFAULT_CLUSTER_MODE_LOAD), None)
    profile_mode = next((item for item in mode_candidates if item["mode_key"] == DEFAULT_CLUSTER_MODE_PROFILE), None)
    if load_mode is None:
        return profile_mode or max(mode_candidates, key=_subset_result_sort_key)
    if profile_mode is None:
        return load_mode

    score_gap = float(load_mode["score"] - profile_mode["score"])
    load_silhouette = float(load_mode.get("silhouette", float("-inf")))
    profile_silhouette = float(profile_mode.get("silhouette", float("-inf")))
    if score_gap <= PROFILE_MODE_SCORE_TOLERANCE and profile_silhouette >= load_silhouette - PROFILE_MODE_SILHOUETTE_TOLERANCE:
        return profile_mode
    return load_mode


def _build_feature_ablation_rows(
    feature_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    selected_features: Sequence[str],
    candidate_features: Sequence[str],
    cluster_count: int,
    weighting_strategy: str = WEIGHTING_STRATEGY_INCIDENT_LOG,
) -> List[Dict[str, Any]]:
    base_result = _evaluate_feature_subset(
        feature_frame,
        entity_frame,
        selected_features,
        cluster_count,
        weighting_strategy=weighting_strategy,
    )
    rows: List[Dict[str, Any]] = []

    for feature in selected_features:
        reduced_features = [item for item in selected_features if item != feature]
        reduced_result = _evaluate_feature_subset(
            feature_frame,
            entity_frame,
            reduced_features,
            cluster_count,
            weighting_strategy=weighting_strategy,
        )
        rows.append(
            {
                "feature": feature,
                "direction": "drop",
                "delta_score": float(base_result["score"] - reduced_result["score"]),
                "delta_silhouette": float(base_result["silhouette"] - reduced_result["silhouette"]),
            }
        )

    for feature in candidate_features:
        if feature in selected_features:
            continue
        expanded_result = _evaluate_feature_subset(
            feature_frame,
            entity_frame,
            [*selected_features, feature],
            cluster_count,
            weighting_strategy=weighting_strategy,
        )
        rows.append(
            {
                "feature": feature,
                "direction": "add",
                "delta_score": float(expanded_result["score"] - base_result["score"]),
                "delta_silhouette": float(expanded_result["silhouette"] - base_result["silhouette"]),
            }
        )

    return sorted(
        rows,
        key=lambda item: (
            1 if item["direction"] == "drop" else 0,
            abs(float(item["delta_score"])),
            abs(float(item["delta_silhouette"])),
        ),
        reverse=True,
    )


def _summarize_volume_role(selection_report: Dict[str, Any]) -> Dict[str, str]:
    if selection_report["mode_key"] == DEFAULT_CLUSTER_MODE_PROFILE:
        return {
            "code": DEFAULT_CLUSTER_MODE_PROFILE,
            "label": DEFAULT_CLUSTER_MODE_PROFILE_LABEL,
            "note": "По умолчанию кластеризация описывает профиль территории без отдельного разбиения по объёму нагрузки: признак 'Число пожаров' не нужен, чтобы сохранить качество на текущем срезе.",
        }

    count_drop = next(
        (
            row
            for row in selection_report["ablation_rows"]
            if row["direction"] == "drop" and row["feature"] == "Число пожаров"
        ),
        None,
    )
    strongest_other = max(
        (
            row
            for row in selection_report["ablation_rows"]
            if row["direction"] == "drop" and row["feature"] != "Число пожаров"
        ),
        key=lambda item: float(item["delta_score"]),
        default=None,
    )
    count_delta = float(count_drop["delta_score"]) if count_drop else 0.0
    strongest_other_delta = float(strongest_other["delta_score"]) if strongest_other else 0.0
    if count_drop and count_delta >= max(VOLUME_DOMINANCE_MIN_SCORE_DELTA, strongest_other_delta * VOLUME_DOMINANCE_RATIO):
        return {
            "code": "load_dominant",
            "label": DEFAULT_CLUSTER_MODE_LOAD_LABEL,
            "note": "По умолчанию кластеризация описывает профиль территории с сильной компонентой объёма нагрузки: признак 'Число пожаров' даёт самый заметный вклад в качество разбиения.",
        }
    return {
        "code": "load_aware",
        "label": DEFAULT_CLUSTER_MODE_LOAD_LABEL,
        "note": "По умолчанию кластеризация описывает профиль территории с учётом объёма нагрузки, но не сводится только к числу пожаров: кроме объёма, кластеризацию удерживают и профильные признаки.",
    }


def _evaluate_feature_subset(
    feature_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    selected_features: Sequence[str],
    cluster_count: int,
    weighting_strategy: str = WEIGHTING_STRATEGY_INCIDENT_LOG,
) -> Dict[str, float]:
    subset_frame, subset_entities = _prepare_subset_frame(feature_frame, entity_frame, selected_features)
    if len(subset_frame) <= 2:
        return {
            "score": float("-inf"),
            "silhouette": float("-inf"),
            "davies_bouldin": float("inf"),
            "calinski_harabasz": float("-inf"),
            "cluster_balance_ratio": 0.0,
        }

    actual_cluster_count = min(max(CLUSTER_COUNT_OPTIONS[0], int(cluster_count)), len(subset_frame) - 1)
    if actual_cluster_count < 2:
        return {
            "score": float("-inf"),
            "silhouette": float("-inf"),
            "davies_bouldin": float("inf"),
            "calinski_harabasz": float("-inf"),
            "cluster_balance_ratio": 0.0,
        }

    _, scaled_points, _, _, sample_weights = _prepare_model_inputs(
        subset_frame,
        subset_entities,
        weighting_strategy=weighting_strategy,
    )
    model = _fit_weighted_kmeans(scaled_points, sample_weights, actual_cluster_count, random_state=42, n_init=20)
    metrics = compute_clustering_metrics(scaled_points, model.labels_)
    return {
        "score": _cluster_quality_score(metrics, len(subset_frame)),
        "silhouette": float(metrics.get("silhouette") or float("-inf")),
        "davies_bouldin": float(metrics.get("davies_bouldin") or float("inf")),
        "calinski_harabasz": float(metrics.get("calinski_harabasz") or float("-inf")),
        "cluster_balance_ratio": float(metrics.get("cluster_balance_ratio") or 0.0),
    }


def _cluster_quality_score(metrics: Dict[str, float | None], row_count: int) -> float:
    silhouette = float(metrics.get("silhouette") or 0.0)
    davies_bouldin = metrics.get("davies_bouldin")
    calinski_harabasz = float(metrics.get("calinski_harabasz") or 0.0)
    balance_ratio = float(metrics.get("cluster_balance_ratio") or 0.0)
    inverse_db = 0.0 if davies_bouldin is None else 1.0 / (1.0 + max(float(davies_bouldin), 0.0))
    scaled_ch = 1.0 - math.exp(-max(calinski_harabasz, 0.0) / max(float(row_count), 1.0))
    shape_penalty = float(_cluster_shape_diagnostics(metrics, row_count)["shape_penalty"])
    return float((silhouette * 0.55) + (inverse_db * 0.20) + (scaled_ch * 0.15) + (balance_ratio * 0.10) - shape_penalty)


def _cluster_shape_diagnostics(metrics: Dict[str, float | None], row_count: int) -> Dict[str, float | bool | int]:
    smallest_cluster_size = int(metrics.get("smallest_cluster_size") or 0)
    balance_ratio = float(metrics.get("cluster_balance_ratio") or 0.0)
    microcluster_threshold = max(3, int(math.ceil(max(float(row_count), 1.0) * 0.03)))
    has_microclusters = 0 < smallest_cluster_size < microcluster_threshold
    has_balance_warning = balance_ratio < 0.18

    microcluster_penalty = 0.0
    if has_microclusters:
        shortfall = (microcluster_threshold - smallest_cluster_size) / max(microcluster_threshold, 1)
        microcluster_penalty = min(0.14, 0.04 + (shortfall * 0.10))

    imbalance_penalty = 0.0
    if has_balance_warning:
        shortfall = (0.18 - balance_ratio) / 0.18
        imbalance_penalty = min(0.10, shortfall * 0.08)

    return {
        "microcluster_threshold": microcluster_threshold,
        "has_microclusters": has_microclusters,
        "has_balance_warning": has_balance_warning,
        "shape_penalty": float(microcluster_penalty + imbalance_penalty),
    }


def _subset_result_sort_key(result: Dict[str, float]) -> tuple[float, float, float, float, float]:
    davies_bouldin = result.get("davies_bouldin", float("inf"))
    return (
        float(result.get("score", float("-inf"))),
        float(result.get("silhouette", float("-inf"))),
        -float(davies_bouldin if math.isfinite(davies_bouldin) else 1e9),
        float(result.get("calinski_harabasz", float("-inf"))),
        float(result.get("cluster_balance_ratio", 0.0)),
    )


def _diagnostics_row_sort_key(result: Dict[str, Any]) -> tuple[float, float, float, float, float]:
    davies_bouldin = result.get("davies_bouldin", float("inf"))
    davies_value = float("inf") if davies_bouldin is None else float(davies_bouldin)
    return (
        float(result.get("quality_score", float("-inf"))),
        float(result.get("silhouette", float("-inf"))),
        -float(davies_value if math.isfinite(davies_value) else 1e9),
        float(result.get("cluster_balance_ratio", 0.0)),
        -float(result.get("shape_penalty", 0.0)),
    )


def _prepare_subset_frame(
    feature_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    selected_features: Sequence[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    numeric_frame = feature_frame.loc[:, list(selected_features)].apply(pd.to_numeric, errors="coerce")
    required_non_null = min(len(selected_features), max(2, math.ceil(len(selected_features) * 0.6)))
    row_mask = numeric_frame.notna().sum(axis=1) >= required_non_null
    prepared_numeric = numeric_frame.loc[row_mask].copy()
    prepared_entities = entity_frame.loc[row_mask].copy()
    if prepared_numeric.empty:
        return prepared_numeric, prepared_entities
    prepared_numeric = prepared_numeric.fillna(prepared_numeric.median(numeric_only=True))
    return prepared_numeric.reset_index(drop=True), prepared_entities.reset_index(drop=True)


def _prepare_model_inputs(
    cluster_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    weighting_strategy: str = WEIGHTING_STRATEGY_INCIDENT_LOG,
) -> tuple[pd.DataFrame, np.ndarray, StandardScaler, set[str], np.ndarray]:
    model_frame, transformed_columns = _prepare_model_frame(cluster_frame)
    scaler = StandardScaler()
    scaled_points = scaler.fit_transform(model_frame.to_numpy(dtype=float))
    sample_weights = _build_sample_weights(entity_frame, weighting_strategy=weighting_strategy)
    return model_frame, scaled_points, scaler, transformed_columns, sample_weights


def _build_sample_weights(
    entity_frame: pd.DataFrame,
    weighting_strategy: str = WEIGHTING_STRATEGY_INCIDENT_LOG,
) -> np.ndarray:
    if (
        weighting_strategy in {WEIGHTING_STRATEGY_UNIFORM, WEIGHTING_STRATEGY_NOT_APPLICABLE}
        or "Число пожаров" not in entity_frame.columns
    ):
        return np.ones(len(entity_frame), dtype=float)
    counts = pd.to_numeric(entity_frame["Число пожаров"], errors="coerce").fillna(1.0).clip(lower=1.0).to_numpy(dtype=float)
    weights = np.log1p(counts)
    mean_weight = float(np.mean(weights))
    if mean_weight <= 0:
        return np.ones(len(counts), dtype=float)
    return weights / mean_weight


def _fit_clustering_labels(
    scaled_points: np.ndarray,
    cluster_count: int,
    *,
    algorithm_key: str,
    sample_weights: np.ndarray,
    random_state: int,
    n_init: int,
) -> np.ndarray:
    if algorithm_key == "kmeans":
        model = _fit_weighted_kmeans(
            scaled_points,
            sample_weights,
            cluster_count,
            random_state=random_state,
            n_init=n_init,
        )
        labels = model.labels_
    elif algorithm_key == "agglomerative":
        labels = AgglomerativeClustering(n_clusters=cluster_count, linkage="ward").fit_predict(scaled_points)
    elif algorithm_key == "birch":
        labels = Birch(n_clusters=cluster_count).fit_predict(scaled_points)
    else:
        raise ValueError(f"Unsupported clustering algorithm: {algorithm_key}")
    _validate_cluster_labels(labels, cluster_count)
    return labels


def _validate_cluster_labels(labels: np.ndarray, cluster_count: int) -> None:
    unique_labels = np.unique(labels)
    if len(unique_labels) != cluster_count:
        raise ValueError(f"Expected {cluster_count} clusters, got {len(unique_labels)}.")


def _derive_cluster_centers(
    cluster_frame: pd.DataFrame,
    scaled_points: np.ndarray,
    labels: np.ndarray,
    cluster_count: int,
) -> tuple[np.ndarray, np.ndarray]:
    raw_points = cluster_frame.to_numpy(dtype=float)
    raw_centers: List[np.ndarray] = []
    scaled_centers: List[np.ndarray] = []
    for cluster_id in range(cluster_count):
        mask = labels == cluster_id
        if not np.any(mask):
            raise ValueError(f"Cluster {cluster_id} is empty.")
        raw_centers.append(np.mean(raw_points[mask], axis=0))
        scaled_centers.append(np.mean(scaled_points[mask], axis=0))
    return np.vstack(raw_centers), np.vstack(scaled_centers)


def _compute_cluster_inertia(
    scaled_points: np.ndarray,
    labels: np.ndarray,
    *,
    scaled_centers: np.ndarray | None = None,
) -> float:
    if scaled_centers is None:
        cluster_count = int(np.max(labels)) + 1
        _, scaled_centers = _derive_cluster_centers(
            pd.DataFrame(scaled_points),
            scaled_points,
            labels,
            cluster_count,
        )
    distances = scaled_points - scaled_centers[labels]
    return float(np.sum(np.square(distances)))


def _fit_weighted_kmeans(
    scaled_points: np.ndarray,
    sample_weights: np.ndarray,
    cluster_count: int,
    random_state: int,
    n_init: int,
) -> KMeans:
    model = KMeans(n_clusters=cluster_count, random_state=random_state, n_init=n_init)
    model.fit(scaled_points, sample_weight=sample_weights)
    return model


def _estimate_kmeans_initialization_stability(
    scaled_points: np.ndarray,
    cluster_count: int,
    sample_weights: np.ndarray,
) -> float | None:
    reference = None
    scores: List[float] = []
    for seed in STABILITY_RANDOM_SEEDS:
        model = _fit_weighted_kmeans(scaled_points, sample_weights, cluster_count, random_state=seed, n_init=25)
        labels = model.labels_
        if reference is None:
            reference = labels
            continue
        scores.append(float(adjusted_rand_score(reference, labels)))
    if not scores:
        return None
    return float(np.mean(scores))


def _estimate_resampled_stability(
    scaled_points: np.ndarray,
    cluster_count: int,
    sample_weights: np.ndarray,
    algorithm_key: str = "kmeans",
) -> float | None:
    row_count = len(scaled_points)
    if row_count <= max(cluster_count + 1, 8):
        return None

    subset_size = min(row_count, max(cluster_count * 2, int(round(row_count * STABILITY_RESAMPLE_RATIO))))
    resampled_models: List[Dict[str, Any]] = []
    for seed in STABILITY_RANDOM_SEEDS:
        rng = np.random.default_rng(seed)
        sampled_indexes = np.sort(rng.choice(row_count, size=subset_size, replace=False))
        try:
            labels = _fit_clustering_labels(
                scaled_points[sampled_indexes],
                cluster_count,
                algorithm_key=algorithm_key,
                sample_weights=sample_weights[sampled_indexes],
                random_state=seed,
                n_init=25,
            )
        except Exception:
            continue
        resampled_models.append({"indexes": sampled_indexes, "labels": labels})

    pair_scores: List[float] = []
    minimum_overlap = max(cluster_count + 2, 4)
    for left_model, right_model in combinations(resampled_models, 2):
        overlap_indexes = np.intersect1d(left_model["indexes"], right_model["indexes"])
        if len(overlap_indexes) < minimum_overlap:
            continue
        left_positions = np.searchsorted(left_model["indexes"], overlap_indexes)
        right_positions = np.searchsorted(right_model["indexes"], overlap_indexes)
        left_labels = left_model["labels"][left_positions]
        right_labels = right_model["labels"][right_positions]
        pair_scores.append(float(adjusted_rand_score(left_labels, right_labels)))
    if not pair_scores:
        return None
    return float(np.mean(pair_scores))


def _assign_labels_from_centers(scaled_points: np.ndarray, centers: np.ndarray) -> np.ndarray:
    distances = np.linalg.norm(scaled_points[:, np.newaxis, :] - centers[np.newaxis, :, :], axis=2)
    return np.argmin(distances, axis=1)


def _compare_clustering_methods(
    cluster_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    cluster_count: int,
    weighting_strategy: str = WEIGHTING_STRATEGY_INCIDENT_LOG,
    selected_method_key: str | None = None,
) -> List[Dict[str, Any]]:
    _, scaled_points, _, _, _ = _prepare_model_inputs(
        cluster_frame,
        entity_frame,
        weighting_strategy=weighting_strategy,
    )
    rows: List[Dict[str, Any]] = []
    row_count = len(cluster_frame)
    primary_method_key = selected_method_key or _primary_method_key(weighting_strategy)

    def _append_method_row(candidate: Dict[str, Any]) -> None:
        try:
            row_weighting_strategy = str(candidate.get("weighting_strategy") or WEIGHTING_STRATEGY_NOT_APPLICABLE)
            sample_weights = _build_sample_weights(entity_frame, weighting_strategy=row_weighting_strategy)
            labels = _fit_clustering_labels(
                scaled_points,
                cluster_count,
                algorithm_key=str(candidate["algorithm_key"]),
                sample_weights=sample_weights,
                random_state=42,
                n_init=40,
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


def _select_recommended_method_row(method_rows: Sequence[Dict[str, Any]]) -> Dict[str, Any] | None:
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
    diagnostics: Dict[str, Any],
    total_incidents: int,
    total_entities: int,
    sampled_entities: int,
    support_summary: Dict[str, float] | None = None,
    stability_ari: float | None = None,
    feature_selection_report: Dict[str, Any] | None = None,
) -> List[str]:
    notes = [
        f"Кластеризация построена по { _format_integer(sampled_entities) } территориям/населённым пунктам, агрегированным из { _format_integer(total_incidents) } пожаров.",
    ]
    if total_entities > sampled_entities:
        notes.append(
            f"До кластеризации были собраны агрегаты по { _format_integer(total_entities) } территориям, а затем отобрана подвыборка без смещения по первым строкам таблицы."
        )

    if support_summary:
        low_support_share = float(support_summary.get("low_support_share") or 0.0)
        if low_support_share > 0.2:
            algorithm_key = str((feature_selection_report or {}).get("selected_algorithm_key") or "kmeans")
            if bool((feature_selection_report or {}).get("uses_incident_weights")):
                notes.append(
                    f"{_format_percent(low_support_share)} территорий имеют не более {LOW_SUPPORT_TERRITORY_THRESHOLD} пожаров, поэтому долевые признаки считаются через empirical Bayes shrinkage к глобальному среднему: вместо raw 0/1 к истории территории добавляются около {int(RATE_SMOOTHING_PRIOR_STRENGTH)} псевдо-наблюдений, а в KMeans территории с большей историей получают только умеренный log-вес."
                )
            elif algorithm_key == "kmeans":
                notes.append(
                    f"{_format_percent(low_support_share)} территорий имеют не более {LOW_SUPPORT_TERRITORY_THRESHOLD} пожаров, поэтому долевые признаки считаются через empirical Bayes shrinkage к глобальному среднему: вместо raw 0/1 к истории территории добавляются около {int(RATE_SMOOTHING_PRIOR_STRENGTH)} псевдо-наблюдений, а в KMeans все территории остаются с равным весом."
                )
            else:
                notes.append(
                    f"{_format_percent(low_support_share)} территорий имеют не более {LOW_SUPPORT_TERRITORY_THRESHOLD} пожаров, поэтому долевые признаки считаются через empirical Bayes shrinkage к глобальному среднему: вместо raw 0/1 к истории территории добавляются около {int(RATE_SMOOTHING_PRIOR_STRENGTH)} псевдо-наблюдений, а выбранный алгоритм работает без дополнительных sample weights."
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
                f"Малый ablation-анализ не включил признак '{worst_feature['feature']}', потому что его добавление ухудшало качество разбиения на текущем срезе."
            )

    if silhouette is None:
        notes.append("Коэффициент силуэта не рассчитан: для этого нужно больше территорий, чем кластеров, и хотя бы две непустые группы.")
    elif silhouette < 0.2:
        notes.append("Кластеры отделены слабо: профиль территорий плавный, поэтому результат стоит трактовать как предварительную типологию, а не как жёсткое разбиение.")
    elif silhouette < 0.4:
        notes.append("Разделение умеренное: типы территорий уже читаются, но между ними остаются заметные переходные зоны.")
    else:
        notes.append("Кластеры отделены достаточно чётко для исследовательского сравнения типов территорий риска.")

    if stability_ari is not None:
        if stability_ari < 0.45:
            notes.append(
                f"На повторных подвыборках устойчивость низкая (ARI {_format_number(stability_ari, 3)}): сегментация заметно меняется от состава выборки, поэтому интерпретацию кластеров лучше проверять по представителям и центрам."
            )
        elif stability_ari < 0.7:
            notes.append(
                f"На повторных подвыборках устойчивость умеренная (ARI {_format_number(stability_ari, 3)}): общая типология сохраняется, но границы между соседними кластерами ещё чувствительны к составу данных."
            )
        else:
            notes.append(
                f"На повторных подвыборках сегментация выглядит воспроизводимой (ARI {_format_number(stability_ari, 3)}), хотя это всё равно не гарантирует идеальную устойчивость на новых периодах."
            )

    best_quality_k = diagnostics.get("best_quality_k")
    best_silhouette_k = diagnostics.get("best_silhouette_k")
    elbow_k = diagnostics.get("elbow_k")
    available_k_label = f"{CLUSTER_COUNT_OPTIONS[0]}..{CLUSTER_COUNT_OPTIONS[-1]}"
    if best_quality_k and best_silhouette_k and best_quality_k != best_silhouette_k:
        notes.append(
            f"В доступном пользователю диапазоне k={available_k_label} по совокупности метрик и размеров кластеров лучше выглядит k={best_quality_k}, хотя пик silhouette отдельно приходится на k={best_silhouette_k}."
        )
    elif best_quality_k:
        notes.append(
            f"В доступном пользователю диапазоне k={available_k_label} по совокупности метрик и размеров кластеров наиболее убедительно выглядит k={best_quality_k}."
        )
    elif best_silhouette_k and elbow_k and best_silhouette_k != elbow_k:
        notes.append(
            f"В доступном пользователю диапазоне k={available_k_label} silhouette лучше всего выглядит при k={best_silhouette_k}, а локоть начинается около k={elbow_k}."
        )
    elif best_silhouette_k:
        notes.append(f"В доступном пользователю диапазоне k={available_k_label} коэффициент силуэта лучше всего выглядит при k={best_silhouette_k}.")
    elif elbow_k:
        notes.append(f"В доступном пользователю диапазоне k={available_k_label} кривая inertia даёт заметный сгиб около k={elbow_k}.")

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


def _prepare_model_frame(cluster_frame: pd.DataFrame) -> Tuple[pd.DataFrame, set[str]]:
    transformed = cluster_frame.copy().astype(float)
    transformed_columns: set[str] = set()
    for column in transformed.columns:
        if column not in LOG_SCALE_FEATURES:
            continue
        series = transformed[column].clip(lower=0.0)
        if float(series.skew(skipna=True) or 0.0) < 1.0:
            continue
        transformed[column] = np.log1p(series)
        transformed_columns.add(column)
    return transformed, transformed_columns


def _restore_raw_centers(transformed_centers: np.ndarray, columns: Sequence[str], transformed_columns: set[str]) -> np.ndarray:
    raw_centers = np.array(transformed_centers, copy=True)
    for column_index, column in enumerate(columns):
        if column in transformed_columns:
            raw_centers[:, column_index] = np.expm1(raw_centers[:, column_index])
    return raw_centers


def _estimate_elbow_k(rows: Sequence[Dict[str, Any]]) -> int | None:
    if len(rows) < 3:
        return None
    x = np.asarray([item["cluster_count"] for item in rows], dtype=float)
    y = np.asarray([item["inertia"] for item in rows], dtype=float)
    start = np.array([x[0], y[0]], dtype=float)
    end = np.array([x[-1], y[-1]], dtype=float)
    baseline = end - start
    norm = np.linalg.norm(baseline)
    if norm == 0:
        return int(x[0])

    interior_distances = []
    interior_ks = []
    for index in range(1, len(rows) - 1):
        point = np.array([x[index], y[index]], dtype=float)
        distance = abs((baseline[0] * (point[1] - start[1])) - (baseline[1] * (point[0] - start[0]))) / norm
        interior_distances.append(float(distance))
        interior_ks.append(int(x[index]))
    if not interior_distances:
        return None
    return interior_ks[int(np.argmax(interior_distances))]


def _feature_phrase(column_name: str, delta_score: float) -> str:
    meta = FEATURE_METADATA.get(column_name, {})
    if delta_score >= 0:
        return meta.get("high_phrase", column_name)
    return meta.get("low_phrase", column_name)


def _format_feature_value(column_name: str, value: Any) -> str:
    if column_name.startswith("Доля") or column_name.startswith("Покрытие"):
        return _format_percent(float(value))
    return _format_number(value, 2)
