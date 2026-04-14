from __future__ import annotations

import math
from itertools import combinations
from typing import Any, Dict, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering, Birch, KMeans
from sklearn.metrics import adjusted_rand_score
from sklearn.preprocessing import StandardScaler

from .constants import (
    CLUSTER_COUNT_OPTIONS,
    LOG_SCALE_FEATURES,
    RATE_SMOOTHING_PRIOR_STRENGTH,
    STABILITY_RANDOM_SEEDS,
    STABILITY_RESAMPLE_RATIO,
    WEIGHTING_STRATEGY_INCIDENT_LOG,
    WEIGHTING_STRATEGY_NOT_APPLICABLE,
    WEIGHTING_STRATEGY_UNIFORM,
)


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
        or "Р§РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ" not in entity_frame.columns
    ):
        return np.ones(len(entity_frame), dtype=float)
    counts = pd.to_numeric(entity_frame["Р§РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ"], errors="coerce").fillna(1.0).clip(lower=1.0).to_numpy(dtype=float)
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
    raw_centers: list[np.ndarray] = []
    scaled_centers: list[np.ndarray] = []
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
    scores: list[float] = []
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
    resampled_models: list[dict[str, Any]] = []
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

    pair_scores: list[float] = []
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


def _estimate_elbow_k(rows: Sequence[dict[str, Any]]) -> int | None:
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
