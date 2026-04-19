from __future__ import annotations

import math
from itertools import combinations
from typing import Any, Dict, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering, Birch, KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import adjusted_rand_score
from sklearn.preprocessing import StandardScaler

from .constants import (
    CLUSTER_COUNT_OPTIONS,
    LOG_SCALE_FEATURES,
    MODEL_N_INIT,
    RATE_SMOOTHING_PRIOR_STRENGTH,
    STABILITY_RANDOM_SEEDS,
    STABILITY_RESAMPLE_RATIO,
    WEIGHTING_STRATEGY_INCIDENT_LOG,
    WEIGHTING_STRATEGY_NOT_APPLICABLE,
    WEIGHTING_STRATEGY_UNIFORM,
)


def _derive_feature_weights_from_profiles(
    cluster_profiles: dict[int, dict[str, float]],
    candidate_features: list[str],
) -> dict[str, float]:
    cv_by_feature: dict[str, float] = {}
    for feature_name in candidate_features:
        values = np.asarray(
            [
                float((profile or {}).get(feature_name, 0.0) or 0.0)
                for profile in cluster_profiles.values()
            ],
            dtype=float,
        )
        if values.size == 0:
            cv_by_feature[feature_name] = 0.0
            continue
        mean_value = float(np.mean(values))
        std_value = float(np.std(values))
        if not math.isfinite(mean_value) or not math.isfinite(std_value) or mean_value == 0.0:
            cv_by_feature[feature_name] = 0.0
            continue
        cv = std_value / abs(mean_value)
        cv_by_feature[feature_name] = float(cv) if math.isfinite(cv) and cv > 0.0 else 0.0

    total_cv = float(sum(cv_by_feature.values()))
    if total_cv <= 0.0:
        return {feature_name: 0.0 for feature_name in candidate_features}
    return {
        feature_name: float(cv_by_feature.get(feature_name, 0.0) / total_cv)
        for feature_name in candidate_features
    }


def compute_cluster_risk_scores(
    cluster_profiles: Dict[int, Dict[str, float]],
    feature_weights: Dict[str, float] | None = None,
) -> list[Dict[str, Any]]:
    if not cluster_profiles:
        return []

    if feature_weights is None:
        inferred_features: list[str] = []
        for profile in cluster_profiles.values():
            for feature_name, feature_value in (profile or {}).items():
                try:
                    float(feature_value)
                except (TypeError, ValueError):
                    continue
                if feature_name not in inferred_features:
                    inferred_features.append(str(feature_name))
        weights = _derive_feature_weights_from_profiles(cluster_profiles, inferred_features)
    else:
        weights = dict(feature_weights)
    weighted_features = [
        feature
        for feature, weight in weights.items()
        if float(weight) > 0 and any(feature in (profile or {}) for profile in cluster_profiles.values())
    ]
    if not weighted_features:
        return []

    feature_ranges: Dict[str, tuple[float, float]] = {}
    for feature_name in weighted_features:
        values = [
            float((profile or {}).get(feature_name, 0.0) or 0.0)
            for profile in cluster_profiles.values()
        ]
        if not values:
            feature_ranges[feature_name] = (0.0, 0.0)
            continue
        feature_ranges[feature_name] = (min(values), max(values))

    effective_weight_sum = float(
        sum(float(weights[feature_name]) for feature_name in weighted_features),
    )
    if effective_weight_sum <= 0:
        return []

    risk_rows: list[Dict[str, Any]] = []
    for cluster_id in sorted(cluster_profiles.keys()):
        profile = cluster_profiles.get(cluster_id) or {}
        weighted_score = 0.0
        for feature_name in weighted_features:
            weight = float(weights.get(feature_name, 0.0) or 0.0)
            raw_value = float(profile.get(feature_name, 0.0) or 0.0)
            min_value, max_value = feature_ranges[feature_name]
            normalized_value = 0.0
            if max_value > min_value:
                normalized_value = (raw_value - min_value) / (max_value - min_value)
            normalized_value = float(np.clip(normalized_value, 0.0, 1.0))
            weighted_score += normalized_value * weight

        risk_score = float(np.clip(weighted_score / effective_weight_sum, 0.0, 1.0))
        if risk_score > 0.65:
            risk_level = "Высокий"
        elif risk_score > 0.35:
            risk_level = "Средний"
        else:
            risk_level = "Низкий"
        risk_rows.append(
            {
                "cluster_id": int(cluster_id),
                "risk_score": round(risk_score, 4),
                "risk_level": risk_level,
            }
        )
    return risk_rows


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


def _compute_hopkins_statistic(
    scaled_points: np.ndarray,
    sample_size: int | None = None,
    random_state: int = 42,
) -> float | None:
    points = np.asarray(scaled_points, dtype=float)
    if points.ndim != 2:
        return None
    row_count, feature_count = points.shape
    if row_count < 10 or feature_count <= 0:
        return None

    if sample_size is None:
        m = min(row_count // 10, 50)
    else:
        m = min(max(int(sample_size), 1), row_count)
    if m <= 0:
        return None

    rng = np.random.default_rng(random_state)
    sampled_indexes = rng.choice(row_count, size=m, replace=False)
    sampled_points = points[sampled_indexes]

    data_min = np.min(points, axis=0)
    data_max = np.max(points, axis=0)
    uniform_points = rng.uniform(data_min, data_max, size=(m, feature_count))

    u_distances = np.empty(m, dtype=float)
    w_distances = np.empty(m, dtype=float)
    for index in range(m):
        deltas_u = points - uniform_points[index]
        u_distances[index] = np.min(np.linalg.norm(deltas_u, axis=1))

        deltas_w = points - sampled_points[index]
        distances_w = np.linalg.norm(deltas_w, axis=1)
        distances_w[sampled_indexes[index]] = np.inf
        w_distances[index] = np.min(distances_w)

    exponent = feature_count
    u_power_sum = float(np.sum(np.power(u_distances, exponent)))
    w_power_sum = float(np.sum(np.power(w_distances, exponent)))
    denominator = u_power_sum + w_power_sum
    if denominator <= 0:
        return None
    return float(np.clip(u_power_sum / denominator, 0.0, 1.0))


def _compute_pca_projection(
    scaled_points: np.ndarray,
    labels: np.ndarray,
    cluster_labels: list[str],
    *,
    projected: np.ndarray | None = None,
    explained_variance: list[float] | None = None,
) -> dict[str, list]:
    points = np.asarray(scaled_points, dtype=float)
    point_labels = np.asarray(labels)
    if points.ndim != 2 or points.shape[0] == 0:
        return {"points": [], "explained_variance": [0.0, 0.0]}

    projection = np.asarray(projected, dtype=float) if projected is not None else None
    explained = [float(item) for item in explained_variance] if explained_variance is not None else None
    if projection is None or explained is None:
        pca = PCA(n_components=2)
        projection = pca.fit_transform(points)
        explained = [float(item) for item in pca.explained_variance_ratio_[:2]]
    while len(explained) < 2:
        explained.append(0.0)

    rows: list[dict] = []
    for index, (x_value, y_value) in enumerate(projection):
        cluster_id = int(point_labels[index]) if index < len(point_labels) else 0
        cluster_label = (
            cluster_labels[cluster_id]
            if 0 <= cluster_id < len(cluster_labels)
            else f"Тип {cluster_id + 1}"
        )
        rows.append(
            {
                "x": float(x_value),
                "y": float(y_value),
                "cluster_id": cluster_id,
                "cluster_label": str(cluster_label),
            }
        )
    return {"points": rows, "explained_variance": explained}


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


def _compute_gap_statistic(
    scaled_points: np.ndarray,
    sample_weights: np.ndarray,
    k_range: Sequence[int],
    n_references: int = 10,
    random_state: int = 42,
) -> dict[int, float]:
    points = np.asarray(scaled_points, dtype=float)
    weights = np.asarray(sample_weights, dtype=float)
    if points.ndim != 2 or len(points) == 0:
        return {}
    if weights.shape[0] != points.shape[0]:
        weights = np.ones(points.shape[0], dtype=float)

    row_count, feature_count = points.shape
    valid_ks = sorted({int(k) for k in k_range if 1 < int(k) <= row_count})
    if not valid_ks:
        return {}

    refs_count = max(int(n_references), 1)
    rng = np.random.default_rng(random_state)
    data_min = np.min(points, axis=0)
    data_max = np.max(points, axis=0)

    gap_scores: dict[int, float] = {}
    for cluster_count in valid_ks:
        model = KMeans(n_clusters=cluster_count, random_state=random_state, n_init=10)
        model.fit(points, sample_weight=weights)
        observed_inertia = max(float(model.inertia_), 1e-12)

        ref_logs: list[float] = []
        for reference_index in range(refs_count):
            reference_points = rng.uniform(data_min, data_max, size=(row_count, feature_count))
            reference_model = KMeans(
                n_clusters=cluster_count,
                random_state=random_state + reference_index + 1,
                n_init=10,
            )
            reference_model.fit(reference_points, sample_weight=np.ones(row_count, dtype=float))
            reference_inertia = max(float(reference_model.inertia_), 1e-12)
            ref_logs.append(float(np.log(reference_inertia)))

        gap_scores[cluster_count] = float(np.mean(ref_logs) - np.log(observed_inertia))
    return gap_scores


def _estimate_best_k_gap(gap_scores: dict[int, float]) -> int | None:
    if not gap_scores:
        return None
    ordered_ks = sorted(int(k) for k in gap_scores)
    if len(ordered_ks) < 2:
        return None

    gap_values = np.asarray([float(gap_scores[k]) for k in ordered_ks], dtype=float)
    if gap_values.size < 2:
        return None
    std_gap = float(np.std(gap_values, ddof=1)) if gap_values.size > 1 else 0.0

    for index in range(len(ordered_ks) - 1):
        current_gap = float(gap_scores[ordered_ks[index]])
        next_gap = float(gap_scores[ordered_ks[index + 1]])
        if current_gap >= (next_gap - std_gap):
            return ordered_ks[index]
    return None


def _estimate_kmeans_initialization_stability(
    scaled_points: np.ndarray,
    cluster_count: int,
    sample_weights: np.ndarray,
) -> float | None:
    labels_per_seed: list[np.ndarray] = []
    for seed in STABILITY_RANDOM_SEEDS:
        model = _fit_weighted_kmeans(
            scaled_points,
            sample_weights,
            cluster_count,
            random_state=seed,
            n_init=MODEL_N_INIT,
        )
        labels_per_seed.append(model.labels_)
    pair_scores: list[float] = []
    for left_labels, right_labels in combinations(labels_per_seed, 2):
        pair_scores.append(float(adjusted_rand_score(left_labels, right_labels)))
    if not pair_scores:
        return None
    return float(np.mean(pair_scores))


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
                n_init=MODEL_N_INIT,
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

    x_min = float(np.min(x))
    x_max = float(np.max(x))
    x_range = x_max - x_min
    x_norm = (x - x_min) / x_range if x_range > 0 else np.zeros_like(x)

    y_min = float(np.min(y))
    y_max = float(np.max(y))
    y_range = y_max - y_min
    y_norm = (y - y_min) / y_range if y_range > 0 else np.zeros_like(y)

    start = np.array([x_norm[0], y_norm[0]], dtype=float)
    end = np.array([x_norm[-1], y_norm[-1]], dtype=float)
    baseline = end - start
    norm = np.linalg.norm(baseline)
    if norm == 0:
        return int(x[0])

    interior_distances = []
    interior_ks = []
    for index in range(1, len(rows) - 1):
        point = np.array([x_norm[index], y_norm[index]], dtype=float)
        distance = abs((baseline[0] * (point[1] - start[1])) - (baseline[1] * (point[0] - start[0]))) / norm
        interior_distances.append(float(distance))
        interior_ks.append(int(x[index]))
    if not interior_distances:
        return None
    return interior_ks[int(np.argmax(interior_distances))]
