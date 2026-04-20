from __future__ import annotations

import math
from typing import Sequence

import numpy as np
from sklearn.metrics import (
    brier_score_loss,
    calinski_harabasz_score,
    davies_bouldin_score,
    f1_score,
    roc_auc_score,
    silhouette_score,
)

from config.constants import MIN_POSITIVE_PREDICTION


def _as_float_array(values: Sequence[float]) -> np.ndarray:
    return np.asarray(list(values), dtype=float)


def _has_both_classes(actual: np.ndarray) -> bool:
    return np.unique(actual).size > 1


def relative_delta(value: float | None, baseline: float | None) -> float | None:
    if value is None or baseline is None or baseline == 0:
        return None
    return float((float(value) - float(baseline)) / float(baseline))


def smape(actuals: Sequence[float], predictions: Sequence[float]) -> float:
    actual = _as_float_array(actuals)
    predicted = _as_float_array(predictions)
    denominator = np.abs(actual) + np.abs(predicted)
    safe_denominator = np.where(denominator <= MIN_POSITIVE_PREDICTION, 1.0, denominator)
    values = 2.0 * np.abs(predicted - actual) / safe_denominator
    values = np.where(denominator <= MIN_POSITIVE_PREDICTION, 0.0, values)
    return float(np.mean(values) * 100.0)


def mean_poisson_deviance(actuals: Sequence[float], predictions: Sequence[float]) -> float:
    actual = _as_float_array(actuals)
    predicted = np.clip(_as_float_array(predictions), MIN_POSITIVE_PREDICTION, None)
    ratio_term = np.zeros_like(actual, dtype=float)
    positive_mask = actual > 0.0
    ratio_term[positive_mask] = actual[positive_mask] * np.log(actual[positive_mask] / predicted[positive_mask])
    deviance = 2.0 * np.mean(ratio_term - (actual - predicted))
    return float(max(0.0, deviance))


def compute_count_metrics(
    actuals: Sequence[float],
    predictions: Sequence[float],
    baseline_metrics: dict[str, float | None] | None = None,
) -> dict[str, float | None]:
    actual = _as_float_array(actuals)
    predicted = _as_float_array(predictions)
    residuals = predicted - actual
    mae = float(np.mean(np.abs(residuals)))
    rmse = float(math.sqrt(np.mean(residuals ** 2)))
    smape_value = smape(actual, predicted)
    poisson = mean_poisson_deviance(actual, predicted)

    baseline = baseline_metrics or {}
    return {
        'mae': mae,
        'rmse': rmse,
        'smape': smape_value,
        'poisson_deviance': poisson,
        'mae_delta_vs_baseline': relative_delta(mae, baseline.get('mae')),
        'rmse_delta_vs_baseline': relative_delta(rmse, baseline.get('rmse')),
        'smape_delta_vs_baseline': relative_delta(smape_value, baseline.get('smape')),
    }


def compute_classification_metrics(
    actuals: Sequence[int],
    probabilities: Sequence[float],
    baseline_probabilities: Sequence[float | None] = None,
    threshold: float = 0.5,
) -> dict[str, float | None]:
    actual = np.asarray(list(actuals), dtype=int)
    predicted_probabilities = np.clip(_as_float_array(probabilities), 0.001, 0.999)
    baseline_values = (
        np.clip(_as_float_array(baseline_probabilities), 0.001, 0.999)
        if baseline_probabilities is not None
        else None
    )
    if actual.size == 0:
        return {
            'available': False,
            'brier_score': None,
            'baseline_brier_score': None,
            'roc_auc': None,
            'f1': None,
            'baseline_f1': None,
        }
    if not _has_both_classes(actual):
        return {
            'available': False,
            'brier_score': None,
            'baseline_brier_score': None,
            'roc_auc': None,
            'f1': None,
            'baseline_f1': None,
        }

    predicted_labels = (predicted_probabilities >= float(threshold)).astype(int)
    roc_auc = None
    if _has_both_classes(actual):
        roc_auc = float(roc_auc_score(actual, predicted_probabilities))

    baseline_brier = None
    baseline_f1 = None
    if baseline_values is not None:
        baseline_brier = float(brier_score_loss(actual, baseline_values))
        baseline_f1 = float(f1_score(actual, (baseline_values >= float(threshold)).astype(int), zero_division=0))

    return {
        'available': True,
        'brier_score': float(brier_score_loss(actual, predicted_probabilities)),
        'baseline_brier_score': baseline_brier,
        'roc_auc': roc_auc,
        'f1': float(f1_score(actual, predicted_labels, zero_division=0)),
        'baseline_f1': baseline_f1,
    }


def compute_clustering_metrics(points: np.ndarray, labels: Sequence[int]) -> dict[str, float | None]:
    label_array = np.asarray(list(labels), dtype=int)
    unique_labels = np.unique(label_array)
    cluster_sizes = [int(np.sum(label_array == label)) for label in unique_labels]
    largest_cluster = max(cluster_sizes) if cluster_sizes else 0
    smallest_cluster = min(cluster_sizes) if cluster_sizes else 0
    balance_ratio = None
    if largest_cluster > 0:
        balance_ratio = float(smallest_cluster / largest_cluster)

    if len(unique_labels) < 2 or len(points) <= len(unique_labels):
        return {
            'silhouette': None,
            'davies_bouldin': None,
            'calinski_harabasz': None,
            'cluster_balance_ratio': balance_ratio,
            'smallest_cluster_size': float(smallest_cluster) if cluster_sizes else None,
            'largest_cluster_size': float(largest_cluster) if cluster_sizes else None,
        }

    return {
        'silhouette': float(silhouette_score(points, label_array)),
        'davies_bouldin': float(davies_bouldin_score(points, label_array)),
        'calinski_harabasz': float(calinski_harabasz_score(points, label_array)),
        'cluster_balance_ratio': balance_ratio,
        'smallest_cluster_size': float(smallest_cluster),
        'largest_cluster_size': float(largest_cluster),
    }
