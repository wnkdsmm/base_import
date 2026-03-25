from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering, Birch, KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import adjusted_rand_score, silhouette_score
from sklearn.preprocessing import StandardScaler

from .constants import CARD_TONES, FEATURE_METADATA, LOG_SCALE_FEATURES, MAX_K_DIAGNOSTICS
from app.services.model_quality import compute_clustering_metrics
from .utils import _format_integer, _format_number, _format_percent



def _run_clustering(cluster_frame: pd.DataFrame, cluster_count: int) -> Dict[str, Any]:
    model_frame, transformed_columns = _prepare_model_frame(cluster_frame)
    scaler = StandardScaler()
    scaled_points = scaler.fit_transform(model_frame.to_numpy(dtype=float))

    model = KMeans(n_clusters=cluster_count, random_state=42, n_init=40)
    labels = model.fit_predict(scaled_points)
    metrics = compute_clustering_metrics(scaled_points, labels)
    stability_ari = _estimate_kmeans_stability(scaled_points, cluster_count)

    pca = PCA(n_components=2)
    pca_points = pca.fit_transform(scaled_points)
    transformed_centers = scaler.inverse_transform(model.cluster_centers_)
    raw_centers = _restore_raw_centers(transformed_centers, cluster_frame.columns, transformed_columns)

    return {
        "labels": labels,
        "scaled_points": scaled_points,
        "scaled_centers": model.cluster_centers_,
        "raw_centers": raw_centers,
        "silhouette": metrics.get("silhouette"),
        "davies_bouldin": metrics.get("davies_bouldin"),
        "calinski_harabasz": metrics.get("calinski_harabasz"),
        "cluster_balance_ratio": metrics.get("cluster_balance_ratio"),
        "smallest_cluster_size": metrics.get("smallest_cluster_size"),
        "largest_cluster_size": metrics.get("largest_cluster_size"),
        "stability_ari": stability_ari,
        "inertia": float(model.inertia_),
        "pca_points": pca_points,
        "explained_variance": float(np.sum(pca.explained_variance_ratio_)),
    }


def _evaluate_cluster_counts(cluster_frame: pd.DataFrame) -> Dict[str, Any]:
    if len(cluster_frame) < 3:
        return {"rows": [], "best_silhouette_k": None, "elbow_k": None}

    model_frame, _ = _prepare_model_frame(cluster_frame)
    scaler = StandardScaler()
    scaled_points = scaler.fit_transform(model_frame.to_numpy(dtype=float))
    max_k = min(MAX_K_DIAGNOSTICS, len(cluster_frame) - 1)

    rows: List[Dict[str, Any]] = []
    for cluster_count in range(2, max_k + 1):
        model = KMeans(n_clusters=cluster_count, random_state=42, n_init=30)
        labels = model.fit_predict(scaled_points)
        silhouette = None
        if len(set(labels)) > 1 and len(cluster_frame) > cluster_count:
            silhouette = float(silhouette_score(scaled_points, labels))
        rows.append(
            {
                "cluster_count": cluster_count,
                "inertia": float(model.inertia_),
                "silhouette": silhouette,
            }
        )

    best_silhouette_row = None
    scored_rows = [item for item in rows if item["silhouette"] is not None]
    if scored_rows:
        best_silhouette_row = max(scored_rows, key=lambda item: (item["silhouette"], -item["cluster_count"]))

    return {
        "rows": rows,
        "best_silhouette_k": best_silhouette_row["cluster_count"] if best_silhouette_row else None,
        "elbow_k": _estimate_elbow_k(rows),
    }




def _estimate_kmeans_stability(scaled_points: np.ndarray, cluster_count: int) -> float | None:
    seeds = [7, 21, 42, 84]
    reference = None
    scores: List[float] = []
    for seed in seeds:
        labels = KMeans(n_clusters=cluster_count, random_state=seed, n_init=25).fit_predict(scaled_points)
        if reference is None:
            reference = labels
            continue
        scores.append(float(adjusted_rand_score(reference, labels)))
    if not scores:
        return None
    return float(np.mean(scores))


def _compare_clustering_methods(cluster_frame: pd.DataFrame, cluster_count: int) -> List[Dict[str, Any]]:
    model_frame, _ = _prepare_model_frame(cluster_frame)
    scaler = StandardScaler()
    scaled_points = scaler.fit_transform(model_frame.to_numpy(dtype=float))
    rows: List[Dict[str, Any]] = []
    methods = [
        ("kmeans", "KMeans", lambda: KMeans(n_clusters=cluster_count, random_state=42, n_init=40).fit_predict(scaled_points)),
        ("agglomerative", "Агломеративная кластеризация (Ward)", lambda: AgglomerativeClustering(n_clusters=cluster_count, linkage='ward').fit_predict(scaled_points)),
        ("birch", "Birch", lambda: Birch(n_clusters=cluster_count).fit_predict(scaled_points)),
    ]
    for method_key, method_label, runner in methods:
        try:
            labels = runner()
        except Exception:
            continue
        metrics = compute_clustering_metrics(scaled_points, labels)
        rows.append(
            {
                "method_key": method_key,
                "method_label": method_label,
                "is_selected": method_key == "kmeans",
                "silhouette": metrics.get("silhouette"),
                "davies_bouldin": metrics.get("davies_bouldin"),
                "calinski_harabasz": metrics.get("calinski_harabasz"),
                "cluster_balance_ratio": metrics.get("cluster_balance_ratio"),
            }
        )
    return rows

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
) -> List[str]:
    notes = [
        f"Кластеризация построена по { _format_integer(sampled_entities) } территориям/населённым пунктам, агрегированным из { _format_integer(total_incidents) } пожаров.",
    ]
    if total_entities > sampled_entities:
        notes.append(
            f"До кластеризации были собраны агрегаты по { _format_integer(total_entities) } территориям, а затем отобрана подвыборка без смещения по первым строкам таблицы."
        )

    if silhouette is None:
        notes.append("Коэффициент силуэта не рассчитан: для этого нужно больше территорий, чем кластеров, и хотя бы две непустые группы.")
    elif silhouette < 0.2:
        notes.append("Кластеры отделены слабо: вероятно, профиль территорий плавный и стоит попробовать меньшее число кластеров или уже другой набор признаков.")
    elif silhouette < 0.4:
        notes.append("Разделение умеренное: типы территорий уже читаются, но между ними остаются заметные переходные зоны.")
    else:
        notes.append("Кластеры отделены достаточно чётко для исследовательского анализа типов территорий риска.")

    best_silhouette_k = diagnostics.get("best_silhouette_k")
    elbow_k = diagnostics.get("elbow_k")
    if best_silhouette_k and elbow_k and best_silhouette_k != elbow_k:
        notes.append(
            f"По silhouette лучше всего выглядит k={best_silhouette_k}, а elbow даёт сгиб около k={elbow_k}; это хороший рабочий диапазон для сравнения интерпретаций."
        )
    elif best_silhouette_k:
        notes.append(f"И коэффициент силуэта, и структура инерции поддерживают около {best_silhouette_k} кластеров как разумный ориентир.")
    elif elbow_k:
        notes.append(f"По кривой inertia заметный сгиб начинается около k={elbow_k} кластеров.")

    notes.append(f"В расчёте участвовали признаки: {', '.join(selected_features)}.")
    if cluster_profiles:
        largest = max(cluster_profiles, key=lambda item: int(item["size_display"].replace(" ", "")))
        notes.append(
            f"Самый крупный тип территорий сейчас — {largest['cluster_label']} ({largest['share_display']} выборки): {largest['segment_title'].lower()}."
        )
    notes.append(
        "Сначала считаются агрегаты территории, затем признаки приводятся к сопоставимому масштабу, поэтому алгоритм k-means выделяет типы территорий риска, а не случайные группы отдельных карточек пожара."
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
        distance = abs(np.cross(baseline, point - start) / norm)
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
