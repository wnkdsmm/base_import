from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Sequence, Tuple

from app.plotly_bundle import get_plotly_bundle
from app.cache import CopyingTtlCache
from app.services.charting import build_empty_chart_bundle as _empty_chart_bundle

from .constants import (
    CLUSTER_COUNT_OPTIONS,
    SAMPLE_LIMIT_OPTIONS,
    SAMPLING_STRATEGY_OPTIONS,
)
from .data import (
    _build_table_options,
    _parse_cluster_count,
    _parse_sample_limit,
    _parse_sampling_strategy,
    _resolve_selected_table,
)
from .quality import _empty_clustering_quality_assessment
from .utils import _format_datetime, _format_integer

_CLUSTERING_CACHE = CopyingTtlCache(ttl_seconds=120.0)

def clear_clustering_cache() -> None:
    _CLUSTERING_CACHE.clear()

def _normalize_clustering_cache_value(value: str) -> str:
    return str(value or "").strip()

def _build_clustering_cache_key(
    selected_table: str,
    cluster_count: int,
    sample_limit: int,
    sampling_strategy: str,
    feature_columns: Sequence[str] | None,
    cluster_count_is_explicit: bool,
) -> Tuple[str, ...]:
    return (
        selected_table,
        str(cluster_count),
        str(sample_limit),
        _normalize_clustering_cache_value(sampling_strategy),
        "manual_k" if cluster_count_is_explicit else "auto_k",
        *tuple(str(item).strip() for item in (feature_columns or []) if str(item).strip()),
    )

def _normalize_feature_columns(feature_columns: Sequence[str] | None) -> List[str]:
    return [str(item).strip() for item in (feature_columns or []) if str(item).strip()]

def _build_clustering_request_state(
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: Sequence[str] | None = None,
    cluster_count_is_explicit: bool = False,
) -> Dict[str, Any]:
    table_options = _build_table_options()
    selected_table = _resolve_selected_table(table_options, table_name)
    requested_cluster_count = _parse_cluster_count(cluster_count)
    requested_sample_limit = _parse_sample_limit(sample_limit)
    selected_sampling_strategy = _parse_sampling_strategy(sampling_strategy)
    normalized_feature_columns = _normalize_feature_columns(feature_columns)
    cache_key = _build_clustering_cache_key(
        selected_table=selected_table,
        cluster_count=requested_cluster_count,
        sample_limit=requested_sample_limit,
        sampling_strategy=selected_sampling_strategy,
        feature_columns=normalized_feature_columns,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    return {
        "table_options": table_options,
        "selected_table": selected_table,
        "cluster_count": requested_cluster_count,
        "sample_limit": requested_sample_limit,
        "sampling_strategy": selected_sampling_strategy,
        "feature_columns": normalized_feature_columns,
        "cluster_count_is_explicit": bool(cluster_count_is_explicit),
        "cache_key": cache_key,
    }

def get_clustering_page_context(
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: Sequence[str] | None = None,
    cluster_count_is_explicit: bool = False,
) -> Dict[str, Any]:
    from .core_runner import get_clustering_data

    initial_data = get_clustering_data(
        table_name=table_name,
        cluster_count=cluster_count,
        sample_limit=sample_limit,
        sampling_strategy=sampling_strategy,
        feature_columns=feature_columns,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    return {
        "generated_at": _format_datetime(datetime.now()),
        "initial_data": initial_data,
        "plotly_js": get_plotly_bundle(),
        "has_data": bool(initial_data["filters"]["available_tables"]),
    }

def get_clustering_shell_context(
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: Sequence[str] | None = None,
    cluster_count_is_explicit: bool = False,
) -> Dict[str, Any]:
    table_options = _build_table_options()
    selected_table = _resolve_selected_table(table_options, table_name)
    requested_cluster_count = _parse_cluster_count(cluster_count)
    requested_sample_limit = _parse_sample_limit(sample_limit)
    selected_sampling_strategy = _parse_sampling_strategy(sampling_strategy)
    initial_data = _empty_clustering_data(
        table_options=table_options,
        selected_table=selected_table,
        cluster_count=requested_cluster_count,
        sample_limit=requested_sample_limit,
        sampling_strategy=selected_sampling_strategy,
    )
    initial_data["bootstrap_mode"] = "deferred"
    if feature_columns:
        initial_data["filters"]["feature_columns"] = [str(item).strip() for item in feature_columns if str(item).strip()]
    return {
        "generated_at": _format_datetime(datetime.now()),
        "initial_data": initial_data,
        "plotly_js": "",
        "has_data": bool(initial_data["filters"]["available_tables"]),
    }

def _empty_clustering_data(
    table_options: List[Dict[str, str]],
    selected_table: str,
    cluster_count: int,
    sample_limit: int,
    sampling_strategy: str,
) -> Dict[str, Any]:
    return {
        "generated_at": _format_datetime(datetime.now()),
        "has_data": False,
        "model_description": "",
        "summary": {
            "selected_table_label": selected_table or "Нет таблицы",
            "total_incidents_display": "0",
            "total_entities_display": "0",
            "sampled_entities_display": "0",
            "clustered_entities_display": "0",
            "excluded_entities_display": "0",
            "candidate_features_display": "0",
            "selected_features_display": "0",
            "cluster_count_display": _format_integer(cluster_count),
            "cluster_count_requested_display": _format_integer(cluster_count),
            "cluster_count_note": f"Сейчас основной вывод показан для k={_format_integer(cluster_count)}.",
            "suggested_cluster_count_label": "Рекомендуемый k",
            "suggested_cluster_count_display": "—",
            "suggested_cluster_count_note": "Диагностика k появится, когда хватит данных для сравнения нескольких вариантов.",
            "elbow_cluster_count_display": "—",
            "silhouette_display": "—",
            "pca_variance_display": "0%",
            "inertia_display": "0",
            "sampling_strategy_label": next(
                (item["label"] for item in SAMPLING_STRATEGY_OPTIONS if item["value"] == sampling_strategy),
                SAMPLING_STRATEGY_OPTIONS[0]["label"],
            ),
        },
        "quality_assessment": _empty_clustering_quality_assessment(),
        "cluster_profiles": [],
        "centroid_columns": [],
        "centroid_rows": [],
        "representative_columns": [],
        "representative_rows": [],
        "charts": {
            "scatter": _empty_chart_bundle(
                "Кластеры территорий на двумерной проекции",
                "Недостаточно данных, чтобы показать типы территорий на проекции главных компонент.",
            ),
            "distribution": _empty_chart_bundle(
                "Размеры кластеров по числу территорий",
                "Распределение территорий по типам появится после расчёта.",
            ),
            "diagnostics": _empty_chart_bundle(
                "Подсказка по числу кластеров",
                "Диагностика k появится, когда хватит территорий для сравнения нескольких вариантов.",
            ),
        },
        "notes": [],
        "filters": {
            "table_name": selected_table,
            "cluster_count": str(cluster_count),
            "sample_limit": str(sample_limit),
            "sampling_strategy": sampling_strategy,
            "feature_columns": [],
            "available_tables": table_options,
            "available_cluster_counts": [
                {"value": str(item), "label": f"{item} кластера" if item < 5 else f"{item} кластеров"}
                for item in CLUSTER_COUNT_OPTIONS
            ],
            "available_sample_limits": [
                {"value": str(item), "label": f"до {item} территорий"} for item in SAMPLE_LIMIT_OPTIONS
            ],
            "available_sampling_strategies": SAMPLING_STRATEGY_OPTIONS,
            "available_features": [],
        },
    }

