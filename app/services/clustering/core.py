from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime

from app.perf import current_perf_trace, profiled
from app.plotly_bundle import get_plotly_bundle
from app.runtime_cache import CopyingTtlCache
from typing import Any, Callable, Dict, List, Sequence, Tuple
from config.db import engine

from .analysis import (
    _build_centroid_table,
    _build_cluster_profiles,
    _build_notes,
    _build_representative_rows,
    _compare_clustering_methods,
    _cluster_labels,
    _evaluate_cluster_counts,
    _run_clustering,
)
from .charts import _build_diagnostics_chart, _build_distribution_chart, _build_scatter_chart, _empty_chart_bundle
from .constants import CLUSTER_COUNT_OPTIONS, MIN_ROWS_PER_CLUSTER, SAMPLE_LIMIT_OPTIONS, SAMPLING_STRATEGY_OPTIONS
from .data import (
    _build_feature_options,
    _build_table_options,
    _load_territory_dataset,
    _parse_cluster_count,
    _parse_sample_limit,
    _parse_sampling_strategy,
    _prepare_cluster_frame,
    _resolve_selected_features,
    _resolve_selected_table,
)
from .utils import _format_datetime, _format_integer, _format_number, _format_percent

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
) -> Tuple[str, ...]:
    return (
        selected_table,
        str(cluster_count),
        str(sample_limit),
        _normalize_clustering_cache_value(sampling_strategy),
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
    )
    return {
        "table_options": table_options,
        "selected_table": selected_table,
        "cluster_count": requested_cluster_count,
        "sample_limit": requested_sample_limit,
        "sampling_strategy": selected_sampling_strategy,
        "feature_columns": normalized_feature_columns,
        "cache_key": cache_key,
    }


def _emit_clustering_progress(
    progress_callback: Callable[[str, str], None] | None,
    phase: str,
    message: str,
) -> None:
    if progress_callback is None:
        return
    progress_callback(phase, message)


def get_clustering_page_context(
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: Sequence[str] | None = None,
) -> Dict[str, Any]:
    initial_data = get_clustering_data(
        table_name=table_name,
        cluster_count=cluster_count,
        sample_limit=sample_limit,
        sampling_strategy=sampling_strategy,
        feature_columns=feature_columns,
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
@profiled("clustering", engine=engine)
def get_clustering_data(
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: Sequence[str] | None = None,
    progress_callback: Callable[[str, str], None] | None = None,
) -> Dict[str, Any]:
    perf = current_perf_trace()
    request_state = _build_clustering_request_state(
        table_name=table_name,
        cluster_count=cluster_count,
        sample_limit=sample_limit,
        sampling_strategy=sampling_strategy,
        feature_columns=feature_columns,
    )
    table_options = request_state["table_options"]
    selected_table = request_state["selected_table"]
    requested_cluster_count = request_state["cluster_count"]
    requested_sample_limit = request_state["sample_limit"]
    selected_sampling_strategy = request_state["sampling_strategy"]
    normalized_feature_columns = request_state["feature_columns"]
    cache_key = request_state["cache_key"]
    if perf is not None:
        perf.update(
            requested_table=table_name,
            selected_table=selected_table,
            cluster_count=requested_cluster_count,
            sample_limit=requested_sample_limit,
            sampling_strategy=selected_sampling_strategy,
        )
    cached_payload = _CLUSTERING_CACHE.get(cache_key)
    if cached_payload is not None:
        if perf is not None:
            perf.update(cache_hit=True, payload_has_data=bool(cached_payload.get("has_data")))
        _emit_clustering_progress(
            progress_callback,
            "clustering.completed",
            "Кластеризация уже была рассчитана ранее и взята из кэша.",
        )
        return cached_payload

    if perf is not None:
        perf.update(cache_hit=False)
    base = _empty_clustering_data(
        table_options=table_options,
        selected_table=selected_table,
        cluster_count=requested_cluster_count,
        sample_limit=requested_sample_limit,
        sampling_strategy=selected_sampling_strategy,
    )
    if not selected_table:
        base["notes"].append("Выберите таблицу, чтобы сгруппировать территории по их пожарному профилю и типу риска.")
        _emit_clustering_progress(
            progress_callback,
            "clustering.completed",
            "Таблица не выбрана, поэтому расчет кластеризации не запускался.",
        )
        return _CLUSTERING_CACHE.set(cache_key, base)

    try:
        _emit_clustering_progress(
            progress_callback,
            "clustering.loading",
            "Загружаем территориальные данные и выбранные параметры кластеризации.",
        )
        aggregation_context = perf.span("aggregation") if perf is not None else nullcontext()
        with aggregation_context:
            dataset = _load_territory_dataset(selected_table, requested_sample_limit, selected_sampling_strategy)
            if perf is not None:
                perf.update(
                    input_rows=dataset["total_incidents"],
                    total_entities=dataset["total_entities"],
                    sampled_entities=dataset["sampled_entities"],
                )
    except Exception as exc:
        base["notes"].append(str(exc))
        return _CLUSTERING_CACHE.set(cache_key, base)

    _emit_clustering_progress(
        progress_callback,
        "clustering.aggregation",
        "Собираем агрегированные признаки территории и проверяем их заполненность.",
    )
    filter_prep_context = perf.span("filter_prep") if perf is not None else nullcontext()
    with filter_prep_context:
        sampling_strategy_label = next(
            (item["label"] for item in SAMPLING_STRATEGY_OPTIONS if item["value"] == selected_sampling_strategy),
            SAMPLING_STRATEGY_OPTIONS[0]["label"],
        )
        summary = base["summary"]
        summary["total_incidents_display"] = _format_integer(dataset["total_incidents"])
        summary["total_entities_display"] = _format_integer(dataset["total_entities"])
        summary["sampled_entities_display"] = _format_integer(dataset["sampled_entities"])
        summary["sampling_strategy_label"] = sampling_strategy_label

        candidate_features = dataset["candidate_features"]
        feature_names = [item["name"] for item in candidate_features]
        selected_features, selection_note = _resolve_selected_features(feature_names, normalized_feature_columns)
        base["filters"]["feature_columns"] = selected_features
        base["filters"]["available_features"] = _build_feature_options(candidate_features, selected_features)
        summary["candidate_features_display"] = _format_integer(len(candidate_features))
        summary["selected_features_display"] = _format_integer(len(selected_features))
        if perf is not None:
            perf.update(
                candidate_features=len(candidate_features),
                selected_features=len(selected_features),
            )

    base["notes"].extend(dataset["notes"])
    if selection_note:
        base["notes"].append(selection_note)
    if dataset["sampling_note"]:
        base["notes"].append(dataset["sampling_note"])

    if len(candidate_features) < 2:
        base["notes"].append("В таблице не хватило агрегированных признаков с вариативностью, чтобы стабильно кластеризовать территории.")
        _emit_clustering_progress(
            progress_callback,
            "clustering.completed",
            "После агрегации оказалось недостаточно вариативных признаков для устойчивой кластеризации.",
        )
        return _CLUSTERING_CACHE.set(cache_key, base)

    if len(selected_features) < 2:
        base["notes"].append("Для кластеризации нужно выбрать минимум два агрегированных признака территории.")
        _emit_clustering_progress(
            progress_callback,
            "clustering.completed",
            "Для расчета нужно минимум два агрегированных признака, поэтому результат остался в placeholder-режиме.",
        )
        return _CLUSTERING_CACHE.set(cache_key, base)

    _emit_clustering_progress(
        progress_callback,
        "clustering.training",
        "Строим кластеры, сравниваем алгоритмы и оцениваем устойчивость сегментации.",
    )
    clustering_run_context = perf.span("model_training") if perf is not None else nullcontext()
    with clustering_run_context:
        cluster_frame, entity_frame, excluded_entities = _prepare_cluster_frame(
            feature_frame=dataset["feature_frame"],
            entity_frame=dataset["entity_frame"],
            selected_features=selected_features,
        )
        summary["clustered_entities_display"] = _format_integer(len(cluster_frame))
        summary["excluded_entities_display"] = _format_integer(excluded_entities)

    if len(cluster_frame) < max(12, requested_cluster_count * MIN_ROWS_PER_CLUSTER):
        base["notes"].append("После отбора и заполнения пропусков осталось слишком мало территорий для устойчивой кластеризации.")
        _emit_clustering_progress(
            progress_callback,
            "clustering.completed",
            "После подготовки выборки осталось слишком мало территорий для устойчивой кластеризации.",
        )
        return _CLUSTERING_CACHE.set(cache_key, base)

    actual_cluster_count = min(max(CLUSTER_COUNT_OPTIONS[0], requested_cluster_count), len(cluster_frame) - 1)
    if actual_cluster_count != requested_cluster_count:
        base["notes"].append(
            f"Количество кластеров автоматически скорректировано до {actual_cluster_count}, чтобы в каждом типе территорий было достаточно наблюдений."
        )

    with (perf.span("model_training") if perf is not None else nullcontext()):
        diagnostics = _evaluate_cluster_counts(cluster_frame)
        clustering = _run_clustering(cluster_frame, actual_cluster_count)
        method_comparison = _compare_clustering_methods(cluster_frame, actual_cluster_count)
        labels = clustering["labels"]
        cluster_labels = _cluster_labels(actual_cluster_count)
        profiles = _build_cluster_profiles(
            cluster_frame=cluster_frame,
            entity_frame=entity_frame,
            labels=labels,
            raw_centers=clustering["raw_centers"],
            cluster_labels=cluster_labels,
        )
        centroid_columns, centroid_rows = _build_centroid_table(
            cluster_frame=cluster_frame,
            entity_frame=entity_frame,
            labels=labels,
            raw_centers=clustering["raw_centers"],
            cluster_labels=cluster_labels,
            cluster_profiles=profiles,
        )
        representative_columns, representative_rows = _build_representative_rows(
            cluster_frame=cluster_frame,
            entity_frame=entity_frame,
            labels=labels,
            scaled_points=clustering["scaled_points"],
            scaled_centers=clustering["scaled_centers"],
            cluster_labels=cluster_labels,
        )
    if perf is not None:
        perf.update(
            clustered_entities=len(cluster_frame),
            excluded_entities=excluded_entities,
            actual_cluster_count=actual_cluster_count,
        )

    summary["cluster_count_display"] = _format_integer(actual_cluster_count)
    summary["cluster_count_requested_display"] = _format_integer(requested_cluster_count)
    summary["suggested_cluster_count_display"] = (
        _format_integer(diagnostics["best_silhouette_k"]) if diagnostics.get("best_silhouette_k") else "—"
    )
    summary["elbow_cluster_count_display"] = _format_integer(diagnostics["elbow_k"]) if diagnostics.get("elbow_k") else "—"
    summary["silhouette_display"] = _format_number(clustering["silhouette"], 3) if clustering["silhouette"] is not None else "—"
    summary["pca_variance_display"] = _format_percent(clustering["explained_variance"])
    summary["inertia_display"] = _format_number(clustering["inertia"], 2)

    _emit_clustering_progress(
        progress_callback,
        "clustering.render",
        "Подготавливаем итоговые таблицы, профили кластеров и визуализации.",
    )
    with (perf.span("payload_render") if perf is not None else nullcontext()):
        payload = {
            **base,
            "has_data": True,
            "model_description": (
                "Кластеризация строится не по отдельным инцидентам, а по агрегированным территориям и населённым пунктам. "
                "Для каждой территории собирается профиль риска: частота пожаров, площадь, ночные инциденты, прибытие, тяжёлые последствия и подтверждённость водоснабжения."
            ),
            "summary": summary,
            "quality_assessment": _build_clustering_quality_assessment(clustering, method_comparison, actual_cluster_count, selected_features),
            "cluster_profiles": profiles,
            "centroid_columns": centroid_columns,
            "centroid_rows": centroid_rows,
            "representative_columns": representative_columns,
            "representative_rows": representative_rows,
            "charts": {
                "scatter": _build_scatter_chart(
                    pca_points=clustering["pca_points"],
                    labels=labels,
                    cluster_labels=cluster_labels,
                    cluster_frame=cluster_frame,
                    entity_frame=entity_frame,
                ),
                "distribution": _build_distribution_chart(
                    labels=labels,
                    cluster_labels=cluster_labels,
                    total_rows=len(cluster_frame),
                    entity_frame=entity_frame,
                ),
                "diagnostics": _build_diagnostics_chart(
                    rows=diagnostics["rows"],
                    requested_cluster_count=actual_cluster_count,
                    best_silhouette_k=diagnostics.get("best_silhouette_k"),
                    elbow_k=diagnostics.get("elbow_k"),
                ),
            },
        }
        payload["notes"].extend(
            _build_notes(
                cluster_profiles=profiles,
                silhouette=clustering["silhouette"],
                selected_features=selected_features,
                diagnostics=diagnostics,
                total_incidents=dataset["total_incidents"],
                total_entities=dataset["total_entities"],
                sampled_entities=dataset["sampled_entities"],
            )
        )
        if perf is not None:
            perf.update(
                payload_has_data=bool(payload.get("has_data")),
                payload_notes=len(payload.get("notes") or []),
            )
        result = _CLUSTERING_CACHE.set(cache_key, payload)
        _emit_clustering_progress(
            progress_callback,
            "clustering.completed",
            "Кластеризация завершена, результаты и графики готовы.",
        )
        return result


def _empty_clustering_quality_assessment() -> Dict[str, Any]:
    return {
        "ready": False,
        "title": "Оценка качества кластеризации",
        "subtitle": "После расчета здесь появятся внутренние метрики качества и сравнение алгоритмов.",
        "metric_cards": [],
        "methodology_items": [],
        "comparison_rows": [],
        "dissertation_points": ["Пока недостаточно данных для расчета метрик качества кластеризации."],
    }


def _build_clustering_quality_assessment(
    clustering: Dict[str, Any],
    method_comparison: Sequence[Dict[str, Any]],
    cluster_count: int,
    selected_features: Sequence[str],
) -> Dict[str, Any]:
    if clustering.get("silhouette") is None:
        payload = _empty_clustering_quality_assessment()
        payload["dissertation_points"] = ["В текущем срезе кластеризация построена, но внутренних метрик пока недостаточно для устойчивой интерпретации качества."]
        return payload

    comparison_rows = [
        {
            "method_label": row.get("method_label", "Метод"),
            "selection_label": "Основной метод" if row.get("is_selected") else "Сравнение",
            "silhouette_display": _format_number(row.get("silhouette"), 3),
            "davies_display": _format_number(row.get("davies_bouldin"), 3),
            "calinski_display": _format_number(row.get("calinski_harabasz"), 1),
            "balance_display": _format_percent(row.get("cluster_balance_ratio") or 0.0),
        }
        for row in method_comparison
    ]

    dissertation_points = [
        f"Основной алгоритм кластеризации — KMeans при k={_format_integer(cluster_count)}.",
        f"Для итоговой модели получены silhouette {_format_number(clustering.get('silhouette'), 3)}, индекс Дэвиса-Болдина {_format_number(clustering.get('davies_bouldin'), 3)} и индекс Калински-Харабаза {_format_number(clustering.get('calinski_harabasz'), 1)}.",
        f"Баланс кластеров составляет {_format_percent(clustering.get('cluster_balance_ratio') or 0.0)}, а устойчивость KMeans при нескольких инициализациях оценивается через ARI = {_format_number(clustering.get('stability_ari'), 3)}.",
        f"Сравнение с альтернативными алгоритмами выполнено на том же наборе признаков: {', '.join(selected_features)}.",
    ]

    return {
        "ready": True,
        "title": "Оценка качества кластеризации",
        "subtitle": "В блоке ниже собраны внутренние метрики качества и сравнительная таблица нескольких алгоритмов на одном наборе агрегированных признаков.",
        "metric_cards": [
            {"label": "Коэффициент силуэта", "value": _format_number(clustering.get("silhouette"), 3), "meta": "выше — лучше"},
            {"label": "Индекс Дэвиса-Болдина", "value": _format_number(clustering.get("davies_bouldin"), 3), "meta": "ниже - лучше"},
            {"label": "Индекс Калински-Харабаза", "value": _format_number(clustering.get("calinski_harabasz"), 1), "meta": "выше - лучше"},
            {"label": "Баланс кластеров", "value": _format_percent(clustering.get("cluster_balance_ratio") or 0.0), "meta": "отношение min/max размера кластера"},
            {"label": "Устойчивость ARI", "value": _format_number(clustering.get("stability_ari"), 3), "meta": "устойчивость KMeans к random_state"},
        ],
        "methodology_items": [
            {"label": "Основной алгоритм", "value": "KMeans", "meta": "текущая рабочая сегментация"},
            {"label": "Число кластеров", "value": _format_integer(cluster_count), "meta": "используется для всех сравниваемых методов"},
            {"label": "Признаков", "value": _format_integer(len(selected_features)), "meta": ", ".join(selected_features[:4]) if selected_features else "—"},
            {"label": "Покрытие PCA", "value": _format_percent(clustering.get("explained_variance") or 0.0), "meta": "доля дисперсии на 2D-проекции"},
        ],
        "comparison_rows": comparison_rows,
        "dissertation_points": dissertation_points,
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
            "suggested_cluster_count_display": "—",
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
