from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime

from app.perf import current_perf_trace, profiled
from app.plotly_bundle import get_plotly_bundle
from app.runtime_cache import CopyingTtlCache
from typing import Any, Callable, Dict, List, Sequence, Tuple
from config.db import engine

from .analysis import (
    _build_clustering_mode_context,
    _build_runtime_clustering_context,
    _build_default_feature_selection_analysis,
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
from .constants import (
    CLUSTER_COUNT_OPTIONS,
    LOW_SUPPORT_TERRITORY_THRESHOLD,
    MIN_ROWS_PER_CLUSTER,
    RATE_SMOOTHING_PRIOR_STRENGTH,
    SAMPLE_LIMIT_OPTIONS,
    SAMPLING_STRATEGY_OPTIONS,
    STABILITY_RESAMPLE_RATIO,
)
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


def _emit_clustering_progress(
    progress_callback: Callable[[str, str], None] | None,
    phase: str,
    message: str,
) -> None:
    if progress_callback is None:
        return
    progress_callback(phase, message)


def _format_configuration_label(configuration: Dict[str, Any] | None) -> str:
    if not configuration:
        return "—"
    method_label = str(configuration.get("method_label") or "Метод")
    cluster_count = configuration.get("cluster_count")
    if cluster_count:
        return f"{method_label}, k={_format_integer(cluster_count)}"
    return method_label


def _select_render_configuration(
    *,
    requested_cluster_count: int,
    cluster_count_is_explicit: bool,
    diagnostics: Dict[str, Any] | None,
    fallback_weighting_strategy: str,
) -> Dict[str, Any]:
    diagnostics = diagnostics or {}
    if not cluster_count_is_explicit:
        best_configuration = diagnostics.get("best_configuration")
        if best_configuration:
            return dict(best_configuration)

    rows_by_cluster_count = diagnostics.get("method_rows_by_cluster_count") or {}
    method_rows = rows_by_cluster_count.get(int(requested_cluster_count)) or []
    selected_row = next((row for row in method_rows if row.get("is_recommended")), None)
    if selected_row is None:
        selected_row = next((row for row in method_rows if row.get("is_selected")), None)
    if selected_row is not None:
        return {**selected_row, "cluster_count": int(requested_cluster_count)}

    return {
        "cluster_count": int(requested_cluster_count),
        "method_key": f"kmeans_{fallback_weighting_strategy}",
        "algorithm_key": "kmeans",
        "method_label": "KMeans",
        "weighting_strategy": fallback_weighting_strategy,
    }


def get_clustering_page_context(
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: Sequence[str] | None = None,
    cluster_count_is_explicit: bool = False,
) -> Dict[str, Any]:
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
@profiled("clustering", engine=engine)
def get_clustering_data(
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: Sequence[str] | None = None,
    cluster_count_is_explicit: bool = False,
    progress_callback: Callable[[str, str], None] | None = None,
) -> Dict[str, Any]:
    perf = current_perf_trace()
    request_state = _build_clustering_request_state(
        table_name=table_name,
        cluster_count=cluster_count,
        sample_limit=sample_limit,
        sampling_strategy=sampling_strategy,
        feature_columns=feature_columns,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    table_options = request_state["table_options"]
    selected_table = request_state["selected_table"]
    requested_cluster_count = request_state["cluster_count"]
    requested_sample_limit = request_state["sample_limit"]
    selected_sampling_strategy = request_state["sampling_strategy"]
    normalized_feature_columns = request_state["feature_columns"]
    cluster_count_is_explicit = request_state["cluster_count_is_explicit"]
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
        feature_selection_report = None
        if normalized_feature_columns:
            selected_features, selection_note = _resolve_selected_features(
                feature_names,
                normalized_feature_columns,
                feature_frame=dataset["feature_frame"],
                entity_frame=dataset["entity_frame"],
                cluster_count=requested_cluster_count,
            )
        else:
            feature_selection_report = _build_default_feature_selection_analysis(
                feature_frame=dataset["feature_frame"],
                entity_frame=dataset["entity_frame"],
                available_features=feature_names,
                cluster_count=requested_cluster_count,
            )
            selected_features = list(feature_selection_report.get("selected_features") or [])
            selection_note = str(feature_selection_report.get("selection_note") or "")
        feature_selection_report = _build_clustering_mode_context(selected_features, feature_selection_report)
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

    requested_working_cluster_count = min(max(CLUSTER_COUNT_OPTIONS[0], requested_cluster_count), len(cluster_frame) - 1)
    actual_cluster_count = requested_working_cluster_count
    if requested_working_cluster_count != requested_cluster_count:
        base["notes"].append(
            f"Количество кластеров автоматически скорректировано до {actual_cluster_count}, чтобы в каждом типе территорий было достаточно наблюдений."
        )

    with (perf.span("model_training") if perf is not None else nullcontext()):
        weighting_strategy = str(feature_selection_report.get("weighting_strategy") or "")
        diagnostics = _evaluate_cluster_counts(
            cluster_frame,
            entity_frame,
            weighting_strategy=weighting_strategy,
        )
        render_configuration = _select_render_configuration(
            requested_cluster_count=requested_working_cluster_count,
            cluster_count_is_explicit=cluster_count_is_explicit,
            diagnostics=diagnostics,
            fallback_weighting_strategy=weighting_strategy,
        )
        actual_cluster_count = int(render_configuration.get("cluster_count") or requested_working_cluster_count)
        actual_method_key = str(render_configuration.get("method_key") or f"kmeans_{weighting_strategy}")
        actual_algorithm_key = str(render_configuration.get("algorithm_key") or "kmeans")
        actual_weighting_strategy = str(render_configuration.get("weighting_strategy") or weighting_strategy)
        runtime_feature_context = _build_runtime_clustering_context(
            feature_selection_report,
            method_label=str(render_configuration.get("method_label") or "KMeans"),
            algorithm_key=actual_algorithm_key,
            weighting_strategy=actual_weighting_strategy,
        )
        clustering = _run_clustering(
            cluster_frame,
            entity_frame,
            actual_cluster_count,
            weighting_strategy=actual_weighting_strategy,
            algorithm_key=actual_algorithm_key,
            method_key=actual_method_key,
        )
        method_comparison = _compare_clustering_methods(
            cluster_frame,
            entity_frame,
            actual_cluster_count,
            weighting_strategy=weighting_strategy,
            selected_method_key=actual_method_key,
        )
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
            actual_method_key=actual_method_key,
        )
    cluster_count_guidance = _build_cluster_count_guidance(
        requested_cluster_count=requested_cluster_count,
        current_cluster_count=actual_cluster_count,
        diagnostics=diagnostics,
        adjusted_requested_cluster_count=requested_working_cluster_count,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    base["filters"]["cluster_count"] = str(actual_cluster_count)
    summary["cluster_count_display"] = _format_integer(actual_cluster_count)
    summary["cluster_count_requested_display"] = _format_integer(requested_cluster_count)
    summary["cluster_count_note"] = str(cluster_count_guidance.get("current_note") or "")
    summary["suggested_cluster_count_label"] = str(cluster_count_guidance.get("suggested_label") or "Рекомендуемый k")
    summary["suggested_cluster_count_display"] = (
        _format_integer(cluster_count_guidance["recommended_cluster_count"])
        if cluster_count_guidance.get("recommended_cluster_count")
        else "—"
    )
    summary["suggested_cluster_count_note"] = str(cluster_count_guidance.get("suggested_note") or "")
    summary["elbow_cluster_count_display"] = _format_integer(diagnostics["elbow_k"]) if diagnostics.get("elbow_k") else "—"
    summary["silhouette_display"] = _format_number(clustering["silhouette"], 3) if clustering["silhouette"] is not None else "—"
    summary["pca_variance_display"] = _format_percent(clustering["explained_variance"])
    summary["inertia_display"] = _format_number(clustering["inertia"], 2)
    if cluster_count_guidance.get("notes_message"):
        base["notes"].append(str(cluster_count_guidance["notes_message"]))

    _emit_clustering_progress(
        progress_callback,
        "clustering.render",
        "Подготавливаем итоговые таблицы, профили кластеров и визуализации.",
    )
    with (perf.span("payload_render") if perf is not None else nullcontext()):
        payload = {
            **base,
            "has_data": True,
            "model_description": " ".join(
                part
                for part in [
                    "Кластеризация строится не по отдельным инцидентам, а по агрегированным территориям и населённым пунктам.",
                    "Для территорий с короткой историей долевые признаки считаются через empirical Bayes shrinkage к глобальному среднему.",
                    str(cluster_count_guidance.get("model_note") or ""),
                    str(runtime_feature_context.get("weighting_note") or ""),
                    str(runtime_feature_context.get("volume_note") or ""),
                ]
                if part
            ),
            "summary": summary,
            "quality_assessment": _build_clustering_quality_assessment(
                clustering,
                method_comparison,
                actual_cluster_count,
                selected_features,
                diagnostics=diagnostics,
                support_summary=dataset.get("support_summary"),
                feature_selection_report=runtime_feature_context,
                requested_cluster_count=requested_cluster_count,
                resolved_requested_cluster_count=requested_working_cluster_count,
                cluster_count_is_explicit=cluster_count_is_explicit,
            ),
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
                    current_cluster_count=actual_cluster_count,
                    recommended_cluster_count=diagnostics.get("best_quality_k"),
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
                support_summary=dataset.get("support_summary"),
                stability_ari=clustering.get("stability_ari"),
                feature_selection_report=runtime_feature_context,
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
        "subtitle": "После расчета здесь появятся метрики качества, устойчивость на повторных подвыборках и сравнение алгоритмов.",
        "metric_cards": [],
        "methodology_items": [],
        "comparison_rows": [],
        "dissertation_points": ["Пока недостаточно данных для расчета метрик качества кластеризации."],
    }




def _build_configuration_recommendation_note(
    working_configuration: Dict[str, Any] | None,
    recommended_configuration: Dict[str, Any] | None,
    *,
    cluster_count_is_explicit: bool,
) -> str:
    working_label = _format_configuration_label(working_configuration)
    recommended_label = _format_configuration_label(recommended_configuration)
    if not recommended_configuration or working_label == recommended_label:
        if cluster_count_is_explicit:
            return f"На пользовательском k текущий вывод уже использует лучшую сопоставимую конфигурацию: {working_label}."
        return f"По умолчанию страница сразу показывает рекомендуемую конфигурацию: {working_label}."
    if cluster_count_is_explicit:
        return (
            f"На пользовательском k текущий вывод использует лучшую сопоставимую конфигурацию {working_label}, "
            f"но по всему доступному диапазону убедительнее выглядит {recommended_label}."
        )
    return f"Рабочий вывод построен по конфигурации {working_label}, а recommendation engine выбирает {recommended_label}."


def _build_clustering_quality_assessment(
    clustering: Dict[str, Any],
    method_comparison: Sequence[Dict[str, Any]],
    cluster_count: int,
    selected_features: Sequence[str],
    diagnostics: Dict[str, Any] | None = None,
    support_summary: Dict[str, Any] | None = None,
    feature_selection_report: Dict[str, Any] | None = None,
    requested_cluster_count: int | None = None,
    resolved_requested_cluster_count: int | None = None,
    cluster_count_is_explicit: bool = False,
) -> Dict[str, Any]:
    if clustering.get("silhouette") is None:
        payload = _empty_clustering_quality_assessment()
        payload["dissertation_points"] = ["В текущем срезе кластеризация построена, но внутренних метрик пока недостаточно для устойчивой интерпретации качества."]
        return payload

    low_support_share = float((support_summary or {}).get("low_support_share") or 0.0)
    low_support_display = _format_percent(low_support_share)
    resample_share_label = f"{int(round(STABILITY_RESAMPLE_RATIO * 100.0))}%"
    recommended_configuration = dict((diagnostics or {}).get("best_configuration") or {})
    recommended_k = int(recommended_configuration.get("cluster_count") or (diagnostics or {}).get("best_quality_k") or cluster_count)
    best_silhouette_k = (diagnostics or {}).get("best_silhouette_k")
    cluster_count_guidance = _build_cluster_count_guidance(
        requested_cluster_count=requested_cluster_count or cluster_count,
        current_cluster_count=cluster_count,
        diagnostics=diagnostics,
        adjusted_requested_cluster_count=resolved_requested_cluster_count,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    selected_method = next((row for row in method_comparison if row.get("is_selected")), method_comparison[0] if method_comparison else None)
    recommended_row = next((row for row in method_comparison if row.get("is_recommended")), selected_method)
    working_configuration = {**dict(selected_method or {}), "cluster_count": cluster_count}
    effective_recommended_configuration = recommended_configuration or {**dict(recommended_row or {}), "cluster_count": recommended_k}
    recommended_method = effective_recommended_configuration or recommended_row or selected_method
    working_config_label = _format_configuration_label(working_configuration)
    recommended_config_label = _format_configuration_label(effective_recommended_configuration)

    segmentation_summary = _summarize_segmentation_strength(
        clustering,
        selected_method=selected_method,
        recommended_method=recommended_method,
        cluster_count=cluster_count,
        recommended_k=recommended_k,
    )
    stability_note = _build_stability_note(clustering, resample_share_label)
    method_note = _build_configuration_recommendation_note(
        working_configuration,
        effective_recommended_configuration,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    comparison_scope_note = _build_method_comparison_scope_note(method_comparison)
    cluster_shape_note = _build_cluster_shape_note(clustering)
    mode_label = str((feature_selection_report or {}).get("volume_role_label") or "Профиль территории")
    mode_note = str((feature_selection_report or {}).get("volume_note") or "")
    weighting_label = str((feature_selection_report or {}).get("weighting_label") or "Равный вес территорий")
    weighting_note = str((feature_selection_report or {}).get("weighting_note") or "")
    weighting_meta = str((feature_selection_report or {}).get("weighting_meta") or "")
    ablation_rows = list((feature_selection_report or {}).get("ablation_rows") or [])
    negative_adds = [
        row for row in ablation_rows if row.get("direction") == "add" and float(row.get("delta_score") or 0.0) < 0.0
    ]
    ablation_note = ""
    if negative_adds:
        worst_feature = min(negative_adds, key=lambda item: float(item.get("delta_score") or 0.0))
        ablation_note = (
            f"В малом ablation-анализе признак '{worst_feature['feature']}' не вошёл в default feature set, "
            "потому что его добавление ухудшало качество кластеризации."
        )

    comparison_rows = [
        {
            "method_label": row.get("method_label", "Метод"),
            "selection_label": (
                "Рабочий и лучший на текущем k"
                if row.get("is_selected") and row.get("is_recommended")
                else "Рабочий вывод"
                if row.get("is_selected")
                else "Лучше на текущем k"
                if row.get("is_recommended")
                else "Сравнение"
            ),
            "silhouette_display": _format_number(row.get("silhouette"), 3),
            "davies_display": _format_number(row.get("davies_bouldin"), 3),
            "calinski_display": _format_number(row.get("calinski_harabasz"), 1),
            "balance_display": _format_percent(row.get("cluster_balance_ratio") or 0.0),
        }
        for row in method_comparison
    ]

    dissertation_points = [
        segmentation_summary["note"],
        method_note,
        str(cluster_count_guidance.get("quality_note") or ""),
        (
            f"Рекомендуемая конфигурация по совокупности метрик: {recommended_config_label}."
            if recommended_config_label != working_config_label
            else f"Рабочая конфигурация {working_config_label} уже совпадает с recommendation engine."
        ),
        (
            f"Пик silhouette отдельно приходится на k={_format_integer(best_silhouette_k)}, поэтому выбор k лучше читать вместе с балансом кластеров и риском микрокластеров."
            if recommended_k and best_silhouette_k and recommended_k != best_silhouette_k
            else "Silhouette, баланс и размеры кластеров не дают заметного конфликта по выбору k."
        ),
        stability_note,
        f"{low_support_display} территорий имеют не более {LOW_SUPPORT_TERRITORY_THRESHOLD} пожаров, поэтому долевые признаки считаются через empirical Bayes shrinkage к глобальному среднему: вместо raw 0/1 к доле добавляются около {int(RATE_SMOOTHING_PRIOR_STRENGTH)} псевдо-наблюдений из общего профиля.",
        f"Сравнение методов выполнено на том же наборе признаков: {', '.join(selected_features)}.",
    ]
    if comparison_scope_note:
        dissertation_points.append(comparison_scope_note)
    if cluster_shape_note:
        dissertation_points.append(cluster_shape_note)
    if weighting_note:
        dissertation_points.append(weighting_note)
    if mode_note:
        dissertation_points.append(mode_note)
    if ablation_note:
        dissertation_points.append(ablation_note)
    dissertation_points = [item for item in dissertation_points if str(item).strip()]

    return {
        "ready": True,
        "title": "Оценка качества кластеризации",
        "subtitle": "Ниже собраны честная оценка силы сегментации, устойчивость на повторных подвыборках и recommendation engine по конфигурации mode / weighting / method / k.",
        "metric_cards": [
            {"label": "Коэффициент силуэта", "value": _format_number(clustering.get("silhouette"), 3), "meta": "выше — лучше"},
            {"label": "Индекс Дэвиса-Болдина", "value": _format_number(clustering.get("davies_bouldin"), 3), "meta": "ниже - лучше"},
            {"label": "Индекс Калински-Харабаза", "value": _format_number(clustering.get("calinski_harabasz"), 1), "meta": "выше - лучше"},
            {
                "label": "Баланс кластеров",
                "value": _format_percent(clustering.get("cluster_balance_ratio") or 0.0),
                "meta": f"min/max: {_format_integer(clustering.get('smallest_cluster_size'))} / {_format_integer(clustering.get('largest_cluster_size'))}",
            },
            {"label": "Устойчивость на подвыборках", "value": _format_number(clustering.get("stability_ari"), 3), "meta": f"повторные {resample_share_label}-подвыборки"},
        ],
        "methodology_items": [
            {"label": "Рабочая конфигурация", "value": working_config_label, "meta": "по ней построены текущие кластеры на странице"},
            {"label": "Рекомендуемая конфигурация", "value": recommended_config_label, "meta": "лучший bundle mode / weighting / method / k в доступном диапазоне"},
            {"label": "Режим выбора k", "value": "Пользовательский k" if cluster_count_is_explicit else "Автовыбор", "meta": str(cluster_count_guidance.get("current_note") or "")},
            {"label": "Рабочий метод", "value": str((selected_method or {}).get("method_label") or "KMeans"), "meta": "лучший метод среди сопоставимых вариантов на текущем k"},
            {"label": "Рекомендация по методу", "value": str((recommended_method or {}).get("method_label") or "KMeans"), "meta": "какой алгоритм выигрывает в рекомендуемой конфигурации"},
            {"label": "Сила сегментации", "value": segmentation_summary["label"], "meta": "сводная оценка по silhouette / DB / устойчивости / размерам кластеров"},
            {"label": "Режим типологии", "value": mode_label, "meta": "что именно кластеризуется по умолчанию"},
            {"label": "Весы территорий", "value": weighting_label, "meta": weighting_meta or "как нагрузка влияет на центры или почему sample weights не используются"},
            {
                "label": "Число кластеров",
                "value": _format_integer(cluster_count),
                "meta": str(cluster_count_guidance.get("methodology_meta") or f"диагностика ограничена диапазоном {CLUSTER_COUNT_OPTIONS[0]}..{CLUSTER_COUNT_OPTIONS[-1]}, как в UI"),
            },
            {"label": "Признаков", "value": _format_integer(len(selected_features)), "meta": "выбраны по малому ablation-анализу и вкладу в silhouette / DB / CH"},
            {"label": "Низкая поддержка", "value": low_support_display, "meta": f"территории с ≤{LOW_SUPPORT_TERRITORY_THRESHOLD} пожарами сглажены к общему уровню"},
            {"label": "Покрытие PCA", "value": _format_percent(clustering.get("explained_variance") or 0.0), "meta": "доля дисперсии на 2D-проекции"},
        ],
        "comparison_rows": comparison_rows,
        "dissertation_points": dissertation_points,
    }




def _build_cluster_count_guidance(
    requested_cluster_count: int,
    current_cluster_count: int,
    diagnostics: Dict[str, Any] | None = None,
    adjusted_requested_cluster_count: int | None = None,
    cluster_count_is_explicit: bool = False,
) -> Dict[str, Any]:
    recommended_k = (diagnostics or {}).get("best_quality_k")
    best_silhouette_k = (diagnostics or {}).get("best_silhouette_k")
    requested_cluster_count = int(requested_cluster_count)
    adjusted_requested_cluster_count = int(
        adjusted_requested_cluster_count if adjusted_requested_cluster_count is not None else requested_cluster_count
    )
    current_cluster_count = int(current_cluster_count)
    request_adjusted = requested_cluster_count != adjusted_requested_cluster_count
    recommendation_gap = bool(recommended_k) and int(recommended_k) != current_cluster_count
    auto_switched_to_recommended = (
        not cluster_count_is_explicit and adjusted_requested_cluster_count != current_cluster_count
    )

    suggested_label = "Автовыбор k" if not cluster_count_is_explicit else "Рекомендуемый k"
    suggested_note = "Диагностика k появится, когда хватит данных для сравнения нескольких вариантов."
    current_note = f"Сейчас основной вывод показан для k={_format_integer(current_cluster_count)}."
    quality_note = ""
    notes_message = ""
    model_note = ""

    if recommended_k:
        recommended_k = int(recommended_k)
        if cluster_count_is_explicit and recommendation_gap:
            suggested_note = (
                f"Диагностика рекомендует k={_format_integer(recommended_k)}; "
                f"сейчас сохранён пользовательский k={_format_integer(current_cluster_count)}."
            )
            current_note = (
                f"Сейчас показан пользовательский k={_format_integer(current_cluster_count)}; "
                f"диагностика рекомендует k={_format_integer(recommended_k)}."
            )
            quality_note = (
                f"Пользовательский k={_format_integer(current_cluster_count)} не совпадает с рекомендацией diagnostics; "
                f"по совокупности метрик лучше выглядит k={_format_integer(recommended_k)}."
            )
            model_note = (
                f"Пользователь зафиксировал k={_format_integer(current_cluster_count)}, "
                "поэтому страница не переключает число кластеров автоматически."
            )
            notes_message = quality_note
        elif cluster_count_is_explicit:
            suggested_note = (
                f"Диагностика подтверждает пользовательский k={_format_integer(current_cluster_count)}."
            )
            current_note = (
                f"Пользовательский k={_format_integer(current_cluster_count)} совпадает с рекомендацией диагностики."
            )
            quality_note = (
                f"Пользовательский k={_format_integer(current_cluster_count)} согласован с recommendation engine."
            )
            model_note = (
                f"Пользовательский k={_format_integer(current_cluster_count)} совпадает с рекомендуемым значением."
            )
            notes_message = quality_note
        else:
            suggested_note = (
                f"Автовыбор использует k={_format_integer(current_cluster_count)} как лучший вариант по совокупности метрик."
            )
            if auto_switched_to_recommended:
                current_note = (
                    f"По умолчанию страница показывает рекомендуемый k={_format_integer(current_cluster_count)} "
                    f"вместо стартового k={_format_integer(adjusted_requested_cluster_count)}."
                )
                quality_note = (
                    "Рабочий вывод автоматически синхронизирован с recommendation engine: "
                    f"вместо стартового k={_format_integer(adjusted_requested_cluster_count)} "
                    f"показан k={_format_integer(current_cluster_count)}."
                )
                model_note = (
                    f"По умолчанию страница показывает рекомендуемый k={_format_integer(current_cluster_count)} "
                    f"вместо стартового k={_format_integer(adjusted_requested_cluster_count)}."
                )
            else:
                current_note = (
                    f"По умолчанию страница показывает рекомендуемый k={_format_integer(current_cluster_count)}."
                )
                quality_note = (
                    "Рабочий вывод уже синхронизирован с recommendation engine "
                    f"по числу кластеров: k={_format_integer(current_cluster_count)}."
                )
                model_note = (
                    f"По умолчанию страница показывает рекомендуемый k={_format_integer(current_cluster_count)}."
                )
            notes_message = quality_note

    if request_adjusted:
        adjustment_note = (
            f"Запрошенное значение k={_format_integer(requested_cluster_count)} автоматически скорректировано до "
            f"k={_format_integer(adjusted_requested_cluster_count)} из-за ограничений выбранной выборки."
        )
        current_note = adjustment_note if not current_note else f"{adjustment_note} {current_note}".strip()
        suggested_note = f"{adjustment_note} {suggested_note}".strip()
        quality_note = adjustment_note if not quality_note else f"{adjustment_note} {quality_note}".strip()
        model_note = adjustment_note if not model_note else f"{adjustment_note} {model_note}".strip()
        notes_message = quality_note

    meta_parts = [
        f"{'пользовательский' if cluster_count_is_explicit else 'рабочий'} k={_format_integer(current_cluster_count)}"
    ]
    if request_adjusted:
        meta_parts.append(f"запрошено k={_format_integer(requested_cluster_count)}")
    elif auto_switched_to_recommended:
        meta_parts.append(f"стартовый k={_format_integer(adjusted_requested_cluster_count)}")
    if recommended_k:
        meta_parts.append(f"рекомендуемое k={_format_integer(recommended_k)}")
    if best_silhouette_k:
        meta_parts.append(f"пик silhouette на k={_format_integer(best_silhouette_k)}")
    if not recommended_k and not best_silhouette_k:
        meta_parts.append(
            f"диагностика ограничена диапазоном {CLUSTER_COUNT_OPTIONS[0]}..{CLUSTER_COUNT_OPTIONS[-1]}, как в UI"
        )

    return {
        "recommended_cluster_count": recommended_k,
        "best_silhouette_k": best_silhouette_k,
        "has_recommendation_gap": recommendation_gap,
        "request_adjusted": request_adjusted,
        "suggested_label": suggested_label,
        "suggested_note": suggested_note,
        "current_note": current_note,
        "quality_note": quality_note,
        "notes_message": notes_message,
        "model_note": model_note,
        "methodology_meta": "; ".join(meta_parts),
    }


def _summarize_segmentation_strength(
    clustering: Dict[str, Any],
    selected_method: Dict[str, Any] | None = None,
    recommended_method: Dict[str, Any] | None = None,
    cluster_count: int | None = None,
    recommended_k: int | None = None,
) -> Dict[str, str]:
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


def _build_stability_note(clustering: Dict[str, Any], resample_share_label: str) -> str:
    stability_ari = clustering.get("stability_ari")
    initialization_ari = clustering.get("initialization_ari")
    if stability_ari is None:
        return "Оценить устойчивость на повторных подвыборках не удалось: в текущем срезе слишком мало территорий для надёжного сравнения пересэмплов."
    if initialization_ari is None:
        return f"Устойчивость ARI = {_format_number(stability_ari, 3)} считается на повторных {resample_share_label}-подвыборках, а не только на смене random_state."

    gap = float(initialization_ari) - float(stability_ari)
    if gap >= 0.15:
        return (
            f"На одном и том же датасете KMeans почти не чувствителен к random_state (ARI {_format_number(initialization_ari, 3)}), "
            f"но на повторных {resample_share_label}-подвыборках устойчивость заметно ниже (ARI {_format_number(stability_ari, 3)}), поэтому уверенность в сегментации не стоит завышать."
        )
    return (
        f"Устойчивость на повторных {resample_share_label}-подвыборках составляет ARI {_format_number(stability_ari, 3)}; "
        f"разница с проверкой только по random_state умеренная (ARI {_format_number(initialization_ari, 3)})."
    )


def _build_method_recommendation_note(
    selected_method: Dict[str, Any] | None,
    recommended_method: Dict[str, Any] | None,
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


def _build_method_comparison_scope_note(method_comparison: Sequence[Dict[str, Any]]) -> str:
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


def _resolve_method_algorithm_key(method_row: Dict[str, Any] | None) -> str:
    if not method_row:
        return ""
    return str(method_row.get("algorithm_key") or method_row.get("method_key") or "")


def _build_cluster_shape_note(clustering: Dict[str, Any]) -> str:
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
