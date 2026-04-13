from __future__ import annotations

from typing import Any, Callable, Dict, Sequence

from app.runtime_cache import build_immutable_payload_ttl_cache, callable_cache_scope
from app.services.forecasting.selection import _canonicalize_source_tables, _normalize_filter_value

_ML_FILTER_BUNDLE_CACHE = build_immutable_payload_ttl_cache(ttl_seconds=120.0)
_ML_AGGREGATION_INPUT_CACHE = build_immutable_payload_ttl_cache(ttl_seconds=120.0)


def clear_ml_model_input_cache() -> None:
    _ML_FILTER_BUNDLE_CACHE.clear()
    _ML_AGGREGATION_INPUT_CACHE.clear()


def load_ml_filter_bundle(
    *,
    source_tables: Sequence[str],
    selected_history_window: str,
    cause: str,
    object_category: str,
    collect_forecasting_metadata: Callable[[Sequence[str]], tuple[list[Dict[str, Any]], list[str]]],
    build_option_catalog_sql: Callable[..., Dict[str, list[Dict[str, str]]]],
    resolve_option_value: Callable[[Sequence[Dict[str, str]], str], str],
) -> Dict[str, Any]:
    bundle = _load_ml_filter_bundle(
        source_tables=source_tables,
        selected_history_window=selected_history_window,
        collect_forecasting_metadata=collect_forecasting_metadata,
        build_option_catalog_sql=build_option_catalog_sql,
    )
    option_catalog = bundle["option_catalog"]
    return {
        **bundle,
        "selected_cause": resolve_option_value(option_catalog["causes"], cause),
        "selected_object_category": resolve_option_value(
            option_catalog["object_categories"],
            object_category,
        ),
    }


def load_ml_aggregation_inputs(
    *,
    source_tables: Sequence[str],
    selected_history_window: str,
    filter_bundle: Dict[str, Any],
    build_daily_history_sql: Callable[..., list[Dict[str, Any]]],
    count_forecasting_records_sql: Callable[..., int],
) -> Dict[str, Any]:
    cache_key = _ml_aggregation_input_cache_key(
        source_tables,
        selected_history_window,
        filter_bundle["selected_cause"],
        filter_bundle["selected_object_category"],
        build_daily_history_sql=build_daily_history_sql,
        count_forecasting_records_sql=count_forecasting_records_sql,
    )
    cached_inputs = _ML_AGGREGATION_INPUT_CACHE.get(cache_key)
    if cached_inputs is not None:
        return cached_inputs

    metadata_items = filter_bundle["metadata_items"]
    payload = {
        **filter_bundle,
        "daily_history": build_daily_history_sql(
            source_tables,
            history_window=selected_history_window,
            cause=filter_bundle["selected_cause"],
            object_category=filter_bundle["selected_object_category"],
            metadata_items=metadata_items,
        ),
    }
    payload["filtered_records_count"] = count_forecasting_records_sql(
        source_tables,
        history_window=selected_history_window,
        cause=payload["selected_cause"],
        object_category=payload["selected_object_category"],
        metadata_items=metadata_items,
    )
    return _ML_AGGREGATION_INPUT_CACHE.set(cache_key, payload)


def _load_ml_filter_bundle(
    *,
    source_tables: Sequence[str],
    selected_history_window: str,
    collect_forecasting_metadata: Callable[[Sequence[str]], tuple[list[Dict[str, Any]], list[str]]],
    build_option_catalog_sql: Callable[..., Dict[str, list[Dict[str, str]]]],
) -> Dict[str, Any]:
    cache_key = _ml_filter_bundle_cache_key(
        source_tables,
        selected_history_window,
        collect_forecasting_metadata=collect_forecasting_metadata,
        build_option_catalog_sql=build_option_catalog_sql,
    )
    cached_bundle = _ML_FILTER_BUNDLE_CACHE.get(cache_key)
    if cached_bundle is not None:
        return cached_bundle

    metadata_items, preload_notes = collect_forecasting_metadata(source_tables)
    payload = {
        "metadata_items": metadata_items,
        "preload_notes": preload_notes,
        "option_catalog": build_option_catalog_sql(
            source_tables,
            history_window=selected_history_window,
            metadata_items=metadata_items,
        ),
    }
    return _ML_FILTER_BUNDLE_CACHE.set(cache_key, payload)


def _ml_filter_bundle_cache_key(
    source_tables: Sequence[str],
    selected_history_window: str,
    *,
    collect_forecasting_metadata: Callable[..., Any],
    build_option_catalog_sql: Callable[..., Any],
) -> tuple[Any, ...]:
    normalized_tables = _canonicalize_source_tables(source_tables)[0]
    return (
        "ml_filter_bundle",
        *callable_cache_scope(collect_forecasting_metadata, build_option_catalog_sql),
        *normalized_tables,
        selected_history_window,
    )


def _ml_aggregation_input_cache_key(
    source_tables: Sequence[str],
    selected_history_window: str,
    selected_cause: str,
    selected_object_category: str,
    *,
    build_daily_history_sql: Callable[..., Any],
    count_forecasting_records_sql: Callable[..., Any],
) -> tuple[Any, ...]:
    normalized_tables = _canonicalize_source_tables(source_tables)[0]
    return (
        "ml_aggregation_inputs",
        *callable_cache_scope(build_daily_history_sql, count_forecasting_records_sql),
        *normalized_tables,
        selected_history_window,
        _normalize_filter_value(selected_cause),
        _normalize_filter_value(selected_object_category),
    )
