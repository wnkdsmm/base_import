from __future__ import annotations

from typing import Any, Callable, Dict, Sequence

from app.cache import CopyingTtlCache, clone_mutable_payload, freeze_mutable_payload

from .selection import _canonicalize_source_tables, _normalize_filter_value
from .types import ForecastingBaseInputs, ForecastingMetadataInputs

ForecastingDeps = Dict[str, Callable[..., Any]]

_FORECASTING_METADATA_BUNDLE_CACHE = CopyingTtlCache(
    ttl_seconds=120.0,
    storer=freeze_mutable_payload,
    loader=clone_mutable_payload,
)
_FORECASTING_BASE_INPUT_CACHE = CopyingTtlCache(
    ttl_seconds=120.0,
    storer=freeze_mutable_payload,
    loader=clone_mutable_payload,
)


def clear_forecasting_input_cache() -> None:
    _FORECASTING_METADATA_BUNDLE_CACHE.clear()
    _FORECASTING_BASE_INPUT_CACHE.clear()


def _deps_cache_scope(deps: ForecastingDeps, *names: str) -> tuple[int, ...]:
    return tuple(id(deps[name]) for name in names)


def _metadata_bundle_cache_key(
    source_tables: Sequence[str],
    selected_history_window: str,
    *,
    deps: ForecastingDeps,
) -> tuple[Any, ...]:
    normalized_tables = _canonicalize_source_tables(source_tables)[0]
    return (
        "forecasting_metadata_bundle",
        *_deps_cache_scope(
            deps,
            "collect_forecasting_metadata",
            "build_option_catalog_sql",
            "build_feature_cards",
        ),
        *normalized_tables,
        selected_history_window,
    )


def _base_input_cache_key(
    source_tables: Sequence[str],
    selected_history_window: str,
    selected_district: str,
    selected_cause: str,
    selected_object_category: str,
    *,
    deps: ForecastingDeps,
) -> tuple[Any, ...]:
    normalized_tables = _canonicalize_source_tables(source_tables)[0]
    return (
        "forecasting_base_inputs",
        *_deps_cache_scope(
            deps,
            "build_daily_history_sql",
            "count_forecasting_records_sql",
            "temperature_quality_from_daily_history",
            "build_feature_cards_with_quality",
        ),
        *normalized_tables,
        selected_history_window,
        _normalize_filter_value(selected_district),
        _normalize_filter_value(selected_cause),
        _normalize_filter_value(selected_object_category),
    )


def load_forecasting_metadata_inputs(
    *,
    source_tables: Sequence[str],
    selected_history_window: str,
    district: str,
    cause: str,
    object_category: str,
    deps: ForecastingDeps,
) -> ForecastingMetadataInputs:
    bundle = _load_forecasting_metadata_bundle(
        source_tables=source_tables,
        selected_history_window=selected_history_window,
        deps=deps,
    )
    option_catalog = bundle["option_catalog"]
    return {
        **bundle,
        "selected_district": deps["resolve_option_value"](option_catalog["districts"], district),
        "selected_cause": deps["resolve_option_value"](option_catalog["causes"], cause),
        "selected_object_category": deps["resolve_option_value"](
            option_catalog["object_categories"],
            object_category,
        ),
    }


def load_base_forecasting_inputs(
    *,
    source_tables: Sequence[str],
    selected_history_window: str,
    district: str,
    cause: str,
    object_category: str,
    deps: ForecastingDeps,
) -> ForecastingBaseInputs:
    metadata_inputs = load_forecasting_metadata_inputs(
        source_tables=source_tables,
        selected_history_window=selected_history_window,
        district=district,
        cause=cause,
        object_category=object_category,
        deps=deps,
    )
    cache_key = _base_input_cache_key(
        source_tables,
        selected_history_window,
        metadata_inputs["selected_district"],
        metadata_inputs["selected_cause"],
        metadata_inputs["selected_object_category"],
        deps=deps,
    )
    cached_inputs = _FORECASTING_BASE_INPUT_CACHE.get(cache_key)
    if cached_inputs is not None:
        return cached_inputs

    metadata_items = metadata_inputs["metadata_items"]
    daily_history = deps["build_daily_history_sql"](
        source_tables,
        history_window=selected_history_window,
        district=metadata_inputs["selected_district"],
        cause=metadata_inputs["selected_cause"],
        object_category=metadata_inputs["selected_object_category"],
        metadata_items=metadata_items,
    )
    filtered_records_count = deps["count_forecasting_records_sql"](
        source_tables,
        history_window=selected_history_window,
        district=metadata_inputs["selected_district"],
        cause=metadata_inputs["selected_cause"],
        object_category=metadata_inputs["selected_object_category"],
        metadata_items=metadata_items,
    )
    temperature_quality = deps["temperature_quality_from_daily_history"](daily_history)
    payload = {
        **metadata_inputs,
        "daily_history": daily_history,
        "filtered_records_count": filtered_records_count,
        "feature_cards": deps["build_feature_cards_with_quality"](
            metadata_items,
            temperature_quality=temperature_quality,
        ),
    }
    return _FORECASTING_BASE_INPUT_CACHE.set(cache_key, payload)


def _load_forecasting_metadata_bundle(
    *,
    source_tables: Sequence[str],
    selected_history_window: str,
    deps: ForecastingDeps,
) -> Dict[str, Any]:
    cache_key = _metadata_bundle_cache_key(
        source_tables,
        selected_history_window,
        deps=deps,
    )
    cached_bundle = _FORECASTING_METADATA_BUNDLE_CACHE.get(cache_key)
    if cached_bundle is not None:
        return cached_bundle

    metadata_items, preload_notes = deps["collect_forecasting_metadata"](source_tables)
    payload = {
        "metadata_items": metadata_items,
        "preload_notes": preload_notes,
        "feature_cards": deps["build_feature_cards"](metadata_items),
        "option_catalog": deps["build_option_catalog_sql"](
            source_tables,
            history_window=selected_history_window,
            metadata_items=metadata_items,
        ),
    }
    return _FORECASTING_METADATA_BUNDLE_CACHE.set(cache_key, payload)

