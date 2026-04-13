from __future__ import annotations

from typing import Any, Callable, Dict, Sequence

from app.services.shared.data_base import DataLoader

from . import data_access_impl as _impl


class MlModelDataLoader(DataLoader):
    def __init__(self) -> None:
        super().__init__(cache=None, cache_namespace="ml_model_data")

    def clear_cache(self) -> None:
        _impl.clear_ml_model_input_cache()

    def load_filter_bundle(
        self,
        *,
        source_tables: Sequence[str],
        selected_history_window: str,
        cause: str,
        object_category: str,
        collect_forecasting_metadata: Callable[[Sequence[str]], tuple[list[Dict[str, Any]], list[str]]],
        build_option_catalog_sql: Callable[..., Dict[str, list[Dict[str, str]]]],
        resolve_option_value: Callable[[Sequence[Dict[str, str]], str], str],
    ) -> Dict[str, Any]:
        return _impl.load_ml_filter_bundle(
            source_tables=source_tables,
            selected_history_window=selected_history_window,
            cause=cause,
            object_category=object_category,
            collect_forecasting_metadata=collect_forecasting_metadata,
            build_option_catalog_sql=build_option_catalog_sql,
            resolve_option_value=resolve_option_value,
        )

    def load_aggregation_inputs(
        self,
        *,
        source_tables: Sequence[str],
        selected_history_window: str,
        filter_bundle: Dict[str, Any],
        build_daily_history_sql: Callable[..., list[Dict[str, Any]]],
        count_forecasting_records_sql: Callable[..., int],
    ) -> Dict[str, Any]:
        return _impl.load_ml_aggregation_inputs(
            source_tables=source_tables,
            selected_history_window=selected_history_window,
            filter_bundle=filter_bundle,
            build_daily_history_sql=build_daily_history_sql,
            count_forecasting_records_sql=count_forecasting_records_sql,
        )


_LOADER = MlModelDataLoader()


def clear_ml_model_input_cache() -> None:
    _LOADER.clear_cache()


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
    return _LOADER.load_filter_bundle(
        source_tables=source_tables,
        selected_history_window=selected_history_window,
        cause=cause,
        object_category=object_category,
        collect_forecasting_metadata=collect_forecasting_metadata,
        build_option_catalog_sql=build_option_catalog_sql,
        resolve_option_value=resolve_option_value,
    )


def load_ml_aggregation_inputs(
    *,
    source_tables: Sequence[str],
    selected_history_window: str,
    filter_bundle: Dict[str, Any],
    build_daily_history_sql: Callable[..., list[Dict[str, Any]]],
    count_forecasting_records_sql: Callable[..., int],
) -> Dict[str, Any]:
    return _LOADER.load_aggregation_inputs(
        source_tables=source_tables,
        selected_history_window=selected_history_window,
        filter_bundle=filter_bundle,
        build_daily_history_sql=build_daily_history_sql,
        count_forecasting_records_sql=count_forecasting_records_sql,
    )


def _reexport_impl(module: object, excluded_names: Sequence[str]) -> list[str]:
    exported: list[str] = []
    excluded = set(excluded_names)
    for name, value in vars(module).items():
        if name.startswith("__") or name in excluded:
            continue
        globals()[name] = value
        exported.append(name)
    return exported


__all__ = [
    "MlModelDataLoader",
    "clear_ml_model_input_cache",
    "load_ml_filter_bundle",
    "load_ml_aggregation_inputs",
]
__all__.extend(_reexport_impl(_impl, __all__))
__all__ = list(dict.fromkeys(__all__))


del _reexport_impl
