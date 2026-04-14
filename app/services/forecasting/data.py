from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from app.services.shared.data_base import DataLoader

from . import selection as _selection
from . import shaping as _shaping
from . import sources as _sources
from . import sql as _sql
from .types import (
    ForecastingDailyHistoryRow,
    ForecastingInputRecord,
    ForecastingOptionCatalog,
    ForecastingTableMetadata,
)


class ForecastingDataLoader(DataLoader):
    def __init__(self) -> None:
        super().__init__(cache=_sql._FORECASTING_SQL_CACHE, cache_namespace="forecasting_data")

    def build_forecasting_table_options(self) -> List[Dict[str, str]]:
        return _selection._build_forecasting_table_options()

    def collect_forecasting_metadata(self, source_tables: Sequence[str]) -> tuple[List[ForecastingTableMetadata], List[str]]:
        return _sources._collect_forecasting_metadata(source_tables)

    def collect_forecasting_inputs(
        self,
        source_tables: List[str],
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
        history_window: str = "all",
    ) -> tuple[List[ForecastingTableMetadata], List[ForecastingInputRecord], List[str]]:
        return _sources._collect_forecasting_inputs(
            source_tables,
            district=district,
            cause=cause,
            object_category=object_category,
            history_window=history_window,
        )

    def build_option_catalog_sql(
        self,
        source_tables: Sequence[str],
        history_window: str = "all",
        metadata_items: Optional[Sequence[ForecastingTableMetadata]] = None,
    ) -> ForecastingOptionCatalog:
        return _sql._build_option_catalog_sql(
            source_tables,
            history_window=history_window,
            metadata_items=metadata_items,
        )

    def build_daily_history_sql(
        self,
        source_tables: Sequence[str],
        history_window: str = "all",
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
        metadata_items: Optional[Sequence[ForecastingTableMetadata]] = None,
    ) -> List[ForecastingDailyHistoryRow]:
        return _sql._build_daily_history_sql(
            source_tables,
            history_window=history_window,
            district=district,
            cause=cause,
            object_category=object_category,
            metadata_items=metadata_items,
        )

    def count_forecasting_records_sql(
        self,
        source_tables: Sequence[str],
        history_window: str = "all",
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
        metadata_items: Optional[Sequence[ForecastingTableMetadata]] = None,
    ) -> int:
        return _sql._count_forecasting_records_sql(
            source_tables,
            history_window=history_window,
            district=district,
            cause=cause,
            object_category=object_category,
            metadata_items=metadata_items,
        )


_LOADER = ForecastingDataLoader()


def _build_forecasting_table_options() -> List[Dict[str, str]]:
    return _LOADER.build_forecasting_table_options()


def _collect_forecasting_metadata(source_tables: Sequence[str]) -> tuple[List[dict[str, Any]], List[str]]:
    return _LOADER.collect_forecasting_metadata(source_tables)


def _collect_forecasting_inputs(
    source_tables: List[str],
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    history_window: str = "all",
) -> tuple[List[dict[str, Any]], List[dict[str, Any]], List[str]]:
    return _LOADER.collect_forecasting_inputs(
        source_tables,
        district=district,
        cause=cause,
        object_category=object_category,
        history_window=history_window,
    )


def _build_option_catalog_sql(
    source_tables: Sequence[str],
    history_window: str = "all",
    metadata_items: Optional[Sequence[dict[str, Any]]] = None,
) -> Dict[str, List[Dict[str, str]]]:
    return _LOADER.build_option_catalog_sql(
        source_tables,
        history_window=history_window,
        metadata_items=metadata_items,
    )


def _build_daily_history_sql(
    source_tables: Sequence[str],
    history_window: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    metadata_items: Optional[Sequence[dict[str, Any]]] = None,
) -> List[dict[str, Any]]:
    return _LOADER.build_daily_history_sql(
        source_tables,
        history_window=history_window,
        district=district,
        cause=cause,
        object_category=object_category,
        metadata_items=metadata_items,
    )


def _count_forecasting_records_sql(
    source_tables: Sequence[str],
    history_window: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    metadata_items: Optional[Sequence[dict[str, Any]]] = None,
) -> int:
    return _LOADER.count_forecasting_records_sql(
        source_tables,
        history_window=history_window,
        district=district,
        cause=cause,
        object_category=object_category,
        metadata_items=metadata_items,
    )


def clear_forecasting_sql_cache() -> None:
    _sql.clear_forecasting_sql_cache()


def _reexport_module(module: object, excluded_names: Sequence[str]) -> list[str]:
    exported: list[str] = []
    excluded = set(excluded_names)
    for name, value in vars(module).items():
        if name.startswith("__") or name in excluded:
            continue
        globals()[name] = value
        exported.append(name)
    return exported


__all__ = [
    "ForecastingDataLoader",
    "_build_forecasting_table_options",
    "_collect_forecasting_metadata",
    "_collect_forecasting_inputs",
    "_build_option_catalog_sql",
    "_build_daily_history_sql",
    "_count_forecasting_records_sql",
    "clear_forecasting_sql_cache",
]
for _module in (_selection, _shaping, _sources, _sql):
    __all__.extend(_reexport_module(_module, __all__))
__all__ = list(dict.fromkeys(__all__))


del _module
del _reexport_module
