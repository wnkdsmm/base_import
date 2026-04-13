from __future__ import annotations

from collections import Counter
from datetime import date
from typing import Any, Dict, List, Optional, Sequence

from app.perf import current_perf_trace
from config.db import engine

from .sql_aggregations import AggregationQueryBuilder, QueryBuilder
from .sql_payload import PayloadQueryBuilder, _DailyHistoryLoadTrace
from .sql_sources import SourceQueryBuilder


def _resolve_public_symbol(name: str) -> Any:
    return globals()[name]


_AGGREGATION_BUILDER = AggregationQueryBuilder(_resolve_public_symbol)
_SOURCE_BUILDER = SourceQueryBuilder(_AGGREGATION_BUILDER, _resolve_public_symbol)
_PAYLOAD_BUILDER = PayloadQueryBuilder(_AGGREGATION_BUILDER, _SOURCE_BUILDER, _resolve_public_symbol)

_FORECASTING_SQL_CACHE = _AGGREGATION_BUILDER.cache


def clear_forecasting_sql_cache() -> None:
    _AGGREGATION_BUILDER.clear_forecasting_sql_cache()


def _build_sql_cache_key(prefix: str, source_tables: Sequence[str], *parts: Any) -> tuple[Any, ...]:
    return _AGGREGATION_BUILDER._build_sql_cache_key(prefix, source_tables, *parts)


def _filtered_record_count_cache_key(
    source_tables: Sequence[str],
    history_window: str,
    district: str,
    cause: str,
    object_category: str,
) -> tuple[Any, ...]:
    return _AGGREGATION_BUILDER._filtered_record_count_cache_key(
        source_tables,
        history_window,
        district,
        cause,
        object_category,
    )


def _daily_history_total_count(history: Sequence[Dict[str, Any]]) -> int:
    return _AGGREGATION_BUILDER._daily_history_total_count(history)


def _materialized_view_suffix(value: str) -> str:
    return _AGGREGATION_BUILDER._materialized_view_suffix(value)


def _daily_aggregate_view_name(table_name: str) -> str:
    return _AGGREGATION_BUILDER._daily_aggregate_view_name(table_name)


def _daily_aggregate_view_status_map(table_names: Sequence[str]) -> Dict[str, bool]:
    return _AGGREGATION_BUILDER._daily_aggregate_view_status_map(table_names)


def _daily_aggregate_view_exists(table_name: str) -> bool:
    return _AGGREGATION_BUILDER._daily_aggregate_view_exists(table_name)


def _build_materialized_scope_conditions(
    resolved_columns: Dict[str, str],
    min_year: Optional[int] = None,
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
) -> tuple[list[str], Dict[str, Any], bool]:
    return _AGGREGATION_BUILDER._build_materialized_scope_conditions(
        resolved_columns,
        min_year=min_year,
        district=district,
        cause=cause,
        object_category=object_category,
    )


def _build_daily_aggregate_view_sql(table_name: str, resolved_columns: Dict[str, str]) -> str:
    return _AGGREGATION_BUILDER._build_daily_aggregate_view_sql(table_name, resolved_columns)


def prepare_forecasting_materialized_views(
    source_tables: Optional[Sequence[str]] = None,
    *,
    refresh_existing: bool = True,
) -> List[str]:
    return _AGGREGATION_BUILDER.prepare_forecasting_materialized_views(
        source_tables=source_tables,
        refresh_existing=refresh_existing,
    )


def _build_scope_conditions(
    resolved_columns: Dict[str, str],
    min_year: Optional[int] = None,
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
) -> tuple[Optional[str], list[str], Dict[str, Any], bool]:
    return _SOURCE_BUILDER._build_scope_conditions(
        resolved_columns,
        min_year=min_year,
        district=district,
        cause=cause,
        object_category=object_category,
    )


def _load_forecasting_records(
    table_name: str,
    resolved_columns: Dict[str, str],
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    min_year: Optional[int] = None,
) -> List[Dict[str, Any]]:
    return _SOURCE_BUILDER._load_forecasting_records(
        table_name,
        resolved_columns,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )


def _load_option_counts(
    table_name: str,
    resolved_columns: Dict[str, str],
    field_name: str,
    min_year: Optional[int] = None,
) -> Counter:
    return _SOURCE_BUILDER._load_option_counts(
        table_name,
        resolved_columns,
        field_name,
        min_year=min_year,
    )


def _load_all_option_counts(
    table_name: str,
    resolved_columns: Dict[str, str],
    min_year: Optional[int] = None,
) -> Dict[str, Counter]:
    return _SOURCE_BUILDER._load_all_option_counts(
        table_name,
        resolved_columns,
        min_year=min_year,
    )


def _build_options_from_counter(counter: Counter, default_label: str) -> List[Dict[str, str]]:
    return _PAYLOAD_BUILDER._build_options_from_counter(counter, default_label)


def _build_option_catalog_sql(
    source_tables: Sequence[str],
    history_window: str = "all",
    metadata_items: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, List[Dict[str, str]]]:
    return _PAYLOAD_BUILDER._build_option_catalog_sql(
        source_tables,
        history_window=history_window,
        metadata_items=metadata_items,
    )


def _load_daily_history_rows(
    table_name: str,
    resolved_columns: Dict[str, str],
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    min_year: Optional[int] = None,
) -> List[Dict[str, Any]]:
    return _SOURCE_BUILDER._load_daily_history_rows(
        table_name,
        resolved_columns,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )


def _daily_history_rows_from_query_rows(rows: Sequence[Any]) -> List[Dict[str, Any]]:
    return _AGGREGATION_BUILDER._daily_history_rows_from_query_rows(rows)


def _execute_daily_history_row_query(query: Any, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    return _AGGREGATION_BUILDER._execute_daily_history_row_query(query, params)


def _load_materialized_daily_history_rows(
    table_name: str,
    resolved_columns: Dict[str, str],
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> List[Dict[str, Any]]:
    return _SOURCE_BUILDER._load_materialized_daily_history_rows(
        table_name,
        resolved_columns,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )


def _daily_history_source_select_parts(
    date_expression: str,
    resolved_columns: Dict[str, str],
) -> List[str]:
    return _SOURCE_BUILDER._daily_history_source_select_parts(date_expression, resolved_columns)


def _load_source_daily_history_rows(
    table_name: str,
    resolved_columns: Dict[str, str],
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> List[Dict[str, Any]]:
    return _SOURCE_BUILDER._load_source_daily_history_rows(
        table_name,
        resolved_columns,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )


def _daily_history_union_materialized_part_sql(
    table_name: str,
    resolved_columns: Dict[str, str],
    params: Dict[str, Any],
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> Optional[str]:
    return _SOURCE_BUILDER._daily_history_union_materialized_part_sql(
        table_name,
        resolved_columns,
        params,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )


def _daily_history_union_source_part_sql(
    table_name: str,
    resolved_columns: Dict[str, str],
    params: Dict[str, Any],
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> Optional[str]:
    return _SOURCE_BUILDER._daily_history_union_source_part_sql(
        table_name,
        resolved_columns,
        params,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )


def _load_daily_history_rows_union(
    metadata_items: Sequence[Dict[str, Any]],
    *,
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    min_year: Optional[int] = None,
) -> Optional[List[Dict[str, Any]]]:
    return _SOURCE_BUILDER._load_daily_history_rows_union(
        metadata_items,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )


def _daily_history_union_query_parts(
    metadata_items: Sequence[Dict[str, Any]],
    params: Dict[str, Any],
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> List[str]:
    return _SOURCE_BUILDER._daily_history_union_query_parts(
        metadata_items,
        params,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )


def _build_daily_history_union_query(query_parts: Sequence[str]) -> Any:
    return _AGGREGATION_BUILDER._build_daily_history_union_query(query_parts)


def _merge_daily_history_rows(
    merged_rows: Dict[date, Dict[str, Any]],
    table_rows: Sequence[Dict[str, Any]],
) -> None:
    _AGGREGATION_BUILDER._merge_daily_history_rows(merged_rows, table_rows)


def _load_scope_total_count(
    table_name: str,
    resolved_columns: Dict[str, str],
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    min_year: Optional[int] = None,
) -> int:
    return _SOURCE_BUILDER._load_scope_total_count(
        table_name,
        resolved_columns,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )


def _daily_history_cache_key(
    normalized_tables: Sequence[str],
    history_window: str,
    district: str,
    cause: str,
    object_category: str,
) -> tuple[Any, ...]:
    return _PAYLOAD_BUILDER._daily_history_cache_key(
        normalized_tables,
        history_window,
        district,
        cause,
        object_category,
    )


def _daily_history_cache_keys(
    normalized_tables: Sequence[str],
    history_window: str,
    district: str,
    cause: str,
    object_category: str,
) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
    return _PAYLOAD_BUILDER._daily_history_cache_keys(
        normalized_tables,
        history_window,
        district,
        cause,
        object_category,
    )


def _record_daily_history_perf(
    perf: Any,
    *,
    cache_hit: bool,
    trace: _DailyHistoryLoadTrace,
    table_count: int,
    row_count: int,
    total_count: int,
) -> None:
    _PAYLOAD_BUILDER._record_daily_history_perf(
        perf,
        cache_hit=cache_hit,
        trace=trace,
        table_count=table_count,
        row_count=row_count,
        total_count=total_count,
    )


def _lookup_daily_history_cache(
    cache_key: tuple[Any, ...],
    count_cache_key: tuple[Any, ...],
    perf: Any,
) -> Optional[List[Dict[str, Any]]]:
    return _PAYLOAD_BUILDER._lookup_daily_history_cache(cache_key, count_cache_key, perf)


def _load_daily_history_metadata_and_min_year(
    normalized_tables: Sequence[str],
    history_window: str,
    metadata_items: Optional[Sequence[Dict[str, Any]]],
) -> tuple[List[Dict[str, Any]], Optional[int]]:
    return _PAYLOAD_BUILDER._load_daily_history_metadata_and_min_year(
        normalized_tables,
        history_window,
        metadata_items,
    )


def _load_daily_history_union_fast_path(
    metadata_items: Sequence[Dict[str, Any]],
    trace: _DailyHistoryLoadTrace,
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> Optional[Dict[date, Dict[str, Any]]]:
    return _PAYLOAD_BUILDER._load_daily_history_union_fast_path(
        metadata_items,
        trace,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )


def _daily_history_rows_from_record_fallback(records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _SOURCE_BUILDER._daily_history_rows_from_record_fallback(records)


def _load_table_daily_history_record_fallback(
    table_name: str,
    resolved_columns: Dict[str, str],
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> List[Dict[str, Any]]:
    return _SOURCE_BUILDER._load_table_daily_history_record_fallback(
        table_name,
        resolved_columns,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )


def _load_daily_history_per_table_fallback(
    metadata_items: Sequence[Dict[str, Any]],
    *,
    district: str,
    cause: str,
    object_category: str,
    min_year: Optional[int],
) -> Dict[date, Dict[str, Any]]:
    return _SOURCE_BUILDER._load_daily_history_per_table_fallback(
        metadata_items,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
    )


def _dense_daily_history_from_merged_rows(
    merged_rows: Dict[date, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return _AGGREGATION_BUILDER._dense_daily_history_from_merged_rows(merged_rows)


def _cache_daily_history_result(
    cache_key: tuple[Any, ...],
    count_cache_key: tuple[Any, ...],
    history: List[Dict[str, Any]],
    *,
    perf: Any,
    trace: _DailyHistoryLoadTrace,
    table_count: int,
) -> List[Dict[str, Any]]:
    return _PAYLOAD_BUILDER._cache_daily_history_result(
        cache_key,
        count_cache_key,
        history,
        perf=perf,
        trace=trace,
        table_count=table_count,
    )


def _load_daily_history_uncached(
    normalized_tables: Sequence[str],
    history_window: str,
    *,
    district: str,
    cause: str,
    object_category: str,
    metadata_items: Optional[Sequence[Dict[str, Any]]],
) -> tuple[List[Dict[str, Any]], _DailyHistoryLoadTrace, int]:
    return _PAYLOAD_BUILDER._load_daily_history_uncached(
        normalized_tables,
        history_window,
        district=district,
        cause=cause,
        object_category=object_category,
        metadata_items=metadata_items,
    )


def _build_daily_history_sql(
    source_tables: Sequence[str],
    history_window: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    metadata_items: Optional[Sequence[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    return _PAYLOAD_BUILDER._build_daily_history_sql(
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
    metadata_items: Optional[Sequence[Dict[str, Any]]] = None,
) -> int:
    return _PAYLOAD_BUILDER._count_forecasting_records_sql(
        source_tables,
        history_window=history_window,
        district=district,
        cause=cause,
        object_category=object_category,
        metadata_items=metadata_items,
    )
