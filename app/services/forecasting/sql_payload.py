from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional, Sequence

from app.perf import current_perf_trace

from .selection import _canonicalize_source_tables, _normalize_filter_value
from .sql_aggregations import AggregationQueryBuilder, QueryBuilder
from .sql_sources import SourceQueryBuilder
from .types import ForecastingOptionCatalog, ForecastingTableMetadata, SqlRow, TableOption


@dataclass
class _DailyHistoryLoadTrace:
    used_union_fast_path: bool = False
    union_fast_path_attempted: bool = False
    union_fast_path_fallback: bool = False
    union_fast_path_error_type: str = ""
    union_fast_path_rows: int = 0


class PayloadQueryBuilder(QueryBuilder):
    def __init__(
        self,
        aggregations: AggregationQueryBuilder,
        sources: SourceQueryBuilder,
        hook_resolver=None,
    ) -> None:
        super().__init__(hook_resolver)
        self._aggregations = aggregations
        self._sources = sources

    def _build_options_from_counter(self, counter: Counter, default_label: str) -> list[TableOption]:
        options = [{"value": "all", "label": default_label}]
        for value, count in sorted(counter.items(), key=lambda item: (-item[1], item[0].lower()))[:200]:
            options.append({"value": value, "label": f"{value} ({count})"})
        return options

    def _build_option_catalog_sql(
        self,
        source_tables: Sequence[str],
        history_window: str = "all",
        metadata_items: Optional[Sequence[ForecastingTableMetadata]] = None,
    ) -> ForecastingOptionCatalog:
        from .sources import _collect_forecasting_metadata, _resolve_history_window_min_year

        normalized_tables = _canonicalize_source_tables(source_tables)[0]
        cache_key = self._build_sql_cache_key("option_catalog", normalized_tables, history_window)
        cached_catalog = self.cache.get(cache_key)
        if cached_catalog is not None:
            return cached_catalog

        local_metadata_items = list(metadata_items) if metadata_items is not None else _collect_forecasting_metadata(normalized_tables)[0]
        min_year = _resolve_history_window_min_year(local_metadata_items, history_window)
        district_counter: Counter = Counter()
        cause_counter: Counter = Counter()
        category_counter: Counter = Counter()
        load_all_option_counts = self._resolve_hook("_load_all_option_counts", self._sources._load_all_option_counts)

        for metadata in local_metadata_items:
            resolved_columns = metadata.get("resolved_columns") or {}
            table_name = str(metadata.get("table_name") or "")
            if not table_name:
                continue
            table_counters = load_all_option_counts(table_name, resolved_columns, min_year=min_year)
            district_counter.update(table_counters.get("district", Counter()))
            cause_counter.update(table_counters.get("cause", Counter()))
            category_counter.update(table_counters.get("object_category", Counter()))

        payload = {
            "districts": self._build_options_from_counter(district_counter, "Все районы"),
            "causes": self._build_options_from_counter(cause_counter, "Все причины"),
            "object_categories": self._build_options_from_counter(category_counter, "Все категории"),
        }
        return self.cache.set(cache_key, payload)

    def _daily_history_cache_key(
        self,
        normalized_tables: Sequence[str],
        history_window: str,
        district: str,
        cause: str,
        object_category: str,
    ) -> tuple[Any, ...]:
        return self._build_sql_cache_key(
            "daily_history",
            normalized_tables,
            history_window,
            _normalize_filter_value(district),
            _normalize_filter_value(cause),
            _normalize_filter_value(object_category),
        )

    def _daily_history_cache_keys(
        self,
        normalized_tables: Sequence[str],
        history_window: str,
        district: str,
        cause: str,
        object_category: str,
    ) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
        return (
            self._daily_history_cache_key(
                normalized_tables,
                history_window,
                district,
                cause,
                object_category,
            ),
            self._filtered_record_count_cache_key(
                normalized_tables,
                history_window,
                district,
                cause,
                object_category,
            ),
        )

    def _record_daily_history_perf(
        self,
        perf: Any,
        *,
        cache_hit: bool,
        trace: _DailyHistoryLoadTrace,
        table_count: int,
        row_count: int,
        total_count: int,
    ) -> None:
        if perf is None:
            return
        perf.update(
            forecasting_daily_history_cache_hit=cache_hit,
            forecasting_daily_history_union_fast_path=trace.used_union_fast_path,
            forecasting_daily_history_union_attempted=trace.union_fast_path_attempted,
            forecasting_daily_history_union_fallback=trace.union_fast_path_fallback,
            forecasting_daily_history_union_error_type=trace.union_fast_path_error_type,
            forecasting_daily_history_union_rows=trace.union_fast_path_rows,
            forecasting_daily_history_tables=table_count,
            forecasting_daily_history_rows=row_count,
            forecasting_daily_history_total_count=total_count,
            forecasting_daily_history_count_cache_populated=True,
        )

    def _lookup_daily_history_cache(
        self,
        cache_key: tuple[Any, ...],
        count_cache_key: tuple[Any, ...],
        perf: Any,
    ) -> Optional[list[SqlRow]]:
        cached_history = self.cache.get(cache_key)
        if cached_history is None:
            return None
        cached_total_count = self._aggregations._daily_history_total_count(cached_history)
        self.cache.set(count_cache_key, cached_total_count)
        if perf is not None:
            perf.update(
                forecasting_daily_history_cache_hit=True,
                forecasting_daily_history_rows=len(cached_history),
                forecasting_daily_history_total_count=cached_total_count,
                forecasting_daily_history_count_cache_populated=True,
            )
        return cached_history

    def _load_daily_history_metadata_and_min_year(
        self,
        normalized_tables: Sequence[str],
        history_window: str,
        metadata_items: Optional[Sequence[ForecastingTableMetadata]],
    ) -> tuple[list[ForecastingTableMetadata], Optional[int]]:
        from .sources import _collect_forecasting_metadata, _resolve_history_window_min_year

        local_metadata_items = list(metadata_items) if metadata_items is not None else _collect_forecasting_metadata(normalized_tables)[0]
        return local_metadata_items, _resolve_history_window_min_year(local_metadata_items, history_window)

    def _load_daily_history_union_fast_path(
        self,
        metadata_items: Sequence[ForecastingTableMetadata],
        trace: _DailyHistoryLoadTrace,
        *,
        district: str,
        cause: str,
        object_category: str,
        min_year: Optional[int],
    ) -> Optional[dict[date, SqlRow]]:
        trace.union_fast_path_attempted = len(metadata_items) > 1
        if not trace.union_fast_path_attempted:
            return None

        load_union = self._resolve_hook("_load_daily_history_rows_union", self._sources._load_daily_history_rows_union)
        try:
            union_rows = load_union(
                metadata_items,
                district=district,
                cause=cause,
                object_category=object_category,
                min_year=min_year,
            )
        except Exception as exc:
            trace.union_fast_path_fallback = True
            trace.union_fast_path_error_type = exc.__class__.__name__
            return None

        if union_rows is None:
            return None

        trace.used_union_fast_path = True
        trace.union_fast_path_rows = len(union_rows)
        merged_rows: dict[date, SqlRow] = {}
        self._aggregations._merge_daily_history_rows(merged_rows, union_rows)
        return merged_rows

    def _load_daily_history_uncached(
        self,
        normalized_tables: Sequence[str],
        history_window: str,
        *,
        district: str,
        cause: str,
        object_category: str,
        metadata_items: Optional[Sequence[ForecastingTableMetadata]],
    ) -> tuple[list[SqlRow], _DailyHistoryLoadTrace, int]:
        local_metadata_items, min_year = self._load_daily_history_metadata_and_min_year(
            normalized_tables,
            history_window,
            metadata_items,
        )
        trace = _DailyHistoryLoadTrace()
        merged_rows = self._load_daily_history_union_fast_path(
            local_metadata_items,
            trace,
            district=district,
            cause=cause,
            object_category=object_category,
            min_year=min_year,
        )
        if merged_rows is None:
            per_table_fallback = self._resolve_hook(
                "_load_daily_history_per_table_fallback",
                self._sources._load_daily_history_per_table_fallback,
            )
            merged_rows = per_table_fallback(
                local_metadata_items,
                district=district,
                cause=cause,
                object_category=object_category,
                min_year=min_year,
            )
        history = self._aggregations._dense_daily_history_from_merged_rows(merged_rows)
        return history, trace, len(local_metadata_items)

    def _cache_daily_history_result(
        self,
        cache_key: tuple[Any, ...],
        count_cache_key: tuple[Any, ...],
        history: list[SqlRow],
        *,
        perf: Any,
        trace: _DailyHistoryLoadTrace,
        table_count: int,
    ) -> list[SqlRow]:
        total_count = self._aggregations._daily_history_total_count(history)
        self.cache.set(count_cache_key, total_count)
        self._record_daily_history_perf(
            perf,
            cache_hit=False,
            trace=trace,
            table_count=table_count,
            row_count=len(history),
            total_count=total_count,
        )
        return self.cache.set(cache_key, history)

    def _build_daily_history_sql(
        self,
        source_tables: Sequence[str],
        history_window: str = "all",
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
        metadata_items: Optional[Sequence[ForecastingTableMetadata]] = None,
    ) -> list[SqlRow]:
        perf_getter = self._resolve_hook("current_perf_trace", current_perf_trace)
        perf = perf_getter()
        normalized_tables = _canonicalize_source_tables(source_tables)[0]
        cache_key, count_cache_key = self._daily_history_cache_keys(
            normalized_tables,
            history_window,
            district,
            cause,
            object_category,
        )
        cached_history = self._lookup_daily_history_cache(cache_key, count_cache_key, perf)
        if cached_history is not None:
            return cached_history

        history, trace, table_count = self._load_daily_history_uncached(
            normalized_tables,
            history_window,
            district=district,
            cause=cause,
            object_category=object_category,
            metadata_items=metadata_items,
        )
        return self._cache_daily_history_result(
            cache_key,
            count_cache_key,
            history,
            perf=perf,
            trace=trace,
            table_count=table_count,
        )

    def _count_forecasting_records_sql(
        self,
        source_tables: Sequence[str],
        history_window: str = "all",
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
        metadata_items: Optional[Sequence[ForecastingTableMetadata]] = None,
    ) -> int:
        from .sources import _collect_forecasting_metadata, _resolve_history_window_min_year

        perf_getter = self._resolve_hook("current_perf_trace", current_perf_trace)
        perf = perf_getter()
        normalized_tables = _canonicalize_source_tables(source_tables)[0]
        cache_key = self._filtered_record_count_cache_key(
            normalized_tables,
            history_window,
            district,
            cause,
            object_category,
        )
        cached_count = self.cache.get(cache_key)
        if cached_count is not None:
            if perf is not None:
                perf.update(
                    forecasting_filtered_record_count_cache_hit=True,
                    forecasting_filtered_record_count=int(cached_count),
                )
            return int(cached_count)

        local_metadata_items = list(metadata_items) if metadata_items is not None else _collect_forecasting_metadata(normalized_tables)[0]
        min_year = _resolve_history_window_min_year(local_metadata_items, history_window)
        total_count = 0
        load_scope_total_count = self._resolve_hook("_load_scope_total_count", self._sources._load_scope_total_count)

        for metadata in local_metadata_items:
            table_name = str(metadata.get("table_name") or "")
            resolved_columns = metadata.get("resolved_columns") or {}
            if not table_name:
                continue
            total_count += load_scope_total_count(
                table_name,
                resolved_columns,
                district=district,
                cause=cause,
                object_category=object_category,
                min_year=min_year,
            )

        cached_total = int(self.cache.set(cache_key, total_count))
        if perf is not None:
            perf.update(
                forecasting_filtered_record_count_cache_hit=False,
                forecasting_filtered_record_count=cached_total,
            )
        return cached_total
