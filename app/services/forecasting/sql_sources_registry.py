from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from .shaping import _build_daily_history


class SourceQueryRegistryMixin:
    def _load_daily_history_rows(
        self,
        table_name: str,
        resolved_columns: Dict[str, str],
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
        min_year: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if self._aggregations._daily_aggregate_view_exists(table_name):
            return self._load_materialized_daily_history_rows(
                table_name,
                resolved_columns,
                min_year=min_year,
                district=district,
                cause=cause,
                object_category=object_category,
            )

        return self._load_source_daily_history_rows(
            table_name,
            resolved_columns,
            min_year=min_year,
            district=district,
            cause=cause,
            object_category=object_category,
        )

    def _load_materialized_daily_history_rows(
        self,
        table_name: str,
        resolved_columns: Dict[str, str],
        *,
        district: str,
        cause: str,
        object_category: str,
        min_year: Optional[int],
    ) -> List[Dict[str, Any]]:
        return self._aggregations._load_materialized_daily_history_rows(
            table_name,
            resolved_columns,
            district=district,
            cause=cause,
            object_category=object_category,
            min_year=min_year,
        )

    def _load_daily_history_rows_union(
        self,
        metadata_items: Sequence[Dict[str, Any]],
        *,
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
        min_year: Optional[int] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        params: Dict[str, Any] = {}
        query_parts = self._daily_history_union_query_parts(
            metadata_items,
            params,
            district=district,
            cause=cause,
            object_category=object_category,
            min_year=min_year,
        )
        if not query_parts:
            return []
        return self._aggregations._execute_daily_history_row_query(
            self._aggregations._build_daily_history_union_query(query_parts),
            params,
        )

    def _daily_history_union_query_parts(
        self,
        metadata_items: Sequence[Dict[str, Any]],
        params: Dict[str, Any],
        *,
        district: str,
        cause: str,
        object_category: str,
        min_year: Optional[int],
    ) -> List[str]:
        query_parts: List[str] = []
        view_status_loader = self._resolve_hook(
            "_daily_aggregate_view_status_map",
            self._aggregations._daily_aggregate_view_status_map,
        )
        view_status = view_status_loader(
            [str(metadata.get("table_name") or "") for metadata in metadata_items]
        )
        for metadata in metadata_items:
            table_name = str(metadata.get("table_name") or "")
            resolved_columns = metadata.get("resolved_columns") or {}
            if not table_name:
                continue
            if view_status.get(table_name, False):
                query_part = self._daily_history_union_materialized_part_sql(
                    table_name,
                    resolved_columns,
                    params,
                    district=district,
                    cause=cause,
                    object_category=object_category,
                    min_year=min_year,
                )
            else:
                query_part = self._daily_history_union_source_part_sql(
                    table_name,
                    resolved_columns,
                    params,
                    district=district,
                    cause=cause,
                    object_category=object_category,
                    min_year=min_year,
                )
            if query_part:
                query_parts.append(query_part)
        return query_parts

    def _daily_history_rows_from_record_fallback(self, records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            {
                "date": item["date"],
                "count": int(item["count"]),
                "avg_temperature": item["avg_temperature"],
                "temperature_samples": 1 if item["avg_temperature"] is not None else 0,
            }
            for item in _build_daily_history(list(records))
        ]

    def _load_table_daily_history_record_fallback(
        self,
        table_name: str,
        resolved_columns: Dict[str, str],
        *,
        district: str,
        cause: str,
        object_category: str,
        min_year: Optional[int],
    ) -> List[Dict[str, Any]]:
        load_records = self._resolve_hook("_load_forecasting_records", self._load_forecasting_records)
        fallback_records = load_records(
            table_name,
            resolved_columns,
            district=district,
            cause=cause,
            object_category=object_category,
            min_year=min_year,
        )
        return self._daily_history_rows_from_record_fallback(fallback_records)

    def _load_daily_history_per_table_fallback(
        self,
        metadata_items: Sequence[Dict[str, Any]],
        *,
        district: str,
        cause: str,
        object_category: str,
        min_year: Optional[int],
    ) -> Dict[Any, Dict[str, Any]]:
        merged_rows: Dict[Any, Dict[str, Any]] = {}
        load_daily_history_rows = self._resolve_hook("_load_daily_history_rows", self._load_daily_history_rows)
        load_record_fallback = self._resolve_hook(
            "_load_table_daily_history_record_fallback",
            self._load_table_daily_history_record_fallback,
        )

        for metadata in metadata_items:
            table_name = str(metadata.get("table_name") or "")
            resolved_columns = metadata.get("resolved_columns") or {}
            if not table_name:
                continue
            try:
                table_rows = load_daily_history_rows(
                    table_name,
                    resolved_columns,
                    district=district,
                    cause=cause,
                    object_category=object_category,
                    min_year=min_year,
                )
            except Exception:
                table_rows = load_record_fallback(
                    table_name,
                    resolved_columns,
                    district=district,
                    cause=cause,
                    object_category=object_category,
                    min_year=min_year,
                )
            self._aggregations._merge_daily_history_rows(merged_rows, table_rows)
        return merged_rows
