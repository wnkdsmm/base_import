from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.services.shared.data_base import DataLoader

from . import data_impl as _impl
from .types import (
    AccessPointInput,
    AccessPointMetadata,
    AccessPointsDataPayload,
    OptionItem,
)


class AccessPointsDataLoader(DataLoader):
    def __init__(self) -> None:
        super().__init__(cache=None, cache_namespace="access_points_data")

    def build_table_options(self) -> List[OptionItem]:
        return _impl._build_access_points_table_options()

    def resolve_selected_table(self, table_options: Sequence[OptionItem], table_name: str) -> str:
        return _impl._resolve_selected_table(table_options, table_name)

    def selected_source_tables(self, table_options: Sequence[OptionItem], selected_table: str) -> List[str]:
        return _impl._selected_source_tables(table_options, selected_table)

    def parse_limit(self, value: str) -> int:
        return _impl._parse_limit(value)

    def resolve_option_value(self, options: Sequence[OptionItem], selected_value: object, default: str = "all") -> str:
        return DataLoader.resolve_option_value(options, selected_value, default=default)

    def collect_access_point_metadata(self, source_tables: Sequence[str]) -> Tuple[List[AccessPointMetadata], List[str]]:
        metadata_items, notes = self.collect_with_notes(source_tables, _impl._load_table_metadata)
        normalized_items: List[dict[str, Any]] = []
        for metadata in metadata_items:
            normalized_items.append(
                {
                    "table_name": metadata["table_name"],
                    "columns": list(metadata["columns"]),
                    "resolved_columns": dict(metadata["resolved_columns"]),
                }
            )
        return normalized_items, notes

    def collect_access_point_inputs(
        self,
        source_tables: Sequence[str],
        *,
        district: str = "all",
        selected_year: Optional[int] = None,
        metadata_items: Optional[Sequence[AccessPointMetadata]] = None,
    ) -> Tuple[List[AccessPointInput], List[str]]:
        del metadata_items
        table_payloads, notes = self.collect_with_notes(
            source_tables,
            lambda table_name: (table_name, _impl._collect_source_records(table_name)),
        )
        records: List[dict[str, Any]] = []
        for table_name, raw_records in table_payloads:
            for record in raw_records:
                records.append(_impl._record_to_access_point_input(record, source_table=table_name))

        normalized_district = self.normalize_value(district, default="all").lower()
        if normalized_district != "all":
            records = self.filter_records(
                records,
                lambda record: _impl._clean_text(record.get("district")).lower() == normalized_district,
            )
        if selected_year is not None:
            records = self.filter_records(records, lambda record: record.get("year") == int(selected_year))
        return records, notes

    def build_option_catalog(
        self,
        source_tables: Sequence[str],
        *,
        metadata_items: Optional[Sequence[AccessPointMetadata]] = None,
    ) -> Dict[str, List[OptionItem]]:
        records, _notes = self.collect_access_point_inputs(source_tables, metadata_items=metadata_items)
        return {
            "districts": _impl._collect_available_districts(records),
            "years": _impl._collect_available_years(records),
        }

    def get_access_points_data(self, **kwargs: Any) -> AccessPointsDataPayload:
        return _impl.get_access_points_data(**kwargs)


_LOADER = AccessPointsDataLoader()


def _build_access_points_table_options() -> List[OptionItem]:
    return _LOADER.build_table_options()


def _resolve_selected_table(table_options: Sequence[OptionItem], table_name: str) -> str:
    return _LOADER.resolve_selected_table(table_options, table_name)


def _selected_source_tables(table_options: Sequence[OptionItem], selected_table: str) -> List[str]:
    return _LOADER.selected_source_tables(table_options, selected_table)


def _parse_limit(value: str) -> int:
    return _LOADER.parse_limit(value)


def _resolve_option_value(options: Sequence[OptionItem], selected_value: object, default: str = "all") -> str:
    return DataLoader.resolve_option_value(options, selected_value, default=default)


def _collect_access_point_metadata(source_tables: Sequence[str]) -> Tuple[List[AccessPointMetadata], List[str]]:
    return _LOADER.collect_access_point_metadata(source_tables)


def _collect_access_point_inputs(
    source_tables: Sequence[str],
    *,
    district: str = "all",
    selected_year: Optional[int] = None,
    metadata_items: Optional[Sequence[AccessPointMetadata]] = None,
) -> Tuple[List[AccessPointInput], List[str]]:
    return _LOADER.collect_access_point_inputs(
        source_tables,
        district=district,
        selected_year=selected_year,
        metadata_items=metadata_items,
    )


def _build_option_catalog(
    source_tables: Sequence[str],
    *,
    metadata_items: Optional[Sequence[AccessPointMetadata]] = None,
) -> Dict[str, List[OptionItem]]:
    return _LOADER.build_option_catalog(source_tables, metadata_items=metadata_items)


def get_access_points_data(**kwargs: Any) -> AccessPointsDataPayload:
    return _LOADER.get_access_points_data(**kwargs)

__all__ = [
    "AccessPointsDataLoader",
    "_build_access_points_table_options",
    "_resolve_selected_table",
    "_selected_source_tables",
    "_parse_limit",
    "_resolve_option_value",
    "_collect_access_point_metadata",
    "_collect_access_point_inputs",
    "_build_option_catalog",
    "get_access_points_data",
]
