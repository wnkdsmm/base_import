from __future__ import annotations

from typing import Any, Sequence

from app.services.shared.data_base import DataLoader

from . import data_impl as _impl
from .types import RiskDataRecord, RiskScopeParams, RiskTableMetadata


class ForecastRiskDataLoader(DataLoader):
    def __init__(self) -> None:
        super().__init__(cache=None, cache_namespace="forecast_risk_data")

    def collect_risk_metadata(self, source_tables: Sequence[str]) -> tuple[list[RiskTableMetadata], list[str]]:
        return self.collect_with_notes(
            source_tables,
            _impl._load_table_metadata,
            note_builder=lambda source_table, exc: f"{source_table}: {exc}",
        )

    def history_window_year_span(self, history_window: str) -> int:
        return _impl._history_window_year_span(history_window)

    def resolve_history_window_min_year(
        self,
        metadata_items: Sequence[RiskTableMetadata],
        history_window: str,
    ) -> int | None:
        return _impl._resolve_history_window_min_year(metadata_items, history_window)

    def build_scope_conditions(
        self,
        resolved_columns: dict[str, str],
        min_year: int | None = None,
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
        selected_year: int | None = None,
    ) -> tuple[str | None, list[str], RiskScopeParams, bool]:
        return _impl._build_scope_conditions(
            resolved_columns,
            min_year=min_year,
            district=district,
            cause=cause,
            object_category=object_category,
            selected_year=selected_year,
        )

    def collect_risk_inputs(
        self,
        source_tables: Sequence[str],
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
        history_window: str = "all",
        selected_year: int | None = None,
    ) -> tuple[list[RiskTableMetadata], list[RiskDataRecord], list[str]]:
        metadata_items, notes = self.collect_risk_metadata(source_tables)
        min_year = self.resolve_history_window_min_year(metadata_items, history_window)

        record_batches, load_notes = self.collect_with_notes(
            metadata_items,
            lambda metadata: self.load_risk_records(
                metadata["table_name"],
                metadata["resolved_columns"],
                district=district,
                cause=cause,
                object_category=object_category,
                min_year=min_year,
                selected_year=selected_year,
            ),
            note_builder=lambda metadata, exc: f"{metadata.get('table_name')}: {exc}",
        )
        notes.extend(load_notes)

        records: list[RiskDataRecord] = []
        for batch in record_batches:
            records.extend(batch)
        return metadata_items, records, notes

    def load_table_metadata(self, table_name: str) -> RiskTableMetadata:
        return _impl._load_table_metadata(table_name)

    def load_risk_records(
        self,
        table_name: str,
        resolved_columns: dict[str, str],
        district: str = "all",
        cause: str = "all",
        object_category: str = "all",
        min_year: int | None = None,
        selected_year: int | None = None,
    ) -> list[RiskDataRecord]:
        return _impl._load_risk_records(
            table_name,
            resolved_columns,
            district=district,
            cause=cause,
            object_category=object_category,
            min_year=min_year,
            selected_year=selected_year,
        )


_LOADER = ForecastRiskDataLoader()


def _collect_risk_metadata(source_tables: Sequence[str]) -> tuple[list[RiskTableMetadata], list[str]]:
    return _LOADER.collect_risk_metadata(source_tables)


def _history_window_year_span(history_window: str) -> int:
    return _LOADER.history_window_year_span(history_window)


def _resolve_history_window_min_year(metadata_items: Sequence[RiskTableMetadata], history_window: str) -> int | None:
    return _LOADER.resolve_history_window_min_year(metadata_items, history_window)


def _build_scope_conditions(
    resolved_columns: dict[str, str],
    min_year: int | None = None,
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    selected_year: int | None = None,
) -> tuple[str | None, list[str], RiskScopeParams, bool]:
    return _LOADER.build_scope_conditions(
        resolved_columns,
        min_year=min_year,
        district=district,
        cause=cause,
        object_category=object_category,
        selected_year=selected_year,
    )


def _collect_risk_inputs(
    source_tables: Sequence[str],
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    history_window: str = "all",
    selected_year: int | None = None,
) -> tuple[list[RiskTableMetadata], list[RiskDataRecord], list[str]]:
    return _LOADER.collect_risk_inputs(
        source_tables,
        district=district,
        cause=cause,
        object_category=object_category,
        history_window=history_window,
        selected_year=selected_year,
    )


def _load_table_metadata(table_name: str) -> RiskTableMetadata:
    return _LOADER.load_table_metadata(table_name)


def _load_risk_records(
    table_name: str,
    resolved_columns: dict[str, str],
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    min_year: int | None = None,
    selected_year: int | None = None,
) -> list[RiskDataRecord]:
    return _LOADER.load_risk_records(
        table_name,
        resolved_columns,
        district=district,
        cause=cause,
        object_category=object_category,
        min_year=min_year,
        selected_year=selected_year,
    )

__all__ = [
    "ForecastRiskDataLoader",
    "_collect_risk_metadata",
    "_history_window_year_span",
    "_resolve_history_window_min_year",
    "_build_scope_conditions",
    "_collect_risk_inputs",
    "_load_table_metadata",
    "_load_risk_records",
]
