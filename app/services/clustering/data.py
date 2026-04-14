from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

import pandas as pd

from app.services.shared.data_base import DataLoader

from . import data_impl as _impl


class ClusteringDataLoader(DataLoader):
    def __init__(self) -> None:
        super().__init__(cache=None, cache_namespace="clustering_data")

    def build_table_options(self) -> List[Dict[str, str]]:
        return _impl._build_table_options()

    def resolve_selected_table(self, table_options: List[Dict[str, str]], table_name: str) -> str:
        return _impl._resolve_selected_table(table_options, table_name)

    def parse_cluster_count(self, value: str) -> int:
        return _impl._parse_cluster_count(value)

    def parse_sample_limit(self, value: str) -> int:
        return _impl._parse_sample_limit(value)

    def parse_sampling_strategy(self, value: str) -> str:
        return _impl._parse_sampling_strategy(value)

    def load_territory_dataset(self, table_name: str, sample_limit: int, sampling_strategy: str) -> Dict[str, Any]:
        return _impl._load_territory_dataset(table_name, sample_limit, sampling_strategy)

    def resolve_selected_features(
        self,
        available_features: Sequence[str],
        requested_features: Sequence[str],
        feature_frame: pd.DataFrame | None = None,
        entity_frame: pd.DataFrame | None = None,
        cluster_count: int = 4,
    ) -> Tuple[List[str], str]:
        return _impl._resolve_selected_features(
            available_features,
            requested_features,
            feature_frame=feature_frame,
            entity_frame=entity_frame,
            cluster_count=cluster_count,
        )

    def build_feature_options(self, candidate_features: Sequence[Dict[str, Any]], selected_features: Sequence[str]) -> List[Dict[str, Any]]:
        return _impl._build_feature_options(candidate_features, selected_features)

    def prepare_cluster_frame(
        self,
        feature_frame: pd.DataFrame,
        entity_frame: pd.DataFrame,
        selected_features: Sequence[str],
    ) -> Tuple[pd.DataFrame, pd.DataFrame, int]:
        return _impl._prepare_cluster_frame(feature_frame, entity_frame, selected_features)


_LOADER = ClusteringDataLoader()


def _build_table_options() -> List[Dict[str, str]]:
    return _LOADER.build_table_options()


def _resolve_selected_table(table_options: List[Dict[str, str]], table_name: str) -> str:
    return _LOADER.resolve_selected_table(table_options, table_name)


def _parse_cluster_count(value: str) -> int:
    return _LOADER.parse_cluster_count(value)


def _parse_sample_limit(value: str) -> int:
    return _LOADER.parse_sample_limit(value)


def _parse_sampling_strategy(value: str) -> str:
    return _LOADER.parse_sampling_strategy(value)


def _load_territory_dataset(table_name: str, sample_limit: int, sampling_strategy: str) -> Dict[str, Any]:
    return _LOADER.load_territory_dataset(table_name, sample_limit, sampling_strategy)


def _resolve_selected_features(
    available_features: Sequence[str],
    requested_features: Sequence[str],
    feature_frame: pd.DataFrame | None = None,
    entity_frame: pd.DataFrame | None = None,
    cluster_count: int = 4,
) -> Tuple[List[str], str]:
    return _LOADER.resolve_selected_features(
        available_features,
        requested_features,
        feature_frame=feature_frame,
        entity_frame=entity_frame,
        cluster_count=cluster_count,
    )


def _build_feature_options(candidate_features: Sequence[Dict[str, Any]], selected_features: Sequence[str]) -> List[Dict[str, Any]]:
    return _LOADER.build_feature_options(candidate_features, selected_features)


def _prepare_cluster_frame(
    feature_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    selected_features: Sequence[str],
) -> Tuple[pd.DataFrame, pd.DataFrame, int]:
    return _LOADER.prepare_cluster_frame(feature_frame, entity_frame, selected_features)

def _aggregate_territory_frame(records: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    return _impl._aggregate_territory_frame(records)


def _shrink_rate(
    successes: float,
    support: float,
    prior_rate: float | None,
    prior_strength: float,
) -> float:
    return _impl._shrink_rate(successes, support, prior_rate, prior_strength)


__all__ = [
    "ClusteringDataLoader",
    "_build_table_options",
    "_resolve_selected_table",
    "_parse_cluster_count",
    "_parse_sample_limit",
    "_parse_sampling_strategy",
    "_load_territory_dataset",
    "_resolve_selected_features",
    "_build_feature_options",
    "_prepare_cluster_frame",
    "_aggregate_territory_frame",
    "_shrink_rate",
]
