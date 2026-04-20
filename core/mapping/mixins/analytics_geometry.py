from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Iterable

import numpy as np

from ...types import ProcessedRecord


def project_records_to_local_xy(records: list[ProcessedRecord]) -> np.ndarray:
    coords = np.array([[item['latitude'], item['longitude']] for item in records], dtype=float)
    lat0 = float(np.mean(coords[:, 0]))
    lon0 = float(np.mean(coords[:, 1]))
    cos_lat = max(math.cos(math.radians(lat0)), 0.1)
    return np.column_stack([
        (coords[:, 1] - lon0) * 111.320 * cos_lat,
        (coords[:, 0] - lat0) * 110.574,
    ])


def group_records_by_field(
    records: Iterable[ProcessedRecord],
    field_name: str,
) -> dict[Any, list[ProcessedRecord]]:
    grouped: dict[Any, list[ProcessedRecord]] = defaultdict(list)
    for item in records:
        grouped[item[field_name]].append(item)
    return dict(grouped)


def group_records_by_cluster_label(
    records: list[ProcessedRecord],
    labels: Iterable[Any],
) -> dict[int, list[ProcessedRecord]]:
    grouped: dict[int, list[ProcessedRecord]] = defaultdict(list)
    for index, label in enumerate(labels):
        if label >= 0:
            grouped[int(label)].append(records[index])
    return dict(grouped)


def mean_record_value(
    records: Iterable[ProcessedRecord],
    field_name: str,
) -> float | None:
    values = [item[field_name] for item in records if item[field_name] is not None]
    return float(np.mean(values)) if values else None


def nanmean_record_value(
    records: Iterable[ProcessedRecord],
    field_name: str,
) -> float | None:
    values = [item[field_name] for item in records if item[field_name] is not None]
    return float(np.nanmean(values)) if values else None


__all__ = [
    "group_records_by_cluster_label",
    "group_records_by_field",
    "mean_record_value",
    "nanmean_record_value",
    "project_records_to_local_xy",
]
