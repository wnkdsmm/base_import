from __future__ import annotations

import math
from collections import Counter
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

from app.db_metadata import get_table_names_cached

from app.services.forecast_risk.data import _collect_risk_inputs
from app.services.forecast_risk.utils import _counter_top_label, _is_rural_label

from .constants import (
    CLUSTER_COUNT_OPTIONS,
    DEFAULT_CLUSTER_FEATURES,
    FEATURE_METADATA,
    MAX_FEATURE_OPTIONS,
    SAMPLE_LIMIT_OPTIONS,
    SAMPLING_STRATEGY_OPTIONS,
    TABLE_EXCLUDED_PREFIXES,
)
from .utils import _format_number, _format_percent


def _build_table_options() -> List[Dict[str, str]]:
    tables = []
    for table_name in get_table_names_cached():
        if table_name.startswith(TABLE_EXCLUDED_PREFIXES):
            continue
        tables.append({"value": table_name, "label": table_name})
    return tables



def _resolve_selected_table(table_options: List[Dict[str, str]], table_name: str) -> str:
    values = {item["value"] for item in table_options}
    if table_name in values:
        return table_name
    return table_options[0]["value"] if table_options else ""



def _parse_cluster_count(value: str) -> int:
    try:
        parsed = int(str(value).strip())
    except Exception:
        return 4
    if parsed in CLUSTER_COUNT_OPTIONS:
        return parsed
    return min(CLUSTER_COUNT_OPTIONS, key=lambda item: abs(item - parsed))



def _parse_sample_limit(value: str) -> int:
    try:
        parsed = int(str(value).strip())
    except Exception:
        return 200
    if parsed in SAMPLE_LIMIT_OPTIONS:
        return parsed
    return min(SAMPLE_LIMIT_OPTIONS, key=lambda item: abs(item - parsed))



def _parse_sampling_strategy(value: str) -> str:
    allowed = {item["value"] for item in SAMPLING_STRATEGY_OPTIONS}
    normalized = str(value or "").strip().lower()
    return normalized if normalized in allowed else SAMPLING_STRATEGY_OPTIONS[0]["value"]



def _load_territory_dataset(table_name: str, sample_limit: int, sampling_strategy: str) -> Dict[str, Any]:
    _, records, notes = _collect_risk_inputs([table_name])
    if not records:
        raise ValueError("В выбранной таблице не нашлось пожаров с датой и территориальной привязкой для кластеризации.")

    territory_frame = _aggregate_territory_frame(records)
    if territory_frame.empty:
        raise ValueError("Не удалось собрать агрегаты по территориям: проверьте наличие населённого пункта, района и базовых пожарных признаков.")

    sampled_frame, sampling_note = _sample_territory_frame(territory_frame, sample_limit, sampling_strategy)
    sampled_frame = sampled_frame.reset_index(drop=True)

    feature_columns = [name for name in FEATURE_METADATA if name in sampled_frame.columns]
    feature_frame = sampled_frame.loc[:, feature_columns].copy()
    candidate_features = _discover_candidate_features(feature_frame)

    return {
        "entity_frame": sampled_frame,
        "feature_frame": feature_frame,
        "candidate_features": candidate_features,
        "total_incidents": len(records),
        "total_entities": len(territory_frame),
        "sampled_entities": len(sampled_frame),
        "sampling_note": sampling_note,
        "notes": [note for note in notes if note],
    }



def _resolve_selected_features(available_features: Sequence[str], requested_features: Sequence[str]) -> Tuple[List[str], str]:
    allowed = set(available_features)
    normalized_requested = [item for item in requested_features if item in allowed]
    if len(normalized_requested) >= 2:
        return normalized_requested, ""

    fallback = [item for item in DEFAULT_CLUSTER_FEATURES if item in allowed]
    if len(fallback) < 2:
        fallback = list(available_features[: max(2, min(len(available_features), 6))])

    if requested_features:
        return fallback, "Часть выбранных агрегированных признаков недоступна, поэтому страница вернулась к базовому профилю территории риска."
    return fallback, "По умолчанию выбраны агрегированные признаки, которые лучше всего описывают тип территории риска, а не отдельный инцидент."



def _build_feature_options(candidate_features: Sequence[Dict[str, Any]], selected_features: Sequence[str]) -> List[Dict[str, Any]]:
    selected_set = set(selected_features)
    prioritized = list(candidate_features[:MAX_FEATURE_OPTIONS])
    selected_rows = [item for item in candidate_features if item["name"] in selected_set and item not in prioritized]
    rows = prioritized + selected_rows
    return [
        {
            "name": item["name"],
            "description": item.get("description", ""),
            "coverage_display": item["coverage_display"],
            "variance_display": item["variance_display"],
            "is_selected": item["name"] in selected_set,
        }
        for item in rows
    ]



def _prepare_cluster_frame(
    feature_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    selected_features: Sequence[str],
) -> Tuple[pd.DataFrame, pd.DataFrame, int]:
    selected_numeric = feature_frame.loc[:, list(selected_features)].apply(pd.to_numeric, errors="coerce")
    required_non_null = min(len(selected_features), max(2, math.ceil(len(selected_features) * 0.6)))
    row_mask = selected_numeric.notna().sum(axis=1) >= required_non_null

    prepared_numeric = selected_numeric.loc[row_mask].copy()
    prepared_entities = entity_frame.loc[row_mask].copy()
    if prepared_numeric.empty:
        return prepared_numeric, prepared_entities, len(feature_frame)

    prepared_numeric = prepared_numeric.fillna(prepared_numeric.median(numeric_only=True))
    prepared_numeric = prepared_numeric.reset_index(drop=True)
    prepared_entities = prepared_entities.reset_index(drop=True)
    excluded_rows = int(len(feature_frame) - len(prepared_numeric))
    return prepared_numeric, prepared_entities, excluded_rows



def _aggregate_territory_frame(records: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    buckets: Dict[str, Dict[str, Any]] = {}
    for record in records:
        label = record.get("territory_label") or record.get("district") or "Территория не указана"
        bucket = buckets.setdefault(
            label,
            {
                "label": label,
                "incidents": 0,
                "districts": Counter(),
                "settlement_types": Counter(),
                "area_sum": 0.0,
                "area_count": 0,
                "night_incidents": 0,
                "response_sum": 0.0,
                "response_count": 0,
                "long_arrivals": 0,
                "severe": 0,
                "water_known": 0,
                "water_available": 0,
                "distance_sum": 0.0,
                "distance_count": 0,
                "heating_incidents": 0,
            },
        )

        bucket["incidents"] += 1
        district_value = record.get("district") or label
        bucket["districts"][district_value] += 1
        settlement_type = record.get("settlement_type") or "Не указано"
        bucket["settlement_types"][settlement_type] += 1

        fire_area = record.get("fire_area")
        if fire_area is not None and fire_area >= 0:
            bucket["area_sum"] += float(fire_area)
            bucket["area_count"] += 1

        response_minutes = record.get("response_minutes")
        if response_minutes is not None:
            bucket["response_sum"] += float(response_minutes)
            bucket["response_count"] += 1
            if record.get("long_arrival"):
                bucket["long_arrivals"] += 1

        if record.get("severe_consequence"):
            bucket["severe"] += 1
        if record.get("night_incident"):
            bucket["night_incidents"] += 1
        if record.get("heating_season"):
            bucket["heating_incidents"] += 1

        if record.get("has_water_supply") is not None:
            bucket["water_known"] += 1
            if record.get("has_water_supply"):
                bucket["water_available"] += 1

        distance_value = record.get("fire_station_distance")
        if distance_value is not None:
            bucket["distance_sum"] += float(distance_value)
            bucket["distance_count"] += 1

    rows: List[Dict[str, Any]] = []
    for bucket in buckets.values():
        incidents = max(1, int(bucket["incidents"]))
        dominant_district = _counter_top_label(bucket["districts"], bucket["label"]) or bucket["label"]
        dominant_settlement_type = _counter_top_label(bucket["settlement_types"], "Не указано") or "Не указано"
        is_rural = _is_rural_label(dominant_settlement_type) or _is_rural_label(bucket["label"])
        rows.append(
            {
                "Территория": bucket["label"],
                "Район": dominant_district,
                "Тип территории": "Сельская территория" if is_rural else "Территория без выраженного сельского профиля",
                "Доминирующий тип населенного пункта": dominant_settlement_type,
                "Число пожаров": incidents,
                "Средняя площадь пожара": bucket["area_sum"] / bucket["area_count"] if bucket["area_count"] else np.nan,
                "Доля ночных пожаров": bucket["night_incidents"] / incidents,
                "Среднее время прибытия, мин": bucket["response_sum"] / bucket["response_count"] if bucket["response_count"] else np.nan,
                "Доля тяжелых последствий": bucket["severe"] / incidents,
                "Доля без подтвержденного водоснабжения": (
                    (bucket["water_known"] - bucket["water_available"]) / bucket["water_known"] if bucket["water_known"] else np.nan
                ),
                "Доля долгих прибытий": bucket["long_arrivals"] / bucket["response_count"] if bucket["response_count"] else np.nan,
                "Средняя удаленность до ПЧ, км": bucket["distance_sum"] / bucket["distance_count"] if bucket["distance_count"] else np.nan,
                "Доля пожаров в отопительный сезон": bucket["heating_incidents"] / incidents,
                "Покрытие данных по водоснабжению": bucket["water_known"] / incidents,
                "Покрытие данных по времени прибытия": bucket["response_count"] / incidents,
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.sort_values(["Число пожаров", "Территория"], ascending=[False, True]).reset_index(drop=True)



def _sample_territory_frame(frame: pd.DataFrame, sample_limit: int, sampling_strategy: str) -> Tuple[pd.DataFrame, str]:
    if frame.empty or len(frame) <= sample_limit:
        return frame.copy(), ""

    if sampling_strategy == "random":
        sampled = frame.sample(n=sample_limit, random_state=42).sort_values("Территория").reset_index(drop=True)
        note = (
            f"Из {len(frame)} территорий для кластеризации выбрана случайная выборка из {len(sampled)} территорий. "
            "Агрегаты по каждой территории при этом посчитаны по всей истории инцидентов, поэтому смещения из-за первых строк больше нет."
        )
        return sampled, note

    work = frame.copy()
    quantiles = max(1, min(4, int(work["Число пожаров"].nunique()), len(work)))
    work["__settlement_group"] = work["Тип территории"].fillna("Не указано")
    if quantiles > 1:
        work["__volume_band"] = pd.qcut(
            work["Число пожаров"].rank(method="first"),
            q=quantiles,
            labels=False,
            duplicates="drop",
        ).astype(str)
    else:
        work["__volume_band"] = "0"
    work["__stratum"] = work["__settlement_group"] + " | Q" + work["__volume_band"]

    fraction = sample_limit / len(work)
    sampled_parts = []
    for _, group in work.groupby("__stratum", sort=False):
        target = max(1, int(round(len(group) * fraction)))
        target = min(target, len(group))
        sampled_parts.append(group.sample(n=target, random_state=42))

    sampled = pd.concat(sampled_parts, axis=0).drop_duplicates()
    if len(sampled) > sample_limit:
        sampled = sampled.sample(n=sample_limit, random_state=42)
    elif len(sampled) < sample_limit:
        remainder = work.drop(index=sampled.index, errors="ignore")
        if not remainder.empty:
            fill_count = min(sample_limit - len(sampled), len(remainder))
            sampled = pd.concat([sampled, remainder.sample(n=fill_count, random_state=42)], axis=0)

    sampled = sampled.drop(columns=["__settlement_group", "__volume_band", "__stratum"], errors="ignore")
    sampled = sampled.sort_values("Территория").reset_index(drop=True)
    note = (
        f"Из {len(frame)} территорий в модель вошла стратифицированная выборка из {len(sampled)} территорий: "
        "сохранён баланс по сельскому/несельскому контексту и по квантилям числа пожаров."
    )
    return sampled, note



def _discover_candidate_features(feature_frame: pd.DataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    row_count = len(feature_frame)
    if row_count == 0:
        return rows

    for order, column in enumerate(feature_frame.columns):
        numeric_series = pd.to_numeric(feature_frame[column], errors="coerce")
        non_null_count = int(numeric_series.notna().sum())
        if non_null_count == 0:
            continue
        coverage = non_null_count / row_count
        unique_count = int(numeric_series.nunique(dropna=True))
        variance = float(numeric_series.var(skipna=True) or 0.0)
        if coverage < 0.15 or unique_count < 2 or variance <= 0:
            continue

        rows.append(
            {
                "name": column,
                "description": FEATURE_METADATA.get(column, {}).get("description", ""),
                "coverage": coverage,
                "coverage_display": _format_percent(coverage),
                "variance": variance,
                "variance_display": _format_number(variance, 3),
                "is_default": column in DEFAULT_CLUSTER_FEATURES,
                "score": (1000.0 if column in DEFAULT_CLUSTER_FEATURES else 0.0) + (coverage * 100.0) - order,
            }
        )

    return sorted(rows, key=lambda item: (item["score"], item["coverage"], item["variance"]), reverse=True)
