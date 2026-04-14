from __future__ import annotations

import math
from itertools import combinations
from typing import Any, List, Sequence, Tuple

import pandas as pd

from app.services.model_quality import compute_clustering_metrics

from .analysis_stats import (
    _cluster_quality_score,
    _fit_weighted_kmeans,
    _prepare_model_inputs,
    _prepare_subset_frame,
)
from .constants import (
    AUTO_DEFAULT_EXCLUDED_FEATURES,
    CLUSTER_COUNT_OPTIONS,
    DEFAULT_CLUSTER_FEATURES,
    DEFAULT_CLUSTER_MODE_LOAD,
    DEFAULT_CLUSTER_MODE_LOAD_LABEL,
    DEFAULT_CLUSTER_MODE_PROFILE,
    DEFAULT_CLUSTER_MODE_PROFILE_LABEL,
    DEFAULT_FEATURE_TARGET_COUNT,
    FEATURE_SELECTION_MIN_IMPROVEMENT,
    MIN_DEFAULT_FEATURE_COUNT,
    PROFILE_MODE_EXCLUDED_FEATURES,
    PROFILE_MODE_SCORE_TOLERANCE,
    PROFILE_MODE_SILHOUETTE_TOLERANCE,
    VOLUME_DOMINANCE_MIN_SCORE_DELTA,
    VOLUME_DOMINANCE_RATIO,
    WEIGHTING_STRATEGY_INCIDENT_LOG,
    WEIGHTING_STRATEGY_INCIDENT_LOG_LABEL,
    WEIGHTING_STRATEGY_NOT_APPLICABLE,
    WEIGHTING_STRATEGY_NOT_APPLICABLE_LABEL,
    WEIGHTING_STRATEGY_UNIFORM,
    WEIGHTING_STRATEGY_UNIFORM_LABEL,
)
from .types import ClusteringFeatureSelectionContext, ClusteringMethodCandidate, FeatureAblationRow


def _weighting_strategy_for_mode(mode_key: str) -> str:
    if mode_key == DEFAULT_CLUSTER_MODE_PROFILE:
        return WEIGHTING_STRATEGY_UNIFORM
    return WEIGHTING_STRATEGY_INCIDENT_LOG


def _describe_weighting_strategy(mode_label: str, weighting_strategy: str) -> tuple[str, str, str]:
    if weighting_strategy == WEIGHTING_STRATEGY_NOT_APPLICABLE:
        return (
            WEIGHTING_STRATEGY_NOT_APPLICABLE_LABEL,
            (
                f"Р’ СЂР°Р±РѕС‡РµРј РІС‹РІРѕРґРµ РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РјРµС‚РѕРґ Р±РµР· sample weights: СЂРµР¶РёРј '{mode_label}' Р·Р°РґР°С‘С‚СЃСЏ РЅР°Р±РѕСЂРѕРј РїСЂРёР·РЅР°РєРѕРІ, "
                "Р° С†РµРЅС‚СЂС‹/РіСЂР°РЅРёС†С‹ РєР»Р°СЃС‚РµСЂРѕРІ РЅРµ СЃРјРµС‰Р°СЋС‚СЃСЏ РґРѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Рј РІРµСЃРѕРј РїРѕ С‡РёСЃР»Сѓ РїРѕР¶Р°СЂРѕРІ."
            ),
            "Р­С‚РѕС‚ Р°Р»РіРѕСЂРёС‚Рј РЅРµ РёСЃРїРѕР»СЊР·СѓРµС‚ РѕС‚РґРµР»СЊРЅС‹Рµ РІРµСЃР° С‚РµСЂСЂРёС‚РѕСЂРёР№: РЅР°РіСЂСѓР·РєР° РјРѕР¶РµС‚ РІР»РёСЏС‚СЊ С‚РѕР»СЊРєРѕ С‡РµСЂРµР· СЃР°РјРё РїСЂРёР·РЅР°РєРё.",
        )
    if weighting_strategy == WEIGHTING_STRATEGY_UNIFORM:
        return (
            WEIGHTING_STRATEGY_UNIFORM_LABEL,
            f"Р’ СЂРµР¶РёРјРµ '{mode_label}' KMeans РѕР±СѓС‡Р°РµС‚СЃСЏ СЃ СЂР°РІРЅС‹Рј РІРµСЃРѕРј С‚РµСЂСЂРёС‚РѕСЂРёР№: С‡РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ РЅРµ СЃРјРµС‰Р°РµС‚ С†РµРЅС‚СЂС‹ РєР»Р°СЃС‚РµСЂРѕРІ.",
            "Р’СЃРµ С‚РµСЂСЂРёС‚РѕСЂРёРё РІР»РёСЏСЋС‚ РЅР° СЂРµР·СѓР»СЊС‚Р°С‚ РѕРґРёРЅР°РєРѕРІРѕ, Р±РµР· РґРѕРїРѕР»РЅРёС‚РµР»СЊРЅРѕРіРѕ РІРµСЃР° РїРѕ С‡РёСЃР»Сѓ РїРѕР¶Р°СЂРѕРІ.",
        )
    return (
        WEIGHTING_STRATEGY_INCIDENT_LOG_LABEL,
        (
            f"Р’ СЂРµР¶РёРјРµ '{mode_label}' KMeans РёСЃРїРѕР»СЊР·СѓРµС‚ СѓРјРµСЂРµРЅРЅС‹Рµ log-РІРµСЃР° РїРѕ С‡РёСЃР»Сѓ РїРѕР¶Р°СЂРѕРІ, "
            "РїРѕСЌС‚РѕРјСѓ С‚РµСЂСЂРёС‚РѕСЂРёРё СЃ Р±РѕР»СЊС€РµР№ РёСЃС‚РѕСЂРёРµР№ РЅРµРјРЅРѕРіРѕ СЃРёР»СЊРЅРµРµ РІР»РёСЏСЋС‚ РЅР° С†РµРЅС‚СЂС‹ РєР»Р°СЃС‚РµСЂРѕРІ."
        ),
        "РўРµСЂСЂРёС‚РѕСЂРёРё СЃ Р±РѕР»СЊС€РµР№ РёСЃС‚РѕСЂРёРµР№ РїРѕР¶Р°СЂРѕРІ РІР»РёСЏСЋС‚ С‡СѓС‚СЊ СЃРёР»СЊРЅРµРµ, РЅРѕ Р±РµР· СЂРµР·РєРѕРіРѕ РїРµСЂРµРєРѕСЃР° РІ РёС… СЃС‚РѕСЂРѕРЅСѓ.",
    )


def _build_clustering_mode_context(
    selected_features: Sequence[str],
    feature_selection_report: ClusteringFeatureSelectionContext | None = None,
) -> ClusteringFeatureSelectionContext:
    context = dict(feature_selection_report or {})
    mode_key = str(
        context.get("selected_mode_key")
        or (
            DEFAULT_CLUSTER_MODE_LOAD
            if "Р§РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ" in selected_features
            else DEFAULT_CLUSTER_MODE_PROFILE
        )
    )
    mode_label = str(
        context.get("selected_mode_label")
        or (
            DEFAULT_CLUSTER_MODE_LOAD_LABEL
            if mode_key == DEFAULT_CLUSTER_MODE_LOAD
            else DEFAULT_CLUSTER_MODE_PROFILE_LABEL
        )
    )
    volume_role_code = str(context.get("volume_role_code") or mode_key)
    volume_role_label = str(context.get("volume_role_label") or mode_label)
    volume_note = str(
        context.get("volume_note")
        or (
            "Р’С‹Р±СЂР°РЅ СЂРµР¶РёРј С‚РёРїРѕР»РѕРіРёРё Р±РµР· РєРѕРјРїРѕРЅРµРЅС‚С‹ РЅР°РіСЂСѓР·РєРё: С‡РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ РЅРµ РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РЅРё РєР°Рє РїСЂРёР·РЅР°Рє, РЅРё РєР°Рє СЃРєСЂС‹С‚С‹Р№ РІРµСЃ С‚РµСЂСЂРёС‚РѕСЂРёРё."
            if mode_key == DEFAULT_CLUSTER_MODE_PROFILE
            else "Р’С‹Р±СЂР°РЅ СЂРµР¶РёРј С‚РёРїРѕР»РѕРіРёРё СЃ РєРѕРјРїРѕРЅРµРЅС‚РѕР№ РЅР°РіСЂСѓР·РєРё: С‡РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ РјРѕР¶РµС‚ РІР»РёСЏС‚СЊ Рё РєР°Рє РїСЂРёР·РЅР°Рє, Рё С‡РµСЂРµР· СѓРјРµСЂРµРЅРЅС‹Рµ РІРµСЃР° С‚РµСЂСЂРёС‚РѕСЂРёРё."
        )
    )
    weighting_strategy = str(context.get("weighting_strategy") or _weighting_strategy_for_mode(mode_key))
    weighting_label, weighting_note, weighting_meta = _describe_weighting_strategy(mode_label, weighting_strategy)
    return {
        **context,
        "selected_features": list(selected_features),
        "selected_mode_key": mode_key,
        "selected_mode_label": mode_label,
        "volume_role_code": volume_role_code,
        "volume_role_label": volume_role_label,
        "volume_note": volume_note,
        "weighting_strategy": weighting_strategy,
        "weighting_label": weighting_label,
        "weighting_note": weighting_note,
        "weighting_meta": weighting_meta,
        "uses_incident_weights": weighting_strategy == WEIGHTING_STRATEGY_INCIDENT_LOG,
        "uses_sample_weights": weighting_strategy == WEIGHTING_STRATEGY_INCIDENT_LOG,
    }


def _build_runtime_clustering_context(
    feature_selection_report: ClusteringFeatureSelectionContext | None,
    *,
    method_label: str,
    algorithm_key: str,
    weighting_strategy: str,
) -> ClusteringFeatureSelectionContext:
    context = dict(feature_selection_report or {})
    mode_label = str(
        context.get("selected_mode_label")
        or context.get("volume_role_label")
        or DEFAULT_CLUSTER_MODE_PROFILE_LABEL
    )
    base_weighting = str(context.get("weighting_strategy") or "")
    weighting_label, weighting_note, weighting_meta = _describe_weighting_strategy(mode_label, weighting_strategy)
    if base_weighting and base_weighting != weighting_strategy:
        weighting_note = (
            f"{weighting_note} РќР° С‚РµРєСѓС‰РµРј СЃСЂРµР·Рµ СЂР°Р±РѕС‡РёР№ РІС‹РІРѕРґ РёСЃРїРѕР»СЊР·СѓРµС‚ РёРјРµРЅРЅРѕ СЌС‚Сѓ СЃС‚СЂР°С‚РµРіРёСЋ, "
            "РїРѕС‚РѕРјСѓ С‡С‚Рѕ РІ С‡РµСЃС‚РЅРѕРј СЃСЂР°РІРЅРµРЅРёРё РЅР° С‚РѕРј Р¶Рµ РЅР°Р±РѕСЂРµ РїСЂРёР·РЅР°РєРѕРІ РѕРЅР° РѕРєР°Р·Р°Р»Р°СЃСЊ СЃРёР»СЊРЅРµРµ Р±Р°Р·РѕРІРѕР№ СЃС‚СЂР°С‚РµРіРёРё СЂРµР¶РёРјР°."
        )
    context.update(
        {
            "weighting_strategy": weighting_strategy,
            "weighting_label": weighting_label,
            "weighting_note": weighting_note,
            "weighting_meta": weighting_meta,
            "uses_incident_weights": weighting_strategy == WEIGHTING_STRATEGY_INCIDENT_LOG,
            "uses_sample_weights": weighting_strategy == WEIGHTING_STRATEGY_INCIDENT_LOG,
            "selected_method_label": method_label,
            "selected_algorithm_key": algorithm_key,
        }
    )
    return context


def _build_default_feature_selection_analysis(
    feature_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    available_features: Sequence[str],
    cluster_count: int,
) -> ClusteringFeatureSelectionContext:
    ordered_features = [
        feature
        for feature in available_features
        if feature in feature_frame.columns and feature not in AUTO_DEFAULT_EXCLUDED_FEATURES
    ]
    if len(ordered_features) < 2:
        selected_features = list(ordered_features)
        return _build_clustering_mode_context(
            selected_features,
            {
                "selected_features": selected_features,
                "selected_mode_key": DEFAULT_CLUSTER_MODE_PROFILE,
                "selected_mode_label": DEFAULT_CLUSTER_MODE_PROFILE_LABEL,
                "mode_candidates": [],
                "ablation_rows": [],
                "volume_role_code": DEFAULT_CLUSTER_MODE_PROFILE,
                "volume_role_label": DEFAULT_CLUSTER_MODE_PROFILE_LABEL,
                "volume_note": "РР·-Р·Р° РјР°Р»РѕРіРѕ С‡РёСЃР»Р° РґРѕСЃС‚СѓРїРЅС‹С… РїСЂРёР·РЅР°РєРѕРІ РєР»Р°СЃС‚РµСЂРёР·Р°С†РёСЏ РѕРїРёСЃС‹РІР°РµС‚ С‚РѕР»СЊРєРѕ С‚РѕС‚ РїСЂРѕС„РёР»СЊ, РєРѕС‚РѕСЂС‹Р№ СѓРґР°Р»РѕСЃСЊ СЃРѕР±СЂР°С‚СЊ РёР· С‚РµРєСѓС‰РµРіРѕ СЃСЂРµР·Р°.",
                "selection_note": "Р‘Р°Р·РѕРІС‹Р№ РЅР°Р±РѕСЂ РїСЂРёР·РЅР°РєРѕРІ СЃРѕР±СЂР°РЅ РїРѕ РєРѕСЂРѕС‚РєРѕРјСѓ РїСЂРѕР±РЅРѕРјСѓ СЃСЂР°РІРЅРµРЅРёСЋ, РЅРѕ РІ С‚РµРєСѓС‰РµРј СЃСЂРµР·Рµ РґР°РЅРЅС‹С… СЃР»РёС€РєРѕРј РјР°Р»Рѕ РґР»СЏ РїРѕР»РЅРѕС†РµРЅРЅРѕРіРѕ СЃСЂР°РІРЅРµРЅРёСЏ СЂРµР¶РёРјРѕРІ.",
            },
        )

    mode_candidates: List[ClusteringMethodCandidate] = []
    for mode_key, mode_label, excluded_features in [
        (DEFAULT_CLUSTER_MODE_PROFILE, DEFAULT_CLUSTER_MODE_PROFILE_LABEL, PROFILE_MODE_EXCLUDED_FEATURES),
        (DEFAULT_CLUSTER_MODE_LOAD, DEFAULT_CLUSTER_MODE_LOAD_LABEL, set()),
    ]:
        weighting_strategy = _weighting_strategy_for_mode(mode_key)
        candidate_pool = [feature for feature in ordered_features if feature not in excluded_features]
        if len(candidate_pool) < 2:
            continue
        selected_features = _choose_features_from_pool(
            feature_frame=feature_frame,
            entity_frame=entity_frame,
            available_features=candidate_pool,
            cluster_count=cluster_count,
            weighting_strategy=weighting_strategy,
        )
        evaluation = _evaluate_feature_subset(
            feature_frame=feature_frame,
            entity_frame=entity_frame,
            selected_features=selected_features,
            cluster_count=cluster_count,
            weighting_strategy=weighting_strategy,
        )
        ablation_rows = _build_feature_ablation_rows(
            feature_frame=feature_frame,
            entity_frame=entity_frame,
            selected_features=selected_features,
            candidate_features=ordered_features,
            cluster_count=cluster_count,
            weighting_strategy=weighting_strategy,
        )
        mode_candidates.append(
            {
                "mode_key": mode_key,
                "mode_label": mode_label,
                "selected_features": selected_features,
                "ablation_rows": ablation_rows,
                "weighting_strategy": weighting_strategy,
                **evaluation,
            }
        )

    if not mode_candidates:
        fallback = ordered_features[: max(2, min(len(ordered_features), MIN_DEFAULT_FEATURE_COUNT))]
        return _build_clustering_mode_context(
            fallback,
            {
                "selected_features": fallback,
                "selected_mode_key": DEFAULT_CLUSTER_MODE_PROFILE,
                "selected_mode_label": DEFAULT_CLUSTER_MODE_PROFILE_LABEL,
                "mode_candidates": [],
                "ablation_rows": [],
                "volume_role_code": DEFAULT_CLUSTER_MODE_PROFILE,
                "volume_role_label": DEFAULT_CLUSTER_MODE_PROFILE_LABEL,
                "volume_note": "РЎСЂР°РІРЅРёС‚СЊ РґРІР° СЂРµР¶РёРјР° С‚РёРїРѕР»РѕРіРёРё РЅРµ СѓРґР°Р»РѕСЃСЊ, РїРѕСЌС‚РѕРјСѓ РІС‹Р±СЂР°РЅ СЃР°РјС‹Р№ СѓСЃС‚РѕР№С‡РёРІС‹Р№ РґРѕСЃС‚СѓРїРЅС‹Р№ РЅР°Р±РѕСЂ РїСЂРёР·РЅР°РєРѕРІ.",
                "selection_note": "Р‘Р°Р·РѕРІС‹Р№ РЅР°Р±РѕСЂ РїСЂРёР·РЅР°РєРѕРІ СЃРѕР±СЂР°РЅ РїРѕ РєРѕСЂРѕС‚РєРѕРјСѓ РїСЂРѕР±РЅРѕРјСѓ СЃСЂР°РІРЅРµРЅРёСЋ, РЅРѕ СЂРµР¶РёРјС‹ С‚РёРїРѕР»РѕРіРёРё РЅРµ СѓРґР°Р»РѕСЃСЊ СЃРѕРїРѕСЃС‚Р°РІРёС‚СЊ РёР·-Р·Р° РѕРіСЂР°РЅРёС‡РµРЅРЅРѕРіРѕ С‡РёСЃР»Р° РїСЂРёР·РЅР°РєРѕРІ.",
            },
        )

    selected_mode = _pick_default_feature_selection_mode(mode_candidates)
    volume_role = _summarize_volume_role(selected_mode)

    return _build_clustering_mode_context(
        list(selected_mode["selected_features"]),
        {
            "selected_features": list(selected_mode["selected_features"]),
            "selected_mode_key": selected_mode["mode_key"],
            "selected_mode_label": selected_mode["mode_label"],
            "mode_candidates": mode_candidates,
            "ablation_rows": selected_mode["ablation_rows"],
            "volume_role_code": volume_role["code"],
            "volume_role_label": volume_role["label"],
            "volume_note": volume_role["note"],
            "weighting_strategy": selected_mode.get("weighting_strategy"),
            "selection_note": (
                "Р‘Р°Р·РѕРІС‹Р№ РЅР°Р±РѕСЂ РїСЂРёР·РЅР°РєРѕРІ СЃРѕР±СЂР°РЅ РїРѕ РєРѕСЂРѕС‚РєРѕРјСѓ РїСЂРѕР±РЅРѕРјСѓ СЃСЂР°РІРЅРµРЅРёСЋ: СЃРЅР°С‡Р°Р»Р° СЃРѕРїРѕСЃС‚Р°РІР»СЏСЋС‚СЃСЏ СЂРµР¶РёРјС‹ "
                f"'{DEFAULT_CLUSTER_MODE_PROFILE_LABEL}' Рё '{DEFAULT_CLUSTER_MODE_LOAD_LABEL}', "
                "РїРѕСЃР»Рµ С‡РµРіРѕ РІ РІС‹Р±СЂР°РЅРЅРѕРј СЂРµР¶РёРјРµ РїСЂРѕРІРµСЂСЏРµС‚СЃСЏ, РєР°РєРёРµ РїСЂРёР·РЅР°РєРё Р»СѓС‡С€Рµ СЂР°Р·РґРµР»СЏСЋС‚ С‚РµСЂСЂРёС‚РѕСЂРёРё."
            ),
        },
    )


def _select_default_cluster_features(
    feature_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    available_features: Sequence[str],
    cluster_count: int,
) -> List[str]:
    return list(
        _build_default_feature_selection_analysis(
            feature_frame=feature_frame,
            entity_frame=entity_frame,
            available_features=available_features,
            cluster_count=cluster_count,
        )["selected_features"]
    )


def _choose_features_from_pool(
    feature_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    available_features: Sequence[str],
    cluster_count: int,
    weighting_strategy: str = WEIGHTING_STRATEGY_INCIDENT_LOG,
) -> List[str]:
    ordered_features = [feature for feature in available_features if feature in feature_frame.columns]
    if len(ordered_features) < 2:
        return list(ordered_features)

    feature_order = {feature: index for index, feature in enumerate(ordered_features)}
    evaluation_cache: Dict[Tuple[str, ...], Dict[str, float]] = {}
    preferred_features = [feature for feature in DEFAULT_CLUSTER_FEATURES if feature in feature_order]
    anchor_feature = preferred_features[0] if preferred_features else ordered_features[0]

    def _normalize_subset(subset: Sequence[str]) -> Tuple[str, ...]:
        return tuple(sorted(dict.fromkeys(subset), key=lambda feature: feature_order[feature]))

    def _evaluate_subset(subset: Sequence[str]) -> Dict[str, float]:
        normalized_subset = _normalize_subset(subset)
        if normalized_subset not in evaluation_cache:
            evaluation_cache[normalized_subset] = _evaluate_feature_subset(
                feature_frame=feature_frame,
                entity_frame=entity_frame,
                selected_features=normalized_subset,
                cluster_count=cluster_count,
                weighting_strategy=weighting_strategy,
            )
        return evaluation_cache[normalized_subset]

    pair_candidates = [
        (anchor_feature, feature)
        for feature in ordered_features
        if feature != anchor_feature
    ]
    if not pair_candidates:
        pair_candidates = list(combinations(ordered_features, 2))

    best_pair: Tuple[str, ...] | None = None
    best_result: Dict[str, float] | None = None
    for pair in pair_candidates:
        result = _evaluate_subset(pair)
        if best_result is None or _subset_result_sort_key(result) > _subset_result_sort_key(best_result):
            best_pair = _normalize_subset(pair)
            best_result = result

    if best_pair is None or best_result is None:
        fallback = [feature for feature in preferred_features if feature in ordered_features]
        if len(fallback) >= 2:
            return fallback
        return ordered_features[:2]

    selected = list(best_pair)
    current_result = best_result
    remaining = [feature for feature in ordered_features if feature not in selected]
    target_feature_count = min(DEFAULT_FEATURE_TARGET_COUNT, len(ordered_features))

    while remaining and len(selected) < target_feature_count:
        candidate_rows = [
            (feature, _evaluate_subset([*selected, feature]))
            for feature in remaining
        ]
        best_feature, candidate_result = max(candidate_rows, key=lambda item: _subset_result_sort_key(item[1]))
        if candidate_result["score"] < current_result["score"] + FEATURE_SELECTION_MIN_IMPROVEMENT:
            break
        selected.append(best_feature)
        remaining.remove(best_feature)
        current_result = candidate_result

    improved = True
    while improved and len(selected) > 2:
        improved = False
        removal_rows = [
            (
                feature,
                _evaluate_subset([selected_feature for selected_feature in selected if selected_feature != feature]),
            )
            for feature in selected
            if feature != anchor_feature
        ]
        if not removal_rows:
            break
        feature_to_remove, removal_result = max(removal_rows, key=lambda item: _subset_result_sort_key(item[1]))
        if removal_result["score"] >= current_result["score"] + (FEATURE_SELECTION_MIN_IMPROVEMENT / 2.0):
            selected.remove(feature_to_remove)
            current_result = removal_result
            improved = True

    min_feature_count = min(max(2, MIN_DEFAULT_FEATURE_COUNT), len(ordered_features))
    while len(selected) < min_feature_count:
        candidate_rows = [
            (feature, _evaluate_subset([*selected, feature]))
            for feature in ordered_features
            if feature not in selected
        ]
        if not candidate_rows:
            break
        best_feature, candidate_result = max(candidate_rows, key=lambda item: _subset_result_sort_key(item[1]))
        selected.append(best_feature)
        current_result = candidate_result

    selected_set = set(selected)
    prioritized = [feature for feature in DEFAULT_CLUSTER_FEATURES if feature in selected_set]
    remainder = [feature for feature in ordered_features if feature in selected_set and feature not in prioritized]
    return prioritized + remainder


def _pick_default_feature_selection_mode(mode_candidates: Sequence[ClusteringMethodCandidate]) -> ClusteringMethodCandidate:
    load_mode = next((item for item in mode_candidates if item["mode_key"] == DEFAULT_CLUSTER_MODE_LOAD), None)
    profile_mode = next((item for item in mode_candidates if item["mode_key"] == DEFAULT_CLUSTER_MODE_PROFILE), None)
    if load_mode is None:
        return profile_mode or max(mode_candidates, key=_subset_result_sort_key)
    if profile_mode is None:
        return load_mode

    score_gap = float(load_mode["score"] - profile_mode["score"])
    load_silhouette = float(load_mode.get("silhouette", float("-inf")))
    profile_silhouette = float(profile_mode.get("silhouette", float("-inf")))
    if score_gap <= PROFILE_MODE_SCORE_TOLERANCE and profile_silhouette >= load_silhouette - PROFILE_MODE_SILHOUETTE_TOLERANCE:
        return profile_mode
    return load_mode


def _build_feature_ablation_rows(
    feature_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    selected_features: Sequence[str],
    candidate_features: Sequence[str],
    cluster_count: int,
    weighting_strategy: str = WEIGHTING_STRATEGY_INCIDENT_LOG,
) -> List[FeatureAblationRow]:
    base_result = _evaluate_feature_subset(
        feature_frame,
        entity_frame,
        selected_features,
        cluster_count,
        weighting_strategy=weighting_strategy,
    )
    rows: List[FeatureAblationRow] = []

    for feature in selected_features:
        reduced_features = [item for item in selected_features if item != feature]
        reduced_result = _evaluate_feature_subset(
            feature_frame,
            entity_frame,
            reduced_features,
            cluster_count,
            weighting_strategy=weighting_strategy,
        )
        rows.append(
            {
                "feature": feature,
                "direction": "drop",
                "delta_score": float(base_result["score"] - reduced_result["score"]),
                "delta_silhouette": float(base_result["silhouette"] - reduced_result["silhouette"]),
            }
        )

    for feature in candidate_features:
        if feature in selected_features:
            continue
        expanded_result = _evaluate_feature_subset(
            feature_frame,
            entity_frame,
            [*selected_features, feature],
            cluster_count,
            weighting_strategy=weighting_strategy,
        )
        rows.append(
            {
                "feature": feature,
                "direction": "add",
                "delta_score": float(expanded_result["score"] - base_result["score"]),
                "delta_silhouette": float(expanded_result["silhouette"] - base_result["silhouette"]),
            }
        )

    return sorted(
        rows,
        key=lambda item: (
            1 if item["direction"] == "drop" else 0,
            abs(float(item["delta_score"])),
            abs(float(item["delta_silhouette"])),
        ),
        reverse=True,
    )


def _summarize_volume_role(selection_report: ClusteringMethodCandidate) -> Dict[str, str]:
    if selection_report["mode_key"] == DEFAULT_CLUSTER_MODE_PROFILE:
        return {
            "code": DEFAULT_CLUSTER_MODE_PROFILE,
            "label": DEFAULT_CLUSTER_MODE_PROFILE_LABEL,
            "note": "РџРѕ СѓРјРѕР»С‡Р°РЅРёСЋ РєР»Р°СЃС‚РµСЂРёР·Р°С†РёСЏ РѕРїРёСЃС‹РІР°РµС‚ РїСЂРѕС„РёР»СЊ С‚РµСЂСЂРёС‚РѕСЂРёРё Р±РµР· РѕС‚РґРµР»СЊРЅРѕРіРѕ СЂР°Р·Р±РёРµРЅРёСЏ РїРѕ РѕР±СЉС‘РјСѓ РЅР°РіСЂСѓР·РєРё: РїСЂРёР·РЅР°Рє 'Р§РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ' РЅРµ РЅСѓР¶РµРЅ, С‡С‚РѕР±С‹ СЃРѕС…СЂР°РЅРёС‚СЊ РєР°С‡РµСЃС‚РІРѕ РЅР° С‚РµРєСѓС‰РµРј СЃСЂРµР·Рµ.",
        }

    count_drop = next(
        (
            row
            for row in selection_report["ablation_rows"]
            if row["direction"] == "drop" and row["feature"] == "Р§РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ"
        ),
        None,
    )
    strongest_other = max(
        (
            row
            for row in selection_report["ablation_rows"]
            if row["direction"] == "drop" and row["feature"] != "Р§РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ"
        ),
        key=lambda item: float(item["delta_score"]),
        default=None,
    )
    count_delta = float(count_drop["delta_score"]) if count_drop else 0.0
    strongest_other_delta = float(strongest_other["delta_score"]) if strongest_other else 0.0
    if count_drop and count_delta >= max(VOLUME_DOMINANCE_MIN_SCORE_DELTA, strongest_other_delta * VOLUME_DOMINANCE_RATIO):
        return {
            "code": "load_dominant",
            "label": DEFAULT_CLUSTER_MODE_LOAD_LABEL,
            "note": "РџРѕ СѓРјРѕР»С‡Р°РЅРёСЋ РєР»Р°СЃС‚РµСЂРёР·Р°С†РёСЏ РѕРїРёСЃС‹РІР°РµС‚ РїСЂРѕС„РёР»СЊ С‚РµСЂСЂРёС‚РѕСЂРёРё СЃ СЃРёР»СЊРЅРѕР№ РєРѕРјРїРѕРЅРµРЅС‚РѕР№ РѕР±СЉС‘РјР° РЅР°РіСЂСѓР·РєРё: РїСЂРёР·РЅР°Рє 'Р§РёСЃР»Рѕ РїРѕР¶Р°СЂРѕРІ' РґР°С‘С‚ СЃР°РјС‹Р№ Р·Р°РјРµС‚РЅС‹Р№ РІРєР»Р°Рґ РІ РєР°С‡РµСЃС‚РІРѕ СЂР°Р·Р±РёРµРЅРёСЏ.",
        }
    return {
        "code": "load_aware",
        "label": DEFAULT_CLUSTER_MODE_LOAD_LABEL,
        "note": "РџРѕ СѓРјРѕР»С‡Р°РЅРёСЋ РєР»Р°СЃС‚РµСЂРёР·Р°С†РёСЏ РѕРїРёСЃС‹РІР°РµС‚ РїСЂРѕС„РёР»СЊ С‚РµСЂСЂРёС‚РѕСЂРёРё СЃ СѓС‡С‘С‚РѕРј РѕР±СЉС‘РјР° РЅР°РіСЂСѓР·РєРё, РЅРѕ РЅРµ СЃРІРѕРґРёС‚СЃСЏ С‚РѕР»СЊРєРѕ Рє С‡РёСЃР»Сѓ РїРѕР¶Р°СЂРѕРІ: РєСЂРѕРјРµ РѕР±СЉС‘РјР°, РєР»Р°СЃС‚РµСЂРёР·Р°С†РёСЋ СѓРґРµСЂР¶РёРІР°СЋС‚ Рё РїСЂРѕС„РёР»СЊРЅС‹Рµ РїСЂРёР·РЅР°РєРё.",
    }


def _evaluate_feature_subset(
    feature_frame: pd.DataFrame,
    entity_frame: pd.DataFrame,
    selected_features: Sequence[str],
    cluster_count: int,
    weighting_strategy: str = WEIGHTING_STRATEGY_INCIDENT_LOG,
) -> Dict[str, float]:
    subset_frame, subset_entities = _prepare_subset_frame(feature_frame, entity_frame, selected_features)
    if len(subset_frame) <= 2:
        return {
            "score": float("-inf"),
            "silhouette": float("-inf"),
            "davies_bouldin": float("inf"),
            "calinski_harabasz": float("-inf"),
            "cluster_balance_ratio": 0.0,
        }

    actual_cluster_count = min(max(CLUSTER_COUNT_OPTIONS[0], int(cluster_count)), len(subset_frame) - 1)
    if actual_cluster_count < 2:
        return {
            "score": float("-inf"),
            "silhouette": float("-inf"),
            "davies_bouldin": float("inf"),
            "calinski_harabasz": float("-inf"),
            "cluster_balance_ratio": 0.0,
        }

    _, scaled_points, _, _, sample_weights = _prepare_model_inputs(
        subset_frame,
        subset_entities,
        weighting_strategy=weighting_strategy,
    )
    model = _fit_weighted_kmeans(scaled_points, sample_weights, actual_cluster_count, random_state=42, n_init=20)
    metrics = compute_clustering_metrics(scaled_points, model.labels_)
    return {
        "score": _cluster_quality_score(metrics, len(subset_frame)),
        "silhouette": float(metrics.get("silhouette") or float("-inf")),
        "davies_bouldin": float(metrics.get("davies_bouldin") or float("inf")),
        "calinski_harabasz": float(metrics.get("calinski_harabasz") or float("-inf")),
        "cluster_balance_ratio": float(metrics.get("cluster_balance_ratio") or 0.0),
    }


def _subset_result_sort_key(result: dict[str, float]) -> tuple[float, float, float, float, float]:
    davies_bouldin = result.get("davies_bouldin", float("inf"))
    return (
        float(result.get("score", float("-inf"))),
        float(result.get("silhouette", float("-inf"))),
        -float(davies_bouldin if math.isfinite(davies_bouldin) else 1e9),
        float(result.get("calinski_harabasz", float("-inf"))),
        float(result.get("cluster_balance_ratio", 0.0)),
    )
