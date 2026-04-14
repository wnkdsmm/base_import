from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import timedelta
import math
from statistics import mean
from typing import Any, Dict, List, Optional, Sequence

from .profile_resolution import resolve_weight_profile_for_records
from .profiles import DEFAULT_RISK_WEIGHT_MODE, get_risk_weight_profile, resolve_component_weights
from .scoring import _build_territory_rows
from .types import (
    HistoricalValidationPayload,
    HistoricalWindow,
    HistoricalWindowsBundle,
    RiskEventRecord,
    RiskProfile,
    RiskScore,
    ValidationEvaluation,
    ValidationMetricsRaw,
    ValidationRankedRow,
    ValidationWindowMetrics,
)
from .utils import _clamp, _format_decimal, _format_integer, _format_probability, _unique_non_empty

DEFAULT_RANKING_K = 3
MIN_VALIDATION_WINDOWS = 3


def build_historical_validation_payload(
    records: Sequence[RiskEventRecord],
    planning_horizon_days: int,
    weight_mode: str = DEFAULT_RISK_WEIGHT_MODE,
    profile_override: Optional[RiskProfile] = None,
) -> HistoricalValidationPayload:
    profile = deepcopy(profile_override) if profile_override is not None else get_risk_weight_profile(weight_mode)
    payload = empty_historical_validation_payload(profile.get("mode_label") or "РђРґР°РїС‚РёРІРЅС‹Рµ РІРµСЃР°")
    if not records:
        payload["summary"] = "Р”Р»СЏ РёСЃС‚РѕСЂРёС‡РµСЃРєРѕР№ РїСЂРѕРІРµСЂРєРё СЂР°РЅР¶РёСЂРѕРІР°РЅРёСЏ РїРѕРєР° РЅРµС‚ Р·Р°РїРёСЃРµР№."
        return payload

    windows_bundle = _build_historical_windows(records, planning_horizon_days)
    if not windows_bundle.get("is_ready"):
        payload["summary"] = windows_bundle.get("reason") or "РСЃС‚РѕСЂРёРё РїРѕРєР° РЅРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР»СЏ РїСЂРѕРІРµСЂРєРё СЂР°РЅР¶РёСЂРѕРІР°РЅРёСЏ РЅР° РёСЃС‚РѕСЂРёС‡РµСЃРєРёС… РѕРєРЅР°С…."
        payload["notes"] = _unique_non_empty(
            [
                f"РЎРµР№С‡Р°СЃ РґРѕСЃС‚СѓРїРЅРѕ {_format_integer(windows_bundle.get('history_days') or 0)} РґРЅРµР№ РёСЃС‚РѕСЂРёРё.",
                f"Р”Р»СЏ РїСЂРѕРІРµСЂРєРё Р¶РµР»Р°С‚РµР»СЊРЅРѕ РЅРµ РјРµРЅСЊС€Рµ {_format_integer((windows_bundle.get('min_training_days') or 0) + (windows_bundle.get('horizon_days') or 0))} РґРЅРµР№.",
                "РњРµС‚СЂРёРєРё РїРѕСЏРІСЏС‚СЃСЏ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё, РєРѕРіРґР° РЅР°РєРѕРїРёС‚СЃСЏ РґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РёСЃС‚РѕСЂРёС‡РµСЃРєРёС… РѕРєРѕРЅ.",
            ]
        )
        return payload

    evaluation = _evaluate_profile_on_windows(profile, windows_bundle.get("windows") or [], DEFAULT_RANKING_K)
    if not evaluation.get("has_metrics"):
        payload["summary"] = "РћРєРЅР° РґР»СЏ РїСЂРѕРІРµСЂРєРё РµСЃС‚СЊ, РЅРѕ РІ РЅРёС… РїРѕРєР° РЅРµ СѓРґР°Р»РѕСЃСЊ СЃРѕР±СЂР°С‚СЊ СѓСЃС‚РѕР№С‡РёРІСѓСЋ РѕС†РµРЅРєСѓ ranking-РєР°С‡РµСЃС‚РІР°."
        payload["notes"] = _unique_non_empty(
            [
                f"РћРєРѕРЅ Р±РµР· СЂР°СЃС‡С‘С‚РЅРѕРіРѕ СЂР°РЅР¶РёСЂРѕРІР°РЅРёСЏ: {_format_integer(evaluation.get('skipped_no_rows') or 0)}.",
                f"РћРєРѕРЅ СЃ РјРµС‚СЂРёРєР°РјРё: {_format_integer(len(evaluation.get('window_metrics') or []))}.",
                "РџРѕСЃР»Рµ СЂР°СЃС€РёСЂРµРЅРёСЏ РёСЃС‚РѕСЂРёРё РїР°РЅРµР»СЊ РЅР°С‡РЅРµС‚ РїРѕРєР°Р·С‹РІР°С‚СЊ ranking-РјРµС‚СЂРёРєРё Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё.",
            ]
        )
        return payload

    aggregate = evaluation.get("aggregate") or {}
    k_value = int(aggregate.get("k_value") or DEFAULT_RANKING_K)
    status_label, status_tone = _validation_status(aggregate)
    recent_windows = [item.get("summary_card") for item in (evaluation.get("window_metrics") or [])[-3:] if item.get("summary_card")][::-1]
    summary = (
        "Р­С‚Рѕ rolling-origin РїСЂРѕРІРµСЂРєР° ranking-Р±Р»РѕРєР°: СЃРµСЂРІРёСЃ СЃС‚СЂРѕРёС‚ РїСЂРёРѕСЂРёС‚РµС‚С‹ С‚РѕР»СЊРєРѕ РїРѕ РїСЂРѕС€Р»РѕР№ РёСЃС‚РѕСЂРёРё РѕРєРЅР° Рё СЃРјРѕС‚СЂРёС‚, "
        f"РєСѓРґР° СЂРµР°Р»СЊРЅРѕ РїСЂРёС€Р»Рё РїРѕР¶Р°СЂС‹ РІ СЃР»РµРґСѓСЋС‰РёРµ {aggregate.get('horizon_days') or windows_bundle.get('horizon_days') or max(7, int(planning_horizon_days or 14))} РґРЅРµР№."
    )

    notes = [
        f"РџСЂРѕС„РёР»СЊ РІРµСЃРѕРІ РґР»СЏ РїСЂРѕРІРµСЂРєРё: {profile.get('mode_label') or 'РђРґР°РїС‚РёРІРЅС‹Рµ РІРµСЃР°'} ({profile.get('status_label') or 'Р°РєС‚РёРІРµРЅ'}).",
        f"РљР»СЋС‡РµРІР°СЏ РјРµС‚СЂРёРєР° ranking-РєР°С‡РµСЃС‚РІР°: NDCG@{k_value} {_format_decimal(float(aggregate.get('ndcg_at_k') or 0.0))}.",
        f"Top-{k_value} capture РЅР° РёСЃС‚РѕСЂРёС‡РµСЃРєРёС… РѕРєРЅР°С…: {_format_probability(float(aggregate.get('topk_capture_rate') or 0.0))}.",
        f"Precision@{k_value} РЅР° РёСЃС‚РѕСЂРёС‡РµСЃРєРёС… РѕРєРЅР°С…: {_format_probability(float(aggregate.get('precision_at_k') or 0.0))}.",
    ]
    calibration_summary = str((profile.get("calibration") or {}).get("summary") or "").strip()
    if calibration_summary:
        notes.append(calibration_summary)
    notes.extend(list((profile.get("calibration") or {}).get("notes") or [])[:2])

    payload.update(
        {
            "has_metrics": True,
            "status_label": status_label,
            "status_tone": status_tone,
            "summary": summary,
            "metric_cards": [
                {
                    "label": "РћРєРѕРЅ РѕС†РµРЅРµРЅРѕ",
                    "value": _format_integer(aggregate.get("windows_count") or 0),
                    "meta": f"Р“РѕСЂРёР·РѕРЅС‚: {_format_integer(aggregate.get('horizon_days') or 0)} РґРЅРµР№",
                },
                {
                    "label": "Top-1 hit",
                    "value": _format_probability(float(aggregate.get("top1_hit_rate") or 0.0)),
                    "meta": "РљР°Рє С‡Р°СЃС‚Рѕ С‚РµСЂСЂРёС‚РѕСЂРёСЏ-Р»РёРґРµСЂ РґРµР№СЃС‚РІРёС‚РµР»СЊРЅРѕ РіРѕСЂРµР»Р° РІ СЃР»РµРґСѓСЋС‰РµРј РѕРєРЅРµ",
                },
                {
                    "label": f"Top-{k_value} capture",
                    "value": _format_probability(float(aggregate.get("topk_capture_rate") or 0.0)),
                    "meta": "РљР°РєР°СЏ РґРѕР»СЏ Р±СѓРґСѓС‰РёС… РїРѕР¶Р°СЂРѕРІ РїРѕРїР°РґР°Р»Р° РІ РІРµСЂС…РЅСЋСЋ С‡Р°СЃС‚СЊ СЃРїРёСЃРєР°",
                },
                {
                    "label": f"Precision@{k_value}",
                    "value": _format_probability(float(aggregate.get("precision_at_k") or 0.0)),
                    "meta": "РљР°РєР°СЏ РґРѕР»СЏ С‚РµСЂСЂРёС‚РѕСЂРёР№ РІ top-k РїРѕРґС‚РІРµСЂР¶РґР°Р»Р°СЃСЊ РїРѕР¶Р°СЂРѕРј",
                },
                {
                    "label": f"NDCG@{k_value}",
                    "value": _format_decimal(float(aggregate.get("ndcg_at_k") or 0.0)),
                    "meta": "РќР°СЃРєРѕР»СЊРєРѕ РїРѕСЂСЏРґРѕРє СЂР°РЅР¶РёСЂРѕРІР°РЅРёСЏ СЃРѕРІРїР°РґР°Р» СЃ С„Р°РєС‚РёС‡РµСЃРєРѕР№ РєРѕРЅС†РµРЅС‚СЂР°С†РёРµР№ РїРѕР¶Р°СЂРѕРІ",
                },
            ],
            "notes": _unique_non_empty(notes),
            "recent_windows": recent_windows,
            "metrics_raw": {
                "windows_count": int(aggregate.get("windows_count") or 0),
                "horizon_days": int(aggregate.get("horizon_days") or 0),
                "k_value": k_value,
                "top1_hit_rate": float(aggregate.get("top1_hit_rate") or 0.0),
                "topk_capture_rate": float(aggregate.get("topk_capture_rate") or 0.0),
                "precision_at_k": float(aggregate.get("precision_at_k") or 0.0),
                "ndcg_at_k": float(aggregate.get("ndcg_at_k") or 0.0),
                "objective_score": float(aggregate.get("objective_score") or 0.0),
            },
        }
    )
    return payload


def empty_historical_validation_payload(mode_label: str = "РђРґР°РїС‚РёРІРЅС‹Рµ РІРµСЃР°") -> HistoricalValidationPayload:
    return {
        "title": "РСЃС‚РѕСЂРёС‡РµСЃРєР°СЏ РїСЂРѕРІРµСЂРєР° ranking-РєР°С‡РµСЃС‚РІР°",
        "mode_label": mode_label,
        "has_metrics": False,
        "status_label": "РџРѕРєР° Р±РµР· РїСЂРѕРІРµСЂРєРё",
        "status_tone": "fire",
        "summary": "РџРѕСЃР»Рµ СЂР°СЃС‡С‘С‚Р° Р·РґРµСЃСЊ РїРѕСЏРІРёС‚СЃСЏ РїСЂРѕРІРµСЂРєР° С‚РѕРіРѕ, РЅР°СЃРєРѕР»СЊРєРѕ РІРµСЂС…РЅРёРµ С‚РµСЂСЂРёС‚РѕСЂРёРё СЂРµР°Р»СЊРЅРѕ СЃРѕРІРїР°РґР°Р»Рё СЃ Р±СѓРґСѓС‰РёРјРё РѕС‡Р°РіР°РјРё РЅР° РёСЃС‚РѕСЂРёС‡РµСЃРєРёС… РѕРєРЅР°С….",
        "metric_cards": [
            {"label": "РћРєРѕРЅ РѕС†РµРЅРµРЅРѕ", "value": "0", "meta": "РќРµС‚ РґР°РЅРЅС‹С…"},
            {"label": "Top-1 hit", "value": "0%", "meta": "РќРµС‚ РґР°РЅРЅС‹С…"},
            {"label": "Top-3 capture", "value": "0%", "meta": "РќРµС‚ РґР°РЅРЅС‹С…"},
            {"label": "Precision@3", "value": "0%", "meta": "РќРµС‚ РґР°РЅРЅС‹С…"},
            {"label": "NDCG@3", "value": "0", "meta": "РќРµС‚ РґР°РЅРЅС‹С…"},
        ],
        "notes": [
            "Р­С‚Р° РїР°РЅРµР»СЊ РїРѕРєР°Р·С‹РІР°РµС‚ rolling-origin РїСЂРѕРІРµСЂРєСѓ ranking-РєР°С‡РµСЃС‚РІР° РЅР° РёСЃС‚РѕСЂРёС‡РµСЃРєРёС… РѕРєРЅР°С….",
            "Р•СЃР»Рё РёСЃС‚РѕСЂРёРё РјР°Р»Рѕ, СЃРµСЂРІРёСЃ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё РѕСЃС‚Р°РµС‚СЃСЏ РЅР° СЌРєСЃРїРµСЂС‚РЅРѕРј fallback-РїСЂРѕС„РёР»Рµ.",
        ],
        "recent_windows": [],
        "metrics_raw": {
            "windows_count": 0,
            "horizon_days": 0,
            "k_value": DEFAULT_RANKING_K,
            "top1_hit_rate": 0.0,
            "topk_capture_rate": 0.0,
            "precision_at_k": 0.0,
            "ndcg_at_k": 0.0,
            "objective_score": 0.0,
        },
    }


def _build_historical_windows(
    records: Sequence[RiskEventRecord],
    planning_horizon_days: int,
) -> HistoricalWindowsBundle:
    if not records:
        return {
            "is_ready": False,
            "reason": "Р”Р»СЏ РїСЂРѕРІРµСЂРєРё СЂР°РЅР¶РёСЂРѕРІР°РЅРёСЏ РїРѕРєР° РЅРµС‚ Р·Р°РїРёСЃРµР№.",
            "history_days": 0,
            "horizon_days": max(7, int(planning_horizon_days or 14)),
            "min_training_days": 0,
            "windows": [],
        }

    history_start = min(record["date"] for record in records)
    history_end = max(record["date"] for record in records)
    history_days = max(1, (history_end - history_start).days + 1)
    horizon_days = max(7, int(planning_horizon_days or 14))
    min_training_days = max(180, horizon_days * 6)

    if history_days < min_training_days + horizon_days:
        return {
            "is_ready": False,
            "reason": (
                "РСЃС‚РѕСЂРёРё РїРѕРєР° РЅРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР»СЏ РїСЂРѕРІРµСЂРєРё ranking-Р±Р»РѕРєР° РЅР° РёСЃС‚РѕСЂРёС‡РµСЃРєРёС… РѕРєРЅР°С…. "
                "РќСѓР¶РЅРѕ Р±РѕР»СЊС€Рµ РЅР°Р±Р»СЋРґРµРЅРёР№ РёР»Рё Р±РѕР»РµРµ РєРѕСЂРѕС‚РєРёР№ РіРѕСЂРёР·РѕРЅС‚."
            ),
            "history_days": history_days,
            "horizon_days": horizon_days,
            "min_training_days": min_training_days,
            "windows": [],
        }

    step_days = max(horizon_days, 30)
    earliest_cutoff = history_start + timedelta(days=min_training_days - 1)
    latest_cutoff = history_end - timedelta(days=horizon_days)
    cutoffs: List[Any] = []
    cursor = earliest_cutoff
    while cursor <= latest_cutoff:
        cutoffs.append(cursor)
        cursor += timedelta(days=step_days)
    cutoffs = cutoffs[-6:]

    windows: List[HistoricalWindow] = []
    skipped_no_future = 0
    for cutoff in cutoffs:
        train_records = [record for record in records if record["date"] <= cutoff]
        future_end = cutoff + timedelta(days=horizon_days)
        future_records = [record for record in records if cutoff < record["date"] <= future_end]
        if not future_records:
            skipped_no_future += 1
            continue
        windows.append(
            {
                "cutoff": cutoff,
                "future_end": future_end,
                "horizon_days": horizon_days,
                "train_records": train_records,
                "future_records": future_records,
            }
        )

    return {
        "is_ready": len(windows) >= MIN_VALIDATION_WINDOWS,
        "reason": "" if len(windows) >= MIN_VALIDATION_WINDOWS else "РќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕР±СЂР°С‚СЊ РґРѕСЃС‚Р°С‚РѕС‡РЅРѕРµ С‡РёСЃР»Рѕ РёСЃС‚РѕСЂРёС‡РµСЃРєРёС… РѕРєРѕРЅ РґР»СЏ ranking-РїСЂРѕРІРµСЂРєРё.",
        "history_days": history_days,
        "horizon_days": horizon_days,
        "min_training_days": min_training_days,
        "windows": windows,
        "skipped_no_future": skipped_no_future,
    }


def _evaluate_profile_on_windows(
    profile: RiskProfile,
    windows: Sequence[HistoricalWindow],
    ranking_k: int,
) -> ValidationEvaluation:
    window_metrics: List[ValidationWindowMetrics] = []
    skipped_no_rows = 0

    for window in windows:
        predicted_rows = _rerank_predicted_rows_for_profile(
            window.get("predicted_rows") or [],
            profile,
        )
        if not predicted_rows:
            predicted_rows = _build_territory_rows(
                window.get("train_records") or [],
                int(window.get("horizon_days") or ranking_k or DEFAULT_RANKING_K),
                weight_mode=profile.get("mode") or DEFAULT_RISK_WEIGHT_MODE,
                profile_override=profile,
            )
        if not predicted_rows:
            skipped_no_rows += 1
            continue
        metrics = _evaluate_ranking_window(
            predicted_rows=predicted_rows,
            future_records=window.get("future_records") or [],
            ranking_k=ranking_k,
            cutoff=window.get("cutoff"),
        )
        if metrics is not None:
            window_metrics.append(metrics)

    aggregate = _aggregate_window_metrics(window_metrics, ranking_k)
    aggregate["horizon_days"] = int((windows[0].get("horizon_days") if windows else 0) or 0)
    return {
        "has_metrics": bool(window_metrics),
        "window_metrics": window_metrics,
        "aggregate": aggregate,
        "skipped_no_rows": skipped_no_rows,
    }


def _rerank_predicted_rows_for_profile(
    predicted_rows: Sequence[RiskScore],
    profile: RiskProfile,
) -> List[ValidationRankedRow]:
    if not predicted_rows:
        return []

    urban_weights = {
        item["key"]: float(item.get("weight") or 0.0)
        for item in resolve_component_weights(profile, is_rural=False)
    }
    rural_weights = {
        item["key"]: float(item.get("weight") or 0.0)
        for item in resolve_component_weights(profile, is_rural=True)
    }

    reranked: List[ValidationRankedRow] = []
    for row in predicted_rows:
        component_scores = row.get("component_scores") or []
        weight_map = rural_weights if row.get("is_rural") else urban_weights
        risk_score = _clamp(
            sum(
                float(component.get("score") or 0.0) * weight_map.get(component.get("key"), 0.0)
                for component in component_scores
            ),
            1.0,
            99.0,
        )
        reranked.append(
            {
                "label": row.get("label") or "РўРµСЂСЂРёС‚РѕСЂРёСЏ",
                "risk_score": round(risk_score, 1),
                "history_pressure": float(row.get("history_pressure") or 0.0),
            }
        )

    reranked.sort(key=lambda item: (item["risk_score"], item["history_pressure"]), reverse=True)
    return reranked


def _evaluate_ranking_window(
    predicted_rows: Sequence[ValidationRankedRow],
    future_records: Sequence[RiskEventRecord],
    ranking_k: int,
    cutoff: Any,
) -> Optional[ValidationWindowMetrics]:
    if not predicted_rows or not future_records:
        return None

    actual_counts = Counter(
        (record.get("territory_label") or record.get("district") or "РўРµСЂСЂРёС‚РѕСЂРёСЏ РЅРµ СѓРєР°Р·Р°РЅР°")
        for record in future_records
    )
    total_future_incidents = sum(actual_counts.values())
    if total_future_incidents <= 0:
        return None

    effective_k = max(1, min(int(ranking_k or DEFAULT_RANKING_K), len(predicted_rows)))
    top_labels = [row.get("label") or "РўРµСЂСЂРёС‚РѕСЂРёСЏ" for row in predicted_rows[:effective_k]]
    top1_label = top_labels[0] if top_labels else "-"
    top1_hit = 1.0 if actual_counts.get(top1_label, 0) > 0 else 0.0
    topk_capture = sum(actual_counts.get(label, 0) for label in top_labels) / total_future_incidents
    precision_at_k = sum(1 for label in top_labels if actual_counts.get(label, 0) > 0) / max(1, effective_k)
    ndcg_at_k = _compute_ndcg_at_k(actual_counts, top_labels, effective_k)

    return {
        "cutoff": cutoff,
        "top_label": top1_label,
        "future_incidents": total_future_incidents,
        "top1_hit": top1_hit,
        "topk_capture": topk_capture,
        "precision_at_k": precision_at_k,
        "ndcg_at_k": ndcg_at_k,
        "summary_card": {
            "label": f"РћРєРЅРѕ РґРѕ {cutoff.strftime('%d.%m.%Y') if cutoff else '-'}",
            "risk_display": f"Top-{effective_k}: {_format_probability(topk_capture)}",
            "meta": (
                f"Top-1: {'РґР°' if top1_hit >= 0.5 else 'РЅРµС‚'} | Precision@{effective_k}: {_format_probability(precision_at_k)} | "
                f"NDCG@{effective_k}: {_format_decimal(ndcg_at_k)} | Р±СѓРґСѓС‰РёС… РїРѕР¶Р°СЂРѕРІ: {_format_integer(total_future_incidents)}"
            ),
        },
    }


def _aggregate_window_metrics(
    window_metrics: Sequence[ValidationWindowMetrics],
    ranking_k: int,
) -> ValidationMetricsRaw:
    if not window_metrics:
        return {
            "windows_count": 0,
            "horizon_days": 0,
            "k_value": ranking_k,
            "top1_hit_rate": 0.0,
            "topk_capture_rate": 0.0,
            "precision_at_k": 0.0,
            "ndcg_at_k": 0.0,
            "objective_score": 0.0,
        }

    aggregate = {
        "windows_count": len(window_metrics),
        "horizon_days": 0,
        "k_value": ranking_k,
        "top1_hit_rate": mean(float(item.get("top1_hit") or 0.0) for item in window_metrics),
        "topk_capture_rate": mean(float(item.get("topk_capture") or 0.0) for item in window_metrics),
        "precision_at_k": mean(float(item.get("precision_at_k") or 0.0) for item in window_metrics),
        "ndcg_at_k": mean(float(item.get("ndcg_at_k") or 0.0) for item in window_metrics),
    }
    aggregate["objective_score"] = _ranking_objective(aggregate)
    return aggregate


def _ranking_objective(metrics: ValidationMetricsRaw) -> float:
    top1 = float(metrics.get("top1_hit_rate") or 0.0)
    capture = float(metrics.get("topk_capture_rate") or 0.0)
    precision = float(metrics.get("precision_at_k") or 0.0)
    ndcg = float(metrics.get("ndcg_at_k") or 0.0)
    return 0.24 * top1 + 0.31 * capture + 0.20 * precision + 0.25 * ndcg


def _validation_status(aggregate: ValidationMetricsRaw) -> tuple[str, str]:
    windows_count = int(aggregate.get("windows_count") or 0)
    topk_capture = float(aggregate.get("topk_capture_rate") or 0.0)
    ndcg = float(aggregate.get("ndcg_at_k") or 0.0)
    if windows_count >= 4 and topk_capture >= 0.60 and ndcg >= 0.62:
        return "РСЃС‚РѕСЂРёС‡РµСЃРєРёР№ СЃРёРіРЅР°Р» СѓСЃС‚РѕР№С‡РёРІ", "forest"
    if windows_count >= 3 and topk_capture >= 0.45 and ndcg >= 0.48:
        return "РСЃС‚РѕСЂРёС‡РµСЃРєРёР№ СЃРёРіРЅР°Р» СЂР°Р±РѕС‡РёР№", "sky"
    return "РџСЂРѕРІРµСЂРєР° С‡Р°СЃС‚РёС‡РЅР°СЏ", "sand"


def _compute_ndcg_at_k(actual_counts: Counter, ranked_labels: Sequence[str], k_value: int) -> float:
    if not ranked_labels or k_value <= 0:
        return 0.0
    effective_k = max(1, min(k_value, len(ranked_labels)))
    predicted_relevance = [float(actual_counts.get(label, 0)) for label in ranked_labels[:effective_k]]
    ideal_relevance = sorted((float(value) for value in actual_counts.values()), reverse=True)[:effective_k]
    dcg = _discounted_gain(predicted_relevance)
    idcg = _discounted_gain(ideal_relevance)
    if idcg <= 0:
        return 0.0
    return dcg / idcg


def _discounted_gain(relevance_values: Sequence[float]) -> float:
    gain = 0.0
    for index, relevance in enumerate(relevance_values):
        gain += (math.pow(2.0, float(relevance)) - 1.0) / math.log2(index + 2.0)
    return gain
