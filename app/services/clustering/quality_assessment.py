from __future__ import annotations

import math
from typing import Sequence

from .types import ClusterLabel, ClusterMethod, ClusterMetrics, ClusteringMethodRow


def compute_method_algorithm_key(method_row: ClusterMethod | None) -> str:
    if not method_row:
        return ""
    return str(method_row.get("algorithm_key") or method_row.get("method_key") or "")


def compute_segmentation_strength(
    clustering: ClusterMetrics,
    selected_method: ClusterMethod | None = None,
    recommended_method: ClusterMethod | None = None,
    cluster_count: int | None = None,
    recommended_k: int | None = None,
) -> ClusterLabel:
    silhouette = float(clustering.get("silhouette") or 0.0)
    davies_bouldin = float(clustering.get("davies_bouldin") or 0.0)
    balance_ratio = float(clustering.get("cluster_balance_ratio") or 0.0)
    stability_ari = float(clustering.get("stability_ari") or 0.0)
    initialization_ari = float(clustering.get("initialization_ari") or 0.0)
    has_microclusters = bool(clustering.get("has_microclusters"))
    selected_algorithm_key = compute_method_algorithm_key(selected_method)
    recommended_algorithm_key = compute_method_algorithm_key(recommended_method)
    algorithm_mismatch = bool(selected_method and recommended_method) and selected_algorithm_key != recommended_algorithm_key
    configuration_mismatch = bool(selected_method and recommended_method) and (
        (selected_method or {}).get("method_key") != (recommended_method or {}).get("method_key")
    )
    k_mismatch = bool(recommended_k and cluster_count) and int(recommended_k) != int(cluster_count)
    stability_gap = initialization_ari - stability_ari if initialization_ari else 0.0
    requires_caution = configuration_mismatch or k_mismatch or stability_gap >= 0.18

    if (
        not has_microclusters
        and silhouette >= 0.40
        and davies_bouldin <= 1.00
        and stability_ari >= 0.70
        and balance_ratio >= 0.18
        and not requires_caution
    ):
        return {
            "label": "РЎРёР»СЊРЅР°СЏ",
            "note": "РЎРµРіРјРµРЅС‚Р°С†РёСЏ РІС‹РіР»СЏРґРёС‚ СЃРёР»СЊРЅРѕР№: РјРµС‚СЂРёРєРё СЃРѕРіР»Р°СЃРѕРІР°РЅС‹ РјРµР¶РґСѓ СЃРѕР±РѕР№, РєР»Р°СЃС‚РµСЂС‹ Р·Р°РјРµС‚РЅРѕ РѕС‚РґРµР»СЏСЋС‚СЃСЏ Рё РІ С†РµР»РѕРј РІРѕСЃСЃРїСЂРѕРёР·РІРѕРґСЏС‚СЃСЏ РЅР° РїРѕРІС‚РѕСЂРЅС‹С… РїРѕРґРІС‹Р±РѕСЂРєР°С….",
        }
    if not has_microclusters and silhouette >= 0.25 and davies_bouldin <= 1.30 and stability_ari >= 0.45 and balance_ratio >= 0.10:
        caution_suffix = ""
        if algorithm_mismatch:
            caution_suffix = " РџСЂРё СЌС‚РѕРј РёС‚РѕРі Р»СѓС‡С€Рµ С‚СЂР°РєС‚РѕРІР°С‚СЊ РѕСЃС‚РѕСЂРѕР¶РЅРµРµ: РґР»СЏ С‚РµРєСѓС‰РµРіРѕ СЃСЂРµР·Р° СѓР¶Рµ РІРёРґРµРЅ Р±РѕР»РµРµ СѓР±РµРґРёС‚РµР»СЊРЅС‹Р№ Р°Р»СЊС‚РµСЂРЅР°С‚РёРІРЅС‹Р№ РјРµС‚РѕРґ."
        elif configuration_mismatch:
            caution_suffix = " РџСЂРё СЌС‚РѕРј РёС‚РѕРі Р»СѓС‡С€Рµ С‚СЂР°РєС‚РѕРІР°С‚СЊ РѕСЃС‚РѕСЂРѕР¶РЅРµРµ: РЅР° С‚РѕРј Р¶Рµ РЅР°Р±РѕСЂРµ РїСЂРёР·РЅР°РєРѕРІ Р±РѕР»РµРµ СѓР±РµРґРёС‚РµР»СЊРЅРѕ РІС‹РіР»СЏРґРёС‚ РґСЂСѓРіР°СЏ РєРѕРЅС„РёРіСѓСЂР°С†РёСЏ РІРµСЃРѕРІ РёР»Рё РїР°СЂР°РјРµС‚СЂРѕРІ."
        elif k_mismatch:
            caution_suffix = " РџСЂРё СЌС‚РѕРј РёС‚РѕРі Р»СѓС‡С€Рµ С‚СЂР°РєС‚РѕРІР°С‚СЊ РѕСЃС‚РѕСЂРѕР¶РЅРµРµ: СЂР°Р±РѕС‡РµРµ С‡РёСЃР»Рѕ РєР»Р°СЃС‚РµСЂРѕРІ РЅРµ СЃРѕРІРїР°РґР°РµС‚ СЃ СЂРµРєРѕРјРµРЅРґР°С†РёРµР№ РїРѕ СЃРѕРІРѕРєСѓРїРЅРѕСЃС‚Рё РјРµС‚СЂРёРє."
        elif stability_gap >= 0.18:
            caution_suffix = " РџСЂРё СЌС‚РѕРј РёС‚РѕРі Р»СѓС‡С€Рµ С‚СЂР°РєС‚РѕРІР°С‚СЊ РѕСЃС‚РѕСЂРѕР¶РЅРµРµ: СѓСЃС‚РѕР№С‡РёРІРѕСЃС‚СЊ РЅР° РѕРґРЅРѕРј Рё С‚РѕРј Р¶Рµ РґР°С‚Р°СЃРµС‚Рµ Р·Р°РјРµС‚РЅРѕ РІС‹С€Рµ, С‡РµРј РЅР° РїРѕРІС‚РѕСЂРЅС‹С… РїРѕРґРІС‹Р±РѕСЂРєР°С…."
        return {
            "label": "РЈРјРµСЂРµРЅРЅР°СЏ",
            "note": (
                "РЎРµРіРјРµРЅС‚Р°С†РёСЏ РІС‹РіР»СЏРґРёС‚ СѓРјРµСЂРµРЅРЅРѕР№: С‚РёРїРѕР»РѕРіРёСЏ СѓР¶Рµ С‡РёС‚Р°РµС‚СЃСЏ, РЅРѕ С‡Р°СЃС‚СЊ РіСЂР°РЅРёС† РјРµР¶РґСѓ РєР»Р°СЃС‚РµСЂР°РјРё РѕСЃС‚Р°С‘С‚СЃСЏ С‡СѓРІСЃС‚РІРёС‚РµР»СЊРЅРѕР№ Рє СЃРѕСЃС‚Р°РІСѓ РґР°РЅРЅС‹С… РёР»Рё Рє Р±Р°Р»Р°РЅСЃСѓ СЂР°Р·РјРµСЂРѕРІ РіСЂСѓРїРї."
                f"{caution_suffix}"
            ),
        }
    return {
        "label": "РЎР»Р°Р±Р°СЏ",
        "note": "РЎРµРіРјРµРЅС‚Р°С†РёСЏ РІС‹РіР»СЏРґРёС‚ СЃР»Р°Р±РѕР№: Р»РёР±Рѕ РјРµС‚СЂРёРєРё РјРµР¶РґСѓ СЃРѕР±РѕР№ РЅРµ СЃРѕРіР»Р°СЃРѕРІР°РЅС‹, Р»РёР±Рѕ СЂР°Р·Р±РёРµРЅРёРµ СЃР»РёС€РєРѕРј С‡СѓРІСЃС‚РІРёС‚РµР»СЊРЅРѕ Рє СЃРѕСЃС‚Р°РІСѓ РІС‹Р±РѕСЂРєРё, Р»РёР±Рѕ РµРіРѕ РєР°С‡РµСЃС‚РІРѕ РїСЂРѕСЃРµРґР°РµС‚ РёР·-Р·Р° РјРёРєСЂРѕРєР»Р°СЃС‚РµСЂРѕРІ Рё РґРёСЃР±Р°Р»Р°РЅСЃР°.",
    }


def compute_diagnostics_row_sort_key(result: ClusteringMethodRow) -> tuple[float, float, float, float, float]:
    davies_bouldin = result.get("davies_bouldin", float("inf"))
    davies_value = float("inf") if davies_bouldin is None else float(davies_bouldin)
    return (
        float(result.get("quality_score", float("-inf"))),
        float(result.get("silhouette", float("-inf"))),
        -float(davies_value if math.isfinite(davies_value) else 1e9),
        float(result.get("cluster_balance_ratio", 0.0)),
        -float(result.get("shape_penalty", 0.0)),
    )


def compute_recommended_method_row(method_rows: Sequence[ClusteringMethodRow]) -> ClusteringMethodRow | None:
    if not method_rows:
        return None
    current_row = next((row for row in method_rows if row.get("is_selected")), None)
    if current_row is None:
        current_row = next(
            (
                row
                for row in method_rows
                if str(row.get("algorithm_key") or row.get("method_key") or "").startswith("kmeans")
            ),
            method_rows[0],
        )
    best_row = max(method_rows, key=compute_diagnostics_row_sort_key)
    if best_row.get("method_key") == current_row.get("method_key"):
        return current_row

    quality_gap = float(best_row.get("quality_score") or 0.0) - float(current_row.get("quality_score") or 0.0)
    balance_gap = float(best_row.get("cluster_balance_ratio") or 0.0) - float(current_row.get("cluster_balance_ratio") or 0.0)
    smallest_gap = int(best_row.get("smallest_cluster_size") or 0) - int(current_row.get("smallest_cluster_size") or 0)
    if (
        quality_gap >= 0.01
        and not bool(best_row.get("has_microclusters"))
        and float(best_row.get("shape_penalty") or 0.0) <= float(current_row.get("shape_penalty") or 0.0) + 0.01
        and balance_gap >= -0.05
        and smallest_gap >= -2
    ):
        return best_row
    return current_row

