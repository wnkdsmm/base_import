from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from app.services.model_quality import relative_delta

from ..ml_model_types import (
    CountComparisonRow,
    CountMetrics,
    COUNT_MODEL_KEYS,
    COUNT_MODEL_LABELS,
    COUNT_MODEL_SELECTION_TOLERANCE,
    EXPLAINABLE_COUNT_MODEL_KEY,
    MIN_POSITIVE_PREDICTION,
)


def _available_count_model_labels(count_metrics: Dict[str, CountMetrics]) -> List[str]:
    labels = [
        COUNT_MODEL_LABELS.get('seasonal_baseline', 'Seasonal baseline'),
        COUNT_MODEL_LABELS.get('heuristic_forecast', 'Heuristic forecast'),
    ]
    labels.extend(COUNT_MODEL_LABELS.get(model_key, model_key) for model_key in COUNT_MODEL_KEYS if model_key in count_metrics)
    return labels


def _all_count_metrics(
    baseline_metrics: Dict[str, Optional[float]] | CountMetrics,
    heuristic_metrics: Dict[str, Optional[float]] | CountMetrics,
    count_metrics: Dict[str, Dict[str, Optional[float]] | CountMetrics],
) -> Dict[str, CountMetrics]:
    metrics: Dict[str, CountMetrics] = {
        'seasonal_baseline': CountMetrics.coerce(baseline_metrics),
        'heuristic_forecast': CountMetrics.coerce(heuristic_metrics),
    }
    metrics.update(
        {
            model_key: CountMetrics.coerce(values)
            for model_key, values in count_metrics.items()
            if values is not None
        }
    )
    return metrics


def _metric_sort_key(metrics: Dict[str, Optional[float]] | CountMetrics) -> Tuple[float, float, float, float]:
    metrics = CountMetrics.coerce(metrics)
    return (
        metrics.poisson_deviance if metrics.poisson_deviance is not None else float('inf'),
        metrics.mae if metrics.mae is not None else float('inf'),
        metrics.rmse if metrics.rmse is not None else float('inf'),
        metrics.smape if metrics.smape is not None else float('inf'),
    )


def _within_relative_margin(candidate: Optional[float], reference: Optional[float], tolerance: float) -> bool:
    if candidate is None or reference is None:
        return False
    if reference <= MIN_POSITIVE_PREDICTION:
        return candidate <= reference + tolerance
    return candidate <= reference * (1.0 + tolerance)


def _metrics_within_selection_tolerance(
    candidate_metrics: Dict[str, Optional[float]] | CountMetrics,
    reference_metrics: Dict[str, Optional[float]] | CountMetrics,
) -> bool:
    candidate = CountMetrics.coerce(candidate_metrics)
    reference = CountMetrics.coerce(reference_metrics)
    return _within_relative_margin(
        candidate.poisson_deviance,
        reference.poisson_deviance,
        COUNT_MODEL_SELECTION_TOLERANCE,
    ) and _within_relative_margin(
        candidate.mae,
        reference.mae,
        COUNT_MODEL_SELECTION_TOLERANCE,
    ) and _within_relative_margin(
        candidate.rmse,
        reference.rmse,
        COUNT_MODEL_SELECTION_TOLERANCE,
    )


def _select_count_model(
    count_metrics: Dict[str, Dict[str, Optional[float]] | CountMetrics],
) -> Tuple[str, CountMetrics]:
    normalized_metrics = {
        model_key: CountMetrics.coerce(metrics)
        for model_key, metrics in count_metrics.items()
        if metrics is not None
    }
    poisson_metrics = normalized_metrics.get(EXPLAINABLE_COUNT_MODEL_KEY)
    negative_binomial_metrics = normalized_metrics.get('negative_binomial')

    if poisson_metrics is None and negative_binomial_metrics is None:
        raise ValueError('No ML count metrics are available for selection.')
    if poisson_metrics is None:
        return 'negative_binomial', negative_binomial_metrics
    if negative_binomial_metrics is None:
        return EXPLAINABLE_COUNT_MODEL_KEY, poisson_metrics

    if _metric_sort_key(negative_binomial_metrics) < _metric_sort_key(poisson_metrics) and not _metrics_within_selection_tolerance(
        poisson_metrics,
        negative_binomial_metrics,
    ):
        return 'negative_binomial', negative_binomial_metrics
    return EXPLAINABLE_COUNT_MODEL_KEY, poisson_metrics


def _select_count_method(
    baseline_metrics: Dict[str, Optional[float]] | CountMetrics,
    heuristic_metrics: Dict[str, Optional[float]] | CountMetrics,
    count_metrics: Dict[str, Dict[str, Optional[float]] | CountMetrics],
) -> Tuple[str, CountMetrics, dict[str, Any]]:
    all_metrics = _all_count_metrics(baseline_metrics, heuristic_metrics, count_metrics)
    ranking = sorted(all_metrics.items(), key=lambda item: _metric_sort_key(item[1]))
    raw_best_key, raw_best_metrics = ranking[0]

    selected_key = raw_best_key
    tie_break_reason = None
    if raw_best_key in COUNT_MODEL_KEYS and _metrics_within_selection_tolerance(heuristic_metrics, raw_best_metrics):
        selected_key = 'heuristic_forecast'
        tie_break_reason = 'heuristic_over_ml'
    elif raw_best_key == 'negative_binomial':
        selected_ml_key, _ = _select_count_model(count_metrics)
        if selected_ml_key != raw_best_key:
            selected_key = selected_ml_key
            tie_break_reason = 'poisson_over_negative_binomial'

    runner_up_key = raw_best_key if raw_best_key != selected_key else None
    if runner_up_key is None:
        runner_up_key = next((candidate_key for candidate_key, _ in ranking if candidate_key != selected_key), None)

    return selected_key, all_metrics[selected_key], {
        'raw_best_key': raw_best_key,
        'raw_best_metrics': raw_best_metrics,
        'runner_up_key': runner_up_key,
        'tie_break_reason': tie_break_reason,
        'all_metrics': all_metrics,
    }


def _build_count_selection_details(
    selected_count_model_key: str,
    selected_metrics: Dict[str, Optional[float]] | CountMetrics,
    count_metrics: Dict[str, Dict[str, Optional[float]] | CountMetrics],
    baseline_metrics: Dict[str, Optional[float]] | CountMetrics,
    heuristic_metrics: Dict[str, Optional[float]] | CountMetrics,
    overdispersion_ratio: float,
    raw_best_key: Optional[str] = None,
    tie_break_reason: Optional[str] = None,
) -> Dict[str, str]:
    all_metrics = _all_count_metrics(baseline_metrics, heuristic_metrics, count_metrics)
    ranking = sorted(all_metrics.items(), key=lambda item: _metric_sort_key(item[1]))
    raw_best_key = raw_best_key or ranking[0][0]
    runner_up_key = raw_best_key if raw_best_key != selected_count_model_key else None
    if runner_up_key is None:
        runner_up_key = next((candidate_key for candidate_key, _ in ranking if candidate_key != selected_count_model_key), None)

    selected_label = COUNT_MODEL_LABELS.get(selected_count_model_key, selected_count_model_key)
    runner_up_label = COUNT_MODEL_LABELS.get(runner_up_key, runner_up_key) if runner_up_key else None
    selected_metrics = CountMetrics.coerce(selected_metrics)
    heuristic_metrics = CountMetrics.coerce(heuristic_metrics)
    baseline_delta = selected_metrics.mae_delta_vs_baseline

    if selected_count_model_key == 'seasonal_baseline':
        short_reason = 'Р’С‹Р±СЂР°РЅ seasonal baseline: РЅР° rolling-origin РѕРєРЅР°С… СЌС‚Рѕ Р±С‹Р» Р»СѓС‡С€РёР№ СЂР°Р±РѕС‡РёР№ РјРµС‚РѕРґ СЃСЂРµРґРё РІСЃРµС… РєР°РЅРґРёРґР°С‚РѕРІ.'
        long_reason = (
            'Seasonal baseline СЃС‚Р°Р» СЂР°Р±РѕС‡РёРј count-РјРµС‚РѕРґРѕРј РїРѕ СЂРµР·СѓР»СЊС‚Р°С‚Р°Рј rolling-origin backtesting, '
            'РїРѕС‚РѕРјСѓ С‡С‚Рѕ РїРѕ РїСЂР°РІРёР»Сѓ РѕС‚Р±РѕСЂР° РѕР±РѕС€С‘Р» heuristic forecast Рё РѕР±СѓС‡Р°РµРјС‹Рµ count-model.'
        )
    elif selected_count_model_key == 'heuristic_forecast' and tie_break_reason == 'heuristic_over_ml' and raw_best_key in COUNT_MODEL_KEYS:
        short_reason = (
            'Р’С‹Р±СЂР°РЅ heuristic forecast: РѕРЅ РїРѕС‡С‚Рё РЅРµ С…СѓР¶Рµ Р»СѓС‡С€РµР№ count-model, '
            'Р° explainability tie-break СЃРѕС…СЂР°РЅСЏРµС‚ Р±РѕР»РµРµ РѕР±СЉСЏСЃРЅРёРјС‹Р№ РјРµС‚РѕРґ.'
        )
        long_reason = (
            f'Р›СѓС‡С€РµР№ РѕР±СѓС‡Р°РµРјРѕР№ count-model РїРѕ РјРµС‚СЂРёРєР°Рј Р±С‹Р»Р° {COUNT_MODEL_LABELS.get(raw_best_key, raw_best_key)}, '
            f'РЅРѕ heuristic forecast СѓСЃС‚СѓРїРёР» РјРµРЅРµРµ С‡РµРј РЅР° {int(COUNT_MODEL_SELECTION_TOLERANCE * 100)}% '
            'РїРѕ Poisson deviance, MAE Рё RMSE. РџРѕ explainability tie-break СЂР°Р±РѕС‡РёРј РјРµС‚РѕРґРѕРј РѕСЃС‚Р°РІР»РµРЅ '
            'heuristic forecast.'
        )
    elif selected_count_model_key == 'heuristic_forecast':
        short_reason = 'Р’С‹Р±СЂР°РЅ heuristic forecast: РЅР° rolling-origin РѕРєРЅР°С… СЌС‚Рѕ Р±С‹Р» Р»СѓС‡С€РёР№ СЂР°Р±РѕС‡РёР№ РјРµС‚РѕРґ СЃСЂРµРґРё РІСЃРµС… РєР°РЅРґРёРґР°С‚РѕРІ.'
        long_reason = (
            'Heuristic forecast СЃС‚Р°Р» СЂР°Р±РѕС‡РёРј count-РјРµС‚РѕРґРѕРј РїРѕ СЂРµР·СѓР»СЊС‚Р°С‚Р°Рј rolling-origin backtesting, '
            'РїРѕС‚РѕРјСѓ С‡С‚Рѕ РѕР±РѕС€С‘Р» seasonal baseline Рё РѕР±СѓС‡Р°РµРјС‹Рµ count-model РїРѕ РїСЂР°РІРёР»Сѓ РѕС‚Р±РѕСЂР°.'
        )
    elif selected_count_model_key == EXPLAINABLE_COUNT_MODEL_KEY and raw_best_key != selected_count_model_key:
        short_reason = 'Р’С‹Р±СЂР°РЅР° СЂРµРіСЂРµСЃСЃРёСЏ РџСѓР°СЃСЃРѕРЅР°: РєР°С‡РµСЃС‚РІРѕ Р±Р»РёР·РєРѕ Рє Р»СѓС‡С€РµР№ count-model, Р° РёРЅС‚РµСЂРїСЂРµС‚Р°С†РёСЏ РїСЂРѕС‰Рµ.'
        long_reason = (
            'Р РµРіСЂРµСЃСЃРёСЏ РџСѓР°СЃСЃРѕРЅР° РѕСЃС‚Р°РІР»РµРЅР° СЂР°Р±РѕС‡РёРј count-РјРµС‚РѕРґРѕРј, РїРѕС‚РѕРјСѓ С‡С‚Рѕ РЅР° rolling-origin backtesting '
            f'РµС‘ Poisson deviance, MAE Рё RMSE РѕС‚Р»РёС‡Р°СЋС‚СЃСЏ РѕС‚ Р»РёРґРµСЂР° {COUNT_MODEL_LABELS.get(raw_best_key, raw_best_key)} '
            f'РјРµРЅРµРµ С‡РµРј РЅР° {int(COUNT_MODEL_SELECTION_TOLERANCE * 100)}%, РЅРѕ СЌС‚Р° РјРѕРґРµР»СЊ РїСЂРѕС‰Рµ РґР»СЏ РёРЅС‚РµСЂРїСЂРµС‚Р°С†РёРё.'
        )
    elif selected_count_model_key == 'negative_binomial':
        short_reason = f'Р’С‹Р±СЂР°РЅР° {selected_label}: СЂСЏРґ РїРµСЂРµ-РґРёСЃРїРµСЂСЃРЅС‹Р№, Рё СЌС‚Р° РјРѕРґРµР»СЊ Р»СѓС‡С€Рµ СѓРґРµСЂР¶РёРІР°РµС‚ deviance.'
        long_reason = (
            f'{selected_label} РІС‹Р±СЂР°РЅР° РєР°Рє СЂР°Р±РѕС‡РёР№ count-РјРµС‚РѕРґ, РїРѕС‚РѕРјСѓ С‡С‚Рѕ СЂСЏРґ РїРѕРєР°Р·С‹РІР°РµС‚ РїРµСЂРµ-РґРёСЃРїРµСЂСЃРёСЋ '
            f'(РѕС‚РЅРѕС€РµРЅРёРµ variance/mean = {overdispersion_ratio:.2f}), Р° РЅР° rolling-origin backtesting РёРјРµРЅРЅРѕ '
            'СЌС‚Р° РјРѕРґРµР»СЊ РґР°Р»Р° РЅР°РёРјРµРЅСЊС€СѓСЋ Poisson deviance РїСЂРё СЃРѕС…СЂР°РЅРµРЅРёРё РёРЅС‚РµСЂРїСЂРµС‚РёСЂСѓРµРјРѕР№ GLM-СЃС‚СЂСѓРєС‚СѓСЂС‹.'
        )
    else:
        short_reason = f'Р’С‹Р±СЂР°РЅР° {selected_label}: Р»СѓС‡С€РёР№ Р±Р°Р»Р°РЅСЃ deviance, MAE Рё RMSE РЅР° rolling-origin.'
        long_reason = (
            f'{selected_label} РІС‹Р±СЂР°РЅР° РїРѕ СЂРµР·СѓР»СЊС‚Р°С‚Р°Рј rolling-origin backtesting, РїРѕС‚РѕРјСѓ С‡С‚Рѕ РїРѕРєР°Р·Р°Р»Р° Р»СѓС‡С€РёР№ '
            'СЂРµР·СѓР»СЊС‚Р°С‚ РїРѕ Poisson deviance, MAE Рё RMSE СЃСЂРµРґРё РІСЃРµС… count-РєР°РЅРґРёРґР°С‚РѕРІ.'
        )

    if runner_up_label:
        long_reason += f' Р‘Р»РёР¶Р°Р№С€РёР№ Р°Р»СЊС‚РµСЂРЅР°С‚РёРІРЅС‹Р№ РєР°РЅРґРёРґР°С‚: {runner_up_label}.'
    if baseline_delta is not None:
        long_reason += f' РР·РјРµРЅРµРЅРёРµ MAE РѕС‚РЅРѕСЃРёС‚РµР»СЊРЅРѕ seasonal baseline СЃРѕСЃС‚Р°РІРёР»Рѕ {baseline_delta * 100.0:+.1f}%.'
    heuristic_improvement = relative_delta(selected_metrics.mae, heuristic_metrics.mae)
    if heuristic_improvement is not None and selected_count_model_key != 'heuristic_forecast':
        long_reason += f' РР·РјРµРЅРµРЅРёРµ MAE РѕС‚РЅРѕСЃРёС‚РµР»СЊРЅРѕ heuristic forecast СЃРѕСЃС‚Р°РІРёР»Рѕ {heuristic_improvement * 100.0:+.1f}%.'

    return {
        'short': short_reason,
        'long': long_reason,
    }


def _build_count_comparison_rows(
    baseline_metrics: Dict[str, Optional[float]] | CountMetrics,
    heuristic_metrics: Dict[str, Optional[float]] | CountMetrics,
    count_metrics: Dict[str, Dict[str, Optional[float]] | CountMetrics],
    selected_count_model_key: str,
) -> List[CountComparisonRow]:
    rows: List[CountComparisonRow] = [
        CountComparisonRow(
            method_key='seasonal_baseline',
            method_label=COUNT_MODEL_LABELS.get('seasonal_baseline', 'Seasonal baseline'),
            role_label='Р‘Р°Р·РѕРІР°СЏ РјРѕРґРµР»СЊ',
            is_selected=selected_count_model_key == 'seasonal_baseline',
            metrics=CountMetrics.coerce(baseline_metrics),
        ),
        CountComparisonRow(
            method_key='heuristic_forecast',
            method_label=COUNT_MODEL_LABELS.get('heuristic_forecast', 'Heuristic forecast'),
            role_label='РЎС†РµРЅР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР·',
            is_selected=selected_count_model_key == 'heuristic_forecast',
            metrics=CountMetrics.coerce(heuristic_metrics),
        ),
    ]
    for model_key in COUNT_MODEL_KEYS:
        metrics = count_metrics.get(model_key)
        if not metrics:
            continue
        rows.append(
            CountComparisonRow(
                method_key=model_key,
                method_label=COUNT_MODEL_LABELS.get(model_key, model_key),
                role_label='РРЅС‚РµСЂРїСЂРµС‚РёСЂСѓРµРјР°СЏ count-РјРѕРґРµР»СЊ',
                is_selected=model_key == selected_count_model_key,
                metrics=CountMetrics.coerce(metrics),
            )
        )
    return rows
