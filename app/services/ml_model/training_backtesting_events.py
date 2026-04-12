from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sklearn.metrics import log_loss, roc_auc_score

from app.services.model_quality import compute_classification_metrics

from .constants import (
    CLASSIFICATION_THRESHOLD,
    EVENT_BASELINE_METHOD_LABEL,
    EVENT_BASELINE_ROLE_LABEL,
    EVENT_CLASSIFIER_ROLE_LABEL,
    EVENT_HEURISTIC_METHOD_LABEL,
    EVENT_HEURISTIC_ROLE_LABEL,
    EVENT_MODEL_LABEL,
    EVENT_PROBABILITY_REASON_SATURATED_EVENT_RATE,
    EVENT_PROBABILITY_REASON_SINGLE_CLASS_EVALUATION,
    EVENT_PROBABILITY_REASON_TOO_FEW_COMPARABLE_WINDOWS,
    EVENT_RATE_SATURATION_MARGIN,
    EVENT_SELECTION_RULE,
    MIN_BACKTEST_POINTS,
)
from .domain_types import BacktestEvaluationRow, EventComparisonRow, EventMetrics
from .training_backtesting_support import (
    _empty_float_array,
    _empty_int_array,
    _optional_float,
    _optional_float_array,
)
from .training_backtesting_types import (
    _EventMetricContext,
    _EventMetricInputs,
    _EventMetricMaskContext,
    _EventMetricSelection,
    _EventProbabilityScores,
    _HorizonEvaluationData,
)


def _event_rate(actuals: np.ndarray) -> Optional[float]:
    if actuals.size == 0:
        return None
    return float(np.mean(actuals))


def _has_both_event_classes(actuals: np.ndarray) -> bool:
    return bool(actuals.size and np.min(actuals) != np.max(actuals))


def _event_rate_is_saturated(event_rate: Optional[float]) -> bool:
    if event_rate is None:
        return True
    return event_rate <= EVENT_RATE_SATURATION_MARGIN or event_rate >= (1.0 - EVENT_RATE_SATURATION_MARGIN)


def _event_probability_note(
    reason_code: Optional[str],
    *,
    rows_used: int,
    event_rate: Optional[float],
) -> Optional[str]:
    if reason_code == EVENT_PROBABILITY_REASON_TOO_FEW_COMPARABLE_WINDOWS:
        if rows_used > 0:
            return (
                'Вероятностный блок события пожара скрыт: в rolling-origin backtesting доступно только '
                f'{rows_used} сопоставимых окон, где можно корректно сравнить вероятности.'
            )
        return (
            'Вероятностный блок события пожара скрыт: в rolling-origin backtesting слишком мало сопоставимых окон, '
            'где можно корректно сравнить вероятности.'
        )
    if reason_code == EVENT_PROBABILITY_REASON_SINGLE_CLASS_EVALUATION:
        class_note = (
            'только дни с пожаром'
            if event_rate is not None and event_rate >= 0.5
            else 'только дни без пожара'
        )
        if rows_used > 0:
            return (
                'Вероятностный блок события пожара скрыт: '
                f'все {rows_used} evaluation-окон rolling-origin backtesting относятся к одному классу ({class_note}), '
                'поэтому вероятностная валидация некорректна.'
            )
        return (
            'Вероятностный блок события пожара скрыт: в evaluation-окнах rolling-origin backtesting наблюдался '
            f'только один класс ({class_note}), поэтому вероятностная валидация некорректна.'
        )
    if reason_code == EVENT_PROBABILITY_REASON_SATURATED_EVENT_RATE and event_rate is not None:
        return (
            'Вероятность P(>=1 пожара) скрыта: '
            f'доля события в evaluation-окнах rolling-origin backtesting составила {event_rate * 100.0:.1f}%, '
            'поэтому событие почти тривиально и неинформативно.'
        )
    return None


def _empty_event_metrics(
    *,
    rows_used: int,
    event_rate: Optional[float],
    evaluation_has_both_classes: bool,
    reason_code: Optional[str],
) -> EventMetrics:
    return EventMetrics(
        available=False,
        logistic_available=False,
        selected_model_key=None,
        selected_model_label=None,
        comparison_rows=[],
        rows_used=rows_used,
        selection_rule=EVENT_SELECTION_RULE,
        event_rate=event_rate,
        evaluation_has_both_classes=evaluation_has_both_classes,
        event_probability_informative=False,
        event_probability_note=_event_probability_note(
            reason_code,
            rows_used=rows_used,
            event_rate=event_rate,
        ),
        event_probability_reason_code=reason_code,
    )


def _normalized_event_model_label(selected_model_key: Optional[str], fallback_label: Optional[str]) -> Optional[str]:
    if selected_model_key == 'event_baseline':
        return EVENT_BASELINE_METHOD_LABEL
    if selected_model_key == 'heuristic_probability':
        return EVENT_HEURISTIC_METHOD_LABEL
    if selected_model_key == 'logistic_regression':
        return EVENT_MODEL_LABEL
    return fallback_label


def _normalize_event_comparison_rows(
    rows: List[Dict[str, Any] | EventComparisonRow],
) -> List[EventComparisonRow]:
    normalized_rows: List[EventComparisonRow] = []
    for row in rows:
        normalized_row = EventComparisonRow.coerce(row)
        method_key = str(normalized_row.method_key or '')
        if method_key == 'event_baseline':
            normalized_row = normalized_row.clone(
                method_label=EVENT_BASELINE_METHOD_LABEL,
                role_label=EVENT_BASELINE_ROLE_LABEL,
            )
        elif method_key == 'heuristic_probability':
            normalized_row = normalized_row.clone(
                method_label=EVENT_HEURISTIC_METHOD_LABEL,
                role_label=EVENT_HEURISTIC_ROLE_LABEL,
            )
        elif method_key == 'logistic_regression':
            normalized_row = normalized_row.clone(
                method_label=EVENT_MODEL_LABEL,
                role_label=EVENT_CLASSIFIER_ROLE_LABEL,
            )
        normalized_rows.append(normalized_row)
    return normalized_rows


def _event_metric_mask_context(
    *,
    baseline_probabilities: np.ndarray,
    heuristic_probabilities: np.ndarray,
    classifier_probabilities: np.ndarray,
) -> _EventMetricMaskContext:
    common_mask = np.isfinite(baseline_probabilities) & np.isfinite(heuristic_probabilities)
    common_rows = int(np.sum(common_mask))
    if common_rows < MIN_BACKTEST_POINTS:
        return _EventMetricMaskContext(
            common_rows=common_rows,
            evaluation_mask=common_mask,
            rows_used=common_rows,
            logistic_available=False,
        )

    classifier_mask = common_mask & np.isfinite(classifier_probabilities)
    logistic_available = int(np.sum(classifier_mask)) >= MIN_BACKTEST_POINTS
    evaluation_mask = classifier_mask if logistic_available else common_mask
    return _EventMetricMaskContext(
        common_rows=common_rows,
        evaluation_mask=evaluation_mask,
        rows_used=int(np.sum(evaluation_mask)),
        logistic_available=logistic_available,
    )


def _empty_event_metric_inputs(common_rows: int) -> _EventMetricInputs:
    return _EventMetricInputs(
        common_rows=common_rows,
        rows_used=common_rows,
        actuals=_empty_int_array(),
        baseline_probabilities=_empty_float_array(),
        heuristic_probabilities=_empty_float_array(),
        classifier_probabilities=_empty_float_array(),
        logistic_available=False,
    )


def _masked_event_metric_inputs(
    *,
    actual_events: np.ndarray,
    baseline_probabilities: np.ndarray,
    heuristic_probabilities: np.ndarray,
    classifier_probabilities: np.ndarray,
    mask_context: _EventMetricMaskContext,
) -> _EventMetricInputs:
    evaluation_mask = mask_context.evaluation_mask
    return _EventMetricInputs(
        common_rows=mask_context.common_rows,
        rows_used=mask_context.rows_used,
        actuals=actual_events[evaluation_mask].astype(int, copy=False),
        baseline_probabilities=baseline_probabilities[evaluation_mask],
        heuristic_probabilities=heuristic_probabilities[evaluation_mask],
        classifier_probabilities=(
            classifier_probabilities[evaluation_mask] if mask_context.logistic_available else _empty_float_array()
        ),
        logistic_available=mask_context.logistic_available,
    )


def _build_event_metric_inputs_from_arrays(
    *,
    actual_events: np.ndarray,
    baseline_probabilities: np.ndarray,
    heuristic_probabilities: np.ndarray,
    classifier_probabilities: np.ndarray,
) -> _EventMetricInputs:
    mask_context = _event_metric_mask_context(
        baseline_probabilities=baseline_probabilities,
        heuristic_probabilities=heuristic_probabilities,
        classifier_probabilities=classifier_probabilities,
    )
    if mask_context.common_rows < MIN_BACKTEST_POINTS:
        return _empty_event_metric_inputs(mask_context.common_rows)
    return _masked_event_metric_inputs(
        actual_events=actual_events,
        baseline_probabilities=baseline_probabilities,
        heuristic_probabilities=heuristic_probabilities,
        classifier_probabilities=classifier_probabilities,
        mask_context=mask_context,
    )


def _event_metric_arrays(
    rows: List[Dict[str, Any] | BacktestEvaluationRow] | _HorizonEvaluationData,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    if isinstance(rows, _HorizonEvaluationData):
        return (
            rows.actual_events,
            rows.baseline_event_probabilities,
            rows.heuristic_event_probabilities,
            rows.selected_event_probabilities,
        )

    row_count = len(rows)
    return (
        np.fromiter((int(row.get('actual_event', 0)) for row in rows), dtype=int, count=row_count),
        _optional_float_array(row.get('baseline_event_probability') for row in rows),
        _optional_float_array(row.get('heuristic_event_probability') for row in rows),
        _optional_float_array(row.get('predicted_event_probability') for row in rows),
    )


def _event_metric_inputs(
    rows: List[Dict[str, Any] | BacktestEvaluationRow] | _HorizonEvaluationData,
) -> _EventMetricInputs:
    (
        actual_events,
        baseline_probabilities,
        heuristic_probabilities,
        classifier_probabilities,
    ) = _event_metric_arrays(rows)
    return _build_event_metric_inputs_from_arrays(
        actual_events=actual_events,
        baseline_probabilities=baseline_probabilities,
        heuristic_probabilities=heuristic_probabilities,
        classifier_probabilities=classifier_probabilities,
    )


def _score_event_probability_candidates(
    event_inputs: _EventMetricInputs,
) -> _EventProbabilityScores:
    actuals = event_inputs.actuals
    heuristic_metrics = compute_classification_metrics(
        actuals,
        event_inputs.heuristic_probabilities,
        event_inputs.baseline_probabilities,
        threshold=CLASSIFICATION_THRESHOLD,
    )
    return _EventProbabilityScores(
        heuristic_metrics=heuristic_metrics,
        baseline_roc_auc=_safe_roc_auc(actuals, event_inputs.baseline_probabilities),
        heuristic_roc_auc=_safe_roc_auc(actuals, event_inputs.heuristic_probabilities),
        baseline_log_loss=_safe_log_loss(actuals, event_inputs.baseline_probabilities),
        heuristic_log_loss=_safe_log_loss(actuals, event_inputs.heuristic_probabilities),
    )


def _initial_event_metric_selection(
    *,
    heuristic_metrics: Dict[str, Any],
    baseline_roc_auc: Optional[float],
    heuristic_roc_auc: Optional[float],
    baseline_log_loss: Optional[float],
    heuristic_log_loss: Optional[float],
) -> _EventMetricSelection:
    return _EventMetricSelection(
        selected_model_key='heuristic_probability',
        selected_model_label=EVENT_HEURISTIC_METHOD_LABEL,
        selected_metrics=heuristic_metrics,
        selected_roc_auc=heuristic_roc_auc,
        selected_log_loss=heuristic_log_loss,
        comparison_rows=[
            EventComparisonRow(
                method_key='event_baseline',
                method_label=EVENT_BASELINE_METHOD_LABEL,
                role_label=EVENT_BASELINE_ROLE_LABEL,
                brier_score=_optional_float(heuristic_metrics.get('baseline_brier_score')),
                roc_auc=baseline_roc_auc,
                f1=_optional_float(heuristic_metrics.get('baseline_f1')),
                log_loss=baseline_log_loss,
                is_selected=False,
            ),
            EventComparisonRow(
                method_key='heuristic_probability',
                method_label=EVENT_HEURISTIC_METHOD_LABEL,
                role_label=EVENT_HEURISTIC_ROLE_LABEL,
                brier_score=_optional_float(heuristic_metrics.get('brier_score')),
                roc_auc=heuristic_roc_auc,
                f1=_optional_float(heuristic_metrics.get('f1')),
                log_loss=heuristic_log_loss,
                is_selected=True,
            ),
        ],
    )


def _with_classifier_event_selection(
    selection: _EventMetricSelection,
    *,
    event_inputs: _EventMetricInputs,
    event_probability_informative: bool,
    heuristic_metrics: Dict[str, Any],
    heuristic_log_loss: Optional[float],
    heuristic_roc_auc: Optional[float],
) -> _EventMetricSelection:
    if not event_inputs.logistic_available:
        return selection

    actuals = event_inputs.actuals
    classifier_metrics = compute_classification_metrics(
        actuals,
        event_inputs.classifier_probabilities,
        event_inputs.baseline_probabilities,
        threshold=CLASSIFICATION_THRESHOLD,
    )
    classifier_roc_auc = _safe_roc_auc(actuals, event_inputs.classifier_probabilities)
    classifier_log_loss = _safe_log_loss(actuals, event_inputs.classifier_probabilities)
    classifier_selected = (
        event_probability_informative
        and bool(classifier_metrics.get('available'))
        and _event_metric_sort_key(
            classifier_metrics.get('brier_score'),
            classifier_log_loss,
            classifier_roc_auc,
            classifier_metrics.get('f1'),
        ) < _event_metric_sort_key(
            heuristic_metrics.get('brier_score'),
            heuristic_log_loss,
            heuristic_roc_auc,
            heuristic_metrics.get('f1'),
        )
    )
    comparison_rows = list(selection.comparison_rows)
    comparison_rows.append(
        EventComparisonRow(
            method_key='logistic_regression',
            method_label=EVENT_MODEL_LABEL,
            role_label=EVENT_CLASSIFIER_ROLE_LABEL,
            brier_score=_optional_float(classifier_metrics.get('brier_score')),
            roc_auc=classifier_roc_auc,
            f1=_optional_float(classifier_metrics.get('f1')),
            log_loss=classifier_log_loss,
            is_selected=classifier_selected,
        )
    )
    if not classifier_selected:
        return _EventMetricSelection(
            selected_model_key=selection.selected_model_key,
            selected_model_label=selection.selected_model_label,
            selected_metrics=selection.selected_metrics,
            selected_roc_auc=selection.selected_roc_auc,
            selected_log_loss=selection.selected_log_loss,
            comparison_rows=comparison_rows,
        )

    comparison_rows[1] = comparison_rows[1].clone(is_selected=False)
    return _EventMetricSelection(
        selected_model_key='logistic_regression',
        selected_model_label=EVENT_MODEL_LABEL,
        selected_metrics=classifier_metrics,
        selected_roc_auc=classifier_roc_auc,
        selected_log_loss=classifier_log_loss,
        comparison_rows=comparison_rows,
    )


def _event_metric_context(event_inputs: _EventMetricInputs) -> _EventMetricContext:
    event_rate = _event_rate(event_inputs.actuals)
    event_probability_informative = not _event_rate_is_saturated(event_rate)
    event_probability_reason_code = (
        EVENT_PROBABILITY_REASON_SATURATED_EVENT_RATE if not event_probability_informative else None
    )
    return _EventMetricContext(
        event_rate=event_rate,
        evaluation_has_both_classes=_has_both_event_classes(event_inputs.actuals),
        event_probability_informative=event_probability_informative,
        event_probability_note=_event_probability_note(
            event_probability_reason_code,
            rows_used=event_inputs.rows_used,
            event_rate=event_rate,
        ),
        event_probability_reason_code=event_probability_reason_code,
    )


def _build_event_metrics_result(
    *,
    event_inputs: _EventMetricInputs,
    context: _EventMetricContext,
    probability_scores: _EventProbabilityScores,
    selection: _EventMetricSelection,
) -> EventMetrics:
    heuristic_metrics = probability_scores.heuristic_metrics
    return EventMetrics(
        available=True,
        logistic_available=event_inputs.logistic_available,
        selected_model_key=selection.selected_model_key,
        selected_model_label=_normalized_event_model_label(selection.selected_model_key, selection.selected_model_label),
        brier_score=_optional_float(selection.selected_metrics.get('brier_score')),
        baseline_brier_score=_optional_float(heuristic_metrics.get('baseline_brier_score')),
        heuristic_brier_score=_optional_float(heuristic_metrics.get('brier_score')),
        roc_auc=selection.selected_roc_auc,
        baseline_roc_auc=probability_scores.baseline_roc_auc,
        heuristic_roc_auc=probability_scores.heuristic_roc_auc,
        f1=_optional_float(selection.selected_metrics.get('f1')),
        baseline_f1=_optional_float(heuristic_metrics.get('baseline_f1')),
        heuristic_f1=_optional_float(heuristic_metrics.get('f1')),
        log_loss=selection.selected_log_loss,
        baseline_log_loss=probability_scores.baseline_log_loss,
        heuristic_log_loss=probability_scores.heuristic_log_loss,
        comparison_rows=_normalize_event_comparison_rows(selection.comparison_rows),
        rows_used=event_inputs.rows_used,
        selection_rule=EVENT_SELECTION_RULE,
        event_rate=context.event_rate,
        evaluation_has_both_classes=context.evaluation_has_both_classes,
        event_probability_informative=context.event_probability_informative,
        event_probability_note=context.event_probability_note,
        event_probability_reason_code=context.event_probability_reason_code,
    )


def _compute_event_metrics(
    rows: List[Dict[str, Any] | BacktestEvaluationRow] | _HorizonEvaluationData,
) -> EventMetrics:
    event_inputs = _event_metric_inputs(rows)
    if event_inputs.common_rows < MIN_BACKTEST_POINTS:
        return _empty_event_metrics(
            rows_used=event_inputs.common_rows,
            event_rate=None,
            evaluation_has_both_classes=False,
            reason_code=EVENT_PROBABILITY_REASON_TOO_FEW_COMPARABLE_WINDOWS,
        )

    context = _event_metric_context(event_inputs)
    if not context.evaluation_has_both_classes:
        return _empty_event_metrics(
            rows_used=event_inputs.rows_used,
            event_rate=context.event_rate,
            evaluation_has_both_classes=False,
            reason_code=EVENT_PROBABILITY_REASON_SINGLE_CLASS_EVALUATION,
        )

    probability_scores = _score_event_probability_candidates(event_inputs)
    selection = _initial_event_metric_selection(
        heuristic_metrics=probability_scores.heuristic_metrics,
        baseline_roc_auc=probability_scores.baseline_roc_auc,
        heuristic_roc_auc=probability_scores.heuristic_roc_auc,
        baseline_log_loss=probability_scores.baseline_log_loss,
        heuristic_log_loss=probability_scores.heuristic_log_loss,
    )
    selection = _with_classifier_event_selection(
        selection,
        event_inputs=event_inputs,
        event_probability_informative=context.event_probability_informative,
        heuristic_metrics=probability_scores.heuristic_metrics,
        heuristic_log_loss=probability_scores.heuristic_log_loss,
        heuristic_roc_auc=probability_scores.heuristic_roc_auc,
    )

    return _build_event_metrics_result(
        event_inputs=event_inputs,
        context=context,
        probability_scores=probability_scores,
        selection=selection,
    )


def _event_metric_sort_key(
    brier_score: Optional[float],
    log_loss_value: Optional[float],
    roc_auc: Optional[float],
    f1_score: Optional[float],
) -> Tuple[float, float, float, float]:
    return (
        brier_score if brier_score is not None else float('inf'),
        log_loss_value if log_loss_value is not None else float('inf'),
        -(roc_auc if roc_auc is not None else -1.0),
        -(f1_score if f1_score is not None else -1.0),
    )


def _safe_roc_auc(actuals: np.ndarray, probabilities: np.ndarray) -> Optional[float]:
    if len(np.unique(actuals)) <= 1:
        return None
    return float(roc_auc_score(actuals, probabilities))


def _safe_log_loss(actuals: np.ndarray, probabilities: np.ndarray) -> Optional[float]:
    if actuals.size == 0:
        return None
    clipped = np.clip(probabilities.astype(float, copy=False), 0.001, 0.999)
    return float(log_loss(actuals, clipped, labels=[0, 1]))
