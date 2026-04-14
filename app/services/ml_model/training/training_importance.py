from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List

import numpy as np
import pandas as pd

try:
    from joblib import parallel_backend
except Exception:  # pragma: no cover - optional dependency
    parallel_backend = None

try:
    from sklearn.inspection import permutation_importance
except Exception:  # pragma: no cover - graceful fallback for older sklearn
    permutation_importance = None

from app.services.forecasting.utils import _format_number

from ..ml_model_types import FEATURE_LABELS, PERMUTATION_REPEATS
from .training_dataset import _build_design_matrix


def _build_feature_importance(model_bundle: dict[str, Any], dataset: pd.DataFrame) -> List[dict[str, Any]]:
    design = _build_design_matrix(dataset, model_bundle['columns'])
    target = dataset['count'].to_numpy(dtype=float)
    grouped_scores: Dict[str, float] = defaultdict(float)

    if permutation_importance is not None and model_bundle.get('backend') == 'sklearn':
        sample_size = min(len(design), 180)
        sample_X = design.tail(sample_size)
        sample_y = target[-sample_size:]
        try:
            if parallel_backend is not None:
                with parallel_backend('threading', n_jobs=-1):
                    result = permutation_importance(
                        model_bundle['model'],
                        sample_X,
                        sample_y,
                        n_repeats=PERMUTATION_REPEATS,
                        random_state=42,
                        scoring='neg_mean_absolute_error',
                        n_jobs=-1,
                    )
            else:
                result = permutation_importance(
                    model_bundle['model'],
                    sample_X,
                    sample_y,
                    n_repeats=PERMUTATION_REPEATS,
                    random_state=42,
                    scoring='neg_mean_absolute_error',
                    n_jobs=1,
                )
            for column_name, score in zip(sample_X.columns, result.importances_mean):
                grouped_scores[_aggregate_feature_name(column_name)] += max(0.0, float(score))
        except Exception:
            grouped_scores.clear()

    if not grouped_scores:
        fallback = _fallback_feature_importance(model_bundle)
        for column_name, score in fallback.items():
            grouped_scores[_aggregate_feature_name(column_name)] += max(0.0, float(score))

    total_score = sum(grouped_scores.values())
    if total_score <= 0:
        return []

    items = []
    for feature_name, score in sorted(grouped_scores.items(), key=lambda item: item[1], reverse=True):
        share = score / total_score
        items.append(
            {
                'feature': feature_name,
                'label': FEATURE_LABELS.get(feature_name, feature_name),
                'importance': round(float(share), 4),
                'importance_display': _format_number(float(share) * 100.0),
            }
        )
    return items


def _fallback_feature_importance(model_bundle: dict[str, Any]) -> Dict[str, float]:
    model = model_bundle['model']
    columns = model_bundle['columns']
    if hasattr(model, 'feature_importances_'):
        values = [float(item) for item in getattr(model, 'feature_importances_')]
        return dict(zip(columns, values))
    if hasattr(model, 'coef_'):
        raw = np.asarray(getattr(model, 'coef_'), dtype=float).reshape(-1)
        return dict(zip(columns, np.abs(raw)))
    if hasattr(model, 'params'):
        params = getattr(model, 'params')
        param_values = np.asarray(params, dtype=float).reshape(-1)
        if param_values.size == len(columns) + 1:
            param_values = param_values[1:]
        return dict(zip(columns, np.abs(param_values[: len(columns)])))
    return {column_name: 0.0 for column_name in columns}


def _aggregate_feature_name(column_name: str) -> str:
    if column_name.startswith('weekday_'):
        return 'weekday'
    if column_name.startswith('month_'):
        return 'month'
    return column_name
