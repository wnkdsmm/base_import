from __future__ import annotations

import warnings
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from sklearn.linear_model import LogisticRegression, PoissonRegressor, TweedieRegressor
except Exception:  # pragma: no cover - graceful fallback for older sklearn
    from sklearn.linear_model import LogisticRegression

    PoissonRegressor = None
    TweedieRegressor = None

try:
    from sklearn.compose import ColumnTransformer
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
except Exception:  # pragma: no cover - graceful fallback for older sklearn
    ColumnTransformer = None
    ConvergenceWarning = None
    Pipeline = None
    StandardScaler = None

try:
    import statsmodels.api as sm
    from statsmodels.tools.sm_exceptions import (
        ConvergenceWarning as StatsmodelsConvergenceWarning,
        HessianInversionWarning,
        PerfectSeparationWarning,
    )
except Exception:  # pragma: no cover - optional dependency
    sm = None
    StatsmodelsConvergenceWarning = None
    HessianInversionWarning = None
    PerfectSeparationWarning = None

from .constants import (
    COUNT_MODEL_CONTINUOUS_COLUMNS,
    MIN_EVENT_CLASS_COUNT,
    MIN_POSITIVE_PREDICTION,
    WARNING_INSTABILITY_MESSAGE_TOKENS,
    _LOGISTIC_PARAMS,
    _POISSON_PARAMS,
    _TWEEDIE_PARAMS,
)
from .training_dataset import _build_design_matrix


def _build_count_model(model_key: str):
    if model_key == 'poisson':
        if PoissonRegressor is None:
            return None
        return PoissonRegressor(**_POISSON_PARAMS)
    if model_key == 'tweedie':
        if TweedieRegressor is None:
            return None
        return TweedieRegressor(**_TWEEDIE_PARAMS)
    raise ValueError(f'Unsupported count model: {model_key}')


def _count_model_scaled_columns(columns: List[str]) -> List[str]:
    return [column for column in COUNT_MODEL_CONTINUOUS_COLUMNS if column in columns]


def _build_count_model_pipeline(model_key: str, X_train: pd.DataFrame):
    model = _build_count_model(model_key)
    if model is None:
        return None

    scaled_columns = _count_model_scaled_columns(list(X_train.columns))
    if ColumnTransformer is None or Pipeline is None or StandardScaler is None or not scaled_columns:
        return model

    preprocessor = ColumnTransformer(
        transformers=[
            ('scaled_continuous', StandardScaler(), scaled_columns),
        ],
        remainder='passthrough',
        sparse_threshold=0.0,
    )
    return Pipeline(
        steps=[
            ('preprocess', preprocessor),
            ('model', model),
        ]
    )


def _prepare_statsmodels_count_design(
    X_train: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[str], Optional[Any]]:
    prepared = X_train.copy()
    scaled_columns = _count_model_scaled_columns(list(prepared.columns))
    if StandardScaler is None or not scaled_columns:
        return prepared, [], None

    scaler = StandardScaler()
    prepared.loc[:, scaled_columns] = scaler.fit_transform(prepared[scaled_columns])
    return prepared, scaled_columns, scaler


def _warning_indicates_unstable_fit(warning_item: warnings.WarningMessage) -> bool:
    warning_categories = (
        ConvergenceWarning,
        StatsmodelsConvergenceWarning,
        PerfectSeparationWarning,
        HessianInversionWarning,
    )
    if any(category is not None and issubclass(warning_item.category, category) for category in warning_categories):
        return True

    message = str(warning_item.message).lower()
    return any(token in message for token in WARNING_INSTABILITY_MESSAGE_TOKENS)


def _has_warning_instability(caught_warnings: List[warnings.WarningMessage]) -> bool:
    return any(_warning_indicates_unstable_fit(item) for item in caught_warnings)


def _fit_with_convergence_guard(model: Any, X_train: pd.DataFrame, y_train: np.ndarray) -> bool:
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter('always')
        if ConvergenceWarning is not None:
            warnings.simplefilter('always', ConvergenceWarning)
        model.fit(X_train, y_train)
    return not _has_warning_instability(caught_warnings)


def _fit_count_model(model_key: str, frame: pd.DataFrame, feature_columns: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    X_train = _build_design_matrix(frame, feature_columns=feature_columns)
    y_train = frame['count'].to_numpy(dtype=float)
    return _fit_count_model_from_design(model_key, X_train, y_train)


def _fit_count_model_from_design(model_key: str, X_train: pd.DataFrame, y_train: np.ndarray) -> Optional[Dict[str, Any]]:
    if model_key == 'negative_binomial':
        return _fit_negative_binomial_model_from_design(X_train, y_train)

    model = _build_count_model_pipeline(model_key, X_train)
    if model is None:
        return None

    try:
        if not _fit_with_convergence_guard(model, X_train, y_train):
            return None
    except Exception:
        return None
    return {
        'key': model_key,
        'backend': 'sklearn',
        'model': model,
        'columns': list(X_train.columns),
    }


def _fit_negative_binomial_model_from_design(X_train: pd.DataFrame, y_train: np.ndarray) -> Optional[Dict[str, Any]]:
    if sm is None:
        return None

    alpha = _estimate_negative_binomial_alpha(y_train)
    try:
        prepared_X, scaled_columns, scaler = _prepare_statsmodels_count_design(X_train)
        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter('always')
            if ConvergenceWarning is not None:
                warnings.simplefilter('always', ConvergenceWarning)
            if StatsmodelsConvergenceWarning is not None:
                warnings.simplefilter('always', StatsmodelsConvergenceWarning)
            if PerfectSeparationWarning is not None:
                warnings.simplefilter('always', PerfectSeparationWarning)
            if HessianInversionWarning is not None:
                warnings.simplefilter('always', HessianInversionWarning)
            exog = sm.add_constant(prepared_X, has_constant='add')
            model = sm.GLM(y_train, exog, family=sm.families.NegativeBinomial(alpha=alpha))
            result = model.fit(maxiter=300, disp=0)
    except Exception:
        return None
    if _has_warning_instability(caught_warnings):
        return None
    if getattr(result, 'converged', True) is False:
        return None
    return {
        'key': 'negative_binomial',
        'backend': 'statsmodels',
        'model': result,
        'columns': list(X_train.columns),
        'alpha': alpha,
        'scaled_columns': scaled_columns,
        'scaler': scaler,
    }


def _predict_count_model(model_bundle: Dict[str, Any], frame: pd.DataFrame) -> np.ndarray:
    X = _build_design_matrix(frame, model_bundle['columns'])
    return _predict_count_from_design(model_bundle, X)


def _predict_count_from_design(model_bundle: Dict[str, Any], X: pd.DataFrame) -> np.ndarray:
    X = X.reindex(columns=model_bundle['columns'], fill_value=0.0)
    if model_bundle.get('backend') == 'statsmodels':
        scaled_columns = list(model_bundle.get('scaled_columns') or [])
        scaler = model_bundle.get('scaler')
        prepared_X = X.copy()
        if scaler is not None and scaled_columns:
            prepared_X.loc[:, scaled_columns] = scaler.transform(prepared_X[scaled_columns])
        exog = sm.add_constant(prepared_X, has_constant='add')
        predictions = np.asarray(model_bundle['model'].predict(exog), dtype=float)
    else:
        predictions = np.asarray(model_bundle['model'].predict(X), dtype=float)
    return np.clip(predictions, 0.0, None)


def _estimate_negative_binomial_alpha(counts: np.ndarray) -> float:
    values = np.asarray(counts, dtype=float)
    if values.size <= 1:
        return 0.25
    mean_value = max(float(np.mean(values)), MIN_POSITIVE_PREDICTION)
    variance = float(np.var(values, ddof=1))
    alpha = max((variance - mean_value) / max(mean_value ** 2, MIN_POSITIVE_PREDICTION), 1e-4)
    return min(alpha, 5.0)


def _can_train_event_model(event_series: pd.Series) -> bool:
    positives = int(event_series.sum())
    negatives = int(len(event_series) - positives)
    return positives >= MIN_EVENT_CLASS_COUNT and negatives >= MIN_EVENT_CLASS_COUNT


def _fit_event_model(frame: pd.DataFrame, feature_columns: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    X_train = _build_design_matrix(frame, feature_columns=feature_columns)
    y_train = frame['event'].to_numpy(dtype=int)
    return _fit_event_model_from_design(X_train, y_train)


def _fit_event_model_from_design(X_train: pd.DataFrame, y_train: np.ndarray) -> Optional[Dict[str, Any]]:
    if not _can_train_event_model(pd.Series(y_train)):
        return None
    model = LogisticRegression(**_LOGISTIC_PARAMS)
    try:
        model.fit(X_train, y_train)
    except Exception:
        return None
    return {
        'model': model,
        'columns': list(X_train.columns),
    }


def _predict_event_probability(model_bundle: Dict[str, Any], frame: pd.DataFrame) -> np.ndarray:
    X = _build_design_matrix(frame, model_bundle['columns'])
    return _predict_event_probability_from_design(model_bundle, X)


def _predict_event_probability_from_design(model_bundle: Dict[str, Any], X: pd.DataFrame) -> np.ndarray:
    X = X.reindex(columns=model_bundle['columns'], fill_value=0.0)
    probabilities = np.asarray(model_bundle['model'].predict_proba(X)[:, 1], dtype=float)
    return probabilities
