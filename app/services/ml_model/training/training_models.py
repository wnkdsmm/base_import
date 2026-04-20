from __future__ import annotations

import logging
import warnings
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    from sklearn.linear_model import LogisticRegression, PoissonRegressor
except Exception:  # pragma: no cover - graceful fallback for older sklearn
    logger.debug("skipping PoissonRegressor import, fallback to LogisticRegression-only mode", exc_info=True)
    from sklearn.linear_model import LogisticRegression

    PoissonRegressor = None

try:
    from sklearn.compose import ColumnTransformer
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
except Exception:  # pragma: no cover - graceful fallback for older sklearn
    logger.debug("sklearn preprocessing pipeline imports unavailable, using plain models", exc_info=True)
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
    logger.debug("statsmodels imports unavailable, negative binomial backend disabled", exc_info=True)
    sm = None
    StatsmodelsConvergenceWarning = None
    HessianInversionWarning = None
    PerfectSeparationWarning = None

from ..ml_model_config_types import COUNT_MODEL_CONTINUOUS_COLUMNS, MIN_EVENT_CLASS_COUNT, NEGATIVE_BINOMIAL_MIN_TRAIN_ROWS, NEGATIVE_BINOMIAL_OVERDISPERSION_THRESHOLD, MIN_POSITIVE_PREDICTION, WARNING_INSTABILITY_MESSAGE_TOKENS, _LOGISTIC_PARAMS, _POISSON_PARAMS
from .training_dataset import _build_design_matrix


COUNT_PREDICTION_FALLBACK_COLUMNS = ('lag_1', 'rolling_7', 'lag_7', 'rolling_28', 'lag_14')
COUNT_PREDICTION_SUPPORT_COLUMNS = ('lag_1', 'lag_7', 'lag_14', 'rolling_7', 'rolling_28')


def _build_count_model(model_key: str):
    if model_key == 'poisson':
        if PoissonRegressor is None:
            return None
        return PoissonRegressor(**_POISSON_PARAMS)
    raise ValueError(f'Unsupported count model: {model_key}')


def _count_model_scaled_columns(columns: list[str]) -> list[str]:
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
) -> tuple[pd.DataFrame, list[str], Any | None]:
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


def _has_warning_instability(caught_warnings: list[warnings.WarningMessage]) -> bool:
    return any(_warning_indicates_unstable_fit(item) for item in caught_warnings)


def _fit_with_convergence_guard(model: Any, X_train: pd.DataFrame, y_train: np.ndarray) -> bool:
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter('always')
        if ConvergenceWarning is not None:
            warnings.simplefilter('always', ConvergenceWarning)
        model.fit(X_train, y_train)
    return not _has_warning_instability(caught_warnings)


def _fit_count_model(model_key: str, frame: pd.DataFrame, feature_columns: list[str | None] = None) -> dict[str, Any | None]:  # one-off
    X_train = _build_design_matrix(frame, feature_columns=feature_columns)
    y_train = frame['count'].to_numpy(dtype=float)
    return _fit_count_model_from_design(model_key, X_train, y_train)


def _fit_count_model_from_design(model_key: str, X_train: pd.DataFrame, y_train: np.ndarray) -> dict[str, Any | None]:  # one-off
    if model_key == 'negative_binomial':
        if not _can_train_negative_binomial(y_train):
            return None
        return _fit_negative_binomial_model_from_design(X_train, y_train)

    model = _build_count_model_pipeline(model_key, X_train)
    if model is None:
        return None

    try:
        if not _fit_with_convergence_guard(model, X_train, y_train):
            return None
    except Exception:
        logger.warning("count model fit failed, returning no fitted count model", exc_info=True)
        return None
    return {
        'key': model_key,
        'backend': 'sklearn',
        'model': model,
        'columns': list(X_train.columns),
    }


def _fit_negative_binomial_model_from_design(X_train: pd.DataFrame, y_train: np.ndarray) -> dict[str, Any | None]:  # one-off
    if sm is None:
        return None

    overdispersion_ratio = _estimate_count_overdispersion_ratio(y_train)
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
        logger.warning("negative binomial fit failed, returning no fitted model", exc_info=True)
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
        'overdispersion_ratio': overdispersion_ratio,
        'scaled_columns': scaled_columns,
        'scaler': scaler,
    }


def _predict_count_model(model_bundle: dict[str, Any], frame: pd.DataFrame) -> np.ndarray:  # one-off
    X = _build_design_matrix(frame, model_bundle['columns'])
    return _predict_count_from_design(model_bundle, X)


def _nonnegative_finite_column(X: pd.DataFrame, column_name: str, fill_value: float) -> np.ndarray:
    if column_name not in X.columns:
        return np.full(len(X), fill_value, dtype=float)
    column_values = np.asarray(X[column_name], dtype=float)
    return np.where(np.isfinite(column_values), np.clip(column_values, 0.0, None), fill_value)


def _count_prediction_fallbacks(X: pd.DataFrame) -> np.ndarray:
    fallback = np.full(len(X), np.nan, dtype=float)
    for column_name in COUNT_PREDICTION_FALLBACK_COLUMNS:
        column_values = _nonnegative_finite_column(X, column_name, np.nan)
        replace_mask = ~np.isfinite(fallback) & np.isfinite(column_values)
        fallback[replace_mask] = column_values[replace_mask]
    return np.where(np.isfinite(fallback), fallback, 0.0)


def _count_prediction_support(X: pd.DataFrame, fallback: np.ndarray) -> np.ndarray:
    support = np.zeros(len(X), dtype=float)
    for column_name in COUNT_PREDICTION_SUPPORT_COLUMNS:
        support = np.maximum(support, _nonnegative_finite_column(X, column_name, 0.0))
    return np.maximum(support, fallback)


def _count_prediction_upper_cap_from_support(support: np.ndarray | float) -> np.ndarray | float:
    # Keep a very generous cap so plausible burst days survive, while still cutting
    # obviously explosive numeric artifacts such as 1e300 after unstable exp().
    return np.maximum(250.0, support * 20.0 + 50.0)


def _count_prediction_upper_caps(X: pd.DataFrame, fallback: np.ndarray) -> np.ndarray:
    return _count_prediction_upper_cap_from_support(_count_prediction_support(X, fallback))


def _sanitize_count_predictions(predictions: np.ndarray, X: pd.DataFrame) -> np.ndarray:
    normalized = np.asarray(predictions, dtype=float).reshape(-1)
    fallback = _count_prediction_fallbacks(X)
    if normalized.size != len(X):
        normalized = np.full(len(X), np.nan, dtype=float)
    sanitized = np.where(np.isfinite(normalized), normalized, fallback)
    sanitized = np.clip(sanitized, 0.0, None)
    sanitized = np.minimum(sanitized, _count_prediction_upper_caps(X, fallback))
    return np.where(np.isfinite(sanitized), sanitized, fallback)


def _predict_count_from_design(model_bundle: dict[str, Any], X: pd.DataFrame) -> np.ndarray:  # one-off
    X = X.reindex(columns=model_bundle['columns'], fill_value=0.0)
    try:
        with warnings.catch_warnings(record=True):
            warnings.simplefilter('always')
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
    except Exception:
        logger.warning("count prediction failed, using NaN fallback predictions", exc_info=True)
        predictions = np.full(len(X), np.nan, dtype=float)
    return _sanitize_count_predictions(predictions, X)


def _estimate_negative_binomial_alpha(counts: np.ndarray) -> float:
    values = np.asarray(counts, dtype=float)
    if values.size <= 1:
        return 0.25
    mean_value = max(float(np.mean(values)), MIN_POSITIVE_PREDICTION)
    variance = float(np.var(values, ddof=1))
    alpha = max((variance - mean_value) / max(mean_value ** 2, MIN_POSITIVE_PREDICTION), 1e-4)
    return min(alpha, 5.0)


def _estimate_count_overdispersion_ratio(counts: np.ndarray) -> float:
    values = np.asarray(counts, dtype=float)
    if values.size == 0:
        return 1.0
    mean_value = max(float(np.mean(values)), MIN_POSITIVE_PREDICTION)
    variance = float(np.var(values, ddof=1)) if values.size > 1 else float(np.var(values))
    return max(variance / mean_value, 1.0)


def _can_train_negative_binomial(counts: np.ndarray) -> bool:
    values = np.asarray(counts, dtype=float)
    if sm is None or values.size < NEGATIVE_BINOMIAL_MIN_TRAIN_ROWS:
        return False
    return _estimate_count_overdispersion_ratio(values) >= NEGATIVE_BINOMIAL_OVERDISPERSION_THRESHOLD


def _can_train_event_model(event_series: pd.Series) -> bool:
    positives = int(event_series.sum())
    negatives = int(len(event_series) - positives)
    return positives >= MIN_EVENT_CLASS_COUNT and negatives >= MIN_EVENT_CLASS_COUNT


def _fit_event_model(frame: pd.DataFrame, feature_columns: list[str | None] = None) -> dict[str, Any | None]:  # one-off
    X_train = _build_design_matrix(frame, feature_columns=feature_columns)
    y_train = frame['event'].to_numpy(dtype=int)
    return _fit_event_model_from_design(X_train, y_train)


def _fit_event_model_from_design(X_train: pd.DataFrame, y_train: np.ndarray) -> dict[str, Any | None]:  # one-off
    if not _can_train_event_model(pd.Series(y_train)):
        return None
    model = LogisticRegression(**_LOGISTIC_PARAMS)
    try:
        model.fit(X_train, y_train)
    except Exception:
        logger.warning("event model fit failed, returning no fitted event model", exc_info=True)
        return None
    return {
        'model': model,
        'columns': list(X_train.columns),
    }


def _predict_event_probability(model_bundle: dict[str, Any], frame: pd.DataFrame) -> np.ndarray:  # one-off
    X = _build_design_matrix(frame, model_bundle['columns'])
    return _predict_event_probability_from_design(model_bundle, X)


def _predict_event_probability_from_design(model_bundle: dict[str, Any], X: pd.DataFrame) -> np.ndarray:  # one-off
    X = X.reindex(columns=model_bundle['columns'], fill_value=0.0)
    probabilities = np.asarray(model_bundle['model'].predict_proba(X)[:, 1], dtype=float)
    return probabilities
