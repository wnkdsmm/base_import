from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import math
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from app.services.forecasting.data import _build_forecast_rows as _build_scenario_forecast_rows
from app.services.forecasting.utils import _format_number

from .forecast_bounds import (
    _bound_probability,
    _count_interval,
    _forecast_interval_coverage_metadata,
    _format_probability,
    _resolve_interval_calibration,
    _risk_band_from_index,
    _risk_index,
)
from ..ml_model_interval_types import PREDICTION_INTERVAL_LEVEL, PredictionIntervalCalibration
from .training_dataset import _build_design_row, _prepare_reference_frame
from .training_models import (
    _count_prediction_upper_cap_from_support,
    _predict_count_from_design,
    _predict_event_probability_from_design,
)
from .types import (
    IntervalCalibrationInput,
    TrainingForecastPathPoint,
    TrainingForecastRow,
    TrainingHistoryRecord,
    TrainingModelArtifact,
    TrainingTemperatureStats,
)


@dataclass
class _RecursiveForecastSeed:
    temperature_usable: bool
    monthly_temp: Dict[int, float]
    overall_temp: float
    history_counts: List[float]
    history_records: List[TrainingHistoryRecord]
    last_date: date


def _history_records_from_frame(frame: pd.DataFrame, temperature_usable: bool = True) -> List[TrainingHistoryRecord]:
    history_frame = frame.loc[:, ['date', 'count', 'avg_temperature']].copy()
    history_frame['date'] = pd.to_datetime(history_frame['date'])
    history_frame['count'] = pd.to_numeric(history_frame['count'], errors='coerce').fillna(0.0).astype(float)
    if temperature_usable:
        avg_temperature = pd.to_numeric(history_frame['avg_temperature'], errors='coerce')
        history_frame['avg_temperature'] = avg_temperature.where(avg_temperature.notna(), None)
    else:
        history_frame['avg_temperature'] = None
    return history_frame.to_dict(orient='records')


def _build_recursive_forecast_seed(
    frame: pd.DataFrame,
    temperature_stats: Optional[TrainingTemperatureStats] = None,
) -> _RecursiveForecastSeed:
    temperature_usable = bool((temperature_stats or {}).get('usable', True))
    monthly_temp = frame.groupby(frame['date'].dt.month)['temp_value'].mean().to_dict() if temperature_usable else {}
    overall_temp = float(frame['temp_value'].mean()) if temperature_usable and not frame.empty else 0.0
    return _RecursiveForecastSeed(
        temperature_usable=temperature_usable,
        monthly_temp={int(month): float(value) for month, value in monthly_temp.items()},
        overall_temp=overall_temp,
        history_counts=list(frame['count'].astype(float)),
        history_records=_history_records_from_frame(frame, temperature_usable=temperature_usable),
        last_date=frame['date'].dt.date.iloc[-1],
    )


def _future_feature_row(history_counts: List[float], target_date: date, temp_value: float) -> Dict[str, float]:
    def lag_value(offset: int) -> float:
        if len(history_counts) >= offset:
            return float(history_counts[-offset])
        return float(np.mean(history_counts)) if history_counts else 0.0

    rolling_7 = float(np.mean(history_counts[-7:])) if history_counts else 0.0
    rolling_28 = float(np.mean(history_counts[-28:])) if history_counts else rolling_7
    return {
        'temp_value': float(temp_value),
        'weekday': float(target_date.weekday()),
        'month': float(target_date.month),
        'lag_1': lag_value(1),
        'lag_7': lag_value(7),
        'lag_14': lag_value(14),
        'rolling_7': rolling_7,
        'rolling_28': rolling_28,
        'trend_gap': rolling_7 - rolling_28,
    }


def _predict_heuristic_future_step(
    history_records: List[TrainingHistoryRecord],
    target_date: date,
    temp_value: float,
    temperature_usable: bool,
    baseline_expected_count: Callable[[pd.DataFrame, pd.Timestamp], float],
    reference_train_factory: Optional[Callable[[], pd.DataFrame]] = None,
) -> Tuple[float, Optional[float]]:
    forecast_rows = _build_scenario_forecast_rows(history_records, 1, temp_value if temperature_usable else None)
    if forecast_rows:
        row = forecast_rows[0]
        probability = row.get('fire_probability')
        return (
            max(0.0, float(row.get('forecast_value', 0.0))),
            _bound_probability(probability if probability is not None else 0.0),
        )

    reference_train = (
        reference_train_factory()
        if reference_train_factory is not None
        else _prepare_reference_frame(pd.DataFrame(history_records))
    )
    fallback_count = float(baseline_expected_count(reference_train, pd.Timestamp(target_date)))
    return fallback_count, _bound_probability(1.0 - math.exp(-max(0.0, fallback_count)))


def _predict_future_count(
    selected_count_model_key: str,
    history_records: List[TrainingHistoryRecord],
    history_counts: List[float],
    target_date: date,
    temp_value: float,
    count_model: Optional[TrainingModelArtifact],
    temperature_usable: bool,
    baseline_expected_count: Callable[[pd.DataFrame, pd.Timestamp], float],
    feature_design_row: Optional[pd.DataFrame] = None,
    reference_train_factory: Optional[Callable[[], pd.DataFrame]] = None,
) -> float:
    if selected_count_model_key == 'seasonal_baseline':
        reference_train = (
            reference_train_factory()
            if reference_train_factory is not None
            else _prepare_reference_frame(pd.DataFrame(history_records))
        )
        return float(baseline_expected_count(reference_train, pd.Timestamp(target_date)))

    if selected_count_model_key == 'heuristic_forecast':
        prediction, _ = _predict_heuristic_future_step(
            history_records=history_records,
            target_date=target_date,
            temp_value=temp_value,
            temperature_usable=temperature_usable,
            baseline_expected_count=baseline_expected_count,
            reference_train_factory=reference_train_factory,
        )
        return prediction

    if count_model is None:
        return 0.0

    if feature_design_row is None:
        feature_row = _future_feature_row(history_counts, target_date, temp_value)
        feature_design_row = _build_design_row(feature_row, expected_columns=count_model['columns'])
    return float(_predict_count_from_design(count_model, feature_design_row)[0])


def _sanitize_recursive_count_prediction(prediction: float, history_counts: List[float]) -> float:
    finite_history = np.asarray([float(value) for value in history_counts if np.isfinite(value)], dtype=float)
    recent_support = float(np.max(finite_history[-28:])) if finite_history.size else 0.0
    upper_cap = float(_count_prediction_upper_cap_from_support(recent_support))
    bounded_prediction = min(max(0.0, float(prediction)), upper_cap)
    if not np.isfinite(bounded_prediction):
        return max(0.0, min(upper_cap, recent_support))
    return bounded_prediction


def _simulate_recursive_forecast_path(
    frame: pd.DataFrame,
    selected_count_model_key: str,
    count_model: Optional[TrainingModelArtifact],
    event_model: Optional[TrainingModelArtifact],
    forecast_days: int,
    scenario_temperature: Optional[float],
    baseline_expected_count: Callable[[pd.DataFrame, pd.Timestamp], float],
    temperature_stats: Optional[TrainingTemperatureStats] = None,
    baseline_event_probability: Optional[Callable[[pd.DataFrame, pd.Timestamp], Optional[float]]] = None,
    simulation_seed: Optional[_RecursiveForecastSeed] = None,
) -> List[TrainingForecastPathPoint]:
    seed = simulation_seed or _build_recursive_forecast_seed(frame, temperature_stats)
    temperature_usable = seed.temperature_usable
    monthly_temp = dict(seed.monthly_temp)
    overall_temp = seed.overall_temp
    history_counts = list(seed.history_counts)
    history_records = [dict(record) for record in seed.history_records]
    last_date = seed.last_date

    forecast_path: List[TrainingForecastPathPoint] = []
    for step in range(1, forecast_days + 1):
        target_date = last_date + timedelta(days=step)
        historical_temp_value = (
            float(monthly_temp.get(target_date.month, overall_temp))
            if temperature_usable and (monthly_temp or not frame.empty)
            else None
        )
        temp_value = scenario_temperature if temperature_usable and scenario_temperature is not None else historical_temp_value
        model_temp_value = float(temp_value) if temp_value is not None else 0.0
        feature_row = _future_feature_row(history_counts, target_date, model_temp_value)
        design_rows_by_columns: Dict[Tuple[str, ...], pd.DataFrame] = {}
        reference_train: Optional[pd.DataFrame] = None

        def _design_row_for(columns: Optional[List[str]]) -> pd.DataFrame:
            key = tuple(columns or [])
            design_row = design_rows_by_columns.get(key)
            if design_row is None:
                design_row = _build_design_row(feature_row, expected_columns=list(columns or []))
                design_rows_by_columns[key] = design_row
            return design_row

        def _reference_train() -> pd.DataFrame:
            nonlocal reference_train
            if reference_train is None:
                reference_train = _prepare_reference_frame(pd.DataFrame(history_records))
            return reference_train

        heuristic_probability = None
        if selected_count_model_key == 'heuristic_forecast':
            point_prediction, heuristic_probability = _predict_heuristic_future_step(
                history_records=history_records,
                target_date=target_date,
                temp_value=model_temp_value,
                temperature_usable=temperature_usable,
                baseline_expected_count=baseline_expected_count,
                reference_train_factory=_reference_train,
            )
        else:
            try:
                point_prediction = _predict_future_count(
                    selected_count_model_key=selected_count_model_key,
                    history_records=history_records,
                    history_counts=history_counts,
                    target_date=target_date,
                    temp_value=model_temp_value,
                    count_model=count_model,
                    temperature_usable=temperature_usable,
                    baseline_expected_count=baseline_expected_count,
                    feature_design_row=(
                        None
                        if count_model is None
                        else _design_row_for(list(count_model.get('columns') or []))
                    ),
                    reference_train_factory=_reference_train,
                )
            except Exception:
                point_prediction = float(baseline_expected_count(_reference_train(), pd.Timestamp(target_date)))

        point_prediction = _sanitize_recursive_count_prediction(point_prediction, history_counts)

        event_probability = None
        if event_model is not None:
            try:
                event_probability = float(
                    _predict_event_probability_from_design(
                        event_model,
                        _design_row_for(list(event_model.get('columns') or [])),
                    )[0]
                )
            except Exception:
                event_probability = None
        elif selected_count_model_key == 'heuristic_forecast':
            event_probability = heuristic_probability
        elif selected_count_model_key == 'seasonal_baseline' and baseline_event_probability is not None:
            event_probability = baseline_event_probability(_reference_train(), pd.Timestamp(target_date))

        forecast_path.append(
            {
                'step': step,
                'target_date': target_date,
                'temp_value': temp_value,
                'forecast_value': max(0.0, float(point_prediction)),
                'event_probability': _bound_probability(event_probability) if event_probability is not None else None,
            }
        )
        history_counts.append(point_prediction)
        history_records.append(
            {
                'date': pd.Timestamp(target_date),
                'count': point_prediction,
                'avg_temperature': temp_value if temperature_usable else None,
            }
        )

    return forecast_path


def _build_future_forecast_rows(
    frame: pd.DataFrame,
    selected_count_model_key: str,
    count_model: Optional[TrainingModelArtifact],
    event_model: Optional[TrainingModelArtifact],
    forecast_days: int,
    scenario_temperature: Optional[float],
    interval_calibration: IntervalCalibrationInput,
    baseline_expected_count: Callable[[pd.DataFrame, pd.Timestamp], float],
    temperature_stats: Optional[TrainingTemperatureStats] = None,
) -> List[TrainingForecastRow]:
    history_counts = list(frame['count'].astype(float))
    sorted_history_counts = np.sort(np.asarray(history_counts, dtype=float)) if history_counts else np.asarray([], dtype=float)
    forecast_path = _simulate_recursive_forecast_path(
        frame=frame,
        selected_count_model_key=selected_count_model_key,
        count_model=count_model,
        event_model=event_model,
        forecast_days=forecast_days,
        scenario_temperature=scenario_temperature,
        baseline_expected_count=baseline_expected_count,
        temperature_stats=temperature_stats,
    )
    reference_calibration = _resolve_interval_calibration(interval_calibration, 1)
    interval_calibrations_by_step = {
        step: _resolve_interval_calibration(interval_calibration, step)
        for step in range(1, forecast_days + 1)
    }

    interval_label = str(
        reference_calibration.get('level_display')
        or f'{int(round(float(reference_calibration.get("level", PREDICTION_INTERVAL_LEVEL)) * 100.0))}%'
    )
    forecast_rows: List[TrainingForecastRow] = []
    for point in forecast_path:
        step = int(point['step'])
        target_date = point['target_date']
        temp_value = point['temp_value']
        point_prediction = float(point['forecast_value'])
        event_probability = point['event_probability']
        row_interval_calibration = interval_calibrations_by_step[step]

        lower_bound, upper_bound = _count_interval(point_prediction, row_interval_calibration)
        risk_index = _risk_index(point_prediction, sorted_history_counts)
        risk_level_label, risk_level_tone = _risk_band_from_index(risk_index)

        forecast_rows.append(
            {
                'horizon_days': step,
                'date': target_date.isoformat(),
                'date_display': target_date.strftime('%d.%m.%Y'),
                'forecast_value': round(point_prediction, 3),
                'forecast_value_display': _format_number(point_prediction),
                'lower_bound': round(lower_bound, 3),
                'lower_bound_display': _format_number(lower_bound),
                'upper_bound': round(upper_bound, 3),
                'upper_bound_display': _format_number(upper_bound),
                'range_label': f'{interval_label} interval',
                'range_display': f"{interval_label}: {_format_number(lower_bound)} - {_format_number(upper_bound)} пожара",
                'temperature_display': f"{_format_number(temp_value)} В°C",
                'risk_index': round(risk_index, 1),
                'risk_index_display': f"{int(round(risk_index))} / 100",
                'risk_level_label': risk_level_label,
                'risk_level_tone': risk_level_tone,
                **_forecast_interval_coverage_metadata(row_interval_calibration),
                'event_probability': round(event_probability, 4) if event_probability is not None else None,
                'event_probability_display': _format_probability(event_probability) if event_probability is not None else '—',
            }
        )
    return forecast_rows
