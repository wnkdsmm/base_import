import warnings
import unittest
from datetime import date, timedelta
from unittest.mock import patch

import numpy as np
import pandas as pd
from sklearn.exceptions import ConvergenceWarning

from app.services.ml_model import training as ml_training
from app.services.model_quality import compute_classification_metrics
from app.services.ml_model.presentation import _build_notes, _build_quality_assessment, _build_summary
from app.services.ml_model.training import (
    _build_backtest_seed_dataset,
    _build_count_selection_details,
    _build_history_frame,
    _build_prediction_interval_calibration,
    _count_interval,
    _compute_event_metrics,
    _evaluate_prediction_interval_backtest,
    _fit_count_model_from_design,
    _interval_coverage,
    _predict_event_probability_from_design,
    _prediction_interval_margin,
    _run_backtest,
    _select_count_method,
    _select_count_model,
)


class CountModelSelectionTests(unittest.TestCase):
    def test_selects_heuristic_when_it_beats_ml_candidates(self) -> None:
        baseline_metrics = {
            'mae': 1.45,
            'rmse': 1.62,
            'smape': 23.0,
            'poisson_deviance': 0.98,
            'mae_delta_vs_baseline': None,
        }
        heuristic_metrics = {
            'mae': 1.01,
            'rmse': 1.18,
            'smape': 17.4,
            'poisson_deviance': 0.81,
            'mae_delta_vs_baseline': -0.303,
        }
        count_metrics = {
            'poisson': {
                'mae': 1.08,
                'rmse': 1.24,
                'smape': 18.1,
                'poisson_deviance': 0.86,
                'mae_delta_vs_baseline': -0.255,
            },
            'tweedie': {
                'mae': 1.05,
                'rmse': 1.22,
                'smape': 17.9,
                'poisson_deviance': 0.84,
                'mae_delta_vs_baseline': -0.276,
            },
        }

        selected_key, selected_metrics, context = _select_count_method(baseline_metrics, heuristic_metrics, count_metrics)

        self.assertEqual(selected_key, 'heuristic_forecast')
        self.assertAlmostEqual(selected_metrics['poisson_deviance'], 0.81)
        self.assertEqual(context['raw_best_key'], 'heuristic_forecast')

    def test_selects_baseline_when_it_beats_all_candidates(self) -> None:
        baseline_metrics = {
            'mae': 0.94,
            'rmse': 1.10,
            'smape': 15.6,
            'poisson_deviance': 0.73,
            'mae_delta_vs_baseline': None,
        }
        heuristic_metrics = {
            'mae': 1.02,
            'rmse': 1.18,
            'smape': 17.0,
            'poisson_deviance': 0.79,
            'mae_delta_vs_baseline': 0.085,
        }
        count_metrics = {
            'poisson': {
                'mae': 1.00,
                'rmse': 1.16,
                'smape': 16.8,
                'poisson_deviance': 0.77,
                'mae_delta_vs_baseline': 0.064,
            },
            'negative_binomial': {
                'mae': 1.03,
                'rmse': 1.17,
                'smape': 16.9,
                'poisson_deviance': 0.78,
                'mae_delta_vs_baseline': 0.096,
            },
        }

        selected_key, selected_metrics, context = _select_count_method(baseline_metrics, heuristic_metrics, count_metrics)

        self.assertEqual(selected_key, 'seasonal_baseline')
        self.assertAlmostEqual(selected_metrics['mae'], 0.94)
        self.assertEqual(context['raw_best_key'], 'seasonal_baseline')

    def test_prefers_heuristic_over_nearly_equal_ml_for_explainability(self) -> None:
        baseline_metrics = {
            'mae': 1.22,
            'rmse': 1.40,
            'smape': 21.0,
            'poisson_deviance': 0.95,
            'mae_delta_vs_baseline': None,
        }
        heuristic_metrics = {
            'mae': 1.03,
            'rmse': 1.20,
            'smape': 17.8,
            'poisson_deviance': 0.83,
            'mae_delta_vs_baseline': -0.156,
        }
        count_metrics = {
            'negative_binomial': {
                'mae': 1.00,
                'rmse': 1.18,
                'smape': 17.5,
                'poisson_deviance': 0.81,
                'mae_delta_vs_baseline': -0.180,
            },
            'poisson': {
                'mae': 1.02,
                'rmse': 1.19,
                'smape': 17.7,
                'poisson_deviance': 0.82,
                'mae_delta_vs_baseline': -0.164,
            },
        }

        selected_key, selected_metrics, context = _select_count_method(baseline_metrics, heuristic_metrics, count_metrics)

        self.assertEqual(selected_key, 'heuristic_forecast')
        self.assertAlmostEqual(selected_metrics['mae'], 1.03)
        self.assertEqual(context['raw_best_key'], 'negative_binomial')
        self.assertEqual(context['tie_break_reason'], 'heuristic_over_ml')

    def test_documents_heuristic_explainability_tie_break(self) -> None:
        baseline_metrics = {
            'mae': 1.22,
            'rmse': 1.40,
            'smape': 21.0,
            'poisson_deviance': 0.95,
            'mae_delta_vs_baseline': None,
        }
        heuristic_metrics = {
            'mae': 1.03,
            'rmse': 1.20,
            'smape': 17.8,
            'poisson_deviance': 0.83,
            'mae_delta_vs_baseline': -0.156,
        }
        count_metrics = {
            'negative_binomial': {
                'mae': 1.00,
                'rmse': 1.18,
                'smape': 17.5,
                'poisson_deviance': 0.81,
                'mae_delta_vs_baseline': -0.180,
            },
            'poisson': {
                'mae': 1.02,
                'rmse': 1.19,
                'smape': 17.7,
                'poisson_deviance': 0.82,
                'mae_delta_vs_baseline': -0.164,
            },
        }

        selected_key, selected_metrics, context = _select_count_method(baseline_metrics, heuristic_metrics, count_metrics)
        details = _build_count_selection_details(
            selected_count_model_key=selected_key,
            selected_metrics=selected_metrics,
            count_metrics=count_metrics,
            baseline_metrics=baseline_metrics,
            heuristic_metrics=heuristic_metrics,
            overdispersion_ratio=1.25,
            raw_best_key=context['raw_best_key'],
            tie_break_reason=context['tie_break_reason'],
        )

        self.assertEqual(selected_key, 'heuristic_forecast')
        self.assertIn('tie-break', details['short'].lower())
        self.assertIn('tie-break', details['long'].lower())
        self.assertIn('negative binomial', details['long'].lower())

    def test_selection_details_do_not_emit_legacy_selection_text(self) -> None:
        baseline_metrics = {
            'mae': 1.42,
            'rmse': 1.61,
            'smape': 23.5,
            'poisson_deviance': 1.01,
            'mae_delta_vs_baseline': None,
        }
        heuristic_metrics = {
            'mae': 1.16,
            'rmse': 1.32,
            'smape': 19.4,
            'poisson_deviance': 0.90,
            'mae_delta_vs_baseline': -0.183,
        }
        count_metrics = {
            'poisson': {
                'mae': 1.05,
                'rmse': 1.20,
                'smape': 18.0,
                'poisson_deviance': 0.84,
                'mae_delta_vs_baseline': -0.261,
            },
            'tweedie': {
                'mae': 1.02,
                'rmse': 1.17,
                'smape': 17.8,
                'poisson_deviance': 0.82,
                'mae_delta_vs_baseline': -0.282,
            },
        }

        selected_key, selected_metrics, context = _select_count_method(baseline_metrics, heuristic_metrics, count_metrics)
        details = _build_count_selection_details(
            selected_count_model_key=selected_key,
            selected_metrics=selected_metrics,
            count_metrics=count_metrics,
            baseline_metrics=baseline_metrics,
            heuristic_metrics=heuristic_metrics,
            overdispersion_ratio=1.20,
            raw_best_key=context['raw_best_key'],
            tie_break_reason=context['tie_break_reason'],
        )

        combined = f"{details['short']} {details['long']}"
        self.assertIn('рабочим count-методом', combined)
        self.assertNotIn('основной count-модель', combined)
        self.assertNotIn('магистерской работы', combined)

    def test_prefers_poisson_when_quality_is_within_tolerance(self) -> None:
        count_metrics = {
            'poisson': {
                'mae': 1.05,
                'rmse': 1.20,
                'smape': 18.0,
                'poisson_deviance': 0.84,
            },
            'tweedie': {
                'mae': 1.02,
                'rmse': 1.17,
                'smape': 17.8,
                'poisson_deviance': 0.82,
            },
        }

        selected_key, selected_metrics = _select_count_model(count_metrics)

        self.assertEqual(selected_key, 'poisson')
        self.assertAlmostEqual(selected_metrics['poisson_deviance'], 0.84)

    def test_selects_poisson_as_working_method_when_ml_quality_is_within_tolerance(self) -> None:
        baseline_metrics = {
            'mae': 1.42,
            'rmse': 1.61,
            'smape': 23.5,
            'poisson_deviance': 1.01,
            'mae_delta_vs_baseline': None,
        }
        heuristic_metrics = {
            'mae': 1.16,
            'rmse': 1.32,
            'smape': 19.4,
            'poisson_deviance': 0.90,
            'mae_delta_vs_baseline': -0.183,
        }
        count_metrics = {
            'poisson': {
                'mae': 1.05,
                'rmse': 1.20,
                'smape': 18.0,
                'poisson_deviance': 0.84,
                'mae_delta_vs_baseline': -0.261,
            },
            'tweedie': {
                'mae': 1.02,
                'rmse': 1.17,
                'smape': 17.8,
                'poisson_deviance': 0.82,
                'mae_delta_vs_baseline': -0.282,
            },
        }

        selected_key, selected_metrics, context = _select_count_method(baseline_metrics, heuristic_metrics, count_metrics)

        self.assertEqual(selected_key, 'poisson')
        self.assertAlmostEqual(selected_metrics['poisson_deviance'], 0.84)
        self.assertEqual(context['raw_best_key'], 'tweedie')
        self.assertEqual(context['tie_break_reason'], 'poisson_over_ml')


class CountModelConvergenceTests(unittest.TestCase):
    class _WarningEstimator:
        def fit(self, X: pd.DataFrame, y: np.ndarray) -> 'CountModelConvergenceTests._WarningEstimator':
            warnings.warn('count model did not converge', ConvergenceWarning)
            return self

        def predict(self, X: pd.DataFrame) -> np.ndarray:
            return np.zeros(len(X), dtype=float)

    class _ColumnPredictor:
        def __init__(self, column_name: str, weight: float) -> None:
            self._column_name = column_name
            self._weight = weight

        def predict(self, X: pd.DataFrame) -> np.ndarray:
            values = np.asarray(X[self._column_name], dtype=float)
            return np.clip(values * self._weight, 0.0, None)

    class _StatsmodelsWarningFit:
        def fit(self, *args, **kwargs):
            warning_category = getattr(ml_training, 'PerfectSeparationWarning', None) or UserWarning
            warnings.warn(
                'Perfect separation or prediction detected, parameter may not be identified',
                warning_category,
            )

            class _Result:
                converged = True

            return _Result()

    @staticmethod
    def _build_daily_history() -> list[dict[str, object]]:
        history: list[dict[str, object]] = []
        current_date = date(2024, 1, 1)
        for index in range(96):
            history.append(
                {
                    'date': current_date,
                    'count': float((index % 6 == 0) + (index % 10 == 0) + (index // 32)),
                    'avg_temperature': float(-18.0 + (index % 24)),
                }
            )
            current_date += timedelta(days=1)
        return history

    def test_fit_count_model_marks_convergence_warning_as_unavailable(self) -> None:
        X_train = pd.DataFrame(
            [
                {'lag_1': 0.0, 'lag_7': 0.0, 'lag_14': 0.0, 'rolling_7': 0.0, 'rolling_28': 0.0, 'trend_gap': 0.0},
                {'lag_1': 1.0, 'lag_7': 0.0, 'lag_14': 0.0, 'rolling_7': 0.5, 'rolling_28': 0.2, 'trend_gap': 0.3},
                {'lag_1': 2.0, 'lag_7': 1.0, 'lag_14': 0.0, 'rolling_7': 0.8, 'rolling_28': 0.4, 'trend_gap': 0.4},
            ]
        )
        y_train = np.asarray([0.0, 1.0, 3.0], dtype=float)

        with patch.object(ml_training, '_build_count_model_pipeline', return_value=self._WarningEstimator()):
            result = _fit_count_model_from_design('poisson', X_train, y_train)

        self.assertIsNone(result)

    def test_fit_negative_binomial_marks_warning_instability_as_unavailable(self) -> None:
        if getattr(ml_training, 'sm', None) is None:
            self.skipTest('statsmodels is unavailable')

        X_train = pd.DataFrame(
            [
                {'lag_1': 0.0, 'lag_7': 0.0, 'lag_14': 0.0, 'rolling_7': 0.0, 'rolling_28': 0.0, 'trend_gap': 0.0},
                {'lag_1': 1.0, 'lag_7': 0.0, 'lag_14': 0.0, 'rolling_7': 0.5, 'rolling_28': 0.2, 'trend_gap': 0.3},
                {'lag_1': 2.0, 'lag_7': 1.0, 'lag_14': 0.0, 'rolling_7': 0.8, 'rolling_28': 0.4, 'trend_gap': 0.4},
            ]
        )
        y_train = np.asarray([0.0, 1.0, 3.0], dtype=float)

        with patch.object(ml_training.sm, 'GLM', return_value=self._StatsmodelsWarningFit()):
            result = ml_training._fit_negative_binomial_model_from_design(X_train, y_train)

        self.assertIsNone(result)

    def test_non_converged_candidate_is_excluded_from_selection_and_comparison(self) -> None:
        history_frame = _build_history_frame(self._build_daily_history())
        dataset = _build_backtest_seed_dataset(history_frame)

        def _fake_fit_count_model_from_design(model_key: str, X_train: pd.DataFrame, y_train: np.ndarray):
            if model_key == 'poisson':
                return None
            if model_key == 'negative_binomial':
                predictor = self._ColumnPredictor('lag_1', 0.85)
            else:
                predictor = self._ColumnPredictor('rolling_7', 0.55)
            return {
                'key': model_key,
                'backend': 'sklearn',
                'model': predictor,
                'columns': list(X_train.columns),
            }

        with (
            patch.object(ml_training, '_fit_count_model_from_design', side_effect=_fake_fit_count_model_from_design),
            patch.object(ml_training, '_fit_event_model_from_design', return_value=None),
        ):
            result = _run_backtest(history_frame, dataset)

        self.assertTrue(result['is_ready'])
        self.assertNotEqual(result['selected_count_model_key'], 'poisson')
        self.assertNotIn('poisson', result['count_metrics'])
        self.assertFalse(any(row['method_key'] == 'poisson' for row in result['count_comparison_rows']))
        self.assertNotIn(
            ml_training.COUNT_MODEL_LABELS['poisson'],
            result['backtest_overview']['candidate_model_labels'],
        )

    def test_warning_unstable_negative_binomial_is_excluded_from_selection_and_comparison(self) -> None:
        if getattr(ml_training, 'sm', None) is None:
            self.skipTest('statsmodels is unavailable')

        history_frame = _build_history_frame(self._build_daily_history())
        dataset = _build_backtest_seed_dataset(history_frame)

        with (
            patch.object(ml_training.sm, 'GLM', return_value=self._StatsmodelsWarningFit()),
            patch.object(ml_training, '_fit_event_model_from_design', return_value=None),
        ):
            result = _run_backtest(history_frame, dataset)

        self.assertTrue(result['is_ready'])
        self.assertNotEqual(result['selected_count_model_key'], 'negative_binomial')
        self.assertNotIn('negative_binomial', result['count_metrics'])
        self.assertFalse(any(row['method_key'] == 'negative_binomial' for row in result['count_comparison_rows']))
        self.assertNotIn(
            ml_training.COUNT_MODEL_LABELS['negative_binomial'],
            result['backtest_overview']['candidate_model_labels'],
        )


class EventSelectionTests(unittest.TestCase):
    def test_keeps_heuristic_probability_when_classifier_is_worse(self) -> None:
        rows = []
        for index in range(10):
            actual_event = 1 if index % 2 == 0 else 0
            rows.append(
                {
                    'actual_event': actual_event,
                    'baseline_event_probability': 0.50,
                    'heuristic_event_probability': 0.90 if actual_event else 0.10,
                    'predicted_event_probability': 0.55 if actual_event else 0.45,
                }
            )

        metrics = _compute_event_metrics(rows)

        self.assertTrue(metrics['available'])
        self.assertTrue(metrics['logistic_available'])
        self.assertEqual(metrics['selected_model_key'], 'heuristic_probability')
        self.assertTrue(any(row['method_key'] == 'logistic_regression' for row in metrics['comparison_rows']))

    def test_marks_all_positive_backtest_as_invalid_probability_validation(self) -> None:
        rows = [
            {
                'actual_event': 1,
                'baseline_event_probability': 0.92,
                'heuristic_event_probability': 0.98,
                'predicted_event_probability': 1.0,
            }
            for _ in range(10)
        ]

        metrics = _compute_event_metrics(rows)

        self.assertFalse(metrics['available'])
        self.assertFalse(metrics['logistic_available'])
        self.assertIsNone(metrics['selected_model_key'])
        self.assertIsNone(metrics['selected_model_label'])
        self.assertFalse(metrics['event_probability_informative'])
        self.assertEqual(metrics['rows_used'], 10)
        self.assertEqual(metrics['comparison_rows'], [])
        self.assertEqual(metrics['event_probability_reason_code'], 'single_class_evaluation')
        self.assertIn('одному классу', str(metrics['event_probability_note']))
        self.assertIn('только дни с пожаром', str(metrics['event_probability_note']))


class EventProbabilityExplanationTests(unittest.TestCase):
    @staticmethod
    def _daily_history() -> list[dict[str, object]]:
        return [
            {
                'date': date(2024, 1, 1) + timedelta(days=index),
                'count': 1.0,
                'avg_temperature': 10.0,
            }
            for index in range(45)
        ]

    def test_all_positive_backtest_uses_single_class_explanation_in_payload(self) -> None:
        rows = [
            {
                'actual_event': 1,
                'baseline_event_probability': 0.92,
                'heuristic_event_probability': 0.98,
                'predicted_event_probability': 1.0,
            }
            for _ in range(45)
        ]
        metrics = _compute_event_metrics(rows)
        ml_result = {
            'is_ready': True,
            'forecast_rows': [],
            'count_comparison_rows': [],
            'event_comparison_rows': metrics['comparison_rows'],
            'selected_event_model_label': metrics['selected_model_label'],
            'event_backtest_available': metrics['available'],
            'event_probability_enabled': False,
            'event_probability_note': metrics['event_probability_note'],
            'event_probability_reason_code': metrics['event_probability_reason_code'],
            'backtest_overview': {
                'folds': len(rows),
                'min_train_rows': 30,
                'candidate_model_labels': [],
                'event_probability_note': metrics['event_probability_note'],
                'event_probability_reason_code': metrics['event_probability_reason_code'],
            },
        }
        daily_history = self._daily_history()

        summary = _build_summary(
            selected_table='all',
            selected_cause='all',
            selected_object_category='all',
            daily_history=daily_history,
            filtered_records_count=len(daily_history),
            ml_result=ml_result,
            history_window='all',
            scenario_temperature=None,
        )
        quality = _build_quality_assessment(ml_result)
        notes = _build_notes(
            preload_notes=[],
            metadata_items=[{'resolved_columns': {'temperature': 'avg_temperature'}}],
            filtered_records_count=len(daily_history),
            daily_history=daily_history,
            ml_result=ml_result,
            scenario_temperature=None,
            source_tables=['fires'],
        )

        self.assertEqual(summary['event_probability_reason_code'], 'single_class_evaluation')
        self.assertIn('одному классу', str(summary['event_probability_note']))
        self.assertEqual(quality['event_probability_reason_code'], 'single_class_evaluation')
        self.assertEqual(quality['event_table']['reason_code'], 'single_class_evaluation')
        self.assertIn('одному классу', str(quality['event_table']['empty_message']))
        self.assertIn('только дни с пожаром', str(quality['event_table']['empty_message']))
        self.assertNotIn('недостаточно окон', str(quality['event_table']['empty_message']).lower())
        self.assertTrue(
            any('одному классу' in str(point) for point in quality['dissertation_points']),
            msg=quality['dissertation_points'],
        )
        self.assertTrue(
            any('одному классу' in str(note) for note in notes),
            msg=notes,
        )


class EventPayloadLabelTests(unittest.TestCase):
    def test_event_payload_uses_unicode_labels_without_mojibake(self) -> None:
        rows = []
        for index in range(10):
            actual_event = 1 if index % 2 == 0 else 0
            rows.append(
                {
                    'actual_event': actual_event,
                    'baseline_event_probability': 0.55 if actual_event else 0.45,
                    'heuristic_event_probability': 0.90 if actual_event else 0.10,
                    'predicted_event_probability': 0.55 if actual_event else 0.45,
                }
            )

        metrics = _compute_event_metrics(rows)
        ml_result = {
            'is_ready': True,
            'forecast_rows': [],
            'count_comparison_rows': [],
            'event_comparison_rows': metrics['comparison_rows'],
            'selected_event_model_label': metrics['selected_model_label'],
            'event_backtest_available': metrics['available'],
            'backtest_overview': {},
        }
        daily_history = [
            {
                'date': date(2024, 1, 1) + timedelta(days=index),
                'count': float(index % 3),
            }
            for index in range(14)
        ]

        summary = _build_summary(
            selected_table='all',
            selected_cause='all',
            selected_object_category='all',
            daily_history=daily_history,
            filtered_records_count=14,
            ml_result=ml_result,
            history_window='all',
            scenario_temperature=None,
        )
        quality = _build_quality_assessment(ml_result)

        self.assertEqual(metrics['selected_model_label'], 'Сценарная эвристическая вероятность')
        self.assertEqual(summary['event_backtest_model_label'], 'Сценарная эвристическая вероятность')

        event_rows = quality['event_table']['rows']
        self.assertEqual(
            [row['method_label'] for row in event_rows],
            [
                'Сезонная событийная базовая модель',
                'Сценарная эвристическая вероятность',
                'Логистическая регрессия',
            ],
        )
        self.assertEqual(
            [row['role_label'] for row in event_rows],
            [
                'Базовая модель',
                'Сценарный прогноз',
                'Классификатор',
            ],
        )

        text_blob = ' | '.join(
            [summary['event_backtest_model_label']]
            + [row['method_label'] for row in event_rows]
            + [row['role_label'] for row in event_rows]
        )
        self.assertNotIn('РЎ', text_blob)
        self.assertNotIn('Р ', text_blob)


class ClassificationMetricsTests(unittest.TestCase):
    def test_marks_single_class_probability_validation_as_unavailable(self) -> None:
        metrics = compute_classification_metrics(
            actuals=[1] * 10,
            probabilities=[1.0] * 10,
            baseline_probabilities=[0.85] * 10,
        )

        self.assertFalse(metrics['available'])
        self.assertIsNone(metrics['brier_score'])
        self.assertIsNone(metrics['roc_auc'])
        self.assertIsNone(metrics['f1'])
        self.assertIsNone(metrics['baseline_brier_score'])
        self.assertIsNone(metrics['baseline_f1'])


class ProbabilityPayloadTests(unittest.TestCase):
    class _FixedProbabilityModel:
        def __init__(self, probabilities: list[float]) -> None:
            self._probabilities = np.asarray(probabilities, dtype=float)

        def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
            probabilities = self._probabilities[: len(X)]
            return np.column_stack([1.0 - probabilities, probabilities])

    def test_payload_keeps_saturated_probabilities_unclipped(self) -> None:
        model_bundle = {
            'model': self._FixedProbabilityModel([1.0, 0.0]),
            'columns': ['lag_1'],
        }
        design = pd.DataFrame([{'lag_1': 1.0}, {'lag_1': 0.0}])

        probabilities = _predict_event_probability_from_design(model_bundle, design)
        metrics = compute_classification_metrics(
            actuals=[1, 0],
            probabilities=probabilities,
            baseline_probabilities=[0.6, 0.4],
        )

        self.assertEqual(probabilities.tolist(), [1.0, 0.0])
        self.assertTrue(metrics['available'])
        self.assertLess(float(metrics['brier_score']), 0.001)


class PredictionIntervalCalibrationTests(unittest.TestCase):
    def test_conformal_interval_is_non_negative_and_monotonic(self) -> None:
        calibration = _build_prediction_interval_calibration(
            np.asarray([0.0, 2.0, 4.0, 6.0], dtype=float),
            np.asarray([0.2, 1.4, 4.8, 5.5], dtype=float),
            level=0.8,
        )

        lower_bound, upper_bound = _count_interval(0.3, calibration)

        self.assertEqual(calibration['level_display'], '80%')
        self.assertGreaterEqual(lower_bound, 0.0)
        self.assertLessEqual(lower_bound, upper_bound)

    def test_adaptive_interval_uses_narrower_margin_for_low_count_bin_and_wider_for_peak_bin(self) -> None:
        calibration = _build_prediction_interval_calibration(
            np.asarray([0.2, 1.0, 1.2, 8.0, 14.0, 20.0], dtype=float),
            np.asarray([0.3, 0.8, 1.1, 7.0, 11.0, 15.0], dtype=float),
            level=0.8,
        )

        low_margin = _prediction_interval_margin(0.8, calibration)
        peak_margin = _prediction_interval_margin(15.0, calibration)
        low_lower, low_upper = _count_interval(0.8, calibration)
        peak_lower, peak_upper = _count_interval(15.0, calibration)

        self.assertEqual(calibration['adaptive_binning_strategy'], 'predicted_count_quantiles')
        self.assertGreaterEqual(int(calibration['adaptive_bin_count']), 2)
        self.assertLess(float(low_margin), float(peak_margin))
        self.assertLess(float(low_upper - low_lower), float(peak_upper - peak_lower))

    def test_conformal_interval_coverage_is_measured_only_on_later_windows(self) -> None:
        actuals = np.asarray([5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 12.0, 12.0, 12.0, 12.0], dtype=float)
        predictions = np.asarray([5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0], dtype=float)
        window_dates = [date(2024, 1, 1) + timedelta(days=index) for index in range(len(actuals))]

        result = _evaluate_prediction_interval_backtest(actuals, predictions, window_dates, level=0.8)

        self.assertTrue(result['coverage_validated'])
        self.assertEqual(result['calibration_window_count'], 6)
        self.assertEqual(result['evaluation_window_count'], 4)
        self.assertEqual(result['validation_scheme_key'], 'rolling_split_conformal')
        self.assertEqual(result['calibration']['residual_count'], len(actuals))
        self.assertAlmostEqual(float(result['coverage']), 0.75)
        self.assertAlmostEqual(float(result['reference_candidate']['coverage']), 0.0)
        self.assertAlmostEqual(float(result['runner_up_candidate']['coverage']), 0.5)
        self.assertIn('out-of-sample coverage', result['coverage_note'])
        self.assertIn('Forward rolling split conformal', result['coverage_note'])

    def test_forward_rolling_backtest_keeps_adaptive_low_and_peak_margins(self) -> None:
        actuals = np.asarray([0.1, 0.4, 1.2, 1.5, 10.0, 18.0, 0.5, 1.0, 13.0, 21.0], dtype=float)
        predictions = np.asarray([0.2, 0.5, 1.0, 1.2, 7.0, 14.0, 0.4, 1.1, 11.0, 15.0], dtype=float)
        window_dates = [date(2024, 3, 1) + timedelta(days=index) for index in range(len(actuals))]

        result = _evaluate_prediction_interval_backtest(actuals, predictions, window_dates, level=0.8)
        calibration = result['calibration']

        self.assertTrue(result['coverage_validated'])
        self.assertEqual(result['calibration_window_count'], 6)
        self.assertEqual(result['evaluation_window_count'], 4)
        self.assertEqual(result['validation_scheme_key'], 'rolling_split_conformal')
        self.assertGreater(
            _prediction_interval_margin(15.0, calibration),
            _prediction_interval_margin(0.5, calibration),
        )

    def test_conformal_interval_hides_validated_coverage_when_windows_are_too_few(self) -> None:
        actuals = np.asarray([2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 10.0, 11.0, 12.0], dtype=float)
        predictions = np.asarray([2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 8.0, 8.0], dtype=float)
        window_dates = [date(2024, 2, 1) + timedelta(days=index) for index in range(len(actuals))]

        result = _evaluate_prediction_interval_backtest(actuals, predictions, window_dates, level=0.8)

        self.assertFalse(result['coverage_validated'])
        self.assertIsNone(result['coverage'])
        self.assertEqual(result['evaluation_window_count'], 0)
        self.assertIn('out-of-sample coverage', result['coverage_note'])
        self.assertIn('too few rolling-origin windows', result['coverage_note'])
        self.assertIn('forward-only interval validation', result['coverage_note'])

    def test_regime_shift_benchmark_prefers_forward_rolling_split_over_fixed_chrono_baseline(self) -> None:
        actuals = np.asarray(
            [2.0, 2.0, 3.0, 2.0, 3.0, 2.0, 9.0, 10.0, 11.0, 10.0, 12.0, 11.0],
            dtype=float,
        )
        predictions = np.asarray(
            [2.0, 2.0, 3.0, 2.0, 3.0, 2.0, 4.0, 4.5, 5.0, 5.0, 5.5, 5.5],
            dtype=float,
        )
        window_dates = [date(2024, 5, 1) + timedelta(days=index) for index in range(len(actuals))]

        result = _evaluate_prediction_interval_backtest(actuals, predictions, window_dates, level=0.8)
        reference_candidate = result['reference_candidate']
        runner_up_candidate = result['runner_up_candidate']

        self.assertTrue(result['coverage_validated'])
        self.assertEqual(result['validation_scheme_key'], 'rolling_split_conformal')
        self.assertIsNotNone(reference_candidate)
        self.assertIsNotNone(runner_up_candidate)
        self.assertLess(
            float(result['coverage']),
            1.0,
        )
        self.assertLess(
            float(result['runner_up_candidate']['stability_score']),
            float(reference_candidate['stability_score']),
        )
        self.assertLess(
            float(result['runner_up_candidate']['coverage_gap']),
            float(reference_candidate['coverage_gap']),
        )
        self.assertIn('Jackknife+ for time series was not adopted', result['coverage_note'])

    def test_synthetic_series_with_quiet_periods_shift_and_spikes_keeps_validated_coverage_out_of_sample(self) -> None:
        actuals = np.asarray(
            [
                0.0, 0.0, 1.0, 0.0, 1.0, 0.0,
                0.0, 1.0, 0.0, 1.0,
                6.0, 7.0, 12.0, 8.0, 13.0, 9.0, 14.0, 10.0,
            ],
            dtype=float,
        )
        predictions = np.asarray(
            [
                0.0, 0.0, 1.0, 0.0, 1.0, 0.0,
                0.0, 1.0, 0.0, 1.0,
                3.0, 3.0, 4.0, 4.0, 5.0, 5.0, 5.5, 5.5,
            ],
            dtype=float,
        )
        window_dates = [date(2024, 7, 1) + timedelta(days=index) for index in range(len(actuals))]

        result = _evaluate_prediction_interval_backtest(actuals, predictions, window_dates, level=0.8)
        pooled_coverage = _interval_coverage(actuals, predictions, result['calibration'])

        self.assertTrue(result['coverage_validated'])
        self.assertEqual(result['validation_scheme_key'], 'rolling_split_conformal')
        self.assertEqual(result['calibration_window_count'] + result['evaluation_window_count'], len(actuals))
        self.assertIsNotNone(pooled_coverage)
        self.assertNotAlmostEqual(float(result['coverage']), float(pooled_coverage), places=6)
        self.assertIn('Coverage is evaluated only on', result['coverage_note'])

    def test_adaptive_bins_keep_non_negative_ordered_bounds_across_quiet_and_peak_predictions(self) -> None:
        actuals = np.asarray(
            [0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 2.0, 3.0, 9.0, 12.0, 15.0, 18.0],
            dtype=float,
        )
        predictions = np.asarray(
            [0.0, 0.2, 0.8, 0.1, 0.9, 0.3, 1.5, 2.2, 6.0, 9.0, 11.0, 13.5],
            dtype=float,
        )
        window_dates = [date(2024, 8, 1) + timedelta(days=index) for index in range(len(actuals))]

        result = _evaluate_prediction_interval_backtest(actuals, predictions, window_dates, level=0.8)
        calibration = result['calibration']

        for point_prediction in [0.0, 0.2, 0.8, 2.0, 6.0, 10.0, 15.0]:
            lower_bound, upper_bound = _count_interval(point_prediction, calibration)
            self.assertGreaterEqual(lower_bound, 0.0)
            self.assertLessEqual(lower_bound, upper_bound)

    def test_small_synthetic_series_marks_validated_coverage_as_unavailable(self) -> None:
        actuals = np.asarray([0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 4.0, 5.0, 6.0], dtype=float)
        predictions = np.asarray([0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 2.0, 2.0, 2.0], dtype=float)
        window_dates = [date(2024, 9, 1) + timedelta(days=index) for index in range(len(actuals))]

        result = _evaluate_prediction_interval_backtest(actuals, predictions, window_dates, level=0.8)

        self.assertFalse(result['coverage_validated'])
        self.assertIsNone(result['coverage'])
        self.assertEqual(result['validation_scheme_key'], 'not_validated')
        self.assertIn('Validated out-of-sample coverage is unavailable', result['coverage_note'])


class PredictionIntervalBacktestIntegrationTests(unittest.TestCase):
    @staticmethod
    def _build_daily_history_with_quiet_periods_shift_and_spikes() -> list[dict[str, object]]:
        history = []
        current_date = date(2024, 1, 1)
        for index in range(96):
            if index < 42:
                count = float(1 if index % 9 == 0 else 0)
            elif index < 70:
                count = float(2 + (index % 3 == 0) + (5 if index % 11 == 0 else 0))
            else:
                count = float(5 + (index % 4) + (7 if index % 10 == 0 else 0))

            history.append(
                {
                    'date': current_date,
                    'count': count,
                    'avg_temperature': float(-8.0 + (index % 18)),
                }
            )
            current_date += timedelta(days=1)
        return history

    def test_run_backtest_on_synthetic_history_reports_validated_coverage_without_in_sample_fallback(self) -> None:
        history_frame = _build_history_frame(self._build_daily_history_with_quiet_periods_shift_and_spikes())
        dataset = _build_backtest_seed_dataset(history_frame)
        result = _run_backtest(history_frame, dataset)

        self.assertTrue(result['is_ready'], msg=result.get('message'))
        overview = result['backtest_overview']

        self.assertTrue(overview['prediction_interval_coverage_validated'])
        self.assertEqual(overview['prediction_interval_validation_scheme_key'], 'rolling_split_conformal')
        self.assertGreater(int(overview['prediction_interval_evaluation_windows']), 0)
        self.assertIn('Coverage is evaluated only on', overview['prediction_interval_coverage_note'])
        self.assertIn('Jackknife+ for time series', overview['prediction_interval_validation_explanation'])
        for row in result['rows']:
            self.assertGreaterEqual(float(row['lower_bound']), 0.0)
            self.assertLessEqual(float(row['lower_bound']), float(row['upper_bound']))


class TemperatureBacktestLeakageTests(unittest.TestCase):
    @staticmethod
    def _build_daily_history() -> list[dict[str, object]]:
        history = []
        current_date = date(2024, 1, 1)
        end_date = date(2024, 3, 15)
        while current_date <= end_date:
            if current_date.month == 1:
                avg_temperature = float((current_date.day % 10) - 5)
            elif current_date.month == 2 and current_date.day < 29:
                avg_temperature = None
            elif current_date.month == 2:
                avg_temperature = 4.0
            else:
                avg_temperature = float(8 + (current_date.day % 6))

            count = float((1 if current_date.weekday() in {0, 2, 4} else 0) + (1 if current_date.day % 5 == 0 else 0))
            history.append({'date': current_date, 'count': count, 'avg_temperature': avg_temperature})
            current_date += timedelta(days=1)
        return history

    def _run_temperature_backtest(self, daily_history: list[dict[str, object]]) -> dict[str, object]:
        history_frame = _build_history_frame(daily_history)
        dataset = _build_backtest_seed_dataset(history_frame)
        result = _run_backtest(history_frame, dataset)
        self.assertTrue(result['is_ready'], msg=result.get('message'))
        return result

    def _assert_optional_almost_equal(self, left: object, right: object, places: int = 6) -> None:
        if left is None or right is None:
            self.assertIsNone(left)
            self.assertIsNone(right)
            return
        self.assertAlmostEqual(float(left), float(right), places=places)

    def test_future_temperatures_do_not_change_past_backtest_windows(self) -> None:
        daily_history = self._build_daily_history()
        original = self._run_temperature_backtest(daily_history)
        first_window = original['window_rows'][0]
        first_window_date = date.fromisoformat(first_window['date'])

        mutated_history = []
        for row in daily_history:
            avg_temperature = row['avg_temperature']
            if row['date'] > first_window_date and row['date'].month == first_window_date.month:
                avg_temperature = 45.0
            mutated_history.append(
                {
                    'date': row['date'],
                    'count': row['count'],
                    'avg_temperature': avg_temperature,
                }
            )

        mutated = self._run_temperature_backtest(mutated_history)
        mutated_first_window = next(row for row in mutated['window_rows'] if row['date'] == first_window['date'])

        self.assertEqual(first_window['date'], mutated_first_window['date'])
        self._assert_optional_almost_equal(first_window['actual_count'], mutated_first_window['actual_count'])
        self._assert_optional_almost_equal(first_window['baseline_count'], mutated_first_window['baseline_count'])
        self._assert_optional_almost_equal(first_window['heuristic_count'], mutated_first_window['heuristic_count'])
        self._assert_optional_almost_equal(
            first_window['predicted_event_probability'],
            mutated_first_window['predicted_event_probability'],
        )

        self.assertEqual(set(first_window['predictions']), set(mutated_first_window['predictions']))
        for model_key in first_window['predictions']:
            self._assert_optional_almost_equal(
                first_window['predictions'][model_key],
                mutated_first_window['predictions'][model_key],
            )

    def test_backtest_prediction_intervals_are_non_negative_and_report_honest_coverage(self) -> None:
        result = self._run_temperature_backtest(self._build_daily_history())
        overview = result['backtest_overview']

        self.assertEqual(overview['prediction_interval_level_display'], '80%')
        if overview['prediction_interval_coverage'] is not None:
            self.assertTrue(overview['prediction_interval_coverage_validated'])
            self.assertGreaterEqual(float(overview['prediction_interval_coverage']), 0.0)
            self.assertLessEqual(float(overview['prediction_interval_coverage']), 1.0)
            self.assertEqual(overview['prediction_interval_validation_scheme_key'], 'rolling_split_conformal')
            self.assertIn('out-of-sample coverage', overview['prediction_interval_coverage_note'])
        else:
            self.assertEqual(overview['prediction_interval_coverage_display'], '—')
            self.assertFalse(overview['prediction_interval_coverage_validated'])
            self.assertTrue(overview['prediction_interval_coverage_note'])
        for row in result['rows']:
            self.assertGreaterEqual(float(row['lower_bound']), 0.0)
            self.assertLessEqual(float(row['lower_bound']), float(row['upper_bound']))

    def test_backtest_out_of_sample_coverage_is_not_computed_on_all_residuals(self) -> None:
        result = self._run_temperature_backtest(self._build_daily_history())
        overview = result['backtest_overview']

        self.assertGreater(int(overview['prediction_interval_calibration_windows']), 0)
        self.assertGreater(int(overview['prediction_interval_evaluation_windows']), 0)
        self.assertEqual(
            int(overview['prediction_interval_calibration_windows']) + int(overview['prediction_interval_evaluation_windows']),
            int(overview['folds']),
        )
        self.assertEqual(overview['prediction_interval_validation_scheme_key'], 'rolling_split_conformal')
        self.assertIn('fixed 60/40 chrono split', overview['prediction_interval_validation_explanation'])
        self.assertIn('Jackknife+ for time series', overview['prediction_interval_validation_explanation'])

        actuals = np.asarray([row['actual_count'] for row in result['rows']], dtype=float)
        predictions = np.asarray([row['predicted_count'] for row in result['rows']], dtype=float)
        pooled_coverage = _interval_coverage(actuals, predictions, result['prediction_interval_calibration'])

        self.assertIsNotNone(overview['prediction_interval_coverage'])
        self.assertIsNotNone(pooled_coverage)
        self.assertNotAlmostEqual(float(overview['prediction_interval_coverage']), float(pooled_coverage), places=6)


class SummaryPresentationTests(unittest.TestCase):
    @staticmethod
    def _daily_history() -> list[dict[str, object]]:
        return [{'date': date(2024, 1, 15), 'count': 2.0}]

    def _build_summary_payload(self, ml_result: dict[str, object]) -> dict[str, object]:
        return _build_summary(
            selected_table='all',
            selected_cause='all',
            selected_object_category='all',
            daily_history=self._daily_history(),
            filtered_records_count=12,
            ml_result=ml_result,
            history_window='all',
            scenario_temperature=None,
        )

    def test_summary_contract_for_baseline_only_nullable_metrics(self) -> None:
        summary = self._build_summary_payload(
            {
                'forecast_rows': [],
                'count_model_label': 'Seasonal baseline',
                'count_mae': 1.6,
                'count_rmse': 2.0,
                'count_smape': 24.1,
                'count_poisson_deviance': 1.25,
                'baseline_count_mae': 1.6,
                'baseline_count_rmse': 2.0,
                'baseline_count_smape': 24.1,
                'heuristic_count_mae': None,
                'heuristic_count_rmse': None,
                'heuristic_count_smape': None,
                'heuristic_count_poisson_deviance': None,
                'count_vs_baseline_delta': None,
                'top_feature_label': None,
                'prediction_interval_level_display': None,
                'prediction_interval_coverage_display': None,
                'prediction_interval_method_label': None,
            }
        )

        expected = {
            'count_model_label': 'Seasonal baseline',
            'count_mae_display': '1,6',
            'count_rmse_display': '2',
            'count_smape_display': '24,1%',
            'count_poisson_deviance_display': '1,2',
            'baseline_count_mae_display': '1,6',
            'baseline_count_rmse_display': '2',
            'baseline_count_smape_display': '24,1%',
            'heuristic_count_mae_display': '—',
            'heuristic_count_rmse_display': '—',
            'heuristic_count_smape_display': '—',
            'heuristic_count_poisson_deviance_display': '—',
            'mae_vs_baseline_display': '—',
            'predicted_total_display': '—',
            'average_expected_count_display': '—',
            'peak_expected_count_display': '—',
            'prediction_interval_level_display': '—',
            'prediction_interval_coverage_display': '—',
            'prediction_interval_method_label': '—',
            'top_feature_label': '—',
        }

        self.assertEqual({key: summary[key] for key in expected}, expected)

    def test_summary_uses_dash_for_missing_baseline_metrics(self) -> None:
        summary = self._build_summary_payload(
            {
                'forecast_rows': [],
                'count_model_label': 'Poisson GLM',
                'baseline_count_mae': None,
                'baseline_count_rmse': None,
                'baseline_count_smape': None,
                'baseline_count_poisson_deviance': None,
                'prediction_interval_level_display': None,
                'prediction_interval_coverage_display': None,
                'prediction_interval_method_label': None,
            }
        )

        self.assertEqual(summary['baseline_count_mae_display'], '—')
        self.assertEqual(summary['baseline_count_rmse_display'], '—')
        self.assertEqual(summary['baseline_count_smape_display'], '—')
        self.assertEqual(summary['prediction_interval_level_display'], '—')
        self.assertEqual(summary['prediction_interval_coverage_display'], '—')
        self.assertEqual(summary['prediction_interval_method_label'], '—')

    def test_summary_uses_dash_for_missing_heuristic_metrics(self) -> None:
        summary = self._build_summary_payload(
            {
                'forecast_rows': [],
                'count_model_label': 'Heuristic forecast',
                'heuristic_count_mae': None,
                'heuristic_count_rmse': None,
                'heuristic_count_smape': None,
                'heuristic_count_poisson_deviance': None,
                'heuristic_brier_score': None,
                'heuristic_roc_auc': None,
                'heuristic_f1_score': None,
            }
        )

        self.assertEqual(summary['heuristic_count_mae_display'], '—')
        self.assertEqual(summary['heuristic_count_rmse_display'], '—')
        self.assertEqual(summary['heuristic_count_smape_display'], '—')
        self.assertEqual(summary['heuristic_count_poisson_deviance_display'], '—')
        self.assertEqual(summary['heuristic_brier_display'], '—')
        self.assertEqual(summary['heuristic_roc_auc_display'], '—')
        self.assertEqual(summary['heuristic_f1_display'], '—')

    def test_summary_uses_dash_for_empty_result_forecast_metrics(self) -> None:
        summary = self._build_summary_payload(
            {
                'forecast_rows': [],
                'count_model_label': None,
                'top_feature_label': None,
                'count_mae': None,
                'count_rmse': None,
                'count_smape': None,
                'count_poisson_deviance': None,
                'brier_score': None,
                'roc_auc': None,
                'f1_score': None,
                'log_loss': None,
            }
        )

        self.assertEqual(summary['count_mae_display'], '—')
        self.assertEqual(summary['count_rmse_display'], '—')
        self.assertEqual(summary['count_smape_display'], '—')
        self.assertEqual(summary['count_poisson_deviance_display'], '—')
        self.assertEqual(summary['predicted_total_display'], '—')
        self.assertEqual(summary['average_expected_count_display'], '—')
        self.assertEqual(summary['peak_expected_count_display'], '—')
        self.assertEqual(summary['elevated_risk_days_display'], '—')
        self.assertEqual(summary['average_event_probability_display'], '—')
        self.assertEqual(summary['peak_event_probability_display'], '—')
        self.assertEqual(summary['top_feature_label'], '—')


    def test_summary_hides_event_probability_block_when_it_is_disabled(self) -> None:
        summary = self._build_summary_payload(
            {
                'forecast_rows': [
                    {
                        'date_display': '16.01.2024',
                        'forecast_value': 2.0,
                        'event_probability': 1.0,
                        'event_probability_display': '100,0%',
                    }
                ],
                'classifier_ready': True,
                'event_probability_enabled': False,
            }
        )

        self.assertFalse(summary['event_probability_enabled'])
        self.assertEqual(summary['average_event_probability_display'], 'вЂ”')
        self.assertEqual(summary['peak_event_probability_display'], 'вЂ”')
        self.assertEqual(summary['peak_event_probability_day_display'], 'вЂ”')


    def test_summary_hides_event_probability_block_when_it_is_disabled(self) -> None:
        summary = self._build_summary_payload(
            {
                'forecast_rows': [
                    {
                        'date_display': '16.01.2024',
                        'forecast_value': 2.0,
                        'event_probability': 1.0,
                        'event_probability_display': '100,0%',
                    }
                ],
                'classifier_ready': True,
                'event_probability_enabled': False,
            }
        )

        self.assertFalse(summary['event_probability_enabled'])
        self.assertEqual(summary['average_event_probability_display'], 'вЂ”')
        self.assertEqual(summary['peak_event_probability_display'], 'вЂ”')
        self.assertEqual(summary['peak_event_probability_day_display'], 'вЂ”')


    def test_summary_hides_event_probability_block_when_it_is_disabled(self) -> None:
        summary = self._build_summary_payload(
            {
                'forecast_rows': [
                    {
                        'date_display': '16.01.2024',
                        'forecast_value': 2.0,
                        'event_probability': 1.0,
                        'event_probability_display': '100,0%',
                    }
                ],
                'classifier_ready': True,
                'event_probability_enabled': False,
            }
        )

        placeholder = summary['count_mae_display']
        self.assertFalse(summary['event_probability_enabled'])
        self.assertEqual(summary['average_event_probability_display'], placeholder)
        self.assertEqual(summary['peak_event_probability_display'], placeholder)
        self.assertEqual(summary['peak_event_probability_day_display'], placeholder)


class PresentationMissingMetricsRegressionTests(unittest.TestCase):
    @staticmethod
    def _daily_history() -> list[dict[str, object]]:
        return [{'date': date(2024, 1, 15), 'count': 2.0}]

    def test_missing_metric_payload_stays_consistent_across_summary_and_quality_assessment(self) -> None:
        ml_result = {
            'is_ready': True,
            'forecast_rows': [],
            'count_model_label': 'Seasonal baseline',
            'count_mae': None,
            'count_rmse': None,
            'count_smape': None,
            'count_poisson_deviance': None,
            'baseline_count_mae': 1.6,
            'baseline_count_rmse': None,
            'baseline_count_smape': None,
            'baseline_count_poisson_deviance': None,
            'heuristic_count_mae': None,
            'heuristic_count_rmse': None,
            'heuristic_count_smape': None,
            'heuristic_count_poisson_deviance': None,
            'count_vs_baseline_delta': None,
            'top_feature_label': None,
            'prediction_interval_level_display': None,
            'prediction_interval_coverage_display': None,
            'prediction_interval_method_label': None,
            'count_comparison_rows': [
                {
                    'method_label': 'Seasonal baseline',
                    'role_label': 'Reference',
                    'is_selected': True,
                    'mae': 1.6,
                    'rmse': None,
                    'smape': None,
                    'poisson_deviance': None,
                    'mae_delta_vs_baseline': None,
                }
            ],
            'event_comparison_rows': [],
            'backtest_overview': {
                'candidate_model_labels': ['Seasonal baseline'],
                'min_train_rows': None,
                'prediction_interval_level_display': None,
                'prediction_interval_coverage_display': None,
                'prediction_interval_method_label': None,
            },
        }

        summary = _build_summary(
            selected_table='all',
            selected_cause='all',
            selected_object_category='all',
            daily_history=self._daily_history(),
            filtered_records_count=12,
            ml_result=ml_result,
            history_window='all',
            scenario_temperature=None,
        )
        quality = _build_quality_assessment(ml_result)

        self.assertEqual(summary['count_mae_display'], '—')
        self.assertEqual(summary['baseline_count_mae_display'], '1,6')
        self.assertEqual(summary['baseline_count_rmse_display'], '—')
        self.assertEqual(summary['mae_vs_baseline_display'], '—')
        self.assertEqual(summary['prediction_interval_method_label'], '—')

        count_row = quality['count_table']['rows'][0]
        self.assertEqual(count_row['mae_display'], '1,6')
        self.assertEqual(count_row['rmse_display'], '—')
        self.assertEqual(count_row['smape_display'], '—')
        self.assertEqual(count_row['poisson_display'], '—')
        self.assertEqual(count_row['mae_delta_display'], '—')

        self.assertEqual(quality['metric_cards'][0]['value'], '—')
        self.assertEqual(quality['metric_cards'][1]['value'], '—')
        self.assertIn('seasonal baseline: 1,6; heuristic forecast: —', quality['metric_cards'][0]['meta'])
        self.assertEqual(quality['methodology_items'][1]['value'], '—')
        self.assertEqual(quality['model_choice']['facts'][2]['value'], '—')


class QualityAssessmentPresentationTests(unittest.TestCase):
    @staticmethod
    def _baseline_only_quality_payload() -> dict[str, object]:
        return {
            'is_ready': True,
            'count_mae': 1.6,
            'count_rmse': 2.0,
            'count_smape': 24.1,
            'count_poisson_deviance': 1.25,
            'baseline_count_mae': 1.6,
            'baseline_count_rmse': 2.0,
            'baseline_count_smape': 24.1,
            'baseline_count_poisson_deviance': 1.25,
            'heuristic_count_mae': None,
            'heuristic_count_rmse': None,
            'heuristic_count_smape': None,
            'heuristic_count_poisson_deviance': None,
            'count_model_label': 'Seasonal baseline',
            'selected_count_model_reason_short': 'Baseline selected.',
            'selected_count_model_reason': 'Baseline selected after backtesting.',
            'top_feature_label': None,
            'count_comparison_rows': [
                {
                    'method_label': 'Seasonal baseline',
                    'role_label': 'Reference',
                    'is_selected': True,
                    'mae': 1.6,
                    'rmse': 2.0,
                    'smape': 24.1,
                    'poisson_deviance': 1.25,
                    'mae_delta_vs_baseline': None,
                }
            ],
            'event_comparison_rows': [],
            'backtest_overview': {
                'candidate_model_labels': ['Seasonal baseline'],
                'selection_rule': 'Minimum Poisson deviance',
                'prediction_interval_level_display': None,
                'prediction_interval_coverage_display': None,
                'prediction_interval_method_label': None,
            },
        }

    def test_quality_assessment_contract_for_baseline_only_scenario(self) -> None:
        quality = _build_quality_assessment(self._baseline_only_quality_payload())

        self.assertEqual(
            quality['count_table'],
            {
                'title': 'Сравнение по числу пожаров',
                'rows': [
                    {
                        'method_label': 'Seasonal baseline',
                        'role_label': 'Reference',
                        'selection_label': 'Рабочий метод',
                        'mae_display': '1,6',
                        'rmse_display': '2',
                        'smape_display': '24,1%',
                        'poisson_display': '1,2',
                        'mae_delta_display': '—',
                    }
                ],
                'empty_message': 'Сравнение seasonal baseline, heuristic forecast и count-model появится после проверки на истории.',
            },
        )
        self.assertEqual(
            quality['metric_cards'][:4],
            [
                {
                    'label': 'MAE по числу пожаров',
                    'value': '1,6',
                    'meta': 'seasonal baseline: 1,6; heuristic forecast: —',
                },
                {
                    'label': 'RMSE по числу пожаров',
                    'value': '2',
                    'meta': 'seasonal baseline: 2; heuristic forecast: —',
                },
                {
                    'label': 'sMAPE по числу пожаров',
                    'value': '24,1%',
                    'meta': 'seasonal baseline: 24,1%; heuristic forecast: —',
                },
                {
                    'label': 'Poisson deviance',
                    'value': '1,2',
                    'meta': 'seasonal baseline: 1,2; heuristic forecast: —',
                },
            ],
        )
        self.assertEqual(quality['metric_cards'][4]['label'], 'Out-of-sample coverage — интервала')
        self.assertEqual(quality['metric_cards'][4]['value'], '—')
        self.assertIn('validated out-of-sample coverage не показывается', quality['metric_cards'][4]['meta'])
        self.assertEqual(quality['count_table']['rows'][0]['mae_delta_display'], '—')
        self.assertNotIn('0%', quality['count_table']['rows'][0].values())

    def test_quality_assessment_contract_for_empty_count_comparison_rows(self) -> None:
        quality = _build_quality_assessment(
            {
                'is_ready': True,
                'count_comparison_rows': [],
                'event_comparison_rows': [],
                'backtest_overview': {},
            }
        )

        self.assertEqual(
            quality['count_table'],
            {
                'title': 'Сравнение по числу пожаров',
                'rows': [],
                'empty_message': 'Сравнение seasonal baseline, heuristic forecast и count-model появится после проверки на истории.',
            },
        )
        self.assertEqual(
            [card['label'] for card in quality['metric_cards']],
            [
                'MAE по числу пожаров',
                'RMSE по числу пожаров',
                'sMAPE по числу пожаров',
                'Poisson deviance',
                'Out-of-sample coverage — интервала',
            ],
        )
        self.assertTrue(all(card['value'] == '—' for card in quality['metric_cards']))
        self.assertEqual(
            [item['label'] for item in quality['methodology_items'][:6]],
            [
                'Схема валидации',
                'Минимум обучающего окна',
                'Сравниваемые count-методы',
                'Индекс пере-дисперсии',
                'Правило выбора',
                'Интервал прогноза',
            ],
        )
        self.assertEqual(quality['methodology_items'][1]['value'], '—')
        self.assertEqual(quality['methodology_items'][2]['value'], '—')

    def test_quality_assessment_uses_dash_for_missing_baseline_row_metrics(self) -> None:
        quality = _build_quality_assessment(
            {
                'is_ready': True,
                'count_comparison_rows': [
                    {
                        'method_label': 'Baseline',
                        'role_label': 'Reference',
                        'is_selected': False,
                        'mae': None,
                        'rmse': None,
                        'smape': None,
                        'poisson_deviance': None,
                        'mae_delta_vs_baseline': None,
                    }
                ],
                'event_comparison_rows': [],
                'backtest_overview': {},
            }
        )

        baseline_row = quality['count_table']['rows'][0]
        self.assertEqual(baseline_row['mae_display'], '—')
        self.assertEqual(baseline_row['rmse_display'], '—')
        self.assertEqual(baseline_row['smape_display'], '—')
        self.assertEqual(baseline_row['poisson_display'], '—')
        self.assertEqual(baseline_row['mae_delta_display'], '—')
        self.assertNotIn('0%', baseline_row.values())

    def _legacy_test_quality_assessment_uses_dash_for_missing_heuristic_row_metrics(self) -> None:
        quality = _build_quality_assessment(
            {
                'is_ready': True,
                'count_comparison_rows': [
                    {
                        'method_label': 'Heuristic',
                        'role_label': 'Scenario forecast',
                        'is_selected': False,
                        'mae': None,
                        'rmse': None,
                        'smape': None,
                        'poisson_deviance': None,
                        'mae_delta_vs_baseline': None,
                    }
                ],
                'event_comparison_rows': [],
                'backtest_overview': {},
            }
        )

        heuristic_row = quality['count_table']['rows'][0]
        self.assertEqual(heuristic_row['mae_display'], 'вЂ”')
        self.assertEqual(heuristic_row['rmse_display'], 'вЂ”')
        self.assertEqual(heuristic_row['smape_display'], 'вЂ”')
        self.assertEqual(heuristic_row['poisson_display'], 'вЂ”')
        self.assertEqual(heuristic_row['mae_delta_display'], 'вЂ”')

    def test_quality_assessment_uses_dash_for_missing_heuristic_row_metrics(self) -> None:
        quality = _build_quality_assessment(
            {
                'is_ready': True,
                'count_comparison_rows': [
                    {
                        'method_label': 'Heuristic',
                        'role_label': 'Scenario forecast',
                        'is_selected': False,
                        'mae': None,
                        'rmse': None,
                        'smape': None,
                        'poisson_deviance': None,
                        'mae_delta_vs_baseline': None,
                    }
                ],
                'event_comparison_rows': [],
                'backtest_overview': {},
            }
        )

        heuristic_row = quality['count_table']['rows'][0]
        self.assertEqual(heuristic_row['mae_display'], '—')
        self.assertEqual(heuristic_row['rmse_display'], '—')
        self.assertEqual(heuristic_row['smape_display'], '—')
        self.assertEqual(heuristic_row['poisson_display'], '—')
        self.assertEqual(heuristic_row['mae_delta_display'], '—')

    def test_quality_assessment_uses_dash_for_empty_metric_row(self) -> None:
        quality = _build_quality_assessment(
            {
                'is_ready': True,
                'count_comparison_rows': [
                    {
                        'method_label': 'Unavailable model',
                        'role_label': 'Count model',
                        'is_selected': False,
                        'mae': None,
                        'rmse': None,
                        'smape': None,
                        'poisson_deviance': None,
                        'mae_delta_vs_baseline': None,
                    }
                ],
                'event_comparison_rows': [
                    {
                        'method_label': 'Unavailable event model',
                        'role_label': 'Classifier',
                        'is_selected': False,
                        'brier_score': None,
                        'roc_auc': None,
                        'f1': None,
                        'log_loss': None,
                    }
                ],
                'backtest_overview': {},
            }
        )

        count_row = quality['count_table']['rows'][0]
        self.assertEqual(count_row['mae_display'], '—')
        self.assertEqual(count_row['rmse_display'], '—')
        self.assertEqual(count_row['smape_display'], '—')
        self.assertEqual(count_row['poisson_display'], '—')
        self.assertEqual(count_row['mae_delta_display'], '—')
        self.assertNotIn('0%', count_row.values())
        self.assertEqual(quality['methodology_items'][1]['value'], '—')
        event_row = quality['event_table']['rows'][0]
        self.assertEqual(event_row['brier_display'], '—')
        self.assertEqual(event_row['roc_auc_display'], '—')
        self.assertEqual(event_row['f1_display'], '—')
        self.assertEqual(event_row['log_loss_display'], '—')

    def test_quality_assessment_explains_when_interval_coverage_is_not_validated(self) -> None:
        quality = _build_quality_assessment(
            {
                'is_ready': True,
                'count_comparison_rows': [],
                'event_comparison_rows': [],
                'backtest_overview': {
                    'prediction_interval_level_display': '80%',
                    'prediction_interval_coverage_display': '—',
                    'prediction_interval_method_label': 'Adaptive conformal interval with predicted-count bins (validated out-of-sample coverage unavailable)',
                    'prediction_interval_coverage_validated': False,
                    'prediction_interval_calibration_windows': 9,
                    'prediction_interval_evaluation_windows': 0,
                },
            }
        )

        interval_card = next(item for item in quality['metric_cards'] if item['label'] == 'Out-of-sample coverage 80% интервала')
        interval_item = next(item for item in quality['methodology_items'] if item['label'] == 'Интервал прогноза')

        self.assertEqual(interval_card['value'], '—')
        self.assertIn('validated out-of-sample coverage', interval_card['meta'])
        self.assertIn('validated out-of-sample coverage', interval_item['meta'])

    def test_quality_assessment_shows_real_selected_working_method(self) -> None:
        ml_result = {
            'is_ready': True,
            'count_mae': 0.98,
            'count_rmse': 1.14,
            'count_smape': 16.8,
            'count_poisson_deviance': 0.79,
            'baseline_count_mae': 1.20,
            'baseline_count_rmse': 1.35,
            'baseline_count_smape': 20.5,
            'baseline_count_poisson_deviance': 0.93,
            'heuristic_count_mae': 0.98,
            'heuristic_count_rmse': 1.14,
            'heuristic_count_smape': 16.8,
            'heuristic_count_poisson_deviance': 0.79,
            'count_vs_baseline_delta': -0.1833,
            'event_backtest_available': False,
            'count_model_label': 'Сценарный эвристический прогноз',
            'prediction_interval_level_display': '80%',
            'prediction_interval_coverage_display': '83.3%',
            'prediction_interval_method_label': 'Adaptive conformal interval with predicted-count bins; validated by Forward rolling split conformal',
            'selected_count_model_reason_short': 'Эвристика почти не хуже лучшей ML-модели, поэтому выбран более объяснимый метод.',
            'selected_count_model_reason': 'Сработал tie-break в пользу сценарной эвристики.',
            'selected_event_model_label': None,
            'top_feature_label': '-',
            'count_comparison_rows': [
                {
                    'method_label': 'Сезонная базовая модель',
                    'role_label': 'Базовая модель',
                    'is_selected': False,
                    'mae': 1.2,
                    'rmse': 1.35,
                    'smape': 20.5,
                    'poisson_deviance': 0.93,
                    'mae_delta_vs_baseline': None,
                },
                {
                    'method_label': 'Сценарный эвристический прогноз',
                    'role_label': 'Сценарный прогноз',
                    'is_selected': True,
                    'mae': 0.98,
                    'rmse': 1.14,
                    'smape': 16.8,
                    'poisson_deviance': 0.79,
                    'mae_delta_vs_baseline': -0.1833,
                },
                {
                    'method_label': 'Poisson GLM',
                    'role_label': 'Count model',
                    'is_selected': False,
                    'mae': 0.97,
                    'rmse': 1.13,
                    'smape': 16.7,
                    'poisson_deviance': 0.78,
                    'mae_delta_vs_baseline': -0.1917,
                },
            ],
            'event_comparison_rows': [],
            'backtest_method_label': 'Rolling-origin backtesting (expanding window): 12 folds',
            'backtest_overview': {
                'folds': 12,
                'min_train_rows': 28,
                'validation_horizon_days': 1,
                'selection_rule': 'Minimum Poisson deviance with explainability tie-break',
                'event_selection_rule': 'Brier score, then log-loss and ROC-AUC',
                'classification_threshold': 0.5,
                'candidate_model_labels': ['Poisson GLM', 'Tweedie GLM'],
                'dispersion_ratio': 1.18,
                'prediction_interval_level_display': '80%',
                'prediction_interval_coverage_display': '83.3%',
                'prediction_interval_method_label': 'Adaptive conformal interval with predicted-count bins; validated by Forward rolling split conformal',
                'prediction_interval_validation_explanation': 'Forward rolling split conformal was selected for validated out-of-sample coverage because it was more stable on later windows than Blocked forward CV conformal and improved coverage stability versus the previous fixed 60/40 chrono split. Jackknife+ for time series was not adopted because an honest time-series variant would require leave-one-block-out refits for every checkpoint.',
            },
        }

        quality = _build_quality_assessment(ml_result)

        self.assertEqual(quality['model_choice']['title'], 'Почему выбран рабочий метод')
        self.assertEqual(quality['model_choice']['facts'][0]['label'], 'Рабочий count-метод')
        self.assertEqual(quality['model_choice']['facts'][0]['value'], 'Сценарный эвристический прогноз')
        self.assertEqual(quality['count_table']['rows'][1]['selection_label'], 'Рабочий метод')
        interval_item = next(item for item in quality['methodology_items'] if item['label'] == 'Интервал прогноза')
        self.assertEqual(interval_item['value'], '80%')
        self.assertIn('Forward rolling split conformal', interval_item['meta'])

    def test_quality_assessment_exposes_poisson_deviance_and_model_choice(self) -> None:
        ml_result = {
            'is_ready': True,
            'count_mae': 1.1,
            'count_rmse': 1.4,
            'count_smape': 19.3,
            'count_poisson_deviance': 0.88,
            'baseline_count_mae': 1.6,
            'baseline_count_rmse': 2.0,
            'baseline_count_smape': 24.1,
            'baseline_count_poisson_deviance': 1.25,
            'heuristic_count_mae': 1.3,
            'heuristic_count_rmse': 1.7,
            'heuristic_count_smape': 21.0,
            'heuristic_count_poisson_deviance': 0.97,
            'count_vs_baseline_delta': -0.3125,
            'event_backtest_available': False,
            'count_model_label': 'Poisson GLM',
            'prediction_interval_level_display': '80%',
            'prediction_interval_coverage_display': '91.7%',
            'prediction_interval_method_label': 'Adaptive conformal interval with predicted-count bins; validated by Forward rolling split conformal',
            'selected_count_model_reason_short': 'Poisson was kept because quality is close to the best candidate.',
            'selected_count_model_reason': 'Poisson remained the working count model after rolling-origin comparison.',
            'selected_event_model_label': None,
            'top_feature_label': 'Rolling 7 mean',
            'count_comparison_rows': [
                {
                    'method_label': 'Baseline',
                    'role_label': 'Reference',
                    'is_selected': False,
                    'mae': 1.6,
                    'rmse': 2.0,
                    'smape': 24.1,
                    'poisson_deviance': 1.25,
                    'mae_delta_vs_baseline': None,
                },
                {
                    'method_label': 'Poisson GLM',
                    'role_label': 'Count model',
                    'is_selected': True,
                    'mae': 1.1,
                    'rmse': 1.4,
                    'smape': 19.3,
                    'poisson_deviance': 0.88,
                    'mae_delta_vs_baseline': -0.3125,
                },
            ],
            'event_comparison_rows': [],
            'backtest_method_label': 'Rolling-origin backtesting (expanding window): 12 folds',
            'backtest_overview': {
                'folds': 12,
                'min_train_rows': 28,
                'validation_horizon_days': 1,
                'selection_rule': 'Poisson deviance, then MAE and RMSE',
                'event_selection_rule': 'Brier score, then log-loss and ROC-AUC',
                'classification_threshold': 0.5,
                'candidate_model_labels': ['Poisson GLM', 'Tweedie GLM'],
                'dispersion_ratio': 1.42,
                'prediction_interval_level_display': '80%',
                'prediction_interval_coverage_display': '91.7%',
                'prediction_interval_method_label': 'Adaptive conformal interval with predicted-count bins; validated by Forward rolling split conformal',
            },
        }

        quality = _build_quality_assessment(ml_result)

        metric_labels = [item['label'] for item in quality['metric_cards']]
        methodology_labels = [item['label'] for item in quality['methodology_items']]

        self.assertIn('Poisson deviance', metric_labels)
        self.assertIn('Out-of-sample coverage 80% интервала', metric_labels)
        self.assertTrue(any('count' in label.lower() for label in methodology_labels))
        self.assertTrue(quality['model_choice']['title'])
        self.assertTrue(quality['model_choice']['facts'])
        self.assertEqual(quality['model_choice']['facts'][0]['value'], 'Poisson GLM')


if __name__ == '__main__':
    unittest.main()
