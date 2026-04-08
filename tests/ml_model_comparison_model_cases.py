import warnings
import unittest
from datetime import date, timedelta
from unittest.mock import patch

import numpy as np
import pandas as pd
from sklearn.exceptions import ConvergenceWarning

from app.services.ml_model.domain_types import BacktestWindowRow, CountMetrics, HorizonSummary
from app.services.ml_model import training as ml_training
from app.services.ml_model import training_backtesting as ml_training_backtesting
from app.services.ml_model import training_forecast as ml_training_forecast
from app.services.ml_model import training_models as ml_training_models
from app.services.model_quality import compute_classification_metrics
from app.services.ml_model.presentation import _build_notes, _build_quality_assessment, _build_summary
from app.services.ml_model.training import (
    _build_backtest_seed_dataset,
    _build_count_selection_details,
    _build_design_row,
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
from tests.mojibake_check import encode_as_mojibake


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

    def test_backtest_selection_reuses_validation_horizon_arrays(self) -> None:
        actual_counts = [float(index) for index in range(1, 10)]
        rows = [
            BacktestWindowRow(
                origin_date=f'2024-01-{index:02d}',
                date=f'2024-02-{index:02d}',
                horizon_days=1,
                actual_count=actual_count,
                baseline_count=actual_count + 10.0,
                heuristic_count=actual_count + 9.0,
                actual_event=1 if index % 2 else 0,
                baseline_event_probability=0.5,
                heuristic_event_probability=0.5,
                predictions={
                    'poisson': actual_count,
                    'negative_binomial': actual_count + 4.0,
                },
                predicted_event_probabilities={
                    'poisson': 0.5,
                    'negative_binomial': 0.5,
                },
            )
            for index, actual_count in enumerate(actual_counts, start=1)
        ]
        dataset = pd.DataFrame({'count': actual_counts})

        selection = ml_training_backtesting._select_backtest_count_model(rows, dataset)

        self.assertEqual(selection.selected_count_model_key, 'poisson')
        np.testing.assert_allclose(selection.validation_evaluation_data.selected_predictions, actual_counts)
        with patch.object(
            ml_training_backtesting,
            '_selected_count_predictions',
            side_effect=AssertionError('validation horizon selected predictions should be reused'),
        ):
            evaluation_data_by_horizon = ml_training_backtesting._build_horizon_evaluation_data_by_horizon(
                {1: rows},
                [1],
                selection.selected_count_model_key,
                precomputed={1: selection.validation_evaluation_data},
            )

        self.assertIs(evaluation_data_by_horizon[1], selection.validation_evaluation_data)

    def test_backtest_artifacts_reuse_selected_predictions_for_rows_and_intervals(self) -> None:
        actual_counts = [float(index) for index in range(1, 10)]
        rows = [
            BacktestWindowRow(
                origin_date=f'2024-01-{index:02d}',
                date=f'2024-02-{index:02d}',
                horizon_days=1,
                actual_count=actual_count,
                baseline_count=actual_count + 10.0,
                heuristic_count=actual_count + 9.0,
                actual_event=1 if index % 2 else 0,
                baseline_event_probability=0.5,
                heuristic_event_probability=0.5,
                predictions={
                    'poisson': actual_count,
                    'negative_binomial': actual_count + 4.0,
                },
                predicted_event_probabilities={
                    'poisson': 0.5,
                    'negative_binomial': 0.5,
                },
            )
            for index, actual_count in enumerate(actual_counts, start=1)
        ]
        horizon_rows = {
            1: rows,
            2: [
                row.clone(
                    date=f'2024-03-{index:02d}',
                    horizon_days=2,
                    actual_count=float(index + 1),
                    baseline_count=float(index + 11),
                    heuristic_count=float(index + 10),
                    predictions={
                        'poisson': float(index + 1),
                        'negative_binomial': float(index + 5),
                    },
                )
                for index, row in enumerate(rows, start=1)
            ],
        }
        dataset = pd.DataFrame({'count': actual_counts})

        with patch.object(
            ml_training_backtesting,
            '_selected_count_predictions',
            side_effect=AssertionError('selected predictions should be carried in horizon evaluation data'),
        ):
            artifacts = ml_training_backtesting._build_backtest_evaluation_artifacts(
                horizon_rows=horizon_rows,
                horizon_days=[1, 2],
                valid_rows=rows,
                dataset=dataset,
                validation_horizon_days=1,
            )

        self.assertEqual(artifacts.selected_count_model_key, 'poisson')
        np.testing.assert_allclose(
            [row.predicted_count for row in artifacts.backtest_rows],
            actual_counts,
        )
        self.assertEqual(artifacts.event_metrics.rows_used, len(rows))

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
            'negative_binomial': {
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
            'negative_binomial': {
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
            'negative_binomial': {
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
        self.assertEqual(context['raw_best_key'], 'negative_binomial')
        self.assertEqual(context['tie_break_reason'], 'poisson_over_negative_binomial')


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

    class _OverflowingPredictor:
        def __init__(self, template: list[float] | None = None) -> None:
            self._template = np.asarray(template or [np.inf], dtype=float)

        def predict(self, X: pd.DataFrame) -> np.ndarray:
            warnings.warn('overflow encountered in exp', RuntimeWarning)
            return np.resize(self._template, len(X))

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

        with patch.object(ml_training_models, '_build_count_model_pipeline', return_value=self._WarningEstimator()):
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

        with patch.object(ml_training_models.sm, 'GLM', return_value=self._StatsmodelsWarningFit()):
            result = ml_training._fit_negative_binomial_model_from_design(X_train, y_train)

        self.assertIsNone(result)

    def test_predict_count_from_design_sanitizes_non_finite_and_caps_overflow_outputs(self) -> None:
        design = pd.DataFrame(
            [
                {'lag_1': 2.0, 'lag_7': 1.0, 'lag_14': 0.0, 'rolling_7': 1.5, 'rolling_28': 1.2, 'trend_gap': 0.2},
                {'lag_1': 4.0, 'lag_7': 2.0, 'lag_14': 1.0, 'rolling_7': 2.5, 'rolling_28': 2.0, 'trend_gap': 0.3},
                {'lag_1': 6.0, 'lag_7': 3.0, 'lag_14': 1.0, 'rolling_7': 3.5, 'rolling_28': 2.5, 'trend_gap': 0.4},
                {'lag_1': 8.0, 'lag_7': 4.0, 'lag_14': 2.0, 'rolling_7': 4.5, 'rolling_28': 3.0, 'trend_gap': 0.5},
            ]
        )
        model_bundle = {
            'key': 'poisson',
            'backend': 'sklearn',
            'model': self._OverflowingPredictor([np.inf, np.nan, -5.0, 1.0e300]),
            'columns': list(design.columns),
        }

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            predictions = ml_training_models._predict_count_from_design(model_bundle, design)

        self.assertFalse(any('overflow encountered in exp' in str(item.message).lower() for item in caught))
        self.assertTrue(np.isfinite(predictions).all())
        np.testing.assert_allclose(predictions, np.asarray([2.0, 4.0, 0.0, 250.0], dtype=float))

    def test_negative_binomial_requires_enough_history_and_clear_overdispersion(self) -> None:
        short_design = pd.DataFrame(
            [
                {'lag_1': float(index % 3), 'lag_7': 0.0, 'lag_14': 0.0, 'rolling_7': 1.0, 'rolling_28': 1.0, 'trend_gap': 0.0}
                for index in range(40)
            ]
        )
        short_counts = np.asarray([0.0, 4.0] * 20, dtype=float)
        self.assertIsNone(_fit_count_model_from_design('negative_binomial', short_design, short_counts))

        long_design = pd.DataFrame(
            [
                {'lag_1': float(index % 4), 'lag_7': 1.0, 'lag_14': 1.0, 'rolling_7': 2.0, 'rolling_28': 2.0, 'trend_gap': 0.0}
                for index in range(70)
            ]
        )
        low_dispersion_counts = np.asarray([1.0, 2.0, 1.0, 2.0, 1.0] * 14, dtype=float)
        self.assertIsNone(_fit_count_model_from_design('negative_binomial', long_design, low_dispersion_counts))

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
            patch.object(ml_training_backtesting, '_fit_count_model_from_design', side_effect=_fake_fit_count_model_from_design),
            patch.object(ml_training_backtesting, '_fit_event_model_from_design', return_value=None),
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
            patch.object(ml_training_models.sm, 'GLM', return_value=self._StatsmodelsWarningFit()),
            patch.object(ml_training_backtesting, '_fit_event_model_from_design', return_value=None),
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

    def test_backtest_keeps_baseline_or_heuristic_when_ml_candidates_are_unavailable(self) -> None:
        history_frame = _build_history_frame(self._build_daily_history())
        dataset = _build_backtest_seed_dataset(history_frame)

        with (
            patch.object(ml_training_backtesting, '_fit_count_model_from_design', return_value=None),
            patch.object(ml_training_backtesting, '_fit_event_model_from_design', return_value=None),
        ):
            result = _run_backtest(history_frame, dataset)

        self.assertTrue(result['is_ready'], msg=result.get('message'))
        self.assertEqual(result['count_metrics'], {})
        self.assertIn(result['selected_count_model_key'], {'seasonal_baseline', 'heuristic_forecast'})
        self.assertEqual(
            [row['method_key'] for row in result['count_comparison_rows']],
            ['seasonal_baseline', 'heuristic_forecast'],
        )
        self.assertNotIn(ml_training.COUNT_MODEL_LABELS['poisson'], result['backtest_overview']['candidate_model_labels'])
        self.assertGreater(len(result['rows']), 0)

    def test_backtest_tracks_partial_window_coverage_for_ml_candidates_without_dropping_baseline_windows(self) -> None:
        history_frame = _build_history_frame(self._build_daily_history())
        dataset = _build_backtest_seed_dataset(history_frame)
        poisson_fit_calls = 0

        def _intermittent_fit_count_model(model_key: str, X_train: pd.DataFrame, y_train: np.ndarray):
            nonlocal poisson_fit_calls
            if model_key == 'negative_binomial':
                return None
            if model_key == 'poisson':
                poisson_fit_calls += 1
                if poisson_fit_calls % 2 == 0:
                    return None
                return {
                    'key': model_key,
                    'backend': 'sklearn',
                    'model': self._ColumnPredictor('lag_1', 0.8),
                    'columns': list(X_train.columns),
                }
            return None

        with (
            patch.object(ml_training_backtesting, '_fit_count_model_from_design', side_effect=_intermittent_fit_count_model),
            patch.object(ml_training_backtesting, '_fit_event_model_from_design', return_value=None),
        ):
            result = _run_backtest(history_frame, dataset)

        self.assertTrue(result['is_ready'], msg=result.get('message'))
        overview = result['backtest_overview']
        self.assertGreater(int(overview['candidate_window_count']), 0)
        self.assertEqual(int(overview['candidate_window_count']), int(overview['folds']))
        self.assertGreater(int(overview['candidate_covered_window_count_by_model']['poisson']), 0)
        self.assertLess(
            int(overview['candidate_covered_window_count_by_model']['poisson']),
            int(overview['candidate_window_count']),
        )
        self.assertGreater(float(overview['candidate_window_coverage_by_model']['poisson']), 0.0)
        self.assertLess(float(overview['candidate_window_coverage_by_model']['poisson']), 1.0)
        self.assertNotIn('poisson', result['count_metrics'])
        self.assertGreater(len(result['rows']), 0)

    def test_run_backtest_sanitizes_non_finite_predictions_before_metrics_and_intervals(self) -> None:
        history_frame = _build_history_frame(self._build_daily_history())
        dataset = _build_backtest_seed_dataset(history_frame)

        def _overflowing_fit_count_model(model_key: str, X_train: pd.DataFrame, y_train: np.ndarray):
            if model_key == 'negative_binomial':
                return None
            return {
                'key': model_key,
                'backend': 'sklearn',
                'model': self._OverflowingPredictor([np.inf, np.nan, 1.0e300]),
                'columns': list(X_train.columns),
            }

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            with (
                patch.object(ml_training_backtesting, '_fit_count_model_from_design', side_effect=_overflowing_fit_count_model),
                patch.object(ml_training_backtesting, '_fit_event_model_from_design', return_value=None),
            ):
                result = _run_backtest(history_frame, dataset)

        self.assertTrue(result['is_ready'], msg=result.get('message'))
        self.assertFalse(any('overflow encountered in exp' in str(item.message).lower() for item in caught))

        backtest_values: list[float] = []
        for row in result['rows']:
            backtest_values.extend([float(row['predicted_count']), float(row['lower_bound']), float(row['upper_bound'])])
        self.assertTrue(np.isfinite(np.asarray(backtest_values, dtype=float)).all())

        for row in result['window_rows']:
            for prediction in row['predictions'].values():
                if prediction is not None:
                    self.assertTrue(np.isfinite(float(prediction)))

        calibration = result['prediction_interval_calibration']
        self.assertTrue(np.isfinite(float(calibration['absolute_error_quantile'])))
        for bin_row in calibration.get('adaptive_bins') or []:
            self.assertTrue(np.isfinite(float(bin_row['absolute_error_quantile'])))

        for metrics in result['count_metrics'].values():
            for metric_name in ['mae', 'rmse', 'smape', 'poisson_deviance']:
                metric_value = metrics[metric_name]
                if metric_value is not None:
                    self.assertTrue(np.isfinite(float(metric_value)), msg=f'{metric_name} should stay finite')


class CountPredictionStressTests(unittest.TestCase):
    class _ConstantPredictor:
        def __init__(self, value: float) -> None:
            self._value = float(value)

        def predict(self, X: pd.DataFrame) -> np.ndarray:
            return np.full(len(X), self._value, dtype=float)

    @staticmethod
    def _count_model_bundle(value: float) -> dict[str, object]:
        return {
            'key': 'poisson',
            'backend': 'sklearn',
            'model': CountPredictionStressTests._ConstantPredictor(value),
            'columns': ['lag_1', 'lag_7', 'lag_14', 'rolling_7', 'rolling_28', 'trend_gap'],
        }

    @staticmethod
    def _history_frame(counts: list[float]) -> pd.DataFrame:
        current_date = date(2024, 1, 1)
        rows = []
        for count in counts:
            rows.append(
                {
                    'date': pd.Timestamp(current_date),
                    'count': float(count),
                    'avg_temperature': None,
                }
            )
            current_date += timedelta(days=1)
        return pd.DataFrame(rows)

    @staticmethod
    def _daily_history_for_backtest(days: int = 96) -> list[dict[str, object]]:
        history: list[dict[str, object]] = []
        current_date = date(2024, 1, 1)
        for index in range(days):
            count = 0.0
            if index in {41, 58, 73, 74, 90}:
                count = float(15 + (index % 3) * 5)
            history.append(
                {
                    'date': current_date,
                    'count': count,
                    'avg_temperature': float(-5.0 + (index % 7)),
                }
            )
            current_date += timedelta(days=1)
        return history

    def test_sanitize_count_predictions_stays_finite_without_truncating_plausible_extremes_too_early(self) -> None:
        scenarios = [
            (
                'very_sparse_series',
                {'lag_1': 1.0, 'lag_7': 0.0, 'lag_14': 0.0, 'rolling_7': 0.2, 'rolling_28': 0.1, 'trend_gap': 0.1},
                120.0,
                120.0,
            ),
            (
                'sudden_spike_after_zero_stretch',
                {'lag_1': 0.0, 'lag_7': 0.0, 'lag_14': 0.0, 'rolling_7': 0.0, 'rolling_28': 0.0, 'trend_gap': 0.0},
                120.0,
                120.0,
            ),
            (
                'regime_shift',
                {'lag_1': 12.0, 'lag_7': 9.0, 'lag_14': 4.0, 'rolling_7': 10.0, 'rolling_28': 5.0, 'trend_gap': 5.0},
                180.0,
                180.0,
            ),
            (
                'noisy_burst',
                {'lag_1': 25.0, 'lag_7': 14.0, 'lag_14': 3.0, 'rolling_7': 16.0, 'rolling_28': 7.0, 'trend_gap': 9.0},
                400.0,
                400.0,
            ),
        ]

        for scenario_name, row, raw_prediction, expected in scenarios:
            with self.subTest(scenario=scenario_name):
                design = pd.DataFrame([row])
                predictions = ml_training_models._sanitize_count_predictions(np.asarray([raw_prediction], dtype=float), design)
                self.assertTrue(np.isfinite(predictions).all())
                self.assertAlmostEqual(float(predictions[0]), expected)

    def test_sanitize_count_predictions_still_caps_only_numeric_explosions_on_extreme_series(self) -> None:
        scenarios = [
            (
                'long_zero_stretch',
                {'lag_1': 0.0, 'lag_7': 0.0, 'lag_14': 0.0, 'rolling_7': 0.0, 'rolling_28': 0.0, 'trend_gap': 0.0},
                250.0,
            ),
            (
                'regime_shift',
                {'lag_1': 12.0, 'lag_7': 9.0, 'lag_14': 4.0, 'rolling_7': 10.0, 'rolling_28': 5.0, 'trend_gap': 5.0},
                290.0,
            ),
            (
                'noisy_burst',
                {'lag_1': 25.0, 'lag_7': 14.0, 'lag_14': 3.0, 'rolling_7': 16.0, 'rolling_28': 7.0, 'trend_gap': 9.0},
                550.0,
            ),
        ]

        for scenario_name, row, expected_cap in scenarios:
            with self.subTest(scenario=scenario_name):
                design = pd.DataFrame([row])
                predictions = ml_training_models._sanitize_count_predictions(np.asarray([1.0e300], dtype=float), design)
                self.assertTrue(np.isfinite(predictions).all())
                self.assertAlmostEqual(float(predictions[0]), expected_cap)

    def test_future_forecast_rows_keep_plausible_spikes_after_zero_stretch_and_noisy_bursts(self) -> None:
        scenarios = [
            ('long_zero_stretch', [0.0] * 40, 120.0, 120.0),
            ('noisy_burst', [0.0, 2.0, 18.0, 4.0, 25.0] * 8, 400.0, 400.0),
        ]
        interval_calibration = {
            'absolute_error_quantile': 1.0,
            'level': 0.8,
            'level_display': '80%',
            'method_label': 'Adaptive conformal interval with predicted-count bins',
            'coverage_validated': False,
            'validated_coverage': None,
        }

        for scenario_name, counts, raw_prediction, expected in scenarios:
            with self.subTest(scenario=scenario_name):
                forecast_rows = ml_training_forecast._build_future_forecast_rows(
                    frame=self._history_frame(counts),
                    selected_count_model_key='poisson',
                    count_model=self._count_model_bundle(raw_prediction),
                    event_model=None,
                    forecast_days=1,
                    scenario_temperature=None,
                    interval_calibration=interval_calibration,
                    baseline_expected_count=lambda train, target_date: 0.0,
                    temperature_stats={'usable': False},
                )
                self.assertEqual(len(forecast_rows), 1)
                self.assertTrue(np.isfinite(float(forecast_rows[0]['forecast_value'])))
                self.assertAlmostEqual(float(forecast_rows[0]['forecast_value']), expected)

    def test_run_backtest_keeps_large_sparse_candidate_predictions_finite_without_truncating_them_to_tiny_values(self) -> None:
        history_frame = _build_history_frame(self._daily_history_for_backtest())
        dataset = _build_backtest_seed_dataset(history_frame)

        def _fit_large_sparse_candidate(model_key: str, X_train: pd.DataFrame, y_train: np.ndarray):
            if model_key == 'negative_binomial':
                return None
            return {
                'key': model_key,
                'backend': 'sklearn',
                'model': self._ConstantPredictor(120.0),
                'columns': list(X_train.columns),
            }

        with (
            patch.object(ml_training_backtesting, '_fit_count_model_from_design', side_effect=_fit_large_sparse_candidate),
            patch.object(ml_training_backtesting, '_fit_event_model_from_design', return_value=None),
        ):
            result = _run_backtest(history_frame, dataset)

        self.assertTrue(result['is_ready'], msg=result.get('message'))
        poisson_predictions = [
            float(row['predictions']['poisson'])
            for row in result['window_rows']
            if row['predictions'].get('poisson') is not None
        ]
        self.assertTrue(poisson_predictions)
        self.assertTrue(np.isfinite(np.asarray(poisson_predictions, dtype=float)).all())
        self.assertGreaterEqual(max(poisson_predictions), 100.0)


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


    def test_marks_saturated_event_rate_as_uninformative_even_when_both_classes_exist(self) -> None:
        rows = []
        for index in range(100):
            actual_event = 0 if index % 20 == 0 else 1
            rows.append(
                {
                    'actual_event': actual_event,
                    'baseline_event_probability': 0.08 if actual_event == 0 else 0.92,
                    'heuristic_event_probability': 0.03 if actual_event == 0 else 0.97,
                    'predicted_event_probability': 0.04 if actual_event == 0 else 0.96,
                }
            )

        metrics = _compute_event_metrics(rows)

        self.assertTrue(metrics['available'])
        self.assertTrue(metrics['logistic_available'])
        self.assertFalse(metrics['event_probability_informative'])
        self.assertEqual(metrics['event_probability_reason_code'], 'saturated_event_rate')
        self.assertEqual(metrics['selected_model_key'], 'heuristic_probability')
        self.assertTrue(metrics['comparison_rows'])
        self.assertIn('95.0%', str(metrics['event_probability_note']))


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
        self.assertNotIn(encode_as_mojibake("\u0421"), text_blob)
        self.assertNotIn(encode_as_mojibake("\u0420"), text_blob)


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


class DesignMatrixShortcutTests(unittest.TestCase):
    def test_build_design_row_matches_full_design_matrix_encoding(self) -> None:
        reference_frame = pd.DataFrame(
            [
                {
                    'temp_value': 4.0,
                    'weekday': 0.0,
                    'month': 1.0,
                    'lag_1': 1.0,
                    'lag_7': 1.0,
                    'lag_14': 0.0,
                    'rolling_7': 1.2,
                    'rolling_28': 1.0,
                    'trend_gap': 0.2,
                },
                {
                    'temp_value': 8.0,
                    'weekday': 3.0,
                    'month': 4.0,
                    'lag_1': 2.0,
                    'lag_7': 1.5,
                    'lag_14': 1.0,
                    'rolling_7': 1.8,
                    'rolling_28': 1.3,
                    'trend_gap': 0.5,
                },
            ]
        )
        expected_columns = list(ml_training._build_design_matrix(reference_frame).columns)
        feature_row = reference_frame.iloc[1].to_dict()

        fast_row = _build_design_row(feature_row, expected_columns=expected_columns)
        full_row = ml_training._build_design_matrix(pd.DataFrame([feature_row]), expected_columns=expected_columns)

        pd.testing.assert_frame_equal(fast_row, full_row)


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

    def test_interval_backtest_reuses_identical_prefix_calibrations(self) -> None:
        actuals = np.asarray([2.0, 2.0, 3.0, 2.0, 3.0, 2.0, 9.0, 10.0, 11.0, 10.0, 12.0, 11.0], dtype=float)
        predictions = np.asarray([2.0, 2.0, 3.0, 2.0, 3.0, 2.0, 4.0, 4.5, 5.0, 5.0, 5.5, 5.5], dtype=float)
        window_dates = [date(2024, 1, 1) + timedelta(days=index) for index in range(len(actuals))]
        original_builder = ml_training_forecast._build_prediction_interval_calibration

        with patch.object(
            ml_training_forecast,
            '_build_prediction_interval_calibration',
            wraps=original_builder,
        ) as calibration_mock:
            result = _evaluate_prediction_interval_backtest(actuals, predictions, window_dates, level=0.8)

        split = ml_training_forecast._split_prediction_interval_windows(len(actuals))
        self.assertIsNotNone(split)
        calibration_windows, _ = split
        prefix_windows = {calibration_windows}
        prefix_windows.update(
            int(block[0])
            for block in ml_training_forecast._prediction_interval_validation_blocks(calibration_windows, len(actuals))
        )
        prefix_windows.update(range(calibration_windows, len(actuals)))

        self.assertTrue(result['coverage_validated'])
        self.assertEqual(calibration_mock.call_count, len(prefix_windows) + 1)
        self.assertIn('validation baseline', result['reference_candidate']['calibration']['method_label'])
        self.assertIn('validated by', result['calibration']['method_label'])

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


class LeadTimeAwareTrainingTests(unittest.TestCase):
    @staticmethod
    def _build_long_horizon_history(days: int = 180) -> list[dict[str, object]]:
        history = []
        current_date = date(2024, 1, 1)
        for index in range(days):
            if index < 60:
                count = float((1 if index % 6 == 0 else 0) + (1 if index % 17 == 0 else 0))
            elif index < 120:
                count = float(1 + (index % 4 == 0) + (2 if index % 19 == 0 else 0))
            else:
                count = float(2 + (index % 3 == 0) + (3 if index % 13 == 0 else 0))
            history.append(
                {
                    'date': current_date,
                    'count': count,
                    'avg_temperature': float(-10.0 + (index % 25)),
                }
            )
            current_date += timedelta(days=1)
        return history

    def test_run_backtest_exposes_horizon_specific_validation_for_7_14_30_days(self) -> None:
        history_frame = _build_history_frame(self._build_long_horizon_history())
        dataset = _build_backtest_seed_dataset(history_frame)
        result = _run_backtest(
            history_frame,
            dataset,
            validation_horizon_days=30,
            max_horizon_days=30,
        )

        self.assertTrue(result['is_ready'], msg=result.get('message'))
        overview = result['backtest_overview']
        calibration_map = result['prediction_interval_calibration_by_horizon']['by_horizon']

        self.assertEqual(int(overview['validation_horizon_days']), 30)
        self.assertEqual(int(overview['forecast_horizon_days']), 30)
        for horizon_day in (7, 14, 30):
            self.assertIn(horizon_day, calibration_map)
            self.assertIn(str(horizon_day), result['horizon_summaries'])
            self.assertIn(str(horizon_day), overview['prediction_interval_coverage_by_horizon'])
        self.assertGreaterEqual(
            float(calibration_map[30]['absolute_error_quantile']),
            float(calibration_map[7]['absolute_error_quantile']),
        )

    def test_lead_time_specific_calibration_is_wider_for_longer_horizons(self) -> None:
        history_frame = _build_history_frame(self._build_long_horizon_history())
        dataset = _build_backtest_seed_dataset(history_frame)
        result = _run_backtest(
            history_frame,
            dataset,
            validation_horizon_days=30,
            max_horizon_days=30,
        )

        self.assertTrue(result['is_ready'], msg=result.get('message'))
        calibration_map = result['prediction_interval_calibration_by_horizon']['by_horizon']
        width_1 = _count_interval(3.0, calibration_map[1])[1] - _count_interval(3.0, calibration_map[1])[0]
        width_7 = _count_interval(3.0, calibration_map[7])[1] - _count_interval(3.0, calibration_map[7])[0]
        width_14 = _count_interval(3.0, calibration_map[14])[1] - _count_interval(3.0, calibration_map[14])[0]
        width_30 = _count_interval(3.0, calibration_map[30])[1] - _count_interval(3.0, calibration_map[30])[0]

        self.assertGreaterEqual(width_7, width_1)
        self.assertGreaterEqual(width_14, width_7)
        self.assertGreaterEqual(width_30, width_14)


    def test_train_ml_model_keeps_horizon_metadata_consistent_for_1_7_14_30_without_temperature(self) -> None:
        daily_history = [{**row, 'avg_temperature': None} for row in self._build_long_horizon_history()]

        for forecast_days in (1, 7, 14, 30):
            with self.subTest(forecast_days=forecast_days):
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    result = ml_training._train_ml_model(daily_history, forecast_days, None)

                self.assertTrue(result['is_ready'], msg=result.get('message'))
                self.assertFalse(result['temperature_feature_enabled'])
                self.assertEqual(int(result['temperature_non_null_days']), 0)
                self.assertEqual(int(result['temperature_total_days']), len(daily_history))
                self.assertEqual(len(result['forecast_rows']), forecast_days)

                overview = result['backtest_overview']
                self.assertEqual(int(overview['validation_horizon_days']), forecast_days)
                self.assertEqual(int(overview['forecast_horizon_days']), forecast_days)

                expected_key_horizons = tuple(horizon for horizon in (1, 7, 14, 30) if horizon <= forecast_days)
                unexpected_key_horizons = tuple(horizon for horizon in (1, 7, 14, 30) if horizon > forecast_days)

                for horizon_day in expected_key_horizons:
                    summary = result['horizon_summaries'][str(horizon_day)]
                    row = result['forecast_rows'][horizon_day - 1]

                    self.assertEqual(int(summary['horizon_days']), horizon_day)
                    self.assertEqual(int(row['horizon_days']), horizon_day)
                    self.assertTrue(summary['prediction_interval_coverage_validated'])
                    self.assertIsNotNone(summary['prediction_interval_coverage'])
                    self.assertAlmostEqual(
                        float(overview['prediction_interval_coverage_by_horizon'][str(horizon_day)]),
                        float(summary['prediction_interval_coverage']),
                    )
                    self.assertEqual(
                        overview['prediction_interval_coverage_display_by_horizon'][str(horizon_day)],
                        summary['prediction_interval_coverage_display'],
                    )
                    self.assertAlmostEqual(
                        float(row['prediction_interval_coverage']),
                        float(summary['prediction_interval_coverage']),
                    )
                    self.assertEqual(
                        row['prediction_interval_coverage_display'],
                        summary['prediction_interval_coverage_display'],
                    )
                    self.assertEqual(
                        bool(row['prediction_interval_coverage_validated']),
                        bool(summary['prediction_interval_coverage_validated']),
                    )

                for horizon_day in unexpected_key_horizons:
                    self.assertNotIn(str(horizon_day), result['horizon_summaries'])
                    self.assertNotIn(str(horizon_day), overview['prediction_interval_coverage_by_horizon'])
                    self.assertNotIn(str(horizon_day), overview['prediction_interval_coverage_display_by_horizon'])

                selected_summary = result['horizon_summaries'][str(forecast_days)]
                self.assertTrue(overview['prediction_interval_coverage_validated'])
                self.assertIsNotNone(result['prediction_interval_coverage'])
                self.assertAlmostEqual(
                    float(result['prediction_interval_coverage']),
                    float(selected_summary['prediction_interval_coverage']),
                )
                self.assertEqual(
                    result['prediction_interval_coverage_display'],
                    selected_summary['prediction_interval_coverage_display'],
                )
                self.assertAlmostEqual(
                    float(overview['prediction_interval_coverage']),
                    float(selected_summary['prediction_interval_coverage']),
                )
                self.assertEqual(
                    overview['prediction_interval_coverage_display'],
                    selected_summary['prediction_interval_coverage_display'],
                )


class IntervalMetadataSyncTests(unittest.TestCase):
    @staticmethod
    def _build_window_rows(
        horizon_days: int,
        actuals: list[float],
        prediction: float = 1.0,
        model_key: str = 'poisson',
    ) -> list[BacktestWindowRow]:
        rows: list[BacktestWindowRow] = []
        current_date = date(2024, 1, 1)
        for actual in actuals:
            rows.append(
                BacktestWindowRow(
                    origin_date=current_date.isoformat(),
                    date=current_date.isoformat(),
                    horizon_days=horizon_days,
                    actual_count=float(actual),
                    baseline_count=float(prediction),
                    heuristic_count=float(prediction),
                    actual_event=0,
                    baseline_event_probability=None,
                    heuristic_event_probability=None,
                    predictions={model_key: float(prediction)},
                    predicted_event_probabilities={},
                )
            )
            current_date += timedelta(days=1)
        return rows

    @staticmethod
    def _build_calibration(
        *,
        absolute_error_quantile: float,
        validated_coverage: float,
    ) -> dict[str, object]:
        return {
            'level': 0.8,
            'level_display': '80%',
            'absolute_error_quantile': float(absolute_error_quantile),
            'residual_count': 10,
            'adaptive_binning_strategy': 'global_absolute_error_quantile',
            'adaptive_bin_count': 0,
            'adaptive_bin_edges': [],
            'adaptive_bins': [],
            'method_label': 'Adaptive conformal interval with predicted-count bins; validated by Forward rolling split conformal',
            'coverage_validated': True,
            'validated_coverage': float(validated_coverage),
            'coverage_note': (
                'Coverage is evaluated only on later 4 windows after an initial calibration prefix of first 6 windows. '
                'Deployment intervals are recalibrated on all available rolling-origin residuals after validation.'
            ),
            'calibration_window_count': 6,
            'evaluation_window_count': 4,
            'calibration_window_range_label': 'first 6 windows',
            'evaluation_window_range_label': 'later 4 windows',
            'validation_scheme_key': 'rolling_split_conformal',
            'validation_scheme_label': 'Forward rolling split conformal',
            'validation_scheme_explanation': 'Forward rolling split conformal was selected.',
        }

    def test_monotonic_widening_remeasures_coverage_and_keeps_forecast_metadata_in_sync(self) -> None:
        horizon_rows = {
            7: self._build_window_rows(7, [1.0] * 10),
            14: self._build_window_rows(14, [1.0] * 8 + [6.0, 6.0]),
        }
        horizon_summaries = {
            '7': HorizonSummary(
                horizon_days=7,
                horizon_label='7 days',
                folds=10,
                count_metrics=CountMetrics(mae=0.0),
                prediction_interval_coverage=0.75,
                prediction_interval_coverage_display='75%',
                prediction_interval_coverage_validated=True,
                prediction_interval_coverage_note='stale',
                prediction_interval_validation_scheme_key='rolling_split_conformal',
                prediction_interval_validation_scheme_label='Forward rolling split conformal',
                prediction_interval_method_label='Adaptive conformal interval',
            ),
            '14': HorizonSummary(
                horizon_days=14,
                horizon_label='14 days',
                folds=10,
                count_metrics=CountMetrics(mae=0.0),
                prediction_interval_coverage=0.5,
                prediction_interval_coverage_display='50%',
                prediction_interval_coverage_validated=True,
                prediction_interval_coverage_note='stale',
                prediction_interval_validation_scheme_key='rolling_split_conformal',
                prediction_interval_validation_scheme_label='Forward rolling split conformal',
                prediction_interval_method_label='Adaptive conformal interval',
            ),
        }
        calibrations = {
            7: self._build_calibration(absolute_error_quantile=5.0, validated_coverage=0.75),
            14: self._build_calibration(absolute_error_quantile=1.0, validated_coverage=0.5),
        }

        widened = ml_training_backtesting._enforce_monotonic_horizon_interval_calibrations(calibrations)
        self.assertTrue(widened[14]['monotone_horizon_adjusted'])
        self.assertAlmostEqual(float(widened[14]['validated_coverage']), 0.5)

        reconciled_calibrations, reconciled_summaries = ml_training_backtesting._reconcile_horizon_interval_metadata(
            widened,
            horizon_rows,
            horizon_summaries,
            selected_count_model_key='poisson',
        )

        self.assertAlmostEqual(float(reconciled_calibrations[14]['validated_coverage_reference']), 0.5)
        self.assertAlmostEqual(float(reconciled_calibrations[14]['validated_coverage']), 1.0)
        self.assertEqual(
            reconciled_calibrations[14]['validated_coverage_scope'],
            'deployed_interval_remeasured_after_monotonic_horizon_widening',
        )
        self.assertIn(
            'deployed interval is remeasured on the same later evaluation windows after monotonic horizon widening',
            str(reconciled_calibrations[14]['coverage_note']).lower(),
        )
        self.assertAlmostEqual(float(reconciled_summaries['14'].prediction_interval_coverage), 1.0)

        frame = pd.DataFrame(
            [
                {
                    'date': pd.Timestamp(date(2024, 1, 1) + timedelta(days=index)),
                    'count': 1.0,
                    'avg_temperature': None,
                }
                for index in range(40)
            ]
        )
        forecast_rows = ml_training_forecast._build_future_forecast_rows(
            frame=frame,
            selected_count_model_key='seasonal_baseline',
            count_model=None,
            event_model=None,
            forecast_days=14,
            scenario_temperature=None,
            interval_calibration=reconciled_calibrations,
            baseline_expected_count=lambda train, target_date: 1.0,
            temperature_stats={'usable': False},
        )

        self.assertAlmostEqual(float(forecast_rows[13]['prediction_interval_coverage']), 1.0)
        self.assertAlmostEqual(
            float(forecast_rows[13]['prediction_interval_coverage']),
            float(reconciled_summaries['14'].prediction_interval_coverage),
        )


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

    def test_backtest_coverage_metadata_is_remeasured_on_deployed_interval_evaluation_slice(self) -> None:
        result = self._run_temperature_backtest(self._build_daily_history())
        overview = result['backtest_overview']
        calibration = result['prediction_interval_calibration']

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
        evaluation_start = int(calibration['calibration_window_count'])
        evaluation_end = evaluation_start + int(calibration['evaluation_window_count'])
        evaluation_coverage = _interval_coverage(
            actuals[evaluation_start:evaluation_end],
            predictions[evaluation_start:evaluation_end],
            calibration,
        )

        self.assertIsNotNone(overview['prediction_interval_coverage'])
        self.assertIsNotNone(evaluation_coverage)
        self.assertEqual(calibration['validated_coverage_scope'], 'deployed_interval_remeasured')
        self.assertIn('deployed interval is remeasured', str(overview['prediction_interval_coverage_note']).lower())
        self.assertAlmostEqual(float(overview['prediction_interval_coverage']), float(evaluation_coverage), places=6)


class TrainingGuardrailTests(unittest.TestCase):
    def test_train_ml_model_returns_not_ready_before_backtest_when_history_is_too_short(self) -> None:
        daily_history = []
        current_date = date(2024, 1, 1)
        for index in range(ml_training.MIN_DAILY_HISTORY - 1):
            daily_history.append(
                {
                    'date': current_date,
                    'count': float(1 if index % 3 == 0 else 0),
                    'avg_temperature': float((index % 9) - 4),
                }
            )
            current_date += timedelta(days=1)

        result = ml_training._train_ml_model(daily_history, 7, None)
        overview = result['backtest_overview']

        self.assertFalse(result['is_ready'])
        self.assertEqual(result['forecast_rows'], [])
        self.assertEqual(result['horizon_summaries'], {})
        self.assertEqual(int(overview['folds']), 0)
        self.assertEqual(int(overview['candidate_window_count']), 0)
        self.assertFalse(overview['prediction_interval_coverage_validated'])
        self.assertEqual(overview['prediction_interval_validation_scheme_key'], 'not_validated')
        self.assertIn(str(ml_training.MIN_DAILY_HISTORY), str(result['message']))
        self.assertIn('rolling-origin backtesting', str(result['message']))


class ExplainabilityFallbackTests(unittest.TestCase):
    @staticmethod
    def _build_daily_history(days: int = 120) -> list[dict[str, object]]:
        history = []
        current_date = date(2024, 1, 1)
        for index in range(days):
            history.append(
                {
                    'date': current_date,
                    'count': float((1 if index % 4 == 0 else 0) + (1 if index % 7 == 0 else 0)),
                    'avg_temperature': float((index % 15) - 5),
                }
            )
            current_date += timedelta(days=1)
        return history

    def test_train_ml_model_keeps_explainability_when_working_method_is_seasonal_baseline(self) -> None:
        daily_history = self._build_daily_history()
        frame = _build_history_frame(daily_history)
        dataset = _build_backtest_seed_dataset(frame)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            backtest = _run_backtest(frame.copy(), dataset)

        forced_backtest = dict(backtest)
        forced_backtest['selected_count_model_key'] = 'seasonal_baseline'
        forced_backtest['selected_metrics'] = dict(backtest['baseline_metrics'])
        forced_backtest['selected_count_model_reason_short'] = 'Рабочим методом оставлен seasonal baseline.'
        forced_backtest['selected_count_model_reason'] = (
            'Seasonal baseline оставлен рабочим методом, но explainability всё равно должна остаться доступной.'
        )

        with patch.object(ml_training, '_run_backtest', return_value=forced_backtest):
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                result = ml_training._train_ml_model(daily_history, 7, None)

        self.assertTrue(result['is_ready'], msg=result.get('message'))
        self.assertEqual(result['selected_count_model_key'], 'seasonal_baseline')
        self.assertTrue(result['feature_importance'])
        self.assertEqual(result['feature_importance_source_key'], 'poisson')
        self.assertEqual(result['feature_importance_source_label'], 'Регрессия Пуассона')
        self.assertIn('Рабочий метод прогноза:', str(result['feature_importance_note']))
