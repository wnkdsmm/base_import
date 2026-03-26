import unittest

from app.services.ml_model.presentation import _build_quality_assessment
from app.services.ml_model.training import _compute_event_metrics, _select_count_model


class CountModelSelectionTests(unittest.TestCase):
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


class QualityAssessmentPresentationTests(unittest.TestCase):
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
            },
        }

        quality = _build_quality_assessment(ml_result)

        metric_labels = [item['label'] for item in quality['metric_cards']]
        methodology_labels = [item['label'] for item in quality['methodology_items']]

        self.assertIn('Poisson deviance', metric_labels)
        self.assertTrue(any('count' in label.lower() for label in methodology_labels))
        self.assertTrue(quality['model_choice']['title'])
        self.assertTrue(quality['model_choice']['facts'])
        self.assertEqual(quality['model_choice']['facts'][0]['value'], 'Poisson GLM')


if __name__ == '__main__':
    unittest.main()
