import unittest
from datetime import date

from app.services.ml_model.presentation import _build_quality_assessment, _build_summary

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
        self.assertEqual(summary['average_event_probability_display'], '—')
        self.assertEqual(summary['peak_event_probability_display'], '—')
        self.assertEqual(summary['peak_event_probability_day_display'], '—')


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
        self.assertEqual(summary['average_event_probability_display'], '—')
        self.assertEqual(summary['peak_event_probability_display'], '—')
        self.assertEqual(summary['peak_event_probability_day_display'], '—')


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
        self.assertEqual(quality['interval_card']['label'], 'Покрытие интервала на отложенных окнах')
        self.assertEqual(quality['interval_card']['value'], '—')
        self.assertIn('Покрытие на отложенных окнах пока не показывается', quality['interval_card']['meta'])
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
            ],
        )
        self.assertTrue(all(card['value'] == '—' for card in quality['metric_cards']))
        self.assertEqual(quality['interval_card']['label'], 'Покрытие интервала на отложенных окнах')
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
        self.assertEqual(heuristic_row['mae_display'], '—')
        self.assertEqual(heuristic_row['rmse_display'], '—')
        self.assertEqual(heuristic_row['smape_display'], '—')
        self.assertEqual(heuristic_row['poisson_display'], '—')
        self.assertEqual(heuristic_row['mae_delta_display'], '—')

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

        interval_card = quality['interval_card']
        interval_item = next(item for item in quality['methodology_items'] if item['label'] == 'Интервал прогноза')

        self.assertEqual(interval_card['value'], '—')
        self.assertIn('Покрытие на отложенных окнах пока', interval_card['meta'])
        self.assertIn('Покрытие на отложенных окнах пока', interval_item['meta'])

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
                    'candidate_model_labels': ['Poisson GLM', 'Negative Binomial GLM'],
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
        self.assertIn('скользящая проверка по истории', interval_item['meta'])

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
                    'candidate_model_labels': ['Poisson GLM', 'Negative Binomial GLM'],
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
        self.assertEqual(quality['interval_card']['label'], 'Покрытие 80% интервала на отложенных окнах')
        self.assertTrue(any('count' in label.lower() for label in methodology_labels))
        self.assertTrue(quality['model_choice']['title'])
        self.assertTrue(quality['model_choice']['facts'])
        self.assertEqual(quality['model_choice']['facts'][0]['value'], 'Poisson GLM')


if __name__ == '__main__':
    unittest.main()
