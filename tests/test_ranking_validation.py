import unittest
from datetime import date

from app.services.forecast_risk.scoring import _build_territory_rows
from app.services.forecast_risk.validation import _evaluate_ranking_window


class RankingValidationTests(unittest.TestCase):
    def test_ranking_window_reports_precision_and_ndcg(self) -> None:
        metrics = _evaluate_ranking_window(
            predicted_rows=[
                {'label': 'Территория А'},
                {'label': 'Территория Б'},
                {'label': 'Территория В'},
            ],
            future_records=[
                {'territory_label': 'Территория А'},
                {'territory_label': 'Территория Б'},
                {'territory_label': 'Территория Б'},
            ],
            ranking_k=2,
            cutoff=date(2025, 1, 31),
        )

        self.assertIsNotNone(metrics)
        self.assertEqual(metrics['top1_hit'], 1.0)
        self.assertEqual(metrics['topk_capture'], 1.0)
        self.assertEqual(metrics['precision_at_k'], 1.0)
        self.assertAlmostEqual(metrics['ndcg_at_k'], 0.797, places=3)

    def test_territory_rows_expose_explainable_logistics_fields(self) -> None:
        records = [
            {
                'date': date(2025, 1, 5),
                'territory_label': 'Северный участок',
                'district': 'Район А',
                'response_minutes': 18.0,
                'long_arrival': False,
                'fire_station_distance': 7.0,
                'has_water_supply': True,
                'severe_consequence': False,
                'victims_present': False,
                'major_damage': False,
                'night_incident': False,
                'heating_season': True,
                'risk_category_score': 0.28,
                'cause': 'Электрика',
                'object_category': 'Жилой сектор',
                'settlement_type': 'село',
            },
            {
                'date': date(2025, 2, 11),
                'territory_label': 'Северный участок',
                'district': 'Район А',
                'response_minutes': 24.0,
                'long_arrival': True,
                'fire_station_distance': 16.0,
                'has_water_supply': False,
                'severe_consequence': True,
                'victims_present': False,
                'major_damage': True,
                'night_incident': True,
                'heating_season': True,
                'risk_category_score': 0.54,
                'cause': 'Печь',
                'object_category': 'Жилой сектор',
                'settlement_type': 'село',
            },
            {
                'date': date(2025, 2, 20),
                'territory_label': 'Центральный сектор',
                'district': 'Район А',
                'response_minutes': 11.0,
                'long_arrival': False,
                'fire_station_distance': 3.5,
                'has_water_supply': True,
                'severe_consequence': False,
                'victims_present': False,
                'major_damage': False,
                'night_incident': False,
                'heating_season': True,
                'risk_category_score': 0.20,
                'cause': 'Неосторожность',
                'object_category': 'Склад',
                'settlement_type': 'город',
            },
        ]

        rows = _build_territory_rows(records, planning_horizon_days=14, weight_mode='expert')
        self.assertTrue(rows)
        north_row = next(row for row in rows if row['label'] == 'Северный участок')

        for key in (
            'travel_time_display',
            'travel_time_source',
            'fire_station_coverage_display',
            'fire_station_coverage_label',
            'service_zone_label',
            'service_zone_reason',
            'logistics_priority_display',
            'logistics_priority_label',
        ):
            self.assertIn(key, north_row)
            self.assertTrue(str(north_row[key]))


if __name__ == '__main__':
    unittest.main()