import unittest

from app.services.explainable_logistics import build_explainable_logistics_profile


class ExplainableLogisticsTests(unittest.TestCase):
    def test_combines_observed_response_and_distance_into_core_service_zone(self) -> None:
        profile = build_explainable_logistics_profile(
            avg_distance_km=8.0,
            avg_response_minutes=14.0,
            long_arrival_rate=0.10,
            is_rural=False,
            response_observations=5,
            distance_observations=5,
        )

        self.assertEqual(profile['travel_time_source'], 'Факт прибытия + модель по расстоянию')
        self.assertEqual(profile['service_zone_label'], 'Ядро зоны обслуживания')
        self.assertEqual(profile['fire_station_coverage_label'], 'Устойчивое прикрытие')
        self.assertLess(profile['logistics_priority_score'], 35.0)

    def test_fallback_path_preserves_explainable_outputs_for_rural_area(self) -> None:
        profile = build_explainable_logistics_profile(
            avg_distance_km=None,
            avg_response_minutes=None,
            long_arrival_rate=0.40,
            is_rural=True,
        )

        self.assertEqual(profile['travel_time_source'], 'Осторожный fallback без прямой логистики')
        self.assertEqual(profile['service_zone_label'], 'Зона напряженного доезда')
        self.assertEqual(profile['fire_station_coverage_label'], 'Напряженное прикрытие')
        self.assertGreaterEqual(profile['logistics_priority_score'], 50.0)


if __name__ == '__main__':
    unittest.main()