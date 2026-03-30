import unittest
from datetime import date

from app.services.access_points.analysis import _build_access_point_rows, _select_incomplete_points


class AccessPointsScoringTests(unittest.TestCase):
    def test_problem_point_ranking_prefers_locations_with_poor_access(self) -> None:
        records = [
            {
                "date": date(2025, 1, 12),
                "year": 2025,
                "source_table": "fires",
                "district": "Район А",
                "territory_label": "с. Березовка",
                "settlement_type": "село",
                "object_category": "Жилой сектор",
                "address": "ул. Центральная, 1",
                "address_comment": "",
                "object_name": "жилой дом",
                "latitude": 56.123,
                "longitude": 92.456,
                "fire_station_distance": 18.0,
                "has_water_supply": False,
                "response_minutes": 31.0,
                "long_arrival": True,
                "heating_season": True,
                "night_incident": True,
                "victims_present": False,
                "major_damage": True,
                "severe_consequence": True,
            },
            {
                "date": date(2025, 2, 3),
                "year": 2025,
                "source_table": "fires",
                "district": "Район А",
                "territory_label": "с. Березовка",
                "settlement_type": "село",
                "object_category": "Жилой сектор",
                "address": "ул. Центральная, 1",
                "address_comment": "",
                "object_name": "жилой дом",
                "latitude": 56.1232,
                "longitude": 92.4561,
                "fire_station_distance": 20.0,
                "has_water_supply": False,
                "response_minutes": 28.0,
                "long_arrival": True,
                "heating_season": True,
                "night_incident": False,
                "victims_present": True,
                "major_damage": True,
                "severe_consequence": True,
            },
            {
                "date": date(2025, 2, 10),
                "year": 2025,
                "source_table": "fires",
                "district": "Район Б",
                "territory_label": "г. Центр",
                "settlement_type": "город",
                "object_category": "Склад",
                "address": "ул. Заводская, 7",
                "address_comment": "",
                "object_name": "склад",
                "latitude": 56.01,
                "longitude": 92.01,
                "fire_station_distance": 2.5,
                "has_water_supply": True,
                "response_minutes": 9.0,
                "long_arrival": False,
                "heating_season": True,
                "night_incident": False,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": False,
            },
        ]

        rows = _build_access_point_rows(records)
        self.assertEqual(len(rows), 2)
        self.assertIn("Центральная", rows[0]["label"])
        self.assertGreater(rows[0]["score"], rows[1]["score"])
        self.assertTrue(rows[0]["reasons"])
        self.assertIn(rows[0]["typology_label"], {"Дальний выезд", "Дефицит воды", "Тяжёлые последствия", "Комбинированный риск"})

    def test_incomplete_points_are_marked_for_review(self) -> None:
        records = [
            {
                "date": date(2025, 1, 5),
                "year": 2025,
                "source_table": "fires",
                "district": "Район С",
                "territory_label": "д. Лесная",
                "settlement_type": "деревня",
                "object_category": "Жилой сектор",
                "address": "",
                "address_comment": "",
                "object_name": "",
                "latitude": None,
                "longitude": None,
                "fire_station_distance": None,
                "has_water_supply": None,
                "response_minutes": None,
                "long_arrival": False,
                "heating_season": True,
                "night_incident": True,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": False,
            },
            {
                "date": date(2025, 2, 5),
                "year": 2025,
                "source_table": "fires",
                "district": "Район С",
                "territory_label": "д. Лесная",
                "settlement_type": "деревня",
                "object_category": "Жилой сектор",
                "address": "",
                "address_comment": "",
                "object_name": "",
                "latitude": None,
                "longitude": None,
                "fire_station_distance": None,
                "has_water_supply": None,
                "response_minutes": None,
                "long_arrival": False,
                "heating_season": True,
                "night_incident": False,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": False,
            },
            {
                "date": date(2025, 3, 5),
                "year": 2025,
                "source_table": "fires",
                "district": "Район С",
                "territory_label": "д. Лесная",
                "settlement_type": "деревня",
                "object_category": "Жилой сектор",
                "address": "",
                "address_comment": "",
                "object_name": "",
                "latitude": None,
                "longitude": None,
                "fire_station_distance": None,
                "has_water_supply": None,
                "response_minutes": None,
                "long_arrival": False,
                "heating_season": True,
                "night_incident": True,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": False,
            },
        ]

        rows = _build_access_point_rows(records)
        incomplete = _select_incomplete_points(rows)

        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["missing_data_priority"])
        self.assertTrue(incomplete)
        self.assertIn("пропуск", rows[0]["incomplete_note"].lower())


if __name__ == "__main__":
    unittest.main()
