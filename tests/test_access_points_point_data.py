import unittest
from datetime import date

from app.services.access_points.point_data import _build_point_entity_frames


class AccessPointsPointDataTests(unittest.TestCase):
    def test_granular_identity_falls_back_from_address_to_settlement_to_territory_and_district(self) -> None:
        records = [
            {
                "date": date(2025, 1, 1),
                "year": 2025,
                "source_table": "fires",
                "district": "Район А",
                "territory_label": "Территория А",
                "settlement": "Село А",
                "settlement_type": "село",
                "object_category": "Жилой сектор",
                "address": "ул. Центральная, 1",
                "address_comment": "",
                "object_name": "жилой дом",
                "latitude": None,
                "longitude": None,
                "fire_station_distance": 10.0,
                "has_water_supply": False,
                "response_minutes": 25.0,
                "long_arrival": True,
                "heating_season": True,
                "night_incident": False,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": False,
            },
            {
                "date": date(2025, 1, 2),
                "year": 2025,
                "source_table": "fires",
                "district": "Район Б",
                "territory_label": "Территория Б",
                "settlement": "Деревня Б",
                "settlement_type": "деревня",
                "object_category": "",
                "address": "",
                "address_comment": "",
                "object_name": "",
                "latitude": None,
                "longitude": None,
                "fire_station_distance": 12.0,
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
                "date": date(2025, 1, 3),
                "year": 2025,
                "source_table": "fires",
                "district": "Район В",
                "territory_label": "Территория В",
                "settlement": "",
                "settlement_type": "",
                "object_category": "",
                "address": "",
                "address_comment": "",
                "object_name": "",
                "latitude": None,
                "longitude": None,
                "fire_station_distance": None,
                "has_water_supply": None,
                "response_minutes": None,
                "long_arrival": False,
                "heating_season": False,
                "night_incident": False,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": False,
            },
            {
                "date": date(2025, 1, 4),
                "year": 2025,
                "source_table": "fires",
                "district": "Район Г",
                "territory_label": "",
                "settlement": "",
                "settlement_type": "",
                "object_category": "",
                "address": "",
                "address_comment": "",
                "object_name": "",
                "latitude": None,
                "longitude": None,
                "fire_station_distance": None,
                "has_water_supply": None,
                "response_minutes": None,
                "long_arrival": False,
                "heating_season": False,
                "night_incident": False,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": False,
            },
        ]

        entity_frame, feature_frame = _build_point_entity_frames(records)

        self.assertEqual(len(entity_frame), 4)
        self.assertEqual(entity_frame.loc[0, "entity_code"], "address")
        self.assertEqual(entity_frame.loc[1, "entity_code"], "settlement")
        self.assertEqual(entity_frame.loc[2, "entity_code"], "territory")
        self.assertEqual(entity_frame.loc[3, "entity_code"], "district")
        self.assertIn("average_response_minutes", feature_frame.columns)
        self.assertIn("no_water_share", feature_frame.columns)

    def test_generic_object_name_does_not_split_same_address(self) -> None:
        base_record = {
            "year": 2025,
            "source_table": "fires",
            "district": "Район А",
            "territory_label": "Село А",
            "settlement": "Село А",
            "settlement_type": "село",
            "object_category": "Жилой сектор",
            "address": "ул. Центральная, 1",
            "address_comment": "",
            "latitude": 56.1,
            "longitude": 92.1,
            "fire_station_distance": 10.0,
            "has_water_supply": False,
            "response_minutes": 25.0,
            "long_arrival": True,
            "heating_season": True,
            "night_incident": False,
            "victims_present": False,
            "major_damage": False,
            "severe_consequence": False,
        }
        records = [
            {**base_record, "date": date(2025, 1, 1), "object_name": "жилой дом"},
            {**base_record, "date": date(2025, 1, 2), "object_name": ""},
        ]

        entity_frame, _feature_frame = _build_point_entity_frames(records)

        self.assertEqual(len(entity_frame), 1)
        self.assertEqual(entity_frame.loc[0, "entity_code"], "address")
        self.assertEqual(int(entity_frame.loc[0, "incident_count"]), 2)

    def test_invalid_coordinate_identity_falls_back_to_settlement(self) -> None:
        records = [
            {
                "date": date(2025, 1, 1),
                "year": 2025,
                "source_table": "fires",
                "district": "Район А",
                "territory_label": "Село А",
                "settlement": "Село А",
                "settlement_type": "село",
                "object_category": "",
                "address": "",
                "address_comment": "",
                "object_name": "",
                "latitude": float("nan"),
                "longitude": 92.1,
                "fire_station_distance": None,
                "has_water_supply": None,
                "response_minutes": None,
                "long_arrival": False,
                "heating_season": False,
                "night_incident": False,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": False,
            }
        ]

        entity_frame, _feature_frame = _build_point_entity_frames(records)

        self.assertEqual(len(entity_frame), 1)
        self.assertEqual(entity_frame.loc[0, "entity_code"], "settlement")
        self.assertIsNone(entity_frame.loc[0, "latitude"])
        self.assertIsNone(entity_frame.loc[0, "longitude"])

    def test_low_support_points_are_flagged_and_fractional_features_are_smoothed(self) -> None:
        records = [
            {
                "date": date(2025, 2, 1),
                "year": 2025,
                "source_table": "fires",
                "district": "Район А",
                "territory_label": "Село Тест",
                "settlement": "Село Тест",
                "settlement_type": "село",
                "object_category": "Жилой сектор",
                "address": "ул. Лесная, 1",
                "address_comment": "",
                "object_name": "",
                "latitude": None,
                "longitude": None,
                "fire_station_distance": 18.0,
                "has_water_supply": False,
                "response_minutes": 28.0,
                "long_arrival": True,
                "heating_season": True,
                "night_incident": True,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": True,
            },
            {
                "date": date(2025, 2, 2),
                "year": 2025,
                "source_table": "fires",
                "district": "Район Б",
                "territory_label": "Город База",
                "settlement": "Город База",
                "settlement_type": "город",
                "object_category": "Склад",
                "address": "ул. Заводская, 2",
                "address_comment": "",
                "object_name": "",
                "latitude": None,
                "longitude": None,
                "fire_station_distance": 2.0,
                "has_water_supply": True,
                "response_minutes": 8.0,
                "long_arrival": False,
                "heating_season": False,
                "night_incident": False,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": False,
            },
            {
                "date": date(2025, 2, 3),
                "year": 2025,
                "source_table": "fires",
                "district": "Район Б",
                "territory_label": "Город База",
                "settlement": "Город База",
                "settlement_type": "город",
                "object_category": "Склад",
                "address": "ул. Заводская, 2",
                "address_comment": "",
                "object_name": "",
                "latitude": None,
                "longitude": None,
                "fire_station_distance": 2.5,
                "has_water_supply": True,
                "response_minutes": 9.0,
                "long_arrival": False,
                "heating_season": False,
                "night_incident": False,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": False,
            },
            {
                "date": date(2025, 2, 4),
                "year": 2025,
                "source_table": "fires",
                "district": "Район Б",
                "territory_label": "Город База",
                "settlement": "Город База",
                "settlement_type": "город",
                "object_category": "Склад",
                "address": "ул. Заводская, 2",
                "address_comment": "",
                "object_name": "",
                "latitude": None,
                "longitude": None,
                "fire_station_distance": 3.0,
                "has_water_supply": True,
                "response_minutes": 10.0,
                "long_arrival": False,
                "heating_season": False,
                "night_incident": False,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": False,
            },
        ]

        entity_frame, _feature_frame = _build_point_entity_frames(records)
        low_support_row = entity_frame.loc[entity_frame["label"] == "ул. Лесная, 1"].iloc[0]

        self.assertTrue(bool(low_support_row["low_support"]))
        self.assertEqual(int(low_support_row["minimum_support"]), 3)
        self.assertGreater(float(low_support_row["long_arrival_share"]), 0.0)
        self.assertLess(float(low_support_row["long_arrival_share"]), 1.0)
        self.assertGreater(float(low_support_row["no_water_share"]), 0.0)
        self.assertLess(float(low_support_row["no_water_share"]), 1.0)


if __name__ == "__main__":
    unittest.main()
