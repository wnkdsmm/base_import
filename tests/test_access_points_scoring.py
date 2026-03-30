import unittest
from datetime import date

import pandas as pd

from app.services.access_points.analysis import _build_access_point_rows, _select_incomplete_points
from app.services.access_points.features import _build_access_point_candidate_features


class AccessPointsScoringTests(unittest.TestCase):
    def test_candidate_features_are_built_for_picker(self) -> None:
        rows = _build_access_point_rows(
            [
                {
                    "date": date(2025, 1, 12),
                    "year": 2025,
                    "source_table": "fires",
                    "district": "District F",
                    "territory_label": "Village F",
                    "settlement": "Village F",
                    "settlement_type": "village",
                    "object_category": "Residential",
                    "address": "Field 1",
                    "address_comment": "",
                    "object_name": "house",
                    "latitude": 56.1,
                    "longitude": 92.1,
                    "fire_station_distance": 12.0,
                    "has_water_supply": False,
                    "response_minutes": 22.0,
                    "long_arrival": True,
                    "heating_season": True,
                    "night_incident": False,
                    "victims_present": False,
                    "major_damage": False,
                    "severe_consequence": False,
                },
                {
                    "date": date(2025, 2, 12),
                    "year": 2025,
                    "source_table": "fires",
                    "district": "District F",
                    "territory_label": "Village F",
                    "settlement": "Village F",
                    "settlement_type": "village",
                    "object_category": "Residential",
                    "address": "Field 1",
                    "address_comment": "",
                    "object_name": "house",
                    "latitude": 56.12,
                    "longitude": 92.11,
                    "fire_station_distance": 16.0,
                    "has_water_supply": True,
                    "response_minutes": 28.0,
                    "long_arrival": True,
                    "heating_season": False,
                    "night_incident": True,
                    "victims_present": False,
                    "major_damage": True,
                    "severe_consequence": True,
                },
                {
                    "date": date(2025, 3, 12),
                    "year": 2025,
                    "source_table": "fires",
                    "district": "District F",
                    "territory_label": "Village F",
                    "settlement": "Village F",
                    "settlement_type": "village",
                    "object_category": "Residential",
                    "address": "Field 1",
                    "address_comment": "",
                    "object_name": "house",
                    "latitude": 56.13,
                    "longitude": 92.12,
                    "fire_station_distance": 14.0,
                    "has_water_supply": False,
                    "response_minutes": 24.0,
                    "long_arrival": False,
                    "heating_season": True,
                    "night_incident": False,
                    "victims_present": True,
                    "major_damage": False,
                    "severe_consequence": True,
                },
            ]
        )

        features = _build_access_point_candidate_features(pd.DataFrame(rows))

        self.assertTrue(features)
        self.assertEqual(features[0]["coverage_display"], "100%")
        self.assertIn("variance_display", features[0])

    def test_problem_point_ranking_prefers_locations_with_poor_access(self) -> None:
        records = [
            {
                "date": date(2025, 1, 12),
                "year": 2025,
                "source_table": "fires",
                "district": "District A",
                "territory_label": "Village Berezovka",
                "settlement": "Village Berezovka",
                "settlement_type": "village",
                "object_category": "Residential",
                "address": "Centralnaya 1",
                "address_comment": "",
                "object_name": "house",
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
                "district": "District A",
                "territory_label": "Village Berezovka",
                "settlement": "Village Berezovka",
                "settlement_type": "village",
                "object_category": "Residential",
                "address": "Centralnaya 1",
                "address_comment": "",
                "object_name": "house",
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
                "district": "District B",
                "territory_label": "City Center",
                "settlement": "City Center",
                "settlement_type": "city",
                "object_category": "Warehouse",
                "address": "Factory 7",
                "address_comment": "",
                "object_name": "warehouse",
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
        self.assertIn("Centralnaya", rows[0]["label"])
        self.assertGreater(rows[0]["total_score"], rows[1]["total_score"])
        self.assertTrue(rows[0]["reasons"])
        self.assertIn("total_score", rows[0])
        self.assertIn("severity_band", rows[0])
        self.assertTrue(rows[0]["top_reason_codes"])
        self.assertTrue(rows[0]["reason_details"])
        self.assertTrue(rows[0]["human_readable_explanation"])
        self.assertIn(rows[0]["typology_code"], {"access", "water", "severity", "recurrence", "mixed"})

    def test_incomplete_points_are_marked_for_review(self) -> None:
        records = [
            {
                "date": date(2025, 1, 5),
                "year": 2025,
                "source_table": "fires",
                "district": "District C",
                "territory_label": "Forest Village",
                "settlement": "Forest Village",
                "settlement_type": "village",
                "object_category": "Residential",
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
                "district": "District C",
                "territory_label": "Forest Village",
                "settlement": "Forest Village",
                "settlement_type": "village",
                "object_category": "Residential",
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
                "district": "District C",
                "territory_label": "Forest Village",
                "settlement": "Forest Village",
                "settlement_type": "village",
                "object_category": "Residential",
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
        self.assertTrue(rows[0]["uncertainty_flag"])
        self.assertTrue(incomplete)
        self.assertTrue(rows[0]["incomplete_note"])

    def test_single_incident_low_support_point_is_smoothed_and_not_critical(self) -> None:
        records = [
            {
                "date": date(2025, 1, 12),
                "year": 2025,
                "source_table": "fires",
                "district": "District T",
                "territory_label": "Sector T",
                "settlement": "Sector T",
                "settlement_type": "village",
                "object_category": "Residential",
                "address": "Testovaya 5",
                "address_comment": "",
                "object_name": "house",
                "latitude": None,
                "longitude": None,
                "fire_station_distance": 25.0,
                "has_water_supply": False,
                "response_minutes": 40.0,
                "long_arrival": True,
                "heating_season": True,
                "night_incident": True,
                "victims_present": True,
                "major_damage": True,
                "severe_consequence": True,
            }
        ]

        rows = _build_access_point_rows(records)

        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["low_support"])
        self.assertLess(float(rows[0]["total_score"]), 75.0)
        self.assertNotEqual(rows[0]["severity_band_code"], "critical")

    def test_selected_features_limit_scoring_drivers(self) -> None:
        records = [
            {
                "date": date(2025, 1, 12),
                "year": 2025,
                "source_table": "fires",
                "district": "District S",
                "territory_label": "Sector S",
                "settlement": "Sector S",
                "settlement_type": "village",
                "object_category": "Residential",
                "address": "Signalnaya 7",
                "address_comment": "",
                "object_name": "house",
                "latitude": 56.2,
                "longitude": 92.5,
                "fire_station_distance": 19.0,
                "has_water_supply": False,
                "response_minutes": 29.0,
                "long_arrival": True,
                "heating_season": True,
                "night_incident": True,
                "victims_present": False,
                "major_damage": True,
                "severe_consequence": True,
            },
            {
                "date": date(2025, 2, 10),
                "year": 2025,
                "source_table": "fires",
                "district": "District S",
                "territory_label": "Sector S",
                "settlement": "Sector S",
                "settlement_type": "village",
                "object_category": "Residential",
                "address": "Signalnaya 7",
                "address_comment": "",
                "object_name": "house",
                "latitude": 56.21,
                "longitude": 92.51,
                "fire_station_distance": 17.0,
                "has_water_supply": False,
                "response_minutes": 25.0,
                "long_arrival": True,
                "heating_season": False,
                "night_incident": False,
                "victims_present": False,
                "major_damage": True,
                "severe_consequence": False,
            },
            {
                "date": date(2025, 3, 3),
                "year": 2025,
                "source_table": "fires",
                "district": "District S",
                "territory_label": "Sector S",
                "settlement": "Sector S",
                "settlement_type": "village",
                "object_category": "Residential",
                "address": "Signalnaya 7",
                "address_comment": "",
                "object_name": "house",
                "latitude": 56.205,
                "longitude": 92.505,
                "fire_station_distance": 18.0,
                "has_water_supply": False,
                "response_minutes": 27.0,
                "long_arrival": True,
                "heating_season": True,
                "night_incident": False,
                "victims_present": True,
                "major_damage": False,
                "severe_consequence": True,
            },
        ]

        all_rows = _build_access_point_rows(records)
        water_only_rows = _build_access_point_rows(records, selected_features=["NO_WATER"])

        self.assertEqual(len(all_rows), 1)
        self.assertEqual(len(water_only_rows), 1)
        self.assertNotEqual(all_rows[0]["total_score"], water_only_rows[0]["total_score"])
        self.assertEqual(water_only_rows[0]["selected_feature_columns"], ["NO_WATER"])
        self.assertGreater(len(all_rows[0]["score_decomposition"]), len(water_only_rows[0]["score_decomposition"]))
        self.assertTrue(water_only_rows[0]["top_reason_codes"])
        self.assertTrue(set(water_only_rows[0]["top_reason_codes"]).issubset({"NO_WATER", "DATA_UNCERTAINTY"}))

    def test_nan_coordinates_are_normalized_before_payload_build(self) -> None:
        records = [
            {
                "date": date(2025, 1, 12),
                "year": 2025,
                "source_table": "fires",
                "district": "District N",
                "territory_label": "Village N",
                "settlement": "Village N",
                "settlement_type": "village",
                "object_category": "Residential",
                "address": "Lesnaya 9",
                "address_comment": "",
                "object_name": "house",
                "latitude": float("nan"),
                "longitude": float("nan"),
                "fire_station_distance": 15.0,
                "has_water_supply": True,
                "response_minutes": 21.0,
                "long_arrival": True,
                "heating_season": True,
                "night_incident": False,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": False,
            },
            {
                "date": date(2025, 2, 2),
                "year": 2025,
                "source_table": "fires",
                "district": "District N",
                "territory_label": "Village N",
                "settlement": "Village N",
                "settlement_type": "village",
                "object_category": "Residential",
                "address": "Lesnaya 9",
                "address_comment": "",
                "object_name": "house",
                "latitude": float("nan"),
                "longitude": float("nan"),
                "fire_station_distance": 18.0,
                "has_water_supply": False,
                "response_minutes": 24.0,
                "long_arrival": True,
                "heating_season": False,
                "night_incident": True,
                "victims_present": False,
                "major_damage": False,
                "severe_consequence": False,
            },
        ]

        rows = _build_access_point_rows(records)

        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["latitude"])
        self.assertIsNone(rows[0]["longitude"])
        self.assertEqual(rows[0]["coordinates_display"], "")


if __name__ == "__main__":
    unittest.main()
