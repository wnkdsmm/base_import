import unittest
from datetime import date

from tests.access_points_scoring_support import _build_access_point_rows, build_access_point_record


class AccessPointRankingTests(unittest.TestCase):
    def test_problem_point_ranking_prefers_locations_with_poor_access(self) -> None:
        records = [
            build_access_point_record(
                date_value=date(2025, 1, 12),
                district="District A",
                territory_label="Village Berezovka",
                settlement="Village Berezovka",
                address="Centralnaya 1",
                latitude=56.123,
                longitude=92.456,
                fire_station_distance=18.0,
                has_water_supply=False,
                response_minutes=31.0,
                night_incident=True,
                major_damage=True,
                severe_consequence=True,
            ),
            build_access_point_record(
                date_value=date(2025, 2, 3),
                district="District A",
                territory_label="Village Berezovka",
                settlement="Village Berezovka",
                address="Centralnaya 1",
                latitude=56.1232,
                longitude=92.4561,
                fire_station_distance=20.0,
                has_water_supply=False,
                response_minutes=28.0,
                victims_present=True,
                major_damage=True,
                severe_consequence=True,
            ),
            build_access_point_record(
                date_value=date(2025, 2, 10),
                district="District B",
                territory_label="City Center",
                settlement="City Center",
                settlement_type="city",
                object_category="Warehouse",
                address="Factory 7",
                object_name="warehouse",
                latitude=56.01,
                longitude=92.01,
                fire_station_distance=2.5,
                has_water_supply=True,
                response_minutes=9.0,
                long_arrival=False,
                major_damage=False,
                severe_consequence=False,
            ),
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

    def test_single_incident_low_support_point_is_smoothed_and_not_critical(self) -> None:
        rows = _build_access_point_rows(
            [
                build_access_point_record(
                    date_value=date(2025, 1, 12),
                    district="District T",
                    territory_label="Sector T",
                    settlement="Sector T",
                    address="Testovaya 5",
                    latitude=None,
                    longitude=None,
                    fire_station_distance=25.0,
                    has_water_supply=False,
                    response_minutes=40.0,
                    night_incident=True,
                    victims_present=True,
                    major_damage=True,
                    severe_consequence=True,
                )
            ]
        )

        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["low_support"])
        self.assertLess(float(rows[0]["total_score"]), 75.0)
        self.assertNotEqual(rows[0]["severity_band_code"], "critical")

    def test_selected_features_limit_scoring_drivers(self) -> None:
        records = [
            build_access_point_record(
                date_value=date(2025, 1, 12),
                district="District S",
                territory_label="Sector S",
                settlement="Sector S",
                address="Signalnaya 7",
                latitude=56.2,
                longitude=92.5,
                fire_station_distance=19.0,
                has_water_supply=False,
                response_minutes=29.0,
                night_incident=True,
                major_damage=True,
                severe_consequence=True,
            ),
            build_access_point_record(
                date_value=date(2025, 2, 10),
                district="District S",
                territory_label="Sector S",
                settlement="Sector S",
                address="Signalnaya 7",
                latitude=56.21,
                longitude=92.51,
                fire_station_distance=17.0,
                has_water_supply=False,
                response_minutes=25.0,
                heating_season=False,
                major_damage=True,
                severe_consequence=False,
            ),
            build_access_point_record(
                date_value=date(2025, 3, 3),
                district="District S",
                territory_label="Sector S",
                settlement="Sector S",
                address="Signalnaya 7",
                latitude=56.205,
                longitude=92.505,
                fire_station_distance=18.0,
                has_water_supply=False,
                response_minutes=27.0,
                victims_present=True,
                severe_consequence=True,
            ),
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
