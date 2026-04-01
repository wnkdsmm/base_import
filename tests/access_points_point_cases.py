import unittest
from datetime import date

from tests.access_points_scoring_support import (
    _build_access_point_candidate_features,
    _build_access_point_rows,
    _select_incomplete_points,
    build_access_point_record,
    pd,
)


class AccessPointDataTests(unittest.TestCase):
    def test_candidate_features_are_built_for_picker(self) -> None:
        rows = _build_access_point_rows(
            [
                build_access_point_record(
                    date_value=date(2025, 1, 12),
                    district="District F",
                    territory_label="Village F",
                    settlement="Village F",
                    address="Field 1",
                    latitude=56.1,
                    longitude=92.1,
                    fire_station_distance=12.0,
                    has_water_supply=False,
                    response_minutes=22.0,
                    long_arrival=True,
                    heating_season=True,
                ),
                build_access_point_record(
                    date_value=date(2025, 2, 12),
                    district="District F",
                    territory_label="Village F",
                    settlement="Village F",
                    address="Field 1",
                    latitude=56.12,
                    longitude=92.11,
                    fire_station_distance=16.0,
                    has_water_supply=True,
                    response_minutes=28.0,
                    night_incident=True,
                    major_damage=True,
                    severe_consequence=True,
                    heating_season=False,
                ),
                build_access_point_record(
                    date_value=date(2025, 3, 12),
                    district="District F",
                    territory_label="Village F",
                    settlement="Village F",
                    address="Field 1",
                    latitude=56.13,
                    longitude=92.12,
                    fire_station_distance=14.0,
                    has_water_supply=False,
                    response_minutes=24.0,
                    long_arrival=False,
                    victims_present=True,
                    severe_consequence=True,
                ),
            ]
        )

        features = _build_access_point_candidate_features(pd.DataFrame(rows))

        self.assertTrue(features)
        self.assertEqual(features[0]["coverage_display"], "100%")
        self.assertIn("variance_display", features[0])

    def test_incomplete_points_are_marked_for_review(self) -> None:
        records = [
            build_access_point_record(
                date_value=date(2025, 1, 5),
                district="District C",
                territory_label="Forest Village",
                settlement="Forest Village",
                address="",
                object_name="",
                latitude=None,
                longitude=None,
                fire_station_distance=None,
                has_water_supply=None,
                response_minutes=None,
                night_incident=True,
            ),
            build_access_point_record(
                date_value=date(2025, 2, 5),
                district="District C",
                territory_label="Forest Village",
                settlement="Forest Village",
                address="",
                object_name="",
                latitude=None,
                longitude=None,
                fire_station_distance=None,
                has_water_supply=None,
                response_minutes=None,
                heating_season=True,
            ),
            build_access_point_record(
                date_value=date(2025, 3, 5),
                district="District C",
                territory_label="Forest Village",
                settlement="Forest Village",
                address="",
                object_name="",
                latitude=None,
                longitude=None,
                fire_station_distance=None,
                has_water_supply=None,
                response_minutes=None,
                night_incident=True,
            ),
        ]

        rows = _build_access_point_rows(records)
        incomplete = _select_incomplete_points(rows)

        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["missing_data_priority"])
        self.assertTrue(rows[0]["uncertainty_flag"])
        self.assertTrue(incomplete)
        self.assertTrue(rows[0]["incomplete_note"])

    def test_nan_coordinates_are_normalized_before_payload_build(self) -> None:
        records = [
            build_access_point_record(
                date_value=date(2025, 1, 12),
                district="District N",
                territory_label="Village N",
                settlement="Village N",
                address="Lesnaya 9",
                latitude=float("nan"),
                longitude=float("nan"),
                fire_station_distance=15.0,
                has_water_supply=True,
                response_minutes=21.0,
            ),
            build_access_point_record(
                date_value=date(2025, 2, 2),
                district="District N",
                territory_label="Village N",
                settlement="Village N",
                address="Lesnaya 9",
                latitude=float("nan"),
                longitude=float("nan"),
                fire_station_distance=18.0,
                has_water_supply=False,
                response_minutes=24.0,
                night_incident=True,
                heating_season=False,
            ),
        ]

        rows = _build_access_point_rows(records)

        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["latitude"])
        self.assertIsNone(rows[0]["longitude"])
        self.assertEqual(rows[0]["coordinates_display"], "")
