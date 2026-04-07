from datetime import date
import math
import unittest

import pandas as pd

from app.services.access_points.analysis import _build_access_point_rows, _build_access_point_rows_from_entity_frame
from app.services.access_points.point_data import _build_point_entity_frames


def _record(**overrides):
    record = {
        "date": date(2025, 1, 1),
        "year": 2025,
        "source_table": "fires",
        "district": "Район",
        "territory_label": "Территория",
        "settlement": "Село",
        "settlement_type": "село",
        "object_category": "Жилой сектор",
        "address": "",
        "address_comment": "",
        "object_name": "",
        "latitude": None,
        "longitude": None,
        "fire_station_distance": 10.0,
        "has_water_supply": True,
        "response_minutes": 10.0,
        "long_arrival": False,
        "heating_season": False,
        "night_incident": False,
        "victims_present": False,
        "major_damage": False,
        "severe_consequence": False,
    }
    record.update(overrides)
    return record


def _is_missing(value) -> bool:
    return value is None or value != value


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

    def test_coordinate_identity_is_used_after_object_and_rejects_inf_or_out_of_range_values(self) -> None:
        records = [
            _record(
                date=date(2025, 1, 1),
                object_name="Школа 1",
                latitude=56.12345,
                longitude=92.54321,
                settlement="Село Объект",
            ),
            _record(
                date=date(2025, 1, 2),
                latitude=56.98765,
                longitude=92.12345,
                settlement="Село Координаты",
            ),
            _record(
                date=date(2025, 1, 3),
                latitude=float("inf"),
                longitude=92.22222,
                settlement="Село Inf",
            ),
            _record(
                date=date(2025, 1, 4),
                latitude=56.22222,
                longitude=181.0,
                settlement="Село Вне Диапазона",
            ),
        ]

        entity_frame, _feature_frame = _build_point_entity_frames(records)
        rows_by_label = {str(row["label"]): row for row in entity_frame.to_dict("records")}

        self.assertEqual(rows_by_label["Школа 1"]["entity_code"], "object")
        coordinate_row = next(row for row in rows_by_label.values() if row["entity_code"] == "coordinates")
        self.assertIn("Село Координаты", coordinate_row["label"])
        self.assertEqual(coordinate_row["point_id"], "coords:56.9877:92.1235")
        self.assertEqual(coordinate_row["latitude"], 56.98765)
        self.assertEqual(coordinate_row["longitude"], 92.12345)
        self.assertEqual(rows_by_label["Село Inf"]["entity_code"], "settlement")
        self.assertTrue(_is_missing(rows_by_label["Село Inf"]["latitude"]))
        self.assertTrue(_is_missing(rows_by_label["Село Inf"]["longitude"]))
        self.assertEqual(rows_by_label["Село Вне Диапазона"]["entity_code"], "settlement")
        self.assertTrue(_is_missing(rows_by_label["Село Вне Диапазона"]["latitude"]))
        self.assertTrue(_is_missing(rows_by_label["Село Вне Диапазона"]["longitude"]))

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

    def test_selected_feature_columns_limit_score_decomposition_and_explanation(self) -> None:
        records = [
            _record(date=date(2025, 3, 1), address="ул. Водная, 1", has_water_supply=False),
            _record(date=date(2025, 3, 2), address="ул. Водная, 1", has_water_supply=False),
            _record(date=date(2025, 3, 3), address="ул. Водная, 1", has_water_supply=False),
        ]

        rows = _build_access_point_rows(records, selected_features=["NO_WATER"])

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["selected_feature_columns"], ["NO_WATER"])
        self.assertEqual([item["code"] for item in row["score_decomposition"]], ["NO_WATER", "DATA_UNCERTAINTY"])
        self.assertEqual([item["code"] for item in row["reason_details"]], ["NO_WATER"])
        self.assertEqual(row["top_reason_codes"], ["NO_WATER"])
        self.assertIn("отсутствие воды", row["human_readable_explanation"].lower())

    def test_nan_inf_selected_no_water_scoring_keeps_uncertainty_as_separate_penalty(self) -> None:
        entity_frame = pd.DataFrame(
            [
                {
                    "point_id": "point:nan-water",
                    "label": "Nan water point",
                    "entity_type": "address",
                    "entity_code": "address",
                    "granularity_rank": 5,
                    "district": "District",
                    "territory_label": "Territory",
                    "settlement": "Settlement",
                    "settlement_type": "",
                    "rural_flag": False,
                    "rural_share": float("nan"),
                    "incident_count": 3,
                    "years_observed": 1,
                    "incidents_per_year": 3.0,
                    "average_response_minutes": float("nan"),
                    "response_coverage_share": float("nan"),
                    "long_arrival_share": float("inf"),
                    "average_distance_km": float("inf"),
                    "distance_coverage_share": float("nan"),
                    "no_water_share": float("nan"),
                    "water_coverage_share": 0.0,
                    "water_unknown_share": float("nan"),
                    "severe_share": float("inf"),
                    "victim_share": float("nan"),
                    "major_damage_share": float("nan"),
                    "victims_count": float("nan"),
                    "major_damage_count": float("inf"),
                    "night_share": float("inf"),
                    "heating_share": float("nan"),
                    "low_support": False,
                    "minimum_support": 3,
                    "support_weight": 1.0,
                    "response_count": 0,
                    "known_water_count": 0,
                    "distance_count": 0,
                    "source_tables": [],
                    "source_tables_display": "",
                    "object_category": "",
                    "location_hint": "",
                    "latitude": float("inf"),
                    "longitude": float("-inf"),
                }
            ]
        )

        rows = _build_access_point_rows_from_entity_frame(entity_frame, selected_features=["NO_WATER"])

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["selected_feature_columns"], ["NO_WATER"])
        self.assertEqual([item["code"] for item in row["score_decomposition"]], ["NO_WATER", "DATA_UNCERTAINTY"])
        self.assertTrue(math.isfinite(float(row["total_score"])))
        self.assertIsNone(row["latitude"])
        self.assertIsNone(row["longitude"])

        water_item, uncertainty_item = row["score_decomposition"]
        self.assertEqual(water_item["code"], "NO_WATER")
        self.assertEqual(water_item["contribution_points"], 0.0)
        self.assertEqual(uncertainty_item["code"], "DATA_UNCERTAINTY")
        self.assertTrue(uncertainty_item["is_penalty"])
        self.assertGreater(uncertainty_item["contribution_points"], 0.0)
        for item in row["score_decomposition"]:
            self.assertTrue(math.isfinite(float(item["factor_score"])))
            self.assertTrue(math.isfinite(float(item["contribution_points"])))
        self.assertTrue(set(row["top_reason_codes"]).issubset({"NO_WATER", "DATA_UNCERTAINTY"}))


if __name__ == "__main__":
    unittest.main()
