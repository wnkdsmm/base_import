import unittest

from app import statistics_constants
from app.dashboard import data_access
from app.domain import access_points_metadata, analytics_metadata, column_matching, fire_columns, time_labels
from app.services.access_points import constants as access_points_constants
from app.services.forecasting import constants as forecasting_constants
from core.processing.steps import keep_important_columns


class DomainMetadataConsolidationTests(unittest.TestCase):
    def test_statistics_constants_remain_compatible_facade(self):
        self.assertEqual(statistics_constants.CAUSE_COLUMNS, fire_columns.CAUSE_COLUMNS)
        self.assertEqual(statistics_constants.DISTRIBUTION_GROUPS, analytics_metadata.DISTRIBUTION_GROUPS)
        self.assertEqual(statistics_constants.MONTH_LABELS, time_labels.DASHBOARD_MONTH_LABELS)

    def test_dashboard_data_access_uses_canonical_district_candidates(self):
        self.assertEqual(
            data_access.DISTRICT_COLUMN_CANDIDATES,
            fire_columns.DASHBOARD_DISTRICT_COLUMN_CANDIDATES,
        )

    def test_keep_important_columns_uses_canonical_matching_metadata(self):
        self.assertIs(keep_important_columns.MANDATORY_FEATURE_REGISTRY, column_matching.MANDATORY_FEATURE_REGISTRY)
        self.assertIs(keep_important_columns.KEYWORD_IMPORTANCE_RULES, column_matching.KEYWORD_IMPORTANCE_RULES)
        self.assertEqual(
            keep_important_columns.get_mandatory_feature_catalog(),
            column_matching.get_mandatory_feature_catalog(),
        )

    def test_forecasting_time_labels_come_from_shared_time_catalog(self):
        self.assertEqual(forecasting_constants.WEEKDAY_LABELS, time_labels.FORECAST_WEEKDAY_LABELS)
        self.assertEqual(forecasting_constants.MONTH_LABELS, time_labels.FORECAST_MONTH_LABELS)

    def test_access_points_metadata_comes_from_domain_layer(self):
        self.assertEqual(
            access_points_constants.OBJECT_CATEGORY_COLUMN_CANDIDATES,
            fire_columns.OBJECT_CATEGORY_COLUMN_CANDIDATES,
        )
        self.assertIs(
            access_points_constants.ACCESS_POINT_FEATURE_METADATA,
            access_points_metadata.ACCESS_POINT_FEATURE_METADATA,
        )


if __name__ == "__main__":
    unittest.main()
