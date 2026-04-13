from contextlib import ExitStack
from datetime import date
import unittest
from unittest.mock import patch

from app.dashboard import distribution, impact, management, service, summary, utils as dashboard_utils
from app.dashboard.data_access import DISTRICT_COLUMN_CANDIDATES
from app.statistics_constants import (
    AREA_COLUMN,
    BUILDING_CATEGORY_COLUMN,
    BUILDING_CAUSE_COLUMN,
    DAMAGE_GROUP_OPTION_VALUE,
    DATE_COLUMN,
    GENERAL_CAUSE_COLUMN,
    REGISTERED_DAMAGE_COLUMN,
)
from tests.dashboard_analytics_test_helpers import (
    MOJIBAKE_TOKEN_RE,
    _DashboardAggregationEngine,
    _DashboardConnection,
    _FailingDashboardEngine,
    _ImpactTimelineConnection,
    _PositiveColumnCountsConnection,
    _SummaryBundleConnection,
    _iter_payload_strings,
)



class DashboardServiceCacheOptimizationTests(unittest.TestCase):
    def test_service_reuses_grouped_counts_bundle_for_distribution_widgets_and_area_buckets(self) -> None:
        metadata = {
            "tables": [{"name": "fires", "column_set": {GENERAL_CAUSE_COLUMN}, "years": [2024], "table_year": None}],
            "table_options": [{"value": "all", "label": "all"}, {"value": "fires", "label": "fires"}],
            "default_group_column": GENERAL_CAUSE_COLUMN,
            "errors": [],
        }
        filter_state = {
            "selected_tables": metadata["tables"],
            "available_years": [{"value": "2024", "label": "2024"}],
            "selected_year": 2024,
            "available_group_columns": [{"value": GENERAL_CAUSE_COLUMN, "label": GENERAL_CAUSE_COLUMN}],
            "selected_group_column": GENERAL_CAUSE_COLUMN,
            "selected_table_name": "fires",
        }
        empty_chart = {"items": []}
        widgets = {"districts": {"items": []}}
        grouped_counts_bundle = {
            "cause_counts": {"cause_a": 5},
            "distribution_counts": {"cause_a": 5},
            "month_counts": {1: 2, 7: 3},
            "district_counts": {"district_a": 4},
            "area_bucket_counts": {"До 1 га": 6},
            "impact_timeline_rows": [{"date_value": date(2024, 1, 2)}],
        }

        with ExitStack() as stack:
            stack.enter_context(patch("app.dashboard.service._collect_dashboard_metadata_cached", return_value=metadata))
            stack.enter_context(patch("app.dashboard.service._resolve_dashboard_filters", return_value=filter_state))
            stack.enter_context(patch("app.dashboard.service._get_dashboard_cache", return_value=None))
            stack.enter_context(patch("app.dashboard.service._set_dashboard_cache"))
            summary_bundle_mock = stack.enter_context(
                patch(
                    "app.dashboard.service._collect_dashboard_summary_bundle",
                    return_value={"summary_rows": [], "yearly_grouped": {}},
                )
            )
            stack.enter_context(patch("app.dashboard.service._build_summary", return_value={"fires_count": 0}))
            yearly_mock = stack.enter_context(patch("app.dashboard.service._build_yearly_chart", return_value=empty_chart))
            table_breakdown_mock = stack.enter_context(
                patch("app.dashboard.service._build_table_breakdown_chart", return_value=empty_chart)
            )
            grouped_counts_mock = stack.enter_context(
                patch("app.dashboard.service._collect_dashboard_grouped_counts", return_value=grouped_counts_bundle)
            )
            stack.enter_context(
                patch(
                    "app.dashboard.service._collect_cause_counts",
                    side_effect=AssertionError("cause counts should come from grouped bundle"),
                )
            )
            stack.enter_context(patch("app.dashboard.service._build_cause_chart", return_value=empty_chart))
            stack.enter_context(
                patch(
                    "app.dashboard.service._collect_month_counts",
                    side_effect=AssertionError("month counts should come from grouped bundle"),
                )
            )
            distribution_mock = stack.enter_context(
                patch("app.dashboard.service._build_distribution_chart", return_value=empty_chart)
            )
            impact_timeline_mock = stack.enter_context(
                patch("app.dashboard.service._build_combined_impact_timeline_chart", return_value=empty_chart)
            )
            stack.enter_context(patch("app.dashboard.service._build_monthly_profile_chart", return_value=empty_chart))
            stack.enter_context(
                patch(
                    "app.dashboard.service._build_area_buckets_chart",
                    side_effect=AssertionError("area buckets should come from grouped bundle"),
                )
            )
            area_buckets_mock = stack.enter_context(
                patch("app.dashboard.service._build_area_buckets_chart_from_counts", return_value=empty_chart)
            )
            stack.enter_context(patch("app.dashboard.service._build_trend", return_value={}))
            stack.enter_context(patch("app.dashboard.service._build_rankings", return_value={}))
            stack.enter_context(patch("app.dashboard.service._build_highlights", return_value=[]))
            sql_widgets_mock = stack.enter_context(patch("app.dashboard.service._build_sql_widgets", return_value=widgets))
            stack.enter_context(patch("app.dashboard.service._build_management_snapshot", return_value={}))
            stack.enter_context(
                patch(
                    "app.dashboard.service._build_scope",
                    return_value={"table_label": "fires", "year_label": "2024", "group_label": "cause"},
                )
            )
            stack.enter_context(patch("app.dashboard.service.compose_executive_brief_text", return_value="brief"))
            service.get_dashboard_data(table_name="fires", year="2024", group_column=GENERAL_CAUSE_COLUMN, allow_fallback=False)

        summary_bundle_mock.assert_called_once_with(metadata["tables"], 2024)
        self.assertFalse(yearly_mock.call_args.kwargs["include_plotly"])
        self.assertFalse(table_breakdown_mock.call_args.kwargs["include_plotly"])
        grouped_counts_mock.assert_called_once_with(
            metadata["tables"],
            2024,
            GENERAL_CAUSE_COLUMN,
            include_area_buckets=True,
            include_impact_timeline=True,
        )
        self.assertEqual(distribution_mock.call_args.kwargs["grouped_counts"], {"cause_a": 5})
        self.assertEqual(area_buckets_mock.call_args.args[0], {"До 1 га": 6})
        self.assertEqual(impact_timeline_mock.call_args.kwargs["impact_timeline_rows"], [{"date_value": date(2024, 1, 2)}])
        self.assertEqual(sql_widgets_mock.call_args.kwargs["cause_counts"], {"cause_a": 5})
        self.assertEqual(sql_widgets_mock.call_args.kwargs["month_counts"], {1: 2, 7: 3})
        self.assertEqual(sql_widgets_mock.call_args.kwargs["district_counts"], {"district_a": 4})

    def test_service_reuses_canonical_cache_entry_for_invalid_request_filters(self) -> None:
        metadata = {
            "tables": [{"name": "fires", "column_set": {GENERAL_CAUSE_COLUMN}, "years": [2024], "table_year": None}],
            "table_signature": ("fires",),
            "table_options": [{"value": "all", "label": "all"}, {"value": "fires", "label": "fires"}],
            "default_group_column": GENERAL_CAUSE_COLUMN,
            "errors": [],
        }
        filter_state = {
            "selected_tables": metadata["tables"],
            "available_years": [{"value": "2024", "label": "2024"}],
            "selected_year": None,
            "available_group_columns": [{"value": GENERAL_CAUSE_COLUMN, "label": GENERAL_CAUSE_COLUMN}],
            "selected_group_column": GENERAL_CAUSE_COLUMN,
            "selected_table_name": "all",
        }
        cached_payload = {"has_data": True, "notes": ["cached canonical payload"]}

        with ExitStack() as stack:
            stack.enter_context(patch("app.dashboard.service._collect_dashboard_metadata_cached", return_value=metadata))
            stack.enter_context(patch("app.dashboard.service._resolve_dashboard_filters", return_value=filter_state))
            cache_get_mock = stack.enter_context(
                patch("app.dashboard.service._get_dashboard_cache", side_effect=[None, cached_payload])
            )
            set_cache_mock = stack.enter_context(patch("app.dashboard.service._set_dashboard_cache"))
            stack.enter_context(
                patch(
                    "app.dashboard.service._build_dashboard_aggregation",
                    side_effect=AssertionError("canonical cache hit should avoid aggregation"),
                )
            )

            payload = service.get_dashboard_data(
                table_name="missing_table",
                year="9999",
                group_column="missing_group",
                allow_fallback=False,
            )

        self.assertEqual(payload, cached_payload)
        self.assertEqual(
            cache_get_mock.call_args_list,
            [
                unittest.mock.call(
                    service._build_dashboard_cache_key(
                        metadata,
                        "missing_table",
                        "9999",
                        "missing_group",
                    )
                ),
                unittest.mock.call(
                    service._build_resolved_dashboard_cache_key(
                        metadata,
                        "all",
                        None,
                        GENERAL_CAUSE_COLUMN,
                    )
                ),
            ],
        )
        set_cache_mock.assert_not_called()

    def test_service_stores_dashboard_payload_under_resolved_cache_key(self) -> None:
        metadata = {
            "tables": [{"name": "fires", "column_set": {GENERAL_CAUSE_COLUMN}, "years": [2024], "table_year": None}],
            "table_signature": ("fires",),
            "table_options": [{"value": "all", "label": "all"}, {"value": "fires", "label": "fires"}],
            "default_group_column": GENERAL_CAUSE_COLUMN,
            "errors": [],
        }
        filter_state = {
            "selected_tables": metadata["tables"],
            "available_years": [{"value": "2024", "label": "2024"}],
            "selected_year": None,
            "available_group_columns": [{"value": GENERAL_CAUSE_COLUMN, "label": GENERAL_CAUSE_COLUMN}],
            "selected_group_column": GENERAL_CAUSE_COLUMN,
            "selected_table_name": "all",
        }
        aggregation = {"summary": {"fires_count": 0}}
        payload = {"has_data": True, "notes": []}

        with ExitStack() as stack:
            stack.enter_context(patch("app.dashboard.service._collect_dashboard_metadata_cached", return_value=metadata))
            stack.enter_context(patch("app.dashboard.service._resolve_dashboard_filters", return_value=filter_state))
            cache_get_mock = stack.enter_context(
                patch("app.dashboard.service._get_dashboard_cache", side_effect=[None, None])
            )
            build_aggregation_mock = stack.enter_context(
                patch("app.dashboard.service._build_dashboard_aggregation", return_value=aggregation)
            )
            build_payload_mock = stack.enter_context(
                patch("app.dashboard.service._build_dashboard_payload", return_value=payload)
            )
            set_cache_mock = stack.enter_context(patch("app.dashboard.service._set_dashboard_cache"))

            result = service.get_dashboard_data(
                table_name="missing_table",
                year="9999",
                group_column="missing_group",
                allow_fallback=False,
            )

        self.assertEqual(result, payload)
        self.assertEqual(len(cache_get_mock.call_args_list), 2)
        build_aggregation_mock.assert_called_once()
        build_payload_mock.assert_called_once()
        set_cache_mock.assert_called_once_with(
            service._build_resolved_dashboard_cache_key(
                metadata,
                "all",
                None,
                GENERAL_CAUSE_COLUMN,
            ),
            payload,
        )

    def test_service_reuses_grouped_counts_bundle_for_non_cause_distribution(self) -> None:
        metadata = {
            "tables": [
                {
                    "name": "fires",
                    "column_set": {GENERAL_CAUSE_COLUMN, BUILDING_CATEGORY_COLUMN},
                    "years": [2024],
                    "table_year": None,
                }
            ],
            "table_options": [{"value": "all", "label": "all"}, {"value": "fires", "label": "fires"}],
            "default_group_column": BUILDING_CATEGORY_COLUMN,
            "errors": [],
        }
        filter_state = {
            "selected_tables": metadata["tables"],
            "available_years": [{"value": "2024", "label": "2024"}],
            "selected_year": 2024,
            "available_group_columns": [{"value": BUILDING_CATEGORY_COLUMN, "label": BUILDING_CATEGORY_COLUMN}],
            "selected_group_column": BUILDING_CATEGORY_COLUMN,
            "selected_table_name": "fires",
        }
        empty_chart = {"items": []}
        widgets = {"districts": {"items": []}}
        grouped_counts_bundle = {
            "cause_counts": {"cause_a": 5},
            "distribution_counts": {"category_a": 7},
            "month_counts": {1: 2},
            "district_counts": {},
            "area_bucket_counts": {},
            "impact_timeline_rows": [{"date_value": date(2024, 1, 2)}],
        }

        with ExitStack() as stack:
            stack.enter_context(patch("app.dashboard.service._collect_dashboard_metadata_cached", return_value=metadata))
            stack.enter_context(patch("app.dashboard.service._resolve_dashboard_filters", return_value=filter_state))
            stack.enter_context(patch("app.dashboard.service._get_dashboard_cache", return_value=None))
            stack.enter_context(patch("app.dashboard.service._set_dashboard_cache"))
            stack.enter_context(
                patch(
                    "app.dashboard.service._collect_dashboard_summary_bundle",
                    return_value={"summary_rows": [], "yearly_grouped": {}},
                )
            )
            stack.enter_context(patch("app.dashboard.service._build_summary", return_value={"fires_count": 0}))
            stack.enter_context(patch("app.dashboard.service._build_yearly_chart", return_value=empty_chart))
            stack.enter_context(patch("app.dashboard.service._build_table_breakdown_chart", return_value=empty_chart))
            grouped_counts_mock = stack.enter_context(
                patch("app.dashboard.service._collect_dashboard_grouped_counts", return_value=grouped_counts_bundle)
            )
            stack.enter_context(
                patch(
                    "app.dashboard.service._collect_cause_counts",
                    side_effect=AssertionError("cause counts should come from grouped bundle"),
                )
            )
            stack.enter_context(patch("app.dashboard.service._build_cause_chart", return_value=empty_chart))
            stack.enter_context(
                patch(
                    "app.dashboard.service._collect_month_counts",
                    side_effect=AssertionError("month counts should come from grouped bundle"),
                )
            )
            distribution_mock = stack.enter_context(
                patch("app.dashboard.service._build_distribution_chart", return_value=empty_chart)
            )
            impact_timeline_mock = stack.enter_context(
                patch("app.dashboard.service._build_combined_impact_timeline_chart", return_value=empty_chart)
            )
            stack.enter_context(patch("app.dashboard.service._build_monthly_profile_chart", return_value=empty_chart))
            stack.enter_context(patch("app.dashboard.service._build_area_buckets_chart", return_value=empty_chart))
            stack.enter_context(patch("app.dashboard.service._build_trend", return_value={}))
            stack.enter_context(patch("app.dashboard.service._build_rankings", return_value={}))
            stack.enter_context(patch("app.dashboard.service._build_highlights", return_value=[]))
            stack.enter_context(patch("app.dashboard.service._build_sql_widgets", return_value=widgets))
            stack.enter_context(patch("app.dashboard.service._build_management_snapshot", return_value={}))
            stack.enter_context(
                patch(
                    "app.dashboard.service._build_scope",
                    return_value={"table_label": "fires", "year_label": "2024", "group_label": "category"},
                )
            )
            stack.enter_context(patch("app.dashboard.service.compose_executive_brief_text", return_value="brief"))
            service.get_dashboard_data(
                table_name="fires",
                year="2024",
                group_column=BUILDING_CATEGORY_COLUMN,
                allow_fallback=False,
            )

        grouped_counts_mock.assert_called_once_with(
            metadata["tables"],
            2024,
            BUILDING_CATEGORY_COLUMN,
            include_area_buckets=True,
            include_impact_timeline=True,
        )
        self.assertEqual(distribution_mock.call_args.kwargs["grouped_counts"], {"category_a": 7})
        self.assertEqual(impact_timeline_mock.call_args.kwargs["impact_timeline_rows"], [{"date_value": date(2024, 1, 2)}])
