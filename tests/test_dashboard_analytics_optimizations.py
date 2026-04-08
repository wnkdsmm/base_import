from contextlib import ExitStack
from datetime import date
import re
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
    IMPACT_METRIC_CONFIG,
    REGISTERED_DAMAGE_COLUMN,
)


MOJIBAKE_TOKEN_RE = re.compile(
    r"[РС][\u00A0\u00B5\u0400-\u040F\u0450-\u045F\u0490\u0491"
    r"\u201A-\u203A\u2122]|В°"
)


def _iter_payload_strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from _iter_payload_strings(key)
            yield from _iter_payload_strings(item)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            yield from _iter_payload_strings(item)


class _DashboardQueryResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)

    def one(self):
        return self._rows[0]


class _DashboardConnection:
    def __init__(self, *, distribution_rows=None):
        self.queries = []
        self.params = []
        self.distribution_rows = distribution_rows or [
            {"label": "Жилое", "fire_count": 2},
            {"label": "Склад", "fire_count": 1},
        ]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=None):
        query_text = str(query)
        self.queries.append(query_text)
        self.params.append(dict(params or {}))

        if "GROUPING SETS" in query_text and "'cause'" in query_text:
            rows = [
                {"metric_kind": "cause", "label": "Электрика", "fire_count": 2},
                {"metric_kind": "cause", "label": "Неосторожность", "fire_count": 1},
                {"metric_kind": "district", "label": "Центральный", "fire_count": 2},
                {"metric_kind": "district", "label": "Северный", "fire_count": 1},
                {"metric_kind": "month", "label": "1", "fire_count": 2},
                {"metric_kind": "month", "label": "2", "fire_count": 1},
            ]
            if "'area_bucket'" in query_text:
                rows.extend(
                    [
                        {"metric_kind": "area_bucket", "label": "До 1 га", "fire_count": 1},
                        {"metric_kind": "area_bucket", "label": "5-20 га", "fire_count": 1},
                        {"metric_kind": "area_bucket", "label": "Не указано", "fire_count": 1},
                    ]
                )
            if "'distribution'" in query_text:
                rows.extend(
                    [
                        {"metric_kind": "distribution", "label": "Жилое", "fire_count": 2},
                        {"metric_kind": "distribution", "label": "Склад", "fire_count": 1},
                    ]
                )
            if "'impact_timeline'" in query_text:
                rows.append(
                    {
                        "metric_kind": "impact_timeline",
                        "label": None,
                        "fire_count": 0,
                        "date_value": date(2024, 1, 2),
                        "deaths": 1,
                        "injuries": 2,
                        "evacuated": 3,
                        "evacuated_children": 4,
                        "rescued_children": 5,
                    }
                )
            if "'positive_column_bundle'" in query_text:
                rows.append(
                    {
                        "metric_kind": "positive_column_bundle",
                        "label": None,
                        "fire_count": 0,
                        "positive_metric_0": 3,
                        "positive_metric_1": 0,
                    }
                )
            return _DashboardQueryResult(rows)

        if "GROUP BY label" in query_text:
            return _DashboardQueryResult(self.distribution_rows)

        raise AssertionError(f"Unexpected dashboard query: {query_text}")


class _SummaryBundleConnection:
    def __init__(self):
        self.queries = []
        self.params = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=None):
        self.queries.append(str(query))
        self.params.append(dict(params or {}))
        metric_defaults = {metric_key: 0 for metric_key in IMPACT_METRIC_CONFIG}
        return _DashboardQueryResult(
            [
                {
                    "table_name": "fires_dashboard",
                    "metric_kind": "summary",
                    "year_value": None,
                    "fire_count": 3,
                    "total_area": 12.5,
                    "area_count": 2,
                    **metric_defaults,
                },
                {
                    "table_name": "fires_dashboard",
                    "metric_kind": "yearly",
                    "year_value": 2024,
                    "fire_count": 3,
                    "total_area": 12.5,
                    "area_count": 0,
                    **metric_defaults,
                },
            ]
        )


class _ImpactTimelineConnection:
    def __init__(self):
        self.queries = []
        self.params = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=None):
        self.queries.append(str(query))
        self.params.append(dict(params or {}))
        return _DashboardQueryResult(
            [
                {
                    "date_value": date(2024, 1, 2),
                    "deaths": 1,
                    "injuries": 2,
                    "evacuated": 3,
                    "evacuated_children": 4,
                    "rescued_children": 5,
                }
            ]
        )


class _PositiveColumnCountsConnection:
    def __init__(self):
        self.queries = []
        self.params = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=None):
        self.queries.append(str(query))
        self.params.append(dict(params or {}))
        return _DashboardQueryResult([{"metric_0": 3, "metric_1": 0}])


class _DashboardAggregationConnection:
    def __init__(self):
        self.queries = []
        self.params = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=None):
        query_text = str(query)
        self.queries.append(query_text)
        self.params.append(dict(params or {}))
        metric_defaults = {metric_key: 0 for metric_key in IMPACT_METRIC_CONFIG}
        if "GROUPING SETS" in query_text:
            return _DashboardQueryResult(
                [
                    {"metric_kind": "cause", "label": "Электрика", "fire_count": 3},
                    {"metric_kind": "month", "label": "1", "fire_count": 3},
                    {
                        "metric_kind": "positive_column_bundle",
                        "label": None,
                        "fire_count": 0,
                        "positive_metric_0": 3,
                    },
                ]
            )
        if "AS table_name" in query_text and "AS year_value" in query_text:
            return _DashboardQueryResult(
                [
                    {
                        "table_name": "fires",
                        "year_value": 2024,
                        "fire_count": 3,
                        "total_area": 0.0,
                        "area_count": 0,
                        **metric_defaults,
                    }
                ]
            )
        raise AssertionError(f"Unexpected dashboard aggregation query: {query_text}")


class _DashboardAggregationEngine:
    def __init__(self):
        self.connection = _DashboardAggregationConnection()

    def connect(self):
        return self.connection


class _FailingDashboardEngine:
    def connect(self):
        raise AssertionError("damage dashboard should reuse grouped counts instead of opening distribution SQL")


class DashboardAnalyticsOptimizationTests(unittest.TestCase):
    def test_management_snapshot_uses_lightweight_decision_support_payload(self) -> None:
        risk_payload = {
            "quality_passport": {
                "validation_summary": "Паспорт качества собран.",
                "confidence_label": "Рабочая",
                "confidence_score_display": "72 / 100",
                "confidence_tone": "sky",
            },
            "territories": [],
            "notes": ["Базовая заметка."],
            "top_territory_label": "-",
            "top_territory_explanation": "Недостаточно данных.",
            "top_territory_confidence_label": "Рабочая",
            "top_territory_confidence_score_display": "72 / 100",
            "top_territory_confidence_tone": "sky",
            "top_territory_confidence_note": "Паспорт качества собран.",
        }
        with patch("app.dashboard.management.build_decision_support_payload", return_value=risk_payload) as payload_mock:
            snapshot = management._build_management_snapshot(
                selected_tables=[{"name": "fires"}],
                selected_year=2024,
                summary={},
                trend={"direction": "flat", "description": "", "delta_display": "0"},
                cause_overview={"items": []},
                district_widget={"items": []},
            )

        payload_mock.assert_called_once()
        self.assertFalse(payload_mock.call_args.kwargs["include_geo_prediction"])
        self.assertFalse(payload_mock.call_args.kwargs["include_historical_validation"])
        self.assertIn("brief", snapshot)
        self.assertIn("summary_line", snapshot)

    def test_sql_widgets_reuse_precomputed_cause_and_month_counts(self) -> None:
        with (
            patch("app.dashboard.impact._build_sql_district_widget", return_value={"items": []}),
            patch("app.dashboard.impact._collect_cause_counts", side_effect=AssertionError("cause counts should be reused")),
            patch("app.dashboard.impact._collect_month_counts", side_effect=AssertionError("month counts should be reused")),
        ):
            widgets = impact._build_sql_widgets(
                [],
                None,
                cause_counts={"cause_a": 5, "cause_b": 2},
                month_counts={1: 2, 7: 3},
            )

        self.assertEqual(widgets["causes"]["items"][0]["label"], "cause_a")
        self.assertEqual([item["value"] for item in widgets["seasons"]["items"]], [2, 3])

    def test_sql_widgets_reuse_precomputed_district_counts_without_sql(self) -> None:
        with patch(
            "app.dashboard.impact._build_sql_district_widget",
            side_effect=AssertionError("district counts should be reused"),
        ):
            widgets = impact._build_sql_widgets(
                [],
                None,
                district_counts={"district_a": 4, "district_b": 2},
            )

        self.assertEqual([item["label"] for item in widgets["districts"]["items"]], ["district_a", "district_b"])
        self.assertEqual([item["value"] for item in widgets["districts"]["items"]], [4, 2])

    def test_table_breakdown_reuses_summary_rows_without_extra_sql(self) -> None:
        with patch("app.dashboard.distribution.engine.connect", side_effect=AssertionError("summary rows should avoid extra SQL")):
            chart = distribution._build_table_breakdown_chart(
                [],
                None,
                summary_rows=[
                    {"table_name": "fires_a", "fire_count": 7},
                    {"table_name": "fires_b", "fire_count": 3},
                ],
            )

        self.assertEqual([item["label"] for item in chart["items"]], ["fires_a", "fires_b"])
        self.assertEqual([item["value"] for item in chart["items"]], [7, 3])

    def test_summary_bundle_collects_summary_and_yearly_rows_in_one_sql_pass(self) -> None:
        table = {
            "name": "fires_dashboard",
            "column_set": {DATE_COLUMN, AREA_COLUMN},
            "years": [2023, 2024],
            "table_year": None,
        }
        second_table = {
            "name": "fires_dashboard_next",
            "column_set": {DATE_COLUMN, AREA_COLUMN},
            "years": [2024],
            "table_year": None,
        }
        conn = _SummaryBundleConnection()

        with patch("app.dashboard.summary.engine.connect", return_value=conn):
            bundle = summary._collect_dashboard_summary_bundle([table, second_table], 2024)

        self.assertEqual(len(conn.queries), 1)
        self.assertIn("AS year_value", conn.queries[0])
        self.assertIn("GROUP BY year_value", conn.queries[0])
        self.assertIn("UNION ALL", conn.queries[0])
        self.assertIn("fires_dashboard_next", conn.queries[0])
        self.assertEqual(conn.params, [{}])
        self.assertEqual(bundle["summary_rows"][0]["fire_count"], 3)
        self.assertEqual(bundle["summary_rows"][1]["table_name"], "fires_dashboard_next")
        self.assertEqual(bundle["summary_rows"][1]["fire_count"], 0)
        self.assertEqual(bundle["yearly_grouped"][2024]["count"], 3.0)

    def test_distribution_chart_reuses_grouped_counts_without_sql(self) -> None:
        with patch("app.dashboard.distribution.engine.connect", side_effect=AssertionError("grouped counts should avoid extra SQL")):
            chart = distribution._build_distribution_chart(
                [],
                None,
                GENERAL_CAUSE_COLUMN,
                grouped_counts={"cause_a": 7, "cause_b": 3},
            )

        self.assertEqual([item["label"] for item in chart["items"]], ["cause_a", "cause_b"])
        self.assertEqual([item["value"] for item in chart["items"]], [7, 3])

    def test_collect_dashboard_grouped_counts_builds_real_bundle_for_cause_and_non_cause(self) -> None:
        table = {
            "name": "fires_dashboard",
            "column_set": {
                DATE_COLUMN,
                GENERAL_CAUSE_COLUMN,
                BUILDING_CATEGORY_COLUMN,
                DISTRICT_COLUMN_CANDIDATES[0],
                AREA_COLUMN,
            },
            "years": [2024],
            "table_year": None,
        }

        cause_conn = _DashboardConnection()
        with patch("app.dashboard.impact.engine.connect", return_value=cause_conn):
            cause_bundle = impact._collect_dashboard_grouped_counts(
                [table, {**table, "name": "fires_dashboard_next"}],
                2024,
                GENERAL_CAUSE_COLUMN,
            )

        self.assertEqual(cause_bundle["cause_counts"], {"Электрика": 2, "Неосторожность": 1})
        self.assertEqual(cause_bundle["distribution_counts"], cause_bundle["cause_counts"])
        self.assertEqual(cause_bundle["district_counts"], {"Центральный": 2, "Северный": 1})
        self.assertEqual(cause_bundle["month_counts"], {1: 2, 2: 1})
        self.assertEqual(cause_bundle["area_bucket_counts"], {"До 1 га": 1, "5-20 га": 1, "Не указано": 1})
        self.assertEqual(cause_bundle["impact_timeline_rows"][0]["date_value"], date(2024, 1, 2))
        self.assertEqual(cause_conn.params, [{"selected_year": 2024}])
        self.assertEqual(len(cause_conn.queries), 1)
        self.assertIn("fires_dashboard_next", cause_conn.queries[0])
        self.assertIn("GROUPING SETS", cause_conn.queries[0])
        for metric_kind in ("'cause'", "'district'", "'month'", "'area_bucket'", "'impact_timeline'"):
            self.assertIn(metric_kind, cause_conn.queries[0])
        self.assertNotIn("'distribution'", cause_conn.queries[0])
        self.assertTrue(service._can_reuse_distribution_counts([table], GENERAL_CAUSE_COLUMN, cause_bundle["distribution_counts"]))

        non_cause_conn = _DashboardConnection()
        with patch("app.dashboard.impact.engine.connect", return_value=non_cause_conn):
            non_cause_bundle = impact._collect_dashboard_grouped_counts([table], 2024, BUILDING_CATEGORY_COLUMN)

        self.assertEqual(non_cause_bundle["cause_counts"], {"Электрика": 2, "Неосторожность": 1})
        self.assertEqual(non_cause_bundle["distribution_counts"], {"Жилое": 2, "Склад": 1})
        self.assertEqual(non_cause_bundle["district_counts"], {"Центральный": 2, "Северный": 1})
        self.assertEqual(non_cause_bundle["month_counts"], {1: 2, 2: 1})
        self.assertEqual(non_cause_bundle["area_bucket_counts"], {"До 1 га": 1, "5-20 га": 1, "Не указано": 1})
        self.assertEqual(non_cause_bundle["impact_timeline_rows"][0]["deaths"], 1)
        self.assertEqual(non_cause_conn.params, [{"selected_year": 2024}])
        self.assertIn("GROUPING SETS", non_cause_conn.queries[0])
        for metric_kind in ("'cause'", "'distribution'", "'district'", "'month'", "'area_bucket'", "'impact_timeline'"):
            self.assertIn(metric_kind, non_cause_conn.queries[0])
        self.assertIn("'distribution'", non_cause_conn.queries[0])
        self.assertTrue(
            service._can_reuse_distribution_counts(
                [table],
                BUILDING_CATEGORY_COLUMN,
                non_cause_bundle["distribution_counts"],
            )
        )

        trimmed_conn = _DashboardConnection()
        with patch("app.dashboard.impact.engine.connect", return_value=trimmed_conn):
            trimmed_bundle = impact._collect_dashboard_grouped_counts(
                [table],
                2024,
                BUILDING_CATEGORY_COLUMN,
                include_area_buckets=False,
                include_impact_timeline=False,
            )

        self.assertEqual(trimmed_bundle["area_bucket_counts"], {})
        self.assertEqual(trimmed_bundle["impact_timeline_rows"], [])
        self.assertNotIn("'area_bucket'", trimmed_conn.queries[0])
        self.assertNotIn("'impact_timeline'", trimmed_conn.queries[0])

    def test_impact_timeline_collects_selected_tables_in_one_union_query(self) -> None:
        tables = [
            {"name": "fires_dashboard", "column_set": {DATE_COLUMN}, "years": [2024], "table_year": None},
            {"name": "fires_dashboard_next", "column_set": {DATE_COLUMN}, "years": [2024], "table_year": None},
        ]
        conn = _ImpactTimelineConnection()

        with patch("app.dashboard.impact.engine.connect", return_value=conn):
            chart = impact._build_combined_impact_timeline_chart(tables, 2024)

        self.assertEqual(len(conn.queries), 1)
        self.assertIn("UNION ALL", conn.queries[0])
        self.assertIn("impact_timeline_0", conn.queries[0])
        self.assertIn("impact_timeline_1", conn.queries[0])
        self.assertEqual(conn.params, [{"selected_year": 2024}])
        self.assertEqual(chart["items"][0]["value"], 15.0)

    def test_impact_timeline_reuses_grouped_bundle_rows_without_sql(self) -> None:
        with patch("app.dashboard.impact.engine.connect", side_effect=AssertionError("timeline rows should be reused")):
            chart = impact._build_combined_impact_timeline_chart(
                [],
                None,
                impact_timeline_rows=[
                    {
                        "date_value": date(2024, 1, 2),
                        "deaths": 1,
                        "injuries": 2,
                        "evacuated": 3,
                        "evacuated_children": 4,
                        "rescued_children": 5,
                    }
                ],
            )

        self.assertEqual(chart["items"][0]["date_value"], "2024-01-02")
        self.assertEqual(chart["items"][0]["value"], 15.0)

    def test_positive_column_counts_collects_selected_tables_in_one_union_query(self) -> None:
        tables = [
            {"name": "fires_dashboard", "column_set": {DATE_COLUMN, AREA_COLUMN}, "years": [2024], "table_year": None},
            {"name": "fires_dashboard_next", "column_set": {DATE_COLUMN, AREA_COLUMN}, "years": [2024], "table_year": None},
        ]
        conn = _PositiveColumnCountsConnection()

        with patch("app.dashboard.distribution.engine.connect", return_value=conn):
            counts = distribution._collect_positive_column_counts(tables, 2024, [AREA_COLUMN, "missing_metric"])

        self.assertEqual(len(conn.queries), 1)
        self.assertIn("UNION ALL", conn.queries[0])
        self.assertIn("positive_column_counts", conn.queries[0])
        self.assertEqual(conn.params, [{"selected_year": 2024}])
        self.assertEqual(counts, {AREA_COLUMN: 3, "missing_metric": 0})

    def test_grouped_bundle_collects_positive_counts_in_same_query(self) -> None:
        table = {
            "name": "fires_dashboard",
            "column_set": {DATE_COLUMN, GENERAL_CAUSE_COLUMN, REGISTERED_DAMAGE_COLUMN},
            "years": [2024],
            "table_year": None,
        }
        conn = _DashboardConnection()

        with patch("app.dashboard.impact.engine.connect", return_value=conn):
            bundle = impact._collect_dashboard_grouped_counts(
                [table],
                2024,
                GENERAL_CAUSE_COLUMN,
                positive_count_columns=[REGISTERED_DAMAGE_COLUMN, "missing_metric"],
            )

        self.assertEqual(len(conn.queries), 1)
        self.assertIn("GROUPING SETS", conn.queries[0])
        self.assertIn("positive_column_bundle", conn.queries[0])
        self.assertIn("GROUP BY GROUPING SETS", conn.queries[0])
        self.assertEqual(conn.params, [{"selected_year": 2024}])
        self.assertEqual(bundle["positive_column_counts"][REGISTERED_DAMAGE_COLUMN], 3)
        self.assertEqual(bundle["positive_column_counts"]["missing_metric"], 0)

    def test_distribution_fallback_keeps_sql_path_and_rejects_unsupported_grouped_counts(self) -> None:
        supported_table = {
            "name": "fires_dashboard",
            "column_set": {DATE_COLUMN, BUILDING_CATEGORY_COLUMN},
            "years": [2024],
            "table_year": None,
        }
        unsupported_table = {
            "name": "fires_without_category",
            "column_set": {DATE_COLUMN, GENERAL_CAUSE_COLUMN},
            "years": [2024],
            "table_year": None,
        }

        self.assertFalse(
            service._can_reuse_distribution_counts(
                [unsupported_table],
                BUILDING_CATEGORY_COLUMN,
                {"Жилое": 2},
            )
        )
        self.assertFalse(
            service._can_reuse_distribution_counts(
                [
                    {
                        "name": "fires_with_other_cause",
                        "column_set": {DATE_COLUMN, BUILDING_CAUSE_COLUMN},
                        "years": [2024],
                        "table_year": None,
                    }
                ],
                GENERAL_CAUSE_COLUMN,
                {"Электрика": 2},
            )
        )

        supported_conn = _DashboardConnection()
        with patch("app.dashboard.distribution.engine.connect", return_value=supported_conn):
            chart = distribution._build_distribution_chart([supported_table], 2024, BUILDING_CATEGORY_COLUMN)

        self.assertEqual([item["label"] for item in chart["items"]], ["Жилое", "Склад"])
        self.assertEqual([item["value"] for item in chart["items"]], [2, 1])
        self.assertEqual(supported_conn.params, [{"selected_year": 2024}])
        self.assertTrue(any("GROUP BY label" in query for query in supported_conn.queries))

        unsupported_conn = _DashboardConnection(distribution_rows=[{"label": "wrong", "fire_count": 99}])
        with patch("app.dashboard.distribution.engine.connect", return_value=unsupported_conn):
            chart = distribution._build_distribution_chart([unsupported_table], 2024, BUILDING_CATEGORY_COLUMN)

        self.assertEqual(chart["items"], [])
        self.assertEqual(unsupported_conn.queries, [])

    def test_select_tables_excludes_benchmark_prefix(self) -> None:
        selected = dashboard_utils._select_tables(
            ["fires_2024", "benchmark_fire_perf_20000_20260403_083825", "tmp_stage_table"]
        )

        self.assertEqual(selected, ["fires_2024"])

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

    def test_service_reuses_grouped_positive_counts_for_damage_dashboard(self) -> None:
        metadata = {
            "tables": [
                {
                    "name": "fires",
                    "column_set": {GENERAL_CAUSE_COLUMN, REGISTERED_DAMAGE_COLUMN},
                    "years": [2024],
                    "table_year": None,
                }
            ],
            "table_options": [{"value": "all", "label": "all"}, {"value": "fires", "label": "fires"}],
            "default_group_column": GENERAL_CAUSE_COLUMN,
            "errors": [],
        }
        empty_chart = {"items": []}
        grouped_counts_bundle = {
            "cause_counts": {"cause_a": 5},
            "distribution_counts": {},
            "month_counts": {1: 2},
            "district_counts": {"district_a": 4},
            "area_bucket_counts": {},
            "positive_column_counts": {REGISTERED_DAMAGE_COLUMN: 3},
            "impact_timeline_rows": [],
        }

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "app.dashboard.service._build_dashboard_summary_series",
                    return_value={
                        "summary": {"fires_count": 3, "year_label": "2024", "tables_used": 1, "tables_used_display": "1"},
                        "yearly_fires_series": {"items": []},
                        "table_breakdown_series": {"items": []},
                    },
                )
            )
            grouped_counts_mock = stack.enter_context(
                patch("app.dashboard.service._collect_dashboard_grouped_counts", return_value=grouped_counts_bundle)
            )
            stack.enter_context(patch("app.dashboard.service._damage_count_columns", return_value=[REGISTERED_DAMAGE_COLUMN]))
            stack.enter_context(
                patch(
                    "app.dashboard.service._collect_damage_counts",
                    side_effect=AssertionError("damage counts should come from grouped bundle"),
                )
            )
            stack.enter_context(
                patch(
                    "app.dashboard.distribution._collect_damage_counts",
                    side_effect=AssertionError("damage chart helpers should reuse provided items"),
                )
            )
            stack.enter_context(patch("app.dashboard.service._build_cause_chart", return_value=empty_chart))
            stack.enter_context(patch("app.dashboard.service._build_trend", return_value={}))
            stack.enter_context(patch("app.dashboard.service._build_rankings", return_value={}))
            stack.enter_context(patch("app.dashboard.service._build_highlights", return_value=[]))
            stack.enter_context(patch("app.dashboard.service._build_dashboard_widgets", return_value={"districts": {"items": []}}))
            stack.enter_context(patch("app.dashboard.service._build_management_snapshot", return_value={}))
            stack.enter_context(
                patch(
                    "app.dashboard.service._build_scope",
                    return_value={"table_label": "fires", "year_label": "2024", "group_label": "damage"},
                )
            )

            aggregation = service._build_dashboard_aggregation(
                metadata=metadata,
                selected_tables=metadata["tables"],
                selected_year=2024,
                selected_group_column=DAMAGE_GROUP_OPTION_VALUE,
                selected_table_name="fires",
                available_years=[{"value": "2024", "label": "2024"}],
                available_group_columns=[{"value": DAMAGE_GROUP_OPTION_VALUE, "label": "damage"}],
            )

        grouped_counts_mock.assert_called_once_with(
            metadata["tables"],
            2024,
            DAMAGE_GROUP_OPTION_VALUE,
            include_area_buckets=False,
            include_impact_timeline=False,
            positive_count_columns=[REGISTERED_DAMAGE_COLUMN],
        )
        self.assertEqual(aggregation["distribution"]["items"][0]["value"], 3)

    def test_damage_dashboard_aggregation_uses_two_sql_queries(self) -> None:
        table = {
            "name": "fires",
            "column_set": {DATE_COLUMN, GENERAL_CAUSE_COLUMN, REGISTERED_DAMAGE_COLUMN},
            "years": [2024],
            "table_year": None,
        }
        metadata = {
            "tables": [table],
            "table_options": [{"value": "all", "label": "all"}, {"value": "fires", "label": "fires"}],
            "default_group_column": GENERAL_CAUSE_COLUMN,
            "errors": [],
        }
        engine = _DashboardAggregationEngine()

        with ExitStack() as stack:
            stack.enter_context(patch("app.dashboard.summary.engine", engine))
            stack.enter_context(patch("app.dashboard.impact.engine", engine))
            stack.enter_context(patch("app.dashboard.distribution.engine", _FailingDashboardEngine()))
            stack.enter_context(patch("app.dashboard.service._damage_count_columns", return_value=[REGISTERED_DAMAGE_COLUMN]))
            stack.enter_context(
                patch(
                    "app.dashboard.service._collect_damage_counts",
                    side_effect=AssertionError("damage counts should come from grouped bundle"),
                )
            )
            stack.enter_context(
                patch(
                    "app.dashboard.distribution._collect_damage_counts",
                    side_effect=AssertionError("damage chart helpers should reuse provided items"),
                )
            )
            stack.enter_context(patch("app.dashboard.service._build_management_snapshot", return_value={}))

            aggregation = service._build_dashboard_aggregation(
                metadata=metadata,
                selected_tables=[table],
                selected_year=2024,
                selected_group_column=DAMAGE_GROUP_OPTION_VALUE,
                selected_table_name="fires",
                available_years=[{"value": "2024", "label": "2024"}],
                available_group_columns=[{"value": DAMAGE_GROUP_OPTION_VALUE, "label": "damage"}],
            )

        self.assertEqual(len(engine.connection.queries), 2)
        self.assertIn("AS table_name", engine.connection.queries[0])
        self.assertIn("GROUPING SETS", engine.connection.queries[1])
        self.assertIn("positive_column_bundle", engine.connection.queries[1])
        self.assertEqual(engine.connection.params, [{}, {"selected_year": 2024}])
        self.assertEqual(aggregation["distribution"]["items"][0]["value"], 3)
        self.assertEqual(aggregation["widgets"]["causes"]["items"][0]["value"], 3)

    def test_service_falls_back_when_distribution_counts_are_empty_or_unsupported(self) -> None:
        self.assertFalse(
            service._can_reuse_distribution_counts(
                [{"name": "fires", "column_set": {BUILDING_CATEGORY_COLUMN}}],
                BUILDING_CATEGORY_COLUMN,
                {},
            )
        )
        self.assertFalse(
            service._can_reuse_distribution_counts(
                [{"name": "fires", "column_set": {GENERAL_CAUSE_COLUMN}}],
                BUILDING_CATEGORY_COLUMN,
                {"category_a": 7},
            )
        )

    def test_dashboard_payload_and_widgets_do_not_emit_mojibake_tokens(self) -> None:
        payload = service._empty_dashboard_data()
        payload["widgets"] = impact._build_sql_widgets(
            [],
            None,
            cause_counts={"Не указано": 3},
            month_counts={1: 2, 7: 1},
            district_counts={"Не указано": 4},
        )
        payload["charts"]["area_buckets"] = impact._build_area_buckets_chart_from_counts(
            {"До 1 га": 2, "Не указано": 1}
        )

        offenders = [text for text in _iter_payload_strings(payload) if MOJIBAKE_TOKEN_RE.search(text)]

        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
