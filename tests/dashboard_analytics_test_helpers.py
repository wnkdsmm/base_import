from __future__ import annotations

from datetime import date
import re

from app.statistics_constants import IMPACT_METRIC_CONFIG


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
