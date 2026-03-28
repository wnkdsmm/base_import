import unittest
from types import SimpleNamespace
from unittest.mock import patch

import app.db_views as db_views


class FakeInspector:
    def __init__(self, columns, pk=None, unique_constraints=None, indexes=None):
        self._columns = columns
        self._pk = pk or {}
        self._unique_constraints = unique_constraints or []
        self._indexes = indexes or []

    def get_columns(self, table_name):
        return list(self._columns)

    def get_pk_constraint(self, table_name):
        return dict(self._pk)

    def get_unique_constraints(self, table_name):
        return list(self._unique_constraints)

    def get_indexes(self, table_name):
        return list(self._indexes)


class FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = list(rows or [])
        self._scalar_value = scalar_value

    def __iter__(self):
        return iter(self._rows)

    def scalar(self):
        return self._scalar_value


class FakeConnection:
    def __init__(self, queries):
        self._queries = queries

    def execute(self, query, params=None):
        sql = str(query)
        self._queries.append((sql, dict(params or {})))
        if "COUNT(*)" in sql:
            return FakeResult(scalar_value=2)
        if 'SELECT "id", "name"' in sql:
            return FakeResult(rows=[(1, "alpha"), (2, "beta")])
        return FakeResult(rows=[("alpha",), ("beta",)])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeEngine:
    def __init__(self, queries, dialect_name="postgresql"):
        self._queries = queries
        self.dialect = SimpleNamespace(name=dialect_name)

    def connect(self):
        return FakeConnection(self._queries)


class TablePaginationOrderTests(unittest.TestCase):
    def setUp(self) -> None:
        db_views.invalidate_table_order_cache()

    def tearDown(self) -> None:
        db_views.invalidate_table_order_cache()

    def test_primary_key_strategy_is_preferred(self) -> None:
        fake_inspector = FakeInspector(
            columns=[
                {"name": "id", "nullable": False},
                {"name": "name", "nullable": True},
            ],
            pk={"constrained_columns": ["id"]},
            indexes=[
                {"name": "uq_name", "unique": True, "column_names": ["name"]},
            ],
        )

        with patch.object(db_views, "inspect", return_value=fake_inspector), patch.object(
            db_views,
            "engine",
            SimpleNamespace(dialect=SimpleNamespace(name="postgresql")),
        ), patch.object(db_views, "get_table_columns_cached", return_value=["id", "name"]):
            strategy = db_views._get_table_order_strategy_cached("fires")

        self.assertEqual(strategy.source, "primary_key")
        self.assertEqual(strategy.order_by_sql, '"id" ASC')

    def test_unique_index_uses_physical_tiebreaker_when_nullable(self) -> None:
        fake_inspector = FakeInspector(
            columns=[
                {"name": "external_id", "nullable": True},
                {"name": "name", "nullable": True},
            ],
            pk={"constrained_columns": []},
            indexes=[
                {"name": "uq_external_id", "unique": True, "column_names": ["external_id"]},
            ],
        )

        with patch.object(db_views, "inspect", return_value=fake_inspector), patch.object(
            db_views,
            "engine",
            SimpleNamespace(dialect=SimpleNamespace(name="postgresql")),
        ), patch.object(db_views, "get_table_columns_cached", return_value=["external_id", "name"]):
            strategy = db_views._get_table_order_strategy_cached("fires")

        self.assertEqual(strategy.source, "unique_key")
        self.assertEqual(strategy.order_by_sql, '"external_id" ASC, ctid ASC')

    def test_preview_and_page_queries_share_the_same_order_clause(self) -> None:
        queries = []
        fake_engine = FakeEngine(queries)
        fake_inspector = FakeInspector(
            columns=[
                {"name": "id", "nullable": False},
                {"name": "name", "nullable": True},
            ],
            pk={"constrained_columns": ["id"]},
        )

        with patch.object(db_views, "inspect", return_value=fake_inspector), patch.object(
            db_views,
            "engine",
            fake_engine,
        ), patch.object(db_views, "get_table_columns_cached", return_value=["id", "name"]):
            preview_columns, preview_rows = db_views.get_table_preview("fires", ["name"], limit=2)
            data_columns, data_rows, total_rows = db_views.get_table_data("fires", limit=2, offset=0)

        ordered_selects = [sql for sql, _params in queries if "COUNT(*)" not in sql]
        self.assertEqual(preview_columns, ["name"])
        self.assertEqual(data_columns, ["id", "name"])
        self.assertEqual(preview_rows, [["alpha"], ["beta"]])
        self.assertEqual(data_rows, [[1, "alpha"], [2, "beta"]])
        self.assertEqual(total_rows, 2)
        self.assertEqual(len(ordered_selects), 2)
        self.assertTrue(all('ORDER BY "id" ASC' in sql for sql in ordered_selects))


if __name__ == "__main__":
    unittest.main()
