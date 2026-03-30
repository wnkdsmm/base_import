import os
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from sqlalchemy import create_engine, text

import app.db_metadata as db_metadata
import app.db_views as db_views


class SqliteTablePaginationFixture(unittest.TestCase):
    PAGE_SIZE = 50
    TOTAL_ROWS = 120

    def setUp(self) -> None:
        super().setUp()
        temp_db = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
        temp_db.close()
        self.db_path = temp_db.name
        self.addCleanup(self._cleanup_db_file)
        self.sqlite_engine = create_engine(
            f"sqlite:///{self.db_path}",
            connect_args={"check_same_thread": False},
            pool_pre_ping=True,
        )
        self.addCleanup(self.sqlite_engine.dispose)

        self._patches = ExitStack()
        self._patches.enter_context(patch.object(db_views, "engine", self.sqlite_engine))
        self._patches.enter_context(patch.object(db_metadata, "engine", self.sqlite_engine))
        self.addCleanup(self._patches.close)
        self.addCleanup(db_views.invalidate_table_order_cache)
        self.addCleanup(db_metadata.invalidate_db_metadata_cache)

        self.expected_labels = self._create_test_tables()
        db_views.invalidate_table_order_cache()
        db_metadata.invalidate_db_metadata_cache()

    def _cleanup_db_file(self) -> None:
        try:
            os.remove(self.db_path)
        except FileNotFoundError:
            pass

    def _create_test_tables(self) -> dict[str, list[str]]:
        pk_labels = [f"pk_{index:03d}" for index in range(1, self.TOTAL_ROWS + 1)]
        unique_labels = [f"uniq_{index:03d}" for index in range(1, self.TOTAL_ROWS + 1)]
        fallback_labels = [f"fallback_{index:03d}" for index in range(self.TOTAL_ROWS, 0, -1)]

        with self.sqlite_engine.begin() as conn:
            conn.execute(text('CREATE TABLE "table_with_pk" (id INTEGER PRIMARY KEY, label TEXT NOT NULL)'))
            for index in range(self.TOTAL_ROWS, 0, -1):
                conn.execute(
                    text('INSERT INTO "table_with_pk" (id, label) VALUES (:id, :label)'),
                    {"id": index, "label": f"pk_{index:03d}"},
                )

            conn.execute(text('CREATE TABLE "table_with_unique" (code TEXT NOT NULL, label TEXT NOT NULL)'))
            conn.execute(text('CREATE UNIQUE INDEX "uq_table_with_unique_code" ON "table_with_unique" (code)'))
            for index in range(self.TOTAL_ROWS, 0, -1):
                conn.execute(
                    text('INSERT INTO "table_with_unique" (code, label) VALUES (:code, :label)'),
                    {"code": f"code_{index:03d}", "label": f"uniq_{index:03d}"},
                )

            conn.execute(text('CREATE TABLE "table_without_keys" (label TEXT NOT NULL, bucket TEXT NOT NULL)'))
            for index in range(self.TOTAL_ROWS, 0, -1):
                conn.execute(
                    text('INSERT INTO "table_without_keys" (label, bucket) VALUES (:label, :bucket)'),
                    {"label": f"fallback_{index:03d}", "bucket": f"group_{index % 5}"},
                )

        return {
            "table_with_pk": pk_labels,
            "table_with_unique": unique_labels,
            "table_without_keys": fallback_labels,
        }

    def _page_labels(self, table_name: str, page: int) -> list[str]:
        payload = db_views.get_table_page(table_name, page=page, page_size=self.PAGE_SIZE)
        label_index = payload["columns"].index("label")
        return [row[label_index] for row in payload["rows"]]

    def _preview_labels(self, table_name: str) -> list[str]:
        columns, rows = db_views.get_table_preview(table_name, ["label"], limit=self.TOTAL_ROWS)
        self.assertEqual(columns, ["label"])
        return [row[0] for row in rows]

    def _assert_non_overlapping_complete_pages(self, table_name: str, expected_labels: list[str]) -> None:
        page_1 = self._page_labels(table_name, 1)
        page_2 = self._page_labels(table_name, 2)
        page_3 = self._page_labels(table_name, 3)
        repeated_page_2 = self._page_labels(table_name, 2)
        preview_labels = self._preview_labels(table_name)

        self.assertEqual(len(page_1), self.PAGE_SIZE)
        self.assertEqual(len(page_2), self.PAGE_SIZE)
        self.assertEqual(len(page_3), self.TOTAL_ROWS - (self.PAGE_SIZE * 2))
        self.assertEqual(page_2, repeated_page_2)
        self.assertFalse(set(page_1) & set(page_2))
        self.assertFalse(set(page_2) & set(page_3))
        self.assertFalse(set(page_1) & set(page_3))
        self.assertEqual(page_1 + page_2 + page_3, expected_labels)
        self.assertEqual(preview_labels, expected_labels)
        self.assertEqual(preview_labels, page_1 + page_2 + page_3)


class TablePaginationDbViewsTests(SqliteTablePaginationFixture):
    def test_primary_key_table_uses_primary_key_order_without_page_drift(self) -> None:
        strategy = db_views._get_table_order_strategy_cached("table_with_pk")

        self.assertEqual(strategy.source, "primary_key")
        self.assertEqual(strategy.order_by_sql, '"id" ASC')
        self._assert_non_overlapping_complete_pages("table_with_pk", self.expected_labels["table_with_pk"])

    def test_unique_index_table_uses_unique_order_without_page_drift(self) -> None:
        strategy = db_views._get_table_order_strategy_cached("table_with_unique")

        self.assertEqual(strategy.source, "unique_key")
        self.assertEqual(strategy.order_by_sql, '"code" ASC')
        self._assert_non_overlapping_complete_pages("table_with_unique", self.expected_labels["table_with_unique"])

    def test_table_without_keys_uses_safe_rowid_fallback_without_page_drift(self) -> None:
        strategy = db_views._get_table_order_strategy_cached("table_without_keys")

        self.assertEqual(strategy.source, "physical_row_fallback")
        self.assertEqual(strategy.order_by_sql, "rowid ASC")
        self.assertIn("physical row identifier", strategy.note or "")
        self._assert_non_overlapping_complete_pages("table_without_keys", self.expected_labels["table_without_keys"])


if __name__ == "__main__":
    unittest.main()
