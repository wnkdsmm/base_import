import asyncio
import html
import json
import os
import re
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from sqlalchemy import create_engine, text
from starlette.requests import Request

import app.db_metadata as db_metadata
import app.db_views as db_views
from app.routes import api as api_routes
from app.routes import pages as pages_routes


class TablePaginationRoutesFixture(unittest.TestCase):
    PAGE_SIZE = 50
    TOTAL_ROWS = 120
    _TAG_RE = re.compile(r"<[^>]+>")

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

    def _decode_html_text(self, value: str) -> str:
        return html.unescape(self._TAG_RE.sub("", value)).strip()

    def _build_request(self, path: str) -> Request:
        return Request(
            {
                "type": "http",
                "method": "GET",
                "path": path,
                "headers": [],
                "query_string": b"",
            }
        )

    def _get_api_page_labels(self, table_name: str, page: int) -> list[str]:
        response = api_routes.table_page_endpoint(
            table_name=table_name,
            page=page,
            page_size=self.PAGE_SIZE,
        )
        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body.decode("utf-8"))
        self.assertTrue(payload["ok"])
        label_index = payload["columns"].index("label")
        return [row[label_index] for row in payload["rows"]]

    def _get_html_page_labels(self, table_name: str, page: int) -> list[str]:
        response = asyncio.run(
            pages_routes.view_table(
                request=self._build_request(f"/tables/{table_name}"),
                table_name=table_name,
                page=page,
                page_size=self.PAGE_SIZE,
            )
        )
        self.assertEqual(response.status_code, 200)
        response_html = response.body.decode("utf-8")

        body_match = re.search(r'<tbody id="tableDataBody">(.*?)</tbody>', response_html, re.S)
        self.assertIsNotNone(body_match)
        body_html = body_match.group(1)
        row_html_items = re.findall(r"<tr[^>]*>(.*?)</tr>", body_html, re.S)
        rows = []
        for row_html in row_html_items:
            cells = [self._decode_html_text(cell) for cell in re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.S)]
            if cells:
                rows.append(cells)

        head_match = re.search(r'<thead id="tableDataHead">(.*?)</thead>', response_html, re.S)
        self.assertIsNotNone(head_match)
        headers = [self._decode_html_text(cell) for cell in re.findall(r"<th[^>]*>(.*?)</th>", head_match.group(1), re.S)]
        label_index = headers.index("label")
        return [row[label_index] for row in rows]

    def _assert_route_page_sequence(self, table_name: str, expected_labels: list[str]) -> None:
        api_page_1 = self._get_api_page_labels(table_name, 1)
        api_page_2 = self._get_api_page_labels(table_name, 2)
        api_page_3 = self._get_api_page_labels(table_name, 3)
        api_page_2_repeat = self._get_api_page_labels(table_name, 2)

        html_page_1 = self._get_html_page_labels(table_name, 1)
        html_page_2 = self._get_html_page_labels(table_name, 2)
        html_page_3 = self._get_html_page_labels(table_name, 3)
        html_page_2_repeat = self._get_html_page_labels(table_name, 2)

        self.assertEqual(api_page_2, api_page_2_repeat)
        self.assertEqual(html_page_2, html_page_2_repeat)
        self.assertFalse(set(api_page_1) & set(api_page_2))
        self.assertFalse(set(api_page_2) & set(api_page_3))
        self.assertFalse(set(api_page_1) & set(api_page_3))
        self.assertEqual(api_page_1 + api_page_2 + api_page_3, expected_labels)
        self.assertEqual(html_page_1 + html_page_2 + html_page_3, expected_labels)
        self.assertEqual(api_page_1, html_page_1)
        self.assertEqual(api_page_2, html_page_2)
        self.assertEqual(api_page_3, html_page_3)


class TablePaginationRoutesTests(TablePaginationRoutesFixture):
    def test_api_and_html_routes_keep_primary_key_pages_stable(self) -> None:
        self._assert_route_page_sequence("table_with_pk", self.expected_labels["table_with_pk"])

    def test_api_and_html_routes_keep_unique_index_pages_stable(self) -> None:
        self._assert_route_page_sequence("table_with_unique", self.expected_labels["table_with_unique"])

    def test_api_and_html_routes_keep_fallback_pages_stable(self) -> None:
        self._assert_route_page_sequence("table_without_keys", self.expected_labels["table_without_keys"])


if __name__ == "__main__":
    unittest.main()
