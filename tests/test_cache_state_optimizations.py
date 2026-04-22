import unittest
from unittest.mock import patch

from app.dashboard import cache as dashboard_cache
from app.cache import CopyingTtlCache, clone_mutable_payload, freeze_mutable_payload
import app.services.forecasting.forecasting_pipeline as forecasting_core
from app.services.ml_model import core as ml_core
from app.state import JobStore


class RuntimeCacheSnapshotTests(unittest.TestCase):
    def test_freeze_thaw_cache_isolates_nested_mutations(self) -> None:
        cache = CopyingTtlCache[str, dict](
            ttl_seconds=60.0,
            storer=freeze_mutable_payload,
            loader=clone_mutable_payload,
        )
        payload = {
            "items": [{"value": 1}],
            "meta": {"tags": {"fires"}},
        }

        cache.set("payload", payload)
        payload["items"][0]["value"] = 99
        payload["meta"]["tags"].add("mutated")

        first = cache.get("payload")
        self.assertIsNotNone(first)
        self.assertEqual(first["items"][0]["value"], 1)
        self.assertEqual(first["meta"]["tags"], {"fires"})

        first["items"][0]["value"] = 7
        first["meta"]["tags"].add("client")

        second = cache.get("payload")
        self.assertEqual(second["items"][0]["value"], 1)
        self.assertEqual(second["meta"]["tags"], {"fires"})


class JobStoreSnapshotTests(unittest.TestCase):
    def test_job_store_preserves_mutation_boundaries_for_result_and_meta(self) -> None:
        store = JobStore()
        session_id = store.ensure_session("snapshot-session")
        job = store.create_or_reset_job(session_id, kind="forecasting")
        result = {
            "summary": {"selected_table_label": "fires"},
            "rows": [{"fires": 3}],
        }
        meta = {
            "params": {"table_name": "fires"},
            "steps": ["queued"],
        }

        store.set_job_result(session_id, job.job_id, result)
        store.update_job_meta(session_id, job.job_id, **meta)

        result["summary"]["selected_table_label"] = "mutated"
        result["rows"][0]["fires"] = 999
        meta["params"]["table_name"] = "mutated"
        meta["steps"].append("mutated")

        stored_result = store.get_job_result(session_id, job.job_id)
        snapshot = store.get_job_snapshot(session_id, job_id=job.job_id)

        self.assertEqual(stored_result["summary"]["selected_table_label"], "fires")
        self.assertEqual(stored_result["rows"][0]["fires"], 3)
        self.assertEqual(snapshot["meta"]["params"]["table_name"], "fires")
        self.assertEqual(snapshot["meta"]["steps"], ["queued"])

        stored_result["rows"][0]["fires"] = 5
        snapshot["meta"]["steps"].append("client")

        reloaded_result = store.get_job_result(session_id, job.job_id)
        reloaded_snapshot = store.get_job_snapshot(session_id, job_id=job.job_id)

        self.assertEqual(reloaded_result["rows"][0]["fires"], 3)
        self.assertEqual(reloaded_snapshot["meta"]["steps"], ["queued"])


class DashboardCacheSnapshotTests(unittest.TestCase):
    def tearDown(self) -> None:
        dashboard_cache._invalidate_dashboard_caches()

    def test_dashboard_metadata_cache_isolated_from_returned_mutations(self) -> None:
        metadata = {
            "table_signature": ("fires",),
            "tables": [
                {
                    "name": "fires",
                    "column_set": {"fire_date"},
                }
            ],
        }

        with (
            patch.object(dashboard_cache, "_current_dashboard_table_names", return_value=("fires",)),
            patch.object(dashboard_cache, "_collect_dashboard_metadata", return_value=metadata) as collect_mock,
        ):
            first = dashboard_cache._collect_dashboard_metadata_cached()
            first["tables"][0]["name"] = "mutated"
            first["tables"][0]["column_set"].add("mutated")

            second = dashboard_cache._collect_dashboard_metadata_cached()

        collect_mock.assert_called_once()
        self.assertEqual(second["tables"][0]["name"], "fires")
        self.assertEqual(second["tables"][0]["column_set"], {"fire_date"})


class MlCacheSnapshotTests(unittest.TestCase):
    def tearDown(self) -> None:
        ml_core.clear_ml_model_cache()

    def test_ml_cache_returns_fresh_payloads_after_store(self) -> None:
        cache_key = (99, "fires", "all", "all", "", 14, "all")
        payload = {
            "summary": {"selected_table_label": "fires"},
            "quality_assessment": {"count_table": {"rows": [{"method_label": "cached"}]}},
            "filters": {"table_name": "fires"},
            "notes": ["ready"],
            "charts": {},
            "forecast_rows": [],
            "feature_importance": [],
            "features": [],
        }

        stored = ml_core._cache_store(cache_key, payload)
        self.assertIs(stored, payload)

        payload["summary"]["selected_table_label"] = "mutated"
        first = ml_core._cache_get(cache_key)
        self.assertEqual(first["summary"]["selected_table_label"], "fires")

        first["summary"]["selected_table_label"] = "client"
        second = ml_core._cache_get(cache_key)
        self.assertEqual(second["summary"]["selected_table_label"], "fires")


class ForecastingCacheSnapshotTests(unittest.TestCase):
    def tearDown(self) -> None:
        forecasting_core.clear_forecasting_cache()

    def test_forecasting_cache_uses_frozen_storage_with_mutable_reads(self) -> None:
        cache_key = ("fires", "north", "grass", "warehouse", "", "14", "all", "full")
        payload = {
            "summary": {"selected_table_label": "fires"},
            "filters": {"available_tables": [{"value": "fires", "label": "fires"}]},
            "notes": ["ready"],
            "risk_prediction": {"territories": []},
            "charts": {"daily": {"series": [1, 2, 3]}},
            "has_data": True,
        }

        forecasting_core._FORECASTING_CACHE.set(cache_key, payload)
        payload["notes"].append("mutated")

        first = forecasting_core._FORECASTING_CACHE.get(cache_key)
        self.assertEqual(first["notes"], ["ready"])

        first["charts"]["daily"]["series"].append(4)
        second = forecasting_core._FORECASTING_CACHE.get(cache_key)
        self.assertEqual(second["charts"]["daily"]["series"], [1, 2, 3])


if __name__ == "__main__":
    unittest.main()
