import unittest
from unittest.mock import patch

from app.services.clustering import jobs as clustering_jobs
from app.services.forecasting import jobs as forecasting_jobs


class ClusteringJobsTests(unittest.TestCase):
    def tearDown(self) -> None:
        clustering_jobs._CLUSTERING_JOB_IDS_BY_CACHE_KEY.clear()

    def test_start_clustering_job_uses_cached_payload(self) -> None:
        session_id = "clustering-job-cache"
        request_state = {
            "cache_key": ("fires", "4", "1000", "stratified"),
            "table_options": [],
            "selected_table": "fires",
            "cluster_count": 4,
            "sample_limit": 1000,
            "sampling_strategy": "stratified",
            "feature_columns": [],
        }
        cached_payload = {
            "summary": {"selected_table_label": "fires"},
            "filters": {"table_name": "fires"},
            "charts": {},
            "notes": [],
        }

        with (
            patch.object(clustering_jobs, "_build_clustering_request_state", return_value=request_state),
            patch.object(clustering_jobs._CLUSTERING_CACHE, "get", return_value=cached_payload),
        ):
            payload = clustering_jobs.start_clustering_job(session_id=session_id, table_name="fires")

        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["result"]["summary"]["selected_table_label"], "fires")
        self.assertFalse(payload["reused"])

    def test_start_clustering_job_reuses_running_job_for_same_filters(self) -> None:
        session_id = "clustering-job-running"
        request_state = {
            "cache_key": ("fires", "4", "1000", "stratified"),
            "table_options": [],
            "selected_table": "fires",
            "cluster_count": 4,
            "sample_limit": 1000,
            "sampling_strategy": "stratified",
            "feature_columns": [],
        }

        with (
            patch.object(clustering_jobs, "_build_clustering_request_state", return_value=request_state),
            patch.object(clustering_jobs._CLUSTERING_CACHE, "get", return_value=None),
            patch.object(clustering_jobs._CLUSTERING_JOB_EXECUTOR, "submit", return_value=None) as submit_mock,
        ):
            first = clustering_jobs.start_clustering_job(session_id=session_id, table_name="fires")
            second = clustering_jobs.start_clustering_job(session_id=session_id, table_name="fires")

        self.assertEqual(first["job_id"], second["job_id"])
        self.assertEqual(second["status"], "pending")
        self.assertTrue(second["reused"])
        submit_mock.assert_called_once()


class ForecastingDecisionSupportJobsTests(unittest.TestCase):
    def tearDown(self) -> None:
        forecasting_jobs._FORECASTING_DECISION_SUPPORT_JOB_IDS_BY_CACHE_KEY.clear()

    def test_start_forecasting_decision_support_job_uses_cached_payload(self) -> None:
        session_id = "forecasting-ds-cache"
        request_state = {
            "cache_key": ("fires", "all", "all", "all", "", "14", "all", "full"),
            "table_options": [],
            "selected_table": "fires",
            "source_tables": ["fires"],
            "days_ahead": 14,
            "history_window": "all",
        }
        cached_payload = {
            "summary": {"selected_table_label": "fires"},
            "filters": {"table_name": "fires"},
            "risk_prediction": {"territories": []},
            "charts": {},
            "notes": [],
        }

        with (
            patch.object(forecasting_jobs, "_build_forecasting_request_state", return_value=request_state),
            patch.object(forecasting_jobs._FORECASTING_CACHE, "get", return_value=cached_payload),
        ):
            payload = forecasting_jobs.start_forecasting_decision_support_job(session_id=session_id, table_name="fires")

        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["result"]["summary"]["selected_table_label"], "fires")
        self.assertFalse(payload["reused"])

    def test_start_forecasting_decision_support_job_reuses_running_job_for_same_filters(self) -> None:
        session_id = "forecasting-ds-running"
        request_state = {
            "cache_key": ("fires", "all", "all", "all", "", "14", "all", "full"),
            "table_options": [],
            "selected_table": "fires",
            "source_tables": ["fires"],
            "days_ahead": 14,
            "history_window": "all",
        }

        with (
            patch.object(forecasting_jobs, "_build_forecasting_request_state", return_value=request_state),
            patch.object(forecasting_jobs._FORECASTING_CACHE, "get", return_value=None),
            patch.object(forecasting_jobs._FORECASTING_DECISION_SUPPORT_EXECUTOR, "submit", return_value=None) as submit_mock,
        ):
            first = forecasting_jobs.start_forecasting_decision_support_job(session_id=session_id, table_name="fires")
            second = forecasting_jobs.start_forecasting_decision_support_job(session_id=session_id, table_name="fires")

        self.assertEqual(first["job_id"], second["job_id"])
        self.assertEqual(second["status"], "pending")
        self.assertTrue(second["reused"])
        submit_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
