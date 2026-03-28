import unittest
from unittest.mock import patch

from app.services.ml_model import jobs as ml_jobs
from app.state import job_store


class MlModelJobsTests(unittest.TestCase):
    def tearDown(self) -> None:
        ml_jobs._ML_JOB_IDS_BY_CACHE_KEY.clear()

    def test_start_ml_model_job_uses_cached_payload(self) -> None:
        session_id = "ml-job-cache"
        request_state = {"cache_key": ("fires", "all", "all", "", 14, "all")}
        cached_payload = {
            "summary": {"selected_table_label": "fires", "backtest_method_label": "Rolling-origin"},
            "quality_assessment": {"title": "quality"},
            "filters": {"table_name": "fires"},
            "notes": [],
            "charts": {},
            "forecast_rows": [],
            "feature_importance": [],
            "features": [],
        }

        with (
            patch.object(ml_jobs, "_build_ml_request_state", return_value=request_state),
            patch.object(ml_jobs, "_cache_get", return_value=cached_payload),
        ):
            payload = ml_jobs.start_ml_model_job(session_id=session_id, table_name="fires")

        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["result"]["summary"]["selected_table_label"], "fires")
        self.assertEqual(payload["backtest_job"]["status"], "completed")
        self.assertFalse(payload["reused"])

    def test_start_ml_model_job_reuses_running_job_for_same_filters(self) -> None:
        session_id = "ml-job-running"
        request_state = {"cache_key": ("fires", "all", "all", "", 14, "all")}

        with (
            patch.object(ml_jobs, "_build_ml_request_state", return_value=request_state),
            patch.object(ml_jobs, "_cache_get", return_value=None),
            patch.object(ml_jobs._ML_JOB_EXECUTOR, "submit", return_value=None) as submit_mock,
        ):
            first = ml_jobs.start_ml_model_job(session_id=session_id, table_name="fires")
            second = ml_jobs.start_ml_model_job(session_id=session_id, table_name="fires")

        self.assertEqual(first["job_id"], second["job_id"])
        self.assertEqual(second["status"], "pending")
        self.assertTrue(second["reused"])
        submit_mock.assert_called_once()

    def test_get_ml_job_status_returns_result_and_backtest_snapshot(self) -> None:
        session_id = "ml-job-status"
        request_state = {"cache_key": ("fires", "all", "all", "", 14, "all")}
        payload = {
            "summary": {
                "selected_table_label": "fires",
                "backtest_method_label": "Rolling-origin backtesting",
                "count_model_label": "Poisson",
                "event_backtest_model_label": "Heuristic",
            },
            "quality_assessment": {"title": "quality"},
            "filters": {"table_name": "fires"},
            "notes": ["done"],
            "charts": {},
            "forecast_rows": [],
            "feature_importance": [],
            "features": [],
        }

        with (
            patch.object(ml_jobs, "_build_ml_request_state", return_value=request_state),
            patch.object(ml_jobs, "_cache_get", return_value=None),
            patch.object(ml_jobs._ML_JOB_EXECUTOR, "submit", return_value=None),
        ):
            started = ml_jobs.start_ml_model_job(session_id=session_id, table_name="fires")

        main_job_id = started["job_id"]
        backtest_job_id = started["backtest_job"]["job_id"]
        params_payload = {
            "table_name": "fires",
            "cause": "all",
            "object_category": "all",
            "temperature": "",
            "forecast_days": "14",
            "history_window": "all",
        }

        def _fake_get_ml_model_data(**kwargs):
            progress_callback = kwargs.get("progress_callback")
            if progress_callback is not None:
                progress_callback("ml_model.running", "Собираем SQL-агрегаты.")
                progress_callback("ml_backtest.running", "Backtesting выполняется.")
                progress_callback("ml_backtest.completed", "Backtesting завершён.")
                progress_callback("ml_model.completed", "ML-анализ завершён.")
            return payload

        with patch.object(ml_jobs, "get_ml_model_data", side_effect=_fake_get_ml_model_data):
            ml_jobs._run_ml_model_job(
                session_id=session_id,
                main_job_id=main_job_id,
                backtest_job_id=backtest_job_id,
                params_payload=params_payload,
                cache_key_token=ml_jobs._serialize_cache_key(request_state["cache_key"]),
            )

        status_payload = ml_jobs.get_ml_job_status(session_id=session_id, job_id=main_job_id)
        self.assertEqual(status_payload["status"], "completed")
        self.assertEqual(status_payload["result"]["summary"]["selected_table_label"], "fires")
        self.assertEqual(status_payload["backtest_job"]["status"], "completed")
        self.assertTrue(any("ML-анализ" in line for line in status_payload["logs"]))
        self.assertEqual(job_store.get_job_status(session_id, job_id=backtest_job_id), "completed")


if __name__ == "__main__":
    unittest.main()
