import unittest

from scripts.check_generated_artifacts import is_generated_artifact_path


class RepoHygieneTests(unittest.TestCase):
    def test_generated_artifact_paths_are_detected(self) -> None:
        for path in (
            "app/__pycache__/main.cpython-314.pyc",
            "results/dashboard_8010.err.log",
            "data/uploads/current_upload.xlsx",
            "tmp_ml_test.txt",
            ".venv/Scripts/python.exe",
        ):
            self.assertTrue(is_generated_artifact_path(path), path)

    def test_source_paths_are_not_treated_as_generated(self) -> None:
        for path in (
            "app/main.py",
            "app/services/forecasting/core.py",
            "scripts/benchmark_analytics_perf.py",
            "tests/test_dashboard_analytics_optimizations.py",
            "sample_data/uploads/ekup_Yemelyanovo_2023.xlsx",
        ):
            self.assertFalse(is_generated_artifact_path(path), path)


if __name__ == "__main__":
    unittest.main()
