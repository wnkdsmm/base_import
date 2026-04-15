from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd

from app.services import pipeline_service
from config.constants import PROFILING_CSV_SUFFIX
from core.processing.steps.create_clean_table import CreateCleanTableStep
from core.processing.steps.fires_feature_profiling import FiresFeatureProfilingStep
from core.processing.steps.import_data import ImportDataStep
from core.processing.steps.keep_important_columns import KeepImportantColumnsStep


class _DummyConnection:
    def __init__(self) -> None:
        self.executed = []

    def execute(self, query, *args, **kwargs):
        self.executed.append((query, args, kwargs))


class _DummyBegin:
    def __init__(self) -> None:
        self.connection = _DummyConnection()

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class _FakeMatcher:
    def match_column_metadata(self, column_name):
        return None

    def get_mandatory_feature_catalog(self):
        return []


class PipelineIoOptimizationTests(unittest.TestCase):
    def test_import_step_keeps_dataframe_in_memory_and_does_not_dispose_engine(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            csv_path = Path(tempdir) / "fires.csv"
            csv_path.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
            settings = SimpleNamespace(
                input_file=str(csv_path),
                output_folder=tempdir,
                project_name="fires",
            )
            begin_ctx = _DummyBegin()

            with (
                patch("core.processing.steps.import_data.engine.begin", return_value=begin_ctx),
                patch("core.processing.steps.import_data.engine.dispose") as dispose_mock,
                patch("pandas.DataFrame.to_sql", autospec=True) as to_sql_mock,
            ):
                step = ImportDataStep()
                step.run(settings)

            dispose_mock.assert_not_called()
            self.assertIsInstance(settings._pipeline_source_df, pd.DataFrame)
            self.assertEqual(settings._pipeline_source_df.shape, (2, 2))
            to_sql_mock.assert_called_once()

    def test_profiling_step_reuses_provided_dataframe_without_sql_read(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            settings = SimpleNamespace(project_name="fires", output_folder=tempdir)
            source_df = pd.DataFrame(
                {
                    "district": ["north", None, "north"],
                    "count": [1, 1, 1],
                }
            )

            with (
                patch("core.processing.steps.fires_feature_profiling.pd.read_sql", side_effect=AssertionError("unexpected SQL read")),
                patch("pandas.DataFrame.to_excel", autospec=True),
            ):
                result = FiresFeatureProfilingStep(settings).run(source_df)

            self.assertEqual(result["total_columns"], 2)
            self.assertIs(result["source_df"], source_df)
            self.assertIs(settings._pipeline_source_df, source_df)
            self.assertTrue((Path(tempdir) / f"fires{PROFILING_CSV_SUFFIX}").exists())

    def test_profiling_numeric_dominant_ratio_excludes_nan_values(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            settings = SimpleNamespace(project_name="fires", output_folder=tempdir)
            source_df = pd.DataFrame(
                {
                    "numeric_col": [None] * 17 + [1.0, 1.0, 2.0],
                }
            )

            with patch("pandas.DataFrame.to_excel", autospec=True):
                result = FiresFeatureProfilingStep(settings).run(source_df)

            profile_df = result["profile_df"]
            row = profile_df.loc[profile_df["column"] == "numeric_col"].iloc[0]
            self.assertEqual(float(row["null_ratio"]), 0.85)
            self.assertAlmostEqual(float(row["dominant_ratio"]), 2.0 / 3.0, places=4)
            self.assertFalse(bool(row["drop_null"]))
            self.assertFalse(bool(row["almost_constant"]))

    def test_profiling_string_unique_count_uses_normalized_values_for_drop_constant(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            settings = SimpleNamespace(project_name="fires", output_folder=tempdir)
            source_df = pd.DataFrame(
                {
                    "flag": ["Да", "да", " ДА "],
                }
            )

            with patch("pandas.DataFrame.to_excel", autospec=True):
                result = FiresFeatureProfilingStep(settings).run(source_df)

            profile_df = result["profile_df"]
            row = profile_df.loc[profile_df["column"] == "flag"].iloc[0]
            self.assertEqual(int(row["unique_count"]), 1)
            self.assertTrue(bool(row["drop_constant"]))

    def test_profiling_numeric_column_with_85_percent_nan_and_same_non_nan_is_almost_constant(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            settings = SimpleNamespace(project_name="fires", output_folder=tempdir)
            source_df = pd.DataFrame(
                {
                    "col": [None] * 85 + [42.0] * 15,
                }
            )

            with patch("pandas.DataFrame.to_excel", autospec=True):
                result = FiresFeatureProfilingStep(settings).run(source_df)

            profile_df = result["profile_df"]
            row = profile_df.loc[profile_df["column"] == "col"].iloc[0]
            self.assertTrue(bool(row["almost_constant"]))

    def test_keep_step_uses_in_memory_profile_without_csv_read(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            settings = SimpleNamespace(project_name="fires", output_folder=tempdir)
            placeholder_report = Path(tempdir) / f"fires{PROFILING_CSV_SUFFIX}"
            placeholder_report.write_text("placeholder\n", encoding="utf-8")
            profile_df = pd.DataFrame(
                {
                    "column": ["district", "unused"],
                    "dtype": ["object", "object"],
                    "null_ratio": [0.0, 1.0],
                    "dominant_ratio": [0.6, 1.0],
                    "unique_count": [2, 1],
                    "variance": [1.0, 1.0],
                    "drop_null": [False, True],
                    "drop_constant": [False, True],
                    "low_variance": [False, False],
                    "almost_constant": [False, True],
                    "candidate_to_drop": [False, True],
                    "profiling_candidate_to_drop": [False, True],
                    "mandatory_feature_detected": [False, False],
                    "protected_from_drop": [False, False],
                    "drop_reasons": [[], ["Много пропусков"]],
                }
            )

            with (
                patch("core.processing.steps.keep_important_columns.get_column_matcher", return_value=_FakeMatcher()),
                patch("core.processing.steps.keep_important_columns.pd.read_csv", side_effect=AssertionError("unexpected CSV read")),
                patch("pandas.DataFrame.to_excel", autospec=True),
            ):
                result = KeepImportantColumnsStep().run(settings, profile_df=profile_df)

            self.assertIn("profile_df", result)
            self.assertTrue(isinstance(settings._pipeline_profile_df, pd.DataFrame))
            self.assertEqual(result["protected_count"], 0)

    def test_create_clean_step_uses_in_memory_source_without_sql_export_read(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            settings = SimpleNamespace(project_name="fires", output_folder=tempdir)
            profile_df = pd.DataFrame(
                {
                    "column": ["district", "count"],
                    "candidate_to_drop": [False, True],
                }
            )
            source_df = pd.DataFrame(
                {
                    "district": ["north", "south"],
                    "count": [1, 2],
                }
            )
            begin_ctx = _DummyBegin()

            with (
                patch("core.processing.steps.create_clean_table.engine.begin", return_value=begin_ctx),
                patch("core.processing.steps.create_clean_table.pd.read_sql", side_effect=AssertionError("unexpected clean table read")),
                patch("pandas.DataFrame.to_excel", autospec=True),
            ):
                result = CreateCleanTableStep().run(settings, profile_df=profile_df, source_df=source_df)

            self.assertEqual(result["rows"], 2)
            self.assertEqual(result["columns"], 1)
            self.assertEqual(result["kept_columns"], ["district"])
            self.assertEqual(len(begin_ctx.connection.executed), 2)

    def test_run_profiling_for_table_passes_intermediate_frames_between_steps(self) -> None:
        job = SimpleNamespace(job_id="job-1")
        settings = SimpleNamespace(project_name="fires", selected_table="fires", output_folder="F:/tmp/fires")
        source_df = pd.DataFrame({"district": ["north"]})
        profile_df = pd.DataFrame({"column": ["district"], "candidate_to_drop": [False]})

        profiling_step = Mock()
        profiling_step.run.return_value = {
            "source_df": source_df,
            "profile_df": profile_df,
            "thresholds": {"null_threshold": 0.5},
            "reason_summary": [{"id": "drop_null", "label": "Много пропусков", "description": "", "count": 0}],
        }
        keep_step = Mock()
        keep_step.run.return_value = {
            "profile_df": profile_df,
            "updated_csv": "updated.csv",
            "updated_xlsx": "updated.xlsx",
            "protected_report_csv": "protected.csv",
            "protected_report_xlsx": "protected.xlsx",
            "mandatory_feature_catalog": [],
        }
        clean_step = Mock()
        clean_step.run.return_value = {
            "clean_table": "clean_fires",
            "kept_columns": ["district"],
            "removed_columns": [],
            "rows": 1,
            "columns": 1,
            "export_file": "clean.xlsx",
        }
        profile_summary = {
            "total_columns": 1,
            "candidates": [],
            "reason_summary": [],
            "candidate_details": [],
            "protected_details": [],
            "thresholds": {"null_threshold": 0.5},
            "report_csv": "updated.csv",
            "report_xlsx": "updated.xlsx",
        }

        with (
            patch.object(pipeline_service.job_store, "create_or_reset_job", return_value=job),
            patch.object(pipeline_service.job_store, "mark_job_status"),
            patch.object(pipeline_service, "Settings", return_value=settings),
            patch.object(pipeline_service, "add_log"),
            patch.object(pipeline_service, "_invalidate_runtime_caches"),
            patch.object(pipeline_service, "FiresFeatureProfilingStep", return_value=profiling_step),
            patch.object(pipeline_service, "KeepImportantColumnsStep", return_value=keep_step),
            patch.object(pipeline_service, "CreateCleanTableStep", return_value=clean_step),
            patch.object(pipeline_service, "_load_profile_summary", return_value=profile_summary) as summary_mock,
        ):
            result = pipeline_service.run_profiling_for_table("session-1", "fires")

        profiling_step.run.assert_called_once_with()
        keep_step.run.assert_called_once_with(settings, profile_df=profile_df)
        summary_mock.assert_called_once_with(
            output_folder=settings.output_folder,
            table_name="fires",
            thresholds={"null_threshold": 0.5},
            base_reason_summary=[{"id": "drop_null", "label": "Много пропусков", "description": "", "count": 0}],
            profile_df=profile_df,
            report_csv="updated.csv",
            report_xlsx="updated.xlsx",
        )
        clean_step.run.assert_called_once_with(settings, profile_df=profile_df, source_df=source_df)
        self.assertEqual(result["status"], "success")


if __name__ == "__main__":
    unittest.main()
