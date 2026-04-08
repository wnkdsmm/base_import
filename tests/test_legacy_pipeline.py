import ast
from pathlib import Path
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from core.processing.pipeline import Pipeline, PipelineStep
import main


class _RecordingStep(PipelineStep):
    def __init__(self, name, calls):
        super().__init__(name)
        self._calls = calls

    def run(self, settings):
        self._calls.append((self.name, settings.project_name))


class LegacyDesktopPipelineTests(unittest.TestCase):
    def test_legacy_runtime_code_uses_logging_instead_of_print(self):
        step_files = [
            "config/db.py",
            "core/processing/steps/create_clean_table.py",
            "core/processing/steps/create_fire_map.py",
            "core/processing/steps/fires_feature_profiling.py",
            "core/processing/steps/import_data.py",
            "core/processing/steps/keep_important_columns.py",
        ]

        print_calls = []
        for relative_path in step_files:
            path = Path(relative_path)
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "print":
                    print_calls.append(f"{relative_path}:{node.lineno}")

        self.assertEqual(print_calls, [])

    def test_cli_logging_uses_plain_info_messages_when_unconfigured(self):
        fake_root_logger = SimpleNamespace(handlers=[])
        with patch.object(main.logging, "getLogger", return_value=fake_root_logger), patch.object(main.logging, "basicConfig") as basic_config:
            main._configure_cli_logging()

        basic_config.assert_called_once_with(level=main.logging.INFO, format="%(message)s")

    def test_cli_logging_does_not_override_existing_handlers(self):
        fake_root_logger = SimpleNamespace(handlers=[object()])
        with patch.object(main.logging, "getLogger", return_value=fake_root_logger), patch.object(main.logging, "basicConfig") as basic_config:
            main._configure_cli_logging()

        basic_config.assert_not_called()

    def test_pipeline_runs_steps_sequentially_and_logs_progress(self):
        calls = []
        settings = SimpleNamespace(project_name="desktop-smoke")
        pipeline = Pipeline(settings)
        pipeline.add_step(_RecordingStep("first", calls))
        pipeline.add_step(_RecordingStep("second", calls))

        with self.assertLogs("core.processing.pipeline", level="INFO") as logs:
            pipeline.run()

        self.assertEqual(calls, [("first", "desktop-smoke"), ("second", "desktop-smoke")])
        self.assertIn("Шаг: first", "\n".join(logs.output))
        self.assertIn("Шаг: second", "\n".join(logs.output))
        self.assertIn("Конвейер завершён: desktop-smoke", "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()
