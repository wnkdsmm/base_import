# main.py
import logging
import sys
import tkinter as tk
from tkinter import filedialog

from config.settings import Settings
from core.processing.pipeline import Pipeline
from core.processing.steps.create_clean_table import CreateCleanTableStep
from core.processing.steps.create_fire_map import CreateFireMapStep
from core.processing.steps.fires_feature_profiling import FiresFeatureProfilingStep
from core.processing.steps.import_data import ImportDataStep
from core.processing.steps.keep_important_columns import KeepImportantColumnsStep


logger = logging.getLogger(__name__)


def _configure_cli_logging():
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")


def choose_file():
    root = tk.Tk()
    root.withdraw()

    file_path = filedialog.askopenfilename(
        title="Select file",
        filetypes=[
            ("Excel and CSV files", "*.xlsx *.xls *.csv"),
            ("Excel files", "*.xlsx *.xls"),
            ("CSV files", "*.csv"),
            ("All files", "*.*"),
        ],
    )
    root.destroy()

    if not file_path:
        logger.info("No file selected")
        sys.exit(1)

    return file_path


def main():
    _configure_cli_logging()
    input_file = choose_file()
    settings = Settings(input_file)

    pipeline = Pipeline(settings)
    pipeline.add_step(ImportDataStep())
    pipeline.add_step(CreateFireMapStep())
    pipeline.add_step(FiresFeatureProfilingStep(settings))
    pipeline.add_step(KeepImportantColumnsStep())
    pipeline.add_step(CreateCleanTableStep())

    pipeline.run()


if __name__ == "__main__":
    main()
