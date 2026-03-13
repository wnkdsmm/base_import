# main.py
import sys
import tkinter as tk
from tkinter import filedialog

from config.settings import Settings
from pipeline import Pipeline
from steps.import_data import ImportDataStep
from steps.fires_feature_profiling import FiresFeatureProfilingStep
from steps.keep_important_columns import KeepImportantColumnsStep
from steps.create_clean_table import CreateCleanTableStep
from steps.feature_selection import FeatureSelectionStep
from steps.create_fire_map import CreateFireMapStep

def choose_file():
    root = tk.Tk()
    root.withdraw()

    file_path = filedialog.askopenfilename(
        title="Выберите файл",
        filetypes=[
            ("Excel и CSV файлы", "*.xlsx *.xls *.csv"),
            ("Excel файлы", "*.xlsx *.xls"),
            ("CSV файлы", "*.csv"),
            ("Все файлы", "*.*")
        ]
    )
    root.destroy()

    if not file_path:
        print("❌ Файл не выбран")
        sys.exit(1)

    return file_path


def main():
    input_file = choose_file()
    settings = Settings(input_file)

    pipeline = Pipeline(settings)
    pipeline.add_step(ImportDataStep())
    pipeline.add_step(CreateFireMapStep())
    pipeline.add_step(FiresFeatureProfilingStep())
    pipeline.add_step(KeepImportantColumnsStep())  # новый шаг интеллектуальной фильтрации
    pipeline.add_step(CreateCleanTableStep())
    pipeline.add_step(FeatureSelectionStep())
    
   

    pipeline.run()


if __name__ == "__main__":
    main()