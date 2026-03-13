from config.settings import Settings
from pipeline import Pipeline

from steps.import_data import ImportDataStep
from steps.fires_feature_profiling import FiresFeatureProfilingStep
from steps.keep_important_columns import KeepImportantColumnsStep
from steps.create_clean_table import CreateCleanTableStep
from steps.feature_selection import FeatureSelectionStep
from steps.create_fire_map import CreateFireMapStep


def run_full_pipeline(file_path):

    settings = Settings(file_path)

    pipeline = Pipeline(settings)

    pipeline.add_step(ImportDataStep())
    pipeline.add_step(CreateFireMapStep())
    pipeline.add_step(FiresFeatureProfilingStep())
    pipeline.add_step(KeepImportantColumnsStep())
    pipeline.add_step(CreateCleanTableStep())
    pipeline.add_step(FeatureSelectionStep())

    pipeline.run()