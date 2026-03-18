from config.settings import Settings
from core.processing.pipeline import Pipeline

from core.processing.steps.import_data import ImportDataStep
from core.processing.steps.fires_feature_profiling import FiresFeatureProfilingStep
from core.processing.steps.keep_important_columns import KeepImportantColumnsStep
from core.processing.steps.create_clean_table import CreateCleanTableStep
from core.processing.steps.feature_selection import FeatureSelectionStep
from core.processing.steps.create_fire_map import CreateFireMapStep


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
