from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
APP_DIR = BASE_DIR / "app"
CORE_DIR = BASE_DIR / "core"
DATA_DIR = BASE_DIR / "data"
UPLOADS_DIR = DATA_DIR / "uploads"
RESULTS_DIR = DATA_DIR / "results"
TEMPLATES_DIR = APP_DIR / "templates"
STATIC_DIR = APP_DIR / "static"
MAP_TEMPLATES_DIR = CORE_DIR / "mapping" / "templates"


for path in (DATA_DIR, UPLOADS_DIR, RESULTS_DIR, MAP_TEMPLATES_DIR):
    path.mkdir(parents=True, exist_ok=True)


def get_result_folder(project_name: str) -> Path:
    result_folder = RESULTS_DIR / f"folder_{project_name}"
    result_folder.mkdir(parents=True, exist_ok=True)
    return result_folder
