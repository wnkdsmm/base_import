from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.routes import api_router, pages_router
from config.paths import STATIC_DIR


def create_app() -> FastAPI:
    application = FastAPI(title="Fire Data Pipeline")
    application.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    application.include_router(api_router)
    application.include_router(pages_router)
    return application


app = create_app()
