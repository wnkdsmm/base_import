import asyncio

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.routes.api import router as api_router
from app.routes.pages import router as pages_router
from app.runtime_invalidation import warmup_runtime_caches
from config.db import check_connection, get_db_info
from config.paths import STATIC_DIR


def create_app() -> FastAPI:
    application = FastAPI(title="Fire Data Pipeline")
    application.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    application.include_router(api_router)
    application.include_router(pages_router)
    return application


app = create_app()


@app.on_event("startup")
async def startup_event() -> None:
    success, message = check_connection()
    if success:
        print(f"[OK] {message} ({get_db_info()})")
        asyncio.create_task(
            asyncio.to_thread(warmup_runtime_caches, lambda w: print(f"[WARN] {w}"))
        )
    else:
        print(f"[ERROR] {message}")
        print("  Проверьте настройки DATABASE_URL в файле .env")
