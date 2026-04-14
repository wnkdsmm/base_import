import logging
import os
from typing import Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:1234@localhost/fires",
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

logger = logging.getLogger(__name__)


def check_connection() -> Tuple[bool, str]:
    """Проверка подключения к БД без выброса исключений."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        message = "Подключение к БД установлено"
        logger.info(message)
        return True, message
    except Exception as exc:
        message = f"Не удалось подключиться: {exc}"
        logger.warning(message)
        return False, message


def get_db_info() -> str:
    """Возвращает безопасное описание БД для логов старта."""
    try:
        parsed = make_url(DATABASE_URL)
        backend = parsed.get_backend_name() or "unknown"
        host = parsed.host or "localhost"
        port = parsed.port or 5432
        database = parsed.database or ""
        db_path = f"/{database}" if database else ""
        return f"{backend} @ {host}:{port}{db_path}"
    except Exception:
        return "unknown @ ****"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    success, message = check_connection()
    print("✓" if success else "✗", message)
