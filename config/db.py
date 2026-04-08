import logging
import os

from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:1234@localhost/fires"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

logger = logging.getLogger(__name__)


def check_connection():
    """Проверка подключения к БД."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Подключение к БД успешно")
    except Exception:
        logger.exception("Ошибка подключения к БД")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    check_connection()
