from sqlalchemy import create_engine, text
import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:1234@localhost/fires"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

def check_connection():
    """Проверка подключения к БД."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Подключение к БД успешно")
    except Exception as e:
        print("❌ Ошибка подключения:", e)
        raise

if __name__ == "__main__":
    check_connection()