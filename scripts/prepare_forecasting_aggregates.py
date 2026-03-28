from __future__ import annotations

import argparse

from app.services.forecasting.data import prepare_forecasting_materialized_views


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Создает или обновляет materialized view для forecasting/ML дневных агрегатов."
    )
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        help="Имя таблицы. Можно передать несколько раз. Если не указано, будут обработаны все forecasting/fire-map таблицы.",
    )
    parser.add_argument(
        "--no-refresh",
        action="store_true",
        help="Не обновлять уже существующие materialized view, только создавать отсутствующие.",
    )
    args = parser.parse_args()

    prepared_views = prepare_forecasting_materialized_views(
        source_tables=args.tables,
        refresh_existing=not args.no_refresh,
    )
    if not prepared_views:
        print("Не удалось подготовить materialized view: либо таблицы не найдены, либо текущая БД не PostgreSQL.")
        return

    print("Подготовлены materialized view:")
    for view_name in prepared_views:
        print(f" - {view_name}")


if __name__ == "__main__":
    main()
