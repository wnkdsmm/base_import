from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from time import perf_counter
from typing import Any, Callable, Iterable

from sqlalchemy import text

from app.dashboard.cache import _invalidate_dashboard_caches
from app.dashboard.service import get_dashboard_data
from app.db_metadata import invalidate_db_metadata_cache
from app.db_views import get_table_page, get_table_preview
from app.services.clustering.core import clear_clustering_cache, get_clustering_data
from app.services.forecast_risk.core import build_decision_support_payload
from app.services.forecasting.core import clear_forecasting_cache, get_forecasting_data, get_forecasting_shell_context
from app.services.ml_model.core import clear_ml_model_cache, get_ml_model_data
from config.db import engine


@dataclass
class BenchmarkResult:
    name: str
    elapsed_ms: float
    has_data: bool | None
    notes_count: int | None


def _quote(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _build_table_name(row_count: int) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"benchmark_fire_perf_{row_count}_{timestamp}"


def _clear_all_caches() -> None:
    invalidate_db_metadata_cache()
    _invalidate_dashboard_caches()
    clear_forecasting_cache()
    clear_clustering_cache()
    clear_ml_model_cache()


def _create_benchmark_table(table_name: str) -> None:
    create_sql = f"""
    CREATE TABLE {_quote(table_name)} (
        id BIGSERIAL PRIMARY KEY,
        {_quote("Дата возникновения пожара")} DATE NOT NULL,
        {_quote("Район")} TEXT NOT NULL,
        {_quote("Территория")} TEXT NOT NULL,
        {_quote("Населенный пункт")} TEXT NOT NULL,
        {_quote("Причина пожара (общая)")} TEXT NOT NULL,
        {_quote("Категория объекта")} TEXT NOT NULL,
        {_quote("Температура")} DOUBLE PRECISION,
        {_quote("Площадь пожара")} DOUBLE PRECISION,
        {_quote("Вид населенного пункта")} TEXT,
        {_quote("Категория здания")} TEXT,
        {_quote("Категория риска")} TEXT,
        {_quote("Удаленность от ближайшей ПЧ")} DOUBLE PRECISION,
        {_quote("Количество записей о водоснабжении на пожаре")} INTEGER,
        {_quote("Сведения о водоснабжении на пожаре")} TEXT,
        {_quote("Время сообщения")} TEXT,
        {_quote("Время прибытия 1-го ПП")} TEXT,
        {_quote("Наличие последствий пожара")} TEXT,
        {_quote("Зарегистрированный ущерб от пожара")} DOUBLE PRECISION,
        {_quote("Здания (сооружения), уничтожено")} INTEGER,
        {_quote("Площадь м2, уничтожено")} DOUBLE PRECISION,
        {_quote("Количество травмированных в КУП")} INTEGER,
        {_quote("Количество погибших в КУП")} INTEGER,
        {_quote("Эвакуировано на пожаре")} INTEGER
    )
    """
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {_quote(table_name)}"))
        conn.execute(text(create_sql))


def _generate_rows(row_count: int) -> Iterable[dict[str, Any]]:
    districts = [
        "Красноярск",
        "Емельяновский",
        "Минусинский",
        "Канский",
        "Ачинский",
        "Шушенский",
    ]
    causes = [
        "Неосторожное обращение с огнем",
        "Короткое замыкание",
        "Поджог",
        "Печное отопление",
        "Аварийный режим оборудования",
    ]
    object_categories = [
        "Жилой сектор",
        "Производство",
        "Склад",
        "Социальный объект",
        "Открытая территория",
    ]
    settlement_types = ["Город", "Село", "Поселок", "Деревня"]
    building_categories = ["Жилое", "Промышленное", "Социальное", "Складское"]
    risk_categories = ["Высокий", "Средний", "Низкий"]
    start_date = date(2022, 1, 1)

    for index in range(row_count):
        fire_date = start_date + timedelta(days=index % 900)
        district = districts[index % len(districts)]
        cause = causes[index % len(causes)]
        object_category = object_categories[index % len(object_categories)]
        settlement_type = settlement_types[index % len(settlement_types)]
        building_category = building_categories[index % len(building_categories)]
        risk_category = risk_categories[index % len(risk_categories)]
        territory = f"{district} / сектор {index % 48:02d}"

        report_at = datetime.combine(fire_date, time(hour=(index * 7) % 24, minute=(index * 13) % 60))
        arrival_at = report_at + timedelta(minutes=4 + (index % 28))
        temperature = round(-28.0 + ((index * 3) % 70) * 0.9, 1)
        fire_area = round(0.8 + (index % 35) * 1.7, 2)
        water_count = index % 4
        damage = round((index % 17) * 12000.0, 2)
        destroyed_buildings = 1 if index % 37 == 0 else 0
        destroyed_area = round((index % 11) * 18.0, 1)
        injuries = 1 if index % 113 == 0 else 0
        deaths = 1 if index % 997 == 0 else 0
        evacuated = (index % 6) * 2
        consequence = "Да" if (destroyed_buildings or injuries or deaths or damage > 0) else "Нет"

        yield {
            "fire_date": fire_date.isoformat(),
            "district": district,
            "territory": territory,
            "settlement_label": f"НП-{district[:3]}-{index % 48:02d}",
            "cause": cause,
            "object_category": object_category,
            "temperature": temperature,
            "fire_area": fire_area,
            "settlement_type": settlement_type,
            "building_category": building_category,
            "risk_category": risk_category,
            "station_distance": round(1.5 + (index % 23) * 0.8, 2),
            "water_count": water_count,
            "water_details": "есть водоисточник" if water_count > 0 else "отсутствует",
            "report_time": report_at.strftime("%Y-%m-%d %H:%M:%S"),
            "arrival_time": arrival_at.strftime("%Y-%m-%d %H:%M:%S"),
            "consequence": consequence,
            "registered_damage": damage,
            "destroyed_buildings": destroyed_buildings,
            "destroyed_area": destroyed_area,
            "injuries": injuries,
            "deaths": deaths,
            "evacuated": evacuated,
        }


def _populate_benchmark_table(table_name: str, row_count: int, batch_size: int = 5000) -> None:
    insert_sql = text(
        f"""
        INSERT INTO {_quote(table_name)} (
            {_quote("Дата возникновения пожара")},
            {_quote("Район")},
            {_quote("Территория")},
            {_quote("Населенный пункт")},
            {_quote("Причина пожара (общая)")},
            {_quote("Категория объекта")},
            {_quote("Температура")},
            {_quote("Площадь пожара")},
            {_quote("Вид населенного пункта")},
            {_quote("Категория здания")},
            {_quote("Категория риска")},
            {_quote("Удаленность от ближайшей ПЧ")},
            {_quote("Количество записей о водоснабжении на пожаре")},
            {_quote("Сведения о водоснабжении на пожаре")},
            {_quote("Время сообщения")},
            {_quote("Время прибытия 1-го ПП")},
            {_quote("Наличие последствий пожара")},
            {_quote("Зарегистрированный ущерб от пожара")},
            {_quote("Здания (сооружения), уничтожено")},
            {_quote("Площадь м2, уничтожено")},
            {_quote("Количество травмированных в КУП")},
            {_quote("Количество погибших в КУП")},
            {_quote("Эвакуировано на пожаре")}
        ) VALUES (
            :fire_date,
            :district,
            :territory,
            :settlement_label,
            :cause,
            :object_category,
            :temperature,
            :fire_area,
            :settlement_type,
            :building_category,
            :risk_category,
            :station_distance,
            :water_count,
            :water_details,
            :report_time,
            :arrival_time,
            :consequence,
            :registered_damage,
            :destroyed_buildings,
            :destroyed_area,
            :injuries,
            :deaths,
            :evacuated
        )
        """
    )
    buffer: list[dict[str, Any]] = []
    with engine.begin() as conn:
        for row in _generate_rows(row_count):
            buffer.append(row)
            if len(buffer) >= batch_size:
                conn.execute(insert_sql, buffer)
                buffer.clear()
        if buffer:
            conn.execute(insert_sql, buffer)
        conn.execute(text(f"ANALYZE {_quote(table_name)}"))


def _drop_benchmark_table(table_name: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {_quote(table_name)}"))


def _run_case(name: str, fn: Callable[[], Any]) -> BenchmarkResult:
    _clear_all_caches()
    started_at = perf_counter()
    payload = fn()
    elapsed_ms = (perf_counter() - started_at) * 1000.0
    has_data = payload.get("has_data") if isinstance(payload, dict) else None
    notes_count = len(payload.get("notes") or []) if isinstance(payload, dict) else None
    return BenchmarkResult(name=name, elapsed_ms=elapsed_ms, has_data=has_data, notes_count=notes_count)


def run_benchmark(row_count: int, keep_table: bool = False) -> list[BenchmarkResult]:
    if engine.dialect.name != "postgresql":
        raise RuntimeError("Benchmark script currently expects PostgreSQL, because analytics SQL uses PostgreSQL-specific syntax.")

    table_name = _build_table_name(row_count)
    logging.getLogger(__name__).info("Creating benchmark table %s with %s rows", table_name, row_count)
    _create_benchmark_table(table_name)
    _populate_benchmark_table(table_name, row_count)
    invalidate_db_metadata_cache()

    selected_columns = [
        "Дата возникновения пожара",
        "Район",
        "Причина пожара (общая)",
        "Категория объекта",
        "Температура",
    ]

    try:
        results = [
            _run_case("table.preview", lambda: {"has_data": bool(get_table_preview(table_name, selected_columns, limit=100)[1])}),
            _run_case("table.page", lambda: get_table_page(table_name, page=1, page_size=100)),
            _run_case(
                "dashboard",
                lambda: get_dashboard_data(
                    table_name=table_name,
                    year="all",
                    group_column="Причина пожара (общая)",
                ),
            ),
            _run_case(
                "forecasting.shell",
                lambda: get_forecasting_shell_context(
                    table_name=table_name,
                    district="all",
                    cause="all",
                    object_category="all",
                    forecast_days="14",
                    history_window="recent_3",
                )["initial_data"],
            ),
            _run_case(
                "forecasting.base_forecast",
                lambda: get_forecasting_data(
                    table_name=table_name,
                    district="all",
                    cause="all",
                    object_category="all",
                    forecast_days="14",
                    history_window="recent_3",
                    include_decision_support=False,
                ),
            ),
            _run_case(
                "decision_support",
                lambda: build_decision_support_payload(
                    source_tables=[table_name],
                    selected_district="all",
                    selected_cause="all",
                    selected_object_category="all",
                    history_window="recent_3",
                    planning_horizon_days=14,
                ),
            ),
            _run_case(
                "clustering",
                lambda: get_clustering_data(
                    table_name=table_name,
                    cluster_count="4",
                    sample_limit="1000",
                    sampling_strategy="stratified",
                ),
            ),
            _run_case(
                "ml_model",
                lambda: get_ml_model_data(
                    table_name=table_name,
                    cause="all",
                    object_category="all",
                    forecast_days="14",
                    history_window="recent_3",
                ),
            ),
        ]
        return results
    finally:
        if keep_table:
            logging.getLogger(__name__).info("Keeping benchmark table %s", table_name)
        else:
            _drop_benchmark_table(table_name)
            invalidate_db_metadata_cache()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a cold-start performance benchmark for analytics services.")
    parser.add_argument("--rows", type=int, default=50000, help="How many synthetic incidents to generate.")
    parser.add_argument("--keep-table", action="store_true", help="Keep the generated benchmark table after the run.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s %(message)s")
    results = run_benchmark(row_count=max(1000, int(args.rows)), keep_table=bool(args.keep_table))

    print("\nBenchmark summary:")
    for result in results:
        print(
            f"- {result.name}: {result.elapsed_ms:.1f} ms"
            + (f", has_data={result.has_data}" if result.has_data is not None else "")
            + (f", notes={result.notes_count}" if result.notes_count is not None else "")
        )


if __name__ == "__main__":
    main()
