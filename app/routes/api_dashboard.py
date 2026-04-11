from __future__ import annotations

from fastapi import APIRouter

from .api_common import run_analytics_request


router = APIRouter()


def get_dashboard_data(**kwargs):
    from app.dashboard.service import get_dashboard_data as _get_dashboard_data

    return _get_dashboard_data(**kwargs)


@router.get("/api/dashboard-data")
def dashboard_data_endpoint(table_name: str = "all", year: str = "all", group_column: str = ""):
    return run_analytics_request(
        lambda: get_dashboard_data(
            table_name=table_name,
            year=year,
            group_column=group_column,
            allow_fallback=False,
        ),
        invalid_code="dashboard_invalid_request",
        invalid_message="Не удалось обработать параметры dashboard.",
        failed_code="dashboard_failed",
        failed_message="Не удалось обновить dashboard. Попробуйте повторить запрос.",
    )
