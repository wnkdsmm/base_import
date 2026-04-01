from __future__ import annotations

from fastapi import APIRouter, Body, Request

from app.services.forecasting.core import (
    get_forecasting_data,
    get_forecasting_decision_support_data,
    get_forecasting_metadata,
)
from app.services.forecasting.jobs import (
    get_forecasting_decision_support_job_status,
    start_forecasting_decision_support_job,
)

from .api_common import ensure_session_id, run_analytics_request, utf8_json


router = APIRouter()


@router.get("/api/forecasting-data")
def forecasting_data_endpoint(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
    include_decision_support: bool = True,
):
    def action():
        if include_decision_support:
            return get_forecasting_decision_support_data(
                table_name=table_name,
                district=district,
                cause=cause,
                object_category=object_category,
                temperature=temperature,
                forecast_days=forecast_days,
                history_window=history_window,
            )
        return get_forecasting_data(
            table_name=table_name,
            district=district,
            cause=cause,
            object_category=object_category,
            temperature=temperature,
            forecast_days=forecast_days,
            history_window=history_window,
            include_decision_support=False,
        )

    return run_analytics_request(
        action,
        invalid_code="forecasting_invalid_request",
        invalid_message="Некорректные параметры прогноза.",
        failed_code="forecasting_failed",
        failed_message="Не удалось собрать данные прогноза. Попробуйте повторить запрос.",
    )


@router.get("/api/forecasting-metadata")
def forecasting_metadata_endpoint(
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
):
    return run_analytics_request(
        lambda: get_forecasting_metadata(
            table_name=table_name,
            district=district,
            cause=cause,
            object_category=object_category,
            temperature=temperature,
            forecast_days=forecast_days,
            history_window=history_window,
        ),
        invalid_code="forecasting_metadata_invalid_request",
        invalid_message="Не удалось обработать параметры загрузки фильтров и признаков.",
        failed_code="forecasting_metadata_failed",
        failed_message="Не удалось загрузить фильтры и признаки прогноза. Попробуйте повторить запрос.",
    )


@router.post("/api/forecasting-decision-support-jobs")
def start_forecasting_decision_support_job_endpoint(request: Request, payload: dict = Body(...)):
    session_id = ensure_session_id(request)

    def action():
        return start_forecasting_decision_support_job(
            session_id=session_id,
            table_name=str(payload.get("table_name") or "all"),
            district=str(payload.get("district") or "all"),
            cause=str(payload.get("cause") or "all"),
            object_category=str(payload.get("object_category") or "all"),
            temperature=str(payload.get("temperature") or ""),
            forecast_days=str(payload.get("forecast_days") or "14"),
            history_window=str(payload.get("history_window") or "all"),
        )

    result = run_analytics_request(
        action,
        invalid_code="forecasting_decision_support_invalid_request",
        invalid_message="Некорректные параметры для фонового блока поддержки решений.",
        failed_code="forecasting_decision_support_failed",
        failed_message="Не удалось запустить фоновый расчет блока поддержки решений. Попробуйте повторить запрос.",
    )
    if isinstance(result, dict):
        return utf8_json(result, session_id=session_id)
    return result


@router.get("/api/forecasting-decision-support-jobs/{job_id}")
def forecasting_decision_support_job_status_endpoint(request: Request, job_id: str):
    session_id = ensure_session_id(request)
    result = get_forecasting_decision_support_job_status(session_id=session_id, job_id=job_id)
    status_code = 404 if result.get("status") == "missing" else 200
    return utf8_json(result, status_code=status_code, session_id=session_id)
