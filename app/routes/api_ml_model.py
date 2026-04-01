from __future__ import annotations

from fastapi import APIRouter, Body, Request

from app.services.ml_model.core import get_ml_model_data
from app.services.ml_model.jobs import get_ml_job_status, start_ml_model_job

from .api_common import ensure_session_id, run_analytics_request, utf8_json


router = APIRouter()


@router.get("/api/ml-model-data")
def ml_model_data_endpoint(
    table_name: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
):
    return run_analytics_request(
        lambda: get_ml_model_data(
            table_name=table_name,
            cause=cause,
            object_category=object_category,
            temperature=temperature,
            forecast_days=forecast_days,
            history_window=history_window,
        ),
        invalid_code="ml_model_invalid_request",
        invalid_message="Не удалось обработать параметры ML-анализа.",
        failed_code="ml_model_failed",
        failed_message="Не удалось рассчитать ML-анализ. Попробуйте повторить запрос.",
    )


@router.post("/api/ml-model-jobs")
def start_ml_model_job_endpoint(request: Request, payload: dict = Body(...)):
    session_id = ensure_session_id(request)
    result = start_ml_model_job(
        session_id=session_id,
        table_name=str(payload.get("table_name") or "all"),
        cause=str(payload.get("cause") or "all"),
        object_category=str(payload.get("object_category") or "all"),
        temperature=str(payload.get("temperature") or ""),
        forecast_days=str(payload.get("forecast_days") or "14"),
        history_window=str(payload.get("history_window") or "all"),
    )
    return utf8_json(result, session_id=session_id)


@router.get("/api/ml-model-jobs/{job_id}")
def ml_model_job_status_endpoint(request: Request, job_id: str):
    session_id = ensure_session_id(request)
    result = get_ml_job_status(session_id=session_id, job_id=job_id)
    status_code = 404 if result.get("status") == "missing" else 200
    return utf8_json(result, status_code=status_code, session_id=session_id)
