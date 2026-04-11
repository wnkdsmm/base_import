from __future__ import annotations

from fastapi import APIRouter, Body, Request

from .api_common import job_status_response, run_analytics_request, run_session_json_action


router = APIRouter()

_INVALID_ML_MODEL_MESSAGE = "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0430\u0442\u044c \u043f\u0430\u0440\u0430\u043c\u0435\u0442\u0440\u044b ML-\u0430\u043d\u0430\u043b\u0438\u0437\u0430."
_FAILED_ML_MODEL_MESSAGE = (
    "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0440\u0430\u0441\u0441\u0447\u0438\u0442\u0430\u0442\u044c "
    "ML-\u0430\u043d\u0430\u043b\u0438\u0437. \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 "
    "\u043f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u044c \u0437\u0430\u043f\u0440\u043e\u0441."
)


def get_ml_model_data(**kwargs):
    from app.services.ml_model.core import get_ml_model_data as _get_ml_model_data

    return _get_ml_model_data(**kwargs)


def start_ml_model_job(**kwargs):
    from app.services.ml_model.jobs import start_ml_model_job as _start_ml_model_job

    return _start_ml_model_job(**kwargs)


def get_ml_job_status(**kwargs):
    from app.services.ml_model.jobs import get_ml_job_status as _get_ml_job_status

    return _get_ml_job_status(**kwargs)


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
        invalid_message=_INVALID_ML_MODEL_MESSAGE,
        failed_code="ml_model_failed",
        failed_message=_FAILED_ML_MODEL_MESSAGE,
    )


@router.post("/api/ml-model-jobs")
def start_ml_model_job_endpoint(request: Request, payload: dict = Body(...)):
    return run_session_json_action(
        request,
        lambda session_id: start_ml_model_job(
            session_id=session_id,
            table_name=str(payload.get("table_name") or "all"),
            cause=str(payload.get("cause") or "all"),
            object_category=str(payload.get("object_category") or "all"),
            temperature=str(payload.get("temperature") or ""),
            forecast_days=str(payload.get("forecast_days") or "14"),
            history_window=str(payload.get("history_window") or "all"),
        ),
    )


@router.get("/api/ml-model-jobs/{job_id}")
def ml_model_job_status_endpoint(request: Request, job_id: str):
    return job_status_response(request, job_id, get_ml_job_status)
