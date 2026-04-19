from __future__ import annotations

from fastapi import APIRouter, Body, Request

from app.services.forecasting.forecasting_pipeline import (
    get_forecasting_data,
    get_forecasting_decision_support_data,
    get_forecasting_metadata,
)

from .api_common import (
    job_status_response,
    run_analytics_request,
    run_session_analytics_request,
)


router = APIRouter()

_INVALID_FORECASTING_MESSAGE = "\u041d\u0435\u043a\u043e\u0440\u0440\u0435\u043a\u0442\u043d\u044b\u0435 \u043f\u0430\u0440\u0430\u043c\u0435\u0442\u0440\u044b \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430."
_FAILED_FORECASTING_MESSAGE = (
    "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0441\u043e\u0431\u0440\u0430\u0442\u044c "
    "\u0434\u0430\u043d\u043d\u044b\u0435 \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430. "
    "\u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u043f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u044c \u0437\u0430\u043f\u0440\u043e\u0441."
)
_INVALID_FORECASTING_METADATA_MESSAGE = (
    "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0430\u0442\u044c "
    "\u043f\u0430\u0440\u0430\u043c\u0435\u0442\u0440\u044b \u0437\u0430\u0433\u0440\u0443\u0437\u043a\u0438 "
    "\u0444\u0438\u043b\u044c\u0442\u0440\u043e\u0432 \u0438 \u043f\u0440\u0438\u0437\u043d\u0430\u043a\u043e\u0432."
)
_FAILED_FORECASTING_METADATA_MESSAGE = (
    "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0437\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u044c "
    "\u0444\u0438\u043b\u044c\u0442\u0440\u044b \u0438 \u043f\u0440\u0438\u0437\u043d\u0430\u043a\u0438 \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u0430. "
    "\u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u043f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u044c \u0437\u0430\u043f\u0440\u043e\u0441."
)
_INVALID_DECISION_SUPPORT_MESSAGE = (
    "\u041d\u0435\u043a\u043e\u0440\u0440\u0435\u043a\u0442\u043d\u044b\u0435 \u043f\u0430\u0440\u0430\u043c\u0435\u0442\u0440\u044b "
    "\u0434\u043b\u044f \u0444\u043e\u043d\u043e\u0432\u043e\u0433\u043e \u0431\u043b\u043e\u043a\u0430 "
    "\u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 \u0440\u0435\u0448\u0435\u043d\u0438\u0439."
)
_FAILED_DECISION_SUPPORT_MESSAGE = (
    "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0437\u0430\u043f\u0443\u0441\u0442\u0438\u0442\u044c "
    "\u0444\u043e\u043d\u043e\u0432\u044b\u0439 \u0440\u0430\u0441\u0447\u0435\u0442 \u0431\u043b\u043e\u043a\u0430 "
    "\u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 \u0440\u0435\u0448\u0435\u043d\u0438\u0439. "
    "\u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u043f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u044c \u0437\u0430\u043f\u0440\u043e\u0441."
)

def start_forecasting_decision_support_job(**kwargs):
    from app.services.forecasting.jobs import (
        start_forecasting_decision_support_job as _start_forecasting_decision_support_job,
    )

    return _start_forecasting_decision_support_job(**kwargs)


def get_forecasting_decision_support_job_status(**kwargs):
    from app.services.forecasting.jobs import (
        get_forecasting_decision_support_job_status as _get_forecasting_decision_support_job_status,
    )

    return _get_forecasting_decision_support_job_status(**kwargs)


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
        invalid_message=_INVALID_FORECASTING_MESSAGE,
        failed_code="forecasting_failed",
        failed_message=_FAILED_FORECASTING_MESSAGE,
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
        invalid_message=_INVALID_FORECASTING_METADATA_MESSAGE,
        failed_code="forecasting_metadata_failed",
        failed_message=_FAILED_FORECASTING_METADATA_MESSAGE,
    )


@router.post("/api/forecasting-decision-support-jobs")
def start_forecasting_decision_support_job_endpoint(request: Request, payload: dict = Body(...)):
    return run_session_analytics_request(
        request,
        lambda session_id: start_forecasting_decision_support_job(
            session_id=session_id,
            table_name=str(payload.get("table_name") or "all"),
            district=str(payload.get("district") or "all"),
            cause=str(payload.get("cause") or "all"),
            object_category=str(payload.get("object_category") or "all"),
            temperature=str(payload.get("temperature") or ""),
            forecast_days=str(payload.get("forecast_days") or "14"),
            history_window=str(payload.get("history_window") or "all"),
        ),
        invalid_code="forecasting_decision_support_invalid_request",
        invalid_message=_INVALID_DECISION_SUPPORT_MESSAGE,
        failed_code="forecasting_decision_support_failed",
        failed_message=_FAILED_DECISION_SUPPORT_MESSAGE,
    )


@router.get("/api/forecasting-decision-support-jobs/{job_id}")
def forecasting_decision_support_job_status_endpoint(request: Request, job_id: str):
    return job_status_response(request, job_id, get_forecasting_decision_support_job_status)
