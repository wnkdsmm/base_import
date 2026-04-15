from __future__ import annotations

from fastapi import APIRouter, Body, Request

from app.services.clustering.core import get_clustering_data

from .api_common import (
    coerce_string_list,
    job_status_response,
    run_analytics_request,
    run_session_analytics_request,
)


router = APIRouter()

_INVALID_CLUSTERING_MESSAGE = "\u041d\u0435\u043a\u043e\u0440\u0440\u0435\u043a\u0442\u043d\u044b\u0435 \u043f\u0430\u0440\u0430\u043c\u0435\u0442\u0440\u044b \u043a\u043b\u0430\u0441\u0442\u0435\u0440\u0438\u0437\u0430\u0446\u0438\u0438."
_FAILED_CLUSTERING_MESSAGE = (
    "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0441\u043e\u0431\u0440\u0430\u0442\u044c "
    "clustering-\u0434\u0430\u043d\u043d\u044b\u0435. \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 "
    "\u043f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u044c \u0437\u0430\u043f\u0440\u043e\u0441."
)
_INVALID_CLUSTERING_JOB_MESSAGE = (
    "\u041d\u0435\u043a\u043e\u0440\u0440\u0435\u043a\u0442\u043d\u044b\u0435 \u043f\u0430\u0440\u0430\u043c\u0435\u0442\u0440\u044b "
    "\u0434\u043b\u044f \u0444\u043e\u043d\u043e\u0432\u043e\u0439 clustering-\u0437\u0430\u0434\u0430\u0447\u0438."
)
_FAILED_CLUSTERING_JOB_MESSAGE = (
    "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0437\u0430\u043f\u0443\u0441\u0442\u0438\u0442\u044c "
    "\u0444\u043e\u043d\u043e\u0432\u0443\u044e clustering-\u0437\u0430\u0434\u0430\u0447\u0443. "
    "\u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u043f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u044c \u0437\u0430\u043f\u0440\u043e\u0441."
)

def start_clustering_job(**kwargs):
    from app.services.clustering.jobs import start_clustering_job as _start_clustering_job

    return _start_clustering_job(**kwargs)


def get_clustering_job_status(**kwargs):
    from app.services.clustering.jobs import get_clustering_job_status as _get_clustering_job_status

    return _get_clustering_job_status(**kwargs)


@router.get("/api/clustering-data")
def clustering_data_endpoint(
    request: Request,
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: list[str] | None = None,
):
    return run_analytics_request(
        lambda: get_clustering_data(
            table_name=table_name,
            cluster_count=cluster_count,
            sample_limit=sample_limit,
            sampling_strategy=sampling_strategy,
            feature_columns=feature_columns or [],
            cluster_count_is_explicit="cluster_count" in request.query_params,
        ),
        invalid_code="clustering_invalid_request",
        invalid_message=_INVALID_CLUSTERING_MESSAGE,
        failed_code="clustering_failed",
        failed_message=_FAILED_CLUSTERING_MESSAGE,
    )


@router.post("/api/clustering-jobs")
def start_clustering_job_endpoint(request: Request, payload: dict = Body(...)):
    return run_session_analytics_request(
        request,
        lambda session_id: start_clustering_job(
            session_id=session_id,
            table_name=str(payload.get("table_name") or ""),
            cluster_count=str(payload.get("cluster_count") or "4"),
            sample_limit=str(payload.get("sample_limit") or "1000"),
            sampling_strategy=str(payload.get("sampling_strategy") or "stratified"),
            feature_columns=coerce_string_list(payload.get("feature_columns")),
            cluster_count_is_explicit="cluster_count" in payload,
        ),
        invalid_code="clustering_job_invalid_request",
        invalid_message=_INVALID_CLUSTERING_JOB_MESSAGE,
        failed_code="clustering_job_failed",
        failed_message=_FAILED_CLUSTERING_JOB_MESSAGE,
    )


@router.get("/api/clustering-jobs/{job_id}")
def clustering_job_status_endpoint(request: Request, job_id: str):
    return job_status_response(request, job_id, get_clustering_job_status)
