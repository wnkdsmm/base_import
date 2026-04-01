from __future__ import annotations

from fastapi import APIRouter, Body, Request

from app.services.clustering.core import get_clustering_data
from app.services.clustering.jobs import get_clustering_job_status, start_clustering_job

from .api_common import coerce_string_list, ensure_session_id, run_analytics_request, utf8_json


router = APIRouter()


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
        invalid_message="Некорректные параметры кластеризации.",
        failed_code="clustering_failed",
        failed_message="Не удалось собрать clustering-данные. Попробуйте повторить запрос.",
    )


@router.post("/api/clustering-jobs")
def start_clustering_job_endpoint(request: Request, payload: dict = Body(...)):
    session_id = ensure_session_id(request)

    def action():
        return start_clustering_job(
            session_id=session_id,
            table_name=str(payload.get("table_name") or ""),
            cluster_count=str(payload.get("cluster_count") or "4"),
            sample_limit=str(payload.get("sample_limit") or "1000"),
            sampling_strategy=str(payload.get("sampling_strategy") or "stratified"),
            feature_columns=coerce_string_list(payload.get("feature_columns")),
            cluster_count_is_explicit="cluster_count" in payload,
        )

    result = run_analytics_request(
        action,
        invalid_code="clustering_job_invalid_request",
        invalid_message="Некорректные параметры для фоновой clustering-задачи.",
        failed_code="clustering_job_failed",
        failed_message="Не удалось запустить фоновую clustering-задачу. Попробуйте повторить запрос.",
    )
    if isinstance(result, dict):
        return utf8_json(result, session_id=session_id)
    return result


@router.get("/api/clustering-jobs/{job_id}")
def clustering_job_status_endpoint(request: Request, job_id: str):
    session_id = ensure_session_id(request)
    result = get_clustering_job_status(session_id=session_id, job_id=job_id)
    status_code = 404 if result.get("status") == "missing" else 200
    return utf8_json(result, status_code=status_code, session_id=session_id)
