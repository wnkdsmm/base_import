from __future__ import annotations

from fastapi import APIRouter, Body, File, Form, Request, UploadFile

from app.log_manager import clear_logs as clear_job_logs
from app.log_manager import get_logs
from app.services.pipeline_service import import_uploaded_data, run_profiling_for_table, save_uploaded_file
from app.state import job_store

from .api_common import ensure_session_id, utf8_json


router = APIRouter()


@router.post("/upload")
async def upload_file(request: Request, file: UploadFile = File(...), job_id: str | None = Form(None)):
    session_id = ensure_session_id(request)
    payload = save_uploaded_file(file=file, session_id=session_id, job_id=job_id)
    return utf8_json(payload, session_id=session_id)


@router.post("/import_data")
def import_data_endpoint(request: Request, output_folder: str | None = Form(None), job_id: str | None = Form(None)):
    session_id = ensure_session_id(request)
    payload = import_uploaded_data(session_id=session_id, output_folder=output_folder, job_id=job_id)
    return utf8_json(payload, session_id=session_id)


@router.get("/logs")
def logs(request: Request, job_id: str | None = None):
    session_id = ensure_session_id(request)
    resolved_job = job_store.resolve_job(session_id=session_id, job_id=job_id)
    resolved_job_id = resolved_job.job_id if resolved_job is not None else (job_id or "")
    status = resolved_job.status if resolved_job is not None else "missing"
    payload = {
        "job_id": resolved_job_id,
        "status": status,
        "logs": get_logs(session_id=session_id, job_id=resolved_job_id) if resolved_job_id else [],
    }
    return utf8_json(payload, session_id=session_id)


@router.post("/clear_logs")
def clear_logs_endpoint(request: Request, job_id: str | None = None):
    session_id = ensure_session_id(request)
    resolved_job = job_store.resolve_job(session_id=session_id, job_id=job_id)
    if resolved_job is None:
        return utf8_json({"status": "missing", "job_id": job_id or ""}, session_id=session_id)

    clear_job_logs(session_id=session_id, job_id=resolved_job.job_id)
    pruned = job_store.prune_job_if_idle(session_id=session_id, job_id=resolved_job.job_id)
    return utf8_json({"status": "cleared", "job_id": resolved_job.job_id, "pruned": pruned}, session_id=session_id)


@router.get("/health")
def health_check(request: Request):
    session_id = ensure_session_id(request)
    latest_import_job = job_store.resolve_job(session_id=session_id, kind="import")
    return utf8_json(
        {
            "status": "healthy",
            "uploaded_file": job_store.has_uploaded_file(session_id=session_id),
            "job_id": latest_import_job.job_id if latest_import_job is not None else "",
        },
        session_id=session_id,
    )


@router.post("/run_profiling")
def run_profiling_endpoint(request: Request, payload: dict = Body(...)):
    session_id = ensure_session_id(request)
    raw_job_id = str(payload.get("job_id") or "").strip()
    result = run_profiling_for_table(
        session_id=session_id,
        table_name=str(payload.get("table") or ""),
        thresholds=payload.get("thresholds"),
        job_id=raw_job_id or None,
    )
    return utf8_json(result, session_id=session_id)
