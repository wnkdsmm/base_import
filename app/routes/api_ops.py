from __future__ import annotations

from fastapi import APIRouter, Body, File, Form, Request, UploadFile

from app.state import job_store

from .api_common import run_session_json_action


router = APIRouter()


def save_uploaded_file(*, file: UploadFile, session_id: str, job_id: str | None):
    from app.services.pipeline_service import save_uploaded_file as _save_uploaded_file

    return _save_uploaded_file(file=file, session_id=session_id, job_id=job_id)


def import_uploaded_data(*, session_id: str, output_folder: str | None, job_id: str | None):
    from app.services.pipeline_service import import_uploaded_data as _import_uploaded_data

    return _import_uploaded_data(session_id=session_id, output_folder=output_folder, job_id=job_id)


def run_profiling_for_table(*, session_id: str, table_name: str, thresholds, job_id: str | None):
    from app.services.pipeline_service import run_profiling_for_table as _run_profiling_for_table

    return _run_profiling_for_table(
        session_id=session_id,
        table_name=table_name,
        thresholds=thresholds,
        job_id=job_id,
    )


@router.post("/upload")
async def upload_file(request: Request, file: UploadFile = File(...), job_id: str | None = Form(None)):
    return run_session_json_action(
        request,
        lambda session_id: save_uploaded_file(file=file, session_id=session_id, job_id=job_id),
    )


@router.post("/import_data")
def import_data_endpoint(request: Request, output_folder: str | None = Form(None), job_id: str | None = Form(None)):
    return run_session_json_action(
        request,
        lambda session_id: import_uploaded_data(session_id=session_id, output_folder=output_folder, job_id=job_id),
    )


@router.get("/logs")
def logs(request: Request, job_id: str | None = None):
    def build_payload(session_id: str):
        resolved_job = job_store.resolve_job(session_id=session_id, job_id=job_id)
        resolved_job_id = resolved_job.job_id if resolved_job is not None else (job_id or "")
        status = resolved_job.status if resolved_job is not None else "missing"
        return {
            "job_id": resolved_job_id,
            "status": status,
            "logs": job_store.get_logs(session_id=session_id, job_id=resolved_job_id) if resolved_job_id else [],
        }

    return run_session_json_action(request, build_payload)


@router.post("/clear_logs")
def clear_logs_endpoint(request: Request, job_id: str | None = None):
    def clear_payload(session_id: str):
        resolved_job = job_store.resolve_job(session_id=session_id, job_id=job_id)
        if resolved_job is None:
            return {"status": "missing", "job_id": job_id or ""}

        job_store.clear_logs(session_id=session_id, job_id=resolved_job.job_id)
        pruned = job_store.prune_job_if_idle(session_id=session_id, job_id=resolved_job.job_id)
        return {"status": "cleared", "job_id": resolved_job.job_id, "pruned": pruned}

    return run_session_json_action(request, clear_payload)


@router.get("/health")
def health_check(request: Request):
    def health_payload(session_id: str):
        latest_import_job = job_store.resolve_job(session_id=session_id, kind="import")
        return {
            "status": "healthy",
            "uploaded_file": job_store.has_uploaded_file(session_id=session_id),
            "job_id": latest_import_job.job_id if latest_import_job is not None else "",
        }

    return run_session_json_action(request, health_payload)


@router.post("/run_profiling")
def run_profiling_endpoint(request: Request, payload: dict = Body(...)):
    raw_job_id = str(payload.get("job_id") or "").strip()
    return run_session_json_action(
        request,
        lambda session_id: run_profiling_for_table(
            session_id=session_id,
            table_name=str(payload.get("table") or ""),
            thresholds=payload.get("thresholds"),
            job_id=raw_job_id or None,
        ),
    )
