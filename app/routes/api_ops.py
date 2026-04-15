from __future__ import annotations

from fastapi import APIRouter, Body, File, Form, Request, UploadFile

from app.services.ops_service import build_health_payload, build_logs_payload, clear_logs_payload

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
    return run_session_json_action(
        request,
        lambda session_id: build_logs_payload(session_id=session_id, job_id=job_id),
    )


@router.post("/clear_logs")
def clear_logs_endpoint(request: Request, job_id: str | None = None):
    return run_session_json_action(
        request,
        lambda session_id: clear_logs_payload(session_id=session_id, job_id=job_id),
    )


@router.get("/health")
def health_check(request: Request):
    return run_session_json_action(request, lambda session_id: build_health_payload(session_id=session_id))


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
