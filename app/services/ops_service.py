from __future__ import annotations

from app.state import job_store


def build_logs_payload(*, session_id: str, job_id: str | None = None) -> dict[str, object]:
    resolved_job = job_store.resolve_job(session_id=session_id, job_id=job_id)
    resolved_job_id = resolved_job.job_id if resolved_job is not None else (job_id or "")
    status = resolved_job.status if resolved_job is not None else "missing"
    return {
        "job_id": resolved_job_id,
        "status": status,
        "logs": job_store.get_logs(session_id=session_id, job_id=resolved_job_id) if resolved_job_id else [],
    }


def clear_logs_payload(*, session_id: str, job_id: str | None = None) -> dict[str, object]:
    resolved_job = job_store.resolve_job(session_id=session_id, job_id=job_id)
    if resolved_job is None:
        return {"status": "missing", "job_id": job_id or ""}

    job_store.clear_logs(session_id=session_id, job_id=resolved_job.job_id)
    pruned = job_store.prune_job_if_idle(session_id=session_id, job_id=resolved_job.job_id)
    return {"status": "cleared", "job_id": resolved_job.job_id, "pruned": pruned}


def build_health_payload(*, session_id: str) -> dict[str, object]:
    latest_import_job = job_store.resolve_job(session_id=session_id, kind="import")
    return {
        "status": "healthy",
        "uploaded_file": job_store.has_uploaded_file(session_id=session_id),
        "job_id": latest_import_job.job_id if latest_import_job is not None else "",
    }

