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


