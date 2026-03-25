from __future__ import annotations

from typing import Optional

from app.state import job_store


def add_log(session_id: str, job_id: str, message: str) -> None:
    job_store.add_log(session_id, job_id, message)


def get_logs(session_id: str, job_id: Optional[str] = None, kind: Optional[str] = None) -> list[str]:
    return job_store.get_logs(session_id=session_id, job_id=job_id, kind=kind)


def clear_logs(session_id: str, job_id: str) -> None:
    job_store.clear_logs(session_id=session_id, job_id=job_id)
