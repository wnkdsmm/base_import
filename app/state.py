from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any, Dict, Optional
from uuid import uuid4

from config.paths import UPLOADS_DIR


SESSION_COOKIE_NAME = "fire_monitor_session_id"
UPLOAD_FOLDER = UPLOADS_DIR
UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)

FINAL_JOB_STATUSES = {"completed", "failed"}


def _utcnow() -> datetime:
    return datetime.utcnow()


@dataclass
class JobState:
    job_id: str
    kind: str
    current_file_path: Optional[Path] = None
    original_filename: Optional[str] = None
    history: Dict[str, dict] = field(default_factory=dict)
    logs: list[str] = field(default_factory=list)
    result: Any = None
    error_message: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    status: str = "created"
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)

    def touch(self) -> None:
        self.updated_at = _utcnow()


@dataclass
class SessionState:
    session_id: str
    jobs: Dict[str, JobState] = field(default_factory=dict)
    latest_job_ids: Dict[str, str] = field(default_factory=dict)
    last_job_id: Optional[str] = None


class JobStore:
    def __init__(self) -> None:
        self._sessions: Dict[str, SessionState] = {}
        self._lock = RLock()

    def ensure_session(self, session_id: Optional[str] = None) -> str:
        with self._lock:
            normalized_session_id = (session_id or "").strip() or uuid4().hex
            self._sessions.setdefault(normalized_session_id, SessionState(session_id=normalized_session_id))
            return normalized_session_id

    def create_or_reset_job(self, session_id: str, kind: str, job_id: Optional[str] = None) -> JobState:
        with self._lock:
            normalized_session_id = self.ensure_session(session_id)
            normalized_job_id = (job_id or "").strip() or uuid4().hex
            session = self._sessions[normalized_session_id]
            existing_job = session.jobs.get(normalized_job_id)
            if existing_job is not None and existing_job.kind != kind:
                raise ValueError(f"Job {normalized_job_id} belongs to {existing_job.kind}, not {kind}.")

            if existing_job is None:
                job = JobState(job_id=normalized_job_id, kind=kind)
                session.jobs[normalized_job_id] = job
            else:
                job = existing_job

            now = _utcnow()
            job.kind = kind
            job.current_file_path = None
            job.original_filename = None
            job.history = {}
            job.logs = []
            job.result = None
            job.error_message = ""
            job.meta = {}
            job.status = "created"
            job.created_at = now
            job.updated_at = now

            session.latest_job_ids[kind] = normalized_job_id
            session.last_job_id = normalized_job_id
            return job

    def get_job(self, session_id: str, job_id: str) -> Optional[JobState]:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return None
            return session.jobs.get(job_id)

    def get_latest_job(self, session_id: str, kind: Optional[str] = None) -> Optional[JobState]:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return None

            job_id = session.latest_job_ids.get(kind) if kind else session.last_job_id
            if job_id:
                job = session.jobs.get(job_id)
                if job is not None:
                    return job

            candidates = [job for job in session.jobs.values() if kind is None or job.kind == kind]
            if not candidates:
                return None
            job = max(candidates, key=lambda item: item.updated_at)
            if kind:
                session.latest_job_ids[kind] = job.job_id
            session.last_job_id = job.job_id
            return job

    def resolve_job(self, session_id: str, job_id: Optional[str] = None, kind: Optional[str] = None) -> Optional[JobState]:
        if job_id:
            return self.get_job(session_id, job_id)
        return self.get_latest_job(session_id, kind=kind)

    def set_uploaded_file(self, session_id: str, job_id: str, file_path: Path, original_name: str) -> None:
        with self._lock:
            job = self._require_job(session_id, job_id)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            job.current_file_path = file_path
            job.original_filename = original_name
            job.history[str(file_path)] = {
                "original_name": original_name,
                "upload_time": timestamp,
                "path": str(file_path),
            }
            job.status = "uploaded"
            self._touch_job(session_id, job)

    def get_current_file_path(self, session_id: str, job_id: str) -> Optional[Path]:
        with self._lock:
            job = self._require_job(session_id, job_id)
            return job.current_file_path

    def clear_current_file(self, session_id: str, job_id: str) -> None:
        with self._lock:
            job = self._require_job(session_id, job_id)
            job.current_file_path = None
            job.original_filename = None
            self._touch_job(session_id, job)

    def has_uploaded_file(self, session_id: str, job_id: Optional[str] = None) -> bool:
        with self._lock:
            job = self.resolve_job(session_id, job_id=job_id, kind="import")
            return job is not None and job.current_file_path is not None and job.current_file_path.exists()

    def add_log(self, session_id: str, job_id: str, message: str) -> None:
        with self._lock:
            job = self._require_job(session_id, job_id)
            job.logs.append(message)
            self._touch_job(session_id, job)

    def get_logs(self, session_id: str, job_id: Optional[str] = None, kind: Optional[str] = None) -> list[str]:
        with self._lock:
            job = self.resolve_job(session_id, job_id=job_id, kind=kind)
            if job is None:
                return []
            return list(job.logs)

    def clear_logs(self, session_id: str, job_id: str) -> None:
        with self._lock:
            job = self._require_job(session_id, job_id)
            job.logs = []
            self._touch_job(session_id, job)

    def mark_job_status(self, session_id: str, job_id: str, status: str) -> None:
        with self._lock:
            job = self._require_job(session_id, job_id)
            job.status = status
            self._touch_job(session_id, job)

    def set_job_result(self, session_id: str, job_id: str, result: Any) -> None:
        with self._lock:
            job = self._require_job(session_id, job_id)
            job.result = deepcopy(result)
            job.error_message = ""
            self._touch_job(session_id, job)

    def get_job_result(self, session_id: str, job_id: str) -> Any:
        with self._lock:
            job = self._require_job(session_id, job_id)
            return deepcopy(job.result)

    def set_job_error(self, session_id: str, job_id: str, error_message: str) -> None:
        with self._lock:
            job = self._require_job(session_id, job_id)
            job.error_message = error_message
            self._touch_job(session_id, job)

    def update_job_meta(self, session_id: str, job_id: str, **meta: Any) -> None:
        with self._lock:
            job = self._require_job(session_id, job_id)
            job.meta.update(deepcopy(meta))
            self._touch_job(session_id, job)

    def get_job_snapshot(self, session_id: str, job_id: Optional[str] = None, kind: Optional[str] = None) -> Optional[dict]:
        with self._lock:
            job = self.resolve_job(session_id, job_id=job_id, kind=kind)
            if job is None:
                return None
            return {
                "job_id": job.job_id,
                "kind": job.kind,
                "status": job.status,
                "logs": list(job.logs),
                "result": deepcopy(job.result),
                "error_message": job.error_message,
                "meta": deepcopy(job.meta),
                "created_at": job.created_at.isoformat(),
                "updated_at": job.updated_at.isoformat(),
            }

    def get_job_status(self, session_id: str, job_id: Optional[str] = None, kind: Optional[str] = None) -> Optional[str]:
        with self._lock:
            job = self.resolve_job(session_id, job_id=job_id, kind=kind)
            return job.status if job is not None else None

    def prune_job_if_idle(self, session_id: str, job_id: str) -> bool:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return False

            job = session.jobs.get(job_id)
            if job is None:
                return False

            if (
                job.current_file_path is not None
                or job.logs
                or job.result is not None
                or job.error_message
                or job.meta
                or job.status not in FINAL_JOB_STATUSES
            ):
                return False

            del session.jobs[job_id]
            self._refresh_session_indexes(session)
            return True

    def _require_job(self, session_id: str, job_id: str) -> JobState:
        session = self._sessions.get(session_id)
        if session is None or job_id not in session.jobs:
            raise ValueError(f"Job {job_id} was not found for the current session.")
        return session.jobs[job_id]

    def _touch_job(self, session_id: str, job: JobState) -> None:
        session = self._sessions[session_id]
        job.touch()
        session.latest_job_ids[job.kind] = job.job_id
        session.last_job_id = job.job_id

    def _refresh_session_indexes(self, session: SessionState) -> None:
        session.latest_job_ids = {}
        session.last_job_id = None
        jobs = sorted(session.jobs.values(), key=lambda item: item.updated_at)
        for job in jobs:
            session.latest_job_ids[job.kind] = job.job_id
            session.last_job_id = job.job_id


job_store = JobStore()
