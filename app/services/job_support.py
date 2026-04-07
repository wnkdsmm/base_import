from __future__ import annotations

import json
from typing import Any, Callable, Dict, MutableMapping, Optional, Tuple

from app.log_manager import add_log
from app.state import FINAL_JOB_STATUSES, job_store

JobCacheMapping = MutableMapping[Tuple[str, str], str]
StageMetaResolver = Callable[[str], Optional[Dict[str, Any]]]


def serialize_job_cache_key(cache_key: Tuple[Any, ...]) -> str:
    return json.dumps(list(cache_key), ensure_ascii=False, default=str)


def find_reusable_job_id(
    session_id: str,
    cache_key_token: str,
    *,
    job_ids_by_cache_key: JobCacheMapping,
) -> Optional[str]:
    job_id = job_ids_by_cache_key.get((session_id, cache_key_token))
    if not job_id:
        return None
    snapshot = job_store.get_job_snapshot(session_id, job_id=job_id)
    if snapshot is None or snapshot["status"] == "failed":
        job_ids_by_cache_key.pop((session_id, cache_key_token), None)
        return None
    if snapshot["status"] not in {"pending", "running"}:
        return None
    return job_id


def discard_reusable_job_id(
    session_id: str,
    cache_key_token: str,
    job_id: str,
    *,
    job_ids_by_cache_key: JobCacheMapping,
) -> None:
    current_job_id = job_ids_by_cache_key.get((session_id, cache_key_token))
    if current_job_id == job_id:
        job_ids_by_cache_key.pop((session_id, cache_key_token), None)


def attach_standard_job_metadata(
    *,
    session_id: str,
    job_id: str,
    cache_key_token: str,
    params_payload: Dict[str, Any],
    cache_hit: bool,
    **meta: Any,
) -> None:
    job_store.update_job_meta(
        session_id,
        job_id,
        cache_key=cache_key_token,
        cache_hit=cache_hit,
        params=params_payload,
        **meta,
    )


def build_missing_job_payload(
    job_id: str,
    *,
    reused: bool | None,
    include_meta: bool,
) -> Dict[str, Any]:
    payload = {
        "job_id": job_id,
        "status": "missing",
        "kind": "",
        "logs": [],
        "result": None,
        "error_message": "Job не найден для текущей сессии.",
        "is_final": True,
    }
    if include_meta:
        payload["meta"] = {}
    if reused is not None:
        payload["reused"] = reused
    return payload


def build_job_payload_from_snapshot(
    snapshot: Dict[str, Any],
    *,
    reused: bool | None,
    include_result: bool = True,
    include_meta: bool = True,
) -> Dict[str, Any]:
    payload = {
        "job_id": snapshot["job_id"],
        "kind": snapshot["kind"],
        "status": snapshot["status"],
        "logs": snapshot.get("logs") or [],
    }
    if include_result:
        payload["result"] = snapshot.get("result")
    payload["error_message"] = snapshot.get("error_message") or ""
    if include_meta:
        payload["meta"] = snapshot.get("meta") or {}
    if reused is not None:
        payload["reused"] = reused
    payload["is_final"] = snapshot["status"] in FINAL_JOB_STATUSES
    return payload


def build_standard_job_status_payload(session_id: str, job_id: str, *, reused: bool) -> Dict[str, Any]:
    snapshot = job_store.get_job_snapshot(session_id, job_id=job_id)
    if snapshot is None:
        return build_missing_job_payload(job_id, reused=reused, include_meta=True)
    return build_job_payload_from_snapshot(snapshot, reused=reused)


def default_stage_meta_for_phase(phase: str) -> Optional[Dict[str, Any]]:
    normalized_phase = str(phase or "").strip().lower()
    if "loading" in normalized_phase:
        return {"stage_index": 0, "stage_label": "Загрузка данных"}
    if "aggregation" in normalized_phase:
        return {"stage_index": 1, "stage_label": "Агрегация"}
    if "training" in normalized_phase:
        return {"stage_index": 2, "stage_label": "Обучение / валидация"}
    if "render" in normalized_phase or "completed" in normalized_phase:
        return {"stage_index": 3, "stage_label": "Построение визуализаций"}
    return None


class StageTrackingJobProgressReporter:
    def __init__(
        self,
        *,
        session_id: str,
        job_id: str,
        stage_meta_resolver: StageMetaResolver = default_stage_meta_for_phase,
    ) -> None:
        self._session_id = session_id
        self._job_id = job_id
        self._stage_meta_resolver = stage_meta_resolver
        self._last_message = ""

    def handle_progress(self, phase: str, message: str) -> None:
        normalized_phase = str(phase or "").strip().lower()
        normalized_message = str(message or "").strip()
        if not normalized_message:
            return
        if normalized_message != self._last_message:
            add_log(self._session_id, self._job_id, normalized_message)
            self._last_message = normalized_message
        stage_meta = self._stage_meta_resolver(normalized_phase)
        if stage_meta is not None:
            job_store.update_job_meta(
                self._session_id,
                self._job_id,
                stage_index=stage_meta["stage_index"],
                stage_label=stage_meta["stage_label"],
                stage_message=normalized_message,
            )
        self._update_status(normalized_phase, normalized_message)

    def _update_status(self, normalized_phase: str, normalized_message: str) -> None:
        return
