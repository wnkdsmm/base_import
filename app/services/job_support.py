from __future__ import annotations

import json
from dataclasses import dataclass, field
from threading import RLock
from typing import Any, Callable, MutableMapping, Sequence

from app.state import FINAL_JOB_STATUSES, job_store
from app.services.forecasting.types import JobMetaPayload, JobSnapshot, JobStageMeta, JobStatusPayload

JobCacheMapping = MutableMapping[tuple[str, str], str]
StageMetaResolver = Callable[[str], JobStageMeta | None]
StatusResolver = Callable[[str, str], str | None]
StatusPayloadBuilder = Callable[[str, bool], JobStatusPayload]
CachedPayloadLoader = Callable[[], Any | None]
JobBundleFactory = Callable[[bool], "JobLaunchBundle"]
CachedPayloadHandler = Callable[["JobLaunchBundle", Any], None]
QueuedJobSubmitter = Callable[["JobLaunchBundle"], None]
ErrorMessageBuilder = Callable[[Exception], str]
JobStartHook = Callable[[], None]
JobExecuteHook = Callable[[], Any]
JobSuccessHook = Callable[[Any], None]
JobFailureHook = Callable[[str], None]


@dataclass(frozen=True)


class JobLaunchBundle:
    primary_job_id: str
    related_job_ids: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)


class LinkedJobStatusSpec:
    payload_key: str
    meta_key: str
    include_result: bool = False
    include_meta: bool = False


@dataclass(frozen=True)


class JobReuseCoordinator:
    session_id: str
    cache_key_token: str
    job_ids_by_cache_key: JobCacheMapping
    job_lock: RLock

    def find_reusable_job_id(self) -> str | None:
        return find_reusable_job_id(
            self.session_id,
            self.cache_key_token,
            job_ids_by_cache_key=self.job_ids_by_cache_key,
        )

    def register(self, job_id: str) -> None:
        self.job_ids_by_cache_key[(self.session_id, self.cache_key_token)] = job_id

    def discard(self, job_id: str) -> None:
        discard_reusable_job_id(
            self.session_id,
            self.cache_key_token,
            job_id,
            job_ids_by_cache_key=self.job_ids_by_cache_key,
        )


def serialize_job_cache_key(cache_key: tuple[Any, ...]) -> str:
    return json.dumps(list(cache_key), ensure_ascii=False, default=str)


def find_reusable_job_id(
    session_id: str,
    cache_key_token: str,
    *,
    job_ids_by_cache_key: JobCacheMapping,
) -> str | None:
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
    params_payload: dict[str, object],
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


def attach_linked_job_metadata(
    *,
    session_id: str,
    primary_job_id: str,
    cache_key_token: str,
    params_payload: dict[str, object],
    cache_hit: bool,
    primary_meta: JobMetaPayload | None = None,
    linked_meta_by_job_id: dict[str, JobMetaPayload | None] = None,
) -> None:
    attach_standard_job_metadata(
        session_id=session_id,
        job_id=primary_job_id,
        cache_key_token=cache_key_token,
        params_payload=params_payload,
        cache_hit=cache_hit,
        **(primary_meta or {}),
    )
    for linked_job_id, linked_meta in (linked_meta_by_job_id or {}).items():
        attach_standard_job_metadata(
            session_id=session_id,
            job_id=linked_job_id,
            cache_key_token=cache_key_token,
            params_payload=params_payload,
            cache_hit=cache_hit,
            **linked_meta,
        )


def build_missing_job_payload(
    job_id: str,
    *,
    reused: bool | None,
    include_meta: bool,
) -> JobStatusPayload:
    payload = {
        "job_id": job_id,
        "status": "missing",
        "kind": "",
        "logs": [],
        "result": None,
        "error_message": "Job \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d \u0434\u043b\u044f \u0442\u0435\u043a\u0443\u0449\u0435\u0439 \u0441\u0435\u0441\u0441\u0438\u0438.",
        "is_final": True,
    }
    if include_meta:
        payload["meta"] = {}
    if reused is not None:
        payload["reused"] = reused
    return payload


def build_job_payload_from_snapshot(
    snapshot: JobSnapshot,
    *,
    reused: bool | None,
    include_result: bool = True,
    include_meta: bool = True,
) -> JobStatusPayload:
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


def build_standard_job_status_payload(session_id: str, job_id: str, *, reused: bool) -> JobStatusPayload:
    snapshot = job_store.get_job_snapshot(session_id, job_id=job_id)
    if snapshot is None:
        return build_missing_job_payload(job_id, reused=reused, include_meta=True)
    return build_job_payload_from_snapshot(snapshot, reused=reused)


def build_linked_job_status_payload(
    session_id: str,
    job_id: str,
    *,
    reused: bool,
    linked_specs: Sequence[LinkedJobStatusSpec],
    include_meta: bool = True,
    missing_include_meta: bool | None = None,
) -> JobStatusPayload:
    snapshot = job_store.get_job_snapshot(session_id, job_id=job_id)
    if snapshot is None:
        return build_missing_job_payload(
            job_id,
            reused=reused,
            include_meta=include_meta if missing_include_meta is None else missing_include_meta,
        )

    payload = build_job_payload_from_snapshot(snapshot, reused=reused, include_meta=include_meta)
    meta = snapshot.get("meta") or {}
    for spec in linked_specs:
        linked_job_id = str(meta.get(spec.meta_key) or "")
        linked_snapshot = job_store.get_job_snapshot(session_id, job_id=linked_job_id) if linked_job_id else None
        if linked_snapshot is None:
            payload[spec.payload_key] = None
            continue
        payload[spec.payload_key] = build_job_payload_from_snapshot(
            linked_snapshot,
            reused=None,
            include_result=spec.include_result,
            include_meta=spec.include_meta,
        )
    return payload


def start_cache_aware_job(
    *,
    reuse_coordinator: JobReuseCoordinator,
    build_status_payload: StatusPayloadBuilder,
    load_cached_payload: CachedPayloadLoader,
    create_jobs: JobBundleFactory,
    handle_cached_payload: CachedPayloadHandler,
    submit_background_job: QueuedJobSubmitter,
) -> JobStatusPayload:
    with reuse_coordinator.job_lock:
        reusable_job_id = reuse_coordinator.find_reusable_job_id()
        if reusable_job_id:
            return build_status_payload(reusable_job_id, True)

        cached_payload = load_cached_payload()
        bundle = create_jobs(cached_payload is not None)
        if cached_payload is not None:
            handle_cached_payload(bundle, cached_payload)
        else:
            submit_background_job(bundle)

        reuse_coordinator.register(bundle.primary_job_id)
        return build_status_payload(bundle.primary_job_id, False)


def run_background_job(
    *,
    reuse_coordinator: JobReuseCoordinator,
    primary_job_id: str,
    on_start: JobStartHook,
    execute: JobExecuteHook,
    on_success: JobSuccessHook,
    on_failure: JobFailureHook,
    build_error_message: ErrorMessageBuilder,
) -> None:
    completed = False
    try:
        on_start()
        payload = execute()
        on_success(payload)
        completed = True
    except Exception as exc:
        on_failure(build_error_message(exc))
    finally:
        if not completed:
            with reuse_coordinator.job_lock:
                reuse_coordinator.discard(primary_job_id)


def default_stage_meta_for_phase(phase: str) -> JobStageMeta | None:
    normalized_phase = str(phase or "").strip().lower()
    if "loading" in normalized_phase:
        return {
            "stage_index": 0,
            "stage_label": "\u0417\u0430\u0433\u0440\u0443\u0437\u043a\u0430 \u0434\u0430\u043d\u043d\u044b\u0445",
        }
    if "aggregation" in normalized_phase:
        return {
            "stage_index": 1,
            "stage_label": "\u0410\u0433\u0440\u0435\u0433\u0430\u0446\u0438\u044f",
        }
    if "training" in normalized_phase:
        return {
            "stage_index": 2,
            "stage_label": "\u041e\u0431\u0443\u0447\u0435\u043d\u0438\u0435 / \u0432\u0430\u043b\u0438\u0434\u0430\u0446\u0438\u044f",
        }
    if "render" in normalized_phase or "completed" in normalized_phase:
        return {
            "stage_index": 3,
            "stage_label": "\u041f\u043e\u0441\u0442\u0440\u043e\u0435\u043d\u0438\u0435 \u0432\u0438\u0437\u0443\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u0439",
        }
    return None


class StageTrackingJobProgressReporter:
    def __init__(
        self,
        *,
        session_id: str,
        job_id: str,
        stage_meta_resolver: StageMetaResolver = default_stage_meta_for_phase,
        status_resolver: StatusResolver | None = None,
    ) -> None:
        self._session_id = session_id
        self._job_id = job_id
        self._stage_meta_resolver = stage_meta_resolver
        self._status_resolver = status_resolver
        self._last_message = ""

    def handle_progress(self, phase: str, message: str) -> None:
        normalized_phase = str(phase or "").strip().lower()
        normalized_message = str(message or "").strip()
        if not normalized_message:
            return
        if normalized_message != self._last_message:
            job_store.add_log(self._session_id, self._job_id, normalized_message)
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
        if self._status_resolver is None:
            return
        status = self._status_resolver(normalized_phase, normalized_message)
        if status:
            job_store.mark_job_status(self._session_id, self._job_id, status)


class LinkedJobProgressReporter:
    def __init__(
        self,
        *,
        primary_reporter: StageTrackingJobProgressReporter,
        session_id: str,
        primary_job_id: str,
        secondary_job_id: str,
        secondary_phase_prefix: str,
        secondary_status_resolver: StatusResolver | None = None,
        mirror_to_primary_prefix: str = "",
        secondary_error_on_failed: bool = True,
    ) -> None:
        self._primary_reporter = primary_reporter
        self._session_id = session_id
        self._primary_job_id = primary_job_id
        self._secondary_job_id = secondary_job_id
        self._secondary_phase_prefix = str(secondary_phase_prefix or "").strip().lower()
        self._secondary_status_resolver = secondary_status_resolver
        self._mirror_to_primary_prefix = mirror_to_primary_prefix
        self._secondary_error_on_failed = secondary_error_on_failed
        self._last_secondary_message = ""

    def handle_progress(self, phase: str, message: str) -> None:
        normalized_phase = str(phase or "").strip().lower()
        normalized_message = str(message or "").strip()
        if not normalized_message:
            return
        if self._secondary_phase_prefix and normalized_phase.startswith(self._secondary_phase_prefix):
            self._handle_secondary_progress(normalized_phase, normalized_message)
            return
        self._primary_reporter.handle_progress(phase, message)

    def _handle_secondary_progress(self, normalized_phase: str, normalized_message: str) -> None:
        if normalized_message != self._last_secondary_message:
            job_store.add_log(self._session_id, self._secondary_job_id, normalized_message)
            if self._mirror_to_primary_prefix:
                job_store.add_log(
                    self._session_id,
                    self._primary_job_id,
                    f"{self._mirror_to_primary_prefix}{normalized_message}",
                )
            self._last_secondary_message = normalized_message
        if self._secondary_status_resolver is None:
            return
        status = self._secondary_status_resolver(normalized_phase, normalized_message)
        if not status:
            return
        if status == "failed" and self._secondary_error_on_failed:
            job_store.set_job_error(self._session_id, self._secondary_job_id, normalized_message)
        job_store.mark_job_status(self._session_id, self._secondary_job_id, status)
