from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from typing import Any, Tuple

from app.services.job_support import (
    JobLaunchBundle,
    JobReuseCoordinator,
    StageTrackingJobProgressReporter,
    attach_standard_job_metadata,
    build_standard_job_status_payload,
    run_background_job,
    serialize_job_cache_key,
    start_cache_aware_job,
)
from app.state import job_store

from .core_runner import _CLUSTERING_CACHE, _build_clustering_request_state, get_clustering_data

_CLUSTERING_JOB_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="clustering")
_CLUSTERING_JOB_LOCK = RLock()
_CLUSTERING_JOB_IDS_BY_CACHE_KEY: dict[Tuple[str, str], str] = {}


def start_clustering_job(
    session_id: str,
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: list[str] | None = None,
    cluster_count_is_explicit: bool = False,
) -> dict[str, Any]:  # one-off
    request_state = _build_clustering_request_state(
        table_name=table_name,
        cluster_count=cluster_count,
        sample_limit=sample_limit,
        sampling_strategy=sampling_strategy,
        feature_columns=feature_columns,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    cache_key_token = serialize_job_cache_key(request_state["cache_key"])
    params_payload = _build_params_payload(
        table_name=table_name,
        cluster_count=cluster_count,
        sample_limit=sample_limit,
        sampling_strategy=sampling_strategy,
        feature_columns=feature_columns,
        cluster_count_is_explicit=cluster_count_is_explicit,
    )
    reuse_coordinator = JobReuseCoordinator(
        session_id=session_id,
        cache_key_token=cache_key_token,
        job_ids_by_cache_key=_CLUSTERING_JOB_IDS_BY_CACHE_KEY,
        job_lock=_CLUSTERING_JOB_LOCK,
    )
    return start_cache_aware_job(
        reuse_coordinator=reuse_coordinator,
        build_status_payload=lambda job_id, reused: build_standard_job_status_payload(
            session_id,
            job_id,
            reused=reused,
        ),
        load_cached_payload=lambda: _CLUSTERING_CACHE.get(request_state["cache_key"]),
        create_jobs=lambda cache_hit: _create_clustering_job_bundle(
            session_id=session_id,
            cache_key_token=cache_key_token,
            params_payload=params_payload,
            cache_hit=cache_hit,
        ),
        handle_cached_payload=lambda bundle, cached_payload: _handle_cached_clustering_payload(
            session_id=session_id,
            bundle=bundle,
            cached_payload=cached_payload,
        ),
        submit_background_job=lambda bundle: _submit_clustering_job(
            session_id=session_id,
            bundle=bundle,
            params_payload=params_payload,
            cache_key_token=cache_key_token,
        ),
    )


def get_clustering_job_status(session_id: str, job_id: str) -> dict[str, Any]:  # one-off
    return build_standard_job_status_payload(session_id, job_id, reused=False)


def _run_clustering_job(
    session_id: str,
    job_id: str,
    params_payload: dict[str, Any],
    cache_key_token: str,
) -> None:
    reporter = StageTrackingJobProgressReporter(
        session_id=session_id,
        job_id=job_id,
        status_resolver=_clustering_status_resolver,
    )
    reuse_coordinator = JobReuseCoordinator(
        session_id=session_id,
        cache_key_token=cache_key_token,
        job_ids_by_cache_key=_CLUSTERING_JOB_IDS_BY_CACHE_KEY,
        job_lock=_CLUSTERING_JOB_LOCK,
    )
    run_background_job(
        reuse_coordinator=reuse_coordinator,
        primary_job_id=job_id,
        on_start=lambda: _start_clustering_job_execution(session_id=session_id, job_id=job_id),
        execute=lambda: get_clustering_data(
            table_name=str(params_payload["table_name"]),
            cluster_count=str(params_payload["cluster_count"]),
            sample_limit=str(params_payload["sample_limit"]),
            sampling_strategy=str(params_payload["sampling_strategy"]),
            feature_columns=list(params_payload.get("feature_columns") or []),
            cluster_count_is_explicit=bool(params_payload.get("cluster_count_is_explicit")),
            progress_callback=reporter.handle_progress,
        ),
        on_success=lambda payload: _finalize_clustering_job_success(
            session_id=session_id,
            job_id=job_id,
            payload=payload,
        ),
        on_failure=lambda error_message: _finalize_clustering_job_failure(
            session_id=session_id,
            job_id=job_id,
            error_message=error_message,
        ),
        build_error_message=lambda exc: f"\u041e\u0448\u0438\u0431\u043a\u0430 clustering-\u0437\u0430\u0434\u0430\u0447\u0438: {exc}",
    )


def _create_clustering_job_bundle(
    *,
    session_id: str,
    cache_key_token: str,
    params_payload: dict[str, Any],
    cache_hit: bool,
) -> JobLaunchBundle:
    job = job_store.create_or_reset_job(session_id=session_id, kind="clustering")
    attach_standard_job_metadata(
        session_id=session_id,
        job_id=job.job_id,
        cache_key_token=cache_key_token,
        params_payload=params_payload,
        cache_hit=cache_hit,
        stage_index=3 if cache_hit else 0,
        stage_label=(
            "\u041f\u043e\u0441\u0442\u0440\u043e\u0435\u043d\u0438\u0435 \u0432\u0438\u0437\u0443\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u0439"
            if cache_hit
            else "\u0417\u0430\u0433\u0440\u0443\u0437\u043a\u0430 \u0434\u0430\u043d\u043d\u044b\u0445"
        ),
        stage_message=(
            "\u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442 clustering \u0432\u0437\u044f\u0442 \u0438\u0437 \u043a\u044d\u0448\u0430."
            if cache_hit
            else "\u041a\u043b\u0430\u0441\u0442\u0435\u0440\u0438\u0437\u0430\u0446\u0438\u044f \u043f\u043e\u0441\u0442\u0430\u0432\u043b\u0435\u043d\u0430 \u0432 \u043e\u0447\u0435\u0440\u0435\u0434\u044c."
        ),
    )
    if cache_hit:
        return JobLaunchBundle(primary_job_id=job.job_id)
    job_store.mark_job_status(session_id, job.job_id, "pending")
    job_store.add_log(
        session_id,
        job.job_id,
        "\u0417\u0430\u0434\u0430\u0447\u0430 clustering \u043f\u043e\u0441\u0442\u0430\u0432\u043b\u0435\u043d\u0430 \u0432 \u043e\u0447\u0435\u0440\u0435\u0434\u044c. "
        "\u0418\u043d\u0442\u0435\u0440\u0444\u0435\u0439\u0441 \u043f\u0440\u043e\u0434\u043e\u043b\u0436\u0430\u0435\u0442 \u0440\u0430\u0431\u043e\u0442\u0430\u0442\u044c, "
        "\u043f\u043e\u043a\u0430 \u0440\u0430\u0441\u0447\u0435\u0442 \u0438\u0434\u0435\u0442 \u0432 \u0444\u043e\u043d\u0435.",
    )
    return JobLaunchBundle(primary_job_id=job.job_id)


def _handle_cached_clustering_payload(
    *,
    session_id: str,
    bundle: JobLaunchBundle,
    cached_payload: dict[str, Any],
) -> None:
    job_store.complete_job(
        session_id,
        bundle.primary_job_id,
        result=cached_payload,
        log_message=(
            "\u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442 clustering \u0432\u0437\u044f\u0442 \u0438\u0437 \u043a\u044d\u0448\u0430 "
            "\u0431\u0435\u0437 \u043f\u043e\u0432\u0442\u043e\u0440\u043d\u043e\u0433\u043e \u0437\u0430\u043f\u0443\u0441\u043a\u0430 "
            "\u0444\u043e\u043d\u043e\u0432\u043e\u0439 \u0437\u0430\u0434\u0430\u0447\u0438."
        ),
    )


def _submit_clustering_job(
    *,
    session_id: str,
    bundle: JobLaunchBundle,
    params_payload: dict[str, Any],
    cache_key_token: str,
) -> None:
    _CLUSTERING_JOB_EXECUTOR.submit(
        _run_clustering_job,
        session_id,
        bundle.primary_job_id,
        params_payload,
        cache_key_token,
    )


def _start_clustering_job_execution(*, session_id: str, job_id: str) -> None:
    job_store.mark_job_status(session_id, job_id, "running")
    job_store.add_log(
        session_id,
        job_id,
        "\u0424\u043e\u043d\u043e\u0432\u0430\u044f clustering-\u0437\u0430\u0434\u0430\u0447\u0430 \u0437\u0430\u043f\u0443\u0449\u0435\u043d\u0430.",
    )


def _finalize_clustering_job_success(
    *,
    session_id: str,
    job_id: str,
    payload: dict[str, Any],
) -> None:
    job_store.complete_job(
        session_id,
        job_id,
        result=payload,
        stage_index=3,
        stage_label="\u041f\u043e\u0441\u0442\u0440\u043e\u0435\u043d\u0438\u0435 \u0432\u0438\u0437\u0443\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u0439",
        stage_message="\u041a\u043b\u0430\u0441\u0442\u0435\u0440\u0438\u0437\u0430\u0446\u0438\u044f \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0430, \u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442\u044b \u0433\u043e\u0442\u043e\u0432\u044b.",
        log_message="\u041a\u043b\u0430\u0441\u0442\u0435\u0440\u0438\u0437\u0430\u0446\u0438\u044f \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0430, \u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442 \u0441\u043e\u0445\u0440\u0430\u043d\u0435\u043d \u0432 job_store.",
    )


def _finalize_clustering_job_failure(
    *,
    session_id: str,
    job_id: str,
    error_message: str,
) -> None:
    job_store.fail_job(
        session_id,
        job_id,
        error_message=error_message,
        log_message=error_message,
        stage_message=error_message,
    )


def _build_params_payload(
    *,
    table_name: str,
    cluster_count: str,
    sample_limit: str,
    sampling_strategy: str,
    feature_columns: list[str] | None,
    cluster_count_is_explicit: bool,
) -> dict[str, Any]:  # one-off
    return {
        "table_name": str(table_name or ""),
        "cluster_count": str(cluster_count or "4"),
        "sample_limit": str(sample_limit or "1000"),
        "sampling_strategy": str(sampling_strategy or "stratified"),
        "feature_columns": [str(item).strip() for item in (feature_columns or []) if str(item).strip()],
        "cluster_count_is_explicit": bool(cluster_count_is_explicit),
    }


def _clustering_status_resolver(normalized_phase: str, normalized_message: str) -> str | None:
    del normalized_message
    if normalized_phase.endswith(".running") or normalized_phase.startswith("clustering."):
        return "running"
    return None
