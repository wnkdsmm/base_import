from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from typing import Any, Dict, Tuple

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

from .core import (
    _FORECASTING_CACHE,
    _build_forecasting_request_state,
    get_forecasting_decision_support_data,
)
from .types import ForecastingJobStatus

_FORECASTING_DECISION_SUPPORT_EXECUTOR = ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix="forecasting-decision-support",
)
_FORECASTING_DECISION_SUPPORT_LOCK = RLock()
_FORECASTING_DECISION_SUPPORT_JOB_IDS_BY_CACHE_KEY: Dict[Tuple[str, str], str] = {}


def start_forecasting_decision_support_job(
    session_id: str,
    table_name: str = "all",
    district: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> ForecastingJobStatus:
    request_state = _build_forecasting_request_state(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
        include_decision_support=True,
    )
    cache_key_token = serialize_job_cache_key(request_state["cache_key"])
    params_payload = _build_params_payload(
        table_name=table_name,
        district=district,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
    )
    reuse_coordinator = JobReuseCoordinator(
        session_id=session_id,
        cache_key_token=cache_key_token,
        job_ids_by_cache_key=_FORECASTING_DECISION_SUPPORT_JOB_IDS_BY_CACHE_KEY,
        job_lock=_FORECASTING_DECISION_SUPPORT_LOCK,
    )
    return start_cache_aware_job(
        reuse_coordinator=reuse_coordinator,
        build_status_payload=lambda job_id, reused: build_standard_job_status_payload(
            session_id,
            job_id,
            reused=reused,
        ),
        load_cached_payload=lambda: _FORECASTING_CACHE.get(request_state["cache_key"]),
        create_jobs=lambda cache_hit: _create_forecasting_job_bundle(
            session_id=session_id,
            cache_key_token=cache_key_token,
            params_payload=params_payload,
            cache_hit=cache_hit,
        ),
        handle_cached_payload=lambda bundle, cached_payload: _handle_cached_forecasting_payload(
            session_id=session_id,
            bundle=bundle,
            cached_payload=cached_payload,
        ),
        submit_background_job=lambda bundle: _submit_forecasting_job(
            session_id=session_id,
            bundle=bundle,
            params_payload=params_payload,
            cache_key_token=cache_key_token,
        ),
    )


def get_forecasting_decision_support_job_status(session_id: str, job_id: str) -> ForecastingJobStatus:
    return build_standard_job_status_payload(session_id, job_id, reused=False)


def _run_forecasting_decision_support_job(
    session_id: str,
    job_id: str,
    params_payload: Dict[str, str],
    cache_key_token: str,
) -> None:
    reporter = StageTrackingJobProgressReporter(
        session_id=session_id,
        job_id=job_id,
        status_resolver=_forecasting_status_resolver,
    )
    reuse_coordinator = JobReuseCoordinator(
        session_id=session_id,
        cache_key_token=cache_key_token,
        job_ids_by_cache_key=_FORECASTING_DECISION_SUPPORT_JOB_IDS_BY_CACHE_KEY,
        job_lock=_FORECASTING_DECISION_SUPPORT_LOCK,
    )
    run_background_job(
        reuse_coordinator=reuse_coordinator,
        primary_job_id=job_id,
        on_start=lambda: _start_forecasting_job_execution(session_id=session_id, job_id=job_id),
        execute=lambda: get_forecasting_decision_support_data(
            table_name=params_payload["table_name"],
            district=params_payload["district"],
            cause=params_payload["cause"],
            object_category=params_payload["object_category"],
            temperature=params_payload["temperature"],
            forecast_days=params_payload["forecast_days"],
            history_window=params_payload["history_window"],
            progress_callback=reporter.handle_progress,
        ),
        on_success=lambda payload: _finalize_forecasting_job_success(
            session_id=session_id,
            job_id=job_id,
            payload=payload,
        ),
        on_failure=lambda error_message: _finalize_forecasting_job_failure(
            session_id=session_id,
            job_id=job_id,
            error_message=error_message,
        ),
        build_error_message=lambda exc: (
            f"\u041e\u0448\u0438\u0431\u043a\u0430 \u0431\u043b\u043e\u043a\u0430 "
            f"\u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 \u0440\u0435\u0448\u0435\u043d\u0438\u0439: {exc}"
        ),
    )


def _create_forecasting_job_bundle(
    *,
    session_id: str,
    cache_key_token: str,
    params_payload: Dict[str, str],
    cache_hit: bool,
) -> JobLaunchBundle:
    job = job_store.create_or_reset_job(session_id=session_id, kind="forecasting_decision_support")
    attach_standard_job_metadata(
        session_id=session_id,
        job_id=job.job_id,
        cache_key_token=cache_key_token,
        params_payload=params_payload,
        cache_hit=cache_hit,
        stage_index=3 if cache_hit else 0,
        stage_label=(
            "\u041f\u043e\u0441\u0442\u0440\u043e\u0435\u043d\u0438\u0435 "
            "\u0432\u0438\u0437\u0443\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u0439"
            if cache_hit
            else "\u0417\u0430\u0433\u0440\u0443\u0437\u043a\u0430 \u0434\u0430\u043d\u043d\u044b\u0445"
        ),
        stage_message=(
            "\u0411\u043b\u043e\u043a \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 "
            "\u0440\u0435\u0448\u0435\u043d\u0438\u0439 \u0432\u0437\u044f\u0442 \u0438\u0437 \u043a\u044d\u0448\u0430."
            if cache_hit
            else "\u0411\u043b\u043e\u043a \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 "
            "\u0440\u0435\u0448\u0435\u043d\u0438\u0439 \u043f\u043e\u0441\u0442\u0430\u0432\u043b\u0435\u043d \u0432 "
            "\u043e\u0447\u0435\u0440\u0435\u0434\u044c."
        ),
    )
    if cache_hit:
        return JobLaunchBundle(primary_job_id=job.job_id)
    job_store.mark_job_status(session_id, job.job_id, "pending")
    job_store.add_log(
        session_id,
        job.job_id,
        "\u0411\u043b\u043e\u043a \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 "
        "\u0440\u0435\u0448\u0435\u043d\u0438\u0439 \u043f\u043e\u0441\u0442\u0430\u0432\u043b\u0435\u043d \u0432 "
        "\u043e\u0447\u0435\u0440\u0435\u0434\u044c \u0438 \u0431\u0443\u0434\u0435\u0442 \u0440\u0430\u0441\u0441\u0447"
        "\u0438\u0442\u0430\u043d \u0432 \u0444\u043e\u043d\u0435.",
    )
    return JobLaunchBundle(primary_job_id=job.job_id)


def _handle_cached_forecasting_payload(
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
            "Decision support \u0443\u0436\u0435 \u0431\u044b\u043b \u0440\u0430\u0441\u0441\u0447"
            "\u0438\u0442\u0430\u043d \u0440\u0430\u043d\u0435\u0435 \u0438 \u0432\u0437\u044f\u0442 "
            "\u0438\u0437 \u043a\u044d\u0448\u0430."
        ),
    )


def _submit_forecasting_job(
    *,
    session_id: str,
    bundle: JobLaunchBundle,
    params_payload: Dict[str, str],
    cache_key_token: str,
) -> None:
    _FORECASTING_DECISION_SUPPORT_EXECUTOR.submit(
        _run_forecasting_decision_support_job,
        session_id,
        bundle.primary_job_id,
        params_payload,
        cache_key_token,
    )


def _start_forecasting_job_execution(*, session_id: str, job_id: str) -> None:
    job_store.mark_job_status(session_id, job_id, "running")
    job_store.add_log(
        session_id,
        job_id,
        "\u0424\u043e\u043d\u043e\u0432\u0430\u044f \u0437\u0430\u0434\u0430\u0447\u0430 decision support "
        "\u0437\u0430\u043f\u0443\u0449\u0435\u043d\u0430.",
    )


def _finalize_forecasting_job_success(
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
        stage_message=(
            "\u0411\u043b\u043e\u043a \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 "
            "\u0440\u0435\u0448\u0435\u043d\u0438\u0439 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d, "
            "\u0440\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0430\u0446\u0438\u0438 \u0433\u043e\u0442\u043e\u0432\u044b."
        ),
        log_message=(
            "\u0411\u043b\u043e\u043a \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438 "
            "\u0440\u0435\u0448\u0435\u043d\u0438\u0439 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d, "
            "\u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442 \u0441\u043e\u0445\u0440\u0430\u043d\u0435\u043d "
            "\u0432 job_store."
        ),
    )


def _finalize_forecasting_job_failure(
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
    district: str,
    cause: str,
    object_category: str,
    temperature: str,
    forecast_days: str,
    history_window: str,
) -> Dict[str, str]:
    return {
        "table_name": str(table_name or "all"),
        "district": str(district or "all"),
        "cause": str(cause or "all"),
        "object_category": str(object_category or "all"),
        "temperature": str(temperature or ""),
        "forecast_days": str(forecast_days or "14"),
        "history_window": str(history_window or "all"),
    }


def _forecasting_status_resolver(normalized_phase: str, normalized_message: str) -> str | None:
    del normalized_message
    if normalized_phase.endswith(".pending"):
        return "pending"
    if normalized_phase.endswith(".completed"):
        return None
    if normalized_phase.startswith("decision_support.") or normalized_phase.startswith(
        "forecasting_decision_support."
    ):
        return "running"
    return None
