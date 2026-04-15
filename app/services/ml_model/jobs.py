from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from typing import Any, Dict, Tuple

from app.services.job_support import (
    JobLaunchBundle,
    JobReuseCoordinator,
    LinkedJobProgressReporter,
    LinkedJobStatusSpec,
    StageTrackingJobProgressReporter,
    attach_linked_job_metadata,
    build_linked_job_status_payload,
    run_background_job,
    serialize_job_cache_key,
    start_cache_aware_job,
)
from app.state import FINAL_JOB_STATUSES, job_store

from .core import _build_ml_request_state, _cache_get, get_ml_model_data

_ML_JOB_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ml-model")
_ML_JOB_LOCK = RLock()
_ML_JOB_IDS_BY_CACHE_KEY: Dict[Tuple[str, str], str] = {}
_BACKTEST_STATUS_SPEC = LinkedJobStatusSpec(
    payload_key="backtest_job",
    meta_key="backtest_job_id",
    include_result=False,
    include_meta=False,
)


def start_ml_model_job(
    session_id: str,
    table_name: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> dict[str, Any]:
    request_state = _build_ml_request_state(
        table_name=table_name,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
    )
    cache_key_token = _serialize_cache_key(request_state["cache_key"])
    params_payload = _build_params_payload(
        table_name=table_name,
        cause=cause,
        object_category=object_category,
        temperature=temperature,
        forecast_days=forecast_days,
        history_window=history_window,
    )
    reuse_coordinator = JobReuseCoordinator(
        session_id=session_id,
        cache_key_token=cache_key_token,
        job_ids_by_cache_key=_ML_JOB_IDS_BY_CACHE_KEY,
        job_lock=_ML_JOB_LOCK,
    )
    return start_cache_aware_job(
        reuse_coordinator=reuse_coordinator,
        build_status_payload=lambda job_id, reused: _build_job_status_payload(
            session_id,
            job_id,
            reused=reused,
        ),
        load_cached_payload=lambda: _cache_get(request_state["cache_key"]),
        create_jobs=lambda cache_hit: _create_ml_job_bundle(
            session_id=session_id,
            cache_key_token=cache_key_token,
            params_payload=params_payload,
            cache_hit=cache_hit,
        ),
        handle_cached_payload=lambda bundle, cached_payload: _handle_cached_ml_payload(
            session_id=session_id,
            bundle=bundle,
            cached_payload=cached_payload,
        ),
        submit_background_job=lambda bundle: _submit_ml_job(
            session_id=session_id,
            bundle=bundle,
            params_payload=params_payload,
            cache_key_token=cache_key_token,
        ),
    )


def get_ml_job_status(session_id: str, job_id: str) -> dict[str, Any]:
    return _build_job_status_payload(session_id, job_id, reused=False)


def _run_ml_model_job(
    session_id: str,
    main_job_id: str,
    backtest_job_id: str,
    params_payload: Dict[str, str],
    cache_key_token: str,
) -> None:
    primary_reporter = StageTrackingJobProgressReporter(
        session_id=session_id,
        job_id=main_job_id,
        status_resolver=_ml_main_status_resolver,
    )
    reporter = LinkedJobProgressReporter(
        primary_reporter=primary_reporter,
        session_id=session_id,
        primary_job_id=main_job_id,
        secondary_job_id=backtest_job_id,
        secondary_phase_prefix="ml_backtest.",
        secondary_status_resolver=_ml_backtest_status_resolver,
        mirror_to_primary_prefix="[Backtesting] ",
    )
    reuse_coordinator = JobReuseCoordinator(
        session_id=session_id,
        cache_key_token=cache_key_token,
        job_ids_by_cache_key=_ML_JOB_IDS_BY_CACHE_KEY,
        job_lock=_ML_JOB_LOCK,
    )
    run_background_job(
        reuse_coordinator=reuse_coordinator,
        primary_job_id=main_job_id,
        on_start=lambda: _start_ml_job_execution(session_id=session_id, main_job_id=main_job_id),
        execute=lambda: get_ml_model_data(
            table_name=params_payload["table_name"],
            cause=params_payload["cause"],
            object_category=params_payload["object_category"],
            temperature=params_payload["temperature"],
            forecast_days=params_payload["forecast_days"],
            history_window=params_payload["history_window"],
            progress_callback=reporter.handle_progress,
        ),
        on_success=lambda payload: _finalize_ml_job_success(
            session_id=session_id,
            main_job_id=main_job_id,
            backtest_job_id=backtest_job_id,
            payload=payload,
        ),
        on_failure=lambda error_message: _finalize_ml_job_failure(
            session_id=session_id,
            main_job_id=main_job_id,
            backtest_job_id=backtest_job_id,
            error_message=error_message,
        ),
        build_error_message=lambda exc: f"\u041e\u0448\u0438\u0431\u043a\u0430 ML-\u0430\u043d\u0430\u043b\u0438\u0437\u0430: {exc}",
    )


def _create_ml_job_bundle(
    *,
    session_id: str,
    cache_key_token: str,
    params_payload: Dict[str, str],
    cache_hit: bool,
) -> JobLaunchBundle:
    main_job = job_store.create_or_reset_job(session_id=session_id, kind="ml_model")
    backtest_job = job_store.create_or_reset_job(session_id=session_id, kind="ml_backtest")
    attach_linked_job_metadata(
        session_id=session_id,
        primary_job_id=main_job.job_id,
        cache_key_token=cache_key_token,
        params_payload=params_payload,
        cache_hit=cache_hit,
        primary_meta={"backtest_job_id": backtest_job.job_id},
        linked_meta_by_job_id={
            backtest_job.job_id: {"parent_job_id": main_job.job_id},
        },
    )
    if cache_hit:
        return JobLaunchBundle(
            primary_job_id=main_job.job_id,
            related_job_ids={"backtest": backtest_job.job_id},
        )
    job_store.mark_job_status(session_id, main_job.job_id, "pending")
    job_store.mark_job_status(session_id, backtest_job.job_id, "pending")
    job_store.add_log(
        session_id,
        main_job.job_id,
        "ML-\u0437\u0430\u0434\u0430\u0447\u0430 \u043f\u043e\u0441\u0442\u0430\u0432\u043b\u0435\u043d\u0430 \u0432 "
        "\u043e\u0447\u0435\u0440\u0435\u0434\u044c. \u0418\u043d\u0442\u0435\u0440\u0444\u0435\u0439\u0441 "
        "\u043f\u0440\u043e\u0434\u043e\u043b\u0436\u0430\u0435\u0442 \u0440\u0430\u0431\u043e\u0442\u0430\u0442\u044c, "
        "\u043f\u043e\u043a\u0430 \u0440\u0430\u0441\u0447\u0451\u0442 \u0438\u0434\u0451\u0442 \u0432 \u0444\u043e\u043d\u0435.",
    )
    job_store.add_log(
        session_id,
        backtest_job.job_id,
        "Backtesting \u043e\u0436\u0438\u0434\u0430\u0435\u0442 \u0437\u0430\u043f\u0443\u0441\u043a\u0430 \u043f\u043e\u0441"
        "\u043b\u0435 \u043f\u043e\u0434\u0433\u043e\u0442\u043e\u0432\u043a\u0438 ML-\u0434\u0430\u043d\u043d\u044b\u0445.",
    )
    return JobLaunchBundle(
        primary_job_id=main_job.job_id,
        related_job_ids={"backtest": backtest_job.job_id},
    )


def _handle_cached_ml_payload(
    *,
    session_id: str,
    bundle: JobLaunchBundle,
    cached_payload: dict[str, Any],
) -> None:
    backtest_job_id = bundle.related_job_ids["backtest"]
    job_store.complete_job(
        session_id,
        bundle.primary_job_id,
        result=cached_payload,
        log_message=(
            "\u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442 ML-\u0430\u043d\u0430\u043b\u0438\u0437\u0430 "
            "\u0432\u0437\u044f\u0442 \u0438\u0437 \u043a\u044d\u0448\u0430 \u0431\u0435\u0437 \u043f\u043e\u0432\u0442"
            "\u043e\u0440\u043d\u043e\u0433\u043e \u0437\u0430\u043f\u0443\u0441\u043a\u0430 \u0444\u043e\u043d\u043e\u0432\u043e\u0439 "
            "\u0437\u0430\u0434\u0430\u0447\u0438."
        ),
    )
    job_store.complete_job(
        session_id,
        backtest_job_id,
        result=_extract_backtest_result(cached_payload),
        log_message=(
            "Backtesting \u0443\u0436\u0435 \u0431\u044b\u043b \u0440\u0430\u0441\u0441\u0447"
            "\u0438\u0442\u0430\u043d \u0440\u0430\u043d\u0435\u0435 \u0438 \u0432\u0437\u044f\u0442 \u0438\u0437 "
            "\u043a\u044d\u0448\u0430 \u0432\u043c\u0435\u0441\u0442\u0435 \u0441 ML-\u0440\u0435\u0437\u0443\u043b\u044c"
            "\u0442\u0430\u0442\u043e\u043c."
        ),
    )


def _submit_ml_job(
    *,
    session_id: str,
    bundle: JobLaunchBundle,
    params_payload: Dict[str, str],
    cache_key_token: str,
) -> None:
    _ML_JOB_EXECUTOR.submit(
        _run_ml_model_job,
        session_id,
        bundle.primary_job_id,
        bundle.related_job_ids["backtest"],
        params_payload,
        cache_key_token,
    )


def _start_ml_job_execution(*, session_id: str, main_job_id: str) -> None:
    job_store.mark_job_status(session_id, main_job_id, "running")
    job_store.add_log(
        session_id,
        main_job_id,
        "\u0424\u043e\u043d\u043e\u0432\u044b\u0439 ML-\u0430\u043d\u0430\u043b\u0438\u0437 \u0437\u0430\u043f\u0443\u0449\u0435\u043d.",
    )


def _finalize_ml_job_success(
    *,
    session_id: str,
    main_job_id: str,
    backtest_job_id: str,
    payload: dict[str, Any],
) -> None:
    job_store.complete_job(
        session_id,
        main_job_id,
        result=payload,
        log_message=(
            "ML-\u0430\u043d\u0430\u043b\u0438\u0437 \u0437\u0430\u0432\u0435\u0440\u0448\u0451\u043d, "
            "\u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442 \u0441\u043e\u0445\u0440\u0430\u043d\u0451\u043d "
            "\u0432 job_store."
        ),
    )
    backtest_result = _extract_backtest_result(payload)
    if job_store.get_job_status(session_id, backtest_job_id) not in FINAL_JOB_STATUSES:
        job_store.complete_job(
            session_id,
            backtest_job_id,
            result=backtest_result,
            log_message=(
                "Backtesting \u0437\u0430\u0432\u0435\u0440\u0448\u0451\u043d \u0432 \u0441\u043e\u0441\u0442\u0430\u0432\u0435 "
                "\u043e\u0431\u0449\u0435\u0439 ML-\u0437\u0430\u0434\u0430\u0447\u0438."
            ),
        )
        return
    job_store.set_job_result(session_id, backtest_job_id, backtest_result)


def _finalize_ml_job_failure(
    *,
    session_id: str,
    main_job_id: str,
    backtest_job_id: str,
    error_message: str,
) -> None:
    job_store.fail_job(
        session_id,
        main_job_id,
        error_message=error_message,
        log_message=error_message,
    )
    if job_store.get_job_status(session_id, backtest_job_id) in FINAL_JOB_STATUSES:
        return
    job_store.fail_job(
        session_id,
        backtest_job_id,
        error_message=error_message,
        log_message=(
            "Backtesting \u043e\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d \u0438\u0437-\u0437\u0430 "
            "\u043e\u0448\u0438\u0431\u043a\u0438 \u0432 \u043e\u0431\u0449\u0435\u0439 ML-\u0437\u0430\u0434\u0430\u0447\u0435."
        ),
    )


def _build_job_status_payload(session_id: str, job_id: str, *, reused: bool) -> dict[str, Any]:
    return build_linked_job_status_payload(
        session_id,
        job_id,
        reused=reused,
        linked_specs=[_BACKTEST_STATUS_SPEC],
        include_meta=True,
        missing_include_meta=False,
    )


def _build_params_payload(
    table_name: str,
    cause: str,
    object_category: str,
    temperature: str,
    forecast_days: str,
    history_window: str,
) -> Dict[str, str]:
    return {
        "table_name": str(table_name or "all"),
        "cause": str(cause or "all"),
        "object_category": str(object_category or "all"),
        "temperature": str(temperature or ""),
        "forecast_days": str(forecast_days or "14"),
        "history_window": str(history_window or "all"),
    }


def _extract_backtest_result(payload: dict[str, Any]) -> dict[str, Any]:
    quality_assessment = payload.get("quality_assessment") or {}
    summary = payload.get("summary") or {}
    return {
        "quality_assessment": quality_assessment,
        "summary": {
            "backtest_method_label": summary.get("backtest_method_label") or "",
            "count_model_label": summary.get("count_model_label") or "",
            "event_backtest_model_label": summary.get("event_backtest_model_label") or "",
        },
    }


def _serialize_cache_key(cache_key: Tuple[Any, ...]) -> str:
    return serialize_job_cache_key(cache_key)


def _ml_main_status_resolver(normalized_phase: str, normalized_message: str) -> str | None:
    del normalized_message
    if normalized_phase.startswith("ml_model.") and normalized_phase.endswith(".running"):
        return "running"
    return None


def _ml_backtest_status_resolver(normalized_phase: str, normalized_message: str) -> str | None:
    del normalized_message
    if normalized_phase.endswith(".pending"):
        return "pending"
    if normalized_phase.endswith(".running"):
        return "running"
    if normalized_phase.endswith(".completed"):
        return "completed"
    if normalized_phase.endswith(".failed"):
        return "failed"
    return None
