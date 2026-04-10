from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from typing import Any, Dict, Tuple

from app.services.job_support import (
    StageTrackingJobProgressReporter,
    attach_standard_job_metadata,
    build_standard_job_status_payload,
    discard_reusable_job_id,
    find_reusable_job_id,
    serialize_job_cache_key,
)
from app.state import job_store

from .core import (
    _FORECASTING_CACHE,
    _build_forecasting_request_state,
    get_forecasting_decision_support_data,
)

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
) -> Dict[str, Any]:
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

    with _FORECASTING_DECISION_SUPPORT_LOCK:
        reusable_job_id = find_reusable_job_id(
            session_id,
            cache_key_token,
            job_ids_by_cache_key=_FORECASTING_DECISION_SUPPORT_JOB_IDS_BY_CACHE_KEY,
        )
        if reusable_job_id:
            return build_standard_job_status_payload(session_id, reusable_job_id, reused=True)

        cached_payload = _FORECASTING_CACHE.get(request_state["cache_key"])
        if cached_payload is not None:
            job = job_store.create_or_reset_job(session_id=session_id, kind="forecasting_decision_support")
            attach_standard_job_metadata(
                session_id=session_id,
                job_id=job.job_id,
                cache_key_token=cache_key_token,
                params_payload=params_payload,
                cache_hit=True,
                stage_index=3,
                stage_label="Построение визуализаций",
                stage_message="Блок поддержки решений взят из кэша.",
            )
            job_store.set_job_result(session_id, job.job_id, cached_payload)
            job_store.mark_job_status(session_id, job.job_id, "completed")
            job_store.add_log(session_id, job.job_id, "Decision support уже был рассчитан ранее и взят из кэша.")
            _FORECASTING_DECISION_SUPPORT_JOB_IDS_BY_CACHE_KEY[(session_id, cache_key_token)] = job.job_id
            return build_standard_job_status_payload(session_id, job.job_id, reused=False)

        job = job_store.create_or_reset_job(session_id=session_id, kind="forecasting_decision_support")
        attach_standard_job_metadata(
            session_id=session_id,
            job_id=job.job_id,
            cache_key_token=cache_key_token,
            params_payload=params_payload,
            cache_hit=False,
            stage_index=0,
            stage_label="Загрузка данных",
            stage_message="Блок поддержки решений поставлен в очередь.",
        )
        job_store.mark_job_status(session_id, job.job_id, "pending")
        job_store.add_log(session_id, job.job_id, "Блок поддержки решений поставлен в очередь и будет рассчитан в фоне.")
        _FORECASTING_DECISION_SUPPORT_JOB_IDS_BY_CACHE_KEY[(session_id, cache_key_token)] = job.job_id
        _FORECASTING_DECISION_SUPPORT_EXECUTOR.submit(
            _run_forecasting_decision_support_job,
            session_id,
            job.job_id,
            params_payload,
            cache_key_token,
        )
        return build_standard_job_status_payload(session_id, job.job_id, reused=False)


def get_forecasting_decision_support_job_status(session_id: str, job_id: str) -> Dict[str, Any]:
    return build_standard_job_status_payload(session_id, job_id, reused=False)


def _run_forecasting_decision_support_job(
    session_id: str,
    job_id: str,
    params_payload: Dict[str, str],
    cache_key_token: str,
) -> None:
    reporter = _ForecastingDecisionSupportProgressReporter(session_id=session_id, job_id=job_id)
    final_status = "failed"
    try:
        job_store.mark_job_status(session_id, job_id, "running")
        job_store.add_log(session_id, job_id, "Фоновая задача decision support запущена.")
        payload = get_forecasting_decision_support_data(
            table_name=params_payload["table_name"],
            district=params_payload["district"],
            cause=params_payload["cause"],
            object_category=params_payload["object_category"],
            temperature=params_payload["temperature"],
            forecast_days=params_payload["forecast_days"],
            history_window=params_payload["history_window"],
            progress_callback=reporter.handle_progress,
        )
        job_store.set_job_result(session_id, job_id, payload)
        job_store.mark_job_status(session_id, job_id, "completed")
        job_store.update_job_meta(
            session_id,
            job_id,
            stage_index=3,
            stage_label="Построение визуализаций",
            stage_message="Блок поддержки решений завершен, рекомендации готовы.",
        )
        job_store.add_log(session_id, job_id, "Блок поддержки решений завершен, результат сохранен в job_store.")
        final_status = "completed"
    except Exception as exc:
        error_message = f"Ошибка блока поддержки решений: {exc}"
        job_store.set_job_error(session_id, job_id, error_message)
        job_store.mark_job_status(session_id, job_id, "failed")
        job_store.update_job_meta(
            session_id,
            job_id,
            stage_message=error_message,
        )
        job_store.add_log(session_id, job_id, error_message)
    finally:
        if final_status != "completed":
            with _FORECASTING_DECISION_SUPPORT_LOCK:
                discard_reusable_job_id(
                    session_id,
                    cache_key_token,
                    job_id,
                    job_ids_by_cache_key=_FORECASTING_DECISION_SUPPORT_JOB_IDS_BY_CACHE_KEY,
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


class _ForecastingDecisionSupportProgressReporter(StageTrackingJobProgressReporter):
    def _update_status(self, normalized_phase: str, normalized_message: str) -> None:
        if normalized_phase.endswith(".pending"):
            job_store.mark_job_status(self._session_id, self._job_id, "pending")
        elif normalized_phase.endswith(".completed"):
            return
        elif normalized_phase.startswith("decision_support.") or normalized_phase.startswith("forecasting_decision_support."):
            job_store.mark_job_status(self._session_id, self._job_id, "running")
