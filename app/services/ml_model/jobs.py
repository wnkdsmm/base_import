from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from typing import Any, Dict, Optional, Tuple

from app.log_manager import add_log
from app.state import FINAL_JOB_STATUSES, job_store

from .core import _build_ml_request_state, _cache_get, get_ml_model_data

_ML_JOB_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ml-model")
_ML_JOB_LOCK = RLock()
_ML_JOB_IDS_BY_CACHE_KEY: Dict[Tuple[str, str], str] = {}


def start_ml_model_job(
    session_id: str,
    table_name: str = "all",
    cause: str = "all",
    object_category: str = "all",
    temperature: str = "",
    forecast_days: str = "14",
    history_window: str = "all",
) -> Dict[str, Any]:
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

    with _ML_JOB_LOCK:
        reusable_job_id = _get_reusable_job_id(session_id, cache_key_token)
        if reusable_job_id:
            return _build_job_status_payload(
                session_id,
                reusable_job_id,
                reused=True,
            )

        cached_payload = _cache_get(request_state["cache_key"])
        if cached_payload is not None:
            main_job = job_store.create_or_reset_job(session_id=session_id, kind="ml_model")
            backtest_job = job_store.create_or_reset_job(session_id=session_id, kind="ml_backtest")
            _attach_job_metadata(
                session_id=session_id,
                main_job_id=main_job.job_id,
                backtest_job_id=backtest_job.job_id,
                cache_key_token=cache_key_token,
                params_payload=params_payload,
                cache_hit=True,
            )
            job_store.set_job_result(session_id, main_job.job_id, cached_payload)
            job_store.set_job_result(session_id, backtest_job.job_id, _extract_backtest_result(cached_payload))
            job_store.mark_job_status(session_id, main_job.job_id, "completed")
            job_store.mark_job_status(session_id, backtest_job.job_id, "completed")
            add_log(session_id, main_job.job_id, "Результат ML-анализа взят из кэша без повторного запуска фоновой задачи.")
            add_log(session_id, backtest_job.job_id, "Backtesting уже был рассчитан ранее и взят из кэша вместе с ML-результатом.")
            _ML_JOB_IDS_BY_CACHE_KEY[(session_id, cache_key_token)] = main_job.job_id
            return _build_job_status_payload(session_id, main_job.job_id, reused=False)

        main_job = job_store.create_or_reset_job(session_id=session_id, kind="ml_model")
        backtest_job = job_store.create_or_reset_job(session_id=session_id, kind="ml_backtest")
        _attach_job_metadata(
            session_id=session_id,
            main_job_id=main_job.job_id,
            backtest_job_id=backtest_job.job_id,
            cache_key_token=cache_key_token,
            params_payload=params_payload,
            cache_hit=False,
        )
        job_store.mark_job_status(session_id, main_job.job_id, "pending")
        job_store.mark_job_status(session_id, backtest_job.job_id, "pending")
        add_log(session_id, main_job.job_id, "ML-задача поставлена в очередь. Интерфейс продолжает работать, пока расчёт идёт в фоне.")
        add_log(session_id, backtest_job.job_id, "Backtesting ожидает запуска после подготовки ML-данных.")
        _ML_JOB_IDS_BY_CACHE_KEY[(session_id, cache_key_token)] = main_job.job_id
        _ML_JOB_EXECUTOR.submit(
            _run_ml_model_job,
            session_id,
            main_job.job_id,
            backtest_job.job_id,
            params_payload,
            cache_key_token,
        )
        return _build_job_status_payload(session_id, main_job.job_id, reused=False)


def get_ml_job_status(session_id: str, job_id: str) -> Dict[str, Any]:
    return _build_job_status_payload(session_id, job_id, reused=False)


def _run_ml_model_job(
    session_id: str,
    main_job_id: str,
    backtest_job_id: str,
    params_payload: Dict[str, str],
    cache_key_token: str,
) -> None:
    reporter = _MlJobProgressReporter(
        session_id=session_id,
        main_job_id=main_job_id,
        backtest_job_id=backtest_job_id,
    )
    final_status = "failed"
    try:
        job_store.mark_job_status(session_id, main_job_id, "running")
        add_log(session_id, main_job_id, "Фоновый ML-анализ запущен.")
        payload = get_ml_model_data(
            table_name=params_payload["table_name"],
            cause=params_payload["cause"],
            object_category=params_payload["object_category"],
            temperature=params_payload["temperature"],
            forecast_days=params_payload["forecast_days"],
            history_window=params_payload["history_window"],
            progress_callback=reporter.handle_progress,
        )
        job_store.set_job_result(session_id, main_job_id, payload)
        if job_store.get_job_status(session_id, backtest_job_id) not in FINAL_JOB_STATUSES:
            job_store.mark_job_status(session_id, backtest_job_id, "completed")
            add_log(session_id, backtest_job_id, "Backtesting завершён в составе общей ML-задачи.")
        job_store.set_job_result(session_id, backtest_job_id, _extract_backtest_result(payload))
        job_store.mark_job_status(session_id, main_job_id, "completed")
        add_log(session_id, main_job_id, "ML-анализ завершён, результат сохранён в job_store.")
        final_status = "completed"
    except Exception as exc:
        error_message = f"Ошибка ML-анализа: {exc}"
        job_store.set_job_error(session_id, main_job_id, error_message)
        job_store.mark_job_status(session_id, main_job_id, "failed")
        add_log(session_id, main_job_id, error_message)
        backtest_status = job_store.get_job_status(session_id, backtest_job_id)
        if backtest_status not in FINAL_JOB_STATUSES:
            job_store.set_job_error(session_id, backtest_job_id, error_message)
            job_store.mark_job_status(session_id, backtest_job_id, "failed")
            add_log(session_id, backtest_job_id, "Backtesting остановлен из-за ошибки в общей ML-задаче.")
    finally:
        if final_status != "completed":
            with _ML_JOB_LOCK:
                current_job_id = _ML_JOB_IDS_BY_CACHE_KEY.get((session_id, cache_key_token))
                if current_job_id == main_job_id:
                    _ML_JOB_IDS_BY_CACHE_KEY.pop((session_id, cache_key_token), None)


def _attach_job_metadata(
    session_id: str,
    main_job_id: str,
    backtest_job_id: str,
    cache_key_token: str,
    params_payload: Dict[str, str],
    cache_hit: bool,
) -> None:
    job_store.update_job_meta(
        session_id,
        main_job_id,
        cache_key=cache_key_token,
        cache_hit=cache_hit,
        params=params_payload,
        backtest_job_id=backtest_job_id,
    )
    job_store.update_job_meta(
        session_id,
        backtest_job_id,
        cache_key=cache_key_token,
        cache_hit=cache_hit,
        params=params_payload,
        parent_job_id=main_job_id,
    )


def _build_job_status_payload(session_id: str, job_id: str, *, reused: bool) -> Dict[str, Any]:
    snapshot = job_store.get_job_snapshot(session_id, job_id=job_id)
    if snapshot is None:
        return {
            "job_id": job_id,
            "status": "missing",
            "kind": "",
            "logs": [],
            "result": None,
            "error_message": "Job не найден для текущей сессии.",
            "reused": reused,
            "is_final": True,
        }

    meta = snapshot.get("meta") or {}
    backtest_job_id = str(meta.get("backtest_job_id") or "")
    backtest_snapshot = job_store.get_job_snapshot(session_id, job_id=backtest_job_id) if backtest_job_id else None
    payload = {
        "job_id": snapshot["job_id"],
        "kind": snapshot["kind"],
        "status": snapshot["status"],
        "logs": snapshot.get("logs") or [],
        "result": snapshot.get("result"),
        "error_message": snapshot.get("error_message") or "",
        "meta": meta,
        "reused": reused,
        "is_final": snapshot["status"] in FINAL_JOB_STATUSES,
    }
    if backtest_snapshot is not None:
        payload["backtest_job"] = {
            "job_id": backtest_snapshot["job_id"],
            "kind": backtest_snapshot["kind"],
            "status": backtest_snapshot["status"],
            "logs": backtest_snapshot.get("logs") or [],
            "error_message": backtest_snapshot.get("error_message") or "",
            "is_final": backtest_snapshot["status"] in FINAL_JOB_STATUSES,
        }
    else:
        payload["backtest_job"] = None
    return payload


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


def _extract_backtest_result(payload: Dict[str, Any]) -> Dict[str, Any]:
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


def _get_reusable_job_id(session_id: str, cache_key_token: str) -> Optional[str]:
    job_id = _ML_JOB_IDS_BY_CACHE_KEY.get((session_id, cache_key_token))
    if not job_id:
        return None
    snapshot = job_store.get_job_snapshot(session_id, job_id=job_id)
    if snapshot is None:
        _ML_JOB_IDS_BY_CACHE_KEY.pop((session_id, cache_key_token), None)
        return None
    if snapshot["status"] == "failed":
        _ML_JOB_IDS_BY_CACHE_KEY.pop((session_id, cache_key_token), None)
        return None
    if snapshot["status"] not in {"pending", "running"}:
        return None
    return job_id


def _serialize_cache_key(cache_key: Tuple[Any, ...]) -> str:
    return json.dumps(list(cache_key), ensure_ascii=False, default=str)


class _MlJobProgressReporter:
    def __init__(self, session_id: str, main_job_id: str, backtest_job_id: str) -> None:
        self._session_id = session_id
        self._main_job_id = main_job_id
        self._backtest_job_id = backtest_job_id
        self._last_main_message = ""
        self._last_backtest_message = ""

    def handle_progress(self, phase: str, message: str) -> None:
        normalized_phase = str(phase or "").strip().lower()
        normalized_message = str(message or "").strip()
        if not normalized_message:
            return
        if normalized_phase.startswith("ml_backtest"):
            self._handle_backtest_progress(normalized_phase, normalized_message)
            return
        if normalized_message != self._last_main_message:
            add_log(self._session_id, self._main_job_id, normalized_message)
            self._last_main_message = normalized_message
        if normalized_phase.endswith(".running"):
            job_store.mark_job_status(self._session_id, self._main_job_id, "running")

    def _handle_backtest_progress(self, phase: str, message: str) -> None:
        if message != self._last_backtest_message:
            add_log(self._session_id, self._backtest_job_id, message)
            add_log(self._session_id, self._main_job_id, f"[Backtesting] {message}")
            self._last_backtest_message = message
        if phase.endswith(".pending"):
            job_store.mark_job_status(self._session_id, self._backtest_job_id, "pending")
        elif phase.endswith(".running"):
            job_store.mark_job_status(self._session_id, self._backtest_job_id, "running")
        elif phase.endswith(".completed"):
            job_store.mark_job_status(self._session_id, self._backtest_job_id, "completed")
        elif phase.endswith(".failed"):
            job_store.set_job_error(self._session_id, self._backtest_job_id, message)
            job_store.mark_job_status(self._session_id, self._backtest_job_id, "failed")
