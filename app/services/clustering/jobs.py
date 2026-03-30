from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from typing import Any, Dict, Optional, Tuple

from app.log_manager import add_log
from app.state import FINAL_JOB_STATUSES, job_store

from .core import _CLUSTERING_CACHE, _build_clustering_request_state, get_clustering_data

_CLUSTERING_JOB_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="clustering")
_CLUSTERING_JOB_LOCK = RLock()
_CLUSTERING_JOB_IDS_BY_CACHE_KEY: Dict[Tuple[str, str], str] = {}


def start_clustering_job(
    session_id: str,
    table_name: str = "",
    cluster_count: str = "4",
    sample_limit: str = "1000",
    sampling_strategy: str = "stratified",
    feature_columns: list[str] | None = None,
) -> Dict[str, Any]:
    request_state = _build_clustering_request_state(
        table_name=table_name,
        cluster_count=cluster_count,
        sample_limit=sample_limit,
        sampling_strategy=sampling_strategy,
        feature_columns=feature_columns,
    )
    cache_key_token = _serialize_cache_key(request_state["cache_key"])
    params_payload = _build_params_payload(
        table_name=table_name,
        cluster_count=cluster_count,
        sample_limit=sample_limit,
        sampling_strategy=sampling_strategy,
        feature_columns=feature_columns,
    )

    with _CLUSTERING_JOB_LOCK:
        reusable_job_id = _get_reusable_job_id(session_id, cache_key_token)
        if reusable_job_id:
            return _build_job_status_payload(session_id, reusable_job_id, reused=True)

        cached_payload = _CLUSTERING_CACHE.get(request_state["cache_key"])
        if cached_payload is not None:
            job = job_store.create_or_reset_job(session_id=session_id, kind="clustering")
            _attach_job_metadata(
                session_id=session_id,
                job_id=job.job_id,
                cache_key_token=cache_key_token,
                params_payload=params_payload,
                cache_hit=True,
                stage_index=3,
                stage_label="Построение визуализаций",
                stage_message="Результат clustering взят из кэша.",
            )
            job_store.set_job_result(session_id, job.job_id, cached_payload)
            job_store.mark_job_status(session_id, job.job_id, "completed")
            add_log(session_id, job.job_id, "Результат clustering взят из кэша без повторного запуска фоновой задачи.")
            _CLUSTERING_JOB_IDS_BY_CACHE_KEY[(session_id, cache_key_token)] = job.job_id
            return _build_job_status_payload(session_id, job.job_id, reused=False)

        job = job_store.create_or_reset_job(session_id=session_id, kind="clustering")
        _attach_job_metadata(
            session_id=session_id,
            job_id=job.job_id,
            cache_key_token=cache_key_token,
            params_payload=params_payload,
            cache_hit=False,
            stage_index=0,
            stage_label="Загрузка данных",
            stage_message="Кластеризация поставлена в очередь.",
        )
        job_store.mark_job_status(session_id, job.job_id, "pending")
        add_log(session_id, job.job_id, "Задача clustering поставлена в очередь. Интерфейс продолжает работать, пока расчет идет в фоне.")
        _CLUSTERING_JOB_IDS_BY_CACHE_KEY[(session_id, cache_key_token)] = job.job_id
        _CLUSTERING_JOB_EXECUTOR.submit(
            _run_clustering_job,
            session_id,
            job.job_id,
            params_payload,
            cache_key_token,
        )
        return _build_job_status_payload(session_id, job.job_id, reused=False)


def get_clustering_job_status(session_id: str, job_id: str) -> Dict[str, Any]:
    return _build_job_status_payload(session_id, job_id, reused=False)


def _run_clustering_job(
    session_id: str,
    job_id: str,
    params_payload: Dict[str, Any],
    cache_key_token: str,
) -> None:
    reporter = _ClusteringJobProgressReporter(session_id=session_id, job_id=job_id)
    final_status = "failed"
    try:
        job_store.mark_job_status(session_id, job_id, "running")
        add_log(session_id, job_id, "Фоновая clustering-задача запущена.")
        payload = get_clustering_data(
            table_name=str(params_payload["table_name"]),
            cluster_count=str(params_payload["cluster_count"]),
            sample_limit=str(params_payload["sample_limit"]),
            sampling_strategy=str(params_payload["sampling_strategy"]),
            feature_columns=list(params_payload.get("feature_columns") or []),
            progress_callback=reporter.handle_progress,
        )
        job_store.set_job_result(session_id, job_id, payload)
        job_store.mark_job_status(session_id, job_id, "completed")
        job_store.update_job_meta(
            session_id,
            job_id,
            stage_index=3,
            stage_label="Построение визуализаций",
            stage_message="Кластеризация завершена, результаты готовы.",
        )
        add_log(session_id, job_id, "Кластеризация завершена, результат сохранен в job_store.")
        final_status = "completed"
    except Exception as exc:
        error_message = f"Ошибка clustering-задачи: {exc}"
        job_store.set_job_error(session_id, job_id, error_message)
        job_store.mark_job_status(session_id, job_id, "failed")
        job_store.update_job_meta(
            session_id,
            job_id,
            stage_message=error_message,
        )
        add_log(session_id, job_id, error_message)
    finally:
        if final_status != "completed":
            with _CLUSTERING_JOB_LOCK:
                current_job_id = _CLUSTERING_JOB_IDS_BY_CACHE_KEY.get((session_id, cache_key_token))
                if current_job_id == job_id:
                    _CLUSTERING_JOB_IDS_BY_CACHE_KEY.pop((session_id, cache_key_token), None)


def _attach_job_metadata(
    *,
    session_id: str,
    job_id: str,
    cache_key_token: str,
    params_payload: Dict[str, Any],
    cache_hit: bool,
    stage_index: int,
    stage_label: str,
    stage_message: str,
) -> None:
    job_store.update_job_meta(
        session_id,
        job_id,
        cache_key=cache_key_token,
        cache_hit=cache_hit,
        params=params_payload,
        stage_index=stage_index,
        stage_label=stage_label,
        stage_message=stage_message,
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
            "meta": {},
            "reused": reused,
            "is_final": True,
        }
    return {
        "job_id": snapshot["job_id"],
        "kind": snapshot["kind"],
        "status": snapshot["status"],
        "logs": snapshot.get("logs") or [],
        "result": snapshot.get("result"),
        "error_message": snapshot.get("error_message") or "",
        "meta": snapshot.get("meta") or {},
        "reused": reused,
        "is_final": snapshot["status"] in FINAL_JOB_STATUSES,
    }


def _build_params_payload(
    *,
    table_name: str,
    cluster_count: str,
    sample_limit: str,
    sampling_strategy: str,
    feature_columns: list[str] | None,
) -> Dict[str, Any]:
    return {
        "table_name": str(table_name or ""),
        "cluster_count": str(cluster_count or "4"),
        "sample_limit": str(sample_limit or "1000"),
        "sampling_strategy": str(sampling_strategy or "stratified"),
        "feature_columns": [str(item).strip() for item in (feature_columns or []) if str(item).strip()],
    }


def _get_reusable_job_id(session_id: str, cache_key_token: str) -> Optional[str]:
    job_id = _CLUSTERING_JOB_IDS_BY_CACHE_KEY.get((session_id, cache_key_token))
    if not job_id:
        return None
    snapshot = job_store.get_job_snapshot(session_id, job_id=job_id)
    if snapshot is None:
        _CLUSTERING_JOB_IDS_BY_CACHE_KEY.pop((session_id, cache_key_token), None)
        return None
    if snapshot["status"] == "failed":
        _CLUSTERING_JOB_IDS_BY_CACHE_KEY.pop((session_id, cache_key_token), None)
        return None
    if snapshot["status"] not in {"pending", "running"}:
        return None
    return job_id


def _serialize_cache_key(cache_key: Tuple[Any, ...]) -> str:
    return json.dumps(list(cache_key), ensure_ascii=False, default=str)


class _ClusteringJobProgressReporter:
    def __init__(self, session_id: str, job_id: str) -> None:
        self._session_id = session_id
        self._job_id = job_id
        self._last_message = ""

    def handle_progress(self, phase: str, message: str) -> None:
        normalized_phase = str(phase or "").strip().lower()
        normalized_message = str(message or "").strip()
        if not normalized_message:
            return
        if normalized_message != self._last_message:
            add_log(self._session_id, self._job_id, normalized_message)
            self._last_message = normalized_message
        stage_meta = _stage_meta_for_phase(normalized_phase)
        if stage_meta is not None:
            job_store.update_job_meta(
                self._session_id,
                self._job_id,
                stage_index=stage_meta["stage_index"],
                stage_label=stage_meta["stage_label"],
                stage_message=normalized_message,
            )
        if normalized_phase.endswith(".running") or normalized_phase.startswith("clustering."):
            job_store.mark_job_status(self._session_id, self._job_id, "running")


def _stage_meta_for_phase(phase: str) -> Optional[Dict[str, Any]]:
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
