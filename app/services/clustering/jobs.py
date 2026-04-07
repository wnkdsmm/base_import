from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from typing import Any, Dict, Tuple

from app.log_manager import add_log
from app.services.job_support import (
    StageTrackingJobProgressReporter,
    attach_standard_job_metadata,
    build_standard_job_status_payload,
    discard_reusable_job_id,
    find_reusable_job_id,
    serialize_job_cache_key,
)
from app.state import job_store

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
    cluster_count_is_explicit: bool = False,
) -> Dict[str, Any]:
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

    with _CLUSTERING_JOB_LOCK:
        reusable_job_id = find_reusable_job_id(
            session_id,
            cache_key_token,
            job_ids_by_cache_key=_CLUSTERING_JOB_IDS_BY_CACHE_KEY,
        )
        if reusable_job_id:
            return build_standard_job_status_payload(session_id, reusable_job_id, reused=True)

        cached_payload = _CLUSTERING_CACHE.get(request_state["cache_key"])
        if cached_payload is not None:
            job = job_store.create_or_reset_job(session_id=session_id, kind="clustering")
            attach_standard_job_metadata(
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
            return build_standard_job_status_payload(session_id, job.job_id, reused=False)

        job = job_store.create_or_reset_job(session_id=session_id, kind="clustering")
        attach_standard_job_metadata(
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
        return build_standard_job_status_payload(session_id, job.job_id, reused=False)


def get_clustering_job_status(session_id: str, job_id: str) -> Dict[str, Any]:
    return build_standard_job_status_payload(session_id, job_id, reused=False)


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
            cluster_count_is_explicit=bool(params_payload.get("cluster_count_is_explicit")),
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
                discard_reusable_job_id(
                    session_id,
                    cache_key_token,
                    job_id,
                    job_ids_by_cache_key=_CLUSTERING_JOB_IDS_BY_CACHE_KEY,
                )


def _build_params_payload(
    *,
    table_name: str,
    cluster_count: str,
    sample_limit: str,
    sampling_strategy: str,
    feature_columns: list[str] | None,
    cluster_count_is_explicit: bool,
) -> Dict[str, Any]:
    return {
        "table_name": str(table_name or ""),
        "cluster_count": str(cluster_count or "4"),
        "sample_limit": str(sample_limit or "1000"),
        "sampling_strategy": str(sampling_strategy or "stratified"),
        "feature_columns": [str(item).strip() for item in (feature_columns or []) if str(item).strip()],
        "cluster_count_is_explicit": bool(cluster_count_is_explicit),
    }


class _ClusteringJobProgressReporter(StageTrackingJobProgressReporter):
    def _update_status(self, normalized_phase: str, normalized_message: str) -> None:
        if normalized_phase.endswith(".running") or normalized_phase.startswith("clustering."):
            job_store.mark_job_status(self._session_id, self._job_id, "running")
