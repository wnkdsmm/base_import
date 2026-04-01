"""Compatibility facade for legacy clustering imports."""

from app.services.clustering.core import get_clustering_data, get_clustering_page_context
from app.services.clustering.jobs import get_clustering_job_status, start_clustering_job

__all__ = [
    "get_clustering_data",
    "get_clustering_job_status",
    "get_clustering_page_context",
    "start_clustering_job",
]
