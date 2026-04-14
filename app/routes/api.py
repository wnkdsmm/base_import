from __future__ import annotations

from fastapi import APIRouter

from .api_access_points import access_points_data_endpoint, router as access_points_router
from .api_clustering import (
    clustering_data_endpoint,
    clustering_job_status_endpoint,
    router as clustering_router,
    start_clustering_job_endpoint,
)
from .api_column_search import (
    column_search_endpoint,
    column_search_preview_endpoint,
    create_modify_table_endpoint,
    router as column_search_router,
)
from .api_common import (
    analytics_error_response,
    analytics_exception_response,
    coerce_string_list,
    ensure_session_id,
    utf8_json,
)
from .api_dashboard import dashboard_data_endpoint, router as dashboard_router
from .api_forecasting import (
    forecasting_data_endpoint,
    forecasting_decision_support_job_status_endpoint,
    forecasting_metadata_endpoint,
    router as forecasting_router,
    start_forecasting_decision_support_job_endpoint,
)
from .api_ml_model import (
    ml_model_data_endpoint,
    ml_model_job_status_endpoint,
    router as ml_model_router,
    start_ml_model_job_endpoint,
)
from .api_ops import (
    clear_logs_endpoint,
    health_check,
    import_data_endpoint,
    logs,
    router as ops_router,
    run_profiling_endpoint,
    upload_file,
)
from .api_tables import delete_table_endpoint, delete_tables_endpoint, router as tables_router, table_page_endpoint


router = APIRouter()
for child_router in (
    dashboard_router,
    forecasting_router,
    clustering_router,
    ml_model_router,
    access_points_router,
    tables_router,
    column_search_router,
    ops_router,
):
    router.include_router(child_router)


__all__ = [
    "access_points_data_endpoint",
    "analytics_error_response",
    "analytics_exception_response",
    "clear_logs_endpoint",
    "clustering_data_endpoint",
    "clustering_job_status_endpoint",
    "coerce_string_list",
    "column_search_endpoint",
    "column_search_preview_endpoint",
    "create_modify_table_endpoint",
    "dashboard_data_endpoint",
    "delete_table_endpoint",
    "delete_tables_endpoint",
    "ensure_session_id",
    "forecasting_data_endpoint",
    "forecasting_decision_support_job_status_endpoint",
    "forecasting_metadata_endpoint",
    "health_check",
    "import_data_endpoint",
    "logs",
    "ml_model_data_endpoint",
    "ml_model_job_status_endpoint",
    "router",
    "run_profiling_endpoint",
    "start_clustering_job_endpoint",
    "start_forecasting_decision_support_job_endpoint",
    "start_ml_model_job_endpoint",
    "table_page_endpoint",
    "upload_file",
    "utf8_json",
]
