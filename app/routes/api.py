from __future__ import annotations

# ROUTE DEPENDENCY GRAPH (endpoint -> service module -> shared dependencies)
# API endpoints
# - GET /api/dashboard-data -> app.dashboard.service.get_dashboard_data -> app.routes.api_common, app.state
# - GET /api/forecasting-data -> app.services.forecasting.core.get_forecasting_data|get_forecasting_decision_support_data -> app.routes.api_common, app.state
# - GET /api/forecasting-metadata -> app.services.forecasting.core.get_forecasting_metadata -> app.routes.api_common, app.state
# - POST /api/forecasting-decision-support-jobs -> app.services.forecasting.jobs.start_forecasting_decision_support_job -> app.routes.api_common, app.state
# - GET /api/forecasting-decision-support-jobs/{job_id} -> app.services.forecasting.jobs.get_forecasting_decision_support_job_status -> app.routes.api_common, app.state
# - GET /api/clustering-data -> app.services.clustering.core.get_clustering_data -> app.routes.api_common, app.state
# - POST /api/clustering-jobs -> app.services.clustering.jobs.start_clustering_job -> app.routes.api_common, app.state
# - GET /api/clustering-jobs/{job_id} -> app.services.clustering.jobs.get_clustering_job_status -> app.routes.api_common, app.state
# - GET /api/ml-model-data -> app.services.ml_model.core.get_ml_model_data -> app.routes.api_common, app.state
# - POST /api/ml-model-jobs -> app.services.ml_model.jobs.start_ml_model_job -> app.routes.api_common, app.state
# - GET /api/ml-model-jobs/{job_id} -> app.services.ml_model.jobs.get_ml_job_status -> app.routes.api_common, app.state
# - GET /api/access-points-data -> app.services.access_points.core.get_access_points_data -> app.routes.api_common, app.state
# - GET /api/column-search -> app.services.table_workflows.build_column_search_payload -> app.routes.api_common
# - POST /api/column-search/preview -> app.services.table_workflows.build_column_search_preview_payload -> app.routes.api_common
# - POST /api/column-search/create-modify-table -> app.services.table_workflows.build_create_modify_table_payload -> app.routes.api_common
# - GET /api/tables/{table_name}/page -> app.services.table_workflows.build_table_page_api_payload -> app.routes.api_common
# - DELETE /api/tables/{table_name} -> app.table_operations.delete_table -> app.routes.api_common
# - POST /api/tables/delete -> app.table_operations.delete_tables -> app.routes.api_common
# - POST /upload -> app.services.pipeline_service.save_uploaded_file -> app.routes.api_common, app.state
# - POST /import_data -> app.services.pipeline_service.import_uploaded_data -> app.routes.api_common, app.state
# - POST /run_profiling -> app.services.pipeline_service.run_profiling_for_table -> app.routes.api_common, app.state
# - GET /logs -> app.services.ops_service.build_logs_payload -> app.routes.api_common, app.state
# - POST /clear_logs -> app.services.ops_service.clear_logs_payload -> app.routes.api_common, app.state
# - GET /health -> app.services.ops_service.build_health_payload -> app.routes.api_common, app.state
#
# Page endpoints (defined in app.routes.pages)
# - /, /forecasting, /ml-model, /backtesting, /clustering, /access-points, /column-search,
#   /fire-map, /fire-map/embed, /tables, /tables/{table_name}, /select_table, /brief/*, /assets/plotly.js
#   -> app.dashboard.service | app.services.forecasting.core | app.services.ml_model.core |
#      app.services.clustering.core | app.services.access_points.core | app.services.fire_map_service |
#      app.services.table_workflows | app.table_metadata | app.plotly_bundle
#   -> shared page dependencies: app.routes.page_common (templates/assets helpers), app.state (session job context)

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
