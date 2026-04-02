import unittest

from app import statistics as legacy_statistics
from app import services as services_package
from app.dashboard import aggregates as legacy_dashboard_aggregates
from app.dashboard import cache as dashboard_cache
from app.dashboard import distribution as dashboard_distribution
from app.dashboard import metadata as dashboard_metadata
from app.dashboard import service as dashboard_service_core
from app.dashboard import summary as dashboard_summary
from app.dashboard import utils as dashboard_utils
from app.services import access_points_service as legacy_access_points_service
from app.services import clustering_service as legacy_clustering_service
from app.services import dashboard_service as legacy_dashboard_service
from app.services import forecast_risk_service as legacy_forecast_risk_service
from app.services import forecasting_service as legacy_forecasting_service
from app.services import ml_model_service as legacy_ml_model_service
from app.services.access_points import core as access_points_core
from app.services.clustering import core as clustering_core
from app.services.clustering import jobs as clustering_jobs
from app.services.forecast_risk import core as forecast_risk_core
from app.services.forecast_risk import profiles as forecast_risk_profiles
from app.services.forecast_risk import validation as forecast_risk_validation
from app.services.forecasting import core as forecasting_core
from app.services.forecasting import jobs as forecasting_jobs
from app.services.ml_model import core as ml_model_core
from app.services.ml_model import jobs as ml_model_jobs

class ServicesCompatibilityFacadeTests(unittest.TestCase):
    def assert_exports_match(self, facade: object, exports: dict[str, object]) -> None:
        for export_name, canonical in exports.items():
            self.assertIs(getattr(facade, export_name), canonical, export_name)

    def test_dashboard_service_reexports_canonical_dashboard_context_functions(self) -> None:
        self.assertIs(legacy_dashboard_service.get_dashboard_page_context, dashboard_service_core.get_dashboard_page_context)
        self.assertIs(legacy_dashboard_service.get_dashboard_shell_context, dashboard_service_core.get_dashboard_shell_context)

    def test_services_package_reexports_canonical_dashboard_page_context(self) -> None:
        self.assertIs(services_package.get_dashboard_page_context, dashboard_service_core.get_dashboard_page_context)

    def test_access_points_service_reexports_canonical_functions(self) -> None:
        self.assert_exports_match(
            legacy_access_points_service,
            {
                "clear_access_points_cache": access_points_core.clear_access_points_cache,
                "get_access_points_data": access_points_core.get_access_points_data,
                "get_access_points_page_context": access_points_core.get_access_points_page_context,
                "get_access_points_shell_context": access_points_core.get_access_points_shell_context,
            },
        )

    def test_clustering_service_reexports_canonical_functions(self) -> None:
        self.assert_exports_match(
            legacy_clustering_service,
            {
                "get_clustering_data": clustering_core.get_clustering_data,
                "get_clustering_job_status": clustering_jobs.get_clustering_job_status,
                "get_clustering_page_context": clustering_core.get_clustering_page_context,
                "start_clustering_job": clustering_jobs.start_clustering_job,
            },
        )

    def test_forecast_risk_service_reexports_canonical_functions(self) -> None:
        self.assert_exports_match(
            legacy_forecast_risk_service,
            {
                "DEFAULT_RISK_WEIGHT_MODE": forecast_risk_profiles.DEFAULT_RISK_WEIGHT_MODE,
                "build_decision_support_payload": forecast_risk_core.build_decision_support_payload,
                "build_historical_validation_payload": forecast_risk_validation.build_historical_validation_payload,
                "build_risk_forecast_payload": forecast_risk_core.build_risk_forecast_payload,
                "build_weight_profile_snapshot": forecast_risk_profiles.build_weight_profile_snapshot,
                "empty_historical_validation_payload": forecast_risk_validation.empty_historical_validation_payload,
                "get_risk_weight_profile": forecast_risk_profiles.get_risk_weight_profile,
                "resolve_component_weights": forecast_risk_profiles.resolve_component_weights,
                "resolve_weight_profile_for_records": forecast_risk_validation.resolve_weight_profile_for_records,
            },
        )

    def test_forecasting_service_reexports_canonical_functions(self) -> None:
        self.assert_exports_match(
            legacy_forecasting_service,
            {
                "clear_forecasting_cache": forecasting_core.clear_forecasting_cache,
                "get_forecasting_data": forecasting_core.get_forecasting_data,
                "get_forecasting_decision_support_data": forecasting_core.get_forecasting_decision_support_data,
                "get_forecasting_decision_support_job_status": forecasting_jobs.get_forecasting_decision_support_job_status,
                "get_forecasting_metadata": forecasting_core.get_forecasting_metadata,
                "get_forecasting_page_context": forecasting_core.get_forecasting_page_context,
                "get_forecasting_shell_context": forecasting_core.get_forecasting_shell_context,
                "start_forecasting_decision_support_job": forecasting_jobs.start_forecasting_decision_support_job,
            },
        )

    def test_ml_model_service_reexports_canonical_functions(self) -> None:
        self.assert_exports_match(
            legacy_ml_model_service,
            {
                "clear_ml_model_cache": ml_model_core.clear_ml_model_cache,
                "get_ml_job_status": ml_model_jobs.get_ml_job_status,
                "get_ml_model_data": ml_model_core.get_ml_model_data,
                "get_ml_model_shell_context": ml_model_core.get_ml_model_shell_context,
                "start_ml_model_job": ml_model_jobs.start_ml_model_job,
            },
        )

    def test_statistics_module_reexports_dashboard_helpers(self) -> None:
        self.assert_exports_match(
            legacy_statistics,
            {
                "_collect_dashboard_metadata_cached": dashboard_cache._collect_dashboard_metadata_cached,
                "_collect_group_column_options": dashboard_metadata._collect_group_column_options,
                "_collect_year_options": dashboard_metadata._collect_year_options,
                "_empty_dashboard_data": dashboard_service_core._empty_dashboard_data,
                "_find_option_label": dashboard_utils._find_option_label,
                "_invalidate_dashboard_caches": dashboard_cache._invalidate_dashboard_caches,
                "_parse_year": dashboard_utils._parse_year,
                "_resolve_group_column": dashboard_metadata._resolve_group_column,
                "_resolve_selected_tables": dashboard_metadata._resolve_selected_tables,
                "build_dashboard_context": dashboard_service_core.build_dashboard_context,
                "get_dashboard_data": dashboard_service_core.get_dashboard_data,
            },
        )

    def test_dashboard_aggregates_module_reexports_canonical_builders(self) -> None:
        self.assert_exports_match(
            legacy_dashboard_aggregates,
            {
                "_build_damage_overview_chart": dashboard_distribution._build_damage_overview_chart,
                "_build_distribution_chart": dashboard_distribution._build_distribution_chart,
                "_build_rankings": dashboard_distribution._build_rankings,
                "_build_summary": dashboard_summary._build_summary,
                "_build_trend": dashboard_summary._build_trend,
                "_build_yearly_chart": dashboard_summary._build_yearly_chart,
            },
        )


if __name__ == "__main__":
    unittest.main()
