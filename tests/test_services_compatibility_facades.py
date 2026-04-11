import importlib
import importlib.util
import unittest

SERVICE_LEGACY_EXPORTS = [
    "build_fire_map_html",
    "get_column_search_table_options",
    "get_dashboard_page_context",
    "get_fire_map_table_options",
    "import_uploaded_data",
    "resolve_selected_table",
    "run_profiling_for_table",
    "save_uploaded_file",
]

DASHBOARD_LEGACY_EXPORTS = [
    "build_dashboard_context",
    "get_dashboard_data",
    "get_dashboard_page_context",
    "get_dashboard_shell_context",
]

ROUTES_LEGACY_EXPORTS = ["api_router", "pages_router"]

ACCESS_POINTS_LEGACY_EXPORTS = [
    "clear_access_points_cache",
    "get_access_points_data",
    "get_access_points_page_context",
    "get_access_points_shell_context",
]

FORECASTING_LEGACY_EXPORTS = ["charts", "constants", "core", "data", "jobs", "utils"]

FORECAST_RISK_LEGACY_EXPORTS = [
    "constants",
    "core",
    "data",
    "presentation",
    "profiles",
    "scoring",
    "utils",
    "validation",
]


class ServicesCompatibilityFacadeTests(unittest.TestCase):
    def assert_no_legacy_package_exports(
        self,
        module_name: str,
        export_names: list[str],
        *,
        assert_attributes: bool = True,
    ) -> None:
        package = importlib.reload(importlib.import_module(module_name))
        self.assertEqual(list(getattr(package, "__all__")), [])
        self.assertFalse(hasattr(package, "__getattr__"))
        if assert_attributes:
            for export_name in export_names:
                self.assertFalse(hasattr(package, export_name), export_name)

    def test_services_package_no_longer_reexports_legacy_helpers(self) -> None:
        self.assert_no_legacy_package_exports("app.services", SERVICE_LEGACY_EXPORTS)

    def test_dashboard_package_no_longer_reexports_legacy_helpers(self) -> None:
        self.assert_no_legacy_package_exports("app.dashboard", DASHBOARD_LEGACY_EXPORTS)

    def test_routes_package_no_longer_reexports_router_aliases(self) -> None:
        self.assert_no_legacy_package_exports("app.routes", ROUTES_LEGACY_EXPORTS)

    def test_access_points_package_no_longer_reexports_core_helpers(self) -> None:
        self.assert_no_legacy_package_exports("app.services.access_points", ACCESS_POINTS_LEGACY_EXPORTS)

    def test_forecasting_package_no_longer_lazy_exports_submodules(self) -> None:
        self.assert_no_legacy_package_exports(
            "app.services.forecasting",
            FORECASTING_LEGACY_EXPORTS,
            assert_attributes=False,
        )

    def test_forecast_risk_package_no_longer_lazy_exports_submodules(self) -> None:
        self.assert_no_legacy_package_exports(
            "app.services.forecast_risk",
            FORECAST_RISK_LEGACY_EXPORTS,
            assert_attributes=False,
        )

    def test_legacy_lazy_export_module_is_removed(self) -> None:
        self.assertIsNone(importlib.util.find_spec("app.compat"))

    def test_unused_forecasting_geo_shim_is_removed(self) -> None:
        self.assertIsNone(importlib.util.find_spec("app.services.forecasting.geo"))
        self.assertIsNotNone(importlib.util.find_spec("app.services.forecast_risk.geo"))

    def test_mapping_compatibility_shims_are_removed(self) -> None:
        self.assertIsNone(importlib.util.find_spec("core.mapping.fire_map_generator"))
        self.assertIsNone(importlib.util.find_spec("core.mapping.mixins.template_fragments"))
        self.assertIsNone(importlib.util.find_spec("app.services.table_options"))

    def test_canonical_service_modules_remain_importable(self) -> None:
        fire_map_service = importlib.import_module("app.services.fire_map_service")
        pipeline_service = importlib.import_module("app.services.pipeline_service")
        table_catalog = importlib.import_module("app.table_catalog")
        access_points_core = importlib.import_module("app.services.access_points.core")
        forecasting_core = importlib.import_module("app.services.forecasting.core")
        forecasting_jobs = importlib.import_module("app.services.forecasting.jobs")
        forecast_risk_core = importlib.import_module("app.services.forecast_risk.core")
        routes_api = importlib.import_module("app.routes.api")
        routes_pages = importlib.import_module("app.routes.pages")

        self.assertTrue(callable(fire_map_service.build_fire_map_html))
        self.assertTrue(callable(pipeline_service.import_uploaded_data))
        self.assertTrue(callable(pipeline_service.run_profiling_for_table))
        self.assertTrue(callable(pipeline_service.save_uploaded_file))
        self.assertTrue(callable(table_catalog.get_user_table_options))
        self.assertTrue(callable(table_catalog.resolve_selected_table_value))
        self.assertTrue(callable(table_catalog.build_table_options))
        self.assertTrue(callable(access_points_core.get_access_points_data))
        self.assertTrue(callable(forecasting_core.get_forecasting_page_context))
        self.assertTrue(callable(forecasting_jobs.start_forecasting_decision_support_job))
        self.assertTrue(callable(forecast_risk_core.build_decision_support_payload))
        self.assertTrue(hasattr(routes_api, "router"))
        self.assertTrue(hasattr(routes_pages, "router"))

    def test_canonical_dashboard_service_module_remains_importable(self) -> None:
        dashboard_service = importlib.import_module("app.dashboard.service")

        self.assertTrue(callable(dashboard_service.build_dashboard_context))
        self.assertTrue(callable(dashboard_service.get_dashboard_data))
        self.assertTrue(callable(dashboard_service.get_dashboard_page_context))
        self.assertTrue(callable(dashboard_service.get_dashboard_shell_context))

    def test_canonical_mapping_modules_remain_importable(self) -> None:
        mapping_config = importlib.import_module("core.mapping.config")
        mapping_creator = importlib.import_module("core.mapping.creator")
        template_analytics = importlib.import_module("core.mapping.mixins.template_analytics")
        template_layout = importlib.import_module("core.mapping.mixins.template_layout")
        template_scripts = importlib.import_module("core.mapping.mixins.template_scripts")

        self.assertTrue(hasattr(mapping_config, "Config"))
        self.assertTrue(hasattr(mapping_config, "MarkerStyle"))
        self.assertTrue(hasattr(mapping_creator, "MapCreator"))
        self.assertTrue(callable(template_analytics.build_analytics_layer_geojsons))
        self.assertTrue(callable(template_analytics.build_analytics_panel_html))
        self.assertTrue(callable(template_layout.build_filter_panel_html))
        self.assertTrue(callable(template_layout.generate_html))
        self.assertTrue(callable(template_scripts.build_tab_script_lines))


if __name__ == "__main__":
    unittest.main()
