import importlib
import unittest

from app import dashboard as dashboard_package
from app import services as services_package


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


class ServicesCompatibilityFacadeTests(unittest.TestCase):
    def assert_no_legacy_package_exports(self, package: object, export_names: list[str]) -> None:
        self.assertEqual(list(getattr(package, "__all__")), [])
        self.assertFalse(hasattr(package, "__getattr__"))
        for export_name in export_names:
            self.assertFalse(hasattr(package, export_name), export_name)

    def test_services_package_no_longer_reexports_legacy_helpers(self) -> None:
        self.assert_no_legacy_package_exports(services_package, SERVICE_LEGACY_EXPORTS)

    def test_dashboard_package_no_longer_reexports_legacy_helpers(self) -> None:
        self.assert_no_legacy_package_exports(dashboard_package, DASHBOARD_LEGACY_EXPORTS)

    def test_canonical_service_modules_remain_importable(self) -> None:
        fire_map_service = importlib.import_module("app.services.fire_map_service")
        pipeline_service = importlib.import_module("app.services.pipeline_service")
        table_options = importlib.import_module("app.services.table_options")

        self.assertTrue(callable(fire_map_service.build_fire_map_html))
        self.assertTrue(callable(pipeline_service.import_uploaded_data))
        self.assertTrue(callable(pipeline_service.run_profiling_for_table))
        self.assertTrue(callable(pipeline_service.save_uploaded_file))
        self.assertTrue(callable(table_options.get_column_search_table_options))
        self.assertTrue(callable(table_options.get_fire_map_table_options))
        self.assertTrue(callable(table_options.resolve_selected_table))

    def test_canonical_dashboard_service_module_remains_importable(self) -> None:
        dashboard_service = importlib.import_module("app.dashboard.service")

        self.assertTrue(callable(dashboard_service.build_dashboard_context))
        self.assertTrue(callable(dashboard_service.get_dashboard_data))
        self.assertTrue(callable(dashboard_service.get_dashboard_page_context))
        self.assertTrue(callable(dashboard_service.get_dashboard_shell_context))


if __name__ == "__main__":
    unittest.main()
