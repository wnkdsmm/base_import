import unittest

from app import dashboard as dashboard_package
from app import services as services_package
from app.dashboard import service as dashboard_service_core
from app.services import fire_map_service, pipeline_service, table_options


class ServicesCompatibilityFacadeTests(unittest.TestCase):
    def assert_exports_match(self, facade: object, exports: dict[str, object]) -> None:
        for export_name, canonical in exports.items():
            self.assertIs(getattr(facade, export_name), canonical, export_name)

    def assert_exports_are_visible(self, facade: object, export_names: list[str]) -> None:
        self.assertEqual(list(getattr(facade, "__all__")), export_names)
        visible_names = dir(facade)
        for export_name in export_names:
            self.assertIn(export_name, visible_names)

    def test_services_package_reexports_canonical_dashboard_page_context(self) -> None:
        self.assertIs(services_package.get_dashboard_page_context, dashboard_service_core.get_dashboard_page_context)

    def test_services_package_reexports_canonical_helpers(self) -> None:
        self.assert_exports_match(
            services_package,
            {
                "build_fire_map_html": fire_map_service.build_fire_map_html,
                "get_column_search_table_options": table_options.get_column_search_table_options,
                "get_dashboard_page_context": dashboard_service_core.get_dashboard_page_context,
                "get_fire_map_table_options": table_options.get_fire_map_table_options,
                "import_uploaded_data": pipeline_service.import_uploaded_data,
                "resolve_selected_table": table_options.resolve_selected_table,
                "run_profiling_for_table": pipeline_service.run_profiling_for_table,
                "save_uploaded_file": pipeline_service.save_uploaded_file,
            },
        )

    def test_dashboard_package_reexports_canonical_dashboard_functions(self) -> None:
        self.assert_exports_match(
            dashboard_package,
            {
                "build_dashboard_context": dashboard_service_core.build_dashboard_context,
                "get_dashboard_data": dashboard_service_core.get_dashboard_data,
                "get_dashboard_page_context": dashboard_service_core.get_dashboard_page_context,
                "get_dashboard_shell_context": dashboard_service_core.get_dashboard_shell_context,
            },
        )

    def test_target_compatibility_facades_keep_all_and_dir_exports(self) -> None:
        self.assert_exports_are_visible(services_package, list(services_package.__all__))
        self.assert_exports_are_visible(dashboard_package, list(dashboard_package.__all__))


if __name__ == "__main__":
    unittest.main()
