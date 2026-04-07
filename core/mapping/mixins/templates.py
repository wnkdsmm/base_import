from __future__ import annotations

from typing import Any, Dict, List

from . import template_fragments as fragments


class MapCreatorTemplateMixin:
    def _build_analytics_layer_geojsons(self, analytics: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        return fragments.build_analytics_layer_geojsons(analytics, self._build_popup_rows)

    def _build_analytics_panel_html(self, analytics: Dict[str, Any], idx: int) -> str:
        return fragments.build_analytics_panel_html(analytics, idx, self._escape_html)

    def _generate_html(self, tables: List[Dict[str, Any]], total_categories: Dict[str, int]) -> str:
        return fragments.generate_html(
            tables,
            total_categories,
            render_tab_content=self._generate_tab_content,
            escape=self._escape_html,
        )

    @staticmethod
    def _default_analytics_layer_flags() -> Dict[str, bool]:
        return fragments.default_analytics_layer_flags()

    def _analytics_layer_defaults(self, analytics: Dict[str, Any]) -> Dict[str, bool]:
        return fragments.analytics_layer_defaults(analytics)

    @staticmethod
    def _analytics_heatmap_config(analytics: Dict[str, Any]) -> Dict[str, Any]:
        return fragments.analytics_heatmap_config(analytics)

    def _build_category_filter_items(self, table: Dict[str, Any]) -> List[str]:
        return fragments.build_category_filter_items(
            self.CATEGORY_STYLES,
            table,
            self._escape_html,
        )

    @staticmethod
    def _analytics_layer_definitions(analytics_layers: Dict[str, Dict[str, Any]]) -> List[tuple[str, str, str, bool]]:
        return fragments.analytics_layer_definitions(analytics_layers)

    def _build_layer_filter_items(
        self,
        analytics_layers: Dict[str, Dict[str, Any]],
        analytics_defaults: Dict[str, bool],
    ) -> List[str]:
        return fragments.build_layer_filter_items(analytics_layers, analytics_defaults)

    def _build_filter_panel_html(
        self,
        idx: int,
        table: Dict[str, Any],
        analytics_layers: Dict[str, Dict[str, Any]],
        analytics_defaults: Dict[str, bool],
    ) -> str:
        return fragments.build_filter_panel_html(
            idx,
            table,
            analytics_layers,
            analytics_defaults,
            self.CATEGORY_STYLES,
            self._escape_html,
        )

    @staticmethod
    def _build_tab_outer_lines(
        idx: int,
        container_id: str,
        filter_panel_html: str,
        analytics_panel_html: str,
        use_tab_wrapper: bool,
        active: bool,
    ) -> List[str]:
        return fragments.build_tab_outer_lines(
            idx,
            container_id,
            filter_panel_html,
            analytics_panel_html,
            use_tab_wrapper,
            active,
        )

    @staticmethod
    def _build_map_setup_script_lines(
        idx: int,
        center_lon: Any,
        center_lat: Any,
        initial_zoom: Any,
        styles_json: str,
        analytics_layers_json: str,
        analytics_defaults_json: str,
        heatmap_json: str,
        geojson_json: str,
    ) -> List[str]:
        return fragments.build_map_setup_script_lines(
            idx,
            center_lon,
            center_lat,
            initial_zoom,
            styles_json,
            analytics_layers_json,
            analytics_defaults_json,
            heatmap_json,
            geojson_json,
        )

    @staticmethod
    def _build_map_layer_script_lines() -> List[str]:
        return fragments.build_map_layer_script_lines()

    @staticmethod
    def _build_popup_script_lines() -> List[str]:
        return fragments.build_popup_script_lines()

    @staticmethod
    def _build_filter_script_lines(idx: int, container_id: str) -> List[str]:
        return fragments.build_filter_script_lines(idx, container_id)

    def _build_tab_script_lines(
        self,
        idx: int,
        table: Dict[str, Any],
        container_id: str,
        analytics_layers: Dict[str, Dict[str, Any]],
        analytics_defaults: Dict[str, bool],
        heatmap_config: Dict[str, Any],
    ) -> List[str]:
        return fragments.build_tab_script_lines(
            idx,
            table,
            container_id,
            analytics_layers,
            analytics_defaults,
            heatmap_config,
            category_styles=self.CATEGORY_STYLES,
            json_for_script=self._json_for_script,
        )

    def _generate_tab_content(
        self,
        idx: int,
        table: Dict[str, Any],
        use_tab_wrapper: bool = True,
        active: bool = True,
    ) -> str:
        analytics = table.get('spatial_analytics') or {}
        analytics_layers = self._build_analytics_layer_geojsons(analytics)
        analytics_defaults = self._analytics_layer_defaults(analytics)
        heatmap_config = self._analytics_heatmap_config(analytics)
        analytics_panel_html = self._build_analytics_panel_html(analytics, idx) if analytics else ''
        container_id = 'map-container-%s' % idx

        outer_lines = self._build_tab_outer_lines(
            idx,
            container_id,
            self._build_filter_panel_html(idx, table, analytics_layers, analytics_defaults),
            analytics_panel_html,
            use_tab_wrapper,
            active,
        )
        script_lines = self._build_tab_script_lines(
            idx,
            table,
            container_id,
            analytics_layers,
            analytics_defaults,
            heatmap_config,
        )
        return '\n'.join(outer_lines + script_lines)


__all__ = ["MapCreatorTemplateMixin"]
