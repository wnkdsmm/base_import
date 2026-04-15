from __future__ import annotations

from typing import Dict, List

from ...types import MapTablePayload
from .template_analytics import (
    analytics_heatmap_config,
    analytics_layer_defaults,
    build_analytics_layer_geojsons,
    build_analytics_panel_html,
)
from .template_layout import build_filter_panel_html, build_tab_outer_lines, generate_html
from .template_scripts import build_tab_script_lines


class MapCreatorTemplateMixin:
    def _generate_html(self, tables: List[MapTablePayload], total_categories: Dict[str, int]) -> str:
        return generate_html(
            tables,
            total_categories,
            render_tab_content=self._generate_tab_content,
            escape=self._escape_html,
        )

    def _generate_tab_content(
        self,
        idx: int,
        table: MapTablePayload,
        use_tab_wrapper: bool = True,
        active: bool = True,
    ) -> str:
        analytics = table.get("spatial_analytics") or {}
        analytics_layers = build_analytics_layer_geojsons(analytics, self._build_popup_rows)
        analytics_defaults = analytics_layer_defaults(analytics)
        heatmap_config = analytics_heatmap_config(analytics)
        analytics_panel_html = build_analytics_panel_html(analytics, idx, self._escape_html) if analytics else ""
        container_id = "map-container-%s" % idx

        outer_lines = build_tab_outer_lines(
            idx,
            container_id,
            build_filter_panel_html(
                idx,
                table,
                analytics_layers,
                analytics_defaults,
                self.CATEGORY_STYLES,
                self._escape_html,
            ),
            analytics_panel_html,
            use_tab_wrapper,
            active,
        )
        script_lines = build_tab_script_lines(
            idx,
            table,
            container_id,
            analytics_layers,
            analytics_defaults,
            heatmap_config,
            category_styles=self.CATEGORY_STYLES,
            json_for_script=self._json_for_script,
        )
        return "\n".join(outer_lines + script_lines)


__all__ = ["MapCreatorTemplateMixin"]
