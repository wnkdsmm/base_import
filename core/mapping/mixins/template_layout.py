from __future__ import annotations

from typing import Any, Callable, Dict, List

from ...types import AnalyticsLayersPayload, CategoryStyleLike, MapTablePayload, SpatialLayerDefaults
from .template_analytics import analytics_layer_definitions


PAGE_HEAD_LINES = [
    "<!DOCTYPE html>",
    "<html>",
    "<head>",
    '    <meta charset="utf-8">',
    '    <meta name="viewport" content="width=device-width, initial-scale=1">',
    "    <title>\u041a\u0430\u0440\u0442\u0430 \u043f\u043e\u0436\u0430\u0440\u043e\u0432</title>",
    '    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">',
    '    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ol@v8.2.0/ol.css">',
    '    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/js/bootstrap.bundle.min.js"></script>',
    '    <script src="https://cdn.jsdelivr.net/npm/ol@v8.2.0/dist/ol.js"></script>',
    "    <style>",
]

PAGE_STYLE_LINES = [
    "        html, body { height: 100%; margin: 0; padding: 0; overflow: hidden; }",
    "        body { display: flex; flex-direction: column; }",
    "        .nav-tabs { flex-shrink: 0; background: white; padding-left: 10px; }",
    "        .tab-content { flex: 1; min-height: 0; }",
    "        .tab-pane { height: 100%; position: relative; }",
    "        .tab-pane .map-container { height: 100%; }",
    "        .map-container { flex: 1; min-height: 0; position: relative; }",
    '        [id^="filter-panel-"] {',
    "            position: absolute; top: 20px; left: 20px; z-index: 1000;",
    "            background: white; padding: 15px; border-radius: 8px;",
    "            box-shadow: 0 2px 10px rgba(0,0,0,0.3);",
    "            max-width: 280px; max-height: calc(100% - 40px);",
    "            overflow-y: auto;",
    "        }",
    "        .popup { background: white; border: 1px solid #ccc; padding: 10px;",
    "                 border-radius: 4px; max-width: 320px; box-shadow: 0 2px 10px rgba(0,0,0,0.2); }",
    "        .category-item { margin-bottom: 8px; padding: 5px; border-radius: 4px; background: #f8f9fa; }",
    "        .category-item label { display: flex; align-items: center; gap: 8px; margin: 0; cursor: pointer; }",
    '        .category-item input[type="checkbox"] { margin-right: 2px; }',
    "        .category-icon { width: 24px; flex: 0 0 24px; text-align: center; }",
    "        .category-label { flex: 1; min-width: 0; }",
    "        .category-count { flex: 0 0 auto; }",
    "        .layer-group-title { font-size: 12px; font-weight: 700; text-transform: uppercase; color: #4f5b66; margin: 14px 0 8px; }",
    "        .analytics-panel {",
    "            position: absolute; top: 20px; right: 20px; z-index: 1000;",
    "            width: min(360px, calc(100% - 40px)); max-height: calc(100% - 40px); overflow-y: auto;",
    "            background: rgba(255,255,255,0.97); padding: 16px; border-radius: 10px;",
    "            box-shadow: 0 10px 24px rgba(25, 35, 45, 0.18);",
    "        }",
    "        .analytics-head h5 { margin: 0 0 4px; }",
    "        .analytics-head span { display: block; color: #5f6b76; font-size: 13px; margin-bottom: 12px; }",
    "        .analytics-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; margin-bottom: 12px; }",
    "        .analytics-card { background: #f4f6f8; border-radius: 8px; padding: 10px; }",
    "        .analytics-card small { display: block; color: #5f6b76; margin-bottom: 4px; }",
    "        .analytics-card strong { display: block; font-size: 15px; }",
    "        .analytics-section { margin-top: 14px; }",
    "        .analytics-section-title { font-size: 12px; text-transform: uppercase; font-weight: 700; color: #596470; margin-bottom: 8px; }",
    "        .analytics-item { background: #f9fafb; border: 1px solid #e2e8f0; border-radius: 8px; padding: 9px 10px; margin-bottom: 8px; }",
    "        .analytics-item strong, .analytics-item span, .analytics-item small { display: block; }",
    "        .analytics-item span { font-weight: 700; color: #b33b2e; margin: 3px 0; }",
    "        .analytics-item small { color: #5f6b76; }",
    "        .analytics-item-empty { color: #5f6b76; font-style: italic; }",
    "        .analytics-chip-group { display: flex; flex-wrap: wrap; gap: 6px; }",
    "        .analytics-chip { background: #edf2f7; color: #30404d; border-radius: 999px; padding: 4px 10px; font-size: 12px; }",
    "        .analytics-list { padding-left: 18px; margin: 0; }",
    "        .analytics-warning { background: rgba(227, 109, 78, 0.12); color: #9f2f1e; border-radius: 8px; padding: 10px 12px; font-size: 13px; margin-bottom: 12px; }",
    "        .analytics-details { margin-top: 14px; }",
    "        .analytics-details summary { cursor: pointer; font-weight: 700; }",
    "        .analytics-thesis p { margin: 10px 0 0; color: #415161; line-height: 1.45; }",
    "        @media (max-width: 960px) {",
    '            [id^="filter-panel-"] { left: 10px; top: 10px; max-width: 230px; }',
    "            .analytics-panel { right: 10px; top: 10px; width: min(250px, calc(100% - 20px)); }",
    "        }",
    "    </style>",
    "</head>",
    "<body>",
]


def build_category_filter_items(category_styles: Dict[str, CategoryStyleLike], table: MapTablePayload, escape: Callable[[Any], str]) -> List[str]:
    category_items: List[str] = []
    for category_id, style in category_styles.items():
        count = table["counts"].get(category_id, 0)
        category_items.append(
            "".join(
                [
                    '<div class="category-item">',
                    "    <label>",
                    '        <input type="checkbox" class="category-filter" data-category="%s" checked>' % category_id,
                    '        <span class="category-icon">%s</span>' % style.icon,
                    '        <span class="category-label">%s</span>' % escape(style.label),
                    '        <span class="badge bg-secondary category-count">%s</span>' % count,
                    "    </label>",
                    "</div>",
                ]
            )
        )
    return category_items


def build_layer_filter_items(
    analytics_layers: AnalyticsLayersPayload,
    analytics_defaults: SpatialLayerDefaults,
) -> List[str]:
    layer_items: List[str] = []
    for layer_id, icon_html, label, available in analytics_layer_definitions(analytics_layers):
        if not available:
            continue
        checked = " checked" if analytics_defaults.get(layer_id, layer_id == "incidents") else ""
        layer_items.append(
            "".join(
                [
                    '<div class="category-item">',
                    "    <label>",
                    '        <input type="checkbox" class="layer-filter" data-layer="%s"%s>' % (layer_id, checked),
                    '        <span class="category-icon">%s</span>' % icon_html,
                    '        <span class="category-label">%s</span>' % label,
                    "    </label>",
                    "</div>",
                ]
            )
        )
    return layer_items


def build_filter_panel_html(
    idx: int,
    table: MapTablePayload,
    analytics_layers: AnalyticsLayersPayload,
    analytics_defaults: SpatialLayerDefaults,
    category_styles: Dict[str, CategoryStyleLike],
    escape: Callable[[Any], str],
) -> str:
    filter_panel_lines = [
        '<div id="filter-panel-%s">' % idx,
        "    <h5 style=\"margin-bottom: 15px;\">&#128269; \u0424\u0438\u043b\u044c\u0442\u0440\u044b \u0438 \u0441\u043b\u043e\u0438</h5>",
        '    <div style="display: flex; gap: 5px; margin-bottom: 15px;">',
        '        <button id="select-all-%s" class="btn btn-primary btn-sm" style="flex:1;">\u0412\u0441\u0435</button>' % idx,
        '        <button id="deselect-all-%s" class="btn btn-secondary btn-sm" style="flex:1;">\u0421\u0431\u0440\u043e\u0441</button>' % idx,
        "    </div>",
        '    <div class="layer-group-title">\u041a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u0438 \u043f\u043e\u0436\u0430\u0440\u043e\u0432</div>',
        "".join(build_category_filter_items(category_styles, table, escape)),
    ]
    layer_items = build_layer_filter_items(analytics_layers, analytics_defaults)
    if layer_items:
        filter_panel_lines.extend(
            [
                '    <div class="layer-group-title">\u0410\u043d\u0430\u043b\u0438\u0442\u0438\u0447\u0435\u0441\u043a\u0438\u0435 \u0441\u043b\u043e\u0438</div>',
                "".join(layer_items),
            ]
        )
    filter_panel_lines.append("</div>")
    return "\n".join(filter_panel_lines)


def _render_tabs(
    tables: List[MapTablePayload],
    *,
    render_tab_content: Callable[..., str],
    escape: Callable[[Any], str],
) -> tuple[str, str]:
    single_table = len(tables) == 1
    tabs_nav: List[str] = []
    tabs_content: List[str] = []

    for idx, table in enumerate(tables):
        if not single_table:
            active_class = "active" if idx == 0 else ""
            tabs_nav.append(
                '<li class="nav-item"><button class="nav-link %s" data-bs-toggle="tab" data-bs-target="#tab%s" type="button">%s</button></li>'
                % (active_class, idx, escape(table["name"]))
            )
        tabs_content.append(
            render_tab_content(
                idx,
                table,
                use_tab_wrapper=not single_table,
                active=(idx == 0),
            )
        )

    if single_table:
        return "".join(tabs_content), ""

    return (
        '<ul class="nav nav-tabs">%s</ul><div class="tab-content">%s</div>' % ("".join(tabs_nav), "".join(tabs_content)),
        """
        document.querySelectorAll('.nav-tabs button').forEach(btn => {
            btn.addEventListener('shown.bs.tab', event => {
                const idx = event.target.dataset.bsTarget?.replace('#tab', '');
                setTimeout(() => window['map' + idx]?.updateSize(), 100);
            });
        });
""",
    )


def _body_script_lines(table_count: int, tab_resize_script: str) -> List[str]:
    return [
        "    <script>",
        "        // Resize maps after tab switches",
        tab_resize_script,
        '        window.addEventListener("resize", () => {',
        '            for (let i = 0; i < %s; i++) window["map" + i]?.updateSize();' % table_count,
        "        });",
        "    </script>",
        "</body>",
        "</html>",
    ]


def generate_html(
    tables: List[MapTablePayload],
    total_categories: Dict[str, int],
    *,
    render_tab_content: Callable[..., str],
    escape: Callable[[Any], str],
) -> str:
    _ = total_categories
    body_content, tab_resize_script = _render_tabs(
        tables,
        render_tab_content=render_tab_content,
        escape=escape,
    )
    lines = PAGE_HEAD_LINES + PAGE_STYLE_LINES + [body_content] + _body_script_lines(len(tables), tab_resize_script)
    return "\n".join(lines)


def build_tab_outer_lines(
    idx: int,
    container_id: str,
    filter_panel_html: str,
    analytics_panel_html: str,
    use_tab_wrapper: bool,
    active: bool,
) -> List[str]:
    outer_lines = ['<div class="tab-pane fade%s" id="tab%s">' % (" show active" if active else "", idx)] if use_tab_wrapper else []
    outer_lines.extend(
        [
            '<div class="map-container" id="%s">' % container_id,
            filter_panel_html,
            analytics_panel_html,
            '<div id="map%s" style="height:100%%; width:100%%;"></div>' % idx,
            "</div>",
        ]
    )
    if use_tab_wrapper:
        outer_lines.append("</div>")
    return outer_lines


__all__ = [
    "build_category_filter_items",
    "build_filter_panel_html",
    "build_layer_filter_items",
    "build_tab_outer_lines",
    "generate_html",
]
