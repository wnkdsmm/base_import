from __future__ import annotations

from typing import Any, Callable, Dict, List

from .template_fragment_analytics import analytics_layer_definitions


def build_category_filter_items(category_styles: Dict[str, Any], table: Dict[str, Any], escape: Callable[[Any], str]) -> List[str]:
    category_items: List[str] = []
    for cat_id, style in category_styles.items():
        count = table['counts'].get(cat_id, 0)
        category_items.append(
            ''.join([
                '<div class="category-item">',
                '    <label>',
                '        <input type="checkbox" class="category-filter" data-category="%s" checked>' % cat_id,
                '        <span class="category-icon">%s</span>' % style.icon,
                '        <span class="category-label">%s</span>' % escape(style.label),
                '        <span class="badge bg-secondary category-count">%s</span>' % count,
                '    </label>',
                '</div>',
            ])
        )
    return category_items

def build_layer_filter_items(
    analytics_layers: Dict[str, Dict[str, Any]],
    analytics_defaults: Dict[str, bool],
) -> List[str]:
    layer_items: List[str] = []
    for layer_id, icon_html, label, available in analytics_layer_definitions(analytics_layers):
        if not available:
            continue
        checked = ' checked' if analytics_defaults.get(layer_id, layer_id == 'incidents') else ''
        layer_items.append(
            ''.join([
                '<div class="category-item">',
                '    <label>',
                '        <input type="checkbox" class="layer-filter" data-layer="%s"%s>' % (layer_id, checked),
                '        <span class="category-icon">%s</span>' % icon_html,
                '        <span class="category-label">%s</span>' % label,
                '    </label>',
                '</div>',
            ])
        )
    return layer_items

def build_filter_panel_html(
    idx: int,
    table: Dict[str, Any],
    analytics_layers: Dict[str, Dict[str, Any]],
    analytics_defaults: Dict[str, bool],
    category_styles: Dict[str, Any],
    escape: Callable[[Any], str],
) -> str:
    filter_panel_lines = [
        '<div id="filter-panel-%s">' % idx,
        '    <h5 style="margin-bottom: 15px;">&#128269; \u0424\u0438\u043b\u044c\u0442\u0440\u044b \u0438 \u0441\u043b\u043e\u0438</h5>',
        '    <div style="display: flex; gap: 5px; margin-bottom: 15px;">',
        '        <button id="select-all-%s" class="btn btn-primary btn-sm" style="flex:1;">\u0412\u0441\u0435</button>' % idx,
        '        <button id="deselect-all-%s" class="btn btn-secondary btn-sm" style="flex:1;">\u0421\u0431\u0440\u043e\u0441</button>' % idx,
        '    </div>',
        '    <div class="layer-group-title">\u041a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u0438 \u043f\u043e\u0436\u0430\u0440\u043e\u0432</div>',
        ''.join(build_category_filter_items(category_styles, table, escape)),
    ]
    layer_items = build_layer_filter_items(analytics_layers, analytics_defaults)
    if layer_items:
        filter_panel_lines.extend([
            '    <div class="layer-group-title">\u0410\u043d\u0430\u043b\u0438\u0442\u0438\u0447\u0435\u0441\u043a\u0438\u0435 \u0441\u043b\u043e\u0438</div>',
            ''.join(layer_items),
        ])
    filter_panel_lines.append('</div>')
    return '\n'.join(filter_panel_lines)

def build_filter_script_lines(idx: int, container_id: str) -> List[str]:
    return [
        '    const categoryCheckboxes = document.querySelectorAll("#%s .category-filter");' % container_id,
        '    const layerCheckboxes = document.querySelectorAll("#%s .layer-filter");' % container_id,
        '',
        '    const updateCategoryLayers = () => {',
        '        const incidentsToggle = Array.from(layerCheckboxes).find(box => box.dataset.layer === "incidents");',
        '        const incidentsVisible = incidentsToggle ? incidentsToggle.checked : true;',
        '        categoryCheckboxes.forEach(box => {',
        '            const layer = categoryLayers[box.dataset.category];',
        '            if (layer) {',
        '                layer.setVisible(incidentsVisible && box.checked);',
        '            }',
        '        });',
        '    };',
        '',
        '    const updateAnalyticsLayers = () => {',
        '        layerCheckboxes.forEach(box => {',
        '            if (box.dataset.layer === "incidents") {',
        '                return;',
        '            }',
        '            const layer = analyticsLayers[box.dataset.layer];',
        '            if (layer) {',
        '                layer.setVisible(box.checked);',
        '            }',
        '        });',
        '        updateCategoryLayers();',
        '    };',
        '',
        '    categoryCheckboxes.forEach(box => box.addEventListener("change", updateCategoryLayers));',
        '    layerCheckboxes.forEach(box => box.addEventListener("change", updateAnalyticsLayers));',
        '',
        '    document.getElementById("select-all-%s").addEventListener("click", () => {' % idx,
        '        categoryCheckboxes.forEach(box => box.checked = true);',
        '        layerCheckboxes.forEach(box => box.checked = true);',
        '        updateAnalyticsLayers();',
        '    });',
        '',
        '    document.getElementById("deselect-all-%s").addEventListener("click", () => {' % idx,
        '        categoryCheckboxes.forEach(box => box.checked = false);',
        '        layerCheckboxes.forEach(box => box.checked = false);',
        '        updateAnalyticsLayers();',
        '    });',
        '',
        '    updateAnalyticsLayers();',
        '    map.addControl(new ol.control.FullScreen());',
        '    map.addControl(new ol.control.ScaleLine());',
        '',
        '    window["map%s"] = map;' % idx,
        '    setTimeout(() => {',
        '        map.updateSize();',
        '        restoreMapView();',
        '    }, 200);',
        '})();',
        '</script>',
    ]


__all__ = [
    "build_category_filter_items",
    "build_layer_filter_items",
    "build_filter_panel_html",
    "build_filter_script_lines",
]
