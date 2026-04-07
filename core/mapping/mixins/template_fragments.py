from __future__ import annotations

from typing import Any, Callable, Dict, List


def build_analytics_layer_geojsons(
    analytics: Dict[str, Any],
    build_popup_rows: Callable[..., List[Dict[str, str]]],
) -> Dict[str, Dict[str, Any]]:
    layers = {
        'heatmap': {'type': 'FeatureCollection', 'features': []},
        'hotspots': {'type': 'FeatureCollection', 'features': []},
        'clusters': {'type': 'FeatureCollection', 'features': []},
        'risk_zones': {'type': 'FeatureCollection', 'features': []},
        'priorities': {'type': 'FeatureCollection', 'features': []},
    }

    for point in analytics.get('heatmap', {}).get('points', []):
        layers['heatmap']['features'].append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [point['longitude'], point['latitude']]},
            'properties': {'weight': point['weight']},
        })

    for item in analytics.get('hotspots', []):
        layers['hotspots']['features'].append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [item['longitude'], item['latitude']]},
            'properties': {
                'popup_rows': build_popup_rows([
                    ('Hotspot', item.get('label', '')),
                    ('Риск', item.get('risk_score_display', '')),
                    ('Пожаров', item.get('support_count', '')),
                    ('Пояснение', item.get('explanation', '')),
                ]),
                'label': item.get('label', ''),
                'rank': item.get('rank'),
                'risk_tone': item.get('risk_tone'),
                'risk_score': item.get('risk_score'),
            },
        })

    for item in analytics.get('dbscan', {}).get('clusters', []):
        layers['clusters']['features'].append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [item['longitude'], item['latitude']]},
            'properties': {
                'popup_rows': build_popup_rows([
                    ('DBSCAN', item.get('cluster_display', '')),
                    ('Локация', item.get('label', '')),
                    ('Риск', item.get('risk_score_display', '')),
                    ('Пожаров', item.get('incident_count', '')),
                    ('Пояснение', item.get('explanation', '')),
                ]),
                'label': item.get('label', ''),
                'rank': item.get('rank'),
                'risk_tone': item.get('risk_tone'),
                'risk_score': item.get('risk_score'),
                'incident_count': item.get('incident_count'),
            },
        })

    for item in analytics.get('risk_zones', []):
        layers['risk_zones']['features'].append({
            'type': 'Feature',
            'geometry': {'type': 'Polygon', 'coordinates': [item['polygon']]},
            'properties': {
                'popup_rows': build_popup_rows(
                    [
                        ('Зона', item.get('label', '')),
                        ('Источник', item.get('source', '')),
                        ('Риск', item.get('risk_score_display', '')),
                        ('Пояснение', item.get('explanation', '')),
                    ],
                    title=item.get('priority_label', ''),
                ),
                'label': item.get('label', ''),
                'priority_label': item.get('priority_label', ''),
                'risk_tone': item.get('risk_tone'),
                'risk_score': item.get('risk_score'),
            },
        })

    for item in analytics.get('priority_territories', []):
        layers['priorities']['features'].append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [item['longitude'], item['latitude']]},
            'properties': {
                'popup_rows': build_popup_rows(
                    [
                        ('Территория', item.get('label', '')),
                        ('Риск', item.get('risk_score_display', '')),
                        ('Пожаров', item.get('incident_count_display', '')),
                        ('Travel-time', item.get('travel_time_display', '')),
                        ('Факт прибытия', item.get('avg_response_display', '')),
                        ('Удалённость до ПЧ', item.get('avg_station_distance_display', '')),
                        ('Покрытие ПЧ', item.get('fire_station_coverage_display', '')),
                        ('Сервисная зона', item.get('service_zone_label', '')),
                        ('Логистический приоритет', item.get('logistics_priority_display', '')),
                        ('Пояснение', item.get('explanation', '')),
                    ],
                    title=item.get('priority_label', ''),
                ),
                'label': item.get('label', ''),
                'rank': item.get('rank'),
                'risk_tone': item.get('risk_tone'),
                'risk_score': item.get('risk_score'),
            },
        })
    return layers


def build_analytics_panel_html(analytics: Dict[str, Any], idx: int, escape: Callable[[Any], str]) -> str:
    quality = analytics.get('quality', {})
    dbscan = analytics.get('dbscan', {})
    logistics = analytics.get('logistics', {})
    summary = analytics.get('summary', {})
    hotspot_items = ''.join(
        f"<div class='analytics-item'><strong>{escape(item.get('label', ''))}</strong><span>{escape(item.get('risk_score_display', ''))}</span><small>{escape(item.get('explanation', ''))}</small></div>"
        for item in analytics.get('hotspots', [])[:4]
    ) or "<div class='analytics-item analytics-item-empty'>Hotspot-данные пока не выделены аналитикой.</div>"
    territory_items = ''.join(
        f"<div class='analytics-item'><strong>{escape(item.get('label', ''))}</strong><span>{escape(item.get('risk_score_display', ''))}</span><small>{escape(item.get('travel_time_display', 'н/д'))} | {escape(item.get('fire_station_coverage_display', 'н/д'))} | {escape(item.get('service_zone_label', 'зона не определена'))}</small></div>"
        for item in analytics.get('priority_territories', [])[:5]
    ) or "<div class='analytics-item analytics-item-empty'>Приоритетные территории пока не определены.</div>"
    method_items = ''.join(f"<span class='analytics-chip'>{escape(item)}</span>" for item in summary.get('methods', []))
    note_items = ''.join(f"<li>{escape(item)}</li>" for item in summary.get('insights', [])) or "<li>Без дополнительных аналитических выводов.</li>"
    thesis_items = ''.join(f"<p>{escape(item)}</p>" for item in summary.get('thesis_paragraphs', []))
    fallback_message = quality.get('fallback_message')
    fallback_html = f"<div class='analytics-warning'>{escape(fallback_message)}</div>" if fallback_message else ''
    logistics_text = escape(logistics.get('summary') or logistics.get('coverage_note') or 'Логистический слой пока не рассчитан.')
    return f'''
        <div id="analytics-panel-{idx}" class="analytics-panel">
            <div class="analytics-head">
                <h5>{escape(summary.get('title', 'Пространственная аналитика пожаров'))}</h5>
                <span>{escape(summary.get('subtitle', ''))}</span>
            </div>
            <div class="analytics-grid">
                <div class="analytics-card"><small>Покрытие координат</small><strong>{escape(quality.get('coordinate_coverage_display', '0'))}</strong></div>
                <div class="analytics-card"><small>Даты для hotspot</small><strong>{escape(quality.get('date_coverage_display', 'н/д'))}</strong></div>
                <div class="analytics-card"><small>Hotspot-ов</small><strong>{escape(len(analytics.get('hotspots', [])))}</strong></div>
                <div class="analytics-card"><small>DBSCAN кластеров</small><strong>{escape(dbscan.get('cluster_count', 0))}</strong></div>
                <div class="analytics-card"><small>Travel-time</small><strong>{escape(logistics.get('average_travel_time_display', 'н/д'))}</strong></div>
                <div class="analytics-card"><small>Покрытие ПЧ</small><strong>{escape(logistics.get('fire_station_coverage_display', 'н/д'))}</strong></div>
            </div>
            {fallback_html}
            <div class="analytics-section">
                <div class="analytics-section-title">Приоритетные территории</div>
                {territory_items}
            </div>
            <div class="analytics-section">
                <div class="analytics-section-title">Hotspot detection</div>
                {hotspot_items}
            </div>
            <div class="analytics-section">
                <div class="analytics-section-title">Логистика прибытия и прикрытия</div>
                <div class="analytics-item">
                    <strong>{escape(logistics.get('service_zone_label', 'Сервисная зона не определена'))}</strong>
                    <span>{escape(logistics.get('average_travel_time_display', 'н/д'))} | {escape(logistics.get('fire_station_coverage_display', 'н/д'))}</span>
                    <small>{logistics_text}</small>
                </div>
            </div>
            <div class="analytics-section">
                <div class="analytics-section-title">Методы</div>
                <div class="analytics-chip-group">{method_items}</div>
            </div>
            <div class="analytics-section">
                <div class="analytics-section-title">Ключевые выводы</div>
                <ul class="analytics-list">{note_items}</ul>
            </div>
            <details class="analytics-details">
                <summary>Тезисы для магистерской</summary>
                <div class="analytics-thesis">{thesis_items}</div>
            </details>
        </div>
        '''


def generate_html(
    tables: List[Dict[str, Any]],
    total_categories: Dict[str, int],
    *,
    render_tab_content: Callable[..., str],
    escape: Callable[[Any], str],
) -> str:
    _ = total_categories
    single_table = len(tables) == 1
    tabs_nav: List[str] = []
    tabs_content: List[str] = []

    for idx, table in enumerate(tables):
        if not single_table:
            active_class = 'active' if idx == 0 else ''
            tabs_nav.append(
                '<li class="nav-item">'
                '<button class="nav-link %s" data-bs-toggle="tab" data-bs-target="#tab%s" type="button">%s</button>'
                '</li>' % (active_class, idx, escape(table['name']))
            )

        tabs_content.append(
            render_tab_content(
                idx,
                table,
                use_tab_wrapper=not single_table,
                active=(idx == 0),
            )
        )

    body_content = ''.join(tabs_content)
    tab_resize_script = ''
    if not single_table:
        body_content = (
            '<ul class="nav nav-tabs">%s</ul>'
            '<div class="tab-content">%s</div>'
        ) % (''.join(tabs_nav), body_content)
        tab_resize_script = """
        document.querySelectorAll('.nav-tabs button').forEach(btn => {
            btn.addEventListener('shown.bs.tab', event => {
                const idx = event.target.dataset.bsTarget?.replace('#tab', '');
                setTimeout(() => window['map' + idx]?.updateSize(), 100);
            });
        });
"""

    lines = [
        '<!DOCTYPE html>',
        '<html>',
        '<head>',
        '    <meta charset="utf-8">',
        '    <meta name="viewport" content="width=device-width, initial-scale=1">',
        '    <title>\u041a\u0430\u0440\u0442\u0430 \u043f\u043e\u0436\u0430\u0440\u043e\u0432</title>',
        '    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">',
        '    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ol@v8.2.0/ol.css">',
        '    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/js/bootstrap.bundle.min.js"></script>',
        '    <script src="https://cdn.jsdelivr.net/npm/ol@v8.2.0/dist/ol.js"></script>',
        '    <style>',
        '        html, body { height: 100%; margin: 0; padding: 0; overflow: hidden; }',
        '        body { display: flex; flex-direction: column; }',
        '        .nav-tabs { flex-shrink: 0; background: white; padding-left: 10px; }',
        '        .tab-content { flex: 1; min-height: 0; }',
        '        .tab-pane { height: 100%; position: relative; }',
        '        .tab-pane .map-container { height: 100%; }',
        '        .map-container { flex: 1; min-height: 0; position: relative; }',
        '        [id^="filter-panel-"] {',
        '            position: absolute; top: 20px; left: 20px; z-index: 1000;',
        '            background: white; padding: 15px; border-radius: 8px;',
        '            box-shadow: 0 2px 10px rgba(0,0,0,0.3);',
        '            max-width: 280px; max-height: calc(100% - 40px);',
        '            overflow-y: auto;',
        '        }',
        '        .popup { background: white; border: 1px solid #ccc; padding: 10px;',
        '                 border-radius: 4px; max-width: 320px; box-shadow: 0 2px 10px rgba(0,0,0,0.2); }',
        '        .category-item { margin-bottom: 8px; padding: 5px; border-radius: 4px; background: #f8f9fa; }',
        '        .category-item label { display: flex; align-items: center; gap: 8px; margin: 0; cursor: pointer; }',
        '        .category-item input[type="checkbox"] { margin-right: 2px; }',
        '        .category-icon { width: 24px; flex: 0 0 24px; text-align: center; }',
        '        .category-label { flex: 1; min-width: 0; }',
        '        .category-count { flex: 0 0 auto; }',
        '        .layer-group-title { font-size: 12px; font-weight: 700; text-transform: uppercase; color: #4f5b66; margin: 14px 0 8px; }',
        '        .analytics-panel {',
        '            position: absolute; top: 20px; right: 20px; z-index: 1000;',
        '            width: min(360px, calc(100% - 40px)); max-height: calc(100% - 40px); overflow-y: auto;',
        '            background: rgba(255,255,255,0.97); padding: 16px; border-radius: 10px;',
        '            box-shadow: 0 10px 24px rgba(25, 35, 45, 0.18);',
        '        }',
        '        .analytics-head h5 { margin: 0 0 4px; }',
        '        .analytics-head span { display: block; color: #5f6b76; font-size: 13px; margin-bottom: 12px; }',
        '        .analytics-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; margin-bottom: 12px; }',
        '        .analytics-card { background: #f4f6f8; border-radius: 8px; padding: 10px; }',
        '        .analytics-card small { display: block; color: #5f6b76; margin-bottom: 4px; }',
        '        .analytics-card strong { display: block; font-size: 15px; }',
        '        .analytics-section { margin-top: 14px; }',
        '        .analytics-section-title { font-size: 12px; text-transform: uppercase; font-weight: 700; color: #596470; margin-bottom: 8px; }',
        '        .analytics-item { background: #f9fafb; border: 1px solid #e2e8f0; border-radius: 8px; padding: 9px 10px; margin-bottom: 8px; }',
        '        .analytics-item strong, .analytics-item span, .analytics-item small { display: block; }',
        '        .analytics-item span { font-weight: 700; color: #b33b2e; margin: 3px 0; }',
        '        .analytics-item small { color: #5f6b76; }',
        '        .analytics-item-empty { color: #5f6b76; font-style: italic; }',
        '        .analytics-chip-group { display: flex; flex-wrap: wrap; gap: 6px; }',
        '        .analytics-chip { background: #edf2f7; color: #30404d; border-radius: 999px; padding: 4px 10px; font-size: 12px; }',
        '        .analytics-list { padding-left: 18px; margin: 0; }',
        '        .analytics-warning { background: rgba(227, 109, 78, 0.12); color: #9f2f1e; border-radius: 8px; padding: 10px 12px; font-size: 13px; margin-bottom: 12px; }',
        '        .analytics-details { margin-top: 14px; }',
        '        .analytics-details summary { cursor: pointer; font-weight: 700; }',
        '        .analytics-thesis p { margin: 10px 0 0; color: #415161; line-height: 1.45; }',
        '        @media (max-width: 960px) {',
        '            [id^="filter-panel-"] { left: 10px; top: 10px; max-width: 230px; }',
        '            .analytics-panel { right: 10px; top: 10px; width: min(250px, calc(100% - 20px)); }',
        '        }',
        '    </style>',
        '</head>',
        '<body>',
        body_content,
        '    <script>',
        '        // Resize maps after tab switches',
        tab_resize_script,
        '        window.addEventListener("resize", () => {',
        '            for (let i = 0; i < %s; i++) window["map" + i]?.updateSize();' % len(tables),
        '        });',
        '    </script>',
        '</body>',
        '</html>',
    ]
    return '\n'.join(lines)


def default_analytics_layer_flags() -> Dict[str, bool]:
    return {
        'incidents': True,
        'heatmap': False,
        'hotspots': False,
        'clusters': False,
        'risk_zones': False,
        'priorities': False,
    }


def analytics_layer_defaults(analytics: Dict[str, Any]) -> Dict[str, bool]:
    return analytics.get('layer_defaults', default_analytics_layer_flags())


def analytics_heatmap_config(analytics: Dict[str, Any]) -> Dict[str, Any]:
    heatmap = analytics.get('heatmap') or {}
    return {
        'enabled': bool(heatmap.get('enabled', False)),
        'radius': heatmap.get('radius', 20),
        'blur': heatmap.get('blur', 26),
    }


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


def analytics_layer_definitions(analytics_layers: Dict[str, Dict[str, Any]]) -> List[tuple[str, str, str, bool]]:
    return [
        ('incidents', '&#128506;', '\u0422\u043e\u0447\u043a\u0438 \u043f\u043e\u0436\u0430\u0440\u043e\u0432', True),
        ('heatmap', '&#128293;', 'KDE / heatmap', bool(analytics_layers.get('heatmap', {}).get('features'))),
        ('hotspots', '&#128205;', 'Hotspot detection', bool(analytics_layers.get('hotspots', {}).get('features'))),
        ('clusters', '&#129517;', 'DBSCAN \u043a\u043b\u0430\u0441\u0442\u0435\u0440\u044b', bool(analytics_layers.get('clusters', {}).get('features'))),
        ('risk_zones', '&#9888;', '\u0417\u043e\u043d\u044b \u0440\u0438\u0441\u043a\u0430', bool(analytics_layers.get('risk_zones', {}).get('features'))),
        ('priorities', '&#127919;', '\u041f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442\u043d\u044b\u0435 \u0442\u0435\u0440\u0440\u0438\u0442\u043e\u0440\u0438\u0438', bool(analytics_layers.get('priorities', {}).get('features'))),
    ]


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


def build_tab_outer_lines(
    idx: int,
    container_id: str,
    filter_panel_html: str,
    analytics_panel_html: str,
    use_tab_wrapper: bool,
    active: bool,
) -> List[str]:
    if use_tab_wrapper:
        outer_lines = ['<div class="tab-pane fade%s" id="tab%s">' % (' show active' if active else '', idx)]
    else:
        outer_lines = []

    outer_lines.extend([
        '<div class="map-container" id="%s">' % container_id,
        filter_panel_html,
        analytics_panel_html,
        '<div id="map%s" style="height:100%%; width:100%%;"></div>' % idx,
        '</div>',
    ])
    if use_tab_wrapper:
        outer_lines.append('</div>')
    return outer_lines


def build_map_setup_script_lines(
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
    return [
        '<script>',
        '(function() {',
        '    const map = new ol.Map({',
        '        target: "map%s",' % idx,
        '        layers: [new ol.layer.Tile({source: new ol.source.OSM()})],',
        '        view: new ol.View({',
        '            center: ol.proj.fromLonLat([%s, %s]),' % (center_lon, center_lat),
        '            zoom: %s' % initial_zoom,
        '        })',
        '    });',
        '',
        '    const styles = %s;' % styles_json,
        '    const analyticsLayersPayload = %s;' % analytics_layers_json,
        '    const analyticsLayerDefaults = %s;' % analytics_defaults_json,
        '    const heatmapConfig = %s;' % heatmap_json,
        '',
        '    function tonePalette(tone) {',
        '        const palette = {',
        '            critical: { fill: "rgba(179, 59, 46, 0.18)", stroke: "#b33b2e", point: "rgba(179, 59, 46, 0.86)" },',
        '            high: { fill: "rgba(226, 107, 66, 0.18)", stroke: "#d25830", point: "rgba(226, 107, 66, 0.82)" },',
        '            medium: { fill: "rgba(240, 176, 62, 0.16)", stroke: "#cc9628", point: "rgba(240, 176, 62, 0.82)" },',
        '            watch: { fill: "rgba(70, 132, 196, 0.15)", stroke: "#3a75aa", point: "rgba(70, 132, 196, 0.8)" },',
        '        };',
        '        return palette[tone] || palette.watch;',
        '    }',
        '',
        '    function createStyle(category) {',
        '        const s = styles[category] || styles.other;',
        '        return new ol.style.Style({',
        '            image: new ol.style.Circle({',
        '                radius: s.radius,',
        '                fill: new ol.style.Fill({color: s.color}),',
        '                stroke: new ol.style.Stroke({color: s.stroke, width: 2})',
        '            })',
        '        });',
        '    }',
        '',
        '    function readGeoJson(collection) {',
        '        return new ol.format.GeoJSON().readFeatures(collection, {',
        '            dataProjection: "EPSG:4326",',
        '            featureProjection: "EPSG:3857"',
        '        });',
        '    }',
        '',
        '    function buildPointStyle(feature, baseRadius) {',
        '        const tone = tonePalette(feature.get("risk_tone"));',
        '        const rank = feature.get("rank") ? String(feature.get("rank")) : "";',
        '        return new ol.style.Style({',
        '            image: new ol.style.Circle({',
        '                radius: baseRadius,',
        '                fill: new ol.style.Fill({color: tone.point}),',
        '                stroke: new ol.style.Stroke({color: tone.stroke, width: 2})',
        '            }),',
        '            text: rank ? new ol.style.Text({',
        '                text: rank,',
        '                fill: new ol.style.Fill({color: "#ffffff"}),',
        '                font: "bold 11px sans-serif"',
        '            }) : undefined',
        '        });',
        '    }',
        '',
        '    const features = readGeoJson(%s);' % geojson_json,
        '    const restoreMapView = () => {',
        '        const targetCenter = ol.proj.fromLonLat([%s, %s]);' % (center_lon, center_lat),
        '        const view = map.getView();',
        '        view.setCenter(targetCenter);',
        '        view.setZoom(%s);' % initial_zoom,
        '    };',
        '',
    ]


def build_map_layer_script_lines() -> List[str]:
    return [
        '    const categoryLayers = {};',
        '    ["deaths", "injured", "children", "evacuated", "other"].forEach(cat => {',
        '        const catFeatures = features.filter(feature => feature.get("category") === cat);',
        '        if (catFeatures.length) {',
        '            const layer = new ol.layer.Vector({',
        '                source: new ol.source.Vector({features: catFeatures}),',
        '                style: createStyle(cat),',
        '                visible: true',
        '            });',
        '            categoryLayers[cat] = layer;',
        '            map.addLayer(layer);',
        '        }',
        '    });',
        '',
        '    const analyticsLayers = {};',
        '    if ((analyticsLayersPayload.heatmap?.features || []).length) {',
        '        const heatmapFeatures = readGeoJson(analyticsLayersPayload.heatmap);',
        '        heatmapFeatures.forEach(feature => feature.set("weight", Number(feature.get("weight") || 0.15)));',
        '        analyticsLayers.heatmap = new ol.layer.Heatmap({',
        '            source: new ol.source.Vector({features: heatmapFeatures}),',
        '            radius: heatmapConfig.radius || 20,',
        '            blur: heatmapConfig.blur || 26,',
        '            visible: !!analyticsLayerDefaults.heatmap,',
        '            opacity: 0.8',
        '        });',
        '        map.addLayer(analyticsLayers.heatmap);',
        '    }',
        '',
        '    if ((analyticsLayersPayload.hotspots?.features || []).length) {',
        '        analyticsLayers.hotspots = new ol.layer.Vector({',
        '            source: new ol.source.Vector({features: readGeoJson(analyticsLayersPayload.hotspots)}),',
        '            visible: !!analyticsLayerDefaults.hotspots,',
        '            style: feature => buildPointStyle(feature, 10)',
        '        });',
        '        map.addLayer(analyticsLayers.hotspots);',
        '    }',
        '',
        '    if ((analyticsLayersPayload.clusters?.features || []).length) {',
        '        analyticsLayers.clusters = new ol.layer.Vector({',
        '            source: new ol.source.Vector({features: readGeoJson(analyticsLayersPayload.clusters)}),',
        '            visible: !!analyticsLayerDefaults.clusters,',
        '            style: feature => buildPointStyle(feature, 12)',
        '        });',
        '        map.addLayer(analyticsLayers.clusters);',
        '    }',
        '',
        '    if ((analyticsLayersPayload.risk_zones?.features || []).length) {',
        '        analyticsLayers.risk_zones = new ol.layer.Vector({',
        '            source: new ol.source.Vector({features: readGeoJson(analyticsLayersPayload.risk_zones)}),',
        '            visible: !!analyticsLayerDefaults.risk_zones,',
        '            style: feature => {',
        '                const tone = tonePalette(feature.get("risk_tone"));',
        '                return new ol.style.Style({',
        '                    stroke: new ol.style.Stroke({color: tone.stroke, width: 2}),',
        '                    fill: new ol.style.Fill({color: tone.fill})',
        '                });',
        '            }',
        '        });',
        '        map.addLayer(analyticsLayers.risk_zones);',
        '    }',
        '',
        '    if ((analyticsLayersPayload.priorities?.features || []).length) {',
        '        analyticsLayers.priorities = new ol.layer.Vector({',
        '            source: new ol.source.Vector({features: readGeoJson(analyticsLayersPayload.priorities)}),',
        '            visible: !!analyticsLayerDefaults.priorities,',
        '            style: feature => buildPointStyle(feature, 11)',
        '        });',
        '        map.addLayer(analyticsLayers.priorities);',
        '    }',
        '',
    ]


def build_popup_script_lines() -> List[str]:
    return [
        '    const overlay = new ol.Overlay({',
        '        element: document.createElement("div"),',
        '        positioning: "bottom-center",',
        '        autoPan: true',
        '    });',
        '    map.addOverlay(overlay);',
        '',
        '    function buildPopupElement(feature) {',
        '        const rows = feature.get("popup_rows");',
        '        if (!Array.isArray(rows) || !rows.length) {',
        '            return null;',
        '        }',
        '        const wrapper = document.createElement("div");',
        '        wrapper.className = "popup";',
        '        const content = document.createElement("div");',
        '        content.style.fontFamily = "Arial, sans-serif";',
        '        content.style.minWidth = "250px";',
        '        content.style.padding = "10px";',
        '        rows.forEach((row, rowIndex) => {',
        '            if (row && Object.prototype.hasOwnProperty.call(row, "title")) {',
        '                const titleNode = document.createElement("b");',
        '                titleNode.textContent = String(row.title ?? "");',
        '                content.appendChild(titleNode);',
        '            } else {',
        '                const labelNode = document.createElement("b");',
        '                labelNode.textContent = String(row?.label ?? "") + ":";',
        '                content.appendChild(labelNode);',
        '                content.appendChild(document.createTextNode(" " + String(row?.value ?? "")));',
        '            }',
        '            if (rowIndex < rows.length - 1) {',
        '                content.appendChild(document.createElement("br"));',
        '            }',
        '        });',
        '        wrapper.appendChild(content);',
        '        return wrapper;',
        '    }',
        '',
        '    map.on("click", event => {',
        '        const feature = map.forEachFeatureAtPixel(event.pixel, item => item);',
        '        const popupElement = feature ? buildPopupElement(feature) : null;',
        '        if (feature && popupElement) {',
        '            const geometry = feature.getGeometry();',
        '            const coordinate = geometry.getType() === "Polygon"',
        '                ? geometry.getInteriorPoint().getCoordinates()',
        '                : geometry.getCoordinates();',
        '            overlay.setPosition(coordinate);',
        '            overlay.getElement().replaceChildren(popupElement);',
        '        } else {',
        '            overlay.getElement().replaceChildren();',
        '            overlay.setPosition(undefined);',
        '        }',
        '    });',
        '',
    ]


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


def build_tab_script_lines(
    idx: int,
    table: Dict[str, Any],
    container_id: str,
    analytics_layers: Dict[str, Dict[str, Any]],
    analytics_defaults: Dict[str, bool],
    heatmap_config: Dict[str, Any],
    *,
    category_styles: Dict[str, Any],
    json_for_script: Callable[[Any], str],
) -> List[str]:
    center_lon, center_lat = table['center']
    initial_zoom = min(table.get('initial_zoom', 6) + 4, 13)
    styles_json = json_for_script({key: vars(value) for key, value in category_styles.items()})
    geojson_json = json_for_script(table['geojson'])
    analytics_layers_json = json_for_script(analytics_layers)
    analytics_defaults_json = json_for_script(analytics_defaults)
    heatmap_json = json_for_script(heatmap_config)
    return (
        build_map_setup_script_lines(
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
        + build_map_layer_script_lines()
        + build_popup_script_lines()
        + build_filter_script_lines(idx, container_id)
    )


__all__ = [
    "analytics_heatmap_config",
    "analytics_layer_defaults",
    "analytics_layer_definitions",
    "build_analytics_layer_geojsons",
    "build_analytics_panel_html",
    "build_category_filter_items",
    "build_filter_panel_html",
    "build_filter_script_lines",
    "build_layer_filter_items",
    "build_map_layer_script_lines",
    "build_map_setup_script_lines",
    "build_popup_script_lines",
    "build_tab_outer_lines",
    "build_tab_script_lines",
    "default_analytics_layer_flags",
    "generate_html",
]
