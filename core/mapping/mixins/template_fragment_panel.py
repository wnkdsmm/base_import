from __future__ import annotations

from typing import Any, Callable, Dict


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


__all__ = [
    "build_analytics_panel_html",
]
