from __future__ import annotations

from typing import List

from ...types import AnalysisExportPayload, MapTablePayload

class MapCreatorExportMixin:
    def _build_analysis_export_payload(self, tables: List[MapTablePayload]) -> AnalysisExportPayload:
        return {
            'tables': [
                {
                    'table_name': table['name'],
                    'feature_count': table['feature_count'],
                    'spatial_analytics': table.get('spatial_analytics', {}),
                }
                for table in tables
            ]
        }

    def _build_analysis_markdown(self, tables: List[MapTablePayload]) -> str:
        lines = ['# РџСЂРѕСЃС‚СЂР°РЅСЃС‚РІРµРЅРЅР°СЏ Р°РЅР°Р»РёС‚РёРєР° РїРѕР¶Р°СЂРѕРІ', '']
        for table in tables:
            analytics = table.get('spatial_analytics', {})
            quality = analytics.get('quality', {})
            logistics = analytics.get('logistics', {})
            summary = analytics.get('summary', {})
            lines.append(f"## {table['name']}")
            lines.append(f"- Р РµР¶РёРј Р°РЅР°Р»РёР·Р°: {quality.get('mode_label', 'РќРµ РѕРїСЂРµРґРµР»С‘РЅ')}")
            lines.append(f"- РџРѕРєСЂС‹С‚РёРµ РєРѕРѕСЂРґРёРЅР°С‚: {quality.get('coordinate_coverage_display', '0')}")
            lines.append(f"- Р”Р°С‚С‹ РґР»СЏ hotspot: {quality.get('date_coverage_display', 'РЅ/Рґ')}")
            lines.append(f"- Hotspot-РѕРІ: {len(analytics.get('hotspots', []))}")
            lines.append(f"- DBSCAN РєР»Р°СЃС‚РµСЂРѕРІ: {analytics.get('dbscan', {}).get('cluster_count', 0)}")
            lines.append(f"- РџСЂРёРѕСЂРёС‚РµС‚РЅС‹С… С‚РµСЂСЂРёС‚РѕСЂРёР№: {len(analytics.get('priority_territories', []))}")
            lines.append(f"- РњРµС‚РѕРґС‹: {', '.join(summary.get('methods', [])) or 'РўРѕС‡РµС‡РЅС‹Р№ СЃР»РѕР№ РїРѕР¶Р°СЂРѕРІ'}")
            lines.append('')
            if summary.get('insights'):
                lines.append('### РљР»СЋС‡РµРІС‹Рµ РІС‹РІРѕРґС‹')
                for item in summary.get('insights', []):
                    lines.append(f"- {item}")
                lines.append('')
            if analytics.get('priority_territories'):
                lines.append('### РџСЂРёРѕСЂРёС‚РµС‚РЅС‹Рµ С‚РµСЂСЂРёС‚РѕСЂРёРё')
                for item in analytics.get('priority_territories', [])[:5]:
                    lines.append(
                        f"- {item['priority_label']}: {item['label']} | СЂРёСЃРє {item['risk_score_display']} | РџР§ {item['avg_station_distance_display']} | РїСЂРёР±С‹С‚РёРµ {item['avg_response_display']}"
                    )
                lines.append('')
            logistics_text = logistics.get('summary') or logistics.get('coverage_note')
            if logistics_text:
                lines.append('### Р›РѕРіРёСЃС‚РёРєР°')
                lines.append(logistics_text)
                lines.append('')
            if quality.get('fallback_message'):
                lines.append('### РћРіСЂР°РЅРёС‡РµРЅРёСЏ')
                lines.append(quality['fallback_message'])
                lines.append('')
            lines.append('### РўРµРєСЃС‚ РґР»СЏ РјР°РіРёСЃС‚РµСЂСЃРєРѕР№')
            for paragraph in summary.get('thesis_paragraphs', []):
                lines.append(paragraph)
                lines.append('')
        return '\n'.join(lines).strip() + '\n'
    # =====================================================
    # GENERATE HTML WITH FILTERS
    # =====================================================

__all__ = ["MapCreatorExportMixin"]
