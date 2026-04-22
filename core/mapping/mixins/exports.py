from __future__ import annotations

from ...types import AnalysisExportPayload, MapTablePayload

_DEFAULT_MODE_LABEL = "Не определён"
_DEFAULT_DATE_COVERAGE_DISPLAY = "н/д"
_DEFAULT_METHOD_LABEL = "Точечный слой пожаров"


class MapCreatorExportMixin:
    def _build_analysis_export_payload(self, tables: list[MapTablePayload]) -> AnalysisExportPayload:
        return {
            "tables": [
                {
                    "table_name": table["name"],
                    "feature_count": table["feature_count"],
                    "spatial_analytics": table.get("spatial_analytics", {}),
                }
                for table in tables
            ]
        }

    def _build_analysis_markdown(self, tables: list[MapTablePayload]) -> str:
        lines = ["# Пространственная аналитика пожаров", ""]
        for table in tables:
            analytics = table.get("spatial_analytics", {})
            quality = analytics.get("quality", {})
            logistics = analytics.get("logistics", {})
            summary = analytics.get("summary", {})
            mode_label = quality.get("mode_label") or _DEFAULT_MODE_LABEL
            date_coverage_display = quality.get("date_coverage_display") or _DEFAULT_DATE_COVERAGE_DISPLAY
            methods_label = ", ".join(summary.get("methods", [])) or _DEFAULT_METHOD_LABEL

            lines.append(f"## {table['name']}")
            lines.append(f"- Режим анализа: {mode_label}")
            lines.append(f"- Покрытие координат: {quality.get('coordinate_coverage_display', '0')}")
            lines.append(f"- Даты для hotspot: {date_coverage_display}")
            lines.append(f"- Hotspot-ов: {len(analytics.get('hotspots', []))}")
            lines.append(f"- DBSCAN кластеров: {analytics.get('dbscan', {}).get('cluster_count', 0)}")
            lines.append(f"- Приоритетных территорий: {len(analytics.get('priority_territories', []))}")
            lines.append(f"- Методы: {methods_label}")
            lines.append("")

            if summary.get("insights"):
                lines.append("### Ключевые выводы")
                for item in summary.get("insights", []):
                    lines.append(f"- {item}")
                lines.append("")

            if analytics.get("priority_territories"):
                lines.append("### Приоритетные территории")
                for item in analytics.get("priority_territories", [])[:5]:
                    lines.append(
                        f"- {item['priority_label']}: {item['label']} | риск {item['risk_score_display']} | "
                        f"ПЧ {item['avg_station_distance_display']} | прибытие {item['avg_response_display']}"
                    )
                lines.append("")

            logistics_text = logistics.get("summary") or logistics.get("coverage_note")
            if logistics_text:
                lines.append("### Логистика")
                lines.append(logistics_text)
                lines.append("")

            if quality.get("fallback_message"):
                lines.append("### Ограничения")
                lines.append(quality["fallback_message"])
                lines.append("")

            lines.append("### Текст для магистерской")
            for paragraph in summary.get("thesis_paragraphs", []):
                lines.append(paragraph)
                lines.append("")

        return "\n".join(lines).strip() + "\n"


__all__ = ["MapCreatorExportMixin"]
