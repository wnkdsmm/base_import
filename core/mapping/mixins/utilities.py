from __future__ import annotations

import html
import json
import math
from collections import Counter
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

class MapCreatorUtilityMixin:
    def _get_marker_category(self, row: pd.Series, columns: Dict[str, Optional[str]]) -> str:
        """Определяет категорию маркера по данным строки"""
        if self.cleaner.safe_get(row, columns.get('deaths'), 0):
            return "deaths"
        if self.cleaner.safe_get(row, columns.get('injured'), 0):
            return "injured"
        # Проверяем наличие детей (спасенных или эвакуированных)
        if (self.cleaner.safe_get(row, columns.get('children_saved'), 0) or 
            self.cleaner.safe_get(row, columns.get('children_evacuated'), 0)):
            return "children"
        if self.cleaner.safe_get(row, columns.get('evacuated'), 0):
            return "evacuated"
        return "other"

    # =====================================================
    # POPUP
    # =====================================================
    @staticmethod
    def _escape_html(value: Any) -> str:
        return html.escape("" if value is None else str(value), quote=True)

    @staticmethod
    def _json_for_script(value: Any) -> str:
        return (
            json.dumps(value, ensure_ascii=False)
            .replace("<", "\\u003c")
            .replace(">", "\\u003e")
            .replace("&", "\\u0026")
            .replace("\u2028", "\\u2028")
            .replace("\u2029", "\\u2029")
        )

    def _build_popup_rows(
        self,
        items: List[Tuple[str, Any]],
        title: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        if title:
            rows.append({"title": "" if title is None else str(title)})
        for label, value in items:
            rows.append({
                "label": "" if label is None else str(label),
                "value": "" if value is None else str(value),
            })
        return rows

    def _build_fire_popup_rows(self, data: Dict[str, str]) -> List[Dict[str, str]]:
        return self._build_popup_rows(
            [
                ("Дата", data.get("date", "")),
                ("Адрес", data.get("address", "")),
                ("Погибшие", data.get("deaths", "")),
                ("Травмированные", data.get("injured", "")),
                ("Эвакуировано", data.get("evacuated", "")),
                ("Спасено детей", data.get("children_saved", "")),
                ("Эвакуировано детей", data.get("children_evacuated", "")),
                ("Причина (общая)", data.get("fire_cause_general", "")),
                ("Причина открытой территории", data.get("fire_cause_open", "")),
                ("Причина здания", data.get("fire_cause_building", "")),
                ("Категория здания", data.get("building_category", "")),
                ("Категория объекта", data.get("object_category", "")),
                ("Общая площадь", data.get("object_area", "")),
            ]
        )

    def _calculate_initial_view(self, features: List[Dict]) -> Tuple[Tuple[float, float], int]:
        """Estimate a robust стартовый центр и зум по координатам."""
        coords = np.array([feature["geometry"]["coordinates"] for feature in features], dtype=float)
        lons = coords[:, 0]
        lats = coords[:, 1]

        if len(coords) == 1:
            return (float(lons[0]), float(lats[0])), 12

        quantile_low = 0.1 if len(coords) >= 10 else 0.0
        quantile_high = 0.9 if len(coords) >= 10 else 1.0
        lon_low, lon_high = np.quantile(lons, [quantile_low, quantile_high])
        lat_low, lat_high = np.quantile(lats, [quantile_low, quantile_high])

        core_mask = (lons >= lon_low) & (lons <= lon_high) & (lats >= lat_low) & (lats <= lat_high)
        if core_mask.any():
            core_lons = lons[core_mask]
            core_lats = lats[core_mask]
        else:
            core_lons = lons
            core_lats = lats

        center_lon = float(np.median(core_lons))
        center_lat = float(np.median(core_lats))
        lon_span = float(core_lons.max() - core_lons.min()) if len(core_lons) > 1 else 0.0
        lat_span = float(core_lats.max() - core_lats.min()) if len(core_lats) > 1 else 0.0
        span = max(lon_span, lat_span)

        if span <= 0.03:
            zoom = 12
        elif span <= 0.08:
            zoom = 11
        elif span <= 0.18:
            zoom = 10
        elif span <= 0.35:
            zoom = 9
        elif span <= 0.75:
            zoom = 8
        elif span <= 1.5:
            zoom = 7
        else:
            zoom = 6

        return (center_lon, center_lat), zoom

    def _clean_text(self, value: Any) -> str:
        return "" if value is None else str(value).strip()

    def _to_float(self, value: Any) -> Optional[float]:
        text_value = self._clean_text(value)
        if not text_value:
            return None
        try:
            return float(text_value.replace(',', '.'))
        except ValueError:
            return None

    def _parse_date(self, value: Any) -> Optional[date]:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        if isinstance(value, pd.Timestamp):
            return value.date()
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        parsed = pd.to_datetime(self._clean_text(value), errors='coerce', dayfirst=True)
        return parsed.date() if pd.notna(parsed) else None

    def _parse_datetime_like(self, value: Any, event_date: Optional[date]) -> Optional[datetime]:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        if isinstance(value, datetime):
            return value
        if isinstance(value, time):
            return datetime.combine(event_date or date(2000, 1, 1), value)

        text_value = self._clean_text(value)
        if not text_value:
            return None

        parsed = pd.to_datetime(text_value, errors='coerce', dayfirst=True)
        if pd.notna(parsed):
            parsed_ts = pd.Timestamp(parsed)
            if parsed_ts.hour or parsed_ts.minute or parsed_ts.second:
                return parsed_ts.to_pydatetime()

        for fmt in ('%H:%M', '%H:%M:%S'):
            try:
                parsed_time = datetime.strptime(text_value, fmt).time()
                return datetime.combine(event_date or date(2000, 1, 1), parsed_time)
            except ValueError:
                continue
        return None

    def _calculate_response_minutes(self, report_value: Any, arrival_value: Any, event_date: Optional[date]) -> Optional[float]:
        report_time = self._parse_datetime_like(report_value, event_date)
        arrival_time = self._parse_datetime_like(arrival_value, event_date)
        if report_time is None or arrival_time is None:
            return None
        delta_minutes = (arrival_time - report_time).total_seconds() / 60.0
        if delta_minutes < 0 and delta_minutes > -1440:
            delta_minutes += 1440
        if delta_minutes < 0 or delta_minutes > 240:
            return None
        return round(delta_minutes, 1)

    def _is_rural_label(self, *values: Any) -> bool:
        normalized = ' '.join(
            self._clean_text(value).lower().replace('ё', 'е').replace('-', ' ')
            for value in values if self._clean_text(value)
        )
        return any(token in normalized for token in ('сель', 'деревн', 'посел', 'село', 'хутор', 'станиц', 'аул', 'снт', 'днп'))

    def _dominant_label(self, records: List[Dict[str, Any]], key: str, fallback: str) -> str:
        counter = Counter(
            self._clean_text(item.get(key))
            for item in records
            if self._clean_text(item.get(key))
        )
        return counter.most_common(1)[0][0] if counter else fallback

    def _km_distance(self, left: Dict[str, Any], right: Dict[str, Any]) -> float:
        lat_factor = 110.574
        avg_lat = (float(left['latitude']) + float(right['latitude'])) / 2.0
        lon_factor = 111.320 * max(math.cos(math.radians(avg_lat)), 0.1)
        dx = (float(left['longitude']) - float(right['longitude'])) * lon_factor
        dy = (float(left['latitude']) - float(right['latitude'])) * lat_factor
        return math.hypot(dx, dy)

    def _risk_level(self, value: float) -> Tuple[str, str]:
        if value >= 80:
            return 'Критический', 'critical'
        if value >= 60:
            return 'Высокий', 'high'
        if value >= 35:
            return 'Средний', 'medium'
        return 'Наблюдение', 'watch'

    def _build_circle_polygon(self, lon: float, lat: float, radius_km: float, steps: int = 36) -> List[List[float]]:
        points: List[List[float]] = []
        lat_factor = 110.574
        lon_factor = max(111.320 * max(math.cos(math.radians(lat)), 0.1), 0.1)
        for step in range(steps):
            angle = 2.0 * math.pi * step / steps
            x_km = math.cos(angle) * radius_km
            y_km = math.sin(angle) * radius_km
            points.append([
                round(lon + x_km / lon_factor, 6),
                round(lat + y_km / lat_factor, 6),
            ])
        if points:
            points.append(points[0])
        return points

__all__ = ["MapCreatorUtilityMixin"]