from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from .config import Config, MarkerStyle
from .data_access import ColumnFinder, DataCleaner
from .mixins.analytics import MapCreatorAnalyticsMixin
from .mixins.exports import MapCreatorExportMixin
from .mixins.templates import MapCreatorTemplateMixin
from .mixins.utilities import MapCreatorUtilityMixin

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MapCreator(MapCreatorUtilityMixin, MapCreatorAnalyticsMixin, MapCreatorTemplateMixin, MapCreatorExportMixin):
    """Создание интерактивной карты с фильтрами"""

    CATEGORY_STYLES = {
        "deaths": MarkerStyle(
            color="rgba(255, 0, 0, 0.8)",
            stroke="darkred",
            icon="🔴",
            label="Есть погибшие",
            radius=8
        ),
        "injured": MarkerStyle(
            color="rgba(255, 165, 0, 0.8)",
            stroke="orange",
            icon="🟠",
            label="Есть травмированные",
            radius=8
        ),
        "children": MarkerStyle(
            color="rgba(173, 216, 230, 0.8)",
            stroke="blue",
            icon="🔵",
            label="Есть дети",
            radius=8
        ),
        "evacuated": MarkerStyle(
            color="rgba(0, 255, 0, 0.6)",
            stroke="green",
            icon="🟢",
            label="Есть эвакуированные",
            radius=8
        ),
        "other": MarkerStyle(
            color="rgba(128, 128, 128, 0.5)",
            stroke="gray",
            icon="⚪",
            label="Другие пожары",
            radius=6
        )
    }

    def __init__(
        self,
        config: Config,
        finder: Optional[ColumnFinder] = None,
        cleaner: Optional[DataCleaner] = None,
    ) -> None:
        self.config = config
        self.finder = finder or ColumnFinder()
        self.cleaner = cleaner or DataCleaner()

    def _prepare_table_data(self, df: pd.DataFrame, table_name: str) -> Optional[Dict]:
        """Подготавливает данные одной таблицы"""
        lat_col = self.finder.find(df, self.config.lat_names)
        lon_col = self.finder.find(df, self.config.lon_names)
        
        if not lat_col or not lon_col:
            logger.warning(f"Таблица {table_name}: не найдены колонки координат")
            return None
        
        source_record_count = len(df)
        df = self.cleaner.clean_coordinates(df, lat_col, lon_col)
        
        if len(df) > self.config.max_records_per_table:
            logger.info(f"Таблица {table_name}: ограничение {self.config.max_records_per_table} записей")
            df = df.head(self.config.max_records_per_table)
        
        # Поиск всех необходимых колонок
        column_names = {
            'date': self.config.date_names,
            'address': self.config.address_names,
            'deaths': self.config.deaths_names,
            'injured': self.config.injured_names,
            'evacuated': self.config.evacuated_names,
            'children_saved': self.config.children_saved_names,
            'children_evacuated': self.config.children_evacuated_names,
            'fire_cause_general': self.config.fire_cause_general_names,
            'fire_cause_open': self.config.fire_cause_open_names,
            'fire_cause_building': self.config.fire_cause_building_names,
            'building_category': self.config.building_category_names,
            'object_category': self.config.object_category_names,
            'object_area': self.config.object_area_names,
            'district': self.config.district_names,
            'territory_label': self.config.territory_names,
            'settlement_type': self.config.settlement_type_names,
            'fire_station_distance': self.config.fire_station_distance_names,
            'report_time': self.config.report_time_names,
            'arrival_time': self.config.arrival_time_names
        }
        
        columns = {
            key: self.finder.find(df, names)
            for key, names in column_names.items()
        }

        spatial_records = self._collect_spatial_records(df, lat_col, lon_col, columns)
        spatial_analytics = self._build_spatial_analytics(table_name, spatial_records, source_record_count)
        
        # Создание GeoJSON
        features = []
        category_counts = {cat: 0 for cat in self.CATEGORY_STYLES}
        
        for _, row in df.iterrows():
            try:
                lat = float(row[lat_col])
                lon = float(row[lon_col])
                
                popup_data = {
                    key: str(self.cleaner.safe_get(row, col, ''))
                    for key, col in columns.items()
                }
                
                category = self._get_marker_category(row, columns)
                category_counts[category] += 1
                
                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [lon, lat]
                    },
                    "properties": {
                        "popup_rows": self._build_fire_popup_rows(popup_data),
                        "category": category,
                        **popup_data
                    }
                })
            except Exception as e:
                logger.debug(f"Ошибка обработки строки: {e}")
                continue
        
        if not features:
            logger.warning(f"Таблица {table_name}: нет валидных точек")
            return None
        
        # Расчет стартового вида карты
        center, initial_zoom = self._calculate_initial_view(features)

        return {
            'name': table_name,
            'geojson': {
                "type": "FeatureCollection",
                "features": features
            },
            'counts': category_counts,
            'center': center,
            'initial_zoom': initial_zoom,
            'feature_count': len(features),
            'spatial_analytics': spatial_analytics
        }
    # =====================================================
    # CREATE MAP
    # =====================================================
    def create_map(self, tables_data: Dict[str, pd.DataFrame]) -> Optional[Path]:
        """Создает HTML-карту с вкладками для всех таблиц"""
        
        # Подготовка данных для всех таблиц
        prepared_tables = []
        total_categories = {cat: 0 for cat in self.CATEGORY_STYLES}
        
        for table_name, df in tables_data.items():
            if df.empty:
                continue
                
            table_data = self._prepare_table_data(df, table_name)
            if table_data and table_data['feature_count'] > 0:
                prepared_tables.append(table_data)
                for cat, count in table_data['counts'].items():
                    total_categories[cat] += count
        
        if not prepared_tables:
            logger.error("Нет данных для отображения")
            return None
        
        # Генерация HTML
        html_content = self._generate_html(prepared_tables, total_categories)
        
        output_file = self.config.output_dir / "fires_map.html"
        output_file.write_text(html_content, encoding="utf-8")

        analysis_json_file = self.config.output_dir / "fires_map_analysis.json"
        analysis_json_file.write_text(
            json.dumps(self._build_analysis_export_payload(prepared_tables), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        analysis_md_file = self.config.output_dir / "fires_map_analysis.md"
        analysis_md_file.write_text(self._build_analysis_markdown(prepared_tables), encoding="utf-8")
        
        logger.info(f"Карта сохранена: {output_file}")
        logger.info(f"Spatial analytics JSON: {analysis_json_file}")
        logger.info(f"Spatial analytics Markdown: {analysis_md_file}")
        return output_file

__all__ = ["MapCreator"]