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
    """РЎРѕР·РґР°РЅРёРµ РёРЅС‚РµСЂР°РєС‚РёРІРЅРѕР№ РєР°СЂС‚С‹ СЃ С„РёР»СЊС‚СЂР°РјРё"""

    CATEGORY_STYLES = {
        "deaths": MarkerStyle(
            color="rgba(255, 0, 0, 0.8)",
            stroke="darkred",
            icon="рџ”ґ",
            label="Р•СЃС‚СЊ РїРѕРіРёР±С€РёРµ",
            radius=8
        ),
        "injured": MarkerStyle(
            color="rgba(255, 165, 0, 0.8)",
            stroke="orange",
            icon="рџџ ",
            label="Р•СЃС‚СЊ С‚СЂР°РІРјРёСЂРѕРІР°РЅРЅС‹Рµ",
            radius=8
        ),
        "children": MarkerStyle(
            color="rgba(173, 216, 230, 0.8)",
            stroke="blue",
            icon="рџ”µ",
            label="Р•СЃС‚СЊ РґРµС‚Рё",
            radius=8
        ),
        "evacuated": MarkerStyle(
            color="rgba(0, 255, 0, 0.6)",
            stroke="green",
            icon="рџџў",
            label="Р•СЃС‚СЊ СЌРІР°РєСѓРёСЂРѕРІР°РЅРЅС‹Рµ",
            radius=8
        ),
        "other": MarkerStyle(
            color="rgba(128, 128, 128, 0.5)",
            stroke="gray",
            icon="вљЄ",
            label="Р”СЂСѓРіРёРµ РїРѕР¶Р°СЂС‹",
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
        """РџРѕРґРіРѕС‚Р°РІР»РёРІР°РµС‚ РґР°РЅРЅС‹Рµ РѕРґРЅРѕР№ С‚Р°Р±Р»РёС†С‹"""
        lat_col = self.finder.find(df, self.config.lat_names)
        lon_col = self.finder.find(df, self.config.lon_names)
        
        if not lat_col or not lon_col:
            logger.warning(f"РўР°Р±Р»РёС†Р° {table_name}: РЅРµ РЅР°Р№РґРµРЅС‹ РєРѕР»РѕРЅРєРё РєРѕРѕСЂРґРёРЅР°С‚")
            return None
        
        source_record_count = len(df)
        df = self.cleaner.clean_coordinates(df, lat_col, lon_col)
        
        if len(df) > self.config.max_records_per_table:
            logger.info(f"РўР°Р±Р»РёС†Р° {table_name}: РѕРіСЂР°РЅРёС‡РµРЅРёРµ {self.config.max_records_per_table} Р·Р°РїРёСЃРµР№")
            df = df.head(self.config.max_records_per_table)

        # РџРѕРёСЃРє РІСЃРµС… РЅРµРѕР±С…РѕРґРёРјС‹С… РєРѕР»РѕРЅРѕРє
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
        category_counts = {cat: 0 for cat in self.CATEGORY_STYLES}
        latitudes = pd.to_numeric(df[lat_col], errors="coerce")
        longitudes = pd.to_numeric(df[lon_col], errors="coerce")
        valid_mask = latitudes.notna() & longitudes.notna()
        if not valid_mask.any():
            logger.warning(f"Таблица {table_name}: нет валидных точек")
            return None

        feature_frame = df.loc[valid_mask].copy()
        latitudes = latitudes.loc[valid_mask].astype(float)
        longitudes = longitudes.loc[valid_mask].astype(float)

        def _popup_series(column_name: Optional[str]) -> pd.Series:
            if column_name and column_name in feature_frame.columns:
                return feature_frame[column_name].astype("string").fillna("").astype(object)
            return pd.Series([""] * len(feature_frame), index=feature_frame.index, dtype=object)

        popup_frame = pd.DataFrame(
            {key: _popup_series(column_name) for key, column_name in columns.items()},
            index=feature_frame.index,
        )

        def _numeric_or_zero(column_name: Optional[str]) -> pd.Series:
            if column_name and column_name in feature_frame.columns:
                return pd.to_numeric(feature_frame[column_name], errors="coerce").fillna(0.0)
            return pd.Series(0.0, index=feature_frame.index)

        deaths_values = _numeric_or_zero(columns.get("deaths"))
        injured_values = _numeric_or_zero(columns.get("injured"))
        children_saved_values = _numeric_or_zero(columns.get("children_saved"))
        children_evacuated_values = _numeric_or_zero(columns.get("children_evacuated"))
        evacuated_values = _numeric_or_zero(columns.get("evacuated"))
        children_mask = children_saved_values.ne(0.0) | children_evacuated_values.ne(0.0)

        categories = pd.Series("other", index=feature_frame.index, dtype=object)
        categories = categories.mask(evacuated_values.ne(0.0), "evacuated")
        categories = categories.mask(children_mask, "children")
        categories = categories.mask(injured_values.ne(0.0), "injured")
        categories = categories.mask(deaths_values.ne(0.0), "deaths")

        for category_name, count in categories.value_counts().to_dict().items():
            category_counts[str(category_name)] = int(count)

        popup_records = popup_frame.to_dict(orient="records")
        features = [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(lon), float(lat)],
                },
                "properties": {
                    "popup_rows": self._build_fire_popup_rows(popup_data),
                    "category": str(category),
                    **popup_data,
                },
            }
            for lon, lat, popup_data, category in zip(
                longitudes.tolist(),
                latitudes.tolist(),
                popup_records,
                categories.tolist(),
            )
        ]

        if not features:
            logger.warning(f"РўР°Р±Р»РёС†Р° {table_name}: РЅРµС‚ РІР°Р»РёРґРЅС‹С… С‚РѕС‡РµРє")
            return None
        
        # Р Р°СЃС‡РµС‚ СЃС‚Р°СЂС‚РѕРІРѕРіРѕ РІРёРґР° РєР°СЂС‚С‹
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
        """РЎРѕР·РґР°РµС‚ HTML-РєР°СЂС‚Сѓ СЃ РІРєР»Р°РґРєР°РјРё РґР»СЏ РІСЃРµС… С‚Р°Р±Р»РёС†"""
        
        # РџРѕРґРіРѕС‚РѕРІРєР° РґР°РЅРЅС‹С… РґР»СЏ РІСЃРµС… С‚Р°Р±Р»РёС†
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
            logger.error("РќРµС‚ РґР°РЅРЅС‹С… РґР»СЏ РѕС‚РѕР±СЂР°Р¶РµРЅРёСЏ")
            return None
        
        # Р“РµРЅРµСЂР°С†РёСЏ HTML
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
        
        logger.info(f"РљР°СЂС‚Р° СЃРѕС…СЂР°РЅРµРЅР°: {output_file}")
        logger.info(f"Spatial analytics JSON: {analysis_json_file}")
        logger.info(f"Spatial analytics Markdown: {analysis_md_file}")
        return output_file

__all__ = ["MapCreator"]
