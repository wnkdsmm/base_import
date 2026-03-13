"""
Генератор интерактивной карты пожаров с фильтрацией по категориям
(Полная версия с фильтрами и правильным позиционированием карты)
"""

import json
import logging
from pathlib import Path
from typing import Tuple, Dict, List, Optional, Any
from dataclasses import dataclass, field
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =====================================================
# CONFIG
# =====================================================

@dataclass
class Config:
    output_dir: Path
    max_records_per_table: int = 10000

    # Имена колонок (в порядке приоритета)
    lat_names: Tuple[str, ...] = ('широта', 'latitude', 'lat')
    lon_names: Tuple[str, ...] = ('долгота', 'longitude', 'lon')
    
    date_names: Tuple[str, ...] = ('дата возникновения пожара', 'date', 'data')
    address_names: Tuple[str, ...] = ('адрес', 'address')
    
    deaths_names: Tuple[str, ...] = ('количество погибших в куп', 'deaths', 'pogibshie')
    injured_names: Tuple[str, ...] = ('количество травмированных в куп', 'injured', 'travmirovano')
    evacuated_names: Tuple[str, ...] = ('эвакуировано на пожаре', 'evacuated', 'evakuirovano')
    children_saved_names: Tuple[str, ...] = ('спасено детей', 'children_saved', 'spaseno_detey')
    children_evacuated_names: Tuple[str, ...] = ('эвакуировано детей', 'children_evacuated', 'evakuirovano_detey')
    
    fire_cause_general_names: Tuple[str, ...] = ('причина пожара (общая)', 'fire_cause_general')
    fire_cause_open_names: Tuple[str, ...] = ('причина пожара для открытой территории', 'fire_cause_open')
    fire_cause_building_names: Tuple[str, ...] = ('причина пожара для зданий (сооружений)', 'fire_cause_building')
    building_category_names: Tuple[str, ...] = ('категория здания', 'building_category')
    object_category_names: Tuple[str, ...] = ('категория объекта', 'object_category')
    object_area_names: Tuple[str, ...] = ('общая площадь объекта', 'object_area')

    def __post_init__(self):
        self.output_dir.mkdir(exist_ok=True)


# =====================================================
# COLUMN FINDER
# =====================================================

class ColumnFinder:
    """Поиск колонок в DataFrame с кэшированием"""
    
    def __init__(self):
        self._cache: Dict[int, Dict[Tuple, Optional[str]]] = {}
    
    def find(self, df: pd.DataFrame, possible_names: Tuple[str, ...]) -> Optional[str]:
        """Находит колонку по возможным именам"""
        df_id = id(df)
        if df_id not in self._cache:
            self._cache[df_id] = {}
        
        cache = self._cache[df_id]
        if possible_names in cache:
            return cache[possible_names]
        
        # Поиск с предварительной нормализацией
        df_lower = {col.lower().strip(): col for col in df.columns}
        for name in possible_names:
            if name in df_lower:
                cache[possible_names] = df_lower[name]
                return df_lower[name]
        
        cache[possible_names] = None
        return None


# =====================================================
# DATA CLEANER
# =====================================================

class DataCleaner:
    """Очистка и подготовка данных"""
    
    @staticmethod
    def clean_coordinates(df: pd.DataFrame, lat_col: str, lon_col: str) -> pd.DataFrame:
        """Очищает координаты и удаляет некорректные"""
        df = df.copy()
        
        for col in [lat_col, lon_col]:
            # Конвертация строк в числа
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(',', '.')
                .str.replace(r'[^\d.-]', '', regex=True)
            )
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Фильтрация валидных координат
        valid_coords = (
            df[lat_col].between(-90, 90) & 
            df[lon_col].between(-180, 180)
        )
        
        return df[valid_coords].dropna(subset=[lat_col, lon_col])
    
    @staticmethod
    def safe_get(row: pd.Series, col: Optional[str], default: Any = 0) -> Any:
        """Безопасное получение значения из строки"""
        if col and col in row.index:
            val = row[col]
            return val if pd.notna(val) else default
        return default


# =====================================================
# MARKER STYLE
# =====================================================

@dataclass
class MarkerStyle:
    color: str
    stroke: str
    icon: str
    label: str
    radius: int = 8


# =====================================================
# MAP CREATOR
# =====================================================

class MapCreator:
    """Создание интерактивной карты с фильтрами"""
    
    # Статические стили для категорий
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

    def __init__(self, config: Config):
        self.config = config
        self.finder = ColumnFinder()
        self.cleaner = DataCleaner()

    # =====================================================
    # CATEGORY
    # =====================================================
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
    def _create_popup_html(self, data: Dict[str, str]) -> str:
        """Создает HTML для всплывающего окна"""
        template = """
        <div style="font-family: Arial, sans-serif; min-width: 250px; padding: 10px;">
            <b>Дата:</b> {date}<br>
            <b>Адрес:</b> {address}<br>
            <b>Погибшие:</b> {deaths}<br>
            <b>Травмированные:</b> {injured}<br>
            <b>Эвакуировано:</b> {evacuated}<br>
            <b>Спасено детей:</b> {children_saved}<br>
            <b>Эвакуировано детей:</b> {children_evacuated}<br>
            <b>Причина (общая):</b> {fire_cause_general}<br>
            <b>Причина открытой территории:</b> {fire_cause_open}<br>
            <b>Причина здания:</b> {fire_cause_building}<br>
            <b>Категория здания:</b> {building_category}<br>
            <b>Категория объекта:</b> {object_category}<br>
            <b>Общая площадь:</b> {object_area}
        </div>
        """
        return template.format(**data)

    # =====================================================
    # PREPARE TABLE
    # =====================================================
    def _prepare_table_data(self, df: pd.DataFrame, table_name: str) -> Optional[Dict]:
        """Подготавливает данные одной таблицы"""
        lat_col = self.finder.find(df, self.config.lat_names)
        lon_col = self.finder.find(df, self.config.lon_names)
        
        if not lat_col or not lon_col:
            logger.warning(f"Таблица {table_name}: не найдены колонки координат")
            return None
        
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
            'object_area': self.config.object_area_names
        }
        
        columns = {
            key: self.finder.find(df, names)
            for key, names in column_names.items()
        }
        
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
                        "popup": self._create_popup_html(popup_data),
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
        
        # Расчет центра карты
        lons = [f['geometry']['coordinates'][0] for f in features]
        lats = [f['geometry']['coordinates'][1] for f in features]
        center_lon = sum(lons) / len(lons)
        center_lat = sum(lats) / len(lats)
        
        return {
            'name': table_name,
            'geojson': {
                "type": "FeatureCollection",
                "features": features
            },
            'counts': category_counts,
            'center': (center_lon, center_lat),
            'feature_count': len(features)
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
        
        logger.info(f"Карта сохранена: {output_file}")
        return output_file

    # =====================================================
    # GENERATE HTML WITH FILTERS
    # =====================================================
    def _generate_html(self, tables: List[Dict], total_categories: Dict[str, int]) -> str:
        """Генерирует полный HTML-код страницы"""
        
        # Создание вкладок
        tabs_nav = []
        tabs_content = []
        
        for idx, table in enumerate(tables):
            active_class = "active" if idx == 0 else ""
            show_class = "show active" if idx == 0 else ""
            
            tabs_nav.append(
                f'<li class="nav-item">'
                f'<button class="nav-link {active_class}" data-bs-toggle="tab" '
                f'data-bs-target="#tab{idx}" type="button">{table["name"]}</button>'
                f'</li>'
            )
            
            tabs_content.append(self._generate_tab_content(idx, table))
        
        # Финальный HTML
        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Карта пожаров</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ol@v8.2.0/ol.css">
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/ol@v8.2.0/dist/ol.js"></script>
    <style>
        html, body {{ height: 100%; margin: 0; padding: 0; overflow: hidden; }}
        body {{ display: flex; flex-direction: column; }}
        .nav-tabs {{ flex-shrink: 0; background: white; padding-left: 10px; }}
        .tab-content {{ flex: 1; min-height: 0; }}
        .tab-pane {{ height: 100%; position: relative; }}
        #filter-panel {{ 
            position: absolute; top: 20px; left: 20px; z-index: 1000;
            background: white; padding: 15px; border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.3);
            max-width: 280px; max-height: calc(100% - 40px);
            overflow-y: auto;
        }}
        .popup {{ background: white; border: 1px solid #ccc; padding: 10px; 
                 border-radius: 4px; max-width: 300px; box-shadow: 0 2px 10px rgba(0,0,0,0.2); }}
        .category-item {{ margin-bottom: 8px; padding: 5px; border-radius: 4px; background: #f8f9fa; }}
        .category-item label {{ display: flex; align-items: center; margin: 0; cursor: pointer; }}
        .category-item input[type="checkbox"] {{ margin-right: 10px; }}
        .category-item span:first-of-type {{ width: 24px; }}
        .category-item span:last-of-type {{ flex: 1; }}
        .btn-sm {{ padding: 0.25rem 0.5rem; font-size: 0.875rem; }}
    </style>
</head>
<body>
    <ul class="nav nav-tabs">{''.join(tabs_nav)}</ul>
    <div class="tab-content">{''.join(tabs_content)}</div>
    <script>
        // Обновление размера карты при переключении вкладок
        document.querySelectorAll('.nav-tabs button').forEach(btn => {{
            btn.addEventListener('shown.bs.tab', e => {{
                const idx = e.target.dataset.bsTarget?.replace('#tab', '');
                setTimeout(() => window['map' + idx]?.updateSize(), 100);
            }});
        }});
        window.addEventListener('resize', () => {{
            for (let i = 0; i < {len(tables)}; i++) window['map' + i]?.updateSize();
        }});
    </script>
</body>
</html>"""
    
    def _generate_tab_content(self, idx: int, table: Dict) -> str:
        """Генерирует содержимое вкладки с картой"""
        geojson = json.dumps(table['geojson'], ensure_ascii=False)
        center_lon, center_lat = table['center']
        styles = {k: vars(v) for k, v in self.CATEGORY_STYLES.items()}
        
        # Панель фильтров
        filter_items = []
        for cat_id, style in self.CATEGORY_STYLES.items():
            count = table['counts'].get(cat_id, 0)
            filter_items.append(f'''
                <div class="category-item">
                    <label>
                        <input type="checkbox" class="category-filter" data-category="{cat_id}" checked>
                        <span>{style.icon}</span>
                        <span>{style.label}</span>
                        <span class="badge bg-secondary">{count}</span>
                    </label>
                </div>
            ''')
        
        filter_panel = f'''
        <div id="filter-panel">
            <h5 style="margin-bottom: 15px;">🔍 Фильтры</h5>
            <div style="display: flex; gap: 5px; margin-bottom: 15px;">
                <button id="select-all-{idx}" class="btn btn-primary btn-sm" style="flex:1;">Все</button>
                <button id="deselect-all-{idx}" class="btn btn-secondary btn-sm" style="flex:1;">Сброс</button>
            </div>
            {''.join(filter_items)}
            
        </div>
        '''
        
        return f'''
        <div class="tab-pane fade show active" id="tab{idx}">
            {filter_panel}
            <div id="map{idx}" style="height:100%; width:100%;"></div>
        </div>
        <script>
            (function() {{
                const map = new ol.Map({{
                    target: 'map{idx}',
                    layers: [new ol.layer.Tile({{source: new ol.source.OSM()}})],
                    view: new ol.View({{
                        center: ol.proj.fromLonLat([{center_lon}, {center_lat}]),
                        zoom: 6
                    }})
                }});
                
                const styles = {json.dumps(styles)};
                
                function createStyle(category) {{
                    const s = styles[category] || styles['other'];
                    return new ol.style.Style({{
                        image: new ol.style.Circle({{
                            radius: s.radius,
                            fill: new ol.style.Fill({{color: s.color}}),
                            stroke: new ol.style.Stroke({{color: s.stroke, width: 2}})
                        }})
                    }});
                }}
                
                const features = new ol.format.GeoJSON().readFeatures({geojson}, {{
                    dataProjection: 'EPSG:4326',
                    featureProjection: 'EPSG:3857'
                }});
                
                const categoryLayers = {{}};
                {json.dumps(list(self.CATEGORY_STYLES.keys()))}.forEach(cat => {{
                    const catFeatures = features.filter(f => f.get('category') === cat);
                    if (catFeatures.length) {{
                        const layer = new ol.layer.Vector({{
                            source: new ol.source.Vector({{features: catFeatures}}),
                            style: createStyle(cat),
                            visible: true
                        }});
                        categoryLayers[cat] = layer;
                        map.addLayer(layer);
                    }}
                }});
                
                // Всплывающие окна
                const overlay = new ol.Overlay({{
                    element: document.createElement('div'),
                    positioning: 'bottom-center',
                    autoPan: true
                }});
                map.addOverlay(overlay);
                
                map.on('click', e => {{
                    const feature = map.forEachFeatureAtPixel(e.pixel, f => f);
                    if (feature) {{
                        overlay.setPosition(feature.getGeometry().getCoordinates());
                        overlay.getElement().innerHTML = 
                            '<div class="popup">' + feature.get('popup') + '</div>';
                    }} else {{
                        overlay.setPosition(undefined);
                    }}
                }});
                
                // Фильтры
                const checkboxes = document.querySelectorAll('#tab{idx} .category-filter');
                const updateLayers = () => {{
                    checkboxes.forEach(cb => {{
                        const layer = categoryLayers[cb.dataset.category];
                        if (layer) layer.setVisible(cb.checked);
                    }});
                }};
                
                checkboxes.forEach(cb => cb.addEventListener('change', updateLayers));
                
                document.getElementById('select-all-{idx}').addEventListener('click', () => {{
                    checkboxes.forEach(cb => cb.checked = true);
                    updateLayers();
                }});
                
                document.getElementById('deselect-all-{idx}').addEventListener('click', () => {{
                    checkboxes.forEach(cb => cb.checked = false);
                    updateLayers();
                }});
                
                map.addControl(new ol.control.FullScreen());
                map.addControl(new ol.control.ScaleLine());
                
                window['map{idx}'] = map;
                setTimeout(() => map.updateSize(), 200);
            }})();
        </script>
        '''