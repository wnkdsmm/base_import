from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pandas as pd

class ColumnFinder:
    """РџРѕРёСЃРє РєРѕР»РѕРЅРѕРє РІ DataFrame СЃ РєСЌС€РёСЂРѕРІР°РЅРёРµРј"""
    
    def __init__(self):
        self._cache: Dict[int, Dict[Tuple, Optional[str]]] = {}
    
    def find(self, df: pd.DataFrame, possible_names: Tuple[str, ...]) -> Optional[str]:
        """РќР°С…РѕРґРёС‚ РєРѕР»РѕРЅРєСѓ РїРѕ РІРѕР·РјРѕР¶РЅС‹Рј РёРјРµРЅР°Рј"""
        df_id = id(df)
        if df_id not in self._cache:
            self._cache[df_id] = {}
        
        cache = self._cache[df_id]
        if possible_names in cache:
            return cache[possible_names]
        
        # РџРѕРёСЃРє СЃ РїСЂРµРґРІР°СЂРёС‚РµР»СЊРЅРѕР№ РЅРѕСЂРјР°Р»РёР·Р°С†РёРµР№
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
    """РћС‡РёСЃС‚РєР° Рё РїРѕРґРіРѕС‚РѕРІРєР° РґР°РЅРЅС‹С…"""
    
    @staticmethod
    def clean_coordinates(df: pd.DataFrame, lat_col: str, lon_col: str) -> pd.DataFrame:
        """РћС‡РёС‰Р°РµС‚ РєРѕРѕСЂРґРёРЅР°С‚С‹ Рё СѓРґР°Р»СЏРµС‚ РЅРµРєРѕСЂСЂРµРєС‚РЅС‹Рµ"""
        df = df.copy()
        
        for col in [lat_col, lon_col]:
            # РљРѕРЅРІРµСЂС‚Р°С†РёСЏ СЃС‚СЂРѕРє РІ С‡РёСЃР»Р°
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(',', '.')
                .str.replace(r'[^\d.-]', '', regex=True)
            )
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Р¤РёР»СЊС‚СЂР°С†РёСЏ РІР°Р»РёРґРЅС‹С… РєРѕРѕСЂРґРёРЅР°С‚
        valid_coords = (
            df[lat_col].between(-90, 90) & 
            df[lon_col].between(-180, 180)
        )
        
        return df[valid_coords].dropna(subset=[lat_col, lon_col])
    
    @staticmethod
    def safe_get(row: pd.Series, col: Optional[str], default: Any = 0) -> Any:
        """Р‘РµР·РѕРїР°СЃРЅРѕРµ РїРѕР»СѓС‡РµРЅРёРµ Р·РЅР°С‡РµРЅРёСЏ РёР· СЃС‚СЂРѕРєРё"""
        if col and col in row.index:
            val = row[col]
            return val if pd.notna(val) else default
        return default


# =====================================================
# MARKER STYLE
# =====================================================

__all__ = ["ColumnFinder", "DataCleaner"]
