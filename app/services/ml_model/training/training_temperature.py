from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from app.services.forecasting.data import _temperature_quality_from_daily_history
from app.services.forecasting.utils import _format_percent

from ..ml_model_types import FEATURE_COLUMNS, MIN_TEMPERATURE_COVERAGE, MIN_TEMPERATURE_NON_NULL_DAYS, NON_TEMPERATURE_FEATURE_COLUMNS
from .training_dataset import _prepare_reference_frame


def _temperature_source_series(frame: pd.DataFrame) -> pd.Series:
    if 'avg_temperature' in frame.columns:
        return pd.to_numeric(frame['avg_temperature'], errors='coerce')
    if 'temp_value' in frame.columns:
        return pd.to_numeric(frame['temp_value'], errors='coerce')
    return pd.Series(np.nan, index=frame.index, dtype=float)


def _temperature_quality_summary(
    frame: pd.DataFrame,
    *,
    frame_is_prepared: bool = False,
) -> Dict[str, object]:
    reference = frame if frame_is_prepared else _prepare_reference_frame(frame)
    temperature_source = _temperature_source_series(reference)
    history_rows = [
        {
            'avg_temperature': None if pd.isna(value) else float(value),
        }
        for value in temperature_source.tolist()
    ]
    return _temperature_quality_from_daily_history(history_rows)


def _temperature_quality_note(temperature_stats: Dict[str, object]) -> str:
    non_null_days = int(temperature_stats.get('non_null_days', 0) or 0)
    total_days = int(temperature_stats.get('total_days', 0) or 0)
    coverage = float(temperature_stats.get('coverage', 0.0) or 0.0)
    coverage_display = _format_percent(coverage * 100.0)
    if temperature_stats.get('usable'):
        return (
            f'РўРµРјРїРµСЂР°С‚СѓСЂРЅС‹С… РґРЅРµР№ СЃ РЅРµРїСѓСЃС‚С‹Рј Р·РЅР°С‡РµРЅРёРµРј: {non_null_days} РёР· {total_days} '
            f'({coverage_display}); С‚РµРјРїРµСЂР°С‚СѓСЂРЅС‹Р№ РїСЂРёР·РЅР°Рє РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РІ ML.'
        )
    return (
        f'РўРµРјРїРµСЂР°С‚СѓСЂРЅС‹С… РґРЅРµР№ СЃ РЅРµРїСѓСЃС‚С‹Рј Р·РЅР°С‡РµРЅРёРµРј: {non_null_days} РёР· {total_days} '
        f'({coverage_display}); СЌС‚Рѕ РЅРёР¶Рµ РїРѕСЂРѕРіР° {MIN_TEMPERATURE_NON_NULL_DAYS} РґРЅРµР№ Рё {int(MIN_TEMPERATURE_COVERAGE * 100)}% РїРѕРєСЂС‹С‚РёСЏ, '
        'РїРѕСЌС‚РѕРјСѓ С‚РµРјРїРµСЂР°С‚СѓСЂРЅС‹Р№ РїСЂРёР·РЅР°Рє РёСЃРєР»СЋС‡С‘РЅ РёР· ML Рё РёСЃС‚РѕСЂРёС‡РµСЃРєРёС… С‚РµРјРїРµСЂР°С‚СѓСЂРЅС‹С… fallback-СЃС‚Р°С‚РёСЃС‚РёРє.'
    )


def _temperature_feature_columns(temperature_stats: Optional[Dict[str, object]]) -> List[str]:
    if temperature_stats is not None and not bool(temperature_stats.get('usable', True)):
        return NON_TEMPERATURE_FEATURE_COLUMNS
    return FEATURE_COLUMNS


def _fit_temperature_statistics(
    frame: pd.DataFrame,
    *,
    frame_is_prepared: bool = False,
) -> Dict[str, object]:
    reference = frame if frame_is_prepared else _prepare_reference_frame(frame)
    quality = _temperature_quality_summary(reference, frame_is_prepared=True)
    if reference.empty or not quality['usable']:
        return {
            'monthly': {},
            'overall': 0.0,
            **quality,
            'note': _temperature_quality_note(quality),
        }

    temperature_source = _temperature_source_series(reference)
    monthly = {
        int(month): float(value)
        for month, value in temperature_source.groupby(reference['date'].dt.month).mean().dropna().items()
    }
    overall = float(temperature_source.mean()) if temperature_source.notna().any() else 0.0
    return {
        'monthly': monthly,
        'overall': overall,
        **quality,
        'note': _temperature_quality_note(quality),
    }


def _apply_temperature_statistics(
    frame: pd.DataFrame,
    temperature_stats: Dict[str, object],
    *,
    frame_is_prepared: bool = False,
) -> pd.DataFrame:
    result = frame.copy() if frame_is_prepared else _prepare_reference_frame(frame)
    temperature_source = _temperature_source_series(result)
    if not bool(temperature_stats.get('usable', True)):
        result['avg_temperature'] = np.nan
        result['temp_value'] = 0.0
        return result

    monthly_fill = result['date'].dt.month.map(temperature_stats.get('monthly', {}))
    overall_temperature = float(temperature_stats.get('overall', 0.0))
    result['temp_value'] = temperature_source.fillna(monthly_fill).fillna(overall_temperature).fillna(0.0)
    return result
