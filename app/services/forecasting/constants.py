from __future__ import annotations

"""Deprecated compatibility layer for forecasting constants.

Canonical sources:
- non-UI constants: ``config.constants``
- UI labels/copy: ``app.labels``
"""

from app.domain.analytics_metadata import PLOTLY_PALETTE
from app.domain.fire_columns import (
    BUILDING_CAUSE_COLUMN,
    CAUSE_COLUMN_CANDIDATES,
    DATE_COLUMN,
    DISTRICT_COLUMN_CANDIDATES,
    GENERAL_CAUSE_COLUMN,
    LATITUDE_COLUMN_CANDIDATES,
    LONGITUDE_COLUMN_CANDIDATES,
    OBJECT_CATEGORY_COLUMN,
    OPEN_AREA_CAUSE_COLUMN,
    TEMPERATURE_COLUMN_CANDIDATES,
)
from app.domain.time_labels import FORECAST_MONTH_LABELS as MONTH_LABELS
from app.domain.time_labels import FORECAST_WEEKDAY_LABELS as WEEKDAY_LABELS
from app.labels import FORECASTING_HISTORY_WINDOW_LABELS, SCENARIO_FORECAST_DESCRIPTION
from config.constants import (
    FORECASTING_FORECAST_DAY_OPTIONS,
    FORECASTING_HISTORY_WINDOWS,
    GEO_LOOKBACK_DAYS,
    MAX_GEO_CHART_POINTS,
    MAX_GEO_HOTSPOTS,
)


FORECAST_DAY_OPTIONS = list(FORECASTING_FORECAST_DAY_OPTIONS)
HISTORY_WINDOW_OPTIONS = [
    {"value": value, "label": FORECASTING_HISTORY_WINDOW_LABELS.get(value, value)}
    for value in FORECASTING_HISTORY_WINDOWS
]
