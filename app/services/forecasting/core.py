from .forecasting_bootstrap import *
from .forecasting_pipeline import *
from . import forecasting_bootstrap as _forecasting_bootstrap_mod
from . import forecasting_pipeline as _forecasting_pipeline_mod

# Rehydrate historical `forecasting.core` namespace for tests/monkeypatching.
globals().update(
    {
        key: value
        for key, value in vars(_forecasting_bootstrap_mod).items()
        if not key.startswith("__")
    }
)
globals().update(
    {
        key: value
        for key, value in vars(_forecasting_pipeline_mod).items()
        if not key.startswith("__")
    }
)

# Backward-compatible symbols historically imported from forecasting.core.
from .forecasting_pipeline import _FORECASTING_CACHE
from .presentation import _build_feature_cards_with_quality


_PATCHABLE_SYMBOLS = [
    "_build_forecasting_table_options",
    "_resolve_forecasting_selection",
    "_selected_source_tables",
    "_selected_source_table_notes",
    "_parse_forecast_days",
    "_parse_history_window",
    "_collect_forecasting_metadata",
    "_build_option_catalog_sql",
    "_resolve_option_value",
    "_build_daily_history_sql",
    "_count_forecasting_records_sql",
    "_build_forecast_rows",
    "build_decision_support_payload",
]


def _sync_forecasting_core_symbols() -> None:
    for name in _PATCHABLE_SYMBOLS:
        if name in globals():
            setattr(_forecasting_bootstrap_mod, name, globals()[name])
            setattr(_forecasting_pipeline_mod, name, globals()[name])


def get_forecasting_page_context(*args, **kwargs):
    _sync_forecasting_core_symbols()
    return _forecasting_pipeline_mod.get_forecasting_page_context(*args, **kwargs)


def get_forecasting_shell_context(*args, **kwargs):
    _sync_forecasting_core_symbols()
    return _forecasting_pipeline_mod.get_forecasting_shell_context(*args, **kwargs)


def get_forecasting_metadata(*args, **kwargs):
    _sync_forecasting_core_symbols()
    return _forecasting_pipeline_mod.get_forecasting_metadata(*args, **kwargs)


def get_forecasting_data(*args, **kwargs):
    _sync_forecasting_core_symbols()
    return _forecasting_pipeline_mod.get_forecasting_data(*args, **kwargs)


def get_forecasting_decision_support_data(*args, **kwargs):
    _sync_forecasting_core_symbols()
    return _forecasting_pipeline_mod.get_forecasting_decision_support_data(*args, **kwargs)
