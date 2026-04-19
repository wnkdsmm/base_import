"""Backtesting subpackage re-export facade."""

from __future__ import annotations

from . import training_backtesting_baselines as _training_backtesting_baselines
from . import training_backtesting_events as _training_backtesting_events
from . import training_backtesting_execution as _training_backtesting_execution
from . import training_backtesting_horizons as _training_backtesting_horizons
from . import training_backtesting_results as _training_backtesting_results
from . import training_backtesting_support as _training_backtesting_support
from . import training_backtesting_types as _training_backtesting_types

training_backtesting = _training_backtesting_execution


def _reexport_module(module: object) -> list[str]:
    exported_names = getattr(module, "__all__", None)
    if exported_names is None:
        exported_names = [name for name in vars(module) if not name.startswith("__")]
    for name in exported_names:
        globals()[name] = getattr(module, name)
    return list(exported_names)


__all__: list[str] = []
for _module in (
    _training_backtesting_baselines,
    _training_backtesting_events,
    _training_backtesting_execution,
    _training_backtesting_horizons,
    _training_backtesting_results,
    _training_backtesting_support,
    _training_backtesting_types,
):
    __all__.extend(_reexport_module(_module))
__all__ = list(dict.fromkeys(__all__))
__all__.append("training_backtesting")


del _module
del _reexport_module
del _training_backtesting_baselines
del _training_backtesting_events
del _training_backtesting_execution
del _training_backtesting_horizons
del _training_backtesting_results
del _training_backtesting_support
del _training_backtesting_types
