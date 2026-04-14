"""Backtesting subpackage re-export facade."""

from __future__ import annotations

from . import training_backtesting as _training_backtesting
from . import training_backtesting_baselines as _training_backtesting_baselines
from . import training_backtesting_events as _training_backtesting_events
from . import training_backtesting_horizons as _training_backtesting_horizons
from . import training_backtesting_runtime as _training_backtesting_runtime
from . import training_backtesting_support as _training_backtesting_support
from . import training_backtesting_types as _training_backtesting_types


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
    _training_backtesting_horizons,
    _training_backtesting_runtime,
    _training_backtesting_support,
    _training_backtesting_types,
    _training_backtesting,
):
    __all__.extend(_reexport_module(_module))
__all__ = list(dict.fromkeys(__all__))


del _module
del _reexport_module
del _training_backtesting
del _training_backtesting_baselines
del _training_backtesting_events
del _training_backtesting_horizons
del _training_backtesting_runtime
del _training_backtesting_support
del _training_backtesting_types
