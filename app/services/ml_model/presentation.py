from __future__ import annotations

from . import presentation_backtesting as _presentation_backtesting
from . import presentation_meta as _presentation_meta
from . import presentation_training as _presentation_training


def _reexport_module(module: object) -> list[str]:
    exported_names = getattr(module, '__all__', None)
    if exported_names is None:
        exported_names = [name for name in vars(module) if not name.startswith('__')]
    for name in exported_names:
        globals()[name] = getattr(module, name)
    return list(exported_names)


__all__: list[str] = []
for _module in (_presentation_meta, _presentation_backtesting, _presentation_training):
    __all__.extend(_reexport_module(_module))
__all__ = list(dict.fromkeys(__all__))


del _module
del _reexport_module
del _presentation_meta
del _presentation_backtesting
del _presentation_training
