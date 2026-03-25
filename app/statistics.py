from __future__ import annotations

from app.dashboard import aggregates as _aggregates
from app.dashboard import charts as _charts
from app.dashboard import data_access as _data_access
from app.dashboard import management as _management
from app.dashboard import service as _service
from app.dashboard import utils as _utils

_MODULES = (_service, _data_access, _aggregates, _management, _charts, _utils)
__all__ = []

for _module in _MODULES:
    for _name in getattr(_module, "__all__", ()):
        globals()[_name] = getattr(_module, _name)
        __all__.append(_name)

del _module
del _name