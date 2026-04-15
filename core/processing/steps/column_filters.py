from __future__ import annotations

from types import ModuleType
from typing import Any

from . import column_filter_match as _match
from . import column_filter_payload as _payload
from . import column_filter_text as _text
from .column_filter_match import NatashaColumnMatcher, _legacy_get_mandatory_feature_catalog, get_column_matcher

_SOURCE_MODULES: tuple[ModuleType, ...] = (_text, _payload, _match)


def __getattr__(name: str) -> Any:
    for module in _SOURCE_MODULES:
        if hasattr(module, name):
            return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    exported = set(globals())
    for module in _SOURCE_MODULES:
        exported.update(name for name in vars(module) if not name.startswith('__'))
    return sorted(exported)
