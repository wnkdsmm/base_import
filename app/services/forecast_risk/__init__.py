"""Public forecast-risk service modules without eager submodule imports."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

__all__ = [
    "constants",
    "core",
    "data",
    "presentation",
    "profiles",
    "scoring",
    "utils",
    "validation",
]


def __getattr__(name: str) -> ModuleType:
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return import_module(f"{__name__}.{name}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
