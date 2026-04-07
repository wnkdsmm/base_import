from __future__ import annotations

"""Helpers for tiny lazy compatibility facades."""

from importlib import import_module
from typing import Any, Callable, Mapping

LazyExportMap = Mapping[str, tuple[str, str]]


def build_lazy_getattr(
    module_name: str,
    module_globals: dict[str, Any],
    exports: LazyExportMap,
) -> Callable[[str], Any]:
    def __getattr__(name: str) -> Any:
        try:
            target_module_name, attr_name = exports[name]
        except KeyError as exc:
            raise AttributeError(f"module {module_name!r} has no attribute {name!r}") from exc
        value = getattr(import_module(target_module_name), attr_name)
        module_globals.setdefault(name, value)
        return value

    return __getattr__


def build_lazy_dir(module_globals: Mapping[str, Any], export_names: list[str]) -> Callable[[], list[str]]:
    def __dir__() -> list[str]:
        return sorted(set(module_globals) | set(export_names))

    return __dir__


def install_lazy_exports(
    module_name: str,
    module_globals: dict[str, Any],
    exports: LazyExportMap,
) -> tuple[list[str], Callable[[str], Any], Callable[[], list[str]]]:
    """Install tiny lazy attribute exports for compatibility facades."""

    export_names = list(exports)
    return (
        export_names,
        build_lazy_getattr(module_name, module_globals, exports),
        build_lazy_dir(module_globals, export_names),
    )


__all__ = ["LazyExportMap", "build_lazy_dir", "build_lazy_getattr", "install_lazy_exports"]
