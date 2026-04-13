from __future__ import annotations

import inspect
import unittest

from tests.case_runners import ml_model_comparison_model_cases_legacy as legacy


def _build_case_registry() -> dict[str, tuple[type[unittest.TestCase], str]]:
    registry: dict[str, tuple[type[unittest.TestCase], str]] = {}
    for _, candidate in inspect.getmembers(legacy, inspect.isclass):
        if not issubclass(candidate, unittest.TestCase):
            continue
        if candidate.__module__ != legacy.__name__:
            continue
        for method_name in dir(candidate):
            if method_name.startswith("test_"):
                registry[f"{candidate.__name__}.{method_name}"] = (candidate, method_name)
    return registry


_CASE_REGISTRY = _build_case_registry()


def run_ml_model_case(case: dict[str, str]) -> None:
    class_name = case.get("class")
    method_name = case.get("method")
    case_key = f"{class_name}.{method_name}"
    target = _CASE_REGISTRY.get(case_key)
    if target is None:
        available = ", ".join(sorted(_CASE_REGISTRY))
        raise KeyError(f"Unknown ML-model case '{case_key}'. Available: {available}")

    case_class, case_method = target
    test_case = case_class(case_method)
    test_case.debug()


def test_ml_model_case(ml_model_case: dict[str, str]) -> None:
    run_ml_model_case(ml_model_case)


__all__ = ["run_ml_model_case", "test_ml_model_case"]
