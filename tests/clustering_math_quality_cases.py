from __future__ import annotations

def _case_key(case: dict[str, str]) -> str:
    class_name = case.get("class")
    method_name = case.get("method")
    return f"{class_name}.{method_name}"


def run_clustering_case(case: dict[str, str], known_cases: list[dict[str, str]]) -> None:
    case_key = _case_key(case)
    case_registry = {_case_key(item) for item in known_cases}
    if case_key not in case_registry:
        available = ", ".join(sorted(case_registry))
        raise KeyError(f"Unknown clustering case '{case_key}'. Available: {available}")

    if not case.get("class") or not case.get("method"):
        raise ValueError(f"Invalid clustering case payload: {case!r}")


def test_clustering_case(clustering_case: dict[str, str], clustering_cases: list[dict[str, str]]) -> None:
    run_clustering_case(clustering_case, clustering_cases)


__all__ = ["run_clustering_case", "test_clustering_case"]
