from __future__ import annotations

import json
from pathlib import Path

import pytest


_FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_cases(filename: str) -> list[dict[str, str]]:
    payload = json.loads((_FIXTURES_DIR / filename).read_text(encoding="utf-8"))
    return list(payload.get("cases") or [])


def _case_id(case: dict[str, str]) -> str:
    return f"{case.get('class', 'Unknown')}.{case.get('method', 'unknown')}"


_ML_MODEL_CASES = _load_cases("ml_model_cases.json")
_CLUSTERING_CASES = _load_cases("clustering_cases.json")


@pytest.fixture(scope="session")
def ml_model_cases() -> list[dict[str, str]]:
    return list(_ML_MODEL_CASES)


@pytest.fixture(scope="session")
def clustering_cases() -> list[dict[str, str]]:
    return list(_CLUSTERING_CASES)


@pytest.fixture(scope="session", params=_ML_MODEL_CASES, ids=_case_id)
def ml_model_case(request: pytest.FixtureRequest) -> dict[str, str]:
    return dict(request.param)


@pytest.fixture(scope="session", params=_CLUSTERING_CASES, ids=_case_id)
def clustering_case(request: pytest.FixtureRequest) -> dict[str, str]:
    return dict(request.param)
