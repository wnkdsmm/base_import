from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from app.cache import build_immutable_payload_lru_cache

from .ml_model_config_types import _CACHE_LIMIT


@dataclass
class MLModelCaches:
    ml_cache: Any
    artifact_cache: OrderedDict[Any, Any]


def create_default_caches() -> MLModelCaches:
    return MLModelCaches(
        ml_cache=build_immutable_payload_lru_cache(max_size=_CACHE_LIMIT),
        artifact_cache=OrderedDict(),
    )
