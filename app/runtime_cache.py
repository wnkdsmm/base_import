from __future__ import annotations

"""Backward-compatible wrapper around :mod:`app.cache`.

New code should import cache primitives from ``app.cache``.
"""

from app.cache import (
    CopyingLruCache,
    CopyingTtlCache,
    build_immutable_payload_lru_cache,
    build_immutable_payload_ttl_cache,
    callable_cache_scope,
    clone_mutable_payload,
    freeze_mutable_payload,
)

__all__ = [
    "CopyingLruCache",
    "CopyingTtlCache",
    "build_immutable_payload_lru_cache",
    "build_immutable_payload_ttl_cache",
    "callable_cache_scope",
    "clone_mutable_payload",
    "freeze_mutable_payload",
]
