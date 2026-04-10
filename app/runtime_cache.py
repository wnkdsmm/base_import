from __future__ import annotations

import copy
import time
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta
from pathlib import PurePath
from threading import RLock
from typing import Any, Callable, Dict, Generic, Hashable, Optional, TypeVar
from uuid import UUID

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


_IMMUTABLE_LEAF_TYPES = (
    str,
    bytes,
    int,
    float,
    bool,
    complex,
    type(None),
    date,
    datetime,
    datetime_time,
    timedelta,
    PurePath,
    UUID,
)


@dataclass(frozen=True, slots=True)
class _FrozenDict:
    items: tuple[tuple[Any, object], ...]


@dataclass(frozen=True, slots=True)
class _FrozenList:
    items: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class _FrozenTuple:
    items: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class _FrozenSet:
    items: frozenset[object]
    preserve_frozenset: bool = False


@dataclass(frozen=True, slots=True)
class _FrozenLeaf:
    value: Any


def freeze_mutable_payload(value: V) -> object:
    """Build an immutable snapshot for JSON-like payloads with deepcopy fallback for rare leaves."""
    if isinstance(value, _IMMUTABLE_LEAF_TYPES):
        return value
    if isinstance(value, _FrozenDict | _FrozenList | _FrozenTuple | _FrozenSet | _FrozenLeaf):
        return value
    if isinstance(value, dict):
        return _FrozenDict(tuple((key, freeze_mutable_payload(item)) for key, item in value.items()))
    if isinstance(value, list):
        return _FrozenList(tuple(freeze_mutable_payload(item) for item in value))
    if isinstance(value, tuple):
        return _FrozenTuple(tuple(freeze_mutable_payload(item) for item in value))
    if isinstance(value, set):
        return _FrozenSet(frozenset(freeze_mutable_payload(item) for item in value))
    if isinstance(value, frozenset):
        return _FrozenSet(
            frozenset(freeze_mutable_payload(item) for item in value),
            preserve_frozenset=True,
        )
    return _FrozenLeaf(copy.deepcopy(value))


def clone_mutable_payload(value: object) -> V:
    """Return a fresh mutable copy from a plain payload or from a frozen snapshot."""
    if isinstance(value, _IMMUTABLE_LEAF_TYPES):
        return value  # type: ignore[return-value]
    if isinstance(value, _FrozenLeaf):
        return copy.deepcopy(value.value)  # type: ignore[return-value]
    if isinstance(value, _FrozenDict):
        return {key: clone_mutable_payload(item) for key, item in value.items}  # type: ignore[return-value]
    if isinstance(value, dict):
        return {key: clone_mutable_payload(item) for key, item in value.items()}  # type: ignore[return-value]
    if isinstance(value, _FrozenList):
        return [clone_mutable_payload(item) for item in value.items]  # type: ignore[return-value]
    if isinstance(value, list):
        return [clone_mutable_payload(item) for item in value]  # type: ignore[return-value]
    if isinstance(value, _FrozenTuple):
        return tuple(clone_mutable_payload(item) for item in value.items)  # type: ignore[return-value]
    if isinstance(value, tuple):
        return tuple(clone_mutable_payload(item) for item in value)  # type: ignore[return-value]
    if isinstance(value, _FrozenSet):
        items = {clone_mutable_payload(item) for item in value.items}
        if value.preserve_frozenset:
            return frozenset(items)  # type: ignore[return-value]
        return items  # type: ignore[return-value]
    if isinstance(value, set):
        return {clone_mutable_payload(item) for item in value}  # type: ignore[return-value]
    if isinstance(value, frozenset):
        return frozenset(clone_mutable_payload(item) for item in value)  # type: ignore[return-value]
    return copy.deepcopy(value)  # type: ignore[return-value]


class CopyingTtlCache(Generic[K, V]):
    def __init__(
        self,
        ttl_seconds: float,
        copier: Optional[Callable[[V], V]] = None,
        *,
        storer: Optional[Callable[[V], object]] = None,
        loader: Optional[Callable[[object], V]] = None,
    ) -> None:
        self._ttl_seconds = max(float(ttl_seconds), 0.0)
        if (storer is None) != (loader is None):
            raise ValueError("CopyingTtlCache requires both storer and loader or neither of them.")
        if storer is not None and loader is not None:
            self._store_value = storer
            self._load_value = loader
        else:
            copy_value = copier or copy.deepcopy
            self._store_value = copy_value
            self._load_value = copy_value
        self._lock = RLock()
        self._items: Dict[K, Dict[str, object]] = {}

    def get(self, key: K) -> Optional[V]:
        now = time.time()
        with self._lock:
            item = self._items.get(key)
            if item is None:
                return None
            if float(item.get("expires_at") or 0.0) <= now:
                self._items.pop(key, None)
                return None
            return self._load_value(item.get("value"))

    def set(self, key: K, value: V) -> V:
        stored_value = self._store_value(value)
        with self._lock:
            self._items[key] = {
                "value": stored_value,
                "expires_at": time.time() + self._ttl_seconds,
            }
        return self._load_value(stored_value)

    def clear(self) -> None:
        with self._lock:
            self._items.clear()

    def delete(self, key: K) -> None:
        with self._lock:
            self._items.pop(key, None)
