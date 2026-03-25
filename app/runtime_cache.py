from __future__ import annotations

import copy
import time
from threading import RLock
from typing import Callable, Dict, Generic, Hashable, Optional, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class CopyingTtlCache(Generic[K, V]):
    def __init__(self, ttl_seconds: float, copier: Optional[Callable[[V], V]] = None) -> None:
        self._ttl_seconds = max(float(ttl_seconds), 0.0)
        self._copier: Callable[[V], V] = copier or copy.deepcopy
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
            return self._copy_value(item.get("value"))

    def set(self, key: K, value: V) -> V:
        stored_value = self._copy_value(value)
        with self._lock:
            self._items[key] = {
                "value": stored_value,
                "expires_at": time.time() + self._ttl_seconds,
            }
        return self._copy_value(stored_value)

    def clear(self) -> None:
        with self._lock:
            self._items.clear()

    def delete(self, key: K) -> None:
        with self._lock:
            self._items.pop(key, None)

    def _copy_value(self, value: object) -> V:
        return self._copier(value)  # type: ignore[return-value]
