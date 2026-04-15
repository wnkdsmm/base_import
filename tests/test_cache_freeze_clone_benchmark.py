from __future__ import annotations

import json
import time
import unittest

from app.cache import CopyingTtlCache, clone_mutable_payload, freeze_mutable_payload


def _build_synthetic_payload(target_bytes: int) -> dict[str, object]:
    rows: list[dict[str, object]] = []
    base_row = {
        "id": 0,
        "name": "fire-event",
        "tags": ["north", "residential", "night"],
        "metrics": {"count": 3, "risk": 0.42, "score": 71},
        "notes": "x" * 120,
    }
    while True:
        row = dict(base_row)
        row["id"] = len(rows)
        rows.append(row)
        payload = {
            "summary": {"rows": len(rows), "kind": "benchmark"},
            "rows": rows,
            "status": {"state": "ok", "updated_at": "2026-01-01T00:00:00"},
            "logs": ["ready", "cached"],
        }
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) >= target_bytes:
            return payload


def _measure_ms(fn, iterations: int = 1) -> float:
    started = time.perf_counter()
    for _ in range(max(1, iterations)):
        fn()
    return (time.perf_counter() - started) * 1000.0


class CacheFreezeCloneBenchmarkTests(unittest.TestCase):
    def test_benchmark_freeze_clone_vs_skip_freeze(self) -> None:
        sizes = [
            ("50KB", 50 * 1024, 3),
            ("500KB", 500 * 1024, 2),
            ("5MB", 5 * 1024 * 1024, 1),
        ]
        report: dict[str, dict[str, float]] = {}

        for label, target_size, iterations in sizes:
            payload = _build_synthetic_payload(target_size)

            before_cache = CopyingTtlCache[str, dict[str, object]](
                ttl_seconds=60.0,
                storer=freeze_mutable_payload,
                loader=clone_mutable_payload,
            )
            after_cache = CopyingTtlCache[str, dict[str, object]](
                ttl_seconds=60.0,
                skip_freeze=True,
            )

            before_ms = _measure_ms(
                lambda: (before_cache.set("payload", payload), before_cache.get("payload")),
                iterations=iterations,
            )
            after_ms = _measure_ms(
                lambda: (after_cache.set("payload", payload), after_cache.get("payload")),
                iterations=iterations,
            )
            report[label] = {
                "before_ms": round(before_ms, 2),
                "after_ms": round(after_ms, 2),
            }

            self.assertGreater(before_ms, 0.0)
            self.assertGreater(after_ms, 0.0)

        print("cache_freeze_clone_benchmark", report)


if __name__ == "__main__":
    unittest.main()

