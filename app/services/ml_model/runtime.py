from __future__ import annotations

from typing import Callable, Optional

MlProgressCallback = Optional[Callable[[str, str], None]]


def _emit_progress(progress_callback: MlProgressCallback, phase: str, message: str) -> None:
    if progress_callback is None:
        return
    try:
        progress_callback(phase, message)
    except Exception:
        return
