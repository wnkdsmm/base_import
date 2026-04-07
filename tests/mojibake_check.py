from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Iterator, NamedTuple


def encode_as_mojibake(value: str) -> str:
    return value.encode("utf-8").decode("cp1251")


def _cp1251_byte_tail() -> str:
    return "".join(bytes([value]).decode("cp1251", errors="ignore") for value in range(0x80, 0xC0)) + chr(0x0098)


_CYRILLIC_ER = chr(0x0420)
_CYRILLIC_ES = chr(0x0421)
_CYRILLIC_VE = chr(0x0432)
_CP1251_TAIL = _cp1251_byte_tail()
_CP1251_TAIL_AFTER_VE = _CP1251_TAIL.replace(chr(0x0401), "").replace(chr(0x0451), "")

MOJIBAKE_PATTERN = re.compile(
    rf"(?:"
    rf"[{re.escape(_CYRILLIC_ER + _CYRILLIC_ES)}][{re.escape(_CP1251_TAIL)}]"
    rf"|{re.escape(_CYRILLIC_VE)}[{re.escape(_CP1251_TAIL_AFTER_VE)}]"
    rf"|\?{{3,}}"
    rf")"
)


class MojibakeHit(NamedTuple):
    path: Path
    line_number: int
    line: str


def iter_mojibake_hits(paths: Iterable[Path], *, skip_dirs: set[str] | None = None) -> Iterator[MojibakeHit]:
    skip_dirs = skip_dirs or set()
    for root in paths:
        candidates = root.rglob("*") if root.is_dir() else [root]
        for path in candidates:
            if path.is_dir() or any(part in skip_dirs for part in path.parts):
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            for line_number, line in enumerate(text.splitlines(), 1):
                if MOJIBAKE_PATTERN.search(line):
                    yield MojibakeHit(path=path, line_number=line_number, line=line.strip())
