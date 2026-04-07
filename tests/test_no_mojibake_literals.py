from __future__ import annotations

import unittest
from pathlib import Path

from tests.mojibake_check import iter_mojibake_hits


class MojibakeLiteralTests(unittest.TestCase):
    def test_source_and_docs_do_not_contain_mojibake_literals(self) -> None:
        roots = [
            Path("app"),
            Path("config"),
            Path("core"),
            Path("tests"),
            Path("main.py"),
            *Path(".").glob("README*"),
        ]
        skip_dirs = {".git", "__pycache__", ".pytest_cache", ".mypy_cache", ".venv", "venv", "node_modules"}

        hits = list(iter_mojibake_hits(roots, skip_dirs=skip_dirs))

        self.assertFalse(
            hits,
            "Unexpected mojibake literals:\n"
            + "\n".join(f"{hit.path}:{hit.line_number}: {hit.line}" for hit in hits[:50]),
        )


if __name__ == "__main__":
    unittest.main()
