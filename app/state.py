from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional


UPLOAD_FOLDER = Path("uploads")
UPLOAD_FOLDER.mkdir(exist_ok=True)


@dataclass
class UploadState:
    current_file_path: Optional[Path] = None
    history: Dict[str, dict] = field(default_factory=dict)

    def set_uploaded_file(self, file_path: Path, original_name: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.current_file_path = file_path
        self.history[str(file_path)] = {
            "original_name": original_name,
            "upload_time": timestamp,
            "path": str(file_path),
        }

    def clear_current_file(self) -> None:
        self.current_file_path = None

    def has_uploaded_file(self) -> bool:
        return self.current_file_path is not None and self.current_file_path.exists()


upload_state = UploadState()
