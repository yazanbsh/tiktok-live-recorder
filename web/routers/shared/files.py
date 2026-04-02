"""
Shared file endpoints — recordings serve/delete, stats, logs.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from .config import RECORDINGS_DIR, LOG_FILE, DATA_DIR

router = APIRouter()


# ── Recordings ────────────────────────────────────────────────────────────────


@router.get("/api/recordings")
def list_recordings():
    files = []
    for ext in ("*.mp4", "*.flv"):
        for f in RECORDINGS_DIR.rglob(ext):
            stat = f.stat()
            username = f.parent.name if f.parent != RECORDINGS_DIR else "unknown"
            files.append(
                {
                    "filename": f.name,
                    "username": username,
                    "size_mb": round(stat.st_size / 1024 / 1024, 2),
                    "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                    "path": str(f),
                }
            )
    files.sort(key=lambda x: x["created_at"], reverse=True)
    return files


@router.get("/api/recordings/{username}/{filename}")
def serve_recording(username: str, filename: str, inline: bool = False):
    file_path = RECORDINGS_DIR / username / filename
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(404, "File not found")
    try:
        file_path.relative_to(RECORDINGS_DIR)
    except ValueError:
        raise HTTPException(403, "Access denied")
    disposition = (
        f'inline; filename="{filename}"'
        if inline
        else f'attachment; filename="{filename}"'
    )
    return FileResponse(
        path=str(file_path),
        filename=filename,
        media_type="video/mp4",
        headers={"Content-Disposition": disposition},
    )


class BatchDeleteRecordingsRequest(BaseModel):
    files: list[str]


@router.delete("/api/recordings")
def batch_delete_recordings(req: BatchDeleteRecordingsRequest):
    deleted, failed = [], []
    for entry in req.files:
        try:
            parts = entry.split("/", 1)
            if len(parts) != 2:
                failed.append({"file": entry, "error": "Invalid path format"})
                continue
            username, filename = parts
            file_path = RECORDINGS_DIR / username / filename
            file_path.relative_to(RECORDINGS_DIR)
            if not file_path.exists():
                failed.append({"file": entry, "error": "File not found"})
                continue
            file_path.unlink()
            deleted.append(entry)
        except ValueError:
            failed.append({"file": entry, "error": "Access denied"})
        except Exception as e:
            failed.append({"file": entry, "error": str(e)})
    return {"deleted": deleted, "failed": failed}


# ── Logs ──────────────────────────────────────────────────────────────────────

APP_LOG_FILE = DATA_DIR / "logs" / "app.log"


@router.get("/api/logs")
def get_logs(lines: int = 200):
    """Serve app.log (Python logging). Falls back to tiktok-recorder.log."""
    log_file = APP_LOG_FILE if APP_LOG_FILE.exists() else LOG_FILE
    if not log_file.exists():
        return {"lines": []}
    try:
        all_lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
        return {"lines": all_lines[-lines:]}
    except Exception as e:
        return {"lines": [], "error": str(e)}
