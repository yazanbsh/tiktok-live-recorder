"""
YouTube live watchlist — monitor channels, record when live using yt-dlp + deno.
"""

import json
import subprocess
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from ..shared.config import (
    YT_WATCHLIST_FILE,
    YT_RECORDINGS_DIR,
    YTDLP_BIN,
    DENO_BIN,
    COOKIES_FILE,
)

router = APIRouter()

# ── State ─────────────────────────────────────────────────────────────────────
_state_lock = threading.Lock()
_workers: dict[str, threading.Thread] = {}
_stop_events: dict[str, threading.Event] = {}


def _load_yt_watchlist() -> dict:
    if YT_WATCHLIST_FILE.exists():
        try:
            return json.loads(YT_WATCHLIST_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_yt_watchlist(data: dict):
    YT_WATCHLIST_FILE.write_text(json.dumps(data, indent=2))


yt_watchlist: dict = _load_yt_watchlist()


# ── Models ────────────────────────────────────────────────────────────────────


class YTAddUserRequest(BaseModel):
    username: str  # YouTube channel handle e.g. @channelname or channel ID
    interval: int = 5  # polling interval in minutes
    file_prefix: Optional[str] = None  # optional filename prefix
    record: bool = True


class YTUpdateUserRequest(BaseModel):
    interval: Optional[int] = None
    file_prefix: Optional[str] = None
    record: Optional[bool] = None


# ── Live detection ────────────────────────────────────────────────────────────


def _is_yt_live(channel_url: str) -> bool:
    """
    Use yt-dlp --simulate to check if a channel is currently live.
    Returns True if live, False otherwise.
    """
    try:
        result = subprocess.run(
            [
                YTDLP_BIN,
                "--simulate",
                "--quiet",
                f"--js-runtimes",
                f"deno:{DENO_BIN}",
                f"{channel_url}/live",
            ],
            capture_output=True,
            timeout=30,
        )
        return result.returncode == 0
    except Exception:
        return False


# ── Recording ─────────────────────────────────────────────────────────────────


def _start_yt_recording(
    username: str,
    channel_url: str,
    file_prefix: Optional[str],
    stop_event: threading.Event,
):
    """
    Launch yt-dlp as a subprocess to record the live stream.
    Monitors the stop_event to terminate the process cleanly.
    """
    output_dir = YT_RECORDINGS_DIR / username
    output_dir.mkdir(parents=True, exist_ok=True)

    prefix = file_prefix if file_prefix else "%(channel)s"
    output_template = str(output_dir / f"{prefix}_%(upload_date)s_%(epoch)s.%(ext)s")

    cmd = [
        YTDLP_BIN,
        "--js-runtimes",
        f"deno:{DENO_BIN}",
        "-f",
        "bv*+ba/b",
        "--live-from-start",
        "--hls-use-mpegts",
        "--socket-timeout",
        "30",
        "--skip-unavailable-fragments",
        "--concurrent-fragments",
        "4",
        "--merge-output-format",
        "mp4",
        "-o",
        output_template,
        "--cookies",
        str(COOKIES_FILE),
        f"{channel_url}/live",
    ]

    try:
        proc = subprocess.Popen(cmd)
        # wait until process ends or stop_event is set
        while proc.poll() is None:
            if stop_event.is_set():
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                break
            stop_event.wait(timeout=2)
    except Exception as e:
        with _state_lock:
            if username in yt_watchlist:
                yt_watchlist[username]["last_error"] = str(e)
                _save_yt_watchlist(yt_watchlist)


# ── Worker ────────────────────────────────────────────────────────────────────


def _yt_recording_worker(username: str, stop_event: threading.Event):
    entry = yt_watchlist.get(username, {})
    interval = entry.get("interval", 5)
    channel_url = entry.get("channel_url", "")
    file_prefix = entry.get("file_prefix")

    while not stop_event.is_set():
        try:
            # refresh entry in case it was updated
            with _state_lock:
                entry = yt_watchlist.get(username, {})
                interval = entry.get("interval", 5)
                file_prefix = entry.get("file_prefix")
                should_record = entry.get("record", True)

            is_live = _is_yt_live(channel_url)

            if is_live:
                with _state_lock:
                    if username in yt_watchlist:
                        yt_watchlist[username]["last_seen_live"] = (
                            datetime.utcnow().isoformat()
                        )
                        yt_watchlist[username]["last_error"] = None
                        _save_yt_watchlist(yt_watchlist)

                if should_record:
                    with _state_lock:
                        if username in yt_watchlist:
                            yt_watchlist[username]["status"] = "recording"
                            _save_yt_watchlist(yt_watchlist)

                    rec_stop = threading.Event()
                    rec_thread = threading.Thread(
                        target=_start_yt_recording,
                        args=(username, channel_url, file_prefix, rec_stop),
                        daemon=True,
                    )
                    rec_thread.start()

                    while rec_thread.is_alive() and not stop_event.is_set():
                        rec_thread.join(timeout=2)

                    if stop_event.is_set():
                        rec_stop.set()
                        rec_thread.join(timeout=10)

                with _state_lock:
                    if username in yt_watchlist:
                        yt_watchlist[username]["status"] = "monitoring"
                        yt_watchlist[username]["last_error"] = None
                        _save_yt_watchlist(yt_watchlist)
            else:
                with _state_lock:
                    if username in yt_watchlist:
                        yt_watchlist[username]["status"] = "monitoring"
                        yt_watchlist[username]["last_error"] = None
                        _save_yt_watchlist(yt_watchlist)

        except Exception as e:
            with _state_lock:
                if username in yt_watchlist:
                    yt_watchlist[username]["status"] = "error"
                    yt_watchlist[username]["last_error"] = str(e)
                    _save_yt_watchlist(yt_watchlist)

        stop_event.wait(timeout=interval * 60)

    with _state_lock:
        if username in yt_watchlist:
            yt_watchlist[username]["status"] = "stopped"
            _save_yt_watchlist(yt_watchlist)


def _start_yt_worker(username: str):
    if username in _workers and _workers[username].is_alive():
        return
    stop_event = threading.Event()
    _stop_events[username] = stop_event
    t = threading.Thread(
        target=_yt_recording_worker,
        args=(username, stop_event),
        daemon=True,
        name=f"yt-worker-{username}",
    )
    t.start()
    _workers[username] = t


def _stop_yt_worker(username: str):
    if username in _stop_events:
        _stop_events[username].set()
    if username in _workers:
        _workers[username].join(timeout=15)


# ── Startup ───────────────────────────────────────────────────────────────────


def startup_yt_watchlist():
    """Called from server.py on app startup."""
    for username, entry in yt_watchlist.items():
        yt_watchlist[username]["status"] = "monitoring"
        _start_yt_worker(username)
    _save_yt_watchlist(yt_watchlist)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _count_yt_recordings(username: str) -> int:
    user_dir = YT_RECORDINGS_DIR / username
    if not user_dir.exists():
        return 0
    return len(list(user_dir.glob("*.mp4")))


def _build_channel_url(username: str) -> str:
    """Build the channel URL from a handle or channel ID."""
    username = username.lstrip("@")
    # if it looks like a channel ID (starts with UC and is long) use /channel/
    if username.startswith("UC") and len(username) > 20:
        return f"https://www.youtube.com/channel/{username}"
    return f"https://www.youtube.com/@{username}"


# ── Routes ────────────────────────────────────────────────────────────────────


@router.get("/api/yt/users")
def list_yt_users():
    with _state_lock:
        users = [dict(e) for e in yt_watchlist.values()]
    for u in users:
        u["recordings_count"] = _count_yt_recordings(u["username"])
    return users


@router.post("/api/yt/users", status_code=201)
def add_yt_user(req: YTAddUserRequest):
    username = req.username.lstrip("@").strip()
    if not username:
        raise HTTPException(400, "Username cannot be empty")
    with _state_lock:
        if username in yt_watchlist:
            raise HTTPException(409, f"@{username} is already in the YT watchlist")
        channel_url = _build_channel_url(username)
        entry = {
            "username": username,
            "channel_url": channel_url,
            "interval": req.interval,
            "file_prefix": req.file_prefix,
            "record": req.record,
            "added_at": datetime.utcnow().isoformat(),
            "status": "monitoring",
            "last_seen_live": None,
            "last_error": None,
        }
        yt_watchlist[username] = entry
        _save_yt_watchlist(yt_watchlist)
    _start_yt_worker(username)
    return entry


@router.delete("/api/yt/users/{username}")
def remove_yt_user(username: str):
    username = username.lstrip("@")
    with _state_lock:
        if username not in yt_watchlist:
            raise HTTPException(404, f"@{username} not found in YT watchlist")
    _stop_yt_worker(username)
    with _state_lock:
        del yt_watchlist[username]
        _save_yt_watchlist(yt_watchlist)
    return {"ok": True, "username": username}


@router.patch("/api/yt/users/{username}")
def update_yt_user(username: str, req: YTUpdateUserRequest):
    username = username.lstrip("@")
    with _state_lock:
        if username not in yt_watchlist:
            raise HTTPException(404, f"@{username} not found in YT watchlist")
        entry = yt_watchlist[username]
        if req.interval is not None:
            entry["interval"] = req.interval
        if req.file_prefix is not None:
            entry["file_prefix"] = req.file_prefix
        if req.record is not None:
            entry["record"] = req.record
        _save_yt_watchlist(yt_watchlist)
    return entry


@router.get("/api/yt/users/{username}/status")
def check_yt_live_status(username: str):
    username = username.lstrip("@")
    with _state_lock:
        if username not in yt_watchlist:
            raise HTTPException(404, f"@{username} not found in YT watchlist")
        entry = yt_watchlist[username]
    is_live = _is_yt_live(entry["channel_url"])
    if is_live:
        with _state_lock:
            yt_watchlist[username]["last_seen_live"] = datetime.utcnow().isoformat()
            _save_yt_watchlist(yt_watchlist)
    return {"username": username, "is_live": is_live}


@router.get("/api/yt/recordings")
def list_yt_recordings():
    files = []
    for f in YT_RECORDINGS_DIR.rglob("*.mp4"):
        stat = f.stat()
        username = f.parent.name if f.parent != YT_RECORDINGS_DIR else "unknown"
        files.append(
            {
                "filename": f.name,
                "username": username,
                "size_mb": round(stat.st_size / 1024 / 1024, 2),
                "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
            }
        )
    files.sort(key=lambda x: x["created_at"], reverse=True)
    return files


@router.get("/api/yt/recordings/{username}/{filename}")
def serve_yt_recording(username: str, filename: str, inline: bool = False):
    from fastapi.responses import FileResponse

    file_path = YT_RECORDINGS_DIR / username / filename
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(404, "File not found")
    try:
        file_path.relative_to(YT_RECORDINGS_DIR)
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


class YTBatchDeleteRequest(BaseModel):
    files: list[str]


@router.delete("/api/yt/recordings")
def batch_delete_yt_recordings(req: YTBatchDeleteRequest):
    deleted, failed = [], []
    for entry in req.files:
        try:
            parts = entry.split("/", 1)
            if len(parts) != 2:
                failed.append({"file": entry, "error": "Invalid path"})
                continue
            username, filename = parts
            file_path = YT_RECORDINGS_DIR / username / filename
            file_path.relative_to(YT_RECORDINGS_DIR)
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


@router.get("/api/yt/stats")
def get_yt_stats():
    with _state_lock:
        total = len(yt_watchlist)
        recording = sum(
            1 for e in yt_watchlist.values() if e.get("status") == "recording"
        )
        monitoring = sum(
            1 for e in yt_watchlist.values() if e.get("status") == "monitoring"
        )
    rec_files = list(YT_RECORDINGS_DIR.rglob("*.mp4"))
    disk_mb = sum(f.stat().st_size for f in rec_files) / 1024 / 1024
    total_recs = len(rec_files)
    return {
        "total_users": total,
        "currently_recording": recording,
        "monitoring": monitoring,
        "total_recordings": total_recs,
        "disk_used_mb": round(disk_mb, 1),
    }
