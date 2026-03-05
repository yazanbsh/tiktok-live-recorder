"""
TikTok Live Recorder — Web UI Backend
Place this file at:  <repo-root>/web/server.py
Run with:  uv run uvicorn web.server:app --host 0.0.0.0 --port 8000 --reload
"""

import json
import os
import sys
import time
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

# ── make sure src/ is importable ──────────────────────────────────────────────
SRC_DIR = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

from utils.recorder_config import RecorderConfig
from utils.enums import Mode
from utils.utils import read_cookies
from core.tiktok_recorder import TikTokRecorder
from core.tiktok_api import TikTokAPI
from utils.video_management import VideoManagement

# ── paths ──────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent
STATIC_DIR     = BASE_DIR / "static"
DATA_DIR       = Path(os.environ.get("DATA_DIR", str(BASE_DIR.parent / "data")))
WATCHLIST_FILE = DATA_DIR / "watchlist.json"
RECORDINGS_DIR = DATA_DIR / "recordings"
LOG_FILE       = DATA_DIR / "logs" / "tiktok-recorder.log"

# ensure all data subdirs exist
(DATA_DIR / "recordings").mkdir(parents=True, exist_ok=True)
(DATA_DIR / "logs").mkdir(parents=True, exist_ok=True)

app = FastAPI(title="TikTok Live Recorder", version="1.0.0")

# ── in-memory state ────────────────────────────────────────────────────────────
_state_lock = threading.Lock()

def _load_watchlist() -> dict:
    if WATCHLIST_FILE.exists():
        try:
            return json.loads(WATCHLIST_FILE.read_text())
        except Exception:
            pass
    return {}

def _save_watchlist(data: dict):
    WATCHLIST_FILE.write_text(json.dumps(data, indent=2))

watchlist: dict = _load_watchlist()

# { username -> Thread }  — the outer monitoring/polling loop
_workers:         dict[str, threading.Thread] = {}
# { username -> Event }   — set to stop the whole worker (monitoring + recording)
_stop_events:     dict[str, threading.Event]  = {}
# { username -> Event }   — set to stop only the active recording, keep monitoring
_rec_stop_events: dict[str, threading.Event]  = {}


# ── pydantic models ────────────────────────────────────────────────────────────

class AddUserRequest(BaseModel):
    username:  str
    mode:      str           = "automatic"
    interval:  int           = 5
    proxy:     Optional[str] = None
    output:    Optional[str] = None
    duration:  Optional[int] = None
    bitrate:   Optional[str] = None

class UpdateUserRequest(BaseModel):
    mode:     Optional[str] = None
    interval: Optional[int] = None
    proxy:    Optional[str] = None


# ── StoppableTikTokRecorder ────────────────────────────────────────────────────

class StoppableTikTokRecorder(TikTokRecorder):
    """
    Subclass of TikTokRecorder that checks a threading.Event on every downloaded
    chunk — allowing a clean stop from the web UI without killing the process.
    """

    def __init__(self, config: RecorderConfig, stop_event: threading.Event):
        super().__init__(config)
        self._stop_event = stop_event

    def start_recording(self, user: str, room_id: str):
        from http.client import HTTPException as HTTPEx
        from requests import RequestException
        from utils.enums import TikTokError
        from utils.custom_exceptions import LiveNotFound
        from utils.logger_manager import logger

        live_url = self.tiktok.get_live_url(room_id)
        if not live_url:
            raise LiveNotFound(TikTokError.RETRIEVE_LIVE_URL)

        output = self._build_output_path(user)
        logger.info("Started recording (web-stoppable)...")

        buffer_size = 512 * 1024
        buffer = bytearray()

        with open(output, "wb") as out_file:
            stop_recording = False
            while not stop_recording and not self._stop_event.is_set():
                try:
                    if not self.tiktok.is_room_alive(room_id):
                        logger.info("User is no longer live. Stopping recording.")
                        break

                    start_time = time.time()
                    for chunk in self.tiktok.download_live_stream(live_url):
                        # checked on every ~4 KB chunk → near-instant response
                        if self._stop_event.is_set():
                            stop_recording = True
                            break
                        buffer.extend(chunk)
                        if len(buffer) >= buffer_size:
                            out_file.write(buffer)
                            buffer.clear()
                        elapsed = time.time() - start_time
                        if self.duration and elapsed >= self.duration:
                            stop_recording = True
                            break

                except (RequestException, HTTPEx) as ex:
                    if self._stop_event.is_set():
                        break
                    logger.warning(f"Network hiccup, retrying: {ex}")
                    time.sleep(2)

                except Exception as ex:
                    logger.error(f"Unexpected error during recording: {ex}", exc_info=True)
                    stop_recording = True

                finally:
                    if buffer:
                        out_file.write(buffer)
                        buffer.clear()
                    out_file.flush()

        logger.info(f"Recording finished: {Path(output).resolve()}\n")
        VideoManagement.convert_flv_to_mp4(output, self.bitrate)


# ── helpers ────────────────────────────────────────────────────────────────────

def _get_cookies() -> dict:
    try:
        return read_cookies()
    except Exception:
        return {}

def _make_config(username: str, entry: dict, mode: Mode) -> RecorderConfig:
    return RecorderConfig(
        mode=mode,
        user=username,
        automatic_interval=entry.get("interval", 5),
        cookies=_get_cookies(),
        proxy=entry.get("proxy"),
        output=str(entry.get("output") or RECORDINGS_DIR / username),
        duration=entry.get("duration"),
        bitrate=entry.get("bitrate"),
    )

def _recording_worker(username: str, stop_event: threading.Event):
    """
    Background thread: polls TikTok every N minutes, records when live.
    stop_event              → kills the whole worker (user removed / server shutdown)
    _rec_stop_events[user]  → stops only the active recording, loop continues
    """
    entry    = watchlist.get(username, {})
    cookies  = _get_cookies()
    api      = TikTokAPI(proxy=entry.get("proxy"), cookies=cookies)
    interval = entry.get("interval", 5)

    while not stop_event.is_set():
        try:
            room_id = api.get_room_id_from_user(username)
            if room_id and api.is_room_alive(room_id):
                with _state_lock:
                    if username in watchlist:
                        watchlist[username]["last_seen_live"] = datetime.utcnow().isoformat()
                        watchlist[username]["status"] = "recording"
                        _save_watchlist(watchlist)

                # fresh stop event for this recording session
                rec_stop = threading.Event()
                _rec_stop_events[username] = rec_stop

                cfg      = _make_config(username, entry, Mode.MANUAL)
                recorder = StoppableTikTokRecorder(cfg, rec_stop)

                rec_thread = threading.Thread(
                    target=recorder.start_recording,
                    args=(username, room_id),
                    daemon=True,
                )
                rec_thread.start()

                # wait until: stream ends | worker stopped | recording manually stopped
                while rec_thread.is_alive() and not stop_event.is_set() and not rec_stop.is_set():
                    rec_thread.join(timeout=2)

                # if the whole worker is being killed, propagate to recording too
                if stop_event.is_set():
                    rec_stop.set()

                rec_thread.join(timeout=10)

                with _state_lock:
                    if username in watchlist:
                        watchlist[username]["status"] = "monitoring"
                        watchlist[username]["recordings_count"] = (
                            watchlist[username].get("recordings_count", 0) + 1
                        )
                        _save_watchlist(watchlist)
            else:
                with _state_lock:
                    if username in watchlist:
                        watchlist[username]["status"] = "monitoring"
                        _save_watchlist(watchlist)

        except Exception as e:
            with _state_lock:
                if username in watchlist:
                    watchlist[username]["status"] = "error"
                    watchlist[username]["last_error"] = str(e)
                    _save_watchlist(watchlist)

        stop_event.wait(timeout=interval * 60)

    with _state_lock:
        if username in watchlist:
            watchlist[username]["status"] = "stopped"
            _save_watchlist(watchlist)


def _start_worker(username: str):
    if username in _workers and _workers[username].is_alive():
        return
    stop_event = threading.Event()
    _stop_events[username] = stop_event
    t = threading.Thread(
        target=_recording_worker,
        args=(username, stop_event),
        daemon=True,
        name=f"worker-{username}",
    )
    t.start()
    _workers[username] = t

def _stop_worker(username: str):
    # first stop the active recording if any
    if username in _rec_stop_events:
        _rec_stop_events[username].set()
    # then stop the whole monitoring loop
    if username in _stop_events:
        _stop_events[username].set()
    if username in _workers:
        _workers[username].join(timeout=10)


# ── startup ────────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    for username, entry in watchlist.items():
        if entry.get("mode", "automatic") == "automatic":
            watchlist[username]["status"] = "monitoring"
            _start_worker(username)
    _save_watchlist(watchlist)


# ── API routes ─────────────────────────────────────────────────────────────────

@app.get("/api/users")
def list_users():
    with _state_lock:
        return list(watchlist.values())


@app.post("/api/users", status_code=201)
def add_user(req: AddUserRequest):
    username = req.username.lstrip("@").strip()
    if not username:
        raise HTTPException(400, "Username cannot be empty")
    with _state_lock:
        if username in watchlist:
            raise HTTPException(409, f"@{username} is already in the watchlist")
        entry = {
            "username":         username,
            "mode":             req.mode,
            "interval":         req.interval,
            "proxy":            req.proxy,
            "output":           req.output,
            "duration":         req.duration,
            "bitrate":          req.bitrate,
            "added_at":         datetime.utcnow().isoformat(),
            "status":           "monitoring" if req.mode == "automatic" else "idle",
            "last_seen_live":   None,
            "last_error":       None,
            "recordings_count": 0,
        }
        watchlist[username] = entry
        _save_watchlist(watchlist)
    if req.mode == "automatic":
        _start_worker(username)
    return entry


@app.delete("/api/users/{username}")
def remove_user(username: str):
    username = username.lstrip("@")
    with _state_lock:
        if username not in watchlist:
            raise HTTPException(404, f"@{username} not found")
    _stop_worker(username)   # stops recording + worker before deleting
    with _state_lock:
        del watchlist[username]
        _save_watchlist(watchlist)
    return {"ok": True, "username": username}


@app.patch("/api/users/{username}")
def update_user(username: str, req: UpdateUserRequest):
    username = username.lstrip("@")
    with _state_lock:
        if username not in watchlist:
            raise HTTPException(404, f"@{username} not found")
        entry = watchlist[username]
        if req.mode     is not None: entry["mode"]     = req.mode
        if req.interval is not None: entry["interval"] = req.interval
        if req.proxy    is not None: entry["proxy"]    = req.proxy
        _save_watchlist(watchlist)
    _stop_worker(username)
    if entry.get("mode") == "automatic":
        _start_worker(username)
    return entry


@app.post("/api/users/{username}/record")
def manual_record(username: str, background_tasks: BackgroundTasks):
    """Trigger a one-shot manual recording."""
    username = username.lstrip("@")
    with _state_lock:
        if username not in watchlist:
            raise HTTPException(404, f"@{username} not found")
        if watchlist[username].get("status") == "recording":
            raise HTTPException(409, f"@{username} is already recording")
        entry = dict(watchlist[username])

    rec_stop = threading.Event()
    _rec_stop_events[username] = rec_stop

    def _do_record():
        try:
            with _state_lock:
                watchlist[username]["status"] = "recording"
                _save_watchlist(watchlist)
            cfg      = _make_config(username, entry, Mode.MANUAL)
            recorder = StoppableTikTokRecorder(cfg, rec_stop)
            recorder.run()
        except Exception as e:
            with _state_lock:
                if username in watchlist:
                    watchlist[username]["status"] = "error"
                    watchlist[username]["last_error"] = str(e)
                    _save_watchlist(watchlist)
        else:
            with _state_lock:
                if username in watchlist:
                    watchlist[username]["status"] = "idle"
                    watchlist[username]["recordings_count"] = (
                        watchlist[username].get("recordings_count", 0) + 1
                    )
                    _save_watchlist(watchlist)

    background_tasks.add_task(_do_record)
    return {"ok": True, "message": f"Manual recording triggered for @{username}"}


@app.post("/api/users/{username}/stop")
def stop_recording(username: str):
    """Stop an active recording — keeps the user in watchlist and monitoring continues."""
    username = username.lstrip("@")
    with _state_lock:
        if username not in watchlist:
            raise HTTPException(404, f"@{username} not found")
        status = watchlist[username].get("status")

    if status != "recording":
        raise HTTPException(409, f"@{username} is not currently recording (status: {status})")

    if username not in _rec_stop_events:
        raise HTTPException(500, "No active recording event found for this user")

    _rec_stop_events[username].set()
    return {"ok": True, "message": f"Stop signal sent to @{username}'s recording"}


@app.get("/api/users/{username}/status")
def check_live_status(username: str):
    username = username.lstrip("@")
    with _state_lock:
        if username not in watchlist:
            raise HTTPException(404, f"@{username} not found")
        entry = watchlist[username]
    try:
        api     = TikTokAPI(proxy=entry.get("proxy"), cookies=_get_cookies())
        room_id = api.get_room_id_from_user(username)
        is_live = bool(room_id and api.is_room_alive(room_id))
        if is_live:
            with _state_lock:
                watchlist[username]["last_seen_live"] = datetime.utcnow().isoformat()
                _save_watchlist(watchlist)
        return {"username": username, "is_live": is_live, "room_id": room_id}
    except Exception as e:
        return {"username": username, "is_live": False, "error": str(e)}


@app.get("/api/recordings")
def list_recordings():
    files = []
    for ext in ("*.mp4", "*.flv"):
        for f in RECORDINGS_DIR.rglob(ext):
            stat = f.stat()
            # parent dir name = username (e.g. /data/recordings/flomtv/file.mp4)
            username = f.parent.name if f.parent != RECORDINGS_DIR else "unknown"
            files.append({
                "filename":   f.name,
                "username":   username,
                "size_mb":    round(stat.st_size / 1024 / 1024, 2),
                "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "path":       str(f),
            })
    files.sort(key=lambda x: x["created_at"], reverse=True)
    return files


@app.get("/api/logs")
def get_logs(lines: int = 100):
    if not LOG_FILE.exists():
        return {"lines": []}
    try:
        all_lines = LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
        return {"lines": all_lines[-lines:]}
    except Exception as e:
        return {"lines": [], "error": str(e)}


@app.get("/api/stats")
def get_stats():
    with _state_lock:
        total      = len(watchlist)
        recording  = sum(1 for e in watchlist.values() if e.get("status") == "recording")
        monitoring = sum(1 for e in watchlist.values() if e.get("status") == "monitoring")
        total_recs = sum(e.get("recordings_count", 0) for e in watchlist.values())
    rec_files = list(RECORDINGS_DIR.glob("*.mp4"))
    disk_mb   = sum(f.stat().st_size for f in rec_files) / 1024 / 1024
    return {
        "total_users":         total,
        "currently_recording": recording,
        "monitoring":          monitoring,
        "total_recordings":    total_recs,
        "disk_used_mb":        round(disk_mb, 1),
    }


# ── static / frontend ──────────────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.get("/")
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)