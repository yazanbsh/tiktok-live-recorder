"""
Shared configuration — paths, constants, common imports.
All routers import from here instead of redeclaring.
"""

import os
import sys
from pathlib import Path

# ── make sure src/ is importable ─────────────────────────────────────────────
SRC_DIR = Path(__file__).parent.parent.parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

# ── paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent.parent  # web/
DATA_DIR = Path(os.environ.get("DATA_DIR", str(BASE_DIR.parent / "data")))
STATIC_DIR = BASE_DIR / "static"
WATCHLIST_FILE = DATA_DIR / "watchlist.json"
YT_WATCHLIST_FILE = DATA_DIR / "yt_watchlist.json"
RECORDINGS_DIR = DATA_DIR / "recordings"
YT_RECORDINGS_DIR = DATA_DIR / "yt_recordings"
DOWNLOADS_DIR = DATA_DIR / "downloads"
QUEUE_FILE = DATA_DIR / "queue.json"
LOG_FILE = DATA_DIR / "logs" / "tiktok-recorder.log"

# ── binary paths ──────────────────────────────────────────────────────────────
YTDLP_BIN = os.environ.get("YTDLP_BIN", "/app/yt-dlp")
DENO_BIN = os.environ.get("DENO_BIN", "/app/deno")
COOKIES_FILE = Path("/app/src/cookies.txt")

# tikwm endpoints
TIKWM_SUBMIT = "https://www.tikwm.com/api/video/task/submit"
TIKWM_RESULT = "https://www.tikwm.com/api/video/task/result"

# ── thumbnail paths ──────────────────────────────────────────────────────────
THUMBNAILS_DIR = DATA_DIR / "thumbnails"
THUMB_RECORDINGS_DIR = THUMBNAILS_DIR / "recordings"
THUMB_YT_RECORDINGS_DIR = THUMBNAILS_DIR / "yt_recordings"
THUMB_DOWNLOADS_DIR = THUMBNAILS_DIR / "downloads"

# ensure dirs exist
for _d in (
    RECORDINGS_DIR,
    YT_RECORDINGS_DIR,
    DOWNLOADS_DIR,
    THUMB_RECORDINGS_DIR,
    THUMB_YT_RECORDINGS_DIR,
    THUMB_DOWNLOADS_DIR,
    DATA_DIR / "logs",
):
    _d.mkdir(parents=True, exist_ok=True)
