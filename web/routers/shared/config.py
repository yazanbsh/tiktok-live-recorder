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
RECORDINGS_DIR = DATA_DIR / "recordings"
DOWNLOADS_DIR = DATA_DIR / "downloads"
QUEUE_FILE = DATA_DIR / "queue.json"
LOG_FILE = DATA_DIR / "logs" / "tiktok-recorder.log"

# tikwm endpoints
TIKWM_SUBMIT = "https://www.tikwm.com/api/video/task/submit"
TIKWM_RESULT = "https://www.tikwm.com/api/video/task/result"

# ensure dirs exist
for _d in (RECORDINGS_DIR, DOWNLOADS_DIR, DATA_DIR / "logs"):
    _d.mkdir(parents=True, exist_ok=True)
