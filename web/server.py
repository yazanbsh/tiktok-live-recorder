"""
TikTok Live Recorder — Web UI Backend
Place this file at:  <repo-root>/web/server.py
Run with:  uv run uvicorn web.server:app --host 0.0.0.0 --port 8000 --reload
"""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from web.routers.shared.config import STATIC_DIR
from web.routers.shared.files import router as files_router
from web.routers.tiktok.watchlist import (
    router as tiktok_watchlist_router,
    startup_watchlist,
)
from web.routers.tiktok.downloader import router as tiktok_downloader_router
from web.routers.youtube.watchlist import (
    router as yt_watchlist_router,
    startup_yt_watchlist,
)

app = FastAPI(title="TikTok Live Recorder", version="2.0.0")

# ── Include routers ───────────────────────────────────────────────────────────
app.include_router(files_router)
app.include_router(tiktok_watchlist_router)
app.include_router(tiktok_downloader_router)
app.include_router(yt_watchlist_router)


# ── Startup ───────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    startup_watchlist()
    startup_yt_watchlist()


# ── Static files + frontend ───────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
