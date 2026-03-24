"""
TikTok Live Recorder — Web UI Backend
Place this file at:  <repo-root>/web/server.py
Run with:  uv run uvicorn web.server:app --host 0.0.0.0 --port 8000 --reload
"""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from routers.shared.config import STATIC_DIR
from routers.shared.files import router as files_router
from routers.tiktok.watchlist import (
    router as tiktok_watchlist_router,
    startup_watchlist,
)
from routers.tiktok.downloader import router as tiktok_downloader_router

app = FastAPI(title="TikTok Live Recorder", version="2.0.0")

# ── Include routers ───────────────────────────────────────────────────────────
app.include_router(files_router)
app.include_router(tiktok_watchlist_router)
app.include_router(tiktok_downloader_router)


# ── Startup ───────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    startup_watchlist()


# ── Static files + frontend ───────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
