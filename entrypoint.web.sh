#!/bin/bash
ln -sf /data/logs/tiktok-recorder.log /app/tiktok-recorder.log
exec uvicorn web.server:app --host 0.0.0.0 --port "${WEBUI_PORT:-8000}"