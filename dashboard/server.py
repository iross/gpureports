"""FastAPI server for the GPU state dashboard."""

import datetime
import hashlib
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from dashboard.data import get_heatmap_data

BASE_DIR = str(Path(__file__).resolve().parent.parent)
DASHBOARD_DIR = Path(__file__).resolve().parent

app = FastAPI(title="GPU State Dashboard")

# Mount static files and templates
app.mount("/static", StaticFiles(directory=str(DASHBOARD_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(DASHBOARD_DIR / "templates"))

# Simple in-memory cache
_cache: dict[str, tuple[float, dict]] = {}
CACHE_TTL = 300  # 5 minutes


def _cache_key(start: str | None, end: str | None, bucket: int) -> str:
    raw = f"{start}|{end}|{bucket}"
    return hashlib.md5(raw.encode()).hexdigest()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/heatmap")
async def heatmap(
    start: str | None = Query(None, description="ISO datetime start, e.g. 2026-01-15T00:00"),
    end: str | None = Query(None, description="ISO datetime end, e.g. 2026-01-16T00:00"),
    bucket_minutes: int = Query(15, description="Time bucket size in minutes"),
):
    # Check cache
    key = _cache_key(start, end, bucket_minutes)
    now = datetime.datetime.now().timestamp()
    if key in _cache:
        cached_time, cached_data = _cache[key]
        if now - cached_time < CACHE_TTL:
            return JSONResponse(content=cached_data)

    # Parse datetimes
    start_dt = None
    end_dt = None
    if start:
        start_dt = datetime.datetime.fromisoformat(start)
    if end:
        end_dt = datetime.datetime.fromisoformat(end)

    data = get_heatmap_data(
        start=start_dt,
        end=end_dt,
        bucket_minutes=bucket_minutes,
        base_dir=BASE_DIR,
    )

    # Cache result
    _cache[key] = (now, data)

    return JSONResponse(content=data)
