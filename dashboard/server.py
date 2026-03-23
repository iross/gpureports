"""FastAPI server for the GPU state dashboard."""

import datetime
import hashlib
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from dashboard.data import get_counts_data, get_heatmap_data, get_open_capacity_jobs_data

BASE_DIR = str(Path(__file__).resolve().parent.parent)
DASHBOARD_DIR = Path(__file__).resolve().parent

app = FastAPI(title="GPU State Dashboard")

# Mount static files and templates
app.mount("/static", StaticFiles(directory=str(DASHBOARD_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(DASHBOARD_DIR / "templates"))

# Simple in-memory cache
_cache: dict[str, tuple[float, dict]] = {}
CACHE_TTL = 300  # 5 minutes


def _cache_key(prefix: str, start: str | None, end: str | None, bucket: int) -> str:
    raw = f"{prefix}|{start}|{end}|{bucket}"
    return hashlib.md5(raw.encode()).hexdigest()


def _parse_params(
    start: str | None, end: str | None, bucket_minutes: int
) -> tuple[datetime.datetime | None, datetime.datetime | None, int]:
    start_dt = datetime.datetime.fromisoformat(start) if start else None
    end_dt = datetime.datetime.fromisoformat(end) if end else None
    return start_dt, end_dt, bucket_minutes


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/heatmap")
async def heatmap(
    start: str | None = Query(None, description="ISO datetime start, e.g. 2026-01-15T00:00"),
    end: str | None = Query(None, description="ISO datetime end, e.g. 2026-01-16T00:00"),
    bucket_minutes: int = Query(15, description="Time bucket size in minutes"),
):
    key = _cache_key("heatmap", start, end, bucket_minutes)
    now = datetime.datetime.now().timestamp()
    if key in _cache:
        cached_time, cached_data = _cache[key]
        if now - cached_time < CACHE_TTL:
            return JSONResponse(content=cached_data)

    start_dt, end_dt, bucket_minutes = _parse_params(start, end, bucket_minutes)
    data = get_heatmap_data(start=start_dt, end=end_dt, bucket_minutes=bucket_minutes, base_dir=BASE_DIR)
    _cache[key] = (now, data)
    return JSONResponse(content=data)


@app.get("/api/jobs")
async def jobs():
    key = _cache_key("jobs", None, None, 0)
    now = datetime.datetime.now().timestamp()
    if key in _cache:
        cached_time, cached_data = _cache[key]
        if now - cached_time < CACHE_TTL:
            return JSONResponse(content=cached_data)

    data = get_open_capacity_jobs_data(base_dir=BASE_DIR)
    _cache[key] = (now, data)
    return JSONResponse(content=data)


@app.get("/api/counts")
async def counts(
    start: str | None = Query(None, description="ISO datetime start, e.g. 2026-01-15T00:00"),
    end: str | None = Query(None, description="ISO datetime end, e.g. 2026-01-16T00:00"),
    bucket_minutes: int = Query(15, description="Time bucket size in minutes"),
):
    key = _cache_key("counts", start, end, bucket_minutes)
    now = datetime.datetime.now().timestamp()
    if key in _cache:
        cached_time, cached_data = _cache[key]
        if now - cached_time < CACHE_TTL:
            return JSONResponse(content=cached_data)

    start_dt, end_dt, bucket_minutes = _parse_params(start, end, bucket_minutes)
    data = get_counts_data(start=start_dt, end=end_dt, bucket_minutes=bucket_minutes, base_dir=BASE_DIR)
    _cache[key] = (now, data)
    return JSONResponse(content=data)
