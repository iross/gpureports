# GPU State Dashboard

**Status: image builds and pushes on every merge to main
(`hub.opensciencegrid.org/xdd/gpu_dashboard`, via `Dockerfile.dashboard`), but the
actual Kubernetes Deployment/Service rollout is still open (TASK-48) — not yet
publicly accessible.**

A FastAPI web dashboard for real-time GPU state monitoring. Reads gpu_state Parquet
files (or, for months predating the Parquet migration, SQLite) through the same
canonical pipeline as the email reports (`classify_slots.prepare_frames()`).

## What it does

- Heatmap of GPU utilization across the cluster over a selectable time window,
  auto-refreshing on an interval
- GPU counts by category over time
- Anonymized per-user open-capacity usage chart

## How to run locally

From the repo root:

```bash
just dashboard   # uv run uvicorn dashboard.server:app --reload --port 8051
```

Then open `http://localhost:8051`.

The app reads `gpu_state_*.parquet` (or `.db`) files from the repo root by default.
You'll need at least one such file present.

## What still needs work before deployment

- Access control (currently open, no auth)
- The Kubernetes Deployment + Service rollout itself (TASK-48 AC #3-5) — same-node
  affinity to the collector/emailer CronJobs plus a read-only PVC mount is the chosen
  approach, per
  [backlog/decisions/task-48-dashboard-pvc-concurrency.md](../backlog/decisions/task-48-dashboard-pvc-concurrency.md)
- CI should build/push the dashboard image only when dashboard source changes, not on
  every push to main (TASK-48 AC #4)
- Mobile/narrow viewport layout

## Structure

```
dashboard/
├── server.py       # FastAPI app, API routes, response caching
├── data.py         # Parquet/SQLite loading, dedup/classify via classify_slots.prepare_frames()
├── templates/      # Jinja2 HTML templates
└── static/         # CSS, JS
```
