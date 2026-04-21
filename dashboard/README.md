# GPU State Dashboard

**Status: Work in progress — not currently deployed or publicly accessible.**

A FastAPI web dashboard for real-time GPU state monitoring. Reads the same
`gpu_state_YYYY-MM.db` databases as the email reports.

## What it does (when finished)

- Heatmap of GPU utilization across the cluster over a selectable time window
- Per-slot job table for open capacity slots
- GPU counts by category over time

## How to run locally

From the repo root:

```bash
uvicorn dashboard.server:app --reload
```

Then open `http://localhost:8000`.

The app reads database files from the repo root by default. You'll need at least
one `gpu_state_YYYY-MM.db` file present.

## What still needs work before deployment

- Access control (currently open, no auth)
- Stable deployment target and process (systemd unit or container)
- Auto-refresh so it stays current without manual reload
- Mobile/narrow viewport layout

## Structure

```
dashboard/
├── server.py       # FastAPI app, API routes, response caching
├── data.py         # Data loading from SQLite DBs
├── templates/      # Jinja2 HTML templates
└── static/         # CSS, JS
```
