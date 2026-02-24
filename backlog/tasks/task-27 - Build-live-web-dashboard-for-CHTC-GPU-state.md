---
id: task-27
title: Build live web dashboard for CHTC GPU state
status: In Progress
assignee:
  - '@claude'
created_date: '2026-02-24 19:29'
updated_date: '2026-02-24 21:53'
labels:
  - dashboard
  - web
  - kubernetes
dependencies:
  - task-2.3
priority: medium
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Once task-2.3 is complete, GPU state database files will be available on a Kubernetes cluster. Build a web application that reads directly from those .db files to provide a near-live view of GPU utilization, allocation, and health across CHTC. The dashboard should update frequently (e.g., every few minutes, matching the data collection cadence) and give operators a quick visual overview without needing to run CLI reports.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Dashboard displays current GPU state (claimed/unclaimed, slot type, device type) pulled live from the SQLite db files
- [x] #2 Data refreshes automatically at an interval matching the collection cadence (no manual reload required)
- [x] #3 View is filterable or navigable by GPU category (Prioritized, Open Capacity, Backfill) and/or host
- [ ] #4 Deployable as a Kubernetes workload (Deployment + Service) that mounts the db files as a volume
- [x] #5 Page loads and renders in under 3 seconds for a typical dataset size
- [x] #6 Dashboard includes total count charts, similar to those generated in scripts/plot_gpu_availability.py
- [ ] #7 A minimal DOckerfile should be created, with CI built so that when the src code is updated, the Docker image is rebuilt and pushed to a registry. This should only trigger if the src code for the dashboard is updated, not ALL pushes to main. 
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add _prepare_bucketed() helper to dashboard/data.py to share load+mask+dedup+classify logic\n2. Add get_counts_data() to dashboard/data.py for time-series GPU counts per category\n3. Add /api/counts endpoint to dashboard/server.py\n4. Add tab bar (Heatmap|Charts), category filter buttons, auto-refresh, and Chart.js count charts to frontend\n5. Update style.css for tabs, category filter, charts layout\n6. Create Dockerfile (local only, CI deferred until k8s piece is ready)
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
## Implementation Notes

### Approach
Built a FastAPI server (`dashboard/server.py`) with two API endpoints (`/api/heatmap`, `/api/counts`) feeding a canvas-based heatmap and Chart.js stacked area charts, served from a single-page Jinja2 template.

### Features implemented
- **Heatmap tab**: canvas-rendered GPU state grid, one row per GPU per machine, time buckets on x-axis. Color-coded by state (idle-prioritized, idle-shared, busy-prioritized, busy-shared, busy-backfill, n/a). Tooltip on hover shows machine, GPU ID, device name, time, and state.
- **Charts tab**: three stacked area charts (Prioritized, Open Capacity, Backfill) showing total vs. claimed GPU counts over time via Chart.js 4.4.7.
- **Category filter**: All / Prioritized / Open Cap. / Backfill buttons filter heatmap rows to machines in that category.
- **Machine filter**: free-text filter on hostname.
- **Auto-refresh**: 5-minute countdown with live display; refreshes both API endpoints in parallel.
- **Custom time range**: datetime-local inputs + Go button; presets for last 6h / 24h / 7d.
- **Sticky machine labels**: left column shows machine hostnames that stick to viewport top while scrolling the heatmap vertically.
- **Dockerfile**: `Dockerfile.dashboard` using python:3.13-slim + uv, copies only dashboard files. Port 8051.

### Data layer (`dashboard/data.py`)
- `STATE_CODES` includes `idle_backfill: 6` (same heatmap color as idle_shared, but counted separately in charts).
- `_prepare_bucketed()`: shared helper for heatmap — loads DBs, masks hosts, deduplicates by time bucket + AssignedGPUs (keeps highest-rank slot), classifies states.
- `get_counts_data()`: counts each category from raw pre-dedup slot rows to avoid the dedup artifact where a GPU disappears from its primary category when its backfill slot wins rank. Each slot type counted independently with `n_unique(AssignedGPUs)` per bucket.

### Technical decisions
- Counts use raw (pre-dedup) rows so a GPU can appear in both prioritized and backfill simultaneously (correct HTCondor behavior: idle primary slot → backfill slot also exists).
- Heatmap continues to use dedup (show the "most important" state per GPU per bucket).
- 5-minute in-memory cache per (endpoint, start, end, bucket) key to avoid re-querying DBs on every request.
- AC#4 (K8s deployment) and AC#7 (CI/registry) deferred pending k8s infrastructure.

### Modified/added files
- `dashboard/data.py` — major refactor + new `get_counts_data()`
- `dashboard/server.py` — added `/api/counts` endpoint and cache key prefix
- `dashboard/templates/index.html` — tab bar, category filter, charts tab, Chart.js CDN
- `dashboard/static/app.js` — complete rewrite: fetchAll, tab switching, category filter, Chart.js rendering, auto-refresh, sticky labels
- `dashboard/static/style.css` — tab, cat-filter, charts-container, sticky label styles
- `Dockerfile.dashboard` — new minimal image
<!-- SECTION:NOTES:END -->
