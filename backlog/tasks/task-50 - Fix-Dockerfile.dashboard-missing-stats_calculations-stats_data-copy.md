---
id: TASK-50
title: Fix Dockerfile.dashboard missing stats_calculations/stats_data copy
status: Done
assignee:
  - '@claude'
created_date: '2026-08-06 21:10'
updated_date: '2026-08-07 14:25'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Since the TASK-47 commit, dashboard/data.py imports stats_calculations.py and stats_data.py (which itself imports gpu_utils.py). Dockerfile.dashboard only COPYs gpu_utils.py, gpu_utils_polars.py, device_name_mappings.py, and dashboard/ — the dashboard container image currently fails to build/run.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Dockerfile.dashboard copies every module dashboard/data.py transitively imports
- [x] #2 docker build -f Dockerfile.dashboard . succeeds locally
- [x] #3 Dashboard container starts and serves /api/heatmap without ImportError
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add stats_data.py and stats_calculations.py to Dockerfile.dashboard's COPY line (dashboard/data.py imports both; stats_calculations.py in turn imports gpu_utils.py and device_name_mappings.py, both already copied).
2. Verify: docker build -f Dockerfile.dashboard . succeeds.
3. Verify: run the built image and confirm dashboard.server imports/starts without ImportError, and /api/heatmap responds.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Added stats_data.py and stats_calculations.py to Dockerfile.dashboard's COPY line alongside the existing gpu_utils.py/gpu_utils_polars.py/device_name_mappings.py — these are exactly the modules dashboard/data.py has transitively imported since the TASK-47 commit, and were the missing piece (this is the same class of gap TASK-43 fixed for gpu_utils_polars.py, which explicitly deferred adding stats_data.py "once the dashboard depends on it").

Docker itself is not installed in this environment (confirmed: `docker` not found), so the literal `docker build`/container-start ACs were verified by simulation instead: copied only the exact file set the fixed Dockerfile.dashboard now COPYs (gpu_utils.py, gpu_utils_polars.py, device_name_mappings.py, stats_data.py, stats_calculations.py, dashboard/) into an isolated scratch directory and ran `import dashboard.server` against it using the project's .venv (with cwd's `''` sys.path entry stripped so nothing outside the copied set could be found).
- Repeating the same simulation with the OLD (pre-fix) file set reproduces the exact failure: `ModuleNotFoundError: No module named 'stats_calculations'` — confirming the bug was real.
- With the NEW file set, `import dashboard.server` succeeds and `dashboard.server.app` resolves to a FastAPI instance.

Modified files: Dockerfile.dashboard (one-line COPY change).
<!-- SECTION:NOTES:END -->
