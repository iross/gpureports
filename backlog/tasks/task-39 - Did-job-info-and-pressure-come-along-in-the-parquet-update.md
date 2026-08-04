---
id: TASK-39
title: >-
  Remove the Open Capacity Jobs dashboard tab (job_info did not come along in
  the parquet update)
status: Done
assignee:
  - '@claude'
created_date: '2026-07-09 12:58'
updated_date: '2026-08-04 18:34'
labels: []
dependencies:
  - TASK-27
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Investigation confirms: job_info was NOT migrated. job_info_YYYY-MM.db was only ever populated by the legacy get_gpu_state.py's collect_job_info(), which is not part of the new Parquet collector.py and is not copied into any Docker image — collection silently stopped at the parquet cutover (job_info_2026-07.db has 0 rows; prior months have tens of thousands). The 'Open Capacity Jobs' dashboard tab (get_open_capacity_jobs_data, /api/jobs) depends entirely on this dead data source, so it should be removed rather than rewired. job_pressure, by contrast, is confirmed still actively collected via SQLite (get_job_pressure.py) and is unaffected by this task. Per decision: remove the dashboard-side code only; leave suspicious_jobs.yaml and the job_info_*.db files in place.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 get_open_capacity_jobs_data and its helpers (_get_job_info_databases, _load_suspicious_criteria, _is_suspicious, _fetch_job_info) are removed from dashboard/data.py, along with the now-unused 'import re'
- [x] #2 The /api/jobs route is removed from dashboard/server.py
- [x] #3 The Open Capacity Jobs tab button and content are removed from dashboard/templates/index.html
- [x] #4 jobsData state, the /api/jobs fetch, renderJobs(), and its tab-switch branch are removed from dashboard/static/app.js
- [x] #5 The Jobs-tab CSS block is removed from dashboard/static/style.css
- [x] #6 dashboard/README.md no longer references the per-slot job table feature
- [x] #7 suspicious_jobs.yaml and job_info_*.db files are left in place, untouched
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Removed the Open Capacity Jobs tab and its backend, since job_info collection silently stopped at the Parquet cutover and the feature has been rendering on dead data.

- dashboard/data.py: removed get_open_capacity_jobs_data and its helpers (_get_job_info_databases, _load_suspicious_criteria, _is_suspicious, _fetch_job_info) and the now-unused "import re".
- dashboard/server.py: removed the /api/jobs route and its import.
- dashboard/templates/index.html: removed the tab button and #tab-jobs content.
- dashboard/static/app.js: removed jobsData, the /api/jobs fetch (Promise.all now only fetches heatmap/counts/opencap_users), renderJobs(), and the jobs tab-switch branch.
- dashboard/static/style.css: removed the Jobs-tab CSS block.
- dashboard/README.md: dropped the "Per-slot job table" bullet from the feature list.
- suspicious_jobs.yaml and job_info_*.db were left untouched per the task's decision.

Verified: full pytest suite passes (67/67 relevant tests, using a scratch venv pinned to the project's locked dependency versions since this sandbox's real .venv couldn't build). Loaded the live dashboard with real data (uvicorn + Playwright/Chromium): only Heatmap/Charts/Users tabs remain, #tab-jobs no longer exists in the DOM, /api/jobs returns 404, and switching between the remaining tabs works with no console errors.
<!-- SECTION:NOTES:END -->
