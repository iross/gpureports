---
id: TASK-43
title: Fix Dockerfile.dashboard missing gpu_utils_polars import
status: Done
assignee:
  - iaross@wisc.edu
created_date: '2026-07-31 20:48'
updated_date: '2026-08-04 19:03'
labels: []
dependencies:
  - TASK-27
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
dashboard/data.py imports from gpu_utils_polars.py (added in the SQLite-to-Parquet migration, commit acaa8d1), but Dockerfile.dashboard only COPYs gpu_utils.py and device_name_mappings.py. A freshly built dashboard image fails at import time before it can serve any requests.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Dockerfile.dashboard copies gpu_utils_polars.py (and stats_data.py, once the dashboard depends on it per the schema-hardening task)
- [x] #2 docker build -f Dockerfile.dashboard . succeeds and the resulting image starts without an ImportError
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add gpu_utils_polars.py to Dockerfile.dashboard COPY line\n2. Verify docker build succeeds and image starts without ImportError
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Added gpu_utils_polars.py to the Dockerfile.dashboard COPY line alongside gpu_utils.py and device_name_mappings.py. stats_data.py is not yet imported by the dashboard, so it wasn't added (per AC #1's conditional). Docker wasn't available in this environment to run the literal build, so verification was done by confirming 'import dashboard.server' resolves cleanly with only the files the Dockerfile now copies (gpu_utils.py, gpu_utils_polars.py, device_name_mappings.py, dashboard/) plus installed deps -- the same set of local modules the image assembles.
<!-- SECTION:NOTES:END -->
