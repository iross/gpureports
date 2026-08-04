---
id: TASK-49.1
title: >-
  Unify dashboard, report.py, and stats_data.py onto one canonical query
  pipeline
status: To Do
assignee: []
created_date: '2026-07-31 21:00'
labels: []
dependencies: []
parent_task_id: TASK-49
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
dashboard/data.py, report.py (via gpu_utils_polars.py), and stats_data.py/stats_calculations.py each independently implement loading, host-masking, deduplication, and classification of gpu_state Parquet data, and they've already been shown to disagree (see TASK-47 on dedup rank order). Rebuild dashboard/data.py and report.py on top of stats_data.py's scan_time_filtered() and stats_calculations.py's prepare_frames() instead of maintaining separate implementations, so there is exactly one place that defines 'how gpu_state rows become classified GPU states.'
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 dashboard/data.py's heatmap/counts/users queries are built on scan_time_filtered()/prepare_frames() rather than its own _query_dbs/_dedup_and_bucket/_classify_states
- [ ] #2 report.py uses the same canonical pipeline rather than its own separate load path
- [ ] #3 Existing dashboard and report behavior (heatmap rendering, email report output) is unchanged for a fixed sample time range, verified by comparing output before/after
- [ ] #4 tests/test_pandas_polars_parity.py and tests/test_parquet_storage.py still pass
<!-- AC:END -->
