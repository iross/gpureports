---
id: TASK-49.1
title: >-
  Unify dashboard, report.py, and stats_data.py onto one canonical query
  pipeline
status: To Do
assignee: []
created_date: '2026-07-31 21:00'
updated_date: '2026-08-06 21:11'
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

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Pre-check: grep justfile/cron/scripts for any caller of report.py's group_by_device=False path before deciding whether to migrate or leave stats_calculations.py's pandas-tail functions alone.
2. Pin 2-3 fixed historical time windows (a plain day, a month-boundary-spanning window to exercise multi-file concat/SQLite-fallback, a window covering a PreventJobsReason period) as literal constants in a throwaway script. Capture JSON snapshots of get_heatmap_data/get_counts_data/get_opencap_users_data and report.py main() output for those windows on main, BEFORE any change.
3. dashboard/data.py: replace _query_dbs/_dedup_and_bucket/_classify_states with scan_time_filtered() + prepare_frames(), plus a thin mapping layer deriving the existing 6 STATE_CODES from prepare_frames's _pp_prio/_is_bf/state columns (fold CHTC-owned into the existing prioritized bucket; check the interactive-slot delta via the snapshot diff rather than assume it's negligible). Switch masked-host loading to gpu_utils.load_host_exclusions() instead of dashboard's own _load_masked_hosts YAML reader.
4. get_counts_data: source its primary/backfill counts from PreparedFrames.raw and PreparedFrames.raw_bf instead of its own load+filter pipeline, preserving the deliberate no-dedup behavior (keep the existing explanatory comment).
5. get_opencap_users_data: replace its standalone file-loading loop and inline dedup with a filter over PreparedFrames.dedup.
6. report.py: delete get_time_filtered_data/get_multi_db_data in favor of stats_data.scan_time_filtered(); delete report.py's own calculate_allocation_usage_by_device_enhanced/calculate_allocation_usage_by_memory in favor of stats_calculations's versions, collapsing main()'s group_by_device=True path to one prepare_frames() call.
7. Re-run the snapshot script on the refactored branch; diff against the BEFORE JSON. Any difference must be either fixed or explicitly documented as a deliberate change (expect one: dashboard's dedup currently truncates-to-bucket before deduping, while prepare_frames dedups at raw timestamp then truncates -- unifying this is a real, small, intentional behavior fix).
8. Rewrite tests/test_parquet_storage.py::TestDashboardQueryDbs and tests/test_dashboard_data.py against the new public entry points (they currently assert on the private _query_dbs/_dedup_and_bucket signatures being removed). Add a small unit test for the state-code mapping table, and fold the snapshot comparison into a permanent regression test (tests/test_report_equivalence.py's synthetic-fixture pattern is the template).
9. Land dashboard changes and report.py changes as separate commits/PRs given their different blast radii; verify report.py via 'emailer.sh test' before the next scheduled cron fire.
<!-- SECTION:PLAN:END -->
