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
0. Correction from initial planning: the live email cron (emailer.sh/_emailer.sh) calls usage_stats.py, not report.py, and usage_stats.py's only cron-exercised path (group_by_device=True, always passed) is already on scan_time_filtered()/prepare_frames(). report.py is invoked only manually via `just last-day`/`last-day-html`/`last-hour` -- no cron caller. Verified: report.py's group_by_device default is True and no caller anywhere in the repo passes --group-by-device=False, so report.py's group_by_device=False branch has zero live callers and can likely be deleted outright rather than migrated (confirm with a final grep before deleting).
1. Pre-check: confirm no live caller of usage_stats.py's group_by_device=False/timeseries paths (get_time_filtered_data + calculate_allocation_usage_enhanced/calculate_time_series_usage) exists beyond scripts/plot_usage_stats.py and scripts/plot_example.py (both already identified) before deciding whether stats_calculations.py's pandas tail needs migrating or can stay a documented holdout -- this feeds TASK-49.4, not 49.1 itself.
2. Pin 2-3 fixed historical time windows (a plain day, a month-boundary-spanning window to exercise multi-file concat/SQLite-fallback, a window covering a PreventJobsReason period) as literal constants in a throwaway script. Capture JSON snapshots of get_heatmap_data/get_counts_data/get_opencap_users_data and report.py main() output for those windows on main, BEFORE any change.
3. dashboard/data.py: replace _query_dbs/_dedup_and_bucket/_classify_states with scan_time_filtered() + prepare_frames(), plus a thin mapping layer deriving the existing 6 STATE_CODES from prepare_frames's _pp_prio/_is_bf/state columns (fold CHTC-owned into the existing prioritized bucket; check the interactive-slot delta via the snapshot diff rather than assume it's negligible). Switch masked-host loading to gpu_utils.load_host_exclusions() instead of dashboard's own _load_masked_hosts YAML reader.
4. get_counts_data: source its primary/backfill counts from PreparedFrames.raw and PreparedFrames.raw_bf instead of its own load+filter pipeline, preserving the deliberate no-dedup behavior (keep the existing explanatory comment).
5. get_opencap_users_data: replace its standalone file-loading loop and inline dedup with a filter over PreparedFrames.dedup.
6. report.py: delete get_time_filtered_data/get_multi_db_data in favor of stats_data.scan_time_filtered(); delete report.py's own calculate_allocation_usage_by_device_enhanced/calculate_allocation_usage_by_memory in favor of stats_calculations's versions, collapsing main()'s group_by_device=True path to one prepare_frames() call. Delete the now-dead group_by_device=False branch per step 0's finding. Fix report.py's stale module docstring ("For reporting, HTML generation, and email functions, see usage_stats.py (pandas version)") -- usage_stats.py's live path is already polars-based via prepare_frames, not pandas.
7. Re-run the snapshot script on the refactored branch; diff against the BEFORE JSON. Any difference must be either fixed or explicitly documented as a deliberate change (expect one: dashboard's dedup currently truncates-to-bucket before deduping, while prepare_frames dedups at raw timestamp then truncates -- unifying this is a real, small, intentional behavior fix).
8. Rewrite tests/test_parquet_storage.py::TestDashboardQueryDbs and tests/test_dashboard_data.py against the new public entry points (they currently assert on the private _query_dbs/_dedup_and_bucket signatures being removed). Add a small unit test for the state-code mapping table, and fold the snapshot comparison into a permanent regression test (tests/test_report_equivalence.py's synthetic-fixture pattern is the template).
9. Land dashboard changes and report.py changes as separate commits/PRs given their different blast radii. Verify report.py by running `just last-day`/`just last-hour` manually and diffing against the pinned snapshot (no cron involvement -- report.py has no automated caller). The live email cron (usage_stats.py via emailer.sh) is unaffected by this task and needs no separate verification here.
<!-- SECTION:PLAN:END -->
