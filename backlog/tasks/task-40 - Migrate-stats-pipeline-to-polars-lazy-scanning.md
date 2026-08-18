---
id: TASK-40
title: Migrate stats pipeline to polars lazy scanning
status: Done
assignee:
  - '@claude'
created_date: '2026-07-14 20:55'
updated_date: '2026-07-14 21:34'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The fiscal-year report (--hours-back 8760) is unusably slow because the full year (~51M rows) is materialized as a pandas DataFrame (~36GB of object-dtype strings on a 19GB machine, causing swap), then repeatedly copied and re-filtered per device/class. Polars lazy scanning over the Parquet files with pushdown and single-pass group_by aggregations removes the materialization and the quadratic per-bucket loops.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 FY report (8760h) completes in under 5 minutes
- [x] #2 Report output matches the pandas implementation on 24h and 7d validation windows
- [x] #3 Existing tests pass
- [x] #4 Peak memory stays within machine RAM (no swap)
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Capture baseline outputs (24h, 7d) from current pandas implementation
2. Rewrite stats_data.py to expose polars lazy scans of gpu_state parquet (replace DuckDB->pandas materialization)
3. Rewrite calculation functions in stats_calculations.py as polars group_by aggregations over shared class-filtered lazy frames (dedup ranking computed once)
4. Update usage_stats.py orchestration; keep stats_reporting consuming plain dicts
5. Validate parity against baselines; run tests + ruff; benchmark FY report
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Replaced the pandas materialization pipeline with polars lazy scans. stats_data.py: added scan_time_filtered() (lazy scan of the parquet glob with pushdown), ported get_draining_data to a polars anti-join, get_latest_timestamp to polars, kept get_time_filtered_data (DuckDB->pandas) for small-window/legacy consumers, removed the DataFrame caches. stats_calculations.py: added prepare_frames(), which computes the duplicate-slot ranking once for the whole window (equivalent to the per-subset pandas dedup because each GPU maps to one machine/device) and collects two compact frames (dedup representatives + backfill rows) reused by all calculations; rewrote device/memory/H200/backfill-user/zero-active/prevent-jobs/draining/monthly as group_by aggregations, eliminating the per-bucket Python loops (the prevent-jobs one was O(buckets x rows), ~35k buckets at year scale). usage_stats.py: run_analysis drives the lazy pipeline for group-by-device and monthly; timeseries/non-device paths still use pandas. Deleted dead code: calculate_performance_usage, calculate_unique_cluster_totals_from_raw_data, raw_data passthrough.

Results: FY report (8760h, 51.4M rows) 34s wall / 9.7GB peak / 0 swaps, vs pandas needing ~36GB on a 19GB machine (swap-bound). 24h/168h windows: 0.1s/0.4s vs 4s/22s. Output parity verified against pandas baselines on 24h and 168h: every value matches except memory_stats Unknown avg_total_available, an intentional fix - pandas double-counted 3 dmorgan2000 GPUs (memory reported on one slot, null on another) in both Unknown and their real category; each GPU now counts in exactly one category. metadata filtered_hosts_info is now one window-level entry instead of per-filter-call entries (it only feeds a records-excluded sum). monthly unique_intervals now reports the real bucket count (was always 0). Tests: prevent-jobs tests updated to the PreparedFrames API; 80 passed.
<!-- SECTION:NOTES:END -->
