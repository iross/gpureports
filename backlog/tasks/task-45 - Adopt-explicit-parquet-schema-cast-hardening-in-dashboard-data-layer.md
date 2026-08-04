---
id: TASK-45
title: Adopt explicit parquet schema/cast hardening in dashboard data layer
status: Done
assignee:
  - iaross@wisc.edu
created_date: '2026-07-31 20:48'
updated_date: '2026-08-04 19:39'
labels: []
dependencies:
  - TASK-27
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
dashboard/data.py's pl.scan_parquet() calls in _query_dbs and get_opencap_users_data scan files individually with no explicit schema, which currently avoids the multi-file SchemaError that stats_data.py's GPU_STATE_SCHEMA / missing_columns='insert' / nanosecond-cast tolerance (commits 0d0ef3e, afd276d) fixed for the reporting pipeline -- but only by accident, since it never combines files into one glob scan. This leaves the dashboard's parquet reads fragile to future schema evolution (new columns, mixed timestamp precision) with no tolerance built in. Reuse the same schema/cast-option constants instead of maintaining separate, unguarded scan calls.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 dashboard/data.py's pl.scan_parquet() calls pass the same GPU_STATE_SCHEMA and cast_options used in stats_data.py, imported rather than re-declared
- [x] #2 A parquet file with columns in a different order, or a missing newer column (e.g. PreventJobsReason), loads without error via the dashboard's API
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Import GPU_STATE_SCHEMA and _SCAN_CAST_OPTIONS from stats_data.py into dashboard/data.py\n2. Pass them to the pl.scan_parquet() calls in _query_dbs and get_opencap_users_data\n3. Test with a synthetic parquet file with reordered/missing columns to confirm no error
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Imported GPU_STATE_SCHEMA and SCAN_CAST_OPTIONS from stats_data.py into dashboard/data.py and passed them to both pl.scan_parquet() call sites (_query_dbs, get_opencap_users_data), replacing the unguarded per-file scans. Renamed stats_data.py's _SCAN_CAST_OPTIONS to SCAN_CAST_OPTIONS (public) since it's now used cross-module -- it had no other consumers, so this was a safe rename.

Added tests/test_dashboard_data.py exercising _query_dbs against two synthetic parquet files: one missing PrioritizedProjects/PreventJobsReason with reordered columns (simulating a pre-migration file), one full-schema file. Verified the test actually catches the regression by reverting the fix (git stash) and confirming it fails with ColumnNotFoundError, then passes once restored.
<!-- SECTION:NOTES:END -->
