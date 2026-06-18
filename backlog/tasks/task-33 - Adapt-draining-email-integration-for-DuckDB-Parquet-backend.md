---
id: TASK-33
title: Adapt draining email integration for DuckDB/Parquet backend
status: To Do
assignee: []
created_date: '2026-06-18 00:58'
updated_date: '2026-06-18 01:17'
labels:
  - reporting
  - migration
  - duckdb
dependencies:
  - TASK-30
references:
  - TASK-32 - Add draining GPU state to email reports
  - Real Slots table drained column
  - get_draining_data() in stats_data.py
priority: medium
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
After migrating gpu_state storage from SQLite to DuckDB/Parquet (TASK-30), adapt the draining GPU state email integration (TASK-32) to work with the new backend. The draining data fetching currently uses SQLite queries and needs to be refactored to query DuckDB or parquet files instead.

Also ensure the Real Slots and Backfill tables maintain the drained GPU column that was added to provide visibility into drained vs claimed GPUs.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Draining data fetching works with DuckDB/Parquet backend instead of SQLite
- [ ] #2 Draining statistics calculation continues to work without changes
- [ ] #3 Email reports show draining status correctly with new backend
- [ ] #4 All tests pass with new data format
<!-- AC:END -->
