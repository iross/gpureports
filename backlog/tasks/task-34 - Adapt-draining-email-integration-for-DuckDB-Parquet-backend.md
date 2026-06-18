---
id: TASK-34
title: Adapt draining email integration for DuckDB/Parquet backend
status: To Do
assignee: []
created_date: '2026-06-18 01:49'
labels:
  - reporting
  - migration
  - duckdb
dependencies:
  - TASK-30
references:
  - TASK-32 - Add draining GPU state to email reports
  - get_draining_data() in stats_data.py
  - calculate_allocation_usage_by_memory() GPU filtering
  - Real Slots table with drained column
priority: medium
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
After migrating gpu_state storage from SQLite to DuckDB/Parquet (TASK-30), adapt the draining GPU state email integration (TASK-32) to work with the new backend. The draining data fetching currently uses SQLite queries and needs to be refactored to query DuckDB or parquet files instead.

Also ensure the Real Slots and Backfill tables maintain:
1. The drained GPU column that was added to provide visibility into drained vs claimed GPUs
2. Consistent GPU filtering across both device-based and memory-category-based reporting (exclude GTX 1080, P100, Quadro, A30, A40)
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Draining data fetching works with DuckDB/Parquet backend instead of SQLite
- [ ] #2 Draining statistics calculation continues to work without changes
- [ ] #3 Email reports show draining status correctly with new backend
- [ ] #4 Real Slots and Memory Category tables show aligned GPU counts with consistent filtering applied
- [ ] #5 All tests pass with new data format
<!-- AC:END -->
