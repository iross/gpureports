---
id: TASK-30
title: Evaluate DuckDB + Parquet migration for gpu_state storage
status: To Do
assignee: []
created_date: '2026-05-14 14:28'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The gpu_state SQLite files total ~12 GB across 14 monthly files (350 MB–1.5 GB each). Dashboard queries load data via pl.read_database_uri over multiple files with in-Python filtering. DuckDB's columnar engine with Parquet storage would reduce file sizes 5–10× and enable single-query cross-month access with pushdown predicates, eliminating the per-file loop and concat in data.py.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Benchmark DuckDB vs current SQLite+Polars query time for a 7-day heatmap range spanning two monthly files,Estimate Parquet compression ratio on a sample gpu_state month,Define write strategy (nightly SQLite→Parquet compaction or direct Parquet append) that preserves the existing collector pattern,Document decision in backlog/decisions/ with recommendation to proceed or keep SQLite
<!-- AC:END -->
