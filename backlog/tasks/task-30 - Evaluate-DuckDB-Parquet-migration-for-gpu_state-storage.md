---
id: TASK-30
title: Evaluate DuckDB + Parquet migration for gpu_state storage
status: Done
assignee:
  - '@claude'
created_date: '2026-05-14 14:28'
updated_date: '2026-05-14 21:42'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The gpu_state SQLite files total ~12 GB across 14 monthly files (350 MB–1.5 GB each). Dashboard queries load data via pl.read_database_uri over multiple files with in-Python filtering. DuckDB's columnar engine with Parquet storage would reduce file sizes 5–10× and enable single-query cross-month access with pushdown predicates, eliminating the per-file loop and concat in data.py.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Benchmark DuckDB vs current SQLite+Polars query time for a 7-day heatmap range spanning two monthly files,Estimate Parquet compression ratio on a sample gpu_state month,Define write strategy (nightly SQLite→Parquet compaction or direct Parquet append) that preserves the existing collector pattern,Document decision in backlog/decisions/ with recommendation to proceed or keep SQLite
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Examine SQLite schema and row counts to choose representative benchmark range\n2. Write benchmark script: DuckDB (attach SQLite) vs Polars+SQLite for 7-day heatmap spanning Mar/Apr boundary\n3. Export one month to Parquet via DuckDB; measure compression ratio\n4. Evaluate write strategies: nightly compaction vs direct append\n5. Document findings and recommendation in backlog/decisions/task-30-duckdb-parquet-evaluation.md
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Wrote benchmark script (analysis/benchmark_duckdb_vs_sqlite.py) against local SQLite DBs. Key findings: DuckDB+Parquet is 17x faster than current SQLite+Polars (0.13s vs 2.2s for 7-day window); compression is 249x (1.1 GB SQLite -> 4.6 MB Parquet for April, row counts identical). DuckDB-over-SQLite attach is actually 2.3x slower than current—ruled out. Recommended strategy: keep collector writing to SQLite unchanged; nightly cron compacts completed months to Parquet; dashboard routes per-month to Parquet vs SQLite. Decision documented in backlog/decisions/task-30-duckdb-parquet-evaluation.md.
<!-- SECTION:NOTES:END -->
