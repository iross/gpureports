---
id: DRAFT-2
title: Evaluate DuckDB SQLite-attach for job_pressure queries
status: Draft
assignee: []
created_date: '2026-07-31 20:48'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
job_pressure_*.db (2.2GB SQLite, dominated by a single 2.1GB May file) was never migrated to Parquet and is currently queried with raw sqlite3 in scripts/host_report.py. DuckDB is already a project dependency and already used elsewhere for parquet_scan(..., union_by_name=true); it can ATTACH a SQLite file directly and JOIN it against Parquet gpu_state in one query (already prototyped in analysis/benchmark_duckdb_vs_sqlite.py), which would let job_pressure data be queried alongside gpu_state without a full Parquet migration first. This directly extends task-39's original question ('did job_pressure come along?') with a concrete lower-cost path forward. Not a performance-driven change -- the one real bottleneck (FY-scale reports) was already fixed via polars lazy-scan -- this is about unifying query surface, and is a candidate for a larger future consolidation of the dashboard/report.py/stats_data.py query layers onto a single implementation.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A spike/prototype demonstrates DuckDB ATTACHing a job_pressure_*.db file and joining it against gpu_state Parquet data in one query
- [ ] #2 A decision is recorded on whether to adopt this pattern in scripts/host_report.py or defer further
<!-- AC:END -->
