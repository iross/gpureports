---
id: TASK-31
title: Rewrite collector to write directly to Parquet instead of SQLite
status: Done
assignee:
  - '@claude'
created_date: '2026-05-15 13:58'
updated_date: '2026-05-15 14:06'
labels: []
dependencies:
  - TASK-30
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The gpu_state collector (get_gpu_state_polars.py) currently appends to a monthly SQLite DB. The DuckDB+Parquet evaluation (task-30) showed 249x compression and 17x faster dashboard queries with Parquet. Since the monthly Parquet file is only ~5 MB, rewriting it on each collection cycle is trivial I/O. Replacing SQLite with direct Parquet writes eliminates the separate compaction step, removes the sqlite dependency from the write path, and shrinks storage from ~1 GB/month to ~5 MB/month.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Collector writes to gpu_state_YYYY-MM.parquet instead of gpu_state_YYYY-MM.db,Each collection cycle reads the existing monthly Parquet (if present), appends new rows, and atomically replaces the file (write to temp then rename) to avoid corruption on crash,dashboard/data.py reads from Parquet files instead of SQLite,Existing SQLite DBs remain readable for any historical gap period,Collector script passes the same correctness checks as before (same row shape and column types)
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add get_required_parquet_files and get_latest_timestamp_from_most_recent_parquet to gpu_utils_polars.py (with SQLite fallback per-month)\n2. Write tests/test_parquet_storage.py covering: collector write/append/atomic-rename, utils discovery functions, dashboard _query_dbs with Parquet and SQLite fallback\n3. Update get_gpu_state_polars.py: replace write_database with read-existing-parquet + concat + atomic rename\n4. Update dashboard/data.py: switch from SQLite to Parquet read path, import from gpu_utils_polars instead of gpu_utils\n5. Run full test suite and lint
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented direct Parquet writes in the collector. Key changes: (1) get_gpu_state_polars.py — replaced write_database(sqlite) with _write_parquet_atomic(): reads existing monthly Parquet, concats new rows, writes to .tmp then os.replace() atomically. (2) gpu_utils_polars.py — added get_required_parquet_files() (returns (path, fmt) tuples, prefers Parquet over SQLite per month), get_most_recent_parquet(), get_latest_timestamp_from_most_recent_parquet() with SQLite fallback. (3) dashboard/data.py — _query_dbs() now accepts list[tuple[str,str]] file_specs; Parquet path uses pl.scan_parquet with lazy pushdown, SQLite path unchanged; timestamp normalisation handles both Datetime and Utf8 strings; same change applied to get_opencap_users_data and get_open_capacity_jobs_data. (4) tests/test_parquet_storage.py — 20 new tests covering all three layers. Email report path (stats_data.py) unchanged; still reads SQLite; would need a follow-on task if collector is deployed to production and email path needs updating.
<!-- SECTION:NOTES:END -->
