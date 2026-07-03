# Decision: DuckDB + Parquet migration for gpu_state storage

**Date:** 2026-05-14  
**Status:** Accepted  
**Task:** TASK-30

## Context

The `gpu_state` SQLite files total ~12 GB across 13 monthly files (349 MB–1.5 GB each).
The live dashboard queries up to two monthly files per request via `pl.read_database_uri`,
concatenates them in Python, and then filters by time range. The collector script appends
rows to the current month's SQLite file every few minutes.

The question: would DuckDB + Parquet reduce storage and improve dashboard query latency
enough to justify the migration?

## Benchmark methodology

- **Test machine:** macOS (Apple Silicon), files on local SSD
- **Query window:** 7-day heatmap range (2026-03-28 → 2026-04-04), spanning two monthly DBs
- **Warm runs:** 5 per approach (first run discards any cold-read penalty)
- **Columns selected:** Name, AssignedGPUs, State, PrioritizedProjects, Machine, GPUs_DeviceName, timestamp
- **Parquet encoding:** ZSTD compression, 100k row groups, ordered by timestamp
- **Benchmark script:** `analysis/benchmark_duckdb_vs_sqlite.py`

## Results

### Query latency

| Approach | Mean (s) | Best (s) | Rows returned |
|---|---|---|---|
| SQLite + Polars — current, 2 DBs | 1.60 | 1.38 | 529,366 |
| DuckDB attach SQLite — 2 DBs, no Parquet | 3.60 | 2.66 | 529,366 |
| **DuckDB Parquet ZSTD — 1 DB, 7-day window** | **0.13** | **0.12** | **1,203,146** |
| SQLite + Polars — same single-DB 7-day window | 2.16 | 1.91 | 1,203,146 |

Key takeaway: DuckDB querying Parquet is **~17× faster** than the current SQLite+Polars path
on a same-window comparison. DuckDB attaching SQLite directly is *slower* than the current
approach (2.3× overhead for the SQLite read path through DuckDB); it is not a viable migration
target.

### Storage (April 2026, 4,887,205 rows)

| Format | Size | Ratio |
|---|---|---|
| SQLite | 1,147.8 MB | 1× |
| Parquet (ZSTD) | 4.6 MB | **249× smaller** |

Extrapolated across 13 months (~12 GB SQLite):

- Parquet total: **~50 MB**
- Savings: ~11.95 GB (~99.6% reduction)

The extreme compression ratio (249×) reflects the columnar layout's efficiency on this
dataset: most columns are short, highly-repetitive TEXT values (machine names, GPU IDs,
state strings, device names) that ZSTD can delta/dictionary-encode to near-zero size.
Row counts are identical (verified: both have 4,887,205 rows for April).

## Write strategy recommendation

**Originally recommended:** keep SQLite for intraday collection, compact to Parquet
nightly, and route dashboard queries per-month between the two formats. In
implementation this was replaced with a simpler direct-write approach (see below) to
avoid maintaining two read paths and a compaction cron job indefinitely.

**As implemented:** the collector (`collector.py`, formerly `get_gpu_state_polars.py`)
writes directly to the current month's Parquet file on every collection cycle, via a
read-concat-atomic-rename cycle (write to `.tmp`, then `os.replace()`, so the live file
is never partially written). Historical SQLite files are converted once via
`scripts/migrate_sqlite_to_parquet.py` and then kept only as an offline backup — there
is no ongoing dual-write or per-month routing logic. All readers (`stats_data.py`,
`stats_calculations.py`, `dashboard/data.py`) glob `gpu_state_*.parquet` in a single
DuckDB query, covering all months in one pass with no per-file loop.

## Recommendation

**Proceed with migration.** The data supports it strongly:

- **Storage**: 12 GB → ~50 MB (fits in RAM, trivially backupable, Git-storable)
- **Query speed**: 17× improvement on the hot dashboard path
- **Risk**: Low — Parquet export is lossless and verified row-for-row; SQLite files can
  remain as backup until Parquet coverage is confirmed correct over multiple months
- **Collector disruption**: Collector now writes Parquet directly instead of SQLite
  (see "As implemented" above) — a bigger change than originally scoped, but it avoids
  running two storage formats side by side indefinitely.

**Do not** migrate to DuckDB-over-SQLite (attach path) — it adds latency without benefit.

## Files

- Benchmark script: `analysis/benchmark_duckdb_vs_sqlite.py`
- Sample Parquet (April 2026): `analysis/gpu_state_2026-04.parquet` (not committed — 4.6 MB)
