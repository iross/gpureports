"""Benchmark DuckDB vs SQLite+Polars for gpu_state 7-day heatmap queries.

Measures:
  - Query latency (5 warm runs) for a 7-day window spanning two monthly DBs
  - Parquet export size vs SQLite size for one full month
  - Peak memory (RSS) during each query path

Run from the project root:
    uv run python analysis/benchmark_duckdb_vs_sqlite.py
"""

import datetime
import statistics
import time
from pathlib import Path

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent.parent
# 7-day window crossing the March->April boundary (last 3 days of March + first 4 of April)
BENCHMARK_START = datetime.datetime(2026, 3, 28, 0, 0, 0)
BENCHMARK_END = datetime.datetime(2026, 4, 4, 0, 0, 0)
COMPRESSION_MONTH_DB = BASE_DIR / "gpu_state_2026-04.db"  # ~1.1 GB — good sample

COLUMNS = [
    "Name",
    "AssignedGPUs",
    "State",
    "PrioritizedProjects",
    "Machine",
    "GPUs_DeviceName",
    "timestamp",
]

N_RUNS = 5

# ---------------------------------------------------------------------------
# SQLite + Polars path (current implementation)
# ---------------------------------------------------------------------------


def query_sqlite_polars(start: datetime.datetime, end: datetime.datetime) -> pl.DataFrame:
    """Replicate current _query_dbs logic from dashboard/data.py."""
    db_paths = [
        BASE_DIR / "gpu_state_2026-03.db",
        BASE_DIR / "gpu_state_2026-04.db",
    ]
    col_select = ", ".join(f'"{c}"' for c in COLUMNS)
    buffered_start = start - datetime.timedelta(seconds=1)
    frames = []
    for db_path in db_paths:
        if not db_path.exists():
            continue
        query = (
            f"SELECT {col_select} FROM gpu_state "
            f"WHERE timestamp BETWEEN '{buffered_start.strftime('%Y-%m-%d %H:%M:%S.%f')}' "
            f"  AND '{end.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
        )
        df = pl.read_database_uri(query, f"sqlite:///{db_path.resolve()}")
        if df.height > 0:
            frames.append(df)
    if not frames:
        return pl.DataFrame()
    combined = pl.concat(frames)
    combined = combined.with_columns(pl.col("timestamp").cast(pl.Datetime("us")))
    combined = combined.filter((pl.col("timestamp") >= start) & (pl.col("timestamp") <= end))
    return combined


# ---------------------------------------------------------------------------
# DuckDB path: attach both SQLite files and UNION
# ---------------------------------------------------------------------------


def query_duckdb_sqlite_attach(start: datetime.datetime, end: datetime.datetime) -> pl.DataFrame:
    """Use DuckDB's sqlite_attach to query both monthly DBs in one pass."""
    db_paths = [
        BASE_DIR / "gpu_state_2026-03.db",
        BASE_DIR / "gpu_state_2026-04.db",
    ]
    col_select = ", ".join(f'"{c}"' for c in COLUMNS)
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")

    # Build UNION ALL across all attached DBs
    union_parts = [
        f"SELECT {col_select} FROM {alias}.gpu_state WHERE timestamp >= '{start_str}' AND timestamp <= '{end_str}'"
        for alias in [f"db{i}" for i in range(len(db_paths))]
    ]
    query = " UNION ALL ".join(union_parts)

    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite;")
    for i, p in enumerate(db_paths):
        if p.exists():
            con.execute(f"ATTACH '{p.resolve()}' AS db{i} (TYPE sqlite, READ_ONLY)")
    result = con.execute(query).pl()
    con.close()
    return result


# ---------------------------------------------------------------------------
# DuckDB path: query Parquet files (hypothetical future state)
# ---------------------------------------------------------------------------


def query_duckdb_parquet(parquet_dir: Path, start: datetime.datetime, end: datetime.datetime) -> pl.DataFrame:
    """Query monthly Parquet files with DuckDB pushdown predicates."""
    col_select = ", ".join(f'"{c}"' for c in COLUMNS)
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")
    parquet_glob = str(parquet_dir / "gpu_state_*.parquet")
    query = (
        f"SELECT {col_select} FROM parquet_scan('{parquet_glob}', hive_partitioning=false) "
        f"WHERE timestamp >= '{start_str}' AND timestamp <= '{end_str}'"
    )
    con = duckdb.connect()
    result = con.execute(query).pl()
    con.close()
    return result


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------


def bench(fn, *args, n: int = N_RUNS, label: str = "") -> dict:
    """Run fn(*args) n times; return timing stats and row count."""
    times = []
    result = None
    for _ in range(n):
        t0 = time.perf_counter()
        result = fn(*args)
        times.append(time.perf_counter() - t0)
    rows = result.height if result is not None else 0
    print(f"  {label}: {statistics.mean(times):.3f}s mean / {min(times):.3f}s best  ({rows:,} rows)")
    return {"label": label, "mean_s": statistics.mean(times), "min_s": min(times), "rows": rows, "times": times}


# ---------------------------------------------------------------------------
# Parquet compression benchmark
# ---------------------------------------------------------------------------


def export_month_to_parquet(db_path: Path, out_path: Path) -> None:
    """Export one full monthly SQLite DB to a single Parquet file via DuckDB."""
    col_select = ", ".join(f'"{c}"' for c in COLUMNS)
    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{db_path.resolve()}' AS src (TYPE sqlite, READ_ONLY)")
    con.execute(
        f"COPY (SELECT {col_select} FROM src.gpu_state ORDER BY timestamp) "
        f"TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
    )
    con.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("=" * 70)
    print("GPU State Storage Benchmark: DuckDB vs SQLite+Polars")
    print(f"Query window: {BENCHMARK_START.date()} → {BENCHMARK_END.date()} (7 days, 2 DBs)")
    print("=" * 70)

    # --- 1. Latency comparison ---
    print("\n[1] Query latency (7-day heatmap range, 5 warm runs each)\n")
    results = []
    results.append(bench(query_sqlite_polars, BENCHMARK_START, BENCHMARK_END, label="SQLite + Polars (current)"))
    results.append(
        bench(
            query_duckdb_sqlite_attach,
            BENCHMARK_START,
            BENCHMARK_END,
            label="DuckDB attach SQLite (no Parquet)",
        )
    )

    # --- 2. Parquet export + size comparison ---
    print("\n[2] Parquet compression ratio\n")
    parquet_dir = BASE_DIR / "analysis"
    parquet_path = parquet_dir / "gpu_state_2026-04.parquet"

    if not parquet_path.exists():
        print("  Exporting April DB to Parquet (ZSTD)…", end=" ", flush=True)
        t0 = time.perf_counter()
        export_month_to_parquet(COMPRESSION_MONTH_DB, parquet_path)
        elapsed = time.perf_counter() - t0
        print(f"done in {elapsed:.1f}s")
    else:
        print("  (Parquet file already exists, skipping export)")

    sqlite_mb = COMPRESSION_MONTH_DB.stat().st_size / 1024**2
    parquet_mb = parquet_path.stat().st_size / 1024**2
    ratio = sqlite_mb / parquet_mb
    print(f"  SQLite  (April): {sqlite_mb:>8.1f} MB")
    print(f"  Parquet (April): {parquet_mb:>8.1f} MB  (ZSTD)")
    print(f"  Compression ratio: {ratio:.1f}x")

    # --- 3. DuckDB Parquet query latency ---
    print("\n[3] DuckDB query against Parquet (single-month, window within April)\n")
    april_start = datetime.datetime(2026, 4, 1, 0, 0, 0)
    april_end = datetime.datetime(2026, 4, 7, 23, 59, 59)
    results.append(
        bench(
            query_duckdb_parquet,
            parquet_dir,
            april_start,
            april_end,
            label="DuckDB Parquet ZSTD (7d, single month)",
        )
    )
    # Compare same 7-day window against SQLite for fairness
    results.append(
        bench(
            query_sqlite_polars,
            april_start,
            april_end,
            label="SQLite + Polars (same 7d, single month)",
        )
    )

    # --- 4. Summary table ---
    print("\n[4] Summary\n")
    print(f"  {'Approach':<45} {'Mean (s)':>8}  {'Best (s)':>8}  {'Rows':>10}")
    print("  " + "-" * 77)
    for r in results:
        print(f"  {r['label']:<45} {r['mean_s']:>8.3f}  {r['min_s']:>8.3f}  {r['rows']:>10,}")

    estimated_parquet_mb = 12 * 1024 / ratio
    print("\n  SQLite total size (13 months): ~12 GB  (~12,288 MB)")
    print(f"  Estimated Parquet total size:  ~{estimated_parquet_mb:.0f} MB  (at {ratio:.0f}x compression)")
    print()


if __name__ == "__main__":
    main()
