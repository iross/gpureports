#!/usr/bin/env python3
"""
Convert SQLite job_pressure_YYYY-MM.db files to job_pressure_YYYY-MM.parquet.

Handles both SQLite schemas the baremetal collector produced:

  Old schema: one row per (snapshot, job) with a TEXT timestamp column.
  Interval schema: one row per idle period with INTEGER first_seen / last_seen.

Old-schema rows are merged per GlobalJobId: consecutive sightings within gap_seconds of
each other become a single interval; a longer gap produces separate intervals (the job
was matched or held, then went idle again). The collection interval is auto-detected and
the merge threshold is set to 2x that interval. Interval-schema rows are already
intervals and are copied as recorded; --gap re-merges them (or overrides the old-schema
threshold).

Every SQLite row was an idle observation, so output rows get JobState = "idle". The
SQLite file is opened read-only and left in place. Output is written atomically with the
schema get_job_pressure.py and read_data.load_job_pressure expect.

Old-schema timestamps are naive local time written by the baremetal host; SQLite would
read them as UTC, so --local-utc-offset (default 18000 s = CDT, which covers the whole
Mar-Nov DST period the old files span) is added to convert them to true Unix seconds.
Interval-schema files already hold true Unix seconds and are not shifted.

Usage:
    python migrate_job_pressure.py job_pressure_2026-05.db
    python migrate_job_pressure.py --output-dir /data job_pressure_2026-05.db job_pressure_2026-06.db
    python migrate_job_pressure.py --gap 3600 job_pressure_2026-04.db
"""

import argparse
import os
import sqlite3
import statistics
from pathlib import Path

import polars as pl

from read_data import JOB_PRESSURE_SCHEMA

BATCH = 500_000

_ATTR_COLUMNS = (
    "GlobalJobId, ScheddName, Owner, RequestGPUs, RequestCPUs, RequestMemory, RequestGPUMemory, QDate, ChtcProjects"
)


def _detect_interval_old(conn: sqlite3.Connection) -> int:
    """Detect collection interval from old-schema DB using LAG window function."""
    rows = conn.execute(
        """
        WITH ordered AS (
            SELECT GlobalJobId,
                   CAST(strftime('%s', timestamp) AS INTEGER) AS ts
            FROM job_pressure
        ),
        gaps AS (
            SELECT ts - LAG(ts) OVER (PARTITION BY GlobalJobId ORDER BY ts) AS gap
            FROM ordered
        )
        SELECT gap FROM gaps WHERE gap IS NOT NULL AND gap > 0
        LIMIT 100000
        """
    ).fetchall()
    return statistics.mode(g[0] for g in rows) if rows else 1800


def _merge_stream(cursor, gap_seconds: int):
    """Yield merged intervals from an ordered (GlobalJobId, ts_or_first_seen) cursor."""
    # Each row: (GlobalJobId[0], ...attrs[1:9]..., first[9], last[10]); for old-schema
    # rows first == last (a single timestamp).
    cur: list | None = None
    first_ts = 0

    for row in cursor:
        row = list(row)
        if cur is None:
            cur = row
            first_ts = row[9]
        elif row[0] == cur[0] and row[9] - cur[10] <= gap_seconds:
            cur[10] = row[10]  # extend last_seen
        else:
            yield (*cur[:9], first_ts, cur[10])
            cur = row
            first_ts = row[9]

    if cur is not None:
        yield (*cur[:9], first_ts, cur[10])


def _intervals_to_frame(intervals: list[tuple]) -> pl.DataFrame:
    columns = [c for c in JOB_PRESSURE_SCHEMA if c != "JobState"]
    frame = pl.DataFrame(intervals, schema=columns, orient="row", infer_schema_length=None)
    return frame.with_columns(pl.lit("idle").alias("JobState")).select(list(JOB_PRESSURE_SCHEMA))


def _write_parquet_atomic(df: pl.DataFrame, parquet_path: Path) -> None:
    """Write via a dot-prefixed .tmp file so readers globbing job_pressure_*.parquet never see it half-written."""
    tmp = parquet_path.with_name(f".{parquet_path.name}.tmp")
    df.cast(JOB_PRESSURE_SCHEMA).write_parquet(str(tmp), compression="zstd")
    os.replace(tmp, parquet_path)


def _migrate(db_path: str, output_dir: Path | None, gap_override: int | None, local_utc_offset: int) -> None:
    path = Path(db_path)
    if not path.exists():
        print(f"Skipping {db_path}: file not found")
        return

    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(job_pressure)")}
    has_old = "timestamp" in cols
    has_new = "first_seen" in cols

    if not has_old and not has_new:
        print(f"Skipping {db_path}: unrecognised schema")
        conn.close()
        return

    print(f"Converting {db_path} ...")
    row_count = conn.execute("SELECT COUNT(*) FROM job_pressure").fetchone()[0]
    print(f"  {row_count:,} rows in {'old snapshot' if has_old else 'interval'} schema")

    gap_seconds: int | None
    if gap_override is not None:
        gap_seconds = gap_override
        print(f"  Using --gap {gap_seconds}s")
    elif has_old:
        interval = _detect_interval_old(conn)
        gap_seconds = interval * 2
        print(f"  Detected collection interval: {interval}s -> merge threshold: {gap_seconds}s")
    else:
        gap_seconds = None
        print("  Interval schema: copying intervals as recorded (pass --gap to re-merge)")

    if has_old:
        ts = f"CAST(strftime('%s', timestamp) AS INTEGER) + {int(local_utc_offset)}"
        query = f"SELECT {_ATTR_COLUMNS}, {ts} AS first_ts, {ts} AS last_ts FROM job_pressure ORDER BY GlobalJobId, first_ts"  # noqa: S608
    else:
        query = f"SELECT {_ATTR_COLUMNS}, first_seen, last_seen FROM job_pressure ORDER BY GlobalJobId, first_seen"  # noqa: S608
    cursor = conn.execute(query)
    cursor.arraysize = 10_000

    frames: list[pl.DataFrame] = []
    pending: list[tuple] = []
    for interval in _merge_stream(cursor, gap_seconds) if gap_seconds is not None else map(tuple, cursor):
        pending.append(interval)
        if len(pending) >= BATCH:
            frames.append(_intervals_to_frame(pending))
            pending.clear()
    if pending:
        frames.append(_intervals_to_frame(pending))
    conn.close()

    result = pl.concat(frames) if frames else pl.DataFrame(schema=JOB_PRESSURE_SCHEMA)
    print(f"  -> {len(result):,} intervals")

    out_dir = output_dir or path.parent
    out_path = out_dir / f"{path.stem}.parquet"
    _write_parquet_atomic(result, out_path)
    print(f"  Wrote {out_path} ({out_path.stat().st_size / 1e6:.1f} MB)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dbs", nargs="+", metavar="DB")
    parser.add_argument(
        "--gap",
        type=int,
        default=None,
        metavar="SECONDS",
        help="Merge threshold in seconds (default: 2x auto-detected collection interval)",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=None, help="Where to write .parquet files (default: next to each DB)"
    )
    parser.add_argument(
        "--local-utc-offset",
        type=int,
        default=18000,
        metavar="SECONDS",
        help="Seconds to add to old-schema naive local timestamps to get UTC (default: 18000, CDT)",
    )
    args = parser.parse_args()
    for db in args.dbs:
        _migrate(db, args.output_dir, args.gap, args.local_utc_offset)


if __name__ == "__main__":
    main()
