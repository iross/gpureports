#!/usr/bin/env python3
"""
Migrate old-schema job_pressure DBs to the interval schema, or re-merge
an already-converted DB that was migrated with the wrong gap threshold.

Old schema: one row per (snapshot, job) with a TEXT timestamp column.
New schema: one row per idle period with INTEGER first_seen / last_seen.

For each GlobalJobId, consecutive sightings within gap_seconds of each
other are merged into a single interval. Sightings separated by a longer
gap produce separate intervals (job was matched/held then went idle again).

The collection interval is auto-detected from the data; the merge threshold
is set to 2× that interval.  Use --gap to override.

Usage:
    python migrate_job_pressure.py job_pressure_2026-04.db
    python migrate_job_pressure.py job_pressure_2026-04.db job_pressure_2026-05.db
    python migrate_job_pressure.py --gap 3600 job_pressure_2026-04.db
"""

import argparse
import sqlite3
import statistics
from pathlib import Path

BATCH = 50_000


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


def _detect_interval_new(conn: sqlite3.Connection) -> int:
    """Detect collection interval from new-schema DB using gaps between intervals."""
    rows = conn.execute(
        """
        WITH gaps AS (
            SELECT first_seen - LAG(last_seen) OVER (
                PARTITION BY GlobalJobId ORDER BY first_seen
            ) AS gap
            FROM job_pressure
        )
        SELECT gap FROM gaps WHERE gap IS NOT NULL AND gap > 0
        LIMIT 100000
        """
    ).fetchall()
    return statistics.mode(g[0] for g in rows) if rows else 1800


def _write_intervals(conn: sqlite3.Connection, intervals_iter) -> int:
    """Write intervals from an iterable to job_pressure, returning count."""
    pending: list[tuple] = []
    n = 0

    def _flush() -> None:
        conn.executemany("INSERT INTO job_pressure VALUES (?,?,?,?,?,?,?,?,?,?,?)", pending)

    for interval in intervals_iter:
        pending.append(interval)
        n += 1
        if len(pending) >= BATCH:
            _flush()
            pending.clear()
    if pending:
        _flush()
    return n


def _merge_stream(cursor, gap_seconds: int):
    """Yield merged intervals from an ordered (GlobalJobId, ts_or_first_seen) cursor."""
    # Each row: (GlobalJobId[0], ...attrs[1:9]..., ts_or_first[9], ts_or_last[10])
    # For old schema rows[10] == rows[9] (single timestamp), for new schema rows[10] is last_seen.
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


def _migrate_old_schema(conn: sqlite3.Connection, gap_seconds: int) -> int:
    """Migrate from TEXT-timestamp schema to interval schema. Returns interval count."""
    conn.execute("BEGIN")
    conn.execute("ALTER TABLE job_pressure RENAME TO job_pressure_old")
    conn.execute(
        """
        CREATE TABLE job_pressure (
            GlobalJobId TEXT NOT NULL, ScheddName TEXT, Owner TEXT,
            RequestGPUs REAL, RequestCPUs REAL, RequestMemory REAL,
            RequestGPUMemory REAL, QDate INTEGER, ChtcProjects TEXT,
            first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL
        )
        """
    )

    cursor = conn.execute(
        """
        SELECT GlobalJobId, ScheddName, Owner,
               RequestGPUs, RequestCPUs, RequestMemory, RequestGPUMemory,
               QDate, ChtcProjects,
               CAST(strftime('%s', timestamp) AS INTEGER) AS ts,
               CAST(strftime('%s', timestamp) AS INTEGER) AS ts2
        FROM job_pressure_old
        ORDER BY GlobalJobId, ts
        """
    )
    cursor.arraysize = 10_000

    n = _write_intervals(conn, _merge_stream(cursor, gap_seconds))
    conn.execute("DROP TABLE job_pressure_old")
    return n


def _remerge_new_schema(conn: sqlite3.Connection, gap_seconds: int) -> int:
    """Re-merge an already-converted interval table with a new gap threshold."""
    conn.execute("BEGIN")
    conn.execute("ALTER TABLE job_pressure RENAME TO job_pressure_old")
    conn.execute(
        """
        CREATE TABLE job_pressure (
            GlobalJobId TEXT NOT NULL, ScheddName TEXT, Owner TEXT,
            RequestGPUs REAL, RequestCPUs REAL, RequestMemory REAL,
            RequestGPUMemory REAL, QDate INTEGER, ChtcProjects TEXT,
            first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL
        )
        """
    )

    cursor = conn.execute(
        """
        SELECT GlobalJobId, ScheddName, Owner,
               RequestGPUs, RequestCPUs, RequestMemory, RequestGPUMemory,
               QDate, ChtcProjects,
               first_seen, last_seen
        FROM job_pressure_old
        ORDER BY GlobalJobId, first_seen
        """
    )
    cursor.arraysize = 10_000

    n = _write_intervals(conn, _merge_stream(cursor, gap_seconds))
    conn.execute("DROP TABLE job_pressure_old")
    return n


def _migrate(db_path: str, gap_override: int | None) -> None:
    path = Path(db_path)
    if not path.exists():
        print(f"Skipping {db_path}: file not found")
        return

    conn = sqlite3.connect(db_path)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(job_pressure)")}

    has_old = "timestamp" in cols
    has_new = "first_seen" in cols

    if not has_old and not has_new:
        print(f"Skipping {db_path}: unrecognised schema")
        conn.close()
        return

    print(f"Migrating {db_path} …")
    old_count = conn.execute("SELECT COUNT(*) FROM job_pressure").fetchone()[0]
    print(f"  {old_count:,} rows in current schema ({'old' if has_old else 'new'})")

    if gap_override is not None:
        gap_seconds = gap_override
        print(f"  Using --gap {gap_seconds}s")
    else:
        detect_fn = _detect_interval_old if has_old else _detect_interval_new
        interval = detect_fn(conn)
        gap_seconds = interval * 2
        print(f"  Detected collection interval: {interval}s → merge threshold: {gap_seconds}s")

    if has_old:
        n = _migrate_old_schema(conn, gap_seconds)
    else:
        n = _remerge_new_schema(conn, gap_seconds)

    print(f"  → {n:,} intervals")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_last_seen  ON job_pressure (last_seen)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_first_seen ON job_pressure (first_seen)")
    conn.execute("COMMIT")
    conn.execute("VACUUM")
    conn.close()

    new_size = path.stat().st_size
    print(f"  Done. File size now {new_size / 1e6:.1f} MB")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dbs", nargs="+", metavar="DB")
    parser.add_argument(
        "--gap",
        type=int,
        default=None,
        metavar="SECONDS",
        help="Merge threshold in seconds (default: 2× auto-detected collection interval)",
    )
    args = parser.parse_args()
    for db in args.dbs:
        _migrate(db, args.gap)


if __name__ == "__main__":
    main()
