#!/usr/bin/env python3
"""
Periodic snapshot of GPU job pressure and job-to-project attribution in the HTCondor pool.

Queries schedds for idle and running jobs requesting GPUs and records each contiguous
observation as a single row with first_seen / last_seen INTEGER timestamps (Unix seconds)
in a monthly Parquet file, job_pressure_YYYY-MM.parquet. Each poll extends last_seen for
jobs still in the same state and opens a new row for jobs newly seen in a state, so a
long-idle job generates one row instead of hundreds. JobState is "idle" (queue pressure)
or "running" (lets gpu_state's GlobalJobId be joined to the job's ChtcProjects group even
for jobs that started before the next poll ever saw them idle).

Storage mirrors collector.py: the month's file is read, updated, and atomically replaced
through a dot-prefixed temp file, so concurrent readers globbing job_pressure_*.parquet
never see a partial file. A per-month lock file serializes overlapping runs.

Runs as a k8s CronJob bundled in the same container image as collector.py -- see
OPERATIONS.md. Old SQLite files are converted with migrate_job_pressure.py.
"""

import datetime
import fcntl
import os
import time
from pathlib import Path
from typing import Annotated

import polars as pl
import typer

from read_data import JOB_PRESSURE_SCHEMA

COLLECTOR_HOST = "cm.chtc.wisc.edu"

PROJ = [
    "GlobalJobId",
    "Owner",
    "RequestGPUs",
    "RequestCPUs",
    "RequestMemory",
    "RequestGPUMemory",
    "QDate",
    "ChtcProjects",
    "JobStatus",
]

# HTCondor JobStatus -> JobState
JOB_STATES = {1: "idle", 2: "running"}
CONSTRAINT = "RequestGPUs >= 1 && (JobStatus == 1 || JobStatus == 2)"

# A job absent for longer than STALE_POLLS polls is treated as a closed interval; the
# next sighting opens a fresh row. Tolerates a couple of missed polls.
STALE_POLLS = 3

_KEY = ["GlobalJobId", "JobState"]


def _eval_classad(val: object) -> object:
    """Evaluate a ClassAd ExprTree to a Python value if possible.

    In k8s (no local HTCondor config) schedd.query() returns unevaluated
    ExprTree objects; on baremetal the local config provides context so values
    come back as native Python types already.
    """
    if hasattr(val, "eval"):
        try:
            return val.eval()
        except Exception:
            pass
    return val


def _float_or_none(val: object) -> float | None:
    val = _eval_classad(val)
    if val is None:
        return None
    try:
        f = float(val)  # type: ignore[arg-type]
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _safe_float(val: object, default: float = 0.0) -> float:
    val = _eval_classad(val)
    if val is None:
        return default
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _safe_int(val: object, default: int = 0) -> int:
    val = _eval_classad(val)
    if val is None:
        return default
    try:
        return int(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _safe_str(val: object) -> str:
    val = _eval_classad(val)
    return "" if val is None else str(val)


# Schedds queried by default: the CHTC access points. Other advertised schedds either deny
# anonymous queries (HEP/physics) or are unreachable, and every poll would wait out their timeouts.
DEFAULT_SCHEDDS = ("ap2001.chtc.wisc.edu", "ap2002.chtc.wisc.edu")


def collect_gpu_jobs(schedd_names: list[str]) -> list[dict]:
    """Query the named schedds for idle and running GPU jobs; return list of job attribute dicts.

    A schedd that denies or fails the query is skipped with a warning so one unreachable
    or unauthorized submit host never blocks the rest.
    """
    import htcondor2 as htcondor

    try:
        schedd_ads = htcondor.Collector(COLLECTOR_HOST).locateAll(htcondor.DaemonTypes.Schedd)
    except Exception as e:
        print(f"Warning: could not query collector for schedds: {e}")
        return []

    jobs: list[dict] = []
    for schedd_ad in schedd_ads:
        schedd_name = schedd_ad.get("Name", "")
        if schedd_name not in schedd_names:
            continue
        try:
            ads = htcondor.Schedd(schedd_ad).query(constraint=CONSTRAINT, projection=PROJ)
        except Exception as e:
            print(f"Warning: query failed for schedd {schedd_name}: {e}")
            continue

        for ad in ads:
            state = JOB_STATES.get(_safe_int(ad.get("JobStatus")))
            if state is None:
                continue
            jobs.append(
                {
                    "GlobalJobId": _safe_str(ad.get("GlobalJobId")),
                    "ScheddName": schedd_name,
                    "Owner": _safe_str(ad.get("Owner")),
                    "RequestGPUs": _safe_float(ad.get("RequestGPUs")),
                    "RequestCPUs": _safe_float(ad.get("RequestCPUs")),
                    "RequestMemory": _safe_float(ad.get("RequestMemory")),
                    "RequestGPUMemory": _float_or_none(ad.get("RequestGPUMemory")),
                    "QDate": _safe_int(ad.get("QDate")),
                    "ChtcProjects": _safe_str(ad.get("ChtcProjects")),
                    "JobState": state,
                }
            )

    return jobs


def update_intervals(existing: pl.DataFrame, jobs: list[dict], now_ts: int, stale_seconds: int) -> pl.DataFrame:
    """Return existing with last_seen extended for continuing jobs and new rows for new ones.

    A row is "open" when its last_seen is within stale_seconds of now_ts. A job is
    continuing when it has an open row with the same (GlobalJobId, JobState); if several
    open rows share the key (shouldn't happen) the most recently seen one is extended.
    """
    current = (
        pl.DataFrame(jobs, schema=JOB_PRESSURE_SCHEMA | {"first_seen": pl.Int64, "last_seen": pl.Int64}, strict=False)
        .select([c for c in JOB_PRESSURE_SCHEMA if c not in ("first_seen", "last_seen")])
        .unique(subset=_KEY, keep="last", maintain_order=True)
    )

    existing = existing.cast(JOB_PRESSURE_SCHEMA).with_row_index("_row")
    open_rows = (
        existing.filter(pl.col("last_seen") >= now_ts - stale_seconds)
        .sort("last_seen")
        .group_by(_KEY)
        .agg(pl.col("_row").last())
    )

    matched = current.join(open_rows, on=_KEY, how="left")
    continuing_rows = matched["_row"].drop_nulls()

    extended = existing.with_columns(
        pl.when(pl.col("_row").is_in(continuing_rows.implode()))
        .then(now_ts)
        .otherwise(pl.col("last_seen"))
        .alias("last_seen")
    ).drop("_row")

    new_rows = (
        matched.filter(pl.col("_row").is_null())
        .drop("_row")
        .with_columns(
            pl.lit(now_ts, dtype=pl.Int64).alias("first_seen"), pl.lit(now_ts, dtype=pl.Int64).alias("last_seen")
        )
        .select(list(JOB_PRESSURE_SCHEMA))
    )
    return pl.concat([extended, new_rows])


def _write_parquet_atomic(df: pl.DataFrame, parquet_path: Path) -> None:
    """Replace parquet_path with df via a temp file readers' glob can never match.

    Dot-prefixed and suffixed `.tmp` (not `.parquet`), for the same reason as
    collector._write_parquet_atomic: a name matching job_pressure_*.parquet could be
    picked up half-written by a concurrent reader. Always written with
    JOB_PRESSURE_SCHEMA -- an all-null column otherwise gets Parquet type Null, which
    DuckDB cannot read.
    """
    tmp = parquet_path.with_name(f".{parquet_path.name}.tmp")
    df.cast(JOB_PRESSURE_SCHEMA).write_parquet(str(tmp), compression="zstd")
    os.replace(tmp, parquet_path)


def record_jobs(jobs: list[dict], parquet_path: Path, now_ts: int, stale_seconds: int) -> None:
    """Merge one poll's jobs into the month's Parquet file under an exclusive lock."""
    lock_path = parquet_path.with_name(f".{parquet_path.name}.lock")
    with open(lock_path, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if parquet_path.exists():
            existing = pl.read_parquet(str(parquet_path))
        else:
            existing = pl.DataFrame(schema=JOB_PRESSURE_SCHEMA)
        _write_parquet_atomic(update_intervals(existing, jobs, now_ts, stale_seconds), parquet_path)


def main(
    data_dir: str = typer.Argument("/home/iaross/gpureports"),
    poll_interval: int = typer.Option(
        1800, help="Seconds between runs of this script; a job unseen for 3x this is treated as gone"
    ),
    schedd: Annotated[
        list[str] | None,
        typer.Option(help=f"Schedd names to query (default: {', '.join(DEFAULT_SCHEDDS)}); repeat for several"),
    ] = None,
) -> None:
    """Record idle and running GPU jobs into the monthly job_pressure Parquet file."""
    now_ts = int(time.time())
    month = datetime.datetime.fromtimestamp(now_ts, datetime.UTC).strftime("%Y-%m")

    jobs = collect_gpu_jobs(schedd or list(DEFAULT_SCHEDDS))
    n_idle = sum(j["JobState"] == "idle" for j in jobs)
    print(f"{datetime.datetime.now().isoformat()}: {n_idle} idle, {len(jobs) - n_idle} running GPU jobs")
    if jobs:
        record_jobs(jobs, Path(data_dir) / f"job_pressure_{month}.parquet", now_ts, STALE_POLLS * poll_interval)


if __name__ == "__main__":
    typer.run(main)
