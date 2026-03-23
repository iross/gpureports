"""Data layer for the GPU state dashboard.

Queries SQLite DBs, deduplicates, classifies GPU states, and returns
structured dicts matching the API response shape.
"""

import datetime
import re
import sqlite3
from pathlib import Path

import polars as pl
import yaml

from gpu_utils import get_latest_timestamp_from_most_recent_db, get_required_databases

# State codes used in the API response (compact integer encoding)
STATE_CODES = {
    "idle_prioritized": 0,
    "idle_shared": 1,
    "busy_prioritized": 2,
    "busy_shared": 3,
    "busy_backfill": 4,
    "na": 5,
    "idle_backfill": 6,
}

STATE_MAP = {v: k for k, v in STATE_CODES.items()}

STATE_COLORS = {
    0: "#ff4444",
    1: "#ff8800",
    2: "#44ff44",
    3: "#00cc99",
    4: "#4488ff",
    5: "#cccccc",
    6: "#334499",
}

# State codes that count as "claimed" per category
_CATEGORY_CODES: dict[str, dict[str, list[int]]] = {
    "prioritized": {"all": [0, 2], "claimed": [2]},
    "open_capacity": {"all": [1, 3], "claimed": [3]},
    "backfill": {"all": [4, 6], "claimed": [4]},
}

COLUMNS = [
    "Name",
    "AssignedGPUs",
    "State",
    "PrioritizedProjects",
    "Machine",
    "GPUs_DeviceName",
    "timestamp",
]


def _load_masked_hosts(base_dir: str = ".") -> set[str]:
    """Load excluded hosts from masked_hosts.yaml."""
    path = Path(base_dir) / "masked_hosts.yaml"
    if not path.exists():
        return set()
    with open(path) as f:
        data = yaml.safe_load(f)
    if data and "excluded_hosts" in data:
        return set(data["excluded_hosts"].keys())
    return set()


def _query_dbs(db_paths: list[str], start: datetime.datetime, end: datetime.datetime) -> pl.DataFrame:
    """Load data from multiple SQLite DBs and combine into one Polars DataFrame."""
    frames = []
    buffered_start = start - datetime.timedelta(seconds=1)
    col_select = ", ".join(f'"{c}"' for c in COLUMNS)

    for db_path in db_paths:
        try:
            abs_path = str(Path(db_path).resolve())
            query = f"""
                SELECT {col_select}
                FROM gpu_state
                WHERE timestamp BETWEEN '{buffered_start.strftime("%Y-%m-%d %H:%M:%S.%f")}'
                  AND '{end.strftime("%Y-%m-%d %H:%M:%S.%f")}'
            """
            df = pl.read_database_uri(query, f"sqlite:///{abs_path}")
            if df.height > 0:
                frames.append(df)
        except Exception as e:
            print(f"Warning: Could not load {db_path}: {e}")

    if not frames:
        return pl.DataFrame()

    combined = pl.concat(frames)

    # Parse timestamps and apply precise time filter
    combined = combined.with_columns(pl.col("timestamp").cast(pl.Datetime("us")))
    combined = combined.filter((pl.col("timestamp") >= start) & (pl.col("timestamp") <= end))
    return combined


def _classify_states(df: pl.DataFrame) -> pl.DataFrame:
    """Classify each row into one of 6 GPU state codes."""
    return df.with_columns(
        pl.when(
            (pl.col("State").str.to_lowercase() == "claimed")
            & (pl.col("PrioritizedProjects") != "")
            & (~pl.col("Name").str.to_lowercase().str.contains("backfill"))
        )
        .then(pl.lit(STATE_CODES["busy_prioritized"]))
        .when(
            (pl.col("State").str.to_lowercase() == "claimed")
            & (~pl.col("Name").str.to_lowercase().str.contains("backfill"))
        )
        .then(pl.lit(STATE_CODES["busy_shared"]))
        .when(
            (pl.col("State").str.to_lowercase() == "claimed")
            & (pl.col("Name").str.to_lowercase().str.contains("backfill"))
        )
        .then(pl.lit(STATE_CODES["busy_backfill"]))
        .when(
            (pl.col("State").str.to_lowercase() == "unclaimed")
            & (pl.col("Name").str.to_lowercase().str.contains("backfill"))
        )
        .then(pl.lit(STATE_CODES["idle_backfill"]))
        .when((pl.col("State").str.to_lowercase() == "unclaimed") & (pl.col("PrioritizedProjects") != ""))
        .then(pl.lit(STATE_CODES["idle_prioritized"]))
        .when(pl.col("State").str.to_lowercase() == "unclaimed")
        .then(pl.lit(STATE_CODES["idle_shared"]))
        .otherwise(pl.lit(STATE_CODES["na"]))
        .alias("state_code")
    )


def _dedup_and_bucket(df: pl.DataFrame, bucket_minutes: int) -> pl.DataFrame:
    """Floor timestamps to buckets, rank duplicates, keep highest-priority entry."""
    df = df.with_columns(pl.col("timestamp").dt.truncate(f"{bucket_minutes}m").alias("time_bucket"))

    # Rank: claimed+primary(3) > claimed+backfill(2) > unclaimed+primary(1) > unclaimed+backfill(0)
    is_claimed = pl.col("State").str.to_lowercase() == "claimed"
    is_backfill = pl.col("Name").str.to_lowercase().str.contains("backfill")

    df = df.with_columns(
        pl.when(is_claimed & ~is_backfill)
        .then(pl.lit(3))
        .when(is_claimed & is_backfill)
        .then(pl.lit(2))
        .when(~is_claimed & ~is_backfill)
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("_rank")
    )

    # Sort by rank descending so first row per group wins
    df = df.sort(["time_bucket", "AssignedGPUs", "_rank"], descending=[False, False, True])
    df = df.unique(subset=["time_bucket", "AssignedGPUs"], keep="first")
    df = df.drop("_rank")

    return df


def _prepare_bucketed(
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    bucket_minutes: int,
    base_dir: str,
) -> tuple[pl.DataFrame, list, list[str]] | None:
    """Load, mask, dedup, bucket, and classify data.

    Returns (df, all_buckets, bucket_strs) or None if no data is available.
    """
    if end is None:
        end = get_latest_timestamp_from_most_recent_db(base_dir)
        if end is None:
            return None
    if start is None:
        start = end - datetime.timedelta(hours=24)

    db_paths = get_required_databases(start, end, base_dir)
    if not db_paths:
        return None

    df = _query_dbs(db_paths, start, end)
    if df.height == 0:
        return None

    masked = _load_masked_hosts(base_dir)
    if masked:
        for host in masked:
            df = df.filter(~pl.col("Machine").str.contains(host))

    df = _dedup_and_bucket(df, bucket_minutes)
    df = _classify_states(df)

    all_buckets = sorted(df["time_bucket"].unique().to_list())
    bucket_strs = [t.strftime("%Y-%m-%dT%H:%M") for t in all_buckets]

    return df, all_buckets, bucket_strs


def get_heatmap_data(
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
    bucket_minutes: int = 15,
    base_dir: str = ".",
) -> dict:
    """Build the heatmap data structure for the API response.

    Parameters
    ----------
    start, end : datetime or None
        Time range. Defaults to last 24h from most recent DB data.
    bucket_minutes : int
        Width of each time bucket in minutes.
    base_dir : str
        Directory containing gpu_state_*.db files.

    Returns
    -------
    dict matching the API response shape.
    """
    prepared = _prepare_bucketed(start, end, bucket_minutes, base_dir)
    if prepared is None:
        return _empty_heatmap_response()

    df, all_buckets, bucket_strs = prepared
    bucket_index = {t: i for i, t in enumerate(all_buckets)}

    # Build machine-grouped structure
    gpu_info = (
        df.select("Machine", "AssignedGPUs", "GPUs_DeviceName")
        .unique(subset=["Machine", "AssignedGPUs"])
        .sort(["Machine", "AssignedGPUs"])
    )

    # Build lookup: (machine, gpu) -> {bucket_index: state_code}
    pivot = {}
    for row in df.iter_rows(named=True):
        key = (row["Machine"], row["AssignedGPUs"])
        if key not in pivot:
            pivot[key] = {}
        bi = bucket_index.get(row["time_bucket"])
        if bi is not None:
            pivot[key][bi] = row["state_code"]

    n_buckets = len(all_buckets)
    machines_dict: dict[str, list] = {}

    for row in gpu_info.iter_rows(named=True):
        machine = row["Machine"]
        gpu_id = row["AssignedGPUs"]
        device = row["GPUs_DeviceName"] or "Unknown"

        state_map = pivot.get((machine, gpu_id), {})
        states = [state_map.get(i, STATE_CODES["na"]) for i in range(n_buckets)]

        if machine not in machines_dict:
            machines_dict[machine] = []
        machines_dict[machine].append(
            {
                "gpu_id": gpu_id,
                "device_name": device,
                "states": states,
            }
        )

    machines_list = [{"name": name, "gpus": gpus} for name, gpus in sorted(machines_dict.items())]

    return {
        "time_buckets": bucket_strs,
        "machines": machines_list,
        "state_map": STATE_MAP,
        "state_colors": STATE_COLORS,
    }


def get_counts_data(
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
    bucket_minutes: int = 15,
    base_dir: str = ".",
) -> dict:
    """Build time-series GPU counts per category for the Charts tab.

    For each time bucket, returns total and claimed GPU counts for each
    of the three categories: prioritized, open_capacity, backfill.

    Parameters
    ----------
    start, end : datetime or None
        Time range. Defaults to last 24h from most recent DB data.
    bucket_minutes : int
        Width of each time bucket in minutes.
    base_dir : str
        Directory containing gpu_state_*.db files.

    Returns
    -------
    dict with 'buckets' list and 'series' dict per category.
    """
    # Resolve time range and load raw data (same steps as _prepare_bucketed,
    # but we need the pre-dedup DataFrame for accurate backfill counts).
    if end is None:
        end = get_latest_timestamp_from_most_recent_db(base_dir)
        if end is None:
            return _empty_counts_response()
    if start is None:
        start = end - datetime.timedelta(hours=24)

    db_paths = get_required_databases(start, end, base_dir)
    if not db_paths:
        return _empty_counts_response()

    df_raw = _query_dbs(db_paths, start, end)
    if df_raw.height == 0:
        return _empty_counts_response()

    masked = _load_masked_hosts(base_dir)
    if masked:
        for host in masked:
            df_raw = df_raw.filter(~pl.col("Machine").str.contains(host))

    # Count all categories directly from raw (pre-dedup) slot rows.
    #
    # A GPU can appear in BOTH a primary slot and a backfill slot simultaneously.
    # Dedup merges these, causing GPUs to disappear from primary totals when
    # their backfill slot wins the rank competition (e.g. backfill-claimed > primary-idle).
    # Counting each slot type independently avoids this entirely.
    df_raw_bucketed = df_raw.with_columns(pl.col("timestamp").dt.truncate(f"{bucket_minutes}m").alias("time_bucket"))

    is_backfill = pl.col("Name").str.to_lowercase().str.contains("backfill")
    is_claimed = pl.col("State").str.to_lowercase() == "claimed"
    has_gpu = pl.col("AssignedGPUs").is_not_null()
    has_priority = pl.col("PrioritizedProjects").is_not_null() & (pl.col("PrioritizedProjects") != "")

    primary_df = df_raw_bucketed.filter(~is_backfill & has_gpu)
    backfill_df = df_raw_bucketed.filter(is_backfill & has_gpu)

    all_buckets = sorted(df_raw_bucketed.filter(has_gpu)["time_bucket"].unique().to_list())
    bucket_strs = [t.strftime("%Y-%m-%dT%H:%M") for t in all_buckets]
    buckets_df = pl.DataFrame({"time_bucket": all_buckets})

    def _count_series(df: pl.DataFrame, total_mask: pl.Expr, claimed_mask: pl.Expr) -> dict[str, list]:
        total_df = df.filter(total_mask).group_by("time_bucket").agg(pl.col("AssignedGPUs").n_unique().alias("total"))
        claimed_df = (
            df.filter(total_mask & claimed_mask)
            .group_by("time_bucket")
            .agg(pl.col("AssignedGPUs").n_unique().alias("claimed"))
        )
        merged = (
            buckets_df.join(total_df, on="time_bucket", how="left")
            .join(claimed_df, on="time_bucket", how="left")
            .fill_null(0)
            .sort("time_bucket")
        )
        return {"total": merged["total"].to_list(), "claimed": merged["claimed"].to_list()}

    series: dict[str, dict[str, list]] = {
        "prioritized": _count_series(primary_df, has_priority, is_claimed),
        "open_capacity": _count_series(primary_df, ~has_priority, is_claimed),
        "backfill": _count_series(backfill_df, pl.lit(True), is_claimed),
    }

    return {"buckets": bucket_strs, "series": series}


def _empty_heatmap_response() -> dict:
    return {
        "time_buckets": [],
        "machines": [],
        "state_map": STATE_MAP,
        "state_colors": STATE_COLORS,
    }


def _empty_counts_response() -> dict:
    return {
        "buckets": [],
        "series": {
            "prioritized": {"total": [], "claimed": []},
            "open_capacity": {"total": [], "claimed": []},
            "backfill": {"total": [], "claimed": []},
        },
    }


# ---------------------------------------------------------------------------
# Open-capacity jobs panel (AC #6-8 of task-29)
# ---------------------------------------------------------------------------


def _get_job_info_databases(start: datetime.datetime, end: datetime.datetime, base_dir: str) -> list[str]:
    """Return job_info_YYYY-MM.db paths that overlap [start, end]."""
    paths = []
    current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_month = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current <= end_month:
        p = Path(base_dir) / f"job_info_{current.strftime('%Y-%m')}.db"
        if p.exists():
            paths.append(str(p))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return paths


def _load_suspicious_criteria(base_dir: str) -> tuple[list[re.Pattern], float]:
    """Load cmd patterns and min_runtime_hours from suspicious_jobs.yaml."""
    path = Path(base_dir) / "suspicious_jobs.yaml"
    if not path.exists():
        return [], 0.0
    with open(path) as f:
        cfg = yaml.safe_load(f)
    criteria = cfg.get("suspicious_jobs", {})
    patterns = [re.compile(p) for p in criteria.get("cmd_patterns", [])]
    min_hours = float(criteria.get("min_runtime_hours", 1))
    return patterns, min_hours


def _is_suspicious(cmd: str, qdate: int, patterns: list[re.Pattern], min_hours: float) -> bool:
    """Return True when cmd basename matches a pattern AND runtime >= min_hours."""
    if not patterns:
        return False
    basename = Path(cmd).name if cmd else ""
    cmd_match = any(p.match(basename) for p in patterns)
    if not cmd_match:
        return False
    runtime_hours = (datetime.datetime.now().timestamp() - qdate) / 3600
    return runtime_hours >= min_hours


def _fetch_job_info(job_ids: list[str], db_paths: list[str]) -> dict[str, dict]:
    """Query job_info DBs for the given GlobalJobIds; return mapping id -> row dict."""
    if not job_ids or not db_paths:
        return {}
    result: dict[str, dict] = {}
    placeholders = ",".join("?" * len(job_ids))
    for db_path in db_paths:
        try:
            conn = sqlite3.connect(db_path)
            rows = conn.execute(
                f"SELECT GlobalJobId, Cmd, Args, Owner, RequestGPUs, QDate "  # noqa: S608
                f"FROM job_info WHERE GlobalJobId IN ({placeholders})",
                job_ids,
            ).fetchall()
            conn.close()
        except Exception as e:
            print(f"Warning: could not read {db_path}: {e}")
            continue
        for row in rows:
            gid, cmd, args, owner, req_gpus, qdate = row
            result[gid] = {
                "GlobalJobId": gid,
                "Cmd": cmd or "",
                "Args": args or "",
                "Owner": owner or "",
                "RequestGPUs": req_gpus or 0,
                "QDate": qdate or 0,
            }
    return result


def get_open_capacity_jobs_data(
    base_dir: str = ".",
) -> dict:
    """Return recent jobs on claimed open-capacity slots, joined with job_info.

    Looks at the latest gpu_state snapshot (most recent timestamp) to find
    currently claimed open-capacity slots, then joins against job_info DBs
    covering the current and previous month so jobs that started last month
    are still resolved.

    Returns a dict with keys:
        jobs      - list of job dicts (machine, gpu_id, GlobalJobId, Owner,
                    Cmd, Args, runtime_hours, suspicious)
        criteria  - the loaded suspicious criteria for display
    """
    end = get_latest_timestamp_from_most_recent_db(base_dir)
    if end is None:
        return {"jobs": [], "criteria": {}}

    # Use a tight 10-minute window around the latest timestamp so we see only
    # the current snapshot, not hours of history.
    start = end - datetime.timedelta(minutes=10)

    db_paths = get_required_databases(start, end, base_dir)
    if not db_paths:
        return {"jobs": [], "criteria": {}}

    col_select = ", ".join(
        f'"{c}"'
        for c in ["Name", "AssignedGPUs", "State", "PrioritizedProjects", "Machine", "GlobalJobId", "timestamp"]
    )
    frames = []
    for db_path in db_paths:
        try:
            abs_path = str(Path(db_path).resolve())
            query = f"""
                SELECT {col_select} FROM gpu_state
                WHERE timestamp BETWEEN '{start.strftime("%Y-%m-%d %H:%M:%S.%f")}'
                  AND '{end.strftime("%Y-%m-%d %H:%M:%S.%f")}'
            """  # noqa: S608
            df = pl.read_database_uri(query, f"sqlite:///{abs_path}")
            if df.height > 0:
                frames.append(df)
        except Exception as e:
            print(f"Warning: could not load {db_path}: {e}")

    if not frames:
        return {"jobs": [], "criteria": {}}

    df = pl.concat(frames)

    # Filter to claimed, non-backfill, non-prioritized = open capacity
    is_claimed = pl.col("State").str.to_lowercase() == "claimed"
    is_backfill = pl.col("Name").str.to_lowercase().str.contains("backfill")
    has_priority = pl.col("PrioritizedProjects").is_not_null() & (pl.col("PrioritizedProjects") != "")
    df = df.filter(is_claimed & ~is_backfill & ~has_priority)

    if df.height == 0:
        return {"jobs": [], "criteria": {}}

    # Deduplicate: one row per (Machine, AssignedGPUs) — take latest timestamp
    df = df.sort("timestamp", descending=True).unique(subset=["Machine", "AssignedGPUs"], keep="first")

    job_ids = df["GlobalJobId"].drop_nulls().unique().to_list()

    # Query job_info DBs covering end's month and the month before (cross-month)
    prev_month_start = (end.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)
    job_db_paths = _get_job_info_databases(prev_month_start, end, base_dir)
    job_map = _fetch_job_info(job_ids, job_db_paths)

    patterns, min_hours = _load_suspicious_criteria(base_dir)
    path_criteria = {
        "cmd_patterns": [p.pattern for p in patterns],
        "min_runtime_hours": min_hours,
    }

    jobs = []
    for row in df.iter_rows(named=True):
        gid = row.get("GlobalJobId") or ""
        info = job_map.get(gid, {})
        cmd = info.get("Cmd", "")
        qdate = info.get("QDate", 0)
        runtime_hours = round((end.timestamp() - qdate) / 3600, 1) if qdate else None
        jobs.append(
            {
                "machine": row["Machine"],
                "gpu_id": row["AssignedGPUs"],
                "GlobalJobId": gid,
                "Owner": info.get("Owner", ""),
                "Cmd": cmd,
                "Args": info.get("Args", ""),
                "RequestGPUs": info.get("RequestGPUs", 0),
                "runtime_hours": runtime_hours,
                "suspicious": _is_suspicious(cmd, qdate, patterns, min_hours),
            }
        )

    jobs.sort(key=lambda j: (not j["suspicious"], j["machine"]))

    return {"jobs": jobs, "criteria": path_criteria}
