"""Data layer for the GPU state dashboard.

Queries gpu_state Parquet (or, for months predating the migration, SQLite)
files, dedups/classifies via stats_calculations.prepare_frames(), and
returns structured dicts matching the API response shape.
"""

import datetime
import logging
import sqlite3
from pathlib import Path

import polars as pl

import gpu_utils
from gpu_utils_polars import (
    get_latest_timestamp_from_most_recent_parquet as get_latest_timestamp_from_most_recent_db,
)
from gpu_utils_polars import (
    get_required_parquet_files as get_required_databases,
)
from stats_calculations import prepare_frames, slot_dedup_rank
from stats_data import GPU_STATE_SCHEMA, SCAN_CAST_OPTIONS

logger = logging.getLogger(__name__)

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
    "GPUs_GlobalMemoryMb",
    "RemoteOwner",
    "PreventJobsReason",
    "timestamp",
]


def _set_host_exclusions(base_dir: str = ".") -> None:
    """Load masked_hosts.yaml into gpu_utils.HOST_EXCLUSIONS, the global
    prepare_frames() reads -- mirrors the pattern report.py/usage_stats.py
    use before calling it."""
    yaml_path = Path(base_dir) / "masked_hosts.yaml"
    gpu_utils.HOST_EXCLUSIONS = gpu_utils.load_host_exclusions(None, str(yaml_path) if yaml_path.exists() else None)


def _file_month_label(path: str) -> str:
    """Extract the YYYY-MM month label from a gpu_state_<month>.<ext> filename."""
    return Path(path).stem.removeprefix("gpu_state_")


def _read_sqlite(path: str, query: str) -> pl.DataFrame:
    """Run a query against a SQLite file and return the result as a Polars DataFrame."""
    conn = sqlite3.connect(str(Path(path).resolve()))
    try:
        return pl.read_database(query, conn)
    finally:
        conn.close()


def _query_dbs(
    file_specs: list[tuple[str, str]], start: datetime.datetime, end: datetime.datetime
) -> tuple[pl.DataFrame, list[str]]:
    """Load data from Parquet and/or SQLite files and combine into one Polars DataFrame.

    file_specs is a list of (path, format) tuples where format is "parquet" or "sqlite".

    Returns (combined DataFrame, list of warning messages for files that failed to load).
    """
    if not file_specs:
        return pl.DataFrame(), []

    frames = []
    warnings: list[str] = []
    buffered_start = start - datetime.timedelta(seconds=1)
    col_select = ", ".join(f'"{c}"' for c in COLUMNS)

    for path, fmt in file_specs:
        try:
            if fmt == "parquet":
                df = (
                    pl.scan_parquet(
                        path, schema=GPU_STATE_SCHEMA, missing_columns="insert", cast_options=SCAN_CAST_OPTIONS
                    )
                    .filter((pl.col("timestamp") >= buffered_start) & (pl.col("timestamp") <= end))
                    .select(COLUMNS)
                    .collect()
                )
            else:
                query = (
                    f"SELECT {col_select} FROM gpu_state "
                    f"WHERE timestamp BETWEEN '{buffered_start.strftime('%Y-%m-%d %H:%M:%S.%f')}' "
                    f"  AND '{end.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
                )
                df = _read_sqlite(path, query)
            if df.height > 0:
                frames.append(df)
        except Exception as e:
            logger.warning("Could not load %s: %s", path, e)
            warnings.append(f"Failed to load data for {_file_month_label(path)}: {e}")

    if not frames:
        return pl.DataFrame(), warnings

    combined = pl.concat(frames, how="diagonal_relaxed")
    # Parquet yields Datetime; SQLite TEXT yields Utf8 — normalise to Datetime.
    if combined.schema["timestamp"] in (pl.Utf8, pl.String):
        combined = combined.with_columns(pl.col("timestamp").str.to_datetime(strict=False))
    elif combined.schema["timestamp"] != pl.Datetime("us"):
        combined = combined.with_columns(pl.col("timestamp").cast(pl.Datetime("us")))
    combined = combined.filter((pl.col("timestamp") >= start) & (pl.col("timestamp") <= end))
    return combined, warnings


def _collapse_to_bucket_winner(dedup: pl.DataFrame) -> pl.DataFrame:
    """Pick one winning row per (bucket, AssignedGPUs).

    prepare_frames() dedups at raw-timestamp granularity, so when the
    requested bucket is wider than the raw collection interval, multiple
    per-timestamp winners can still land in the same display bucket. Collapse
    those using the same canonical rank, then rename to the "time_bucket"
    label the rest of this module uses.
    """
    prev_idle = pl.col("_pj_set") & (pl.col("State") != "Claimed")
    rank = slot_dedup_rank(pl.col("_is_bf"), pl.col("State"), prev_idle)
    return (
        dedup.with_columns(rank.alias("_rank"))
        .sort(["bucket", "AssignedGPUs", "_rank"], descending=[False, False, True])
        .unique(subset=["bucket", "AssignedGPUs"], keep="first")
        .drop("_rank")
        .rename({"bucket": "time_bucket"})
    )


def _map_state_codes(df: pl.DataFrame) -> pl.DataFrame:
    """Map prepare_frames()'s canonical classification columns to the
    dashboard's 6 STATE_CODES. CHTC-owned priority slots are folded into the
    same prioritized bucket as researcher-owned, and interactive slots are
    not distinguished -- matching this module's pre-existing 6-state
    vocabulary (see TASK-49.1)."""
    claimed = pl.col("State") == "Claimed"
    unclaimed = pl.col("State") == "Unclaimed"
    is_bf = pl.col("_is_bf")
    is_prio = pl.col("_pp_prio")
    return df.with_columns(
        pl.when(claimed & is_bf)
        .then(pl.lit(STATE_CODES["busy_backfill"]))
        .when(claimed & is_prio)
        .then(pl.lit(STATE_CODES["busy_prioritized"]))
        .when(claimed)
        .then(pl.lit(STATE_CODES["busy_shared"]))
        .when(unclaimed & is_bf)
        .then(pl.lit(STATE_CODES["idle_backfill"]))
        .when(unclaimed & is_prio)
        .then(pl.lit(STATE_CODES["idle_prioritized"]))
        .when(unclaimed)
        .then(pl.lit(STATE_CODES["idle_shared"]))
        .otherwise(pl.lit(STATE_CODES["na"]))
        .alias("state_code")
    )


def _prepare_bucketed(
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    bucket_minutes: int,
    base_dir: str,
    hours: int = 24,
) -> tuple[pl.DataFrame, list, list[str], list[str]] | None:
    """Load, mask, dedup, bucket, and classify data.

    Returns (df, all_buckets, bucket_strs, warnings) or None if no data is available.
    """
    if end is None:
        end = get_latest_timestamp_from_most_recent_db(base_dir)
        if end is None:
            return None
    if start is None:
        start = end - datetime.timedelta(hours=hours)

    db_paths = get_required_databases(start, end, base_dir)
    if not db_paths:
        return None

    df, warnings = _query_dbs(db_paths, start, end)
    if df.height == 0:
        return df, [], [], warnings

    _set_host_exclusions(base_dir)
    frames = prepare_frames(df.lazy(), bucket_minutes=bucket_minutes)
    dedup = frames.dedup.collect()
    if dedup.height == 0:
        return dedup, [], [], warnings

    df = _map_state_codes(_collapse_to_bucket_winner(dedup))

    all_buckets = sorted(df["time_bucket"].unique().to_list())
    bucket_strs = [t.strftime("%Y-%m-%dT%H:%M") for t in all_buckets]

    return df, all_buckets, bucket_strs, warnings


def get_heatmap_data(
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
    bucket_minutes: int = 15,
    base_dir: str = ".",
    hours: int = 24,
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
    prepared = _prepare_bucketed(start, end, bucket_minutes, base_dir, hours)
    if prepared is None:
        return _empty_heatmap_response()

    df, all_buckets, bucket_strs, warnings = prepared
    if df.height == 0:
        return _empty_heatmap_response(warnings)
    bucket_index = {t: i for i, t in enumerate(all_buckets)}

    # Build machine-grouped structure. Sort nulls last before taking one row per
    # GPU so a bucket where GPUs_DeviceName happened to be unreported doesn't
    # arbitrarily win over a bucket where it was (previously order-dependent).
    gpu_info = (
        df.select("Machine", "AssignedGPUs", "GPUs_DeviceName")
        .sort("GPUs_DeviceName", nulls_last=True)
        .unique(subset=["Machine", "AssignedGPUs"], keep="first")
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
        "warnings": warnings,
    }


def get_counts_data(
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
    bucket_minutes: int = 15,
    base_dir: str = ".",
    hours: int = 24,
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
    if end is None:
        end = get_latest_timestamp_from_most_recent_db(base_dir)
        if end is None:
            return _empty_counts_response()
    if start is None:
        start = end - datetime.timedelta(hours=hours)

    db_paths = get_required_databases(start, end, base_dir)
    if not db_paths:
        return _empty_counts_response()

    df_raw, warnings = _query_dbs(db_paths, start, end)
    if df_raw.height == 0:
        return _empty_counts_response(warnings)

    _set_host_exclusions(base_dir)
    frames = prepare_frames(df_raw.lazy(), bucket_minutes=bucket_minutes)

    # Count each slot type independently off pre-dedup, host-excluded rows.
    #
    # A GPU can appear in BOTH a primary slot and a backfill slot simultaneously.
    # Dedup merges these, causing GPUs to disappear from primary totals when
    # their backfill slot wins the rank competition (e.g. backfill-claimed > primary-idle).
    # Counting each slot type independently avoids this entirely.
    primary_df = frames.raw.filter(
        ~pl.col("_excluded") & ~pl.col("_is_bf") & pl.col("AssignedGPUs").is_not_null()
    ).collect()
    backfill_df = frames.raw_bf.collect()

    is_claimed = pl.col("State") == "Claimed"
    has_priority = pl.col("_pp_prio")

    all_buckets = sorted(
        pl.concat([primary_df.select("bucket"), backfill_df.select("bucket")], how="vertical")
        .unique()["bucket"]
        .to_list()
    )
    bucket_strs = [t.strftime("%Y-%m-%dT%H:%M") for t in all_buckets]
    buckets_df = pl.DataFrame({"bucket": all_buckets})

    def _count_series(df: pl.DataFrame, total_mask: pl.Expr, claimed_mask: pl.Expr) -> dict[str, list]:
        total_df = df.filter(total_mask).group_by("bucket").agg(pl.col("AssignedGPUs").n_unique().alias("total"))
        claimed_df = (
            df.filter(total_mask & claimed_mask)
            .group_by("bucket")
            .agg(pl.col("AssignedGPUs").n_unique().alias("claimed"))
        )
        merged = (
            buckets_df.join(total_df, on="bucket", how="left")
            .join(claimed_df, on="bucket", how="left")
            .fill_null(0)
            .sort("bucket")
        )
        return {"total": merged["total"].to_list(), "claimed": merged["claimed"].to_list()}

    series: dict[str, dict[str, list]] = {
        "prioritized": _count_series(primary_df, has_priority, is_claimed),
        "open_capacity": _count_series(primary_df, ~has_priority, is_claimed),
        "backfill": _count_series(backfill_df, pl.lit(True), is_claimed),
    }

    return {"buckets": bucket_strs, "series": series, "warnings": warnings}


def _empty_heatmap_response(warnings: list[str] | None = None) -> dict:
    return {
        "time_buckets": [],
        "machines": [],
        "state_map": STATE_MAP,
        "state_colors": STATE_COLORS,
        "warnings": warnings or [],
    }


def get_opencap_users_data(
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
    bucket_minutes: int = 15,
    base_dir: str = ".",
    hours: int = 24,
    top_n: int = 6,
) -> dict:
    """Bucketed per-user open-cap GPU counts, anonymized by peak-usage rank.

    Returns dict with 'buckets' (ISO time strings) and 'series' (anonymized
    label -> list of max GPU count per bucket). User names are never returned.
    """
    if end is None:
        end = get_latest_timestamp_from_most_recent_db(base_dir)
        if end is None:
            return {"buckets": [], "series": {}, "warnings": []}
    if start is None:
        start = end - datetime.timedelta(hours=hours)

    db_paths = get_required_databases(start, end, base_dir)
    if not db_paths:
        return {"buckets": [], "series": {}, "warnings": []}

    df_raw, warnings = _query_dbs(db_paths, start, end)
    if df_raw.height == 0:
        return {"buckets": [], "series": {}, "warnings": warnings}

    _set_host_exclusions(base_dir)
    # bucket_minutes=1 keeps prepare_frames()'s internal "bucket" column at
    # effectively raw-snapshot granularity (the collection interval is 5
    # minutes) so per-snapshot GPU counts aren't merged before the
    # peak-within-display-bucket step below.
    frames = prepare_frames(df_raw.lazy(), bucket_minutes=1)

    has_owner = pl.col("RemoteOwner").is_not_null() & (pl.col("RemoteOwner") != "")
    df = frames.dedup.filter(
        (pl.col("State") == "Claimed") & ~pl.col("_is_bf") & ~pl.col("_pp_prio") & has_owner
    ).collect()

    if df.height == 0:
        return {"buckets": [], "series": {}, "warnings": warnings}

    # Count distinct GPUs per (snapshot, owner), then take max within the
    # requested display bucket. Using max (not sum) matches the script:
    # "peak snapshot within the window".
    snap = df.group_by(["bucket", "RemoteOwner"]).agg(pl.col("AssignedGPUs").n_unique().alias("snap_count"))
    snap = snap.with_columns(pl.col("bucket").dt.truncate(f"{bucket_minutes}m").alias("display_bucket"))
    bucketed = snap.group_by(["display_bucket", "RemoteOwner"]).agg(pl.col("snap_count").max().alias("gpu_count"))
    bucketed = bucketed.rename({"display_bucket": "bucket"})

    all_buckets = sorted(bucketed["bucket"].unique().to_list())
    bucket_strs = [t.strftime("%Y-%m-%dT%H:%M") for t in all_buckets]
    bucket_index = {b: i for i, b in enumerate(all_buckets)}
    n_buckets = len(all_buckets)

    # Rank users by peak GPU count; anonymize as "User 1", "User 2", … Break ties
    # on RemoteOwner so equal-peak users get a deterministic (if arbitrary) label
    # instead of one that depends on incidental row order upstream.
    top_users = (
        bucketed.group_by("RemoteOwner")
        .agg(pl.col("gpu_count").max().alias("peak"))
        .sort(["peak", "RemoteOwner"], descending=[True, False])
        .head(top_n)["RemoteOwner"]
        .to_list()
    )
    user_to_label = {u: f"User {i + 1}" for i, u in enumerate(top_users)}
    series: dict[str, list[int]] = {f"User {i + 1}": [0] * n_buckets for i in range(len(top_users))}

    for row in bucketed.filter(pl.col("RemoteOwner").is_in(top_users)).iter_rows(named=True):
        label = user_to_label[row["RemoteOwner"]]
        bi = bucket_index.get(row["bucket"])
        if bi is not None:
            series[label][bi] = row["gpu_count"]

    return {"buckets": bucket_strs, "series": series, "warnings": warnings}


def _empty_counts_response(warnings: list[str] | None = None) -> dict:
    return {
        "buckets": [],
        "series": {
            "prioritized": {"total": [], "claimed": []},
            "open_capacity": {"total": [], "claimed": []},
            "backfill": {"total": [], "claimed": []},
        },
        "warnings": warnings or [],
    }
