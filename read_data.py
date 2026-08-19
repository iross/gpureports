#!/usr/bin/env python3
"""
GPU state reading: Parquet/SQLite loading, file discovery, and config input

Polars lazy scans over gpu_state Parquet files for the aggregation pipeline,
plus a pandas loader retained for legacy consumers (timeseries/snapshot paths
and standalone scripts). Also owns per-month gpu_state file discovery
(Parquet, with SQLite fallback for months predating the TASK-31 migration)
and loading the host-exclusion/CHTC-owned-hosts config files that feed the
classification pipeline in classify_slots.py.
"""

import datetime
import glob as globlib
import os
import sqlite3
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import yaml

# Cache for load_chtc_owned_hosts(), populated on first call.
_CHTC_OWNED_HOSTS = None

# Schema of gpu_state Parquet files. Passed explicitly to scan_parquet() so that
# files from different collector generations - which can have columns in different
# orders, or predate later-added columns like PreventJobsReason - resolve by name
# instead of erroring on order/column-set mismatches. Also used to construct an
# empty frame when a directory contains no data files.
GPU_STATE_SCHEMA = {
    "Name": pl.Utf8,
    "AssignedGPUs": pl.Utf8,
    "AvailableGPUs": pl.Utf8,
    "State": pl.Utf8,
    "GPUs_DeviceName": pl.Utf8,
    "GPUs_GlobalMemoryMb": pl.Int64,
    "PrioritizedProjects": pl.Utf8,
    "GPUsAverageUsage": pl.Float64,
    "Machine": pl.Utf8,
    "RemoteOwner": pl.Utf8,
    "GlobalJobId": pl.Utf8,
    "PreventJobsReason": pl.Utf8,
    "Disk": pl.Int64,
    "timestamp": pl.Datetime("us"),
}

# Some Parquet writers (e.g. pandas' default) emit nanosecond-precision timestamps
# instead of the microsecond precision in GPU_STATE_SCHEMA; allow that downcast
# rather than erroring when scanning files from mixed sources.
SCAN_CAST_OPTIONS = pl.ScanCastOptions(datetime_cast="nanosecond-downcast")


def load_chtc_owned_hosts(chtc_owned_file: str = "chtc_owned") -> set:
    """
    Load CHTC owned hosts from file.

    Args:
        chtc_owned_file: Path to file containing CHTC owned host names

    Returns:
        Set of CHTC owned host names
    """
    global _CHTC_OWNED_HOSTS

    if _CHTC_OWNED_HOSTS is not None:
        return _CHTC_OWNED_HOSTS

    chtc_owned_hosts = set()
    chtc_owned_path = Path(chtc_owned_file)

    if chtc_owned_path.exists():
        try:
            with open(chtc_owned_path) as f:
                for line in f:
                    host = line.strip()
                    if host:  # Skip empty lines
                        chtc_owned_hosts.add(host)
        except Exception as e:
            print(f"Warning: Could not load CHTC owned hosts from {chtc_owned_file}: {e}")
    else:
        print(f"Warning: CHTC owned file {chtc_owned_file} not found")

    _CHTC_OWNED_HOSTS = chtc_owned_hosts
    return chtc_owned_hosts


def load_host_exclusions(exclusions_config: str | None = None, yaml_file: str | None = None) -> dict[str, str]:
    """
    Load host exclusion configuration from YAML file or string.

    Args:
        exclusions_config: Optional string with exclusion configuration
        yaml_file: Optional path to YAML file with exclusions

    Returns:
        Dictionary mapping excluded host patterns to reasons
    """
    exclusions = {}

    if yaml_file and Path(yaml_file).exists():
        try:
            with open(yaml_file) as f:
                data = yaml.safe_load(f)
                if data and "excluded_hosts" in data:
                    exclusions = data["excluded_hosts"]
        except Exception as e:
            print(f"Warning: Could not load exclusions from {yaml_file}: {e}")

    if exclusions_config:
        try:
            data = yaml.safe_load(exclusions_config)
            if data and "excluded_hosts" in data:
                exclusions.update(data["excluded_hosts"])
        except Exception as e:
            print(f"Warning: Could not parse exclusions config: {e}")

    return exclusions


def get_preprocessed_dataframe(df: pd.DataFrame, cache_key: str = None) -> pd.DataFrame:
    """
    Get a pandas DataFrame with timestamp conversion and 15-minute buckets added.

    Args:
        df: Input DataFrame
        cache_key: Ignored, retained for call-site compatibility

    Returns:
        DataFrame with timestamp conversion and 15-minute buckets added
    """
    processed_df = df.copy()
    if "timestamp" not in processed_df.columns or not pd.api.types.is_datetime64_any_dtype(processed_df["timestamp"]):
        processed_df["timestamp"] = pd.to_datetime(processed_df["timestamp"])
    if "15min_bucket" not in processed_df.columns:
        processed_df["15min_bucket"] = processed_df["timestamp"].dt.floor("15min")
    return processed_df


def parquet_glob(base_dir: str) -> str:
    return os.path.join(os.path.abspath(base_dir), "gpu_state_*.parquet")


def get_required_parquet_files(
    start_time: datetime.datetime, end_time: datetime.datetime, base_dir: str = "."
) -> list[tuple[str, str]]:
    """Return (path, format) pairs for each month in [start_time, end_time].

    Prefers Parquet over SQLite for each month; skips months with neither.
    Format is "parquet" or "sqlite".
    """
    files = []
    current = start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_month = end_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current <= end_month:
        stem = f"gpu_state_{current.strftime('%Y-%m')}"
        parquet = Path(base_dir) / f"{stem}.parquet"
        sqlite = Path(base_dir) / f"{stem}.db"
        if parquet.exists():
            files.append((str(parquet), "parquet"))
        elif sqlite.exists():
            files.append((str(sqlite), "sqlite"))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return files


def get_most_recent_parquet(base_dir: str = ".") -> str | None:
    """Return path to the most recent gpu_state Parquet file, or None."""
    files = sorted(globlib.glob(str(Path(base_dir) / "gpu_state_*.parquet")))
    return files[-1] if files else None


def get_latest_timestamp_from_most_recent_parquet(base_dir: str = ".") -> datetime.datetime | None:
    """Return the latest timestamp across the most recent Parquet (or SQLite fallback)."""
    parquet = get_most_recent_parquet(base_dir)
    if parquet:
        try:
            ts = pl.scan_parquet(parquet).select(pl.col("timestamp").max()).collect()["timestamp"][0]
            if ts is not None:
                return ts.replace(tzinfo=None) if hasattr(ts, "tzinfo") else ts
        except Exception:
            pass

    # Fall back to SQLite
    return get_latest_timestamp_from_most_recent_db(base_dir)


def get_required_databases(start_time: datetime.datetime, end_time: datetime.datetime, base_dir: str = ".") -> list:
    """
    Get list of SQLite database files needed to cover the specified time range.

    Args:
        start_time: Start of time range
        end_time: End of time range
        base_dir: Directory containing database files

    Returns:
        List of database file paths
    """
    db_files = []

    current = start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_month = end_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    while current <= end_month:
        db_file = Path(base_dir) / f"gpu_state_{current.strftime('%Y-%m')}.db"
        if db_file.exists():
            db_files.append(str(db_file))

        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return db_files


def get_most_recent_database(base_dir: str = ".") -> str | None:
    """
    Find the most recent SQLite database file in the given directory.

    Args:
        base_dir: Directory to search for database files

    Returns:
        Path to the most recent database file, or None if none found
    """
    pattern = str(Path(base_dir) / "gpu_state_*.db")
    db_files = globlib.glob(pattern)

    if not db_files:
        return None

    db_files.sort()
    return db_files[-1]


def get_latest_timestamp_from_most_recent_db(base_dir: str = ".") -> datetime.datetime | None:
    """
    Get the latest timestamp from the most recent SQLite database file.

    Args:
        base_dir: Directory containing database files

    Returns:
        Latest timestamp from the most recent database, or None if not found
    """
    most_recent_db = get_most_recent_database(base_dir)
    if not most_recent_db:
        return None

    try:
        conn = sqlite3.connect(most_recent_db)
        df = pl.read_database("SELECT MAX(timestamp) as max_time FROM gpu_state", conn)
        conn.close()

        if len(df) > 0 and df["max_time"][0] is not None:
            max_time = df["max_time"][0]
            if isinstance(max_time, str):
                return datetime.datetime.fromisoformat(max_time)
            return max_time
    except Exception:
        pass

    return None


def get_latest_timestamp(data_dir: str) -> datetime.datetime | None:
    """Return the latest timestamp across all gpu_state Parquet files in data_dir."""
    files = globlib.glob(parquet_glob(data_dir))
    if not files:
        return None
    try:
        row = (
            pl.scan_parquet(files, schema=GPU_STATE_SCHEMA, missing_columns="insert", cast_options=SCAN_CAST_OPTIONS)
            .select(pl.col("timestamp").max())
            .collect()
            .item()
        )
        if row is not None:
            return pd.to_datetime(row).to_pydatetime().replace(tzinfo=None)
    except Exception as e:
        print(f"Error: could not read latest timestamp from {parquet_glob(data_dir)}: {e}")
    return None


def scan_time_filtered(
    data_dir: str, hours_back: float = 24, end_time: datetime.datetime | None = None
) -> pl.LazyFrame:
    """
    Lazily scan gpu_state Parquet files filtered to a time range.

    Nothing is read until the returned LazyFrame is collected, so downstream
    aggregations benefit from projection and predicate pushdown instead of
    materializing the full window.

    Args:
        data_dir: Directory containing gpu_state_*.parquet files
        hours_back: Number of hours to look back from end_time
        end_time: End time for the range (defaults to latest timestamp across all Parquet files)

    Returns:
        LazyFrame filtered to the specified time range (inclusive on both ends)
    """
    if end_time is None:
        end_time = get_latest_timestamp(data_dir)
        if end_time is None:
            end_time = datetime.datetime.now()

    start_time = end_time - datetime.timedelta(hours=hours_back)

    files = globlib.glob(parquet_glob(data_dir))
    if not files:
        return pl.DataFrame(schema=GPU_STATE_SCHEMA).lazy()

    return pl.scan_parquet(
        files, schema=GPU_STATE_SCHEMA, missing_columns="insert", cast_options=SCAN_CAST_OPTIONS
    ).filter(pl.col("timestamp").is_between(start_time, end_time))


def get_time_filtered_data(
    data_dir: str, hours_back: int = 24, end_time: datetime.datetime | None = None
) -> pd.DataFrame:
    """
    Get GPU state data for a time range as a pandas DataFrame.

    Retained for consumers of the pandas calculation paths (timeseries,
    non-device allocation, GPU model snapshots, and standalone scripts).
    Materializes the full window — use scan_time_filtered for large ranges.

    Args:
        data_dir: Directory containing gpu_state_*.parquet files
        hours_back: Number of hours to look back from end_time
        end_time: End time for the range (defaults to latest timestamp across all Parquet files)

    Returns:
        DataFrame filtered to the specified time range
    """
    if end_time is None:
        end_time = get_latest_timestamp(data_dir)
        if end_time is None:
            end_time = datetime.datetime.now()

    start_time = end_time - datetime.timedelta(hours=hours_back)

    glob = parquet_glob(data_dir)
    start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    # Note: datetime strings are derived from internal datetime objects, not user input.
    # No ORDER BY: sorting 50M+ rows forces an external sort that can spill to disk,
    # and all downstream calculations group by time bucket rather than relying on row order.
    query = (
        f"SELECT * FROM parquet_scan('{glob}', hive_partitioning=false, union_by_name=true) "
        f"WHERE timestamp >= '{start_str}' AND timestamp <= '{end_str}'"
    )
    try:
        con = duckdb.connect()
        df = con.execute(query).df()
        con.close()
        if len(df) > 0:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        print(f"Error: DuckDB parquet query failed: {e}")
        return pd.DataFrame()


def get_draining_data(data_dir: str, hours_back: int = 24, end_time: datetime.datetime | None = None) -> pl.DataFrame:
    """
    Get GPU draining data (State='Drained') for the specified time range.
    Only includes GPUs that are drained and NOT claimed by any slot at that timestamp.

    Args:
        data_dir: Directory containing gpu_state_*.parquet files
        hours_back: Number of hours to look back from end_time
        end_time: End time for the range (defaults to latest timestamp across all Parquet files)

    Returns:
        DataFrame with draining data (Machine, AssignedGPUs, timestamp)
    """
    lf = scan_time_filtered(data_dir, hours_back, end_time)
    base = lf.filter(pl.col("AssignedGPUs").is_not_null()).select("Machine", "AssignedGPUs", "State", "timestamp")
    drained = base.filter(pl.col("State") == "Drained").select("Machine", "AssignedGPUs", "timestamp").unique()
    claimed = base.filter(pl.col("State") == "Claimed").select("Machine", "AssignedGPUs", "timestamp").unique()
    return (
        drained.join(claimed, on=["Machine", "AssignedGPUs", "timestamp"], how="anti")
        .sort(["Machine", "timestamp"])
        .collect(engine="streaming")
    )
