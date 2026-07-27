#!/usr/bin/env python3
"""
GPU Usage Statistics - Data Loading

Polars lazy scans over gpu_state Parquet files for the aggregation pipeline,
plus a pandas loader retained for legacy consumers (timeseries/snapshot paths
and standalone scripts).
"""

import datetime
import glob as globlib
import os

import duckdb
import pandas as pd
import polars as pl

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
    "timestamp": pl.Datetime("us"),
}

# Some Parquet writers (e.g. pandas' default) emit nanosecond-precision timestamps
# instead of the microsecond precision in GPU_STATE_SCHEMA; allow that downcast
# rather than erroring when scanning files from mixed sources.
_SCAN_CAST_OPTIONS = pl.ScanCastOptions(datetime_cast="nanosecond-downcast")


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


def get_latest_timestamp(data_dir: str) -> datetime.datetime | None:
    """Return the latest timestamp across all gpu_state Parquet files in data_dir."""
    files = globlib.glob(parquet_glob(data_dir))
    if not files:
        return None
    try:
        row = (
            pl.scan_parquet(files, schema=GPU_STATE_SCHEMA, missing_columns="insert", cast_options=_SCAN_CAST_OPTIONS)
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
        files, schema=GPU_STATE_SCHEMA, missing_columns="insert", cast_options=_SCAN_CAST_OPTIONS
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
