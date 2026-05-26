#!/usr/bin/env python3
"""
GPU Usage Statistics - Data Loading and Caching

Functions for loading GPU state data from Parquet files via DuckDB,
filtering by time range, and caching preprocessed DataFrames.
"""

import datetime
import os

import duckdb
import pandas as pd

# Global cache for preprocessed DataFrames and filtered datasets to avoid repeated work
_dataframe_cache = {}
_filtered_cache = {}


def get_preprocessed_dataframe(df: pd.DataFrame, cache_key: str = None) -> pd.DataFrame:
    """
    Get a preprocessed DataFrame with common operations applied, using caching to avoid repeated work.
    Optimized to avoid multiple copies and improve cache effectiveness.

    Args:
        df: Input DataFrame
        cache_key: Optional cache key to avoid reprocessing the same data

    Returns:
        DataFrame with timestamp conversion and 15-minute buckets added
    """
    if not cache_key:
        processed_df = df.copy()
        if "timestamp" not in processed_df.columns or not pd.api.types.is_datetime64_any_dtype(
            processed_df["timestamp"]
        ):
            processed_df["timestamp"] = pd.to_datetime(processed_df["timestamp"])
        if "15min_bucket" not in processed_df.columns:
            processed_df["15min_bucket"] = processed_df["timestamp"].dt.floor("15min")
        return processed_df

    if cache_key in _dataframe_cache:
        return _dataframe_cache[cache_key]

    processed_df = df.copy()

    if "timestamp" not in processed_df.columns or not pd.api.types.is_datetime64_any_dtype(processed_df["timestamp"]):
        processed_df["timestamp"] = pd.to_datetime(processed_df["timestamp"])

    if "15min_bucket" not in processed_df.columns:
        processed_df["15min_bucket"] = processed_df["timestamp"].dt.floor("15min")

    _dataframe_cache[cache_key] = processed_df
    return processed_df


def get_cached_filtered_dataframe(df: pd.DataFrame, filter_func, filter_args, cache_key: str = None) -> pd.DataFrame:
    """
    Get a filtered DataFrame with caching to avoid repeated filtering operations.

    Args:
        df: Input DataFrame
        filter_func: Filtering function to apply
        filter_args: Arguments for the filtering function
        cache_key: Optional cache key to avoid reprocessing the same filter

    Returns:
        Filtered DataFrame
    """
    if cache_key and cache_key in _filtered_cache:
        return _filtered_cache[cache_key]

    filtered_df = filter_func(df, *filter_args)

    if cache_key:
        _filtered_cache[cache_key] = filtered_df

    return filtered_df


def clear_dataframe_cache():
    """Clear all DataFrame caches to free memory."""
    global _dataframe_cache, _filtered_cache
    _dataframe_cache.clear()
    _filtered_cache.clear()


def _parquet_glob(base_dir: str) -> str:
    return os.path.join(os.path.abspath(base_dir), "gpu_state_*.parquet")


def get_latest_timestamp(data_dir: str) -> datetime.datetime | None:
    """Return the latest timestamp across all gpu_state Parquet files in data_dir."""
    glob = _parquet_glob(data_dir)
    try:
        con = duckdb.connect()
        row = con.execute(f"SELECT MAX(timestamp) FROM parquet_scan('{glob}', hive_partitioning=false)").fetchone()
        con.close()
        if row and row[0] is not None:
            ts = pd.to_datetime(row[0])
            return ts.to_pydatetime().replace(tzinfo=None)
    except Exception:
        pass
    return None


def get_time_filtered_data(
    data_dir: str, hours_back: int = 24, end_time: datetime.datetime | None = None
) -> pd.DataFrame:
    """
    Get GPU state data filtered by time range from Parquet files via DuckDB.
    Automatically covers month boundaries by globbing all gpu_state_*.parquet files
    in data_dir.

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

    glob = _parquet_glob(data_dir)
    start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    # Note: datetime strings are derived from internal datetime objects, not user input.
    query = (
        f"SELECT * FROM parquet_scan('{glob}', hive_partitioning=false) "
        f"WHERE timestamp >= '{start_str}' AND timestamp <= '{end_str}' "
        f"ORDER BY timestamp"
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
