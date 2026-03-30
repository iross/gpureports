#!/usr/bin/env python3
"""
GPU Usage Statistics - Data Loading and Caching

Functions for loading GPU state data from SQLite databases,
filtering by time range, and caching preprocessed DataFrames.
"""

import datetime
import sqlite3

import pandas as pd

from gpu_utils import (
    get_latest_timestamp_from_most_recent_db,
    get_required_databases,
)

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
    # If no cache_key provided, process without caching
    if not cache_key:
        processed_df = df.copy()
        if "timestamp" not in processed_df.columns or not pd.api.types.is_datetime64_any_dtype(
            processed_df["timestamp"]
        ):
            processed_df["timestamp"] = pd.to_datetime(processed_df["timestamp"])
        if "15min_bucket" not in processed_df.columns:
            processed_df["15min_bucket"] = processed_df["timestamp"].dt.floor("15min")
        return processed_df

    # Check cache first
    if cache_key in _dataframe_cache:
        return _dataframe_cache[cache_key]

    # Process and cache - avoid unnecessary operations if already processed
    processed_df = df.copy()

    # Only convert timestamp if not already datetime
    if "timestamp" not in processed_df.columns or not pd.api.types.is_datetime64_any_dtype(processed_df["timestamp"]):
        processed_df["timestamp"] = pd.to_datetime(processed_df["timestamp"])

    # Only add 15min_bucket if not already present
    if "15min_bucket" not in processed_df.columns:
        processed_df["15min_bucket"] = processed_df["timestamp"].dt.floor("15min")

    # Cache the result
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

    # Apply the filter
    filtered_df = filter_func(df, *filter_args)

    # Cache the result if cache_key is provided
    if cache_key:
        _filtered_cache[cache_key] = filtered_df

    return filtered_df


def clear_dataframe_cache():
    """Clear all DataFrame caches to free memory."""
    global _dataframe_cache, _filtered_cache
    _dataframe_cache.clear()
    _filtered_cache.clear()


def get_time_filtered_data(
    db_path: str, hours_back: int = 24, end_time: datetime.datetime | None = None
) -> pd.DataFrame:
    """
    Get GPU state data filtered by time range.
    Automatically handles month boundaries by loading data from multiple database files.

    Args:
        db_path: Path to SQLite database (used to determine base directory for multi-db queries)
        hours_back: Number of hours to look back from end_time
        end_time: End time for the range (defaults to latest timestamp in primary DB)

    Returns:
        DataFrame filtered to the specified time range
    """
    from pathlib import Path

    # Get base directory from the provided db_path
    db_path_obj = Path(db_path)
    base_dir = str(db_path_obj.parent) if db_path_obj.parent != Path(".") else "."

    # If end_time is not provided, use the latest timestamp from the most recent database
    if end_time is None:
        # First try to get the latest timestamp from the most recent database
        end_time = get_latest_timestamp_from_most_recent_db(base_dir)

        # If that fails, fall back to the specified database
        if end_time is None:
            try:
                conn = sqlite3.connect(db_path)
                df_temp = pd.read_sql_query("SELECT MAX(timestamp) as max_time FROM gpu_state", conn)
                conn.close()
                if len(df_temp) > 0 and df_temp["max_time"].iloc[0] is not None:
                    end_time = pd.to_datetime(df_temp["max_time"].iloc[0])
                else:
                    end_time = datetime.datetime.now()
            except Exception:
                # Final fallback to current time if there's any issue with the database
                end_time = datetime.datetime.now()

    # Calculate start time
    start_time = end_time - datetime.timedelta(hours=hours_back)

    # Check if the time range spans multiple months
    start_month = (start_time.year, start_time.month)
    end_month = (end_time.year, end_time.month)

    if start_month == end_month:
        # Single month - optimized approach with SQL-level filtering
        try:
            conn = sqlite3.connect(db_path)
            # OPTIMIZATION: Filter at SQL level instead of loading entire database
            # Note: datetime strings are internal values (not user input), so f-string is safe here.
            # Parameterized queries with pandas+sqlite3 are broken in pandas <2.3.0.
            start_str = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            end_str = end_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            optimized_query = f"""
            SELECT * FROM gpu_state
            WHERE CAST(timestamp AS TEXT) >= '{start_str}' AND CAST(timestamp AS TEXT) <= '{end_str}'
            ORDER BY timestamp
            """
            df = pd.read_sql_query(optimized_query, conn)
            conn.close()

            if len(df) > 0:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df
        except Exception as e:
            # If single-db approach fails, fall back to multi-db approach
            print(f"Warning: Single database query failed, trying multi-database approach: {e}")

    # Multi-month query - use the new multi-database functionality
    try:
        return get_time_filtered_data_multi_db(start_time, end_time, base_dir)
    except Exception as e:
        # Final fallback: try just the specified database file
        print(f"Warning: Multi-database query failed, falling back to single database: {e}")
        try:
            conn = sqlite3.connect(db_path)
            start_str = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            end_str = end_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            query = f"""
            SELECT * FROM gpu_state
            WHERE CAST(timestamp AS TEXT) BETWEEN '{start_str}' AND '{end_str}'
            ORDER BY timestamp
            """
            df = pd.read_sql_query(query, conn)
            conn.close()
            if len(df) > 0:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df
        except Exception as final_e:
            print(f"Error: All database query methods failed: {final_e}")
            return pd.DataFrame()


def get_multi_db_data(db_paths: list, start_time: datetime.datetime, end_time: datetime.datetime) -> pd.DataFrame:
    """
    Load and merge data from multiple database files.

    Args:
        db_paths: List of database file paths
        start_time: Start time for filtering
        end_time: End time for filtering

    Returns:
        Combined DataFrame with data from all databases, filtered by time range
    """
    if not db_paths:
        return pd.DataFrame()

    all_dataframes = []

    # Add a small buffer to start_time to handle microsecond precision issues
    # This ensures we don't miss data due to tiny timing differences
    buffered_start = start_time - datetime.timedelta(seconds=1)

    for db_path in db_paths:
        try:
            conn = sqlite3.connect(db_path)
            buf_str = buffered_start.strftime("%Y-%m-%d %H:%M:%S.%f")
            end_str = end_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            query = f"""
            SELECT * FROM gpu_state
            WHERE CAST(timestamp AS TEXT) BETWEEN '{buf_str}' AND '{end_str}'
            ORDER BY timestamp
            """
            df = pd.read_sql_query(query, conn)
            conn.close()

            if len(df) > 0:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                # Apply the precise time filtering after loading, since we used a buffered start
                df = df[(df["timestamp"] >= start_time) & (df["timestamp"] <= end_time)]
                if len(df) > 0:
                    all_dataframes.append(df)

        except Exception as e:
            print(f"Warning: Could not load data from {db_path}: {e}")
            continue

    if not all_dataframes:
        return pd.DataFrame()

    # Combine all dataframes
    combined_df = pd.concat(all_dataframes, ignore_index=True)

    # Sort by timestamp to ensure proper ordering
    combined_df = combined_df.sort_values("timestamp").reset_index(drop=True)

    # Apply final time filtering to handle any edge cases
    combined_df = combined_df[(combined_df["timestamp"] >= start_time) & (combined_df["timestamp"] <= end_time)]

    return combined_df


def get_time_filtered_data_multi_db(
    start_time: datetime.datetime, end_time: datetime.datetime, base_dir: str = "."
) -> pd.DataFrame:
    """
    Get GPU state data filtered by time range, automatically handling multiple database files.

    Args:
        start_time: Start time for the range
        end_time: End time for the range
        base_dir: Directory containing database files (defaults to current directory)

    Returns:
        DataFrame filtered to the specified time range from all relevant databases
    """
    # Discover required database files
    db_paths = get_required_databases(start_time, end_time, base_dir)

    if not db_paths:
        raise FileNotFoundError(f"No database files found for time range {start_time} to {end_time}")

    # Load and combine data
    return get_multi_db_data(db_paths, start_time, end_time)
