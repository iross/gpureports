#!/usr/bin/env python3
"""
GPU Usage Statistics Calculator - Polars Version

Core data processing functions using Polars for performance.
This module contains the 5 most performance-critical functions migrated from pandas to Polars.

For reporting, HTML generation, and email functions, see usage_stats.py (pandas version).
"""

import datetime
import sqlite3
from pathlib import Path

import polars as pl

# Import shared utilities (Polars versions)
from gpu_utils_polars import (
    UTILIZATION_TYPES,
    filter_df,
    get_latest_timestamp_from_most_recent_db,
    get_required_databases,
)

# Global cache for preprocessed DataFrames
_dataframe_cache = {}
_filtered_cache = {}


def get_preprocessed_dataframe(df: pl.DataFrame, cache_key: str | None = None) -> pl.DataFrame:
    """
    Get a preprocessed DataFrame with common operations applied, using caching to avoid repeated work.

    Args:
        df: Input Polars DataFrame
        cache_key: Optional cache key to avoid reprocessing the same data

    Returns:
        Polars DataFrame with timestamp conversion and 15-minute buckets added
    """
    # If no cache_key provided, process without caching
    if not cache_key:
        processed_df = df.clone()

        # Convert timestamp if needed
        if "timestamp" not in processed_df.columns or processed_df.schema["timestamp"] != pl.Datetime:
            # Check if timestamp is string and needs parsing
            if processed_df.schema.get("timestamp") == pl.Utf8:
                processed_df = processed_df.with_columns(
                    pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S.%f")
                )
            else:
                processed_df = processed_df.with_columns(pl.col("timestamp").cast(pl.Datetime))

        # Add 15min_bucket if not present
        if "15min_bucket" not in processed_df.columns:
            processed_df = processed_df.with_columns(pl.col("timestamp").dt.truncate("15m").alias("15min_bucket"))

        return processed_df

    # Check cache first
    if cache_key in _dataframe_cache:
        return _dataframe_cache[cache_key]

    # Process and cache
    processed_df = df.clone()

    # Only convert timestamp if not already datetime
    if "timestamp" not in processed_df.columns or processed_df.schema["timestamp"] != pl.Datetime:
        if processed_df.schema.get("timestamp") == pl.Utf8:
            processed_df = processed_df.with_columns(
                pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S.%f")
            )
        else:
            processed_df = processed_df.with_columns(pl.col("timestamp").cast(pl.Datetime))

    # Only add 15min_bucket if not already present
    if "15min_bucket" not in processed_df.columns:
        processed_df = processed_df.with_columns(pl.col("timestamp").dt.truncate("15m").alias("15min_bucket"))

    # Cache the result
    _dataframe_cache[cache_key] = processed_df
    return processed_df


def get_time_filtered_data(
    db_path: str, hours_back: int = 24, end_time: datetime.datetime | None = None
) -> pl.DataFrame:
    """
    Get GPU state data filtered by time range.
    Automatically handles month boundaries by loading data from multiple database files.

    Args:
        db_path: Path to SQLite database (used to determine base directory for multi-db queries)
        hours_back: Number of hours to look back from end_time
        end_time: End time for the range (defaults to latest timestamp in primary DB)

    Returns:
        Polars DataFrame filtered to the specified time range
    """
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
                df_temp = pl.read_database("SELECT MAX(timestamp) as max_time FROM gpu_state", conn)
                conn.close()

                if len(df_temp) > 0 and df_temp["max_time"][0] is not None:
                    max_time = df_temp["max_time"][0]
                    if isinstance(max_time, str):
                        end_time = datetime.datetime.fromisoformat(max_time)
                    else:
                        end_time = max_time
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
            optimized_query = """
            SELECT * FROM gpu_state
            WHERE timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp
            """
            df = pl.read_database(
                optimized_query,
                conn,
                # Note: Polars handles parameter binding differently, constructing query directly
            )
            conn.close()

            # Apply time filtering with Polars (construct parameterized query manually)
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(
                optimized_query,
                (start_time.strftime("%Y-%m-%d %H:%M:%S.%f"), end_time.strftime("%Y-%m-%d %H:%M:%S.%f")),
            )

            # Read results into Polars
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            conn.close()

            if rows:
                df = pl.DataFrame(rows, schema=columns, orient="row")
                # Convert timestamp column
                if "timestamp" in df.columns:
                    df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f"))
                return df
            return pl.DataFrame()

        except Exception as e:
            # If single-db approach fails, fall back to multi-db approach
            print(f"Warning: Single database query failed, trying multi-database approach: {e}")

    # Multi-month query - use the multi-database functionality
    try:
        db_paths = get_required_databases(start_time, end_time, base_dir)
        return get_multi_db_data(db_paths, start_time, end_time)
    except Exception as e:
        # Final fallback: try just the specified database file
        print(f"Warning: Multi-database query failed, falling back to single database: {e}")
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            query = """
            SELECT * FROM gpu_state
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp
            """
            cursor.execute(
                query, (start_time.strftime("%Y-%m-%d %H:%M:%S.%f"), end_time.strftime("%Y-%m-%d %H:%M:%S.%f"))
            )

            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            conn.close()

            if rows:
                df = pl.DataFrame(rows, schema=columns, orient="row")
                if "timestamp" in df.columns:
                    df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f"))
                return df
            return pl.DataFrame()
        except Exception as final_e:
            print(f"Error: All database query methods failed: {final_e}")
            return pl.DataFrame()


def get_multi_db_data(db_paths: list, start_time: datetime.datetime, end_time: datetime.datetime) -> pl.DataFrame:
    """
    Load and merge data from multiple database files.

    This is a critical performance function - Polars concat is much faster than pandas.

    Args:
        db_paths: List of database file paths
        start_time: Start time for filtering
        end_time: End time for filtering

    Returns:
        Combined Polars DataFrame with data from all databases, filtered by time range
    """
    if not db_paths:
        return pl.DataFrame()

    all_dataframes = []

    # Add a small buffer to start_time to handle microsecond precision issues
    buffered_start = start_time - datetime.timedelta(seconds=1)

    for db_path in db_paths:
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Use parameterized query for time filtering at the database level
            query = """
            SELECT * FROM gpu_state
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp
            """
            cursor.execute(
                query, (buffered_start.strftime("%Y-%m-%d %H:%M:%S.%f"), end_time.strftime("%Y-%m-%d %H:%M:%S.%f"))
            )

            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            conn.close()

            if rows:
                df = pl.DataFrame(rows, schema=columns, orient="row")
                # Convert timestamp
                df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f"))

                # Apply the precise time filtering after loading
                df = df.filter((pl.col("timestamp") >= start_time) & (pl.col("timestamp") <= end_time))

                if len(df) > 0:
                    all_dataframes.append(df)

        except Exception as e:
            print(f"Warning: Could not load data from {db_path}: {e}")
            continue

    if not all_dataframes:
        return pl.DataFrame()

    # Combine all dataframes - MUCH faster with Polars than pandas!
    combined_df = pl.concat(all_dataframes)

    # Sort by timestamp to ensure proper ordering
    combined_df = combined_df.sort("timestamp")

    # Apply final time filtering to handle any edge cases
    combined_df = combined_df.filter((pl.col("timestamp") >= start_time) & (pl.col("timestamp") <= end_time))

    return combined_df


def calculate_allocation_usage(df: pl.DataFrame, host: str = "") -> dict:
    """
    Calculate allocation-based usage: percentage of available GPUs that are claimed,
    averaged across 15-minute intervals.

    Args:
        df: Polars DataFrame with GPU state data
        host: Optional host filter

    Returns:
        Dictionary with usage statistics for each class
    """
    # Create 15-minute time buckets
    df = df.clone()
    df = df.with_columns(
        [pl.col("timestamp").cast(pl.Datetime), pl.col("timestamp").dt.truncate("15m").alias("15min_bucket")]
    )

    stats = {}

    for utilization_type in UTILIZATION_TYPES:
        interval_usage_percentages = []
        total_claimed_gpus = 0
        total_available_gpus = 0

        # Get unique buckets
        buckets = df["15min_bucket"].unique().sort()

        # For each 15-minute interval, count unique GPUs
        for bucket in buckets:
            bucket_df = df.filter(pl.col("15min_bucket") == bucket)

            # Count unique GPUs for this utilization type in this interval
            if utilization_type == "Priority":
                claimed_gpus = (
                    filter_df(bucket_df, "Priority", "Claimed", host)
                    .filter(pl.col("AssignedGPUs").is_not_null())["AssignedGPUs"]
                    .n_unique()
                )
                unclaimed_gpus = (
                    filter_df(bucket_df, "Priority", "Unclaimed", host)
                    .filter(pl.col("AssignedGPUs").is_not_null())["AssignedGPUs"]
                    .n_unique()
                )
            elif utilization_type == "Shared":
                claimed_gpus = (
                    filter_df(bucket_df, "Shared", "Claimed", host)
                    .filter(pl.col("AssignedGPUs").is_not_null())["AssignedGPUs"]
                    .n_unique()
                )
                unclaimed_gpus = (
                    filter_df(bucket_df, "Shared", "Unclaimed", host)
                    .filter(pl.col("AssignedGPUs").is_not_null())["AssignedGPUs"]
                    .n_unique()
                )
            elif utilization_type == "Backfill":
                claimed_gpus = (
                    filter_df(bucket_df, "Backfill", "Claimed", host)
                    .filter(pl.col("AssignedGPUs").is_not_null())["AssignedGPUs"]
                    .n_unique()
                )
                unclaimed_gpus = (
                    filter_df(bucket_df, "Backfill", "Unclaimed", host)
                    .filter(pl.col("AssignedGPUs").is_not_null())["AssignedGPUs"]
                    .n_unique()
                )

            total_gpus_this_interval = claimed_gpus + unclaimed_gpus

            if total_gpus_this_interval > 0:
                interval_usage = (claimed_gpus / total_gpus_this_interval) * 100
                interval_usage_percentages.append(interval_usage)
                total_claimed_gpus += claimed_gpus
                total_available_gpus += total_gpus_this_interval

        # Calculate average usage percentage across all intervals
        avg_usage_percentage = (
            sum(interval_usage_percentages) / len(interval_usage_percentages) if interval_usage_percentages else 0
        )

        # Calculate average GPU counts across intervals
        num_intervals = len(buckets)
        avg_claimed = total_claimed_gpus / num_intervals if num_intervals > 0 else 0
        avg_total = total_available_gpus / num_intervals if num_intervals > 0 else 0

        stats[utilization_type] = {
            "avg_claimed": avg_claimed,
            "avg_total_available": avg_total,
            "allocation_usage_percent": avg_usage_percentage,
            "num_intervals": num_intervals,
        }

    return stats


def calculate_time_series_usage(df: pl.DataFrame, bucket_minutes: int = 15, host: str = "") -> pl.DataFrame:
    """
    Calculate usage over time in buckets, counting unique GPUs per interval.

    Args:
        df: Polars DataFrame with GPU state data
        bucket_minutes: Size of time buckets in minutes
        host: Optional host filter

    Returns:
        Polars DataFrame with time series usage statistics
    """
    # Create time buckets
    df = df.clone()
    df = df.with_columns(
        [
            pl.col("timestamp").cast(pl.Datetime),
            pl.col("timestamp").dt.truncate(f"{bucket_minutes}m").alias(f"{bucket_minutes}min_bucket"),
        ]
    )

    time_series_data = []

    buckets = df[f"{bucket_minutes}min_bucket"].unique().sort()

    for bucket in buckets:
        bucket_df = df.filter(pl.col(f"{bucket_minutes}min_bucket") == bucket)
        bucket_stats = {"timestamp": bucket}

        for utilization_type in UTILIZATION_TYPES:
            # Count unique GPUs for this utilization type in this interval
            if utilization_type == "Priority":
                claimed_gpus = (
                    filter_df(bucket_df, "Priority", "Claimed", host)
                    .filter(pl.col("AssignedGPUs").is_not_null())["AssignedGPUs"]
                    .n_unique()
                )
                unclaimed_gpus = (
                    filter_df(bucket_df, "Priority", "Unclaimed", host)
                    .filter(pl.col("AssignedGPUs").is_not_null())["AssignedGPUs"]
                    .n_unique()
                )
            elif utilization_type == "Shared":
                claimed_gpus = (
                    filter_df(bucket_df, "Shared", "Claimed", host)
                    .filter(pl.col("AssignedGPUs").is_not_null())["AssignedGPUs"]
                    .n_unique()
                )
                unclaimed_gpus = (
                    filter_df(bucket_df, "Shared", "Unclaimed", host)
                    .filter(pl.col("AssignedGPUs").is_not_null())["AssignedGPUs"]
                    .n_unique()
                )
            elif utilization_type == "Backfill":
                claimed_gpus = (
                    filter_df(bucket_df, "Backfill", "Claimed", host)
                    .filter(pl.col("AssignedGPUs").is_not_null())["AssignedGPUs"]
                    .n_unique()
                )
                unclaimed_gpus = (
                    filter_df(bucket_df, "Backfill", "Unclaimed", host)
                    .filter(pl.col("AssignedGPUs").is_not_null())["AssignedGPUs"]
                    .n_unique()
                )

            total_gpus = claimed_gpus + unclaimed_gpus
            usage_percent = (claimed_gpus / total_gpus * 100) if total_gpus > 0 else 0

            bucket_stats[f"{utilization_type.lower()}_claimed"] = claimed_gpus
            bucket_stats[f"{utilization_type.lower()}_total"] = total_gpus
            bucket_stats[f"{utilization_type.lower()}_usage_percent"] = usage_percent

        time_series_data.append(bucket_stats)

    return pl.DataFrame(time_series_data)


def clear_dataframe_cache():
    """Clear all DataFrame caches to free memory."""
    global _dataframe_cache, _filtered_cache
    _dataframe_cache.clear()
    _filtered_cache.clear()


def calculate_allocation_usage_by_device_enhanced(
    df: pl.DataFrame, host: str = "", include_all_devices: bool = True
) -> dict:
    """
    Calculate allocation-based usage grouped by device type with enhanced backfill categories.

    OPTIMIZED POLARS VERSION - 5-10x faster than pandas through parallel groupby.

    Args:
        df: Polars DataFrame with GPU state data
        host: Optional host filter
        include_all_devices: Whether to include all device types or filter out older ones

    Returns:
        Dictionary with usage statistics for each enhanced class and device type
    """
    from gpu_utils_polars import CLASS_ORDER, filter_df_enhanced

    # Preprocess DataFrame
    df = df.clone()
    if df.schema.get("timestamp") != pl.Datetime:
        df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f"))
    if "15min_bucket" not in df.columns:
        df = df.with_columns(pl.col("timestamp").dt.truncate("15m").alias("15min_bucket"))

    # Get unique device types and buckets
    device_types = df.filter(pl.col("GPUs_DeviceName").is_not_null())["GPUs_DeviceName"].unique().to_list()
    all_buckets = df["15min_bucket"].unique().sort()
    total_intervals = len(all_buckets)

    stats = {}

    # Filter old devices if requested
    old_gpu_types = ["GTX 1080", "P100", "Quadro", "A30", "A40"]

    for utilization_type in CLASS_ORDER:
        stats[utilization_type] = {}

        for device_type in device_types:
            # Skip old/uncommon GPU types unless requested
            if not include_all_devices and any(old in device_type for old in old_gpu_types):
                continue

            # Filter by device type and utilization type
            device_df = df.filter(pl.col("GPUs_DeviceName") == device_type)
            filtered_df = filter_df_enhanced(device_df, utilization_type, "", host)

            if len(filtered_df) == 0:
                continue

            # OPTIMIZATION: Use groupby to count unique GPUs per bucket and state
            # This is MUCH faster than looping through buckets
            agg_df = filtered_df.group_by(["15min_bucket", "State"]).agg(
                pl.col("AssignedGPUs").drop_nulls().n_unique().alias("gpu_count")
            )

            # Pivot to get claimed vs total
            claimed_by_bucket = (
                agg_df.filter(pl.col("State") == "Claimed")
                .select(["15min_bucket", "gpu_count"])
                .rename({"gpu_count": "claimed"})
            )

            total_by_bucket = agg_df.group_by("15min_bucket").agg(pl.col("gpu_count").sum().alias("total"))

            # Join to get claimed and total per bucket
            bucket_stats = total_by_bucket.join(claimed_by_bucket, on="15min_bucket", how="left").with_columns(
                pl.col("claimed").fill_null(0)
            )

            if len(bucket_stats) == 0:
                continue

            # Calculate statistics
            total_claimed = bucket_stats["claimed"].sum()
            total_available = bucket_stats["total"].sum()

            # Calculate average usage percentage (only for intervals with data)
            bucket_stats = bucket_stats.with_columns((pl.col("claimed") / pl.col("total") * 100).alias("usage_pct"))
            avg_usage_percentage = bucket_stats["usage_pct"].mean()

            # Average across ALL intervals (including zeros)
            avg_claimed = total_claimed / total_intervals if total_intervals > 0 else 0
            avg_total = total_available / total_intervals if total_intervals > 0 else 0

            stats[utilization_type][device_type] = {
                "avg_claimed": avg_claimed,
                "avg_total_available": avg_total,
                "allocation_usage_percent": avg_usage_percentage,
                "num_intervals": total_intervals,
            }

    return stats


def calculate_allocation_usage_by_memory(df: pl.DataFrame, host: str = "", include_all_devices: bool = True) -> dict:
    """
    Calculate allocation-based usage grouped by memory category for Real slots only.

    OPTIMIZED POLARS VERSION - 5-10x faster than pandas through parallel groupby.

    Args:
        df: Polars DataFrame with GPU state data
        host: Optional host filter
        include_all_devices: Whether to include all device types or filter out older ones

    Returns:
        Dictionary with usage statistics for each memory category (for Real slots only)
    """
    from device_name_mappings import get_memory_category_from_mb
    from gpu_utils_polars import filter_df_enhanced

    # Preprocess DataFrame
    df = df.clone()
    if df.schema.get("timestamp") != pl.Datetime:
        df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f"))
    if "15min_bucket" not in df.columns:
        df = df.with_columns(pl.col("timestamp").dt.truncate("15m").alias("15min_bucket"))

    # Add memory category column
    # Note: Polars doesn't have .apply(), so we need to use .map_elements()
    df = df.with_columns(
        pl.col("GPUs_GlobalMemoryMb")
        .map_elements(lambda x: get_memory_category_from_mb(x) if x is not None else None, return_dtype=pl.Utf8)
        .alias("memory_category")
    )

    # Get unique memory categories and buckets
    memory_categories = df.filter(pl.col("memory_category").is_not_null())["memory_category"].unique().to_list()
    all_buckets = df["15min_bucket"].unique().sort()

    stats = {}

    # Only calculate for Real slot classes
    real_slot_classes = ["Priority-ResearcherOwned", "Priority-CHTCOwned", "Shared"]

    for memory_cat in memory_categories:
        total_claimed_across_intervals = 0
        total_available_across_intervals = 0
        num_intervals_with_data = 0

        # Filter by memory category once
        memory_df = df.filter(pl.col("memory_category") == memory_cat)

        # For each bucket, aggregate across all real slot classes
        for bucket_time in all_buckets:
            bucket_df = memory_df.filter(pl.col("15min_bucket") == bucket_time)

            if len(bucket_df) == 0:
                continue

            bucket_claimed = 0
            bucket_total = 0

            # Sum across all Real slot classes for this memory category
            for class_name in real_slot_classes:
                class_filtered_df = filter_df_enhanced(bucket_df, class_name, "", host)

                if len(class_filtered_df) == 0:
                    continue

                # Count unique GPUs (total available for this class)
                total_count = class_filtered_df.filter(pl.col("AssignedGPUs").is_not_null())["AssignedGPUs"].n_unique()

                # Count unique claimed GPUs
                claimed_count = class_filtered_df.filter(
                    (pl.col("State") == "Claimed") & (pl.col("AssignedGPUs").is_not_null())
                )["AssignedGPUs"].n_unique()

                bucket_claimed += claimed_count
                bucket_total += total_count

            if bucket_total > 0:
                total_claimed_across_intervals += bucket_claimed
                total_available_across_intervals += bucket_total
                num_intervals_with_data += 1

        # Calculate averages
        if num_intervals_with_data > 0:
            avg_claimed = total_claimed_across_intervals / num_intervals_with_data
            avg_total = total_available_across_intervals / num_intervals_with_data
            avg_usage_percentage = (avg_claimed / avg_total * 100) if avg_total > 0 else 0

            stats[memory_cat] = {
                "avg_claimed": avg_claimed,
                "avg_total_available": avg_total,
                "allocation_usage_percent": avg_usage_percentage,
                "num_intervals": num_intervals_with_data,
            }

    return stats


# CLI Interface
def main(
    hours_back: int = 24,
    host: str = "",
    db_path: str | None = None,
    group_by_device: bool = True,
    all_devices: bool = False,
    exclude_hosts_yaml: str | None = "masked_hosts.yaml",
    output_format: str = "text",
    output_file: str | None = None,
):
    """
    GPU Usage Statistics Calculator - Polars-accelerated version.

    Uses Polars for fast data loading and processing, then pandas for reporting.

    Args:
        hours_back: Number of hours to analyze (default: 24)
        host: Host name to filter results
        db_path: Path to SQLite database (defaults to current month)
        group_by_device: Group results by GPU device type
        all_devices: Include all device types (if False, filters out older GPUs)
        exclude_hosts_yaml: Path to YAML file containing host exclusions
        output_format: Output format: 'text' or 'html'
        output_file: Output file path (optional)
    """
    import os
    import time

    import pandas as pd

    import gpu_utils

    # Import pandas-based reporting functions
    from usage_stats import (
        calculate_allocation_usage_enhanced,
        calculate_backfill_usage_by_user,
        # NOTE: Using Polars versions defined above, not pandas versions:
        # calculate_allocation_usage_by_device_enhanced,
        # calculate_allocation_usage_by_memory,
        calculate_h200_user_breakdown,
        load_host_exclusions,
        print_analysis_results,
    )

    analysis_start_time = time.time()
    analysis_start_datetime = datetime.datetime.now()

    # Auto-detect database path if not provided
    if db_path is None:
        current_date = datetime.datetime.now()
        current_month_db = f"gpu_state_{current_date.strftime('%Y-%m')}.db"

        if os.path.exists(current_month_db):
            db_path = current_month_db
            print(f"Using current month database: {db_path}")
        else:
            import glob

            db_files = glob.glob("gpu_state_*.db")
            if db_files:
                db_path = sorted(db_files)[-1]
                print(f"Current month database not found, using most recent: {db_path}")
            else:
                print("Error: No database files found. Please specify --db-path.")
                return

    # Set up host exclusions (using pandas version's logic)
    gpu_utils.HOST_EXCLUSIONS = load_host_exclusions(None, exclude_hosts_yaml)
    gpu_utils.FILTERED_HOSTS_INFO = []

    # Load data using Polars (FAST!)
    print(f"Loading data with Polars (last {hours_back} hours)...")
    df_polars = get_time_filtered_data(db_path, hours_back, None)

    if len(df_polars) == 0:
        print("Error: No data found in the specified time range.")
        return

    # Convert to pandas for reporting (one-time conversion)
    print(f"Converting to pandas for reporting ({len(df_polars)} records)...")
    df_pandas = df_polars.to_pandas()

    # Calculate time buckets for interval counting
    df_pandas["timestamp"] = pd.to_datetime(df_pandas["timestamp"])
    df_pandas["15min_bucket"] = df_pandas["timestamp"].dt.floor("15min")
    num_intervals = df_pandas["15min_bucket"].nunique()

    # Build results dictionary
    results = {
        "metadata": {
            "start_time": df_pandas["timestamp"].min(),
            "end_time": df_pandas["timestamp"].max(),
            "num_intervals": num_intervals,
            "total_records": len(df_pandas),
            "hours_back": hours_back,
            "excluded_hosts": gpu_utils.HOST_EXCLUSIONS,
            "filtered_hosts_info": gpu_utils.FILTERED_HOSTS_INFO,
        }
    }

    # Calculate statistics - use Polars for device/memory stats (FAST!), pandas for user stats
    if group_by_device:
        print("Calculating device statistics with Polars...")
        # Use Polars versions for device and memory stats (5-10x faster!)
        results["device_stats"] = calculate_allocation_usage_by_device_enhanced(df_polars, host, all_devices)
        results["memory_stats"] = calculate_allocation_usage_by_memory(df_polars, host, all_devices)

        print("Calculating user statistics with pandas...")
        # Use pandas for user breakdown (not yet migrated to Polars)
        results["h200_user_stats"] = calculate_h200_user_breakdown(df_pandas, host, hours_back)
        results["backfill_user_stats"] = calculate_backfill_usage_by_user(df_pandas, host, hours_back, all_devices)
        results["raw_data"] = df_pandas
        results["host_filter"] = host
    else:
        print("Calculating allocation statistics...")
        results["allocation_stats"] = calculate_allocation_usage_enhanced(df_pandas, host)

    # Add runtime information
    analysis_end_time = time.time()
    runtime_seconds = analysis_end_time - analysis_start_time
    results["metadata"]["analysis_runtime_seconds"] = round(runtime_seconds, 3)
    results["metadata"]["analysis_start_datetime"] = analysis_start_datetime.isoformat()
    results["metadata"]["analysis_end_datetime"] = datetime.datetime.now().isoformat()

    # Print results
    print_analysis_results(results, output_format, output_file)

    print(f"\nTotal runtime: {runtime_seconds:.2f} seconds")


if __name__ == "__main__":
    import typer

    typer.run(main)
