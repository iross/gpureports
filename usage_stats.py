#!/usr/bin/env python3
"""
GPU Usage Statistics Calculator

This script calculates average usage statistics for GPU classes (Priority, Shared, Backfill)
over a specified time range. It provides both allocation-based usage (percentage of available
GPUs in use) and performance-based usage (actual GPU utilization metrics).
"""

import pandas as pd
import datetime
import typer
import sqlite3
import json
import yaml
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, Dict
import os
from pathlib import Path
# Removed jinja2 and pathlib imports - no longer needed for simple HTML tables

CLASS_ORDER = ["Priority-ResearcherOwned", "Priority-CHTCOwned", "Shared", "Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]

# Import shared utilities
from gpu_utils import (
    filter_df, filter_df_enhanced, count_backfill, count_shared, count_prioritized,
    count_backfill_researcher_owned, count_backfill_chtc_owned, count_glidein,
    load_host_exclusions, get_display_name, get_required_databases,
    get_latest_timestamp_from_most_recent_db, get_machines_by_category,
    HOST_EXCLUSIONS, FILTERED_HOSTS_INFO
)
import gpu_utils
from device_name_mappings import get_human_readable_device_name, get_memory_category_from_mb


def get_time_filtered_data(
    db_path: str,
    hours_back: int = 24,
    end_time: Optional[datetime.datetime] = None
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
    base_dir = str(db_path_obj.parent) if db_path_obj.parent != Path('.') else "."

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
                if len(df_temp) > 0 and df_temp['max_time'].iloc[0] is not None:
                    end_time = pd.to_datetime(df_temp['max_time'].iloc[0])
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
            df = pd.read_sql_query(
                optimized_query,
                conn,
                params=[start_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
                       end_time.strftime('%Y-%m-%d %H:%M:%S.%f')]
            )
            conn.close()

            if len(df) > 0:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
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
            query = """
            SELECT * FROM gpu_state
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp
            """
            df = pd.read_sql_query(query, conn, params=[start_time.strftime('%Y-%m-%d %H:%M:%S.%f'), end_time.strftime('%Y-%m-%d %H:%M:%S.%f')])
            conn.close()
            if len(df) > 0:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
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
            # Use parameterized query for time filtering at the database level for efficiency
            query = """
            SELECT * FROM gpu_state
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp
            """
            df = pd.read_sql_query(query, conn, params=[buffered_start.strftime('%Y-%m-%d %H:%M:%S.%f'), end_time.strftime('%Y-%m-%d %H:%M:%S.%f')])
            conn.close()

            if len(df) > 0:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                # Apply the precise time filtering after loading, since we used a buffered start
                df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
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
    combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)

    # Apply final time filtering to handle any edge cases
    combined_df = combined_df[
        (combined_df['timestamp'] >= start_time) &
        (combined_df['timestamp'] <= end_time)
    ]

    return combined_df


def get_time_filtered_data_multi_db(
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    base_dir: str = "."
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


def calculate_allocation_usage(df: pd.DataFrame, host: str = "") -> dict:
    """
    Calculate allocation-based usage: percentage of available GPUs that are claimed,
    averaged across 15-minute intervals.

    Args:
        df: DataFrame with GPU state data
        host: Optional host filter

    Returns:
        Dictionary with usage statistics for each class
    """
    # Create 15-minute time buckets
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['15min_bucket'] = df['timestamp'].dt.floor('15min')

    stats = {}

    for utilization_type in ["Priority", "Shared", "Backfill"]:
        interval_usage_percentages = []
        total_claimed_gpus = 0
        total_available_gpus = 0

        # For each 15-minute interval, count unique GPUs
        for bucket in sorted(df['15min_bucket'].unique()):
            bucket_df = df[df['15min_bucket'] == bucket]

            # Count unique GPUs for this utilization type in this interval
            if utilization_type == "Priority":
                claimed_gpus = len(filter_df(bucket_df, "Priority", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df(bucket_df, "Priority", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Shared":
                claimed_gpus = len(filter_df(bucket_df, "Shared", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df(bucket_df, "Shared", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Backfill":
                claimed_gpus = len(filter_df(bucket_df, "Backfill", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df(bucket_df, "Backfill", "Unclaimed", host)['AssignedGPUs'].dropna().unique())

            total_gpus_this_interval = claimed_gpus + unclaimed_gpus

            if total_gpus_this_interval > 0:
                interval_usage = (claimed_gpus / total_gpus_this_interval) * 100
                interval_usage_percentages.append(interval_usage)
                total_claimed_gpus += claimed_gpus
                total_available_gpus += total_gpus_this_interval

        # Calculate average usage percentage across all intervals
        avg_usage_percentage = sum(interval_usage_percentages) / len(interval_usage_percentages) if interval_usage_percentages else 0

        # Calculate average GPU counts across intervals
        num_intervals = len(df['15min_bucket'].unique())
        avg_claimed = total_claimed_gpus / num_intervals if num_intervals > 0 else 0
        avg_total = total_available_gpus / num_intervals if num_intervals > 0 else 0

        stats[utilization_type] = {
            'avg_claimed': avg_claimed,
            'avg_total_available': avg_total,
            'allocation_usage_percent': avg_usage_percentage,
            'num_intervals': num_intervals
        }

    return stats


def calculate_allocation_usage_enhanced(df: pd.DataFrame, host: str = "") -> dict:
    """
    Calculate allocation-based usage with enhanced backfill categories.

    Args:
        df: DataFrame with GPU state data
        host: Optional host filter

    Returns:
        Dictionary with usage statistics for each enhanced class
    """
    # Create 15-minute time buckets
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['15min_bucket'] = df['timestamp'].dt.floor('15min')

    stats = {}

    # Utilization types with emphasis on hosted capacity
    utilization_types = [
        "Priority-ResearcherOwned",
        "Priority-CHTCOwned",
        "Shared",
        "Backfill-CHTCOwned",
        "Backfill-ResearcherOwned",
        "Backfill-OpenCapacity"
    ]

    for utilization_type in utilization_types:
        interval_usage_percentages = []
        total_claimed_gpus = 0
        total_available_gpus = 0

        # For each 15-minute interval, count unique GPUs
        for bucket in sorted(df['15min_bucket'].unique()):
            bucket_df = df[df['15min_bucket'] == bucket]

            # Count unique GPUs for this utilization type in this interval
            if utilization_type == "Priority-ResearcherOwned":
                claimed_gpus = len(filter_df_enhanced(bucket_df, "Priority-ResearcherOwned", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df_enhanced(bucket_df, "Priority-ResearcherOwned", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Priority-CHTCOwned":
                claimed_gpus = len(filter_df_enhanced(bucket_df, "Priority-CHTCOwned", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df_enhanced(bucket_df, "Priority-CHTCOwned", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Shared":
                claimed_gpus = len(filter_df_enhanced(bucket_df, "Shared", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df_enhanced(bucket_df, "Shared", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Backfill-CHTCOwned":
                claimed_gpus = len(filter_df_enhanced(bucket_df, "Backfill-CHTCOwned", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df_enhanced(bucket_df, "Backfill-CHTCOwned", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Backfill-ResearcherOwned":
                claimed_gpus = len(filter_df_enhanced(bucket_df, "Backfill-ResearcherOwned", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df_enhanced(bucket_df, "Backfill-ResearcherOwned", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Backfill-OpenCapacity":
                claimed_gpus = len(filter_df_enhanced(bucket_df, "Backfill-OpenCapacity", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df_enhanced(bucket_df, "Backfill-OpenCapacity", "Unclaimed", host)['AssignedGPUs'].dropna().unique())

            total_gpus_this_interval = claimed_gpus + unclaimed_gpus

            if total_gpus_this_interval > 0:
                interval_usage = (claimed_gpus / total_gpus_this_interval) * 100
                interval_usage_percentages.append(interval_usage)
                total_claimed_gpus += claimed_gpus
                total_available_gpus += total_gpus_this_interval

        # Calculate average usage percentage across all intervals
        avg_usage_percentage = sum(interval_usage_percentages) / len(interval_usage_percentages) if interval_usage_percentages else 0

        # Calculate average GPU counts across intervals
        num_intervals = len(df['15min_bucket'].unique())
        avg_claimed = total_claimed_gpus / num_intervals if num_intervals > 0 else 0
        avg_total = total_available_gpus / num_intervals if num_intervals > 0 else 0

        stats[utilization_type] = {
            'avg_claimed': avg_claimed,
            'avg_total_available': avg_total,
            'allocation_usage_percent': avg_usage_percentage,
            'num_intervals': num_intervals
        }

    return stats


def calculate_allocation_usage_by_device_enhanced(df: pd.DataFrame, host: str = "", include_all_devices: bool = True) -> dict:
    """
    Calculate allocation-based usage grouped by device type with enhanced backfill categories.

    Args:
        df: DataFrame with GPU state data
        host: Optional host filter
        include_all_devices: Whether to include all device types or filter out older ones

    Returns:
        Dictionary with usage statistics for each enhanced class and device type
    """
    # Create 15-minute time buckets
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['15min_bucket'] = df['timestamp'].dt.floor('15min')

    # Get unique device types
    device_types = df['GPUs_DeviceName'].dropna().unique()

    stats = {}

    # Utilization types with emphasis on hosted capacity
    utilization_types = [
        "Priority-ResearcherOwned",
        "Priority-CHTCOwned",
        "Shared",
        "Backfill-CHTCOwned",
        "Backfill-ResearcherOwned",
        "Backfill-OpenCapacity"
    ]

    for utilization_type in utilization_types:
        stats[utilization_type] = {}

        for device_type in device_types:
            # Skip old/uncommon GPU types for cleaner output (unless requested to include all)
            if not include_all_devices and any(old_gpu in device_type for old_gpu in ["GTX 1080", "P100", "Quadro", "A30", "A40"]):
                continue

            interval_usage_percentages = []
            total_claimed_gpus = 0
            total_available_gpus = 0

            # For each 15-minute interval, count unique GPUs of this device type
            for bucket in sorted(df['15min_bucket'].unique()):
                bucket_df = df[df['15min_bucket'] == bucket]

                # Filter by device type
                device_df = bucket_df[bucket_df['GPUs_DeviceName'] == device_type]

                if device_df.empty:
                    continue

                # Count unique GPUs for this utilization type and device in this interval
                if utilization_type == "Priority-ResearcherOwned":
                    all_gpus_df = filter_df_enhanced(device_df, "Priority-ResearcherOwned", "", host)
                elif utilization_type == "Priority-CHTCOwned":
                    all_gpus_df = filter_df_enhanced(device_df, "Priority-CHTCOwned", "", host)
                elif utilization_type == "Shared":
                    all_gpus_df = filter_df_enhanced(device_df, "Shared", "", host)
                elif utilization_type == "Backfill-CHTCOwned":
                    all_gpus_df = filter_df_enhanced(device_df, "Backfill-CHTCOwned", "", host)
                elif utilization_type == "Backfill-ResearcherOwned":
                    all_gpus_df = filter_df_enhanced(device_df, "Backfill-ResearcherOwned", "", host)
                elif utilization_type == "Backfill-OpenCapacity":
                    all_gpus_df = filter_df_enhanced(device_df, "Backfill-OpenCapacity", "", host)

                # Count unique GPUs (total available for this utilization type)
                unique_gpu_ids = set(all_gpus_df['AssignedGPUs'].dropna().unique())
                total_gpus_this_interval = len(unique_gpu_ids)

                # Count how many of these unique GPUs are currently claimed
                claimed_gpus_df = all_gpus_df[all_gpus_df['State'] == 'Claimed']
                claimed_unique_gpu_ids = set(claimed_gpus_df['AssignedGPUs'].dropna().unique())
                claimed_gpus = len(claimed_unique_gpu_ids)

                if total_gpus_this_interval > 0:
                    interval_usage = (claimed_gpus / total_gpus_this_interval) * 100
                    interval_usage_percentages.append(interval_usage)
                    total_claimed_gpus += claimed_gpus
                    total_available_gpus += total_gpus_this_interval

            if interval_usage_percentages:
                # Calculate average usage percentage across all intervals
                avg_usage_percentage = sum(interval_usage_percentages) / len(interval_usage_percentages)

                # Calculate average GPU counts across ALL intervals (including those with 0 usage)
                # This matches the user breakdown method and gives consistent results
                total_intervals = len(df['15min_bucket'].unique())
                avg_claimed = total_claimed_gpus / total_intervals if total_intervals > 0 else 0
                avg_total = total_available_gpus / total_intervals if total_intervals > 0 else 0

                stats[utilization_type][device_type] = {
                    'avg_claimed': avg_claimed,
                    'avg_total_available': avg_total,
                    'allocation_usage_percent': avg_usage_percentage,
                    'num_intervals': total_intervals
                }

    return stats


def calculate_performance_usage(df: pd.DataFrame, host: str = "") -> dict:
    """
    Calculate performance-based usage: actual GPU utilization metrics averaged over time.

    Args:
        df: DataFrame with GPU state data
        host: Optional host filter

    Returns:
        Dictionary with performance usage statistics for each class
    """
    stats = {}

    for utilization_type in ["Priority", "Shared", "Backfill"]:
        # Filter to only claimed GPUs with utilization data
        filtered_df = filter_df(df, utilization_type, "Claimed", host)

        # Only consider records with valid utilization data
        util_df = filtered_df[
            (filtered_df['GPUsAverageUsage'].notna()) &
            (filtered_df['GPUsAverageUsage'] >= 0)
        ]

        if len(util_df) > 0:
            avg_utilization = util_df['GPUsAverageUsage'].mean() * 100  # Convert to percentage
            total_records = len(util_df)
            unique_gpus = util_df['AssignedGPUs'].nunique()
        else:
            avg_utilization = 0
            total_records = 0
            unique_gpus = 0

        stats[utilization_type] = {
            'avg_gpu_utilization_percent': avg_utilization,
            'records_with_utilization': total_records,
            'unique_gpus_used': unique_gpus
        }

    return stats


def calculate_time_series_usage(
    df: pd.DataFrame,
    bucket_minutes: int = 15,
    host: str = ""
) -> pd.DataFrame:
    """
    Calculate usage over time in buckets, counting unique GPUs per interval.

    Args:
        df: DataFrame with GPU state data
        bucket_minutes: Size of time buckets in minutes
        host: Optional host filter

    Returns:
        DataFrame with time series usage statistics
    """
    # Create time buckets
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df[f'{bucket_minutes}min_bucket'] = df['timestamp'].dt.floor(f'{bucket_minutes}min')

    time_series_data = []

    for bucket in sorted(df[f'{bucket_minutes}min_bucket'].unique()):
        bucket_df = df[df[f'{bucket_minutes}min_bucket'] == bucket]
        bucket_stats = {'timestamp': bucket}

        for utilization_type in ["Priority", "Shared", "Backfill"]:
            # Count unique GPUs for this utilization type in this interval
            if utilization_type == "Priority":
                claimed_gpus = len(filter_df(bucket_df, "Priority", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df(bucket_df, "Priority", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Shared":
                claimed_gpus = len(filter_df(bucket_df, "Shared", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df(bucket_df, "Shared", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Backfill":
                claimed_gpus = len(filter_df(bucket_df, "Backfill", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df(bucket_df, "Backfill", "Unclaimed", host)['AssignedGPUs'].dropna().unique())

            total_gpus = claimed_gpus + unclaimed_gpus
            usage_percent = (claimed_gpus / total_gpus * 100) if total_gpus > 0 else 0

            bucket_stats[f'{utilization_type.lower()}_claimed'] = claimed_gpus
            bucket_stats[f'{utilization_type.lower()}_total'] = total_gpus
            bucket_stats[f'{utilization_type.lower()}_usage_percent'] = usage_percent

        time_series_data.append(bucket_stats)

    return pd.DataFrame(time_series_data)


def calculate_allocation_usage_by_device(df: pd.DataFrame, host: str = "", include_all_devices: bool = True) -> dict:
    """
    Calculate allocation-based usage grouped by device type, averaged across 15-minute intervals.

    Args:
        df: DataFrame with GPU state data
        host: Optional host filter
        include_all_devices: Whether to include all device types or filter out older ones

    Returns:
        Dictionary with usage statistics for each class and device type
    """
    # Create 15-minute time buckets
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['15min_bucket'] = df['timestamp'].dt.floor('15min')

    # Get unique device types
    device_types = df['GPUs_DeviceName'].dropna().unique()

    stats = {}

    for utilization_type in ["Priority", "Shared", "Backfill"]:
        stats[utilization_type] = {}

        for device_type in device_types:
            # Skip old/uncommon GPU types for cleaner output (unless requested to include all)
            if not include_all_devices and any(old_gpu in device_type for old_gpu in ["GTX 1080", "P100", "Quadro", "A30", "A40"]):
                continue

            interval_usage_percentages = []
            total_claimed_gpus = 0
            total_available_gpus = 0

            # For each 15-minute interval, count unique GPUs of this device type
            for bucket in sorted(df['15min_bucket'].unique()):
                bucket_df = df[df['15min_bucket'] == bucket]

                # Filter by device type
                device_df = bucket_df[bucket_df['GPUs_DeviceName'] == device_type]

                if device_df.empty:
                    continue

                # Count unique GPUs for this utilization type and device in this interval
                # Fixed: Get all GPUs for this utilization type, then count claimed vs total to avoid double-counting
                if utilization_type == "Priority":
                    all_gpus_df = filter_df(device_df, "Priority", "", host)
                elif utilization_type == "Shared":
                    all_gpus_df = filter_df(device_df, "Shared", "", host)
                elif utilization_type == "Backfill":
                    all_gpus_df = filter_df(device_df, "Backfill", "", host)

                # Count unique GPUs (total available for this utilization type)
                unique_gpu_ids = set(all_gpus_df['AssignedGPUs'].dropna().unique())
                total_gpus_this_interval = len(unique_gpu_ids)

                # Count how many of these unique GPUs are currently claimed
                claimed_gpus_df = all_gpus_df[all_gpus_df['State'] == 'Claimed']
                claimed_unique_gpu_ids = set(claimed_gpus_df['AssignedGPUs'].dropna().unique())
                claimed_gpus = len(claimed_unique_gpu_ids)

                if total_gpus_this_interval > 0:
                    interval_usage = (claimed_gpus / total_gpus_this_interval) * 100
                    interval_usage_percentages.append(interval_usage)
                    total_claimed_gpus += claimed_gpus
                    total_available_gpus += total_gpus_this_interval

            if interval_usage_percentages:
                # Calculate average usage percentage across all intervals
                avg_usage_percentage = sum(interval_usage_percentages) / len(interval_usage_percentages)

                # Calculate average GPU counts across ALL intervals (including those with 0 usage)
                # This matches the user breakdown method and gives consistent results
                total_intervals = len(df['15min_bucket'].unique())
                avg_claimed = total_claimed_gpus / total_intervals if total_intervals > 0 else 0
                avg_total = total_available_gpus / total_intervals if total_intervals > 0 else 0

                stats[utilization_type][device_type] = {
                    'avg_claimed': avg_claimed,
                    'avg_total_available': avg_total,
                    'allocation_usage_percent': avg_usage_percentage,
                    'num_intervals': total_intervals
                }

    return stats


def calculate_allocation_usage_by_memory(df: pd.DataFrame, host: str = "", include_all_devices: bool = True) -> dict:
    """
    Calculate allocation-based usage grouped by memory category for Real slots only.
    Uses GPUs_GlobalMemoryMb field for dynamic memory categorization.
    Args:
        df: DataFrame with GPU state data
        host: Optional host filter
        include_all_devices: Whether to include all device types or filter out older ones
    Returns:
        Dictionary with usage statistics for each memory category (for Real slots only)
    """
    # Create 15-minute time buckets
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['15min_bucket'] = df['timestamp'].dt.floor('15min')

    # Add memory category column based on GPUs_GlobalMemoryMb
    df['memory_category'] = df['GPUs_GlobalMemoryMb'].apply(get_memory_category_from_mb)

    # Get unique memory categories
    memory_categories = df['memory_category'].dropna().unique()

    stats = {}

    # Only calculate for Real slot classes (Priority + Shared)
    real_slot_classes = ["Priority", "Shared"]

    for memory_cat in memory_categories:
        total_claimed_across_intervals = 0
        total_available_across_intervals = 0
        num_intervals_with_data = 0

        # Get unique time buckets
        unique_buckets = df['15min_bucket'].unique()

        for bucket_time in unique_buckets:
            bucket_df = df[df['15min_bucket'] == bucket_time]

            # Filter by memory category
            memory_df = bucket_df[bucket_df['memory_category'] == memory_cat]
            if memory_df.empty:
                continue

            bucket_claimed = 0
            bucket_total = 0

            # Sum across all Real slot classes for this memory category
            for class_name in real_slot_classes:
                # Filter by class and apply host filter
                class_df = filter_df(memory_df, class_name, "", host)

                if not class_df.empty:
                    # Count allocated GPUs for this class and memory category
                    allocated_count = len(class_df[class_df['State'] == 'Claimed'])
                    total_count = len(class_df)

                    bucket_claimed += allocated_count
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
                'avg_claimed': avg_claimed,
                'avg_total_available': avg_total,
                'allocation_usage_percent': avg_usage_percentage,
                'num_intervals': num_intervals_with_data
            }

    return stats


def calculate_h200_user_breakdown(df: pd.DataFrame, host: str = "", hours_back: int = 1) -> dict:
    """
    Calculate H200 usage breakdown by user and slot type.

    Args:
        df: DataFrame with GPU state data
        host: Optional host filter
        hours_back: Lookback period in hours

    Returns:
        Dictionary with H200 usage statistics by user and slot type
    """
    # Filter for H200 GPUs only
    h200_df = df[df['GPUs_DeviceName'] == 'NVIDIA H200'].copy()

    if h200_df.empty:
        return {}

    # Create 15-minute time buckets
    h200_df['timestamp'] = pd.to_datetime(h200_df['timestamp'])
    h200_df['15min_bucket'] = h200_df['timestamp'].dt.floor('15min')

    # Apply host filter if specified
    if host:
        h200_df = h200_df[h200_df['Machine'].str.contains(host, case=False, na=False)]

    # Use the actual lookback period to match the device allocation method
    # (Device allocation uses averages across buckets multiplied by lookback period)
    actual_duration_hours = hours_back

    user_stats = {}
    slot_types = ["Priority-ResearcherOwned", "Priority-CHTCOwned", "Shared", "Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]

    # For each slot type, analyze user usage using averaging approach like device allocation
    for slot_type in slot_types:
        # Get slots of this type
        filtered_df = filter_df_enhanced(h200_df, slot_type, "", host)

        if filtered_df.empty:
            continue

        # Only look at claimed slots (where jobs are running)
        claimed_df = filtered_df[filtered_df['State'] == 'Claimed']

        if claimed_df.empty:
            continue

        # For each user, calculate their average GPU usage across buckets, then multiply by actual time
        user_bucket_totals = {}

        # Get all possible buckets for this slot type (from the entire H200 dataset)
        # This ensures we count all time intervals, including those where user has 0 GPUs
        all_buckets = sorted(h200_df['15min_bucket'].unique())
        num_buckets = len(all_buckets)

        for bucket in all_buckets:
            bucket_df = claimed_df[claimed_df['15min_bucket'] == bucket]

            if not bucket_df.empty:
                # Group by user within this time bucket
                user_gpu_counts = bucket_df.groupby('RemoteOwner')['AssignedGPUs'].nunique()

                for user, gpu_count in user_gpu_counts.items():
                    if pd.isna(user) or user == '' or user is None:
                        user = 'Unknown'

                    if user not in user_bucket_totals:
                        user_bucket_totals[user] = 0

                    user_bucket_totals[user] += gpu_count

        # Convert totals to averages, then multiply by actual time duration
        for user, total_gpus in user_bucket_totals.items():
            avg_gpus = total_gpus / num_buckets if num_buckets > 0 else 0
            gpu_hours = avg_gpus * actual_duration_hours

            if user not in user_stats:
                user_stats[user] = {
                    'Priority-ResearcherOwned': 0,
                    'Priority-CHTCOwned': 0,
                    'Shared': 0,
                    'Backfill-ResearcherOwned': 0,
                    'Backfill-CHTCOwned': 0,
                    'Backfill-OpenCapacity': 0
                }

            user_stats[user][slot_type] = gpu_hours

    # Calculate final statistics
    final_stats = {}
    for user, slot_data in user_stats.items():
        total_gpu_hours = sum(gpu_hours for gpu_hours in slot_data.values())

        if total_gpu_hours > 0:
            final_stats[user] = {
                'total_gpu_hours': total_gpu_hours,
                'slot_breakdown': {}
            }

            # Add breakdown by slot type (only include non-zero usage)
            for slot_type, gpu_hours in slot_data.items():
                if gpu_hours > 0:
                    final_stats[user]['slot_breakdown'][slot_type] = {
                        'gpu_hours': gpu_hours,
                        'percentage': (gpu_hours / total_gpu_hours) * 100
                    }

    return final_stats


def calculate_unique_cluster_totals_from_raw_data(df: pd.DataFrame, host: str = "") -> dict:
    """
    Calculate cluster totals from raw data without double-counting GPUs across categories.

    The issue is that GPUs can appear in both Priority and Backfill categories
    (e.g., when a prioritized GPU is idle, it's available for both prioritized and backfill jobs).
    For the TOTAL row, we want to count each unique GPU only once.

    This function goes back to the raw data to count unique AssignedGPUs.

    Args:
        df: Raw DataFrame with GPU state data
        host: Optional host filter

    Returns:
        Dictionary with unique 'claimed' and 'total' counts
    """
    # Create 15-minute time buckets like in the main calculation
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['15min_bucket'] = df['timestamp'].dt.floor('15min')

    total_claimed_across_intervals = 0
    total_available_across_intervals = 0
    num_intervals = 0

    # For each 15-minute interval, count unique GPUs across all categories
    for bucket in sorted(df['15min_bucket'].unique()):
        bucket_df = df[df['15min_bucket'] == bucket]

        if bucket_df.empty:
            continue

        # Get all unique GPUs across all categories (Priority, Shared, and Backfill)
        # Some GPUs may only appear in Backfill category (backfill-only GPUs)
        all_claimed_gpus = set()
        all_available_gpus = set()

        # Collect from all three categories to ensure we don't miss backfill-only GPUs
        for utilization_type in ["Priority", "Shared", "Backfill"]:
            claimed_df = filter_df(bucket_df, utilization_type, "Claimed", host)
            unclaimed_df = filter_df(bucket_df, utilization_type, "Unclaimed", host)

            # Add unique GPUs from this category
            claimed_gpus = set(claimed_df['AssignedGPUs'].dropna().unique())
            unclaimed_gpus = set(unclaimed_df['AssignedGPUs'].dropna().unique())

            all_claimed_gpus.update(claimed_gpus)
            all_available_gpus.update(claimed_gpus)  # claimed GPUs are part of available
            all_available_gpus.update(unclaimed_gpus)

        total_claimed_across_intervals += len(all_claimed_gpus)
        total_available_across_intervals += len(all_available_gpus)
        num_intervals += 1

    # Calculate averages
    avg_claimed = total_claimed_across_intervals / num_intervals if num_intervals > 0 else 0
    avg_available = total_available_across_intervals / num_intervals if num_intervals > 0 else 0

    return {
        'claimed': avg_claimed,
        'total': avg_available
    }




def run_analysis(
    db_path: str,
    hours_back: int = 24,
    host: str = "",
    analysis_type: str = "allocation",
    bucket_minutes: int = 15,
    end_time: Optional[datetime.datetime] = None,
    group_by_device: bool = False,
    all_devices: bool = False,
    exclude_hosts: Optional[str] = None,
    exclude_hosts_yaml: Optional[str] = None,
    use_enhanced_classification: bool = True
) -> dict:
    """
    Core analysis function that can be called programmatically.

    Args:
        exclude_hosts: JSON string with host exclusions
        exclude_hosts_yaml: Path to YAML file with host exclusions
        use_enhanced_classification: Use enhanced backfill classification (default: True)

    Returns:
        Dictionary containing analysis results and metadata
    """
    # Set up host exclusions
    gpu_utils.HOST_EXCLUSIONS = load_host_exclusions(exclude_hosts, exclude_hosts_yaml)
    gpu_utils.FILTERED_HOSTS_INFO = []  # Reset tracking

    # Get filtered data
    df = get_time_filtered_data(db_path, hours_back, end_time)

    if len(df) == 0:
        return {"error": "No data found in the specified time range."}

    # Calculate time buckets for interval counting
    df_temp = df.copy()
    df_temp['timestamp'] = pd.to_datetime(df_temp['timestamp'])
    df_temp['15min_bucket'] = df_temp['timestamp'].dt.floor('15min')
    num_intervals = df_temp['15min_bucket'].nunique()

    result = {
        "metadata": {
            "start_time": df['timestamp'].min(),
            "end_time": df['timestamp'].max(),
            "num_intervals": num_intervals,
            "total_records": len(df),
            "hours_back": hours_back,
            "excluded_hosts": gpu_utils.HOST_EXCLUSIONS,
            "filtered_hosts_info": gpu_utils.FILTERED_HOSTS_INFO
        }
    }

    if analysis_type == "allocation":
        if group_by_device:
            result["device_stats"] = calculate_allocation_usage_by_device_enhanced(df, host, all_devices)
            result["memory_stats"] = calculate_allocation_usage_by_memory(df, host, all_devices)
            result["h200_user_stats"] = calculate_h200_user_breakdown(df, host, hours_back)
            result["raw_data"] = df  # Pass raw data for unique cluster totals calculation
            result["host_filter"] = host  # Pass host filter for consistency
        else:
            result["allocation_stats"] = calculate_allocation_usage_enhanced(df, host)

    elif analysis_type == "timeseries":
        result["timeseries_data"] = calculate_time_series_usage(df, bucket_minutes, host)

    elif analysis_type == "monthly":
        result["monthly_stats"] = calculate_monthly_summary(db_path, end_time)

    return result


def calculate_monthly_summary(db_path: str, end_time: Optional[datetime.datetime] = None) -> dict:
    """
    Calculate complete monthly GPU usage summary for the previous month.

    Args:
        db_path: Path to SQLite database (used to determine base directory)
        end_time: Optional end time (defaults to latest data)

    Returns:
        Dictionary containing monthly usage statistics
    """
    from pathlib import Path
    import calendar

    # Get base directory from the provided db_path
    db_path_obj = Path(db_path)
    base_dir = str(db_path_obj.parent) if db_path_obj.parent != Path('.') else "."

    # If end_time is not provided, use the latest timestamp from the most recent database
    if end_time is None:
        end_time = get_latest_timestamp_from_most_recent_db(base_dir)
        if end_time is None:
            end_time = datetime.datetime.now()

    # Calculate previous month range
    current_month = end_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_month_end = current_month - datetime.timedelta(seconds=1)
    prev_month_start = prev_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # Calculate total hours in the previous month
    days_in_month = calendar.monthrange(prev_month_start.year, prev_month_start.month)[1]
    total_hours = days_in_month * 24

    print(f"Calculating monthly summary for {prev_month_start.strftime('%B %Y')}")
    print(f"Period: {prev_month_start} to {prev_month_end}")
    print(f"Total hours in month: {total_hours}")

    # Get data for the entire previous month
    df = get_time_filtered_data(db_path, total_hours, prev_month_end + datetime.timedelta(seconds=1))

    if df.empty:
        return {
            "error": f"No data found for {prev_month_start.strftime('%B %Y')}",
            "month": prev_month_start.strftime('%B %Y'),
            "start_date": prev_month_start,
            "end_date": prev_month_end,
            "total_hours": total_hours
        }

    # Calculate statistics for the month
    device_stats = calculate_allocation_usage_by_device_enhanced(df, "", False)  # All devices, no host filter
    memory_stats = calculate_allocation_usage_by_memory(df, "", False)  # All devices, no host filter
    h200_stats = calculate_h200_user_breakdown(df, "", total_hours)

    return {
        "month": prev_month_start.strftime('%B %Y'),
        "start_date": prev_month_start,
        "end_date": prev_month_end,
        "total_hours": total_hours,
        "device_stats": device_stats,
        "memory_stats": memory_stats,
        "h200_user_stats": h200_stats,
        "data_coverage": {
            "start_time": df['timestamp'].min(),
            "end_time": df['timestamp'].max(),
            "total_records": len(df),
            "unique_intervals": len(df['15min_bucket'].unique()) if '15min_bucket' in df.columns else 0
        }
    }


def get_gpu_models_at_time(
    db_path: str,
    target_time: datetime.datetime,
    window_minutes: int = 5
) -> list:
    """
    Get all GPU models available at a specific time.

    Args:
        db_path: Path to SQLite database
        target_time: Time to query for GPU models
        window_minutes: Time window around target_time to search (default: 5 minutes)

    Returns:
        List of GPU model names available at the specified time
    """
    conn = sqlite3.connect(db_path)

    # Define time window
    start_time = target_time - datetime.timedelta(minutes=window_minutes)
    end_time = target_time + datetime.timedelta(minutes=window_minutes)

    query = """
    SELECT DISTINCT GPUs_DeviceName
    FROM gpu_state
    WHERE GPUs_DeviceName IS NOT NULL
    AND timestamp BETWEEN ? AND ?
    ORDER BY GPUs_DeviceName
    """

    df = pd.read_sql_query(query, conn, params=[start_time, end_time])
    conn.close()

    return df['GPUs_DeviceName'].tolist()


def get_gpu_model_activity_at_time(
    db_path: str,
    gpu_model: str,
    target_time: datetime.datetime,
    window_minutes: int = 5
) -> pd.DataFrame:
    """
    Get detailed activity for a specific GPU model at a specific time.

    Args:
        db_path: Path to SQLite database
        gpu_model: GPU model name (e.g., 'NVIDIA A100-SXM4-80GB')
        target_time: Time to query for activity
        window_minutes: Time window around target_time to search (default: 5 minutes)

    Returns:
        DataFrame with detailed GPU activity information
    """
    conn = sqlite3.connect(db_path)

    # Define time window
    start_time = target_time - datetime.timedelta(minutes=window_minutes)
    end_time = target_time + datetime.timedelta(minutes=window_minutes)

    query = """
    SELECT timestamp, Name, AssignedGPUs, State, GPUs_DeviceName,
           GPUsAverageUsage, Machine, RemoteOwner, GlobalJobId,
           PrioritizedProjects
    FROM gpu_state
    WHERE GPUs_DeviceName = ?
    AND timestamp BETWEEN ? AND ?
    ORDER BY timestamp DESC, Machine, AssignedGPUs
    """

    df = pd.read_sql_query(query, conn, params=[gpu_model, start_time, end_time])
    conn.close()

    if len(df) > 0:
        df['timestamp'] = pd.to_datetime(df['timestamp'])

    return df


def analyze_gpu_model_at_time(
    db_path: str,
    gpu_model: str,
    target_time: datetime.datetime,
    window_minutes: int = 5
) -> dict:
    """
    Analyze what's happening with a specific GPU model at a specific time.

    Args:
        db_path: Path to SQLite database
        gpu_model: GPU model name
        target_time: Time to analyze
        window_minutes: Time window to search

    Returns:
        Dictionary with analysis results
    """
    df = get_gpu_model_activity_at_time(db_path, gpu_model, target_time, window_minutes)

    if len(df) == 0:
        return {
            "error": f"No data found for {gpu_model} around {target_time.strftime('%Y-%m-%d %H:%M:%S')}"
        }

    # Get the closest timestamp to target
    df['time_diff'] = abs(df['timestamp'] - target_time)
    closest_time = df.loc[df['time_diff'].idxmin(), 'timestamp']

    # Filter to records from the closest timestamp
    snapshot_df = df[df['timestamp'] == closest_time]

    # Analyze the snapshot - count unique GPUs only
    unique_gpus = snapshot_df['AssignedGPUs'].dropna().nunique()

    # Count active GPUs (those actually running jobs with RemoteOwner)
    active_gpus_count = snapshot_df[
        (snapshot_df['State'] == 'Claimed') &
        (snapshot_df['RemoteOwner'].notna())
    ]['AssignedGPUs'].dropna().nunique()

    # Count idle GPUs (those not running jobs)
    idle_gpus_count = unique_gpus - active_gpus_count

    total_gpus = unique_gpus
    claimed_gpus = active_gpus_count  # Rename for compatibility with existing code
    unclaimed_gpus = idle_gpus_count  # Rename for compatibility with existing code

    # Categorize by utilization class
    priority_gpus = filter_df(snapshot_df, "Priority", "", "")
    shared_gpus = filter_df(snapshot_df, "Shared", "", "")
    backfill_gpus = filter_df(snapshot_df, "Backfill", "", "")

    # Get unique machines
    machines = snapshot_df['Machine'].unique()

    # Calculate utilization stats
    claimed_with_usage = snapshot_df[
        (snapshot_df['State'] == 'Claimed') &
        (snapshot_df['GPUsAverageUsage'].notna())
    ]

    avg_utilization = claimed_with_usage['GPUsAverageUsage'].mean() if len(claimed_with_usage) > 0 else 0

    # Get job information - ensure unique GPU IDs
    active_jobs_df = snapshot_df[
        (snapshot_df['State'] == 'Claimed') &
        (snapshot_df['RemoteOwner'].notna())
    ][['RemoteOwner', 'GlobalJobId', 'AssignedGPUs', 'Machine']].copy()

    # Remove duplicates based on AssignedGPUs, keeping first occurrence
    active_jobs = active_jobs_df.drop_duplicates(subset=['AssignedGPUs'], keep='first')

    # Get inactive GPUs - ensure unique GPU IDs and exclude ones that appear in active jobs
    inactive_gpus_df = snapshot_df[
        snapshot_df['State'] == 'Unclaimed'
    ][['AssignedGPUs', 'Machine', 'PrioritizedProjects']].copy()

    # Remove duplicates based on AssignedGPUs, keeping first occurrence
    inactive_gpus_unique = inactive_gpus_df.drop_duplicates(subset=['AssignedGPUs'], keep='first')

    # Get list of GPU IDs that are active (have jobs running)
    active_gpu_ids = set(active_jobs['AssignedGPUs'].dropna().tolist())

    # Filter out GPUs that appear in active jobs list
    inactive_gpus = inactive_gpus_unique[~inactive_gpus_unique['AssignedGPUs'].isin(active_gpu_ids)]

    return {
        "gpu_model": gpu_model,
        "snapshot_time": closest_time,
        "target_time": target_time,
        "window_minutes": window_minutes,
        "summary": {
            "total_gpus": total_gpus,
            "claimed_gpus": claimed_gpus,  # This is now active_gpus_count
            "unclaimed_gpus": unclaimed_gpus,  # This is now idle_gpus_count
            "utilization_percent": (claimed_gpus / total_gpus * 100) if total_gpus > 0 else 0,
            "avg_gpu_usage_percent": avg_utilization * 100 if avg_utilization else 0,
            "num_machines": len(machines)
        },
        "by_class": {
            "Priority": {
                "total": priority_gpus['AssignedGPUs'].dropna().nunique(),
                "claimed": priority_gpus[priority_gpus['State'] == 'Claimed']['AssignedGPUs'].dropna().nunique()
            },
            "Shared": {
                "total": shared_gpus['AssignedGPUs'].dropna().nunique(),
                "claimed": shared_gpus[shared_gpus['State'] == 'Claimed']['AssignedGPUs'].dropna().nunique()
            },
            "Backfill": {
                "total": backfill_gpus['AssignedGPUs'].dropna().nunique(),
                "claimed": backfill_gpus[backfill_gpus['State'] == 'Claimed']['AssignedGPUs'].dropna().nunique()
            }
        },
        "machines": list(machines),
        "active_jobs": active_jobs.to_dict('records') if len(active_jobs) > 0 else [],
        "inactive_gpus": inactive_gpus.to_dict('records') if len(inactive_gpus) > 0 else [],
        "raw_data": snapshot_df
    }


def print_gpu_model_analysis(analysis: dict):
    """Print GPU model analysis results in a formatted way."""
    if "error" in analysis:
        print(analysis["error"])
        return

    gpu_model = analysis["gpu_model"]
    snapshot_time = analysis["snapshot_time"]
    target_time = analysis["target_time"]
    summary = analysis["summary"]
    by_class = analysis["by_class"]
    machines = analysis["machines"]
    active_jobs = analysis["active_jobs"]
    inactive_gpus = analysis["inactive_gpus"]

    print(f"\n{'='*80}")
    print(f"GPU MODEL ACTIVITY REPORT: {gpu_model}")
    print(f"{'='*80}")
    print(f"Target Time: {target_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Snapshot Time: {snapshot_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Time Difference: {abs((snapshot_time - target_time).total_seconds())} seconds")

    print("\nSUMMARY:")
    print(f"{'-'*40}")
    print(f"Total GPUs: {summary['total_gpus']}")
    print(f"Active (with jobs): {summary['claimed_gpus']} ({summary['utilization_percent']:.1f}%)")
    print(f"Idle (no jobs): {summary['unclaimed_gpus']}")
    print(f"Avg GPU Usage: {summary['avg_gpu_usage_percent']:.1f}%")
    print(f"Machines: {summary['num_machines']}")

    # Separate real slots (Priority + Shared) from backfill slots
    real_slot_classes = ['Priority', 'Shared']
    backfill_slot_classes = ['Backfill']

    # Calculate totals for real slots
    real_total = sum(by_class[class_name]['total'] for class_name in real_slot_classes if class_name in by_class)
    real_claimed = sum(by_class[class_name]['claimed'] for class_name in real_slot_classes if class_name in by_class)
    real_usage_pct = (real_claimed / real_total * 100) if real_total > 0 else 0

    # Calculate totals for backfill slots
    backfill_total = sum(by_class[class_name]['total'] for class_name in backfill_slot_classes if class_name in by_class)
    backfill_claimed = sum(by_class[class_name]['claimed'] for class_name in backfill_slot_classes if class_name in by_class)
    backfill_usage_pct = (backfill_claimed / backfill_total * 100) if backfill_total > 0 else 0

    print("\nREAL SLOTS:")
    print(f"{'-'*40}")
    print(f"  TOTAL: {real_claimed}/{real_total} ({real_usage_pct:.1f}%)")
    print(f"{'-'*40}")
    for class_name in real_slot_classes:
        if class_name in by_class and by_class[class_name]['total'] > 0:
            stats = by_class[class_name]
            usage_pct = (stats['claimed'] / stats['total'] * 100) if stats['total'] > 0 else 0
            print(f"  {class_name}: {stats['claimed']}/{stats['total']} ({usage_pct:.1f}%)")


    print("\nBACKFILL SLOTS:")
    print(f"{'-'*40}")
    print(f"  TOTAL: {backfill_claimed}/{backfill_total} ({backfill_usage_pct:.1f}%)")
    print(f"{'-'*40}")
    for class_name in backfill_slot_classes:
        if class_name in by_class and by_class[class_name]['total'] > 0:
            stats = by_class[class_name]
            usage_pct = (stats['claimed'] / stats['total'] * 100) if stats['total'] > 0 else 0
            print(f"  {class_name}: {stats['claimed']}/{stats['total']} ({usage_pct:.1f}%)")

    print(f"\nMACHINES ({len(machines)}):")
    print(f"{'-'*40}")
    for machine in sorted(machines):
        print(f"  {machine}")

    if active_jobs:
        print(f"\nACTIVE JOBS ({len(active_jobs)}):")
        print(f"{'-'*60}")
        print("  User                | Job ID          | GPU ID      | Machine")
        print(f"{'-'*60}")
        for job in active_jobs:
            user = (job.get('RemoteOwner') or 'N/A')[:19]
            job_id = (job.get('GlobalJobId') or 'N/A')[:14]
            gpu_id = (job.get('AssignedGPUs') or 'N/A')[:11]
            machine = (job.get('Machine') or 'N/A')[:19]
            print(f"  {user:<18} | {job_id:<15} | {gpu_id:<11} | {machine}")
    else:
        print("\nNo active jobs found.")

    if inactive_gpus:
        print(f"\nINACTIVE GPUs ({len(inactive_gpus)}):")
        print(f"{'-'*60}")
        print("  GPU ID      | Machine             | Priority Projects")
        print(f"{'-'*60}")
        for gpu in inactive_gpus:
            gpu_id = (gpu.get('AssignedGPUs') or 'N/A')[:11]
            machine = (gpu.get('Machine') or 'N/A')[:19]
            priority_projects = (gpu.get('PrioritizedProjects') or 'None')[:29]
            print(f"  {gpu_id:<11} | {machine:<19} | {priority_projects}")
    else:
        print("\nNo inactive GPUs found.")


# Removed chart generation function - not needed for simple HTML tables


# Removed number_format function - not needed for simple HTML tables


def send_email_report(
    html_content: str,
    to_email: str,
    from_email: str = "iaross@wisc.edu",
    smtp_server: str = "smtp.wiscmail.wisc.edu",
    smtp_port: int = 25,
    subject_prefix: str = "CHTC GPU Allocation",
    usage_percentages: Optional[Dict[str, float]] = None,
    lookback_hours: Optional[int] = None,
    use_auth: bool = False,
    timeout: int = 30,
    debug: bool = False,
    device_stats: Optional[Dict] = None,
    analysis_type = None,
    month = None
) -> bool:
    """
    Send HTML report via email using SMTP, matching mailx behavior.

    Args:
        html_content: HTML content to send
        to_email: Recipient email address(es) - can be comma-separated
        from_email: Sender email address
        smtp_server: SMTP server hostname
        smtp_port: SMTP server port
        subject_prefix: Subject line prefix
        usage_percentages: Dict of class usage percentages (e.g., {"Shared": 65.2, "Priority": 85.5})
        lookback_hours: Number of hours covered by the report (e.g., 24, 168)
        use_auth: Whether to use SMTP authentication
        timeout: Connection timeout in seconds
        debug: Enable debug output

    Returns:
        True if email sent successfully, False otherwise
    """
    try:
        # Parse comma-separated email addresses
        recipients = [email.strip() for email in to_email.split(',') if email.strip()]

        if not recipients:
            print("Error: No valid email addresses provided")
            return False

        # Create message
        msg = MIMEMultipart('alternative')
        today = datetime.datetime.now().strftime('%Y-%m-%d')

        # Build subject with lookback period and usage percentages
        subject = f"{subject_prefix} {today}"

        # Add lookback period
        if lookback_hours:
            if lookback_hours % (24 * 7) == 0 and lookback_hours >= (24 * 7):  # Exact weeks
                weeks = lookback_hours // (24 * 7)
                period_str = f"{weeks}w" if weeks > 1 else "1w"
            elif lookback_hours > 24 and lookback_hours % 24 == 0:  # Days for > 24h
                days = lookback_hours // 24
                period_str = f"{days}d"
            else:  # Hours for <= 24h or non-exact days
                period_str = f"{lookback_hours}h"
            if analysis_type == "monthly":
                subject += f" {month}"
            else:
                subject += f" {period_str}"

        # Add usage percentages in order: Prioritized (Researcher), Prioritized (CHTC), Open Capacity, Backfill
        if usage_percentages:
            class_order = [
                "Priority-ResearcherOwned",  # Prioritized (Researcher Owned)
                "Priority-CHTCOwned",  # Prioritized (CHTC Owned)
                "Shared",  # Open Capacity
                "Backfill"  # Backfill (all types combined)
            ]
            usage_parts = []
            for class_name in class_order:
                if class_name in usage_percentages:
                    percentage = usage_percentages[class_name]
                    usage_parts.append(f"{percentage:.1f}%")
                elif class_name == "Backfill":
                    # For Backfill, combine all backfill types
                    backfill_types = ["Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]
                    total_claimed = 0
                    total_available = 0

                    if device_stats:
                        for backfill_type in backfill_types:
                            if backfill_type in device_stats:
                                device_data = device_stats[backfill_type]
                                if device_data:
                                    total_claimed += sum(stats['avg_claimed'] for stats in device_data.values())
                                    total_available += sum(stats['avg_total_available'] for stats in device_data.values())

                        if total_available > 0:
                            combined_percentage = (total_claimed / total_available) * 100
                            usage_parts.append(f"{combined_percentage:.1f}%")

            if usage_parts:
                subject += f" ({' | '.join(usage_parts)})"

        msg['Subject'] = subject
        if debug:
            print(f"DEBUG: Email subject would be: {subject}")
        msg['From'] = from_email
        msg['To'] = ', '.join(recipients)

        # Attach HTML content
        html_part = MIMEText(html_content, 'html')
        msg.attach(html_part)

        # Try multiple ports if connection fails (common university SMTP setup)
        ports_to_try = [smtp_port]
        if smtp_port != 25:
            ports_to_try.append(25)
        if smtp_port != 587:
            ports_to_try.append(587)

        last_error = None

        for port in ports_to_try:
            try:
                print(f"Connecting to SMTP server {smtp_server}:{port}...")

                # Send email - match mailx behavior more closely
                with smtplib.SMTP(smtp_server, port, timeout=timeout) as server:
                    # Enable debug output for troubleshooting if requested
                    if debug:
                        server.set_debuglevel(1)

                    # Try STARTTLS, but don't fail if not available (like mailx)
                    try:
                        server.starttls()
                        print("STARTTLS enabled")
                    except smtplib.SMTPNotSupportedError:
                        print("STARTTLS not supported, proceeding without encryption")
                    except Exception as e:
                        print(f"STARTTLS failed: {e}, proceeding without encryption")

                    # University SMTP servers often don't require auth from internal networks
                    # Only use auth if explicitly requested
                    if use_auth:
                        print("Note: Authentication not attempted (matching mailx behavior)")

                    server.send_message(msg, to_addrs=recipients)
                    print(f"Email sent successfully to {len(recipients)} recipient(s): {', '.join(recipients)}")
                    return True

            except (smtplib.SMTPException, OSError) as e:
                last_error = e
                print(f"Failed to connect on port {port}: {e}")
                continue

        # If we get here, all ports failed
        raise last_error or Exception("All SMTP ports failed")

    except smtplib.SMTPException as e:
        print(f"SMTP error sending email: {e}")
        return False
    except Exception as e:
        print(f"Error sending email: {e}")
        return False


def simple_markdown_to_html(markdown_text: str) -> str:
    """Convert simple markdown to HTML. Supports headers, bold, lists, and paragraphs."""
    lines = markdown_text.split('\n')
    html_lines = []
    in_list = False

    for line in lines:
        line = line.strip()
        if not line:
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append("")
            continue

        # Headers
        if line.startswith('# '):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h2>{line[2:]}</h2>")
        # Bold text and convert **text:** to <strong>text:</strong>
        elif '**' in line:
            # Handle list items
            if line.startswith('- '):
                if not in_list:
                    html_lines.append("<ul>")
                    in_list = True
                line = line[2:]  # Remove "- "
                line = line.replace('**', '<strong>', 1).replace('**', '</strong>', 1)
                html_lines.append(f"<li>{line}</li>")
            else:
                # Regular paragraph with bold
                if in_list:
                    html_lines.append("</ul>")
                    in_list = False
                line = line.replace('**', '<strong>', 1).replace('**', '</strong>', 1)
                html_lines.append(f"<p>{line}</p>")
        # Regular list items
        elif line.startswith('- '):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            html_lines.append(f"<li>{line[2:]}</li>")
        # Regular paragraphs
        else:
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<p>{line}</p>")

    if in_list:
        html_lines.append("</ul>")

    return '\n'.join(html_lines)

def load_methodology() -> str:
    """Load methodology from external markdown file and convert to HTML."""
    methodology_path = Path(__file__).parent / "methodology.md"
    try:
        with open(methodology_path, 'r', encoding='utf-8') as f:
            markdown_content = f.read()
        return simple_markdown_to_html(markdown_content)
    except FileNotFoundError:
        return "<p><em>Methodology file not found.</em></p>"
    except Exception as e:
        return f"<p><em>Error loading methodology: {e}</em></p>"


def generate_html_report(results: dict, output_file: Optional[str] = None) -> str:
    """
    Generate a simple HTML report with tables from analysis results.

    Args:
        results: Analysis results dictionary
        output_file: Optional path to save HTML file

    Returns:
        HTML content as string
    """
    if "error" in results:
        return f"<html><body><h1>Error</h1><p>{results['error']}</p></body></html>"

    # Handle monthly summary - convert to regular results format and use existing HTML generation
    if "monthly_stats" in results:
        monthly_stats = results["monthly_stats"]
        if "error" in monthly_stats:
            return f"<html><body><h1>Monthly Summary Error</h1><p>{monthly_stats['error']}</p></body></html>"

        # Convert monthly stats to regular results format so we can reuse existing HTML generation
        regular_results = {
            "metadata": {
                "hours_back": monthly_stats["total_hours"],
                "start_time": monthly_stats.get("start_date"),
                "end_time": monthly_stats.get("end_date"),
                "num_intervals": monthly_stats["data_coverage"].get("unique_intervals", 0),
                "total_records": monthly_stats["data_coverage"].get("total_records", 0),
                "excluded_hosts": {},
                "filtered_hosts_info": {}
            }
        }

        # Copy the stats from monthly to regular format
        if "device_stats" in monthly_stats:
            regular_results["device_stats"] = monthly_stats["device_stats"]
        if "memory_stats" in monthly_stats:
            regular_results["memory_stats"] = monthly_stats["memory_stats"]
        if "h200_user_stats" in monthly_stats:
            regular_results["h200_user_stats"] = monthly_stats["h200_user_stats"]
        if "raw_data" in monthly_stats:
            regular_results["raw_data"] = monthly_stats["raw_data"]
        if "host_filter" in monthly_stats:
            regular_results["host_filter"] = monthly_stats["host_filter"]

        # Use existing HTML generation but with monthly title
        html_content = generate_html_report(regular_results, output_file)

        # Update the title to indicate it's a monthly report
        start_date_str = monthly_stats['start_date'].strftime('%B %Y') if hasattr(monthly_stats['start_date'], 'strftime') else str(monthly_stats['month'])
        html_content = html_content.replace(
            "<title>CHTC GPU Allocation Report</title>",
            f"<title>CHTC Monthly GPU Report - {start_date_str}</title>"
        ).replace(
            "<h1>CHTC GPU ALLOCATION REPORT</h1>",
            f"<h1>CHTC MONTHLY GPU REPORT - {start_date_str.upper()}</h1>"
        )

        return html_content

    metadata = results["metadata"]

    # Start building HTML
    html_parts = []
    html_parts.append("<!DOCTYPE html>")
    html_parts.append("<html>")
    html_parts.append("<head>")
    html_parts.append("<title>CHTC GPU Allocation Report</title>")
    html_parts.append("</head>")
    html_parts.append("<body>")

    # Header
    html_parts.append("<h1>CHTC GPU ALLOCATION REPORT</h1>")
    # Simplified period format: just the lookback hours
    hours_back = metadata.get('hours_back', 24)
    hours_str = str(int(hours_back)) if hours_back == int(hours_back) else str(hours_back)
    hour_word = "hour" if hours_back == 1 else "hours"
    period_str = f"{hours_str} {hour_word}"
    html_parts.append(f"<p><strong>Period:</strong> {period_str}</p>")
    html_parts.append(f"<p><strong>Generated:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>")

    # Check if we have device stats for cluster summary
    device_stats = results.get("device_stats", {})
    class_totals = {}

    # Pre-calculate class totals if we have device stats
    if device_stats:
        for class_name in CLASS_ORDER:
            device_data = device_stats.get(class_name, {})
            if device_data:
                total_claimed = 0
                total_available = 0
                for device_type, stats in device_data.items():
                    total_claimed += stats['avg_claimed']
                    total_available += stats['avg_total_available']

                if total_available > 0:
                    class_totals[class_name] = {
                        'claimed': total_claimed,
                        'total': total_available,
                        'percent': (total_claimed / total_available) * 100
                    }

    # Cluster summary at the top with real slots and backfill slots separated
    if class_totals:
        # Separate real slots from backfill slots
        real_slot_classes = ["Priority-ResearcherOwned", "Priority-CHTCOwned", "Shared"]
        backfill_slot_classes = ["Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]

        # Calculate totals for real slots
        real_claimed = sum(class_totals[c]['claimed'] for c in real_slot_classes if c in class_totals)
        real_total = sum(class_totals[c]['total'] for c in real_slot_classes if c in class_totals)
        real_percent = (real_claimed / real_total * 100) if real_total > 0 else 0

        # Calculate totals for backfill slots
        backfill_claimed = sum(class_totals[c]['claimed'] for c in backfill_slot_classes if c in class_totals)
        backfill_total = sum(class_totals[c]['total'] for c in backfill_slot_classes if c in class_totals)
        backfill_percent = (backfill_claimed / backfill_total * 100) if backfill_total > 0 else 0

        # Real Slots Table
        html_parts.append("<h2>Real Slots</h2>")
        html_parts.append("<table border='1' style='margin-top: 20px;'>")
        html_parts.append("<tr style='background-color: #e0e0e0;'><th>Class</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>")

        # Total row for real slots
        html_parts.append("<tr style='background-color: #d0d0d0; font-weight: bold;'>")
        html_parts.append(f"<td style='font-weight: bold;'>TOTAL</td>")
        html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{real_percent:.1f}%</td>")
        html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{real_claimed:.1f}</td>")
        html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{real_total:.1f}</td>")
        html_parts.append("</tr>")

        # Individual real slot classes
        for class_name in real_slot_classes:
            if class_name in class_totals:
                totals = class_totals[class_name]
                html_parts.append("<tr>")
                html_parts.append(f"<td style='font-weight: bold;'>{get_display_name(class_name)}</td>")
                html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['percent']:.1f}%</td>")
                html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['claimed']:.1f}</td>")
                html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['total']:.1f}</td>")
                html_parts.append("</tr>")
        html_parts.append("</table>")

        # Real Slots by Memory Category Table
        if "memory_stats" in results:
            memory_stats = results["memory_stats"]
            if memory_stats:
                html_parts.append("<h2>Real Slots by Memory Category</h2>")
                html_parts.append("<table border='1' style='margin-top: 20px;'>")
                html_parts.append("<tr style='background-color: #e0e0e0;'><th>Memory Category</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>")

                # Calculate totals for memory categories
                memory_total_claimed = sum(stats['avg_claimed'] for stats in memory_stats.values())
                memory_total_available = sum(stats['avg_total_available'] for stats in memory_stats.values())
                memory_total_percent = (memory_total_claimed / memory_total_available * 100) if memory_total_available > 0 else 0

                # Total row for memory categories
                html_parts.append("<tr style='background-color: #d0d0d0; font-weight: bold;'>")
                html_parts.append(f"<td style='font-weight: bold;'>TOTAL</td>")
                html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{memory_total_percent:.1f}%</td>")
                html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{memory_total_claimed:.1f}</td>")
                html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{memory_total_available:.1f}</td>")
                html_parts.append("</tr>")

                # Sort memory categories by numerical value
                def sort_memory_categories(categories):
                    def get_sort_key(cat):
                        if cat == "Unknown":
                            return 999  # Put Unknown at the end
                        elif cat.startswith("<"):
                            # Handle <48GB - extract the number after <
                            return float(cat[1:-2])  # Remove "<" and "GB"
                        elif cat.startswith(">"):
                            # Handle >80GB - extract the number after >
                            return float(cat[1:-2]) + 0.1  # Add 0.1 to sort after exact values
                        elif cat.endswith("GB+"):
                            return float(cat[:-3])  # Remove "GB+" suffix
                        elif cat.endswith("GB"):
                            if "-" in cat:
                                # Handle ranges like "10-12GB"
                                return float(cat.split("-")[0])
                            else:
                                return float(cat[:-2])  # Remove "GB" suffix
                        else:
                            return 0  # Fallback
                    return sorted(categories, key=get_sort_key)

                # Individual memory categories (sorted by memory size)
                sorted_memory_cats = sort_memory_categories(memory_stats.keys())
                for memory_cat in sorted_memory_cats:
                    stats = memory_stats[memory_cat]
                    html_parts.append("<tr>")
                    html_parts.append(f"<td style='font-weight: bold;'>{memory_cat}</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{stats['allocation_usage_percent']:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{stats['avg_claimed']:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{stats['avg_total_available']:.1f}</td>")
                    html_parts.append("</tr>")

                html_parts.append("</table>")

        # Backfill Slots Table
        html_parts.append("<h2>Backfill Slots</h2>")
        html_parts.append("<table border='1' style='margin-top: 20px;'>")
        html_parts.append("<tr style='background-color: #e0e0e0;'><th>Class</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>")

        # Total row for backfill slots
        html_parts.append("<tr style='background-color: #d0d0d0; font-weight: bold;'>")
        html_parts.append(f"<td style='font-weight: bold;'>TOTAL</td>")
        html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{backfill_percent:.1f}%</td>")
        html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{backfill_claimed:.1f}</td>")
        html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{backfill_total:.1f}</td>")
        html_parts.append("</tr>")

        # Individual backfill slot classes
        for class_name in backfill_slot_classes:
            if class_name in class_totals:
                totals = class_totals[class_name]
                html_parts.append("<tr>")
                html_parts.append(f"<td style='font-weight: bold;'>{get_display_name(class_name)}</td>")
                html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['percent']:.1f}%</td>")
                html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['claimed']:.1f}</td>")
                html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['total']:.1f}</td>")
                html_parts.append("</tr>")
        html_parts.append("</table>")

    if "allocation_stats" in results:
        html_parts.append("<h2>Allocation Summary</h2>")
        html_parts.append("<table border='1'>")
        html_parts.append("<tr><th>Class</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>")

        allocation_stats = results["allocation_stats"]

        for class_name in CLASS_ORDER:
            if class_name in allocation_stats:
                stats = allocation_stats[class_name]
                html_parts.append("<tr>")
                html_parts.append(f"<td>{get_display_name(class_name)}</td>")
                html_parts.append(f"<td style='text-align: right'>{stats['allocation_usage_percent']:.1f}%</td>")
                html_parts.append(f"<td style='text-align: right'>{stats['avg_claimed']:.1f}</td>")
                html_parts.append(f"<td style='text-align: right'>{stats['avg_total_available']:.1f}</td>")
                html_parts.append("</tr>")

        html_parts.append("</table>")
    # Device stats tables
    elif "device_stats" in results:
        # H200 Usage by Slot Type (positioned after backfill slots, before device type details)
        if "h200_user_stats" in results:
            h200_stats = results["h200_user_stats"]
            if h200_stats:
                html_parts.append("<h2>H200 Usage by Slot Type and User</h2>")

                # First, aggregate data by slot type
                slot_type_totals = {}
                slot_type_users = {}

                for user, user_data in h200_stats.items():
                    for slot_type, slot_data in user_data['slot_breakdown'].items():
                        if slot_type not in slot_type_totals:
                            slot_type_totals[slot_type] = 0
                            slot_type_users[slot_type] = []

                        slot_type_totals[slot_type] += slot_data['gpu_hours']
                        slot_type_users[slot_type].append({
                            'user': user,
                            'gpu_hours': slot_data['gpu_hours'],
                            'percentage': slot_data['percentage']
                        })

                # Sort slot types by total GPU hours (descending)
                sorted_slot_types = sorted(slot_type_totals.items(), key=lambda x: x[1], reverse=True)

                # Single table with slot type totals and user breakdowns
                html_parts.append("<table border='1' style='margin-top: 20px;'>")
                html_parts.append("<tr style='background-color: #e0e0e0;'><th>Slot Type / User</th><th>GPU-Hours</th><th>% of Total</th></tr>")

                total_gpu_hours = sum(slot_type_totals.values())

                for slot_type, total_hours in sorted_slot_types:
                    display_name = get_display_name(slot_type)
                    user_count = len(slot_type_users[slot_type])
                    percentage = (total_hours / total_gpu_hours * 100) if total_gpu_hours > 0 else 0

                    # Slot type total row
                    html_parts.append("<tr style='background-color: #f0f0f0;'>")
                    html_parts.append(f"<td style='font-weight: bold;'>{display_name} ({user_count} users)</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{total_hours:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{percentage:.1f}%</td>")
                    html_parts.append("</tr>")

                    # User breakdown rows beneath the slot type total
                    users = slot_type_users[slot_type]
                    users.sort(key=lambda x: x['gpu_hours'], reverse=True)

                    for user_info in users:
                        user = user_info['user']
                        gpu_hours = user_info['gpu_hours']
                        user_total_percentage = (gpu_hours / total_gpu_hours * 100) if total_gpu_hours > 0 else 0

                        html_parts.append("<tr>")
                        html_parts.append(f"<td style='padding-left: 30px;'>{user}</td>")
                        html_parts.append(f"<td style='text-align: right;'>{gpu_hours:.1f}</td>")
                        html_parts.append(f"<td style='text-align: right;'>{user_total_percentage:.1f}%</td>")
                        html_parts.append("</tr>")

                html_parts.append("</table>")

        html_parts.append("<h2>Usage by Device Type</h2>")

        for class_name in CLASS_ORDER:
            device_data = device_stats.get(class_name, {})
            if device_data:
                html_parts.append(f"<h3>{get_display_name(class_name)}</h3>")
                html_parts.append("<table border='1'>")
                html_parts.append("<tr><th>Device Type</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>")

                # Calculate totals first
                total_claimed = 0
                total_available = 0
                for device_type, stats in sorted(device_data.items()):
                    total_claimed += stats['avg_claimed']
                    total_available += stats['avg_total_available']

                # Add total row first
                if total_available > 0:
                    total_percent = (total_claimed / total_available) * 100
                    html_parts.append("<tr style='font-weight: bold; background-color: #f0f0f0;'>")
                    html_parts.append("<td>TOTAL</td>")
                    html_parts.append(f"<td style='text-align: right'>{total_percent:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{total_claimed:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{total_available:.1f}</td>")
                    html_parts.append("</tr>")

                    class_totals[class_name] = {
                        'claimed': total_claimed,
                        'total': total_available,
                        'percent': total_percent
                    }

                # Add individual device rows (sorted alphabetically)
                for device_type, stats in sorted(device_data.items()):
                    short_name = get_human_readable_device_name(device_type)
                    html_parts.append("<tr>")
                    html_parts.append(f"<td>{short_name}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['allocation_usage_percent']:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['avg_claimed']:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['avg_total_available']:.1f}</td>")
                    html_parts.append("</tr>")

                html_parts.append("</table>")

        # Machine categories table for enhanced view
        if "machine_categories" in results:
            html_parts.append("<h2>Machine Categories</h2>")
            machine_categories = results["machine_categories"]

            for category, machines in machine_categories.items():
                if machines:  # Only show categories that have machines
                    html_parts.append(f"<h3>{category} ({len(machines)} machines)</h3>")
                    html_parts.append("<table border='1'>")
                    html_parts.append("<tr><th>Machine</th></tr>")

                    for machine in machines:
                        html_parts.append("<tr>")
                        html_parts.append(f"<td>{machine}</td>")
                        html_parts.append("</tr>")

                    html_parts.append("</table>")


    # Device stats tables
    elif "device_stats" in results:
        html_parts.append("<h2>Usage by Device Type</h2>")

        device_stats = results["device_stats"]
        class_totals = {}

        # Define the order: Open Capacity, Prioritized Service, Backfill
        class_order = ["Shared", "Priority", "Backfill"]  # Internal names

        for class_name in class_order:
            device_data = device_stats.get(class_name, {})
            if device_data:
                html_parts.append(f"<h3>{get_display_name(class_name)}</h3>")
                html_parts.append("<table border='1'>")
                html_parts.append("<tr><th>Device Type</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>")

                # Calculate totals first
                total_claimed = 0
                total_available = 0
                for device_type, stats in sorted(device_data.items()):
                    total_claimed += stats['avg_claimed']
                    total_available += stats['avg_total_available']

                # Add total row first
                if total_available > 0:
                    total_percent = (total_claimed / total_available) * 100
                    html_parts.append("<tr style='font-weight: bold; background-color: #f0f0f0;'>")
                    html_parts.append("<td>TOTAL</td>")
                    html_parts.append(f"<td style='text-align: right'>{total_percent:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{total_claimed:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{total_available:.1f}</td>")
                    html_parts.append("</tr>")

                    class_totals[class_name] = {
                        'claimed': total_claimed,
                        'total': total_available,
                        'percent': total_percent
                    }

                # Add individual device rows (sorted alphabetically)
                for device_type, stats in sorted(device_data.items()):
                    short_name = get_human_readable_device_name(device_type)
                    html_parts.append("<tr>")
                    html_parts.append(f"<td>{short_name}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['allocation_usage_percent']:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['avg_claimed']:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['avg_total_available']:.1f}</td>")
                    html_parts.append("</tr>")

                html_parts.append("</table>")


        # Cluster summary with real slots and backfill slots separated
        if class_totals:
            # Separate real slots from backfill slots (using simplified class names)
            real_slot_classes = ["Shared", "Priority"]
            backfill_slot_classes = ["Backfill"]

            # Calculate totals for real slots
            real_claimed = sum(class_totals[c]['claimed'] for c in real_slot_classes if c in class_totals)
            real_total = sum(class_totals[c]['total'] for c in real_slot_classes if c in class_totals)
            real_percent = (real_claimed / real_total * 100) if real_total > 0 else 0

            # Calculate totals for backfill slots
            backfill_claimed = sum(class_totals[c]['claimed'] for c in backfill_slot_classes if c in class_totals)
            backfill_total = sum(class_totals[c]['total'] for c in backfill_slot_classes if c in class_totals)
            backfill_percent = (backfill_claimed / backfill_total * 100) if backfill_total > 0 else 0

            # Real Slots Table
            html_parts.append("<h2>Real Slots</h2>")
            html_parts.append("<table border='1'>")
            html_parts.append("<tr><th>Class</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>")

            # Total row for real slots
            html_parts.append("<tr style='font-weight: bold; background-color: #f0f0f0;'>")
            html_parts.append("<td>TOTAL</td>")
            html_parts.append(f"<td style='text-align: right'>{real_percent:.1f}%</td>")
            html_parts.append(f"<td style='text-align: right'>{real_claimed:.1f}</td>")
            html_parts.append(f"<td style='text-align: right'>{real_total:.1f}</td>")
            html_parts.append("</tr>")

            # Individual real slot classes
            for class_name in real_slot_classes:
                if class_name in class_totals:
                    stats = class_totals[class_name]
                    html_parts.append("<tr>")
                    html_parts.append(f"<td>{get_display_name(class_name)}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['percent']:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['claimed']:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['total']:.1f}</td>")
                    html_parts.append("</tr>")
            html_parts.append("</table>")

            # Real Slots by Memory Category Table
            if "memory_stats" in results:
                memory_stats = results["memory_stats"]
                if memory_stats:
                    html_parts.append("<h2>Real Slots by Memory Category</h2>")
                    html_parts.append("<table border='1'>")
                    html_parts.append("<tr><th>Memory Category</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>")

                    # Calculate totals for memory categories
                    memory_total_claimed = sum(stats['avg_claimed'] for stats in memory_stats.values())
                    memory_total_available = sum(stats['avg_total_available'] for stats in memory_stats.values())
                    memory_total_percent = (memory_total_claimed / memory_total_available * 100) if memory_total_available > 0 else 0

                    # Total row for memory categories
                    html_parts.append("<tr style='font-weight: bold; background-color: #f0f0f0;'>")
                    html_parts.append("<td>TOTAL</td>")
                    html_parts.append(f"<td style='text-align: right'>{memory_total_percent:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{memory_total_claimed:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{memory_total_available:.1f}</td>")
                    html_parts.append("</tr>")

                    # Sort memory categories by numerical value
                    def sort_memory_categories(categories):
                        def get_sort_key(cat):
                            if cat == "Unknown":
                                return 999  # Put Unknown at the end
                            elif cat.endswith("GB+"):
                                return float(cat[:-3])  # Remove "GB+" suffix
                            elif cat.endswith("GB"):
                                if "-" in cat:
                                    # Handle ranges like "10-12GB"
                                    return float(cat.split("-")[0])
                                else:
                                    return float(cat[:-2])  # Remove "GB" suffix
                            else:
                                return 0  # Fallback
                        return sorted(categories, key=get_sort_key)

                    # Individual memory categories (sorted by memory size)
                    sorted_memory_cats = sort_memory_categories(memory_stats.keys())
                    for memory_cat in sorted_memory_cats:
                        stats = memory_stats[memory_cat]
                        html_parts.append("<tr>")
                        html_parts.append(f"<td>{memory_cat}</td>")
                        html_parts.append(f"<td style='text-align: right'>{stats['allocation_usage_percent']:.1f}%</td>")
                        html_parts.append(f"<td style='text-align: right'>{stats['avg_claimed']:.1f}</td>")
                        html_parts.append(f"<td style='text-align: right'>{stats['avg_total_available']:.1f}</td>")
                        html_parts.append("</tr>")

                    html_parts.append("</table>")


            # Backfill Slots Table
            html_parts.append("<h2>Backfill Slots</h2>")
            html_parts.append("<table border='1'>")
            html_parts.append("<tr><th>Class</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>")

            # Total row for backfill slots
            html_parts.append("<tr style='font-weight: bold; background-color: #f0f0f0;'>")
            html_parts.append("<td>TOTAL</td>")
            html_parts.append(f"<td style='text-align: right'>{backfill_percent:.1f}%</td>")
            html_parts.append(f"<td style='text-align: right'>{backfill_claimed:.1f}</td>")
            html_parts.append(f"<td style='text-align: right'>{backfill_total:.1f}</td>")
            html_parts.append("</tr>")

            # Individual backfill slot classes
            for class_name in backfill_slot_classes:
                if class_name in class_totals:
                    stats = class_totals[class_name]
                    html_parts.append("<tr>")
                    html_parts.append(f"<td>{get_display_name(class_name)}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['percent']:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['claimed']:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['total']:.1f}</td>")
                    html_parts.append("</tr>")
            html_parts.append("</table>")


    # Excluded hosts
    excluded_hosts = metadata.get('excluded_hosts', {})
    if excluded_hosts:
        html_parts.append("<h2>Excluded Hosts</h2>")
        html_parts.append("<table border='1'>")
        html_parts.append("<tr><th>Host</th><th>Reason</th></tr>")
        for host, reason in excluded_hosts.items():
            html_parts.append(f"<tr><td>{host}</td><td>{reason}</td></tr>")
        html_parts.append("</table>")

    # Filtering impact
    # filtered_info = metadata.get('filtered_hosts_info', [])
    # if filtered_info:
    #     total_original = sum(info['original_count'] for info in filtered_info)
    #     total_filtered = sum(info['filtered_count'] for info in filtered_info)
        #records_excluded = total_original - total_filtered
        # if records_excluded > 0:
        #     html_parts.append("<h2>Filtering Impact</h2>")
        #     html_parts.append("<table border='1'>")
        #     html_parts.append("<tr><th>Metric</th><th>Count</th></tr>")
        #     html_parts.append(f"<tr><td>Records excluded</td><td>{records_excluded:,}</td></tr>")
        #     html_parts.append(f"<tr><td>Records analyzed</td><td>{total_filtered:,}</td></tr>")
        #     html_parts.append("</table>")

    # Add methodology section from external file
    methodology_html = load_methodology()
    html_parts.append("<div style='background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin-top: 20px;'>")
    html_parts.append(methodology_html)
    html_parts.append("</div>")

    # Add time range information at the end
    html_parts.append("<div style='background-color: #f0f0f0; padding: 10px; margin-top: 20px; text-align: center; font-style: italic; color: #666;'>")
    if metadata.get("is_monthly", False):
        # For monthly reports, show the month
        monthly_period = metadata.get("monthly_period", "Unknown Period")
        html_parts.append(f"<strong>Data Period:</strong> {monthly_period}")
    else:
        # For regular reports, show start and end times
        start_time = metadata.get("start_time")
        end_time = metadata.get("end_time")
        if start_time and end_time:
            # Format timestamps nicely
            if hasattr(start_time, 'strftime'):
                start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            else:
                start_str = str(start_time)
            if hasattr(end_time, 'strftime'):
                end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
            else:
                end_str = str(end_time)
            html_parts.append(f"<strong>Data Period:</strong> {start_str} to {end_str}")
        else:
            # Fallback to hours_back if timestamps not available
            hours_back = metadata.get("hours_back", 24)
            html_parts.append(f"<strong>Data Period:</strong> Last {hours_back} hours")
    html_parts.append("</div>")

    html_parts.append("</body>")
    html_parts.append("</html>")

    html_content = "\n".join(html_parts)

    # Save to file if specified
    if output_file:
        try:
            with open(output_file, 'w') as f:
                f.write(html_content)
            print(f"HTML report saved to: {output_file}")
        except Exception as e:
            import sys
            print(f"Error saving HTML report to {output_file}: {e}", file=sys.stderr)
            # Fall back to stdout
            print(html_content)
            return html_content

    return html_content


def print_analysis_results(results: dict, output_format: str = "text", output_file: Optional[str] = None):
    """Print analysis results in a formatted way.

    Args:
        results: Analysis results dictionary
        output_format: Output format ('text' or 'html')
        output_file: Optional file path to save output
    """
    if output_format == "html":
        html_content = generate_html_report(results, output_file)
        if not output_file:
            print(html_content)
        return

    # Original text output
    if "error" in results:
        print(results["error"])
        return

    # Handle monthly summary - convert to regular results format and use existing text output
    if "monthly_stats" in results:
        monthly_stats = results["monthly_stats"]
        if "error" in monthly_stats:
            print(monthly_stats["error"])
            return

        # Convert monthly stats to regular results format so we can reuse existing text output
        regular_results = {
            "metadata": {
                "hours_back": monthly_stats["total_hours"],
                "start_time": monthly_stats.get("start_date"),
                "end_time": monthly_stats.get("end_date"),
                "num_intervals": monthly_stats["data_coverage"].get("unique_intervals", 0),
                "total_records": monthly_stats["data_coverage"].get("total_records", 0),
                "excluded_hosts": {},
                "filtered_hosts_info": {}
            }
        }

        # Copy the stats from monthly to regular format
        if "device_stats" in monthly_stats:
            regular_results["device_stats"] = monthly_stats["device_stats"]
        if "memory_stats" in monthly_stats:
            regular_results["memory_stats"] = monthly_stats["memory_stats"]
        if "h200_user_stats" in monthly_stats:
            regular_results["h200_user_stats"] = monthly_stats["h200_user_stats"]
        if "raw_data" in monthly_stats:
            regular_results["raw_data"] = monthly_stats["raw_data"]
        if "host_filter" in monthly_stats:
            regular_results["host_filter"] = monthly_stats["host_filter"]

        # Override results with converted monthly data and continue with normal text processing
        results = regular_results

        # Mark as monthly for different header formatting
        results["metadata"]["is_monthly"] = True
        results["metadata"]["monthly_period"] = monthly_stats['start_date'].strftime('%B %Y') if hasattr(monthly_stats['start_date'], 'strftime') else str(monthly_stats['month'])

    metadata = results["metadata"]

    # Print appropriate header based on type
    if metadata.get("is_monthly", False):
        print(f"\n{'='*70}")
        print(f"{'CHTC MONTHLY GPU REPORT - ' + metadata['monthly_period'].upper():^70}")
        print(f"{'='*70}")
        print(f"Period: {metadata['monthly_period']}")
        print(f"{'='*70}")
    else:
        print(f"\n{'='*70}")
        print(f"{'CHTC GPU UTILIZATION REPORT':^70}")
        print(f"{'='*70}")
        # Simplified period format for console: just the lookback hours
        hours_back = metadata.get('hours_back', 24)
        hours_str = str(int(hours_back)) if hours_back == int(hours_back) else str(hours_back)
        hour_word = "hour" if hours_back == 1 else "hours"
        period_str = f"{hours_str} {hour_word}"
        print(f"Period: {period_str}")
        print(f"{'='*70}")

    # Calculate cluster summary first if we have device stats
    grand_totals = {}
    if "device_stats" in results:
        device_stats = results["device_stats"]
        class_order = ["Priority-ResearcherOwned", "Priority-CHTCOwned", "Shared", "Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]

        for class_name in class_order:
            device_data = device_stats.get(class_name, {})
            if device_data:
                total_claimed = 0
                total_available = 0
                for device_type, stats in device_data.items():
                    total_claimed += stats['avg_claimed']
                    total_available += stats['avg_total_available']

                if total_available > 0:
                    grand_totals[class_name] = {
                        'claimed': total_claimed,
                        'total': total_available,
                        'percent': (total_claimed / total_available) * 100
                    }

    # Show cluster summary at the top with real slots and backfill slots separated
    if grand_totals:
        # Separate real slots from backfill slots
        real_slot_classes = ["Priority-ResearcherOwned", "Priority-CHTCOwned", "Shared"]
        backfill_slot_classes = ["Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]

        # Calculate totals for real slots
        real_claimed = sum(grand_totals[c]['claimed'] for c in real_slot_classes if c in grand_totals)
        real_total = sum(grand_totals[c]['total'] for c in real_slot_classes if c in grand_totals)
        real_percent = (real_claimed / real_total * 100) if real_total > 0 else 0

        # Calculate totals for backfill slots
        backfill_claimed = sum(grand_totals[c]['claimed'] for c in backfill_slot_classes if c in grand_totals)
        backfill_total = sum(grand_totals[c]['total'] for c in backfill_slot_classes if c in grand_totals)
        backfill_percent = (backfill_claimed / backfill_total * 100) if backfill_total > 0 else 0

        print(f"\nREAL SLOTS:")
        print(f"{'-'*70}")
        print(f"  TOTAL: {real_percent:.1f}% ({real_claimed:.1f}/{real_total:.1f} GPUs)")
        print(f"{'-'*70}")
        for class_name in real_slot_classes:
            if class_name in grand_totals:
                totals = grand_totals[class_name]
                print(f"  {get_display_name(class_name)}: {totals['percent']:.1f}% "
                      f"({totals['claimed']:.1f}/{totals['total']:.1f} GPUs)")

        # Real Slots by Memory Category
        if "memory_stats" in results:
            memory_stats = results["memory_stats"]
            if memory_stats:
                memory_total_claimed = sum(stats['avg_claimed'] for stats in memory_stats.values())
                memory_total_available = sum(stats['avg_total_available'] for stats in memory_stats.values())
                memory_total_percent = (memory_total_claimed / memory_total_available * 100) if memory_total_available > 0 else 0

                print(f"\nREAL SLOTS BY MEMORY CATEGORY:")
                print(f"{'-'*80}")
                print(f"  TOTAL: {memory_total_percent:.1f}% ({memory_total_claimed:.1f}/{memory_total_available:.1f} GPUs)")
                print(f"{'-'*80}")

                # Sort memory categories by numerical value
                def sort_memory_categories(categories):
                    def get_sort_key(cat):
                        if cat == "Unknown":
                            return 999  # Put Unknown at the end
                        elif cat.startswith("<"):
                            # Handle <48GB - extract the number after <
                            return float(cat[1:-2])  # Remove "<" and "GB"
                        elif cat.startswith(">"):
                            # Handle >80GB - extract the number after >
                            return float(cat[1:-2]) + 0.1  # Add 0.1 to sort after exact values
                        elif cat.endswith("GB+"):
                            return float(cat[:-3])  # Remove "GB+" suffix
                        elif cat.endswith("GB"):
                            if "-" in cat:
                                # Handle ranges like "10-12GB"
                                return float(cat.split("-")[0])
                            else:
                                return float(cat[:-2])  # Remove "GB" suffix
                        else:
                            return 0  # Fallback
                    return sorted(categories, key=get_sort_key)

                # Individual memory categories (sorted by memory size)
                sorted_memory_cats = sort_memory_categories(memory_stats.keys())
                for memory_cat in sorted_memory_cats:
                    stats = memory_stats[memory_cat]
                    print(f"  {memory_cat}: {stats['allocation_usage_percent']:.1f}% "
                          f"({stats['avg_claimed']:.1f}/{stats['avg_total_available']:.1f} GPUs)")

        print(f"\nBACKFILL SLOTS:")
        print(f"{'-'*70}")
        print(f"  TOTAL: {backfill_percent:.1f}% ({backfill_claimed:.1f}/{backfill_total:.1f} GPUs)")
        print(f"{'-'*70}")
        for class_name in backfill_slot_classes:
            if class_name in grand_totals:
                totals = grand_totals[class_name]
                print(f"  {get_display_name(class_name)}: {totals['percent']:.1f}% "
                      f"({totals['claimed']:.1f}/{totals['total']:.1f} GPUs)")

        # H200 Usage by Slot Type
        if "h200_user_stats" in results:
            h200_stats = results["h200_user_stats"]
            if h200_stats:
                print(f"\nH200 USAGE BY SLOT TYPE:")
                print(f"{'-'*80}")

                # Aggregate data by slot type (same logic as HTML)
                slot_type_totals = {}
                slot_type_users = {}

                for user, user_data in h200_stats.items():
                    for slot_type, slot_data in user_data['slot_breakdown'].items():
                        if slot_type not in slot_type_totals:
                            slot_type_totals[slot_type] = 0
                            slot_type_users[slot_type] = []

                        slot_type_totals[slot_type] += slot_data['gpu_hours']
                        slot_type_users[slot_type].append({
                            'user': user,
                            'gpu_hours': slot_data['gpu_hours']
                        })

                # Sort slot types by total GPU hours (descending)
                sorted_slot_types = sorted(slot_type_totals.items(), key=lambda x: x[1], reverse=True)
                total_gpu_hours = sum(slot_type_totals.values())

                # Unified format: slot type totals with user breakdown beneath
                for slot_type, total_hours in sorted_slot_types:
                    display_name = get_display_name(slot_type)
                    user_count = len(slot_type_users[slot_type])
                    percentage = (total_hours / total_gpu_hours * 100) if total_gpu_hours > 0 else 0

                    print(f"\n  {display_name} ({user_count} users): {total_hours:.1f} GPU-hours ({percentage:.1f}%)")
                    print(f"  {'-'*60}")

                    # User breakdown beneath the slot type total
                    users = slot_type_users[slot_type]
                    users.sort(key=lambda x: x['gpu_hours'], reverse=True)

                    for user_info in users:
                        user = user_info['user']
                        gpu_hours = user_info['gpu_hours']
                        user_total_percentage = (gpu_hours / total_gpu_hours * 100) if total_gpu_hours > 0 else 0
                        print(f"    {user}: {gpu_hours:.1f} hrs ({user_total_percentage:.1f}%)")

    if "allocation_stats" in results:
        print("\nAllocation Summary:")
        print(f"{'-'*70}")
        allocation_stats = results["allocation_stats"]

        # Order with hosted capacity emphasis (enhanced format is now default)
        class_order = ["Priority-ResearcherOwned", "Priority-CHTCOwned", "Shared", "Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]


        for class_name in class_order:
            if class_name in allocation_stats:
                stats = allocation_stats[class_name]
                print(f"  {get_display_name(class_name)}: {stats['allocation_usage_percent']:.1f}% "
                      f"({stats['avg_claimed']:.1f}/{stats['avg_total_available']:.1f} GPUs)")

    elif "device_stats" in results:
        print("\nUsage by Device Type:")
        print(f"{'-'*70}")

        # Use the pre-calculated grand_totals and device_stats
        class_order = ["Priority-ResearcherOwned", "Priority-CHTCOwned", "Shared", "Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]

        for class_name in class_order:
            device_data = device_stats.get(class_name, {})
            if device_data:  # Only show classes that have data
                print(f"\n{get_display_name(class_name)}:")
                print(f"{'-'*50}")

                for device_type, stats in sorted(device_data.items()):
                    short_name = get_human_readable_device_name(device_type)
                    print(f"    {short_name}: {stats['allocation_usage_percent']:.1f}% "
                          f"(avg {stats['avg_claimed']:.1f}/{stats['avg_total_available']:.1f} GPUs)")

                # Show class total using pre-calculated data
                if class_name in grand_totals:
                    totals = grand_totals[class_name]
                    print(f"    {'-'*30}")
                    print(f"    TOTAL {get_display_name(class_name)}: {totals['percent']:.1f}% "
                          f"(avg {totals['claimed']:.1f}/{totals['total']:.1f} GPUs)")



    elif "timeseries_data" in results:
        print("\nTime Series Analysis:")
        print(f"{'-'*70}")
        ts_df = results["timeseries_data"]

        # Calculate and display averages
        for class_name in ["priority", "shared", "backfill"]:
            usage_col = f"{class_name}_usage_percent"
            claimed_col = f"{class_name}_claimed"
            total_col = f"{class_name}_total"

            if all(col in ts_df.columns for col in [usage_col, claimed_col, total_col]):
                avg_usage = ts_df[usage_col].mean()
                avg_claimed = ts_df[claimed_col].mean()
                avg_total = ts_df[total_col].mean()
                print(f"  {class_name.title()}: {avg_usage:.1f}% "
                      f"({avg_claimed:.1f}/{avg_total:.1f} GPUs)")

        # Show recent trend
        print("\nRecent Trend:")
        print(f"{'-'*70}")
        recent_df = ts_df.tail(5)
        for _, row in recent_df.iterrows():
            print(f"  {row['timestamp'].strftime('%m-%d %H:%M')}: "
                  f"Priority {row['priority_usage_percent']:.1f}% "
                  f"({int(row['priority_claimed'])}/{int(row['priority_total'])}), "
                  f"Shared {row['shared_usage_percent']:.1f}% "
                  f"({int(row['shared_claimed'])}/{int(row['shared_total'])}), "
                  f"Backfill {row['backfill_usage_percent']:.1f}% "
                  f"({int(row['backfill_claimed'])}/{int(row['backfill_total'])})")

    # Show host exclusion information at the bottom
    excluded_hosts = metadata.get('excluded_hosts', {})
    if excluded_hosts:
        print(f"\n{'='*70}")
        print("EXCLUDED HOSTS:")
        for host, reason in excluded_hosts.items():
            print(f"  {host}: {reason}")

    # Show filtering impact at the bottom
    filtered_info = metadata.get('filtered_hosts_info', [])
    if filtered_info:
        total_original = sum(info['original_count'] for info in filtered_info)
        total_filtered = sum(info['filtered_count'] for info in filtered_info)
        records_excluded = total_original - total_filtered
        if records_excluded > 0:
            if not excluded_hosts:  # Only print separator if excluded hosts wasn't shown
                print(f"\n{'='*70}")
            print("FILTERING IMPACT:")
            print(f"  Records excluded: {records_excluded:,}")
            print(f"  Records analyzed: {total_filtered:,}")

    # Add time range information at the very end
    print(f"\n{'='*70}")
    if metadata.get("is_monthly", False):
        # For monthly reports, show the month
        monthly_period = metadata.get("monthly_period", "Unknown Period")
        print(f"Data Period: {monthly_period}")
    else:
        # For regular reports, show start and end times
        start_time = metadata.get("start_time")
        end_time = metadata.get("end_time")
        if start_time and end_time:
            # Format timestamps nicely
            if hasattr(start_time, 'strftime'):
                start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            else:
                start_str = str(start_time)
            if hasattr(end_time, 'strftime'):
                end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
            else:
                end_str = str(end_time)
            print(f"Data Period: {start_str} to {end_str}")
        else:
            # Fallback to hours_back if timestamps not available
            hours_back = metadata.get("hours_back", 24)
            print(f"Data Period: Last {hours_back} hours")
    print(f"{'='*70}")


def main(
    hours_back: int = typer.Option(24, help="Number of hours to analyze (default: 24)"),
    host: str = typer.Option("", help="Host name to filter results"),
    db_path: str = typer.Option("gpu_state_2025-08.db", help="Path to SQLite database"),
    analysis_type: str = typer.Option(
        "allocation",
        help="Type of analysis: allocation (% GPUs claimed), timeseries, gpu_model_snapshot, or monthly"
    ),
    bucket_minutes: int = typer.Option(15, help="Time bucket size in minutes for timeseries analysis"),
    end_time: Optional[str] = typer.Option(None, help="End time for analysis (YYYY-MM-DD HH:MM:SS), defaults to latest in DB"),
    group_by_device: bool = typer.Option(True, help="Group results by GPU device type"),
    all_devices: bool = typer.Option(False, help="Include all device types (if False, filters out older GPUs)"),
    gpu_model: Optional[str] = typer.Option(None, help="GPU model for snapshot analysis (e.g., 'NVIDIA A100-SXM4-80GB')"),
    snapshot_time: Optional[str] = typer.Option(None, help="Specific time for GPU model snapshot (YYYY-MM-DD HH:MM:SS)"),
    window_minutes: int = typer.Option(5, help="Time window in minutes for snapshot search"),
    exclude_hosts: Optional[str] = typer.Option(None, help="JSON string of hosts to exclude from analysis with reasons, e.g., '{\"host1\": \"misconfigured\", \"host2\": \"maintenance\"}'"),
    exclude_hosts_yaml: Optional[str] = typer.Option("masked_hosts.yaml", help="Path to YAML file containing host exclusions in format: hostname1: reason1"),
    output_format: str = typer.Option("text", help="Output format: 'text' or 'html'"),
    output_file: Optional[str] = typer.Option(None, help="Output file path (optional)"),
    email_to: Optional[str] = typer.Option(None, help="Email address(es) to send HTML report to (comma-separated)"),
    email_from: str = typer.Option("iaross@wisc.edu", help="Sender email address"),
    smtp_server: str = typer.Option("smtp.wiscmail.wisc.edu", help="SMTP server hostname"),
    smtp_port: int = typer.Option(25, help="SMTP server port (25 for standard SMTP, 587 for submission)"),
    email_timeout: int = typer.Option(30, help="SMTP connection timeout in seconds"),
    email_debug: bool = typer.Option(False, help="Enable SMTP debug output")
):
    """
    Calculate GPU usage statistics for Priority, Shared, and Backfill classes.

    This tool provides flexible analysis of GPU usage patterns over time.
    """
    # Validate host exclusion options
    if exclude_hosts and exclude_hosts_yaml and exclude_hosts_yaml != "masked_hosts.yaml":
        print("Error: Cannot use both --exclude-hosts and --exclude-hosts-yaml. Choose one.")
        return

    # If both exclude_hosts is provided and we're using the default yaml file,
    # prioritize the explicit exclude_hosts option
    if exclude_hosts and exclude_hosts_yaml == "masked_hosts.yaml":
        exclude_hosts_yaml = None

    # Parse end_time if provided
    parsed_end_time = None
    if end_time:
        try:
            parsed_end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            print("Error: Invalid end_time format. Use YYYY-MM-DD HH:MM:SS")
            return

    # Handle GPU model snapshot analysis
    if analysis_type == "gpu_model_snapshot":
        if not snapshot_time:
            print("Error: --snapshot-time is required for gpu_model_snapshot analysis")
            return

        # Parse snapshot_time
        try:
            parsed_snapshot_time = datetime.datetime.strptime(snapshot_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            print("Error: Invalid snapshot_time format. Use YYYY-MM-DD HH:MM:SS")
            return

        if gpu_model:
            # Analyze specific GPU model
            analysis = analyze_gpu_model_at_time(db_path, gpu_model, parsed_snapshot_time, window_minutes)
            print_gpu_model_analysis(analysis)
        else:
            # Show all available GPU models at that time
            available_models = get_gpu_models_at_time(db_path, parsed_snapshot_time, window_minutes)
            if not available_models:
                print(f"No GPU models found around {parsed_snapshot_time.strftime('%Y-%m-%d %H:%M:%S')}")
                return

            print(f"\nAvailable GPU models at {parsed_snapshot_time.strftime('%Y-%m-%d %H:%M:%S')}:")
            print("="*60)
            for i, model in enumerate(available_models, 1):
                print(f"  {i}. {model}")

            print("\nTo analyze a specific model, use:")
            print(f"  --analysis-type gpu_model_snapshot --gpu-model \"<model_name>\" --snapshot-time \"{snapshot_time}\"")
        return

    # Run the standard analysis
    try:
        results = run_analysis(
            db_path=db_path,
            hours_back=hours_back,
            host=host,
            analysis_type=analysis_type,
            bucket_minutes=bucket_minutes,
            end_time=parsed_end_time,
            group_by_device=group_by_device,
            all_devices=all_devices,
            exclude_hosts=exclude_hosts,
            exclude_hosts_yaml=exclude_hosts_yaml
        )
    except ValueError as e:
        print(f"Error: {e}")
        return

    # Print results
    print_analysis_results(results, output_format, output_file)

    # Send email if requested
    if email_to:
        if output_format != "html":
            print("Warning: Email functionality requires HTML format. Generating HTML for email...")

        # Generate HTML content for email
        html_content = generate_html_report(results)

        # Extract usage percentages for email subject
        usage_percentages = {}
        if "device_stats" in results:
            device_stats = results["device_stats"]
            for class_name, device_data in device_stats.items():
                if device_data:
                    # Calculate total percentage for this class
                    total_claimed = sum(stats['avg_claimed'] for stats in device_data.values())
                    total_available = sum(stats['avg_total_available'] for stats in device_data.values())
                    if total_available > 0:
                        usage_percentages[class_name] = (total_claimed / total_available) * 100
        elif "allocation_stats" in results:
            allocation_stats = results["allocation_stats"]
            for class_name, stats in allocation_stats.items():
                usage_percentages[class_name] = stats['allocation_usage_percent']

        # Send email
        success = send_email_report(
            html_content=html_content,
            to_email=email_to,
            from_email=email_from,
            smtp_server=smtp_server,
            smtp_port=smtp_port,
            usage_percentages=usage_percentages,
            lookback_hours=hours_back,
            timeout=email_timeout,
            debug=email_debug,
            device_stats=results.get("device_stats"),
            analysis_type=analysis_type,
            month=results["metadata"]["monthly_period"] if "metadata" in results and "monthly_period" in results["metadata"] else None
        )

        if not success:
            print("Failed to send email")
            return


if __name__ == "__main__":
    typer.run(main)
