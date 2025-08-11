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

# Import shared utilities
from gpu_utils import (
    filter_df, filter_df_enhanced, count_backfill, count_shared, count_prioritized,
    count_backfill_researcher_owned, count_backfill_hosted_capacity, count_glidein,
    load_host_exclusions, get_display_name, get_required_databases,
    get_latest_timestamp_from_most_recent_db, get_machines_by_category,
    HOST_EXCLUSIONS, FILTERED_HOSTS_INFO
)
import gpu_utils


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
        # Single month - use traditional approach for backward compatibility and performance
        try:
            conn = sqlite3.connect(db_path)
            df = pd.read_sql_query("SELECT * FROM gpu_state", conn)
            conn.close()
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Filter by time range
            filtered_df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
            return filtered_df
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
        "Priority-HostedCapacity", 
        "Shared", 
        "Backfill-HostedCapacity", 
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
            elif utilization_type == "Priority-HostedCapacity":
                claimed_gpus = len(filter_df_enhanced(bucket_df, "Priority-HostedCapacity", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df_enhanced(bucket_df, "Priority-HostedCapacity", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Shared":
                claimed_gpus = len(filter_df_enhanced(bucket_df, "Shared", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df_enhanced(bucket_df, "Shared", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Backfill-HostedCapacity":
                claimed_gpus = len(filter_df_enhanced(bucket_df, "Backfill-HostedCapacity", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df_enhanced(bucket_df, "Backfill-HostedCapacity", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
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
        "Priority-HostedCapacity", 
        "Shared", 
        "Backfill-HostedCapacity", 
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
                elif utilization_type == "Priority-HostedCapacity":
                    all_gpus_df = filter_df_enhanced(device_df, "Priority-HostedCapacity", "", host)
                elif utilization_type == "Shared":
                    all_gpus_df = filter_df_enhanced(device_df, "Shared", "", host)
                elif utilization_type == "Backfill-HostedCapacity":
                    all_gpus_df = filter_df_enhanced(device_df, "Backfill-HostedCapacity", "", host)
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

                # Calculate average GPU counts across intervals
                num_intervals_with_data = len(interval_usage_percentages)
                avg_claimed = total_claimed_gpus / num_intervals_with_data if num_intervals_with_data > 0 else 0
                avg_total = total_available_gpus / num_intervals_with_data if num_intervals_with_data > 0 else 0

                stats[utilization_type][device_type] = {
                    'avg_claimed': avg_claimed,
                    'avg_total_available': avg_total,
                    'allocation_usage_percent': avg_usage_percentage,
                    'num_intervals': num_intervals_with_data
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

                # Calculate average GPU counts across intervals
                num_intervals_with_data = len(interval_usage_percentages)
                avg_claimed = total_claimed_gpus / num_intervals_with_data if num_intervals_with_data > 0 else 0
                avg_total = total_available_gpus / num_intervals_with_data if num_intervals_with_data > 0 else 0

                stats[utilization_type][device_type] = {
                    'avg_claimed': avg_claimed,
                    'avg_total_available': avg_total,
                    'allocation_usage_percent': avg_usage_percentage,
                    'num_intervals': num_intervals_with_data
                }

    return stats


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
            "excluded_hosts": gpu_utils.HOST_EXCLUSIONS,
            "filtered_hosts_info": gpu_utils.FILTERED_HOSTS_INFO
        }
    }

    if analysis_type == "allocation":
        if group_by_device:
            result["device_stats"] = calculate_allocation_usage_by_device_enhanced(df, host, all_devices)
            result["raw_data"] = df  # Pass raw data for unique cluster totals calculation
            result["host_filter"] = host  # Pass host filter for consistency
        else:
            result["allocation_stats"] = calculate_allocation_usage_enhanced(df, host)

    elif analysis_type == "timeseries":
        result["timeseries_data"] = calculate_time_series_usage(df, bucket_minutes, host)

    return result


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

    print("\nBY UTILIZATION CLASS:")
    print(f"{'-'*40}")
    for class_name, stats in by_class.items():
        if stats['total'] > 0:
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
    debug: bool = False
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
            subject += f" {period_str}"

        # Add usage percentages in order: Open Capacity, Prioritized Service, Backfill
        if usage_percentages:
            class_order = ["Shared", "Priority", "Backfill"]  # Internal names
            usage_parts = []
            for class_name in class_order:
                if class_name in usage_percentages:
                    percentage = usage_percentages[class_name]
                    usage_parts.append(f"{percentage:.1f}%")

            if usage_parts:
                subject += f" ({' | '.join(usage_parts)})"

        msg['Subject'] = subject
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
    html_parts.append(f"<p><strong>Period:</strong> {metadata['start_time'].strftime('%Y-%m-%d %H:%M')} to {metadata['end_time'].strftime('%Y-%m-%d %H:%M')} ({metadata['num_intervals']} intervals)</p>")
    html_parts.append(f"<p><strong>Generated:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>")

    if "allocation_stats" in results:
        html_parts.append("<h2>Allocation Summary</h2>")
        html_parts.append("<table border='1'>")
        html_parts.append("<tr><th>Class</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>")

        allocation_stats = results["allocation_stats"]
        class_order = ["Priority-ResearcherOwned", "Priority-HostedCapacity", "Shared", "Backfill-ResearcherOwned", "Backfill-HostedCapacity", "Backfill-OpenCapacity"]
        
        for class_name in class_order:
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
        html_parts.append("<h2>Usage by Device Type</h2>")

        device_stats = results["device_stats"]
        class_totals = {}

        # Define the order with hosted capacity emphasis
        class_order = ["Priority-ResearcherOwned", "Priority-HostedCapacity", "Shared", "Backfill-ResearcherOwned", "Backfill-HostedCapacity", "Backfill-OpenCapacity"]

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
                    html_parts.append("<tr>")
                    html_parts.append(f"<td>{device_type}</td>")
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

        # Cluster summary for enhanced view (same logic as original but different totals)
        if class_totals:
            html_parts.append("<h2>Cluster Summary</h2>")
            html_parts.append("<table border='1' style='margin-top: 20px;'>")
            html_parts.append("<tr style='background-color: #e0e0e0;'><th>Class</th><th>Total Allocated %</th><th>Total Allocated (avg.)</th><th>Total Available (avg.)</th></tr>")

            for class_name in class_order:
                if class_name in class_totals:
                    totals = class_totals[class_name]
                    html_parts.append("<tr>")
                    html_parts.append(f"<td style='font-weight: bold;'>{get_display_name(class_name)}</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['percent']:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['claimed']:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right; font-weight: bold;'>{totals['total']:.1f}</td>")
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
                    html_parts.append("<tr>")
                    html_parts.append(f"<td>{device_type}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['allocation_usage_percent']:.1f}%</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['avg_claimed']:.1f}</td>")
                    html_parts.append(f"<td style='text-align: right'>{stats['avg_total_available']:.1f}</td>")
                    html_parts.append("</tr>")

                html_parts.append("</table>")

        # Cluster summary
        if class_totals:
            html_parts.append("<h2>Cluster Summary</h2>")
            html_parts.append("<table border='1'>")
            html_parts.append("<tr><th>Class</th><th>Allocated %</th><th>Allocated (avg.)</th><th>Available (avg.)</th></tr>")

            # Calculate unique totals to avoid double-counting GPUs across categories
            if "raw_data" in results and "host_filter" in results:
                # Use raw data to calculate unique totals
                unique_totals = calculate_unique_cluster_totals_from_raw_data(
                    results["raw_data"],
                    results["host_filter"]
                )
                overall_claimed = unique_totals['claimed']
                overall_total = unique_totals['total']
            else:
                # Fallback to simple summation if raw data not available
                overall_claimed = sum(stats['claimed'] for stats in class_totals.values())
                overall_total = sum(stats['total'] for stats in class_totals.values())

            overall_percent = (overall_claimed / overall_total * 100) if overall_total > 0 else 0

            # Add TOTAL row first
            html_parts.append("<tr style='font-weight: bold; background-color: #f0f0f0;'>")
            html_parts.append("<td>TOTAL</td>")
            html_parts.append(f"<td style='text-align: right'>{overall_percent:.1f}%</td>")
            html_parts.append(f"<td style='text-align: right'>{overall_claimed:.1f}</td>")
            html_parts.append(f"<td style='text-align: right'>{overall_total:.1f}</td>")
            html_parts.append("</tr>")

            # Add individual class rows in the same order
            for class_name in class_order:
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

    metadata = results["metadata"]

    print(f"\n{'='*70}")
    print(f"{'CHTC GPU UTILIZATION REPORT':^70}")
    print(f"{'='*70}")
    print(f"Period: {metadata['start_time'].strftime('%Y-%m-%d %H:%M')} to {metadata['end_time'].strftime('%Y-%m-%d %H:%M')} ({metadata['num_intervals']} intervals)")
    print(f"{'='*70}")

    if "allocation_stats" in results:
        print("\nAllocation Summary:")
        print(f"{'-'*70}")
        allocation_stats = results["allocation_stats"]
        
        # Order with hosted capacity emphasis (enhanced format is now default)
        class_order = ["Priority-ResearcherOwned", "Priority-HostedCapacity", "Shared", "Backfill-ResearcherOwned", "Backfill-HostedCapacity", "Backfill-OpenCapacity"]

        
        for class_name in class_order:
            if class_name in allocation_stats:
                stats = allocation_stats[class_name]
                print(f"  {get_display_name(class_name)}: {stats['allocation_usage_percent']:.1f}% "
                      f"({stats['avg_claimed']:.1f}/{stats['avg_total_available']:.1f} GPUs)")

    elif "device_stats" in results:
        print("\nUsage by Device Type:")
        print(f"{'-'*70}")
        device_stats = results["device_stats"]

        # Calculate and display grand totals
        grand_totals = {}

        # Define the order with hosted capacity emphasis
        class_order = ["Priority-ResearcherOwned", "Priority-HostedCapacity", "Shared", "Backfill-ResearcherOwned", "Backfill-HostedCapacity", "Backfill-OpenCapacity"]

        for class_name in class_order:
            device_data = device_stats.get(class_name, {})
            if device_data:  # Only show classes that have data
                print(f"\n{get_display_name(class_name)}:")
                print(f"{'-'*50}")

                # Calculate totals for this class
                total_claimed = 0
                total_available = 0

                for device_type, stats in sorted(device_data.items()):
                    print(f"    {device_type}: {stats['allocation_usage_percent']:.1f}% "
                          f"(avg {stats['avg_claimed']:.1f}/{stats['avg_total_available']:.1f} GPUs)")
                    total_claimed += stats['avg_claimed']
                    total_available += stats['avg_total_available']

                # Calculate and store grand total for this class
                if total_available > 0:
                    grand_total_percent = (total_claimed / total_available) * 100
                    grand_totals[class_name] = {
                        'claimed': total_claimed,
                        'total': total_available,
                        'percent': grand_total_percent
                    }

                    print(f"    {'-'*30}")
                    print(f"    TOTAL {get_display_name(class_name)}: {grand_total_percent:.1f}% "
                          f"(avg {total_claimed:.1f}/{total_available:.1f} GPUs)")

        # Cluster summary
        if grand_totals:
            print(f"\n{'='*70}")
            print("Cluster Summary:")
            print(f"{'-'*70}")
            for class_name in class_order:
                if class_name in grand_totals:
                    totals = grand_totals[class_name]
                    print(f"  {get_display_name(class_name)}: {totals['percent']:.1f}% "
                          f"({totals['claimed']:.1f}/{totals['total']:.1f} GPUs)")
        

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


def main(
    hours_back: int = typer.Option(24, help="Number of hours to analyze (default: 24)"),
    host: str = typer.Option("", help="Host name to filter results"),
    db_path: str = typer.Option("gpu_state_2025-08.db", help="Path to SQLite database"),
    analysis_type: str = typer.Option(
        "allocation",
        help="Type of analysis: allocation (% GPUs claimed), timeseries, or gpu_model_snapshot"
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
            debug=email_debug
        )

        if not success:
            print("Failed to send email")
            return


if __name__ == "__main__":
    typer.run(main)
