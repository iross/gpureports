#!/usr/bin/env python3
"""
GPU Usage Statistics - Calculation Functions

All calculate_* functions and GPU model analysis functions for computing
allocation usage, performance metrics, time series data, and device breakdowns.
"""

import datetime
import sqlite3

import pandas as pd

import gpu_utils
from device_name_mappings import get_memory_category_from_mb
from gpu_utils import (
    BACKFILL_SLOT_TYPES,
    CLASS_ORDER,
    UTILIZATION_TYPES,
    filter_df,
    filter_df_enhanced,
    get_latest_timestamp_from_most_recent_db,
)
from stats_data import (
    get_cached_filtered_dataframe,
    get_preprocessed_dataframe,
    get_time_filtered_data,
)


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
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["15min_bucket"] = df["timestamp"].dt.floor("15min")

    stats = {}

    for utilization_type in UTILIZATION_TYPES:
        interval_usage_percentages = []
        total_claimed_gpus = 0
        total_available_gpus = 0

        # For each 15-minute interval, count unique GPUs
        for bucket in sorted(df["15min_bucket"].unique()):
            bucket_df = df[df["15min_bucket"] == bucket]

            # Count unique GPUs for this utilization type in this interval
            if utilization_type == "Priority":
                claimed_gpus = len(filter_df(bucket_df, "Priority", "Claimed", host)["AssignedGPUs"].dropna().unique())
                unclaimed_gpus = len(
                    filter_df(bucket_df, "Priority", "Unclaimed", host)["AssignedGPUs"].dropna().unique()
                )
            elif utilization_type == "Shared":
                claimed_gpus = len(filter_df(bucket_df, "Shared", "Claimed", host)["AssignedGPUs"].dropna().unique())
                unclaimed_gpus = len(
                    filter_df(bucket_df, "Shared", "Unclaimed", host)["AssignedGPUs"].dropna().unique()
                )
            elif utilization_type == "Backfill":
                claimed_gpus = len(filter_df(bucket_df, "Backfill", "Claimed", host)["AssignedGPUs"].dropna().unique())
                unclaimed_gpus = len(
                    filter_df(bucket_df, "Backfill", "Unclaimed", host)["AssignedGPUs"].dropna().unique()
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
        num_intervals = len(df["15min_bucket"].unique())
        avg_claimed = total_claimed_gpus / num_intervals if num_intervals > 0 else 0
        avg_total = total_available_gpus / num_intervals if num_intervals > 0 else 0

        stats[utilization_type] = {
            "avg_claimed": avg_claimed,
            "avg_total_available": avg_total,
            "allocation_usage_percent": avg_usage_percentage,
            "num_intervals": num_intervals,
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
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["15min_bucket"] = df["timestamp"].dt.floor("15min")

    stats = {}

    # Utilization types with emphasis on hosted capacity
    utilization_types = [
        "Priority-ResearcherOwned",
        "Priority-CHTCOwned",
        "Shared",
        "Backfill-CHTCOwned",
        "Backfill-ResearcherOwned",
        "Backfill-OpenCapacity",
    ]

    for utilization_type in utilization_types:
        interval_usage_percentages = []
        total_claimed_gpus = 0
        total_available_gpus = 0

        # For each 15-minute interval, count unique GPUs
        for bucket in sorted(df["15min_bucket"].unique()):
            bucket_df = df[df["15min_bucket"] == bucket]

            # Count unique GPUs for this utilization type in this interval
            if utilization_type == "Priority-ResearcherOwned":
                claimed_gpus = len(
                    filter_df_enhanced(bucket_df, "Priority-ResearcherOwned", "Claimed", host)["AssignedGPUs"]
                    .dropna()
                    .unique()
                )
                unclaimed_gpus = len(
                    filter_df_enhanced(bucket_df, "Priority-ResearcherOwned", "Unclaimed", host)["AssignedGPUs"]
                    .dropna()
                    .unique()
                )
            elif utilization_type == "Priority-CHTCOwned":
                claimed_gpus = len(
                    filter_df_enhanced(bucket_df, "Priority-CHTCOwned", "Claimed", host)["AssignedGPUs"]
                    .dropna()
                    .unique()
                )
                unclaimed_gpus = len(
                    filter_df_enhanced(bucket_df, "Priority-CHTCOwned", "Unclaimed", host)["AssignedGPUs"]
                    .dropna()
                    .unique()
                )
            elif utilization_type == "Shared":
                claimed_gpus = len(
                    filter_df_enhanced(bucket_df, "Shared", "Claimed", host)["AssignedGPUs"].dropna().unique()
                )
                unclaimed_gpus = len(
                    filter_df_enhanced(bucket_df, "Shared", "Unclaimed", host)["AssignedGPUs"].dropna().unique()
                )
            elif utilization_type == "Backfill-CHTCOwned":
                claimed_gpus = len(
                    filter_df_enhanced(bucket_df, "Backfill-CHTCOwned", "Claimed", host)["AssignedGPUs"]
                    .dropna()
                    .unique()
                )
                unclaimed_gpus = len(
                    filter_df_enhanced(bucket_df, "Backfill-CHTCOwned", "Unclaimed", host)["AssignedGPUs"]
                    .dropna()
                    .unique()
                )
            elif utilization_type == "Backfill-ResearcherOwned":
                claimed_gpus = len(
                    filter_df_enhanced(bucket_df, "Backfill-ResearcherOwned", "Claimed", host)["AssignedGPUs"]
                    .dropna()
                    .unique()
                )
                unclaimed_gpus = len(
                    filter_df_enhanced(bucket_df, "Backfill-ResearcherOwned", "Unclaimed", host)["AssignedGPUs"]
                    .dropna()
                    .unique()
                )
            elif utilization_type == "Backfill-OpenCapacity":
                claimed_gpus = len(
                    filter_df_enhanced(bucket_df, "Backfill-OpenCapacity", "Claimed", host)["AssignedGPUs"]
                    .dropna()
                    .unique()
                )
                unclaimed_gpus = len(
                    filter_df_enhanced(bucket_df, "Backfill-OpenCapacity", "Unclaimed", host)["AssignedGPUs"]
                    .dropna()
                    .unique()
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
        num_intervals = len(df["15min_bucket"].unique())
        avg_claimed = total_claimed_gpus / num_intervals if num_intervals > 0 else 0
        avg_total = total_available_gpus / num_intervals if num_intervals > 0 else 0

        stats[utilization_type] = {
            "avg_claimed": avg_claimed,
            "avg_total_available": avg_total,
            "allocation_usage_percent": avg_usage_percentage,
            "num_intervals": num_intervals,
        }

    return stats


def calculate_allocation_usage_by_device_enhanced(
    df: pd.DataFrame, host: str = "", include_all_devices: bool = True
) -> dict:
    """
    Calculate allocation-based usage grouped by device type with enhanced backfill categories.

    Args:
        df: DataFrame with GPU state data
        host: Optional host filter
        include_all_devices: Whether to include all device types or filter out older ones

    Returns:
        Dictionary with usage statistics for each enhanced class and device type
    """
    # Use cached preprocessing to avoid repeated timestamp/bucket operations
    # Generate unified cache key based on DataFrame identity
    cache_key = f"preprocessed_{len(df)}_{hash(str(df['timestamp'].iloc[0])) if len(df) > 0 else 'empty'}"
    df = get_preprocessed_dataframe(df, cache_key)

    # Get unique device types
    device_types = df["GPUs_DeviceName"].dropna().unique()

    stats = {}

    # Utilization types with emphasis on hosted capacity
    utilization_types = [
        "Priority-ResearcherOwned",
        "Priority-CHTCOwned",
        "Shared",
        "Backfill-CHTCOwned",
        "Backfill-ResearcherOwned",
        "Backfill-OpenCapacity",
    ]

    # Pre-filter data by utilization type and device type to avoid repeated filtering
    filtered_data = {}
    for utilization_type in utilization_types:
        filtered_data[utilization_type] = {}
        for device_type in device_types:
            # Skip old/uncommon GPU types for cleaner output (unless requested to include all)
            if not include_all_devices and any(
                old_gpu in device_type for old_gpu in ["GTX 1080", "P100", "Quadro", "A30", "A40"]
            ):
                continue

            # Create cache key for this specific filter combination - include all parameters
            filter_cache_key = f"enhanced_{utilization_type}_{device_type}_{host}_{len(df)}_{hash(str(df['timestamp'].iloc[0])) if len(df) > 0 else 'empty'}"

            # Get filtered dataset (cached if available) - correct parameter order
            # filter_df_enhanced(df, utilization, state, host)
            # We need to filter by device type separately since filter_df_enhanced doesn't take device_type
            device_df = df[df["GPUs_DeviceName"] == device_type]
            filtered_df = get_cached_filtered_dataframe(
                device_df, filter_df_enhanced, (utilization_type, "", host), filter_cache_key
            )
            filtered_data[utilization_type][device_type] = filtered_df

    for utilization_type in utilization_types:
        stats[utilization_type] = {}

        for device_type in device_types:
            # Skip old/uncommon GPU types for cleaner output (unless requested to include all)
            if not include_all_devices and any(
                old_gpu in device_type for old_gpu in ["GTX 1080", "P100", "Quadro", "A30", "A40"]
            ):
                continue

            # Use pre-filtered data instead of calling filter_df_enhanced repeatedly
            if device_type not in filtered_data[utilization_type]:
                continue
            device_utilization_df = filtered_data[utilization_type][device_type]

            interval_usage_percentages = []
            interval_drained_percentages = []
            total_claimed_gpus = 0
            total_drained_gpus = 0
            total_available_gpus = 0

            # For each 15-minute interval, count unique GPUs using pre-filtered data
            for bucket in sorted(df["15min_bucket"].unique()):
                # Use pre-filtered data for this bucket
                bucket_filtered_df = device_utilization_df[device_utilization_df["15min_bucket"] == bucket]

                if bucket_filtered_df.empty:
                    continue

                # Count unique GPUs (total available for this utilization type)
                unique_gpu_ids = set(bucket_filtered_df["AssignedGPUs"].dropna().unique())
                total_gpus_this_interval = len(unique_gpu_ids)

                # Count how many of these unique GPUs are currently claimed
                claimed_gpus_df = bucket_filtered_df[bucket_filtered_df["State"] == "Claimed"]
                claimed_unique_gpu_ids = set(claimed_gpus_df["AssignedGPUs"].dropna().unique())
                claimed_gpus = len(claimed_unique_gpu_ids)

                # Count how many of these unique GPUs are currently drained
                # EXCLUDE GPUs that are already claimed (prefer Claimed over Drained)
                drained_gpus_df = bucket_filtered_df[bucket_filtered_df["State"] == "Drained"]
                drained_unique_gpu_ids = set(drained_gpus_df["AssignedGPUs"].dropna().unique())
                drained_unique_gpu_ids -= claimed_unique_gpu_ids  # Remove GPUs already counted as claimed
                drained_gpus = len(drained_unique_gpu_ids)

                if total_gpus_this_interval > 0:
                    interval_usage = (claimed_gpus / total_gpus_this_interval) * 100
                    interval_usage_percentages.append(interval_usage)

                    interval_drained = (drained_gpus / total_gpus_this_interval) * 100
                    interval_drained_percentages.append(interval_drained)

                    total_claimed_gpus += claimed_gpus
                    total_drained_gpus += drained_gpus
                    total_available_gpus += total_gpus_this_interval

            if interval_usage_percentages:
                # Average percentages across intervals (correct approach when GPUs can change state)
                # Averaging counts then calculating % is wrong because the same GPU counted as
                # Claimed in one interval and Drained in another adds to both totals
                avg_usage_percentage = sum(interval_usage_percentages) / len(interval_usage_percentages)
                avg_drained_percentage = (
                    sum(interval_drained_percentages) / len(interval_drained_percentages)
                    if interval_drained_percentages
                    else 0.0
                )

                # Calculate average GPU counts across ALL intervals (including those with 0 usage)
                # This matches the user breakdown method and gives consistent results
                total_intervals = len(df["15min_bucket"].unique())
                avg_claimed = total_claimed_gpus / total_intervals if total_intervals > 0 else 0
                avg_drained = total_drained_gpus / total_intervals if total_intervals > 0 else 0
                avg_total = total_available_gpus / total_intervals if total_intervals > 0 else 0

                stats[utilization_type][device_type] = {
                    "avg_claimed": avg_claimed,
                    "avg_drained": avg_drained,
                    "avg_total_available": avg_total,
                    "allocation_usage_percent": avg_usage_percentage,
                    "drained_percent": avg_drained_percentage,
                    "num_intervals": total_intervals,
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

    for utilization_type in UTILIZATION_TYPES:
        # Filter to only claimed GPUs with utilization data
        filtered_df = filter_df(df, utilization_type, "Claimed", host)

        # Only consider records with valid utilization data
        util_df = filtered_df[(filtered_df["GPUsAverageUsage"].notna()) & (filtered_df["GPUsAverageUsage"] >= 0)]

        if len(util_df) > 0:
            avg_utilization = util_df["GPUsAverageUsage"].mean() * 100  # Convert to percentage
            total_records = len(util_df)
            unique_gpus = util_df["AssignedGPUs"].nunique()
        else:
            avg_utilization = 0
            total_records = 0
            unique_gpus = 0

        stats[utilization_type] = {
            "avg_gpu_utilization_percent": avg_utilization,
            "records_with_utilization": total_records,
            "unique_gpus_used": unique_gpus,
        }

    return stats


def calculate_time_series_usage(df: pd.DataFrame, bucket_minutes: int = 15, host: str = "") -> pd.DataFrame:
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
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df[f"{bucket_minutes}min_bucket"] = df["timestamp"].dt.floor(f"{bucket_minutes}min")

    time_series_data = []

    for bucket in sorted(df[f"{bucket_minutes}min_bucket"].unique()):
        bucket_df = df[df[f"{bucket_minutes}min_bucket"] == bucket]
        bucket_stats = {"timestamp": bucket}

        for utilization_type in UTILIZATION_TYPES:
            # Count unique GPUs for this utilization type in this interval
            if utilization_type == "Priority":
                claimed_gpus = len(filter_df(bucket_df, "Priority", "Claimed", host)["AssignedGPUs"].dropna().unique())
                unclaimed_gpus = len(
                    filter_df(bucket_df, "Priority", "Unclaimed", host)["AssignedGPUs"].dropna().unique()
                )
            elif utilization_type == "Shared":
                claimed_gpus = len(filter_df(bucket_df, "Shared", "Claimed", host)["AssignedGPUs"].dropna().unique())
                unclaimed_gpus = len(
                    filter_df(bucket_df, "Shared", "Unclaimed", host)["AssignedGPUs"].dropna().unique()
                )
            elif utilization_type == "Backfill":
                claimed_gpus = len(filter_df(bucket_df, "Backfill", "Claimed", host)["AssignedGPUs"].dropna().unique())
                unclaimed_gpus = len(
                    filter_df(bucket_df, "Backfill", "Unclaimed", host)["AssignedGPUs"].dropna().unique()
                )

            total_gpus = claimed_gpus + unclaimed_gpus
            usage_percent = (claimed_gpus / total_gpus * 100) if total_gpus > 0 else 0

            bucket_stats[f"{utilization_type.lower()}_claimed"] = claimed_gpus
            bucket_stats[f"{utilization_type.lower()}_total"] = total_gpus
            bucket_stats[f"{utilization_type.lower()}_usage_percent"] = usage_percent

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
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["15min_bucket"] = df["timestamp"].dt.floor("15min")

    # Get unique device types
    device_types = df["GPUs_DeviceName"].dropna().unique()

    stats = {}

    for utilization_type in UTILIZATION_TYPES:
        stats[utilization_type] = {}

        for device_type in device_types:
            # Skip old/uncommon GPU types for cleaner output (unless requested to include all)
            if not include_all_devices and any(
                old_gpu in device_type for old_gpu in ["GTX 1080", "P100", "Quadro", "A30", "A40"]
            ):
                continue

            interval_usage_percentages = []
            interval_drained_percentages = []
            total_claimed_gpus = 0
            total_drained_gpus = 0
            total_available_gpus = 0

            # For each 15-minute interval, count unique GPUs of this device type
            for bucket in sorted(df["15min_bucket"].unique()):
                bucket_df = df[df["15min_bucket"] == bucket]

                # Filter by device type
                device_df = bucket_df[bucket_df["GPUs_DeviceName"] == device_type]

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
                unique_gpu_ids = set(all_gpus_df["AssignedGPUs"].dropna().unique())
                total_gpus_this_interval = len(unique_gpu_ids)

                # Count how many of these unique GPUs are currently claimed
                claimed_gpus_df = all_gpus_df[all_gpus_df["State"] == "Claimed"]
                claimed_unique_gpu_ids = set(claimed_gpus_df["AssignedGPUs"].dropna().unique())
                claimed_gpus = len(claimed_unique_gpu_ids)

                # Count how many of these unique GPUs are currently drained
                drained_gpus_df = all_gpus_df[all_gpus_df["State"] == "Drained"]
                drained_unique_gpu_ids = set(drained_gpus_df["AssignedGPUs"].dropna().unique())
                drained_gpus = len(drained_unique_gpu_ids)

                if total_gpus_this_interval > 0:
                    interval_usage = (claimed_gpus / total_gpus_this_interval) * 100
                    interval_usage_percentages.append(interval_usage)

                    interval_drained = (drained_gpus / total_gpus_this_interval) * 100
                    interval_drained_percentages.append(interval_drained)

                    total_claimed_gpus += claimed_gpus
                    total_drained_gpus += drained_gpus
                    total_available_gpus += total_gpus_this_interval

            if interval_usage_percentages:
                # Average percentages across intervals (correct approach when GPUs can change state)
                # Averaging counts then calculating % is wrong because the same GPU counted as
                # Claimed in one interval and Drained in another adds to both totals
                avg_usage_percentage = sum(interval_usage_percentages) / len(interval_usage_percentages)
                avg_drained_percentage = (
                    sum(interval_drained_percentages) / len(interval_drained_percentages)
                    if interval_drained_percentages
                    else 0.0
                )

                # Calculate average GPU counts across ALL intervals (including those with 0 usage)
                # This matches the user breakdown method and gives consistent results
                total_intervals = len(df["15min_bucket"].unique())
                avg_claimed = total_claimed_gpus / total_intervals if total_intervals > 0 else 0
                avg_drained = total_drained_gpus / total_intervals if total_intervals > 0 else 0
                avg_total = total_available_gpus / total_intervals if total_intervals > 0 else 0

                stats[utilization_type][device_type] = {
                    "avg_claimed": avg_claimed,
                    "avg_drained": avg_drained,
                    "avg_total_available": avg_total,
                    "allocation_usage_percent": avg_usage_percentage,
                    "drained_percent": avg_drained_percentage,
                    "num_intervals": total_intervals,
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
    # Use cached preprocessing to avoid repeated timestamp/bucket operations
    # Generate unified cache key based on DataFrame identity
    cache_key = f"preprocessed_{len(df)}_{hash(str(df['timestamp'].iloc[0])) if len(df) > 0 else 'empty'}"
    df = get_preprocessed_dataframe(df, cache_key)

    # Add memory category column based on GPUs_GlobalMemoryMb
    df["memory_category"] = df["GPUs_GlobalMemoryMb"].apply(get_memory_category_from_mb)

    # Get unique memory categories
    memory_categories = df["memory_category"].dropna().unique()

    stats = {}

    # Only calculate for Real slot classes (Priority-ResearcherOwned + Priority-CHTCOwned + Shared)
    real_slot_classes = ["Priority-ResearcherOwned", "Priority-CHTCOwned", "Shared"]

    # Pre-filter data by class and memory category to avoid repeated filtering
    filtered_memory_data = {}
    for memory_cat in memory_categories:
        filtered_memory_data[memory_cat] = {}
        for class_name in real_slot_classes:
            # Create cache key for this specific filter combination
            filter_cache_key = f"memory_{class_name}_{memory_cat}_{len(df)}_{hash(str(df['timestamp'].iloc[0])) if len(df) > 0 else 'empty'}"

            # Filter by memory category first, then by class
            memory_cat_df = df[df["memory_category"] == memory_cat]
            if not memory_cat_df.empty:
                # Get filtered dataset (cached if available)
                # filter_df_enhanced(df, utilization, state, host)
                filtered_df = get_cached_filtered_dataframe(
                    memory_cat_df, filter_df_enhanced, (class_name, "", host), filter_cache_key
                )
                filtered_memory_data[memory_cat][class_name] = filtered_df

    for memory_cat in memory_categories:
        total_claimed_across_intervals = 0
        total_drained_across_intervals = 0
        total_available_across_intervals = 0
        num_intervals_with_data = 0
        interval_usage_percentages = []
        interval_drained_percentages = []

        # Get unique time buckets
        unique_buckets = df["15min_bucket"].unique()

        for bucket_time in unique_buckets:
            bucket_claimed_ids = set()
            bucket_drained_ids = set()
            bucket_total_ids = set()

            # Collect GPU IDs across all Real slot classes for this memory category
            # First pass: collect all claimed and total GPUs
            for class_name in real_slot_classes:
                # Use pre-filtered data for this memory category and class
                if class_name not in filtered_memory_data[memory_cat]:
                    continue

                class_filtered_df = filtered_memory_data[memory_cat][class_name]

                # Filter by bucket time
                bucket_class_df = class_filtered_df[class_filtered_df["15min_bucket"] == bucket_time]

                if not bucket_class_df.empty:
                    # Collect unique GPU IDs for this class
                    unique_gpu_ids = set(bucket_class_df["AssignedGPUs"].dropna().unique())
                    bucket_total_ids.update(unique_gpu_ids)

                    # Collect unique claimed GPUs
                    claimed_gpus_df = bucket_class_df[bucket_class_df["State"] == "Claimed"]
                    claimed_unique_gpu_ids = set(claimed_gpus_df["AssignedGPUs"].dropna().unique())
                    bucket_claimed_ids.update(claimed_unique_gpu_ids)

                    # Collect unique drained GPUs (will deduplicate later)
                    drained_gpus_df = bucket_class_df[bucket_class_df["State"] == "Drained"]
                    drained_unique_gpu_ids = set(drained_gpus_df["AssignedGPUs"].dropna().unique())
                    bucket_drained_ids.update(drained_unique_gpu_ids)

            # Deduplicate: remove GPUs counted as claimed from drained
            bucket_drained_ids -= bucket_claimed_ids

            bucket_claimed = len(bucket_claimed_ids)
            bucket_drained = len(bucket_drained_ids)
            bucket_total = len(bucket_total_ids)

            if bucket_total > 0:
                total_claimed_across_intervals += bucket_claimed
                total_drained_across_intervals += bucket_drained
                total_available_across_intervals += bucket_total
                num_intervals_with_data += 1

                # Track percentages for this interval
                interval_usage_pct = (bucket_claimed / bucket_total * 100) if bucket_total > 0 else 0
                interval_drained_pct = (bucket_drained / bucket_total * 100) if bucket_total > 0 else 0
                interval_usage_percentages.append(interval_usage_pct)
                interval_drained_percentages.append(interval_drained_pct)

        # Calculate averages
        if num_intervals_with_data > 0:
            # Average percentages across intervals (correct approach when GPUs can change state)
            avg_usage_percentage = sum(interval_usage_percentages) / len(interval_usage_percentages)
            avg_drained_percentage = (
                sum(interval_drained_percentages) / len(interval_drained_percentages)
                if interval_drained_percentages
                else 0.0
            )

            avg_claimed = total_claimed_across_intervals / num_intervals_with_data
            avg_drained = total_drained_across_intervals / num_intervals_with_data
            avg_total = total_available_across_intervals / num_intervals_with_data

            stats[memory_cat] = {
                "avg_claimed": avg_claimed,
                "avg_drained": avg_drained,
                "avg_total_available": avg_total,
                "allocation_usage_percent": avg_usage_percentage,
                "drained_percent": avg_drained_percentage,
                "num_intervals": num_intervals_with_data,
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
    h200_df = df[df["GPUs_DeviceName"] == "NVIDIA H200"].copy()

    if h200_df.empty:
        return {}

    # Use cached preprocessing for H200 data
    # Generate unified cache key based on DataFrame identity
    cache_key = (
        f"preprocessed_{len(h200_df)}_{hash(str(h200_df['timestamp'].iloc[0])) if len(h200_df) > 0 else 'empty'}"
    )
    h200_df = get_preprocessed_dataframe(h200_df, cache_key)

    # Apply host filter if specified
    if host:
        h200_df = h200_df[h200_df["Machine"].str.contains(host, case=False, na=False)]

    # Use the actual lookback period to match the device allocation method
    # (Device allocation uses averages across buckets multiplied by lookback period)
    actual_duration_hours = hours_back

    user_stats = {}
    slot_types = CLASS_ORDER

    # For each slot type, analyze user usage using averaging approach like device allocation
    for slot_type in slot_types:
        # Get slots of this type
        filtered_df = filter_df_enhanced(h200_df, slot_type, "", host)

        if filtered_df.empty:
            continue

        # Only look at claimed slots (where jobs are running)
        claimed_df = filtered_df[filtered_df["State"] == "Claimed"]

        if claimed_df.empty:
            continue

        # For each user, calculate their average GPU usage across buckets, then multiply by actual time
        user_bucket_totals = {}

        # Get all possible buckets for this slot type (from the entire H200 dataset)
        # This ensures we count all time intervals, including those where user has 0 GPUs
        all_buckets = sorted(h200_df["15min_bucket"].unique())
        num_buckets = len(all_buckets)

        for bucket in all_buckets:
            bucket_df = claimed_df[claimed_df["15min_bucket"] == bucket]

            if not bucket_df.empty:
                # Group by user within this time bucket
                user_gpu_counts = bucket_df.groupby("RemoteOwner")["AssignedGPUs"].nunique()

                for user, gpu_count in user_gpu_counts.items():
                    if pd.isna(user) or user == "" or user is None:
                        user = "Unknown"

                    if user not in user_bucket_totals:
                        user_bucket_totals[user] = 0

                    user_bucket_totals[user] += gpu_count

        # Convert totals to averages, then multiply by actual time duration
        for user, total_gpus in user_bucket_totals.items():
            avg_gpus = total_gpus / num_buckets if num_buckets > 0 else 0
            gpu_hours = avg_gpus * actual_duration_hours

            if user not in user_stats:
                user_stats[user] = {
                    "Priority-ResearcherOwned": 0,
                    "Priority-CHTCOwned": 0,
                    "Shared": 0,
                    "Backfill-ResearcherOwned": 0,
                    "Backfill-CHTCOwned": 0,
                    "Backfill-OpenCapacity": 0,
                }

            user_stats[user][slot_type] = gpu_hours

    # Calculate final statistics
    final_stats = {}
    for user, slot_data in user_stats.items():
        total_gpu_hours = sum(gpu_hours for gpu_hours in slot_data.values())

        if total_gpu_hours > 0:
            final_stats[user] = {"total_gpu_hours": total_gpu_hours, "slot_breakdown": {}}

            # Add breakdown by slot type (only include non-zero usage)
            for slot_type, gpu_hours in slot_data.items():
                if gpu_hours > 0:
                    final_stats[user]["slot_breakdown"][slot_type] = {
                        "gpu_hours": gpu_hours,
                        "percentage": (gpu_hours / total_gpu_hours) * 100,
                    }

    return final_stats


def calculate_backfill_usage_by_user(
    df: pd.DataFrame, host: str = "", hours_back: int = 1, include_all_devices: bool = False
) -> dict:
    """
    Calculate backfill slot usage breakdown by user and slot type.

    Args:
        df: DataFrame with GPU state data
        host: Optional host filter
        hours_back: Lookback period in hours

    Returns:
        Dictionary with backfill usage statistics by user and slot type
    """
    if df.empty:
        return {}

    # Filter out old/uncommon GPU types for consistency with allocation calculations
    if not include_all_devices:
        old_gpu_types = ["GTX 1080", "P100", "Quadro", "A30", "A40"]
        mask = ~df["GPUs_DeviceName"].str.contains("|".join(old_gpu_types), case=False, na=False)
        df = df[mask]

        if df.empty:
            return {}

    # Use cached preprocessing for data
    cache_key = f"preprocessed_backfill_{len(df)}_{hash(str(df['timestamp'].iloc[0])) if len(df) > 0 else 'empty'}_{include_all_devices}"
    df = get_preprocessed_dataframe(df, cache_key)

    # Apply host filter if specified
    if host:
        df = df[df["Machine"].str.contains(host, case=False, na=False)]

    actual_duration_hours = hours_back
    user_stats = {}

    # Only focus on backfill slot types
    backfill_slot_types = BACKFILL_SLOT_TYPES

    # For each backfill slot type, analyze user usage
    for slot_type in backfill_slot_types:
        # Get slots of this type
        filtered_df = filter_df_enhanced(df, slot_type, "", host)
        if filtered_df.empty:
            continue

        # Only look at claimed slots (where jobs are running)
        claimed_df = filtered_df[filtered_df["State"] == "Claimed"]
        if claimed_df.empty:
            continue

        # For each user, calculate their average GPU usage across buckets, then multiply by actual time
        user_bucket_totals = {}

        # Get all possible buckets for this slot type
        all_buckets = sorted(df["15min_bucket"].unique())
        num_buckets = len(all_buckets)

        for bucket in all_buckets:
            bucket_df = claimed_df[claimed_df["15min_bucket"] == bucket]
            if not bucket_df.empty:
                # Group by user within this time bucket
                user_gpu_counts = bucket_df.groupby("RemoteOwner")["AssignedGPUs"].nunique()
                for user, gpu_count in user_gpu_counts.items():
                    if pd.isna(user) or user == "" or user is None:
                        user = "Unknown"
                    if user not in user_bucket_totals:
                        user_bucket_totals[user] = 0
                    user_bucket_totals[user] += gpu_count

        # Convert totals to averages, then multiply by actual time duration
        for user, total_gpus in user_bucket_totals.items():
            avg_gpus = total_gpus / num_buckets if num_buckets > 0 else 0
            gpu_hours = avg_gpus * actual_duration_hours

            if user not in user_stats:
                user_stats[user] = {"Backfill-ResearcherOwned": 0, "Backfill-CHTCOwned": 0, "Backfill-OpenCapacity": 0}
            user_stats[user][slot_type] = gpu_hours

    # Calculate final statistics
    final_stats = {}
    for user, slot_data in user_stats.items():
        total_gpu_hours = sum(gpu_hours for gpu_hours in slot_data.values())
        if total_gpu_hours > 0:
            final_stats[user] = {"total_gpu_hours": total_gpu_hours, "slot_breakdown": {}}
            # Add breakdown by slot type (only include non-zero usage)
            for slot_type, gpu_hours in slot_data.items():
                if gpu_hours > 0:
                    final_stats[user]["slot_breakdown"][slot_type] = {
                        "gpu_hours": gpu_hours,
                        "percentage": (gpu_hours / total_gpu_hours) * 100,
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
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["15min_bucket"] = df["timestamp"].dt.floor("15min")

    total_claimed_across_intervals = 0
    total_available_across_intervals = 0
    num_intervals = 0

    # For each 15-minute interval, count unique GPUs across all categories
    for bucket in sorted(df["15min_bucket"].unique()):
        bucket_df = df[df["15min_bucket"] == bucket]

        if bucket_df.empty:
            continue

        # Get all unique GPUs across all categories (Priority, Shared, and Backfill)
        # Some GPUs may only appear in Backfill category (backfill-only GPUs)
        all_claimed_gpus = set()
        all_available_gpus = set()

        # Collect from all three categories to ensure we don't miss backfill-only GPUs
        for utilization_type in UTILIZATION_TYPES:
            claimed_df = filter_df(bucket_df, utilization_type, "Claimed", host)
            unclaimed_df = filter_df(bucket_df, utilization_type, "Unclaimed", host)

            # Add unique GPUs from this category
            claimed_gpus = set(claimed_df["AssignedGPUs"].dropna().unique())
            unclaimed_gpus = set(unclaimed_df["AssignedGPUs"].dropna().unique())

            all_claimed_gpus.update(claimed_gpus)
            all_available_gpus.update(claimed_gpus)  # claimed GPUs are part of available
            all_available_gpus.update(unclaimed_gpus)

        total_claimed_across_intervals += len(all_claimed_gpus)
        total_available_across_intervals += len(all_available_gpus)
        num_intervals += 1

    # Calculate averages
    avg_claimed = total_claimed_across_intervals / num_intervals if num_intervals > 0 else 0
    avg_available = total_available_across_intervals / num_intervals if num_intervals > 0 else 0

    return {"claimed": avg_claimed, "total": avg_available}


def calculate_machines_with_zero_active_gpus(
    df: pd.DataFrame, host: str = "", include_all_devices: bool = True
) -> dict:
    """
    Calculate machines that had ZERO active (claimed) GPUs across the entire time span.

    This identifies machines that had GPUs available but never had any claimed during
    the analysis period, which may indicate underutilized or problematic hosts.

    Args:
        df: Raw DataFrame with GPU state data
        host: Optional host filter
        include_all_devices: Whether to include all device types or filter out older ones

    Returns:
        Dictionary with machine information for hosts with zero claimed GPUs:
        {
            "machines": [
                {
                    "machine": str,
                    "gpu_model": str,
                    "total_gpus": int,
                    "total_observations": int,
                    "prioritized_projects": set
                },
                ...
            ],
            "summary": {
                "total_machines": int,
                "total_gpus_idle": int
            }
        }
    """
    # Ensure timestamp is datetime and create 15-minute buckets for consistency
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Apply host exclusions if configured (respects masked_hosts.yaml)
    if gpu_utils.HOST_EXCLUSIONS:
        for excluded_host in gpu_utils.HOST_EXCLUSIONS.keys():
            df = df[~df["Machine"].str.contains(excluded_host, case=False, na=False)]

    # Apply host filter if specified
    if host:
        df = df[df["Machine"] == host]

    # Track per machine: total GPUs seen, claimed GPUs seen, device name, prioritized projects
    machine_stats = {}

    for machine in df["Machine"].unique():
        machine_df = df[df["Machine"] == machine]

        # Separate primary slots from backfill slots (use .copy() to avoid SettingWithCopyWarning)
        primary_df = machine_df[~machine_df["Name"].str.contains("backfill", case=False, na=False)].copy()
        backfill_df = machine_df[machine_df["Name"].str.contains("backfill", case=False, na=False)].copy()

        # Get all unique GPUs on this machine (primary slots)
        all_gpus = set(primary_df["AssignedGPUs"].dropna().unique())

        # Get claimed GPUs on this machine (primary slots)
        claimed_df = primary_df[primary_df["State"] == "Claimed"]
        claimed_gpus = set(claimed_df["AssignedGPUs"].dropna().unique())

        # Calculate average backfill slots in use across 15-minute intervals
        if not backfill_df.empty:
            backfill_df["15min_bucket"] = backfill_df["timestamp"].dt.floor("15min")
            total_backfill_claimed = 0
            num_intervals = 0

            for bucket in sorted(backfill_df["15min_bucket"].unique()):
                bucket_df = backfill_df[backfill_df["15min_bucket"] == bucket]
                claimed_backfill = bucket_df[bucket_df["State"] == "Claimed"]
                total_backfill_claimed += len(claimed_backfill["AssignedGPUs"].dropna().unique())
                num_intervals += 1

            avg_backfill = total_backfill_claimed / num_intervals if num_intervals > 0 else 0
        else:
            avg_backfill = 0

        # Get GPU device name (most common one)
        gpu_models = machine_df["GPUs_DeviceName"].value_counts()
        gpu_model = gpu_models.index[0] if len(gpu_models) > 0 else "Unknown"

        # Get prioritized projects
        prioritized_projects = set()
        for proj in machine_df["PrioritizedProjects"].dropna().unique():
            if proj.strip():
                prioritized_projects.add(proj.strip())

        machine_stats[machine] = {
            "all_gpus": all_gpus,
            "claimed_gpus": claimed_gpus,
            "gpu_model": gpu_model,
            "total_observations": len(machine_df),
            "prioritized_projects": prioritized_projects,
            "avg_backfill_claimed": avg_backfill,
        }

    # Find machines with ZERO claimed GPUs
    machines_with_zero_active = []
    total_gpus_idle = 0

    for machine, stats in machine_stats.items():
        if len(stats["claimed_gpus"]) == 0 and len(stats["all_gpus"]) > 0:
            # Skip old/uncommon GPU types for cleaner output (unless requested to include all)
            gpu_model = stats["gpu_model"]
            if not include_all_devices and any(
                old_gpu in gpu_model for old_gpu in ["GTX 1080", "P100", "Quadro", "A30", "A40"]
            ):
                continue

            # This machine had GPUs but none were ever claimed
            machines_with_zero_active.append(
                {
                    "machine": machine,
                    "gpu_model": gpu_model,
                    "total_gpus": len(stats["all_gpus"]),
                    "total_observations": stats["total_observations"],
                    "prioritized_projects": stats["prioritized_projects"],
                    "avg_backfill_claimed": stats["avg_backfill_claimed"],
                }
            )
            total_gpus_idle += len(stats["all_gpus"])

    # Sort by machine name
    machines_with_zero_active.sort(key=lambda x: x["machine"])

    return {
        "machines": machines_with_zero_active,
        "summary": {
            "total_machines": len(machines_with_zero_active),
            "total_gpus_idle": total_gpus_idle,
        },
    }


def calculate_monthly_summary(db_path: str, end_time: datetime.datetime | None = None) -> dict:
    """
    Calculate complete monthly GPU usage summary for the previous month.

    Args:
        db_path: Path to SQLite database (used to determine base directory)
        end_time: Optional end time (defaults to latest data)

    Returns:
        Dictionary containing monthly usage statistics
    """
    import calendar
    from pathlib import Path

    # Get base directory from the provided db_path
    db_path_obj = Path(db_path)
    base_dir = str(db_path_obj.parent) if db_path_obj.parent != Path(".") else "."

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
            "month": prev_month_start.strftime("%B %Y"),
            "start_date": prev_month_start,
            "end_date": prev_month_end,
            "total_hours": total_hours,
        }

    # Calculate statistics for the month
    device_stats = calculate_allocation_usage_by_device_enhanced(df, "", False)  # All devices, no host filter
    memory_stats = calculate_allocation_usage_by_memory(df, "", False)  # All devices, no host filter
    h200_stats = calculate_h200_user_breakdown(df, "", total_hours)

    return {
        "month": prev_month_start.strftime("%B %Y"),
        "start_date": prev_month_start,
        "end_date": prev_month_end,
        "total_hours": total_hours,
        "device_stats": device_stats,
        "memory_stats": memory_stats,
        "h200_user_stats": h200_stats,
        "data_coverage": {
            "start_time": df["timestamp"].min(),
            "end_time": df["timestamp"].max(),
            "total_records": len(df),
            "unique_intervals": len(df["15min_bucket"].unique()) if "15min_bucket" in df.columns else 0,
        },
    }


def get_gpu_models_at_time(db_path: str, target_time: datetime.datetime, window_minutes: int = 5) -> list:
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

    return df["GPUs_DeviceName"].tolist()


def get_gpu_model_activity_at_time(
    db_path: str, gpu_model: str, target_time: datetime.datetime, window_minutes: int = 5
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
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df


def analyze_gpu_model_at_time(
    db_path: str, gpu_model: str, target_time: datetime.datetime, window_minutes: int = 5
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
        return {"error": f"No data found for {gpu_model} around {target_time.strftime('%Y-%m-%d %H:%M:%S')}"}

    # Get the closest timestamp to target
    df["time_diff"] = abs(df["timestamp"] - target_time)
    closest_time = df.loc[df["time_diff"].idxmin(), "timestamp"]

    # Filter to records from the closest timestamp
    snapshot_df = df[df["timestamp"] == closest_time]

    # Analyze the snapshot - count unique GPUs only
    unique_gpus = snapshot_df["AssignedGPUs"].dropna().nunique()

    # Count active GPUs (those actually running jobs with RemoteOwner)
    active_gpus_count = (
        snapshot_df[(snapshot_df["State"] == "Claimed") & (snapshot_df["RemoteOwner"].notna())]["AssignedGPUs"]
        .dropna()
        .nunique()
    )

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
    machines = snapshot_df["Machine"].unique()

    # Calculate utilization stats
    claimed_with_usage = snapshot_df[(snapshot_df["State"] == "Claimed") & (snapshot_df["GPUsAverageUsage"].notna())]

    avg_utilization = claimed_with_usage["GPUsAverageUsage"].mean() if len(claimed_with_usage) > 0 else 0

    # Get job information - ensure unique GPU IDs
    active_jobs_df = snapshot_df[(snapshot_df["State"] == "Claimed") & (snapshot_df["RemoteOwner"].notna())][
        ["RemoteOwner", "GlobalJobId", "AssignedGPUs", "Machine"]
    ].copy()

    # Remove duplicates based on AssignedGPUs, keeping first occurrence
    active_jobs = active_jobs_df.drop_duplicates(subset=["AssignedGPUs"], keep="first")

    # Get inactive GPUs - ensure unique GPU IDs and exclude ones that appear in active jobs
    inactive_gpus_df = snapshot_df[snapshot_df["State"] == "Unclaimed"][
        ["AssignedGPUs", "Machine", "PrioritizedProjects"]
    ].copy()

    # Remove duplicates based on AssignedGPUs, keeping first occurrence
    inactive_gpus_unique = inactive_gpus_df.drop_duplicates(subset=["AssignedGPUs"], keep="first")

    # Get list of GPU IDs that are active (have jobs running)
    active_gpu_ids = set(active_jobs["AssignedGPUs"].dropna().tolist())

    # Filter out GPUs that appear in active jobs list
    inactive_gpus = inactive_gpus_unique[~inactive_gpus_unique["AssignedGPUs"].isin(active_gpu_ids)]

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
            "num_machines": len(machines),
        },
        "by_class": {
            "Priority": {
                "total": priority_gpus["AssignedGPUs"].dropna().nunique(),
                "claimed": priority_gpus[priority_gpus["State"] == "Claimed"]["AssignedGPUs"].dropna().nunique(),
            },
            "Shared": {
                "total": shared_gpus["AssignedGPUs"].dropna().nunique(),
                "claimed": shared_gpus[shared_gpus["State"] == "Claimed"]["AssignedGPUs"].dropna().nunique(),
            },
            "Backfill": {
                "total": backfill_gpus["AssignedGPUs"].dropna().nunique(),
                "claimed": backfill_gpus[backfill_gpus["State"] == "Claimed"]["AssignedGPUs"].dropna().nunique(),
            },
        },
        "machines": list(machines),
        "active_jobs": active_jobs.to_dict("records") if len(active_jobs) > 0 else [],
        "inactive_gpus": inactive_gpus.to_dict("records") if len(inactive_gpus) > 0 else [],
        "raw_data": snapshot_df,
    }
